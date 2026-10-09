// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/sni"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

type sniListener struct {
	rawLis      net.Listener
	p           *Proxy
	httpsConnCh chan net.Conn
	closed      atomic.Bool
	closeOnce   sync.Once
	done        chan struct{}
}

// NewSNIListener creates a net.Listener that wraps rawLis, inspects TLS SNI on incoming connections,
// splices TLS Passthrough connections directly to their backend, and forwards matching HTTPS connections
// to callers of Accept(). Connections matching no listener or non-matching routes are closed without response.
func (p *Proxy) NewSNIListener(rawLis net.Listener) net.Listener {
	l := &sniListener{
		rawLis:      rawLis,
		p:           p,
		httpsConnCh: make(chan net.Conn, 128),
		done:        make(chan struct{}),
	}
	go l.acceptLoop()
	return l
}

func (l *sniListener) Accept() (net.Conn, error) {
	select {
	case <-l.done:
		return nil, net.ErrClosed
	case conn := <-l.httpsConnCh:
		select {
		case <-l.done:
			_ = conn.Close()
			return nil, net.ErrClosed
		default:
			return conn, nil
		}
	}
}

func (l *sniListener) Close() error {
	var err error
	l.closeOnce.Do(func() {
		l.closed.Store(true)
		close(l.done)
		err = l.rawLis.Close()

		// Drain and close any queued connections after closing done.
		for {
			select {
			case c := <-l.httpsConnCh:
				_ = c.Close()
			default:
				return
			}
		}
	})
	return err
}

func (l *sniListener) Addr() net.Addr {
	return l.rawLis.Addr()
}

func (l *sniListener) acceptLoop() {
	for {
		conn, err := l.rawLis.Accept()
		if err != nil {
			if l.closed.Load() {
				return
			}
			select {
			case <-l.done:
				return
			case <-time.After(5 * time.Millisecond):
			}
			continue
		}

		cfg := l.p.sniConfig.Load()
		if cfg == nil || !cfg.hasPassthrough {
			select {
			case <-l.done:
				_ = conn.Close()
			case l.httpsConnCh <- conn:
			}
			continue
		}

		go l.handleConn(conn, cfg)
	}
}

func (l *sniListener) handleConn(conn net.Conn, cfg *sniConfig) {
	sniHostname, peekedConn, err := sni.SniffSNI(conn, 5*time.Second)
	if err != nil && !errors.Is(err, sni.ErrNoSNI) {
		_ = peekedConn.Close()
		return
	}

	candidates := cfg.candidates
	if len(candidates) == 0 {
		select {
		case l.httpsConnCh <- peekedConn:
		case <-l.done:
			_ = peekedConn.Close()
		}
		return
	}

	matched, matchType := state.MatchListeners(candidates, sniHostname)
	if matchType != state.NoMatch && len(matched) > 0 {
		selected := matched[0]
		if selected.Protocol == gatewayv1.TLSProtocolType && selected.TLSMode != nil && *selected.TLSMode == gatewayv1.TLSModePassthrough {
			backend, ok := selected.SelectTLSBackend(sniHostname)
			if !ok || backend == "" {
				_ = peekedConn.Close()
				return
			}
			spliceToBackend(peekedConn, backend)
			return
		}

		if selected.Protocol == gatewayv1.TLSProtocolType && (selected.TLSMode == nil || *selected.TLSMode == gatewayv1.TLSModeTerminate) {
			backend, ok := selected.SelectTLSBackend(sniHostname)
			if !ok || backend == "" {
				_ = peekedConn.Close()
				return
			}
			l.terminateTLSToBackend(peekedConn, sniHostname, backend)
			return
		}

		if selected.Protocol == gatewayv1.HTTPSProtocolType {
			select {
			case l.httpsConnCh <- peekedConn:
			case <-l.done:
				_ = peekedConn.Close()
			}
			return
		}
	}

	// If an SNI was provided and matched no listener: fail closed.
	if sniHostname != "" {
		_ = peekedConn.Close()
		return
	}

	// No SNI provided (e.g. direct IP connection): hand to HTTPS server
	select {
	case l.httpsConnCh <- peekedConn:
	case <-l.done:
		_ = peekedConn.Close()
	}
}

func (l *sniListener) terminateTLSToBackend(clientConn net.Conn, sniHostname, backend string) {
	select {
	case <-l.done:
		_ = clientConn.Close()
		return
	default:
	}

	if !l.p.HasCertificate(sniHostname) {
		_ = clientConn.Close()
		return
	}

	tlsConfig := &tls.Config{
		GetCertificate: l.p.GetCertificate,
	}
	tlsConn := tls.Server(clientConn, tlsConfig)
	_ = clientConn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := tlsConn.Handshake(); err != nil {
		_ = clientConn.Close()
		return
	}
	_ = clientConn.SetDeadline(time.Time{})

	dialer := &net.Dialer{Timeout: 5 * time.Second}
	backendConn, err := dialer.Dial("tcp", backend)
	if err != nil {
		_ = tlsConn.Close()
		return
	}

	go func() {
		defer tlsConn.Close()
		defer backendConn.Close()

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			_, _ = io.Copy(backendConn, tlsConn)
			closeWrite(backendConn)
		}()

		go func() {
			defer wg.Done()
			_, _ = io.Copy(tlsConn, backendConn)
			closeWrite(tlsConn)
		}()

		wg.Wait()
	}()
}

func spliceToBackend(clientConn net.Conn, backend string) {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	backendConn, err := dialer.Dial("tcp", backend)
	if err != nil {
		_ = clientConn.Close()
		return
	}

	go func() {
		defer clientConn.Close()
		defer backendConn.Close()

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			_, _ = io.Copy(backendConn, clientConn)
			closeWrite(backendConn)
		}()

		go func() {
			defer wg.Done()
			_, _ = io.Copy(clientConn, backendConn)
			closeWrite(clientConn)
		}()

		wg.Wait()
	}()
}

func closeWrite(conn net.Conn) {
	type closeWriter interface {
		CloseWrite() error
	}
	if cw, ok := conn.(closeWriter); ok {
		_ = cw.CloseWrite()
	} else if pc, ok := conn.(*sni.PeekedConn); ok {
		if cw, ok := pc.RawConn().(closeWriter); ok {
			_ = cw.CloseWrite()
		}
	}
}
