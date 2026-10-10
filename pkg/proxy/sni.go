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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/sni"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	"k8s.io/klog/v2"
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

func closeConn(c io.Closer) {
	if c == nil {
		return
	}
	if err := c.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		klog.V(2).Infof("failed to close connection: %v", err)
	}
}

func isNormalClose(err error) bool {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	return strings.Contains(err.Error(), "use of closed network connection")
}

func copyAndCloseWrite(dst, src net.Conn) error {
	var errs []error
	if _, err := io.Copy(dst, src); err != nil && !isNormalClose(err) {
		errs = append(errs, err)
	}
	if err := closeWrite(dst); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (l *sniListener) Accept() (net.Conn, error) {
	select {
	case <-l.done:
		return nil, net.ErrClosed
	case conn := <-l.httpsConnCh:
		select {
		case <-l.done:
			closeConn(conn)
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
				closeConn(c)
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
				closeConn(conn)
			case l.httpsConnCh <- conn:
			}
			continue
		}

		go l.handleConn(conn, cfg)
	}
}

func (l *sniListener) handleConn(conn net.Conn, cfg *sniConfig) {
	sniHostname, peekedConn, err := sni.SniffSNI(conn, 5*time.Second)
	if peekedConn == nil {
		peekedConn = conn
	}
	handedOff := false
	defer func() {
		if !handedOff {
			closeConn(peekedConn)
		}
	}()

	if err != nil && !errors.Is(err, sni.ErrNoSNI) {
		return
	}

	handOffToHTTPS := func() {
		select {
		case l.httpsConnCh <- peekedConn:
			handedOff = true
		case <-l.done:
		}
	}

	candidates := cfg.candidates
	if len(candidates) == 0 {
		handOffToHTTPS()
		return
	}

	matched, matchType := state.MatchListeners(candidates, sniHostname)
	if matchType != state.NoMatch && len(matched) > 0 {
		selected := matched[0]
		if selected.Protocol == gatewayv1.TLSProtocolType && selected.TLSMode != nil && *selected.TLSMode == gatewayv1.TLSModePassthrough {
			backend, ok := selected.SelectTLSBackend(sniHostname)
			if !ok || backend == "" {
				return
			}
			handedOff = true
			spliceToBackend(peekedConn, backend)
			return
		}

		if selected.Protocol == gatewayv1.TLSProtocolType && (selected.TLSMode == nil || *selected.TLSMode == gatewayv1.TLSModeTerminate) {
			backend, ok := selected.SelectTLSBackend(sniHostname)
			if !ok || backend == "" {
				return
			}
			handedOff = true
			l.terminateTLSToBackend(peekedConn, sniHostname, backend)
			return
		}

		if selected.Protocol == gatewayv1.HTTPSProtocolType {
			handOffToHTTPS()
			return
		}
	}

	// If an SNI was provided and matched no listener: fail closed.
	if sniHostname != "" {
		return
	}

	// No SNI provided (e.g. direct IP connection): hand to HTTPS server
	handOffToHTTPS()
}

func (l *sniListener) terminateTLSToBackend(clientConn net.Conn, sniHostname, backend string) {
	select {
	case <-l.done:
		closeConn(clientConn)
		return
	default:
	}

	if !l.p.HasCertificate(sniHostname) {
		closeConn(clientConn)
		return
	}

	tlsConfig := &tls.Config{
		GetCertificate: l.p.GetCertificate,
	}
	tlsConn := tls.Server(clientConn, tlsConfig)
	if err := clientConn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		klog.V(2).Infof("failed to set deadline for TLS handshake: %v", err)
		closeConn(clientConn)
		return
	}
	if err := tlsConn.Handshake(); err != nil {
		closeConn(clientConn)
		return
	}
	if err := clientConn.SetDeadline(time.Time{}); err != nil {
		klog.V(2).Infof("failed to clear deadline after TLS handshake: %v", err)
		closeConn(tlsConn)
		return
	}

	dialer := &net.Dialer{Timeout: 5 * time.Second}
	backendConn, err := dialer.Dial("tcp", backend)
	if err != nil {
		closeConn(tlsConn)
		return
	}

	go func() {
		defer closeConn(tlsConn)
		defer closeConn(backendConn)

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			if err := copyAndCloseWrite(backendConn, tlsConn); err != nil {
				klog.V(2).Infof("splice copy error: %v", err)
			}
		}()

		go func() {
			defer wg.Done()
			if err := copyAndCloseWrite(tlsConn, backendConn); err != nil {
				klog.V(2).Infof("splice copy error: %v", err)
			}
		}()

		wg.Wait()
	}()
}

func spliceToBackend(clientConn net.Conn, backend string) {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	backendConn, err := dialer.Dial("tcp", backend)
	if err != nil {
		closeConn(clientConn)
		return
	}

	go func() {
		defer closeConn(clientConn)
		defer closeConn(backendConn)

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			if err := copyAndCloseWrite(backendConn, clientConn); err != nil {
				klog.V(2).Infof("splice copy error: %v", err)
			}
		}()

		go func() {
			defer wg.Done()
			if err := copyAndCloseWrite(clientConn, backendConn); err != nil {
				klog.V(2).Infof("splice copy error: %v", err)
			}
		}()

		wg.Wait()
	}()
}

func closeWrite(conn net.Conn) error {
	type closeWriter interface {
		CloseWrite() error
	}
	if cw, ok := conn.(closeWriter); ok {
		if err := cw.CloseWrite(); err != nil && !errors.Is(err, net.ErrClosed) {
			klog.V(2).Infof("failed to close write: %v", err)
			return err
		}
	} else if pc, ok := conn.(*sni.PeekedConn); ok {
		if cw, ok := pc.RawConn().(closeWriter); ok {
			if err := cw.CloseWrite(); err != nil && !errors.Is(err, net.ErrClosed) {
				klog.V(2).Infof("failed to close write: %v", err)
				return err
			}
		}
	}
	return nil
}
