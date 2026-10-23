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

package proxyproto

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// ListenerOptions configures source filtering and PROXY protocol parsing on a Listener.
type ListenerOptions struct {
	// AllowedSourceCIDRs is a list of CIDRs permitted to connect to this listener.
	// If empty, all source IPs are allowed.
	AllowedSourceCIDRs []string

	// ProxyProtocolTrustedCIDRs is a list of CIDRs from which PROXY headers are parsed.
	// If a connection's source IP is not in this list, no PROXY header is parsed,
	// and the original remote address is preserved.
	ProxyProtocolTrustedCIDRs []string

	// ProxyProtocol enables PROXY protocol parsing when set to "enabled".
	// If empty or "disabled", PROXY headers are not parsed.
	ProxyProtocol string
}

// Listener wraps a net.Listener to enforce source CIDR allowlists and parse
// PROXY headers strictly from trusted CIDRs.
type Listener struct {
	net.Listener
	mu           sync.RWMutex
	allowedCIDRs []*net.IPNet
	trustedCIDRs []*net.IPNet
	proxyEnabled bool
}

// NewListener creates a new filtered Listener wrapping the provided net.Listener.
func NewListener(lis net.Listener, opts ListenerOptions) (*Listener, error) {
	l := &Listener{
		Listener: lis,
	}
	if err := l.UpdateOptions(opts); err != nil {
		return nil, err
	}
	return l, nil
}

// UpdateOptions dynamically updates the listener's CIDR filters and PROXY protocol settings.
func (l *Listener) UpdateOptions(opts ListenerOptions) error {
	allowed, err := ParseCIDRs(opts.AllowedSourceCIDRs)
	if err != nil {
		return fmt.Errorf("parsing allowedSourceCIDRs: %w", err)
	}

	trusted, err := ParseCIDRs(opts.ProxyProtocolTrustedCIDRs)
	if err != nil {
		return fmt.Errorf("parsing proxyProtocolTrustedCIDRs: %w", err)
	}

	enabled := strings.EqualFold(opts.ProxyProtocol, "enabled") || (opts.ProxyProtocol == "" && len(trusted) > 0)

	l.mu.Lock()
	defer l.mu.Unlock()
	l.allowedCIDRs = allowed
	l.trustedCIDRs = trusted
	l.proxyEnabled = enabled
	return nil
}

func closeConn(c io.Closer) {
	if c == nil {
		return
	}
	if err := c.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		_ = err
	}
}

// Accept waits for and returns the next connection to the listener.
// Connections from sources outside AllowedSourceCIDRs are closed immediately before TLS.
// For sources within ProxyProtocolTrustedCIDRs, the PROXY header is decoded and stripped.
func (l *Listener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}

		l.mu.RLock()
		allowed := l.allowedCIDRs
		trusted := l.trustedCIDRs
		proxyEnabled := l.proxyEnabled
		l.mu.RUnlock()

		remoteIP := extractIP(conn.RemoteAddr())

		// 1. Check allowedSourceCIDRs: close connection immediately if not allowed.
		if len(allowed) > 0 {
			if !matchIP(remoteIP, allowed) {
				closeConn(conn)
				continue
			}
		}

		// 2. Check proxyProtocolTrustedCIDRs: parse PROXY header only if trusted.
		if proxyEnabled && len(trusted) > 0 && matchIP(remoteIP, trusted) {
			if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				closeConn(conn)
				continue
			}
			bufReader := bufio.NewReader(conn)
			hdr, err := Decode(bufReader)
			if err != nil {
				closeConn(conn)
				continue
			}
			if err := conn.SetReadDeadline(time.Time{}); err != nil {
				closeConn(conn)
				continue
			}

			var remaining []byte
			if bufReader.Buffered() > 0 {
				remaining = make([]byte, bufReader.Buffered())
				if _, err := io.ReadFull(bufReader, remaining); err != nil {
					closeConn(conn)
					continue
				}
			}

			var remoteAddr net.Addr = conn.RemoteAddr()
			if hdr.Command == CommandProxy && hdr.SrcAddr != nil {
				remoteAddr = hdr.SrcAddr
			}
			var localAddr net.Addr = conn.LocalAddr()
			if hdr.DstAddr != nil {
				localAddr = hdr.DstAddr
			}

			return NewConn(conn, remoteAddr, localAddr, remaining), nil
		}

		// Remote IP is not in trusted CIDRs or proxy protocol is disabled:
		// Do not parse PROXY header, preserve real RemoteAddr.
		return conn, nil
	}
}

// ParseCIDRs parses a list of CIDR strings into []*net.IPNet.
// Items can be comma-separated or separate list elements.
func ParseCIDRs(cidrs []string) ([]*net.IPNet, error) {
	var result []*net.IPNet
	for _, entry := range cidrs {
		parts := strings.Split(entry, ",")
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed == "" {
				continue
			}
			_, ipNet, err := net.ParseCIDR(trimmed)
			if err != nil {
				return nil, fmt.Errorf("invalid CIDR %q: %w", trimmed, err)
			}
			result = append(result, ipNet)
		}
	}
	return result, nil
}

func extractIP(addr net.Addr) net.IP {
	if addr == nil {
		return nil
	}
	if tcpAddr, ok := addr.(*net.TCPAddr); ok {
		if ip4 := tcpAddr.IP.To4(); ip4 != nil {
			return ip4
		}
		return tcpAddr.IP
	}
	if udpAddr, ok := addr.(*net.UDPAddr); ok {
		if ip4 := udpAddr.IP.To4(); ip4 != nil {
			return ip4
		}
		return udpAddr.IP
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		host = addr.String()
	}
	ip := net.ParseIP(host)
	if ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			return ip4
		}
	}
	return ip
}

func matchIP(ip net.IP, cidrs []*net.IPNet) bool {
	if ip == nil {
		return false
	}
	if ip4 := ip.To4(); ip4 != nil {
		ip = ip4
	}
	for _, cidr := range cidrs {
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}
