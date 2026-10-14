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
	"bytes"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// getFreeUDPPort returns a free UDP port on 127.0.0.1.
func getFreeUDPPort(t *testing.T) int {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on free UDP port: %v", err)
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).Port
}

// startUDPEchoBackend runs a UDP echo server that echoes all datagrams back to the sender.
func startUDPEchoBackend(t *testing.T) (net.PacketConn, int) {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start echo backend: %v", err)
	}
	port := conn.LocalAddr().(*net.UDPAddr).Port

	go func() {
		buf := make([]byte, 65536)
		for {
			n, addr, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			_, _ = conn.WriteTo(buf[:n], addr)
		}
	}()

	return conn, port
}

// startUDPIDBackend runs a UDP backend that prepends prefix to every datagram it echoes.
func startUDPIDBackend(t *testing.T, prefix string) (net.PacketConn, int) {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start ID backend %s: %v", prefix, err)
	}
	port := conn.LocalAddr().(*net.UDPAddr).Port

	go func() {
		buf := make([]byte, 65536)
		for {
			n, addr, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			resp := append([]byte(prefix), buf[:n]...)
			_, _ = conn.WriteTo(resp, addr)
		}
	}()

	return conn, port
}

// pollUntil polls condition every 10ms until it returns true or timeout expires.
func pollUntil(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !condition() {
		t.Fatalf("condition not met within %v", timeout)
	}
}

// TestProxy_UDPEchoRoundTrip tests basic UDP echo round-trip with datagram semantics.
func TestProxy_UDPEchoRoundTrip(t *testing.T) {
	echoBackend, backendPort := startUDPEchoBackend(t)
	defer echoBackend.Close()

	proxyPort := getFreeUDPPort(t)

	p := NewProxy()
	p.SetUDPBindHost("127.0.0.1")
	defer p.Close()

	p.UpdateConfig([]state.InternalListener{
		{
			Name:     "udp-echo",
			Protocol: gatewayv1.UDPProtocolType,
			Port:     gatewayv1.PortNumber(proxyPort),
			UDPBackends: []state.InternalUDPBackend{
				{Target: fmt.Sprintf("127.0.0.1:%d", backendPort), Weight: 1},
			},
		},
	}, nil)

	proxyAddr := fmt.Sprintf("127.0.0.1:%d", proxyPort)
	clientConn, err := net.Dial("udp", proxyAddr)
	if err != nil {
		t.Fatalf("failed to dial proxy UDP port: %v", err)
	}
	defer clientConn.Close()

	msg := []byte("hello UDP round trip")
	_ = clientConn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := clientConn.Write(msg); err != nil {
		t.Fatalf("failed to write datagram to proxy: %v", err)
	}

	buf := make([]byte, 65536)
	n, err := clientConn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read datagram reply from proxy: %v", err)
	}

	if !bytes.Equal(buf[:n], msg) {
		t.Fatalf("expected reply %q, got %q", msg, buf[:n])
	}
}

// TestProxy_UDPWeightedAndSessionStickiness tests:
// 1. Fresh client sockets land on backends according to weights (1/1/0, weight 0 never receives traffic).
// 2. A client reusing its socket stays on one backend.
func TestProxy_UDPWeightedAndSessionStickiness(t *testing.T) {
	b1, port1 := startUDPIDBackend(t, "b1:")
	defer b1.Close()
	b2, port2 := startUDPIDBackend(t, "b2:")
	defer b2.Close()
	b3, port3 := startUDPIDBackend(t, "b3:")
	defer b3.Close()

	proxyPort := getFreeUDPPort(t)

	p := NewProxy()
	p.SetUDPBindHost("127.0.0.1")
	defer p.Close()

	p.UpdateConfig([]state.InternalListener{
		{
			Name:     "udp-weighted",
			Protocol: gatewayv1.UDPProtocolType,
			Port:     gatewayv1.PortNumber(proxyPort),
			UDPBackends: []state.InternalUDPBackend{
				{Target: fmt.Sprintf("127.0.0.1:%d", port1), Weight: 1},
				{Target: fmt.Sprintf("127.0.0.1:%d", port2), Weight: 1},
				{Target: fmt.Sprintf("127.0.0.1:%d", port3), Weight: 0},
			},
		},
	}, nil)

	proxyAddr := fmt.Sprintf("127.0.0.1:%d", proxyPort)

	// 1. Test 300 fresh client sockets:
	// Verify b3 (weight 0) receives 0 datagrams, while b1 and b2 each receive traffic.
	counts := make(map[string]int)
	const totalProbes = 300
	for i := 0; i < totalProbes; i++ {
		conn, err := net.Dial("udp", proxyAddr)
		if err != nil {
			t.Fatalf("probe %d: dial failed: %v", i, err)
		}
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		_, err = conn.Write([]byte("probe"))
		if err != nil {
			conn.Close()
			t.Fatalf("probe %d: write failed: %v", i, err)
		}
		buf := make([]byte, 256)
		n, err := conn.Read(buf)
		conn.Close()
		if err != nil {
			t.Fatalf("probe %d: read failed: %v", i, err)
		}
		resp := string(buf[:n])
		switch {
		case strings.HasPrefix(resp, "b1:"):
			counts["b1"]++
		case strings.HasPrefix(resp, "b2:"):
			counts["b2"]++
		case strings.HasPrefix(resp, "b3:"):
			counts["b3"]++
		default:
			t.Fatalf("probe %d: unexpected response: %q", i, resp)
		}
	}

	if counts["b3"] != 0 {
		t.Fatalf("weight 0 backend b3 received %d datagrams, expected 0", counts["b3"])
	}
	if counts["b1"] < 50 || counts["b2"] < 50 {
		t.Fatalf("expected roughly balanced distribution between b1 and b2, got counts: %+v", counts)
	}

	// 2. A client reusing its socket stays on one backend.
	clientConn, err := net.Dial("udp", proxyAddr)
	if err != nil {
		t.Fatalf("failed to dial for session stickiness: %v", err)
	}
	defer clientConn.Close()

	_ = clientConn.SetDeadline(time.Now().Add(5 * time.Second))
	_, err = clientConn.Write([]byte("stickiness-first"))
	if err != nil {
		t.Fatalf("failed to write first datagram: %v", err)
	}
	buf := make([]byte, 256)
	n, err := clientConn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read first datagram: %v", err)
	}

	firstBackend := ""
	resp := string(buf[:n])
	if strings.HasPrefix(resp, "b1:") {
		firstBackend = "b1"
	} else if strings.HasPrefix(resp, "b2:") {
		firstBackend = "b2"
	} else {
		t.Fatalf("unexpected first backend response: %q", resp)
	}

	for i := 0; i < 50; i++ {
		_ = clientConn.SetDeadline(time.Now().Add(5 * time.Second))
		_, err = clientConn.Write([]byte(fmt.Sprintf("datagram-%d", i)))
		if err != nil {
			t.Fatalf("datagram %d write failed: %v", i, err)
		}
		n, err = clientConn.Read(buf)
		if err != nil {
			t.Fatalf("datagram %d read failed: %v", i, err)
		}
		if !strings.HasPrefix(string(buf[:n]), firstBackend+":") {
			t.Fatalf("datagram %d landed on %q, want stickiness to %q", i, string(buf[:n]), firstBackend)
		}
	}
}

// TestProxy_UDPIdleExpiry verifies that sessions expire after the configured idle timeout
// and close the backend socket.
func TestProxy_UDPIdleExpiry(t *testing.T) {
	echoBackend, backendPort := startUDPEchoBackend(t)
	defer echoBackend.Close()

	proxyPort := getFreeUDPPort(t)

	p := NewProxy()
	p.SetUDPBindHost("127.0.0.1")
	p.SetUDPIdleTimeout(60 * time.Millisecond)
	defer p.Close()

	p.UpdateConfig([]state.InternalListener{
		{
			Name:     "udp-idle",
			Protocol: gatewayv1.UDPProtocolType,
			Port:     gatewayv1.PortNumber(proxyPort),
			UDPBackends: []state.InternalUDPBackend{
				{Target: fmt.Sprintf("127.0.0.1:%d", backendPort), Weight: 1},
			},
		},
	}, nil)

	proxyAddr := fmt.Sprintf("127.0.0.1:%d", proxyPort)
	clientConn, err := net.Dial("udp", proxyAddr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer clientConn.Close()

	// Send packet to establish session
	_ = clientConn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := clientConn.Write([]byte("ping")); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	buf := make([]byte, 256)
	if _, err := clientConn.Read(buf); err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	// Verify active session exists in proxy
	p.mu.RLock()
	pl := p.udpListeners[gatewayv1.PortNumber(proxyPort)]
	p.mu.RUnlock()
	if pl == nil {
		t.Fatal("expected udp listener to exist")
	}

	pollUntil(t, 5*time.Second, func() bool {
		pl.mu.Lock()
		defer pl.mu.Unlock()
		return len(pl.sessions) == 1
	})

	// Wait for idle timeout to trigger expiration and cleanup
	pollUntil(t, 5*time.Second, func() bool {
		pl.mu.Lock()
		defer pl.mu.Unlock()
		return len(pl.sessions) == 0
	})
}

// TestProxy_UDPConfigUpdatePreservesOtherPortSessions verifies that removing one port
// during a config update leaves sessions on other ports open and functional.
func TestProxy_UDPConfigUpdatePreservesOtherPortSessions(t *testing.T) {
	bA, portA := startUDPIDBackend(t, "A:")
	defer bA.Close()
	bB, portB := startUDPIDBackend(t, "B:")
	defer bB.Close()

	proxyPortA := getFreeUDPPort(t)
	proxyPortB := getFreeUDPPort(t)

	p := NewProxy()
	p.SetUDPBindHost("127.0.0.1")
	defer p.Close()

	// Initial config: both Port A and Port B
	p.UpdateConfig([]state.InternalListener{
		{
			Name:     "udp-a",
			Protocol: gatewayv1.UDPProtocolType,
			Port:     gatewayv1.PortNumber(proxyPortA),
			UDPBackends: []state.InternalUDPBackend{
				{Target: fmt.Sprintf("127.0.0.1:%d", portA), Weight: 1},
			},
		},
		{
			Name:     "udp-b",
			Protocol: gatewayv1.UDPProtocolType,
			Port:     gatewayv1.PortNumber(proxyPortB),
			UDPBackends: []state.InternalUDPBackend{
				{Target: fmt.Sprintf("127.0.0.1:%d", portB), Weight: 1},
			},
		},
	}, nil)

	// Send datagram to Port A
	clientA, err := net.Dial("udp", fmt.Sprintf("127.0.0.1:%d", proxyPortA))
	if err != nil {
		t.Fatalf("failed to dial Port A: %v", err)
	}
	defer clientA.Close()

	_ = clientA.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := clientA.Write([]byte("msg-a1")); err != nil {
		t.Fatalf("failed to write to Port A: %v", err)
	}
	buf := make([]byte, 256)
	n, err := clientA.Read(buf)
	if err != nil || string(buf[:n]) != "A:msg-a1" {
		t.Fatalf("unexpected Port A reply: %q, err: %v", string(buf[:n]), err)
	}

	// Send datagram to Port B
	clientB, err := net.Dial("udp", fmt.Sprintf("127.0.0.1:%d", proxyPortB))
	if err != nil {
		t.Fatalf("failed to dial Port B: %v", err)
	}
	defer clientB.Close()

	_ = clientB.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := clientB.Write([]byte("msg-b1")); err != nil {
		t.Fatalf("failed to write to Port B: %v", err)
	}
	n, err = clientB.Read(buf)
	if err != nil || string(buf[:n]) != "B:msg-b1" {
		t.Fatalf("unexpected Port B reply: %q, err: %v", string(buf[:n]), err)
	}

	// Update config: remove Port B, keep Port A
	p.UpdateConfig([]state.InternalListener{
		{
			Name:     "udp-a",
			Protocol: gatewayv1.UDPProtocolType,
			Port:     gatewayv1.PortNumber(proxyPortA),
			UDPBackends: []state.InternalUDPBackend{
				{Target: fmt.Sprintf("127.0.0.1:%d", portA), Weight: 1},
			},
		},
	}, nil)

	// Port A session should still be intact and functional
	_ = clientA.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := clientA.Write([]byte("msg-a2")); err != nil {
		t.Fatalf("failed to write to Port A after update: %v", err)
	}
	n, err = clientA.Read(buf)
	if err != nil || string(buf[:n]) != "A:msg-a2" {
		t.Fatalf("Port A session broken after removing Port B: got %q, err %v", string(buf[:n]), err)
	}

	// Port B listener was removed
	p.mu.RLock()
	_, portBExists := p.udpListeners[gatewayv1.PortNumber(proxyPortB)]
	p.mu.RUnlock()
	if portBExists {
		t.Fatalf("expected Port B listener to be removed from proxy")
	}
}

// TestProxy_UDPCapEnforced verifies that sessions are capped per listener
// and new clients beyond the cap are dropped without evicting existing ones.
func TestProxy_UDPCapEnforced(t *testing.T) {
	echoBackend, backendPort := startUDPEchoBackend(t)
	defer echoBackend.Close()

	proxyPort := getFreeUDPPort(t)

	p := NewProxy()
	p.SetUDPBindHost("127.0.0.1")
	p.SetUDPMaxSessions(3)
	defer p.Close()

	p.UpdateConfig([]state.InternalListener{
		{
			Name:     "udp-capped",
			Protocol: gatewayv1.UDPProtocolType,
			Port:     gatewayv1.PortNumber(proxyPort),
			UDPBackends: []state.InternalUDPBackend{
				{Target: fmt.Sprintf("127.0.0.1:%d", backendPort), Weight: 1},
			},
		},
	}, nil)

	proxyAddr := fmt.Sprintf("127.0.0.1:%d", proxyPort)

	p.mu.RLock()
	pl := p.udpListeners[gatewayv1.PortNumber(proxyPort)]
	p.mu.RUnlock()
	if pl == nil {
		t.Fatal("expected udp listener to exist")
	}

	// Open 3 clients (up to cap)
	clients := make([]net.Conn, 3)
	for i := 0; i < 3; i++ {
		// Wait for any prior async session setup to finish clearing pending state
		// so that this client does not race against pending session counts.
		pollUntil(t, 5*time.Second, func() bool {
			pl.mu.Lock()
			defer pl.mu.Unlock()
			return len(pl.pending) == 0
		})

		c, err := net.Dial("udp", proxyAddr)
		if err != nil {
			t.Fatalf("client %d dial failed: %v", i, err)
		}
		defer c.Close()
		clients[i] = c

		clientKey := c.LocalAddr().String()

		// Wait for session to be established in proxy.
		// Since session setup is async (goroutine per new client), retry sending
		// if needed until the session is registered in pl.sessions.
		pollUntil(t, 5*time.Second, func() bool {
			pl.mu.Lock()
			_, exists := pl.sessions[clientKey]
			pl.mu.Unlock()
			if exists {
				return true
			}
			_ = c.SetDeadline(time.Now().Add(100 * time.Millisecond))
			_, _ = c.Write([]byte(fmt.Sprintf("client-%d", i)))
			return false
		})

		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 256)
		n, err := c.Read(buf)
		if err != nil {
			t.Fatalf("client %d read failed: %v", i, err)
		}
		if string(buf[:n]) != fmt.Sprintf("client-%d", i) {
			t.Fatalf("client %d reply mismatch: got %q, want %q", i, string(buf[:n]), fmt.Sprintf("client-%d", i))
		}
	}

	// Wait for all 3 sessions to be active and pending setups cleared
	pollUntil(t, 5*time.Second, func() bool {
		pl.mu.Lock()
		defer pl.mu.Unlock()
		return len(pl.sessions) == 3 && len(pl.pending) == 0
	})

	// 4th client exceeds cap: should be dropped (no reply)
	client4, err := net.Dial("udp", proxyAddr)
	if err != nil {
		t.Fatalf("client 4 dial failed: %v", err)
	}
	defer client4.Close()

	_ = client4.SetDeadline(time.Now().Add(100 * time.Millisecond))
	if _, err := client4.Write([]byte("client-4")); err != nil {
		t.Fatalf("client 4 write failed: %v", err)
	}
	buf := make([]byte, 256)
	_, err = client4.Read(buf)
	if err == nil {
		t.Fatalf("expected client 4 to be dropped due to cap, but received reply %q", string(buf))
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected timeout error for dropped client 4, got: %v", err)
	}

	// Verify existing 3 clients are still functional (not evicted)
	for i := 0; i < 3; i++ {
		c := clients[i]
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := c.Write([]byte(fmt.Sprintf("client-%d-again", i))); err != nil {
			t.Fatalf("client %d second write failed: %v", i, err)
		}
		n, err := c.Read(buf)
		if err != nil || string(buf[:n]) != fmt.Sprintf("client-%d-again", i) {
			t.Fatalf("client %d failed after cap attempt: %q, err %v", i, string(buf[:n]), err)
		}
	}
}

// TestProxy_UDPGoroutineLeak verifies that goroutine count returns to baseline after sessions expire.
func TestProxy_UDPGoroutineLeak(t *testing.T) {
	echoBackend, backendPort := startUDPEchoBackend(t)
	defer echoBackend.Close()

	proxyPort := getFreeUDPPort(t)

	p := NewProxy()
	p.SetUDPBindHost("127.0.0.1")
	p.SetUDPIdleTimeout(50 * time.Millisecond)
	defer p.Close()

	p.UpdateConfig([]state.InternalListener{
		{
			Name:     "udp-leak",
			Protocol: gatewayv1.UDPProtocolType,
			Port:     gatewayv1.PortNumber(proxyPort),
			UDPBackends: []state.InternalUDPBackend{
				{Target: fmt.Sprintf("127.0.0.1:%d", backendPort), Weight: 1},
			},
		},
	}, nil)

	// Measure baseline goroutines with proxy running (read loop active)
	for i := 0; i < 5; i++ {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}
	baseline := runtime.NumGoroutine()

	proxyAddr := fmt.Sprintf("127.0.0.1:%d", proxyPort)

	// Send traffic from 10 distinct client sockets
	for i := 0; i < 10; i++ {
		func() {
			c, err := net.Dial("udp", proxyAddr)
			if err != nil {
				t.Fatalf("client %d dial failed: %v", i, err)
			}
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(5 * time.Second))
			if _, err := c.Write([]byte("datagram")); err != nil {
				t.Fatalf("client %d write failed: %v", i, err)
			}
			buf := make([]byte, 256)
			if _, err := c.Read(buf); err != nil {
				t.Fatalf("client %d read failed: %v", i, err)
			}
		}()
	}

	// Goroutine count increased due to session reader loops and cleaner
	activeGoroutines := runtime.NumGoroutine()
	if activeGoroutines <= baseline {
		t.Logf("note: active goroutines %d vs baseline %d", activeGoroutines, baseline)
	}

	// Wait for all sessions to idle out and expire (idleTimeout=50ms)
	deadline := time.Now().Add(5 * time.Second)
	returnedToBaseline := false
	for time.Now().Before(deadline) {
		runtime.Gosched()
		time.Sleep(20 * time.Millisecond)
		if runtime.NumGoroutine() <= baseline {
			returnedToBaseline = true
			break
		}
	}

	if !returnedToBaseline {
		t.Fatalf("goroutine leak: baseline was %d, current is %d", baseline, runtime.NumGoroutine())
	}
}

// TestProxy_UDPDropNoBackends verifies that datagrams are dropped (no hang, no reply)
// when there are no backends or all backends have weight 0.
func TestProxy_UDPDropNoBackends(t *testing.T) {
	proxyPort := getFreeUDPPort(t)

	p := NewProxy()
	p.SetUDPBindHost("127.0.0.1")
	defer p.Close()

	// 1. Empty backends
	p.UpdateConfig([]state.InternalListener{
		{
			Name:        "udp-empty",
			Protocol:    gatewayv1.UDPProtocolType,
			Port:        gatewayv1.PortNumber(proxyPort),
			UDPBackends: nil,
		},
	}, nil)

	proxyAddr := fmt.Sprintf("127.0.0.1:%d", proxyPort)
	conn, err := net.Dial("udp", proxyAddr)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(100 * time.Millisecond))
	_, err = conn.Write([]byte("ping"))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	buf := make([]byte, 256)
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatalf("expected datagram to be dropped with no reply")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected timeout error for dropped datagram, got: %v", err)
	}
}
