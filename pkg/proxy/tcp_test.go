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
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// getFreePort returns a free TCP port on 127.0.0.1.
func getFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on free port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// TestProxy_TCPBackendSpeaksFirst tests that a TCP listener immediately splices to a backend
// that writes first without waiting for any client data.
func TestProxy_TCPBackendSpeaksFirst(t *testing.T) {
	backendLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on backend: %v", err)
	}
	defer backendLn.Close()

	welcomeMsg := "WELCOME TO TCP ECHO\n"

	// Backend writes welcome line first, then echoes whatever client sends.
	go func() {
		for {
			conn, err := backendLn.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				if _, err := io.WriteString(c, welcomeMsg); err != nil {
					return
				}
				reader := bufio.NewReader(c)
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					if _, err := io.WriteString(c, line); err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	backendPort := backendLn.Addr().(*net.TCPAddr).Port
	proxyPort := getFreePort(t)

	p := NewProxy()
	p.SetTCPBindHost("127.0.0.1")
	defer p.Close()

	p.UpdateConfig([]state.InternalListener{
		{
			Name:     "tcp-listener",
			Protocol: gatewayv1.TCPProtocolType,
			Port:     gatewayv1.PortNumber(proxyPort),
			TCPBackends: []state.InternalTCPBackend{
				{Target: fmt.Sprintf("127.0.0.1:%d", backendPort), Weight: 1},
			},
		},
	}, nil)

	// Dial the proxy listener
	proxyAddr := fmt.Sprintf("127.0.0.1:%d", proxyPort)
	clientConn, err := net.DialTimeout("tcp", proxyAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial proxy: %v", err)
	}
	defer clientConn.Close()

	// Read welcome line from backend BEFORE sending anything
	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	clientReader := bufio.NewReader(clientConn)
	line, err := clientReader.ReadString('\n')
	if err != nil {
		t.Fatalf("expected backend welcome line, got error: %v", err)
	}
	if line != welcomeMsg {
		t.Fatalf("expected welcome line %q, got %q", welcomeMsg, line)
	}

	// Now send client data and verify echo
	if _, err := io.WriteString(clientConn, "PING\n"); err != nil {
		t.Fatalf("failed to write PING: %v", err)
	}
	resp, err := clientReader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	if resp != "PING\n" {
		t.Fatalf("expected PING\\n, got %q", resp)
	}
}

// TestProxy_TCPConfigUpdateKeepsEstablishedConnections tests that updating config to remove one
// port and keep another leaves connections on the kept port intact.
func TestProxy_TCPConfigUpdateKeepsEstablishedConnections(t *testing.T) {
	startEchoBackend := func() (net.Listener, int) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to listen: %v", err)
		}
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				go func(c net.Conn) {
					defer c.Close()
					r := bufio.NewReader(c)
					for {
						line, err := r.ReadString('\n')
						if err != nil {
							return
						}
						if _, err := io.WriteString(c, line); err != nil {
							return
						}
					}
				}(conn)
			}
		}()
		return ln, ln.Addr().(*net.TCPAddr).Port
	}

	b1Ln, b1Port := startEchoBackend()
	defer b1Ln.Close()
	b2Ln, b2Port := startEchoBackend()
	defer b2Ln.Close()

	portA := getFreePort(t)
	portB := getFreePort(t)

	p := NewProxy()
	p.SetTCPBindHost("127.0.0.1")
	defer p.Close()

	// Configure both port A and port B
	p.UpdateConfig([]state.InternalListener{
		{
			Name:     "tcp-a",
			Protocol: gatewayv1.TCPProtocolType,
			Port:     gatewayv1.PortNumber(portA),
			TCPBackends: []state.InternalTCPBackend{
				{Target: fmt.Sprintf("127.0.0.1:%d", b1Port), Weight: 1},
			},
		},
		{
			Name:     "tcp-b",
			Protocol: gatewayv1.TCPProtocolType,
			Port:     gatewayv1.PortNumber(portB),
			TCPBackends: []state.InternalTCPBackend{
				{Target: fmt.Sprintf("127.0.0.1:%d", b2Port), Weight: 1},
			},
		},
	}, nil)

	// Establish connection on port A
	connA, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", portA), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial port A: %v", err)
	}
	defer connA.Close()

	// Verify connA works
	readerA := bufio.NewReader(connA)
	if _, err := io.WriteString(connA, "HELLO_A\n"); err != nil {
		t.Fatalf("failed to write to connA: %v", err)
	}
	lineA, err := readerA.ReadString('\n')
	if err != nil || lineA != "HELLO_A\n" {
		t.Fatalf("connA read failed: got %q, err: %v", lineA, err)
	}

	// Establish connection on port B
	connB, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", portB), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial port B: %v", err)
	}
	defer connB.Close()

	readerB := bufio.NewReader(connB)
	if _, err := io.WriteString(connB, "HELLO_B\n"); err != nil {
		t.Fatalf("failed to write to connB: %v", err)
	}
	lineB, err := readerB.ReadString('\n')
	if err != nil || lineB != "HELLO_B\n" {
		t.Fatalf("connB read failed: got %q, err: %v", lineB, err)
	}

	// Config update: remove port B, keep port A
	p.UpdateConfig([]state.InternalListener{
		{
			Name:     "tcp-a",
			Protocol: gatewayv1.TCPProtocolType,
			Port:     gatewayv1.PortNumber(portA),
			TCPBackends: []state.InternalTCPBackend{
				{Target: fmt.Sprintf("127.0.0.1:%d", b1Port), Weight: 1},
			},
		},
	}, nil)

	// Port B listening socket should be closed: new dials to port B must fail
	time.Sleep(50 * time.Millisecond)
	newConnB, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", portB), 200*time.Millisecond)
	if err == nil {
		newConnB.Close()
		t.Fatalf("expected dial to removed port B to fail, but succeeded")
	}

	// Established connection on kept port A must remain intact and functional!
	_ = connA.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.WriteString(connA, "STILL_ALIVE_A\n"); err != nil {
		t.Fatalf("failed to write to connA after config update: %v", err)
	}
	lineA2, err := readerA.ReadString('\n')
	if err != nil || lineA2 != "STILL_ALIVE_A\n" {
		t.Fatalf("connA read after config update failed: got %q, err: %v", lineA2, err)
	}

	// New connections on kept port A must also work
	newConnA, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", portA), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial new connection on kept port A: %v", err)
	}
	defer newConnA.Close()
	newReaderA := bufio.NewReader(newConnA)
	if _, err := io.WriteString(newConnA, "NEW_A\n"); err != nil {
		t.Fatalf("failed to write to newConnA: %v", err)
	}
	newLineA, err := newReaderA.ReadString('\n')
	if err != nil || newLineA != "NEW_A\n" {
		t.Fatalf("newConnA read failed: got %q, err: %v", newLineA, err)
	}
}

// TestProxy_TCPWeightedDistribution tests weighted distribution with weights 1/1/0
// over a few hundred connections.
func TestProxy_TCPWeightedDistribution(t *testing.T) {
	startIDBackend := func(id string) (net.Listener, int) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to listen: %v", err)
		}
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				go func(c net.Conn) {
					defer c.Close()
					_, _ = io.WriteString(c, id+"\n")
				}(conn)
			}
		}()
		return ln, ln.Addr().(*net.TCPAddr).Port
	}

	b1Ln, b1Port := startIDBackend("backend1")
	defer b1Ln.Close()
	b2Ln, b2Port := startIDBackend("backend2")
	defer b2Ln.Close()
	b3Ln, b3Port := startIDBackend("backend3")
	defer b3Ln.Close()

	proxyPort := getFreePort(t)

	p := NewProxy()
	p.SetTCPBindHost("127.0.0.1")
	defer p.Close()

	p.UpdateConfig([]state.InternalListener{
		{
			Name:     "tcp-weighted",
			Protocol: gatewayv1.TCPProtocolType,
			Port:     gatewayv1.PortNumber(proxyPort),
			TCPBackends: []state.InternalTCPBackend{
				{Target: fmt.Sprintf("127.0.0.1:%d", b1Port), Weight: 1},
				{Target: fmt.Sprintf("127.0.0.1:%d", b2Port), Weight: 1},
				{Target: fmt.Sprintf("127.0.0.1:%d", b3Port), Weight: 0},
			},
		},
	}, nil)

	counts := make(map[string]int)
	totalConns := 300

	for i := 0; i < totalConns; i++ {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second)
		if err != nil {
			t.Fatalf("conn %d failed to dial: %v", i, err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		line, err := bufio.NewReader(conn).ReadString('\n')
		_ = conn.Close()
		if err != nil {
			t.Fatalf("conn %d failed to read: %v", i, err)
		}
		counts[line[:len(line)-1]]++
	}

	// Backend 3 (weight 0) must NEVER be selected
	if counts["backend3"] != 0 {
		t.Errorf("backend3 with weight 0 received %d connections, want 0", counts["backend3"])
	}

	// Backends 1 and 2 should both receive traffic roughly equally
	if counts["backend1"] < 100 || counts["backend1"] > 200 {
		t.Errorf("backend1 count %d out of expected range [100, 200]", counts["backend1"])
	}
	if counts["backend2"] < 100 || counts["backend2"] > 200 {
		t.Errorf("backend2 count %d out of expected range [100, 200]", counts["backend2"])
	}
	if counts["backend1"]+counts["backend2"] != totalConns {
		t.Errorf("total conns %d != %d", counts["backend1"]+counts["backend2"], totalConns)
	}
}

// TestProxy_TCPRefuseNoBackends tests that a listener with no resolved backends
// (or all weight 0) refuses incoming connections immediately (closes, doesn't hang).
func TestProxy_TCPRefuseNoBackends(t *testing.T) {
	proxyPort := getFreePort(t)

	p := NewProxy()
	p.SetTCPBindHost("127.0.0.1")
	defer p.Close()

	// 1. Empty backends
	p.UpdateConfig([]state.InternalListener{
		{
			Name:        "tcp-empty",
			Protocol:    gatewayv1.TCPProtocolType,
			Port:        gatewayv1.PortNumber(proxyPort),
			TCPBackends: nil,
		},
	}, nil)

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 16)
	_, readErr := conn.Read(buf)
	if !errors.Is(readErr, io.EOF) {
		t.Fatalf("expected EOF (connection closed by server), got %v", readErr)
	}

	// 2. All weight 0 backends
	p.UpdateConfig([]state.InternalListener{
		{
			Name:     "tcp-zero-weight",
			Protocol: gatewayv1.TCPProtocolType,
			Port:     gatewayv1.PortNumber(proxyPort),
			TCPBackends: []state.InternalTCPBackend{
				{Target: "127.0.0.1:59999", Weight: 0},
			},
		},
	}, nil)

	conn2, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn2.Close()

	_ = conn2.SetReadDeadline(time.Now().Add(1 * time.Second))
	_, readErr2 := conn2.Read(buf)
	if !errors.Is(readErr2, io.EOF) {
		t.Fatalf("expected EOF for zero-weight backend, got %v", readErr2)
	}
}

// TestProxy_TCPListenerMaxConnections tests that the TCP listener cap refuses the N+1th connection
// and admits a new one after one closes.
func TestProxy_TCPListenerMaxConnections(t *testing.T) {
	backendLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on backend: %v", err)
	}
	defer backendLn.Close()

	// Simple echo backend
	go func() {
		for {
			conn, err := backendLn.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					if _, err := c.Write(buf[:n]); err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	backendPort := backendLn.Addr().(*net.TCPAddr).Port
	proxyPort := getFreePort(t)

	p := NewProxy()
	p.SetTCPBindHost("127.0.0.1")
	p.SetTCPMaxConnections(2)
	defer p.Close()

	p.UpdateConfig([]state.InternalListener{
		{
			Name:     "tcp-cap-test",
			Protocol: gatewayv1.TCPProtocolType,
			Port:     gatewayv1.PortNumber(proxyPort),
			TCPBackends: []state.InternalTCPBackend{
				{Target: fmt.Sprintf("127.0.0.1:%d", backendPort), Weight: 1},
			},
		},
	}, nil)

	// Conn 1 (admitted)
	conn1, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial conn1: %v", err)
	}
	defer conn1.Close()
	if _, err := conn1.Write([]byte("ping1\n")); err != nil {
		t.Fatalf("conn1 write failed: %v", err)
	}
	buf := make([]byte, 64)
	if _, err := conn1.Read(buf); err != nil {
		t.Fatalf("conn1 read failed: %v", err)
	}

	// Conn 2 (admitted)
	conn2, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial conn2: %v", err)
	}
	defer conn2.Close()
	if _, err := conn2.Write([]byte("ping2\n")); err != nil {
		t.Fatalf("conn2 write failed: %v", err)
	}
	if _, err := conn2.Read(buf); err != nil {
		t.Fatalf("conn2 read failed: %v", err)
	}

	// Conn 3 (N+1 = 3 > 2: refused by server and immediately closed)
	conn3, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial conn3: %v", err)
	}
	defer conn3.Close()
	_ = conn3.SetReadDeadline(time.Now().Add(1 * time.Second))
	_, readErr := conn3.Read(buf)
	if !errors.Is(readErr, io.EOF) && !errors.Is(readErr, net.ErrClosed) {
		t.Fatalf("expected conn3 to be refused and closed (EOF), got %v", readErr)
	}

	// Close Conn 1 to free up a slot
	conn1.Close()
	// Allow server goroutine to finish decrementing activeConns
	time.Sleep(30 * time.Millisecond)

	// Conn 4 (now admitted because active conns is 1 <= 2)
	conn4, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial conn4: %v", err)
	}
	defer conn4.Close()
	if _, err := conn4.Write([]byte("ping4\n")); err != nil {
		t.Fatalf("conn4 write failed: %v", err)
	}
	if _, err := conn4.Read(buf); err != nil {
		t.Fatalf("conn4 read failed: %v", err)
	}
}

// TestProxy_TCPIdleTimeout tests that idle TCP splice is closed after the idle timeout when enabled,
// and untouched when disabled.
func TestProxy_TCPIdleTimeout(t *testing.T) {
	backendLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on backend: %v", err)
	}
	defer backendLn.Close()

	// Simple echo backend
	go func() {
		for {
			conn, err := backendLn.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					if _, err := c.Write(buf[:n]); err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	backendPort := backendLn.Addr().(*net.TCPAddr).Port

	t.Run("idle timeout enabled closes idle connection", func(t *testing.T) {
		proxyPort := getFreePort(t)
		p := NewProxy()
		p.SetTCPBindHost("127.0.0.1")
		p.SetTCPIdleTimeout(100 * time.Millisecond)
		defer p.Close()

		p.UpdateConfig([]state.InternalListener{
			{
				Name:     "tcp-idle-enabled",
				Protocol: gatewayv1.TCPProtocolType,
				Port:     gatewayv1.PortNumber(proxyPort),
				TCPBackends: []state.InternalTCPBackend{
					{Target: fmt.Sprintf("127.0.0.1:%d", backendPort), Weight: 1},
				},
			},
		}, nil)

		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second)
		if err != nil {
			t.Fatalf("failed to dial: %v", err)
		}
		defer conn.Close()

		// Write initial message and verify connection is healthy
		if _, err := conn.Write([]byte("hello\n")); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		buf := make([]byte, 64)
		if _, err := conn.Read(buf); err != nil {
			t.Fatalf("read failed: %v", err)
		}

		// Now stay idle for idle timeout (100ms)
		start := time.Now()
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		for {
			_, rerr := conn.Read(buf)
			if rerr != nil {
				break
			}
		}
		elapsed := time.Since(start)

		if elapsed < 50*time.Millisecond || elapsed > 1500*time.Millisecond {
			t.Errorf("expected idle connection to be closed around 100ms, elapsed %v", elapsed)
		}
	})

	t.Run("idle timeout disabled keeps connection untouched", func(t *testing.T) {
		proxyPort := getFreePort(t)
		p := NewProxy()
		p.SetTCPBindHost("127.0.0.1")
		p.SetTCPIdleTimeout(0) // disabled
		defer p.Close()

		p.UpdateConfig([]state.InternalListener{
			{
				Name:     "tcp-idle-disabled",
				Protocol: gatewayv1.TCPProtocolType,
				Port:     gatewayv1.PortNumber(proxyPort),
				TCPBackends: []state.InternalTCPBackend{
					{Target: fmt.Sprintf("127.0.0.1:%d", backendPort), Weight: 1},
				},
			},
		}, nil)

		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second)
		if err != nil {
			t.Fatalf("failed to dial: %v", err)
		}
		defer conn.Close()

		// Wait 150ms while idle
		time.Sleep(150 * time.Millisecond)

		// Connection should remain untouched; write and read echo
		if _, err := conn.Write([]byte("still alive\n")); err != nil {
			t.Fatalf("expected connection to remain open, but write failed: %v", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		buf := make([]byte, 64)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("expected connection to remain open, but read failed: %v", err)
		}
		if string(buf[:n]) != "still alive\n" {
			t.Errorf("expected %q, got %q", "still alive\n", string(buf[:n]))
		}
	})
}
