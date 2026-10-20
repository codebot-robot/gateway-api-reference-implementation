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

package frontend

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/netip"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/api"
	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"
)

// TestWireGuardNetstackTCPUnit verifies two in-process netstack devices over loopback UDP,
// with TCP echo through the tunnel, requiring no privileges.
func TestWireGuardNetstackTCPUnit(t *testing.T) {
	// Keys for device 1 (frontend) and device 2 (backend)
	k1, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generating key 1: %v", err)
	}
	k2, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generating key 2: %v", err)
	}

	ip1 := netip.MustParseAddr("fd00::1")
	ip2 := netip.MustParseAddr("fd00::2")

	tunDev1, net1, err := netstack.CreateNetTUN([]netip.Addr{ip1}, nil, api.DefaultWireGuardMTU)
	if err != nil {
		t.Fatalf("CreateNetTUN 1: %v", err)
	}

	tunDev2, net2, err := netstack.CreateNetTUN([]netip.Addr{ip2}, nil, api.DefaultWireGuardMTU)
	if err != nil {
		t.Fatalf("CreateNetTUN 2: %v", err)
	}

	dev1 := device.NewDevice(tunDev1, conn.NewDefaultBind(), device.NewLogger(device.LogLevelSilent, ""))
	defer dev1.Close()

	dev2 := device.NewDevice(tunDev2, conn.NewDefaultBind(), device.NewLogger(device.LogLevelSilent, ""))
	defer dev2.Close()

	if err := dev1.IpcSet(fmt.Sprintf("private_key=%s\nlisten_port=0\n", hex.EncodeToString(k1.Bytes()))); err != nil {
		t.Fatalf("dev1 IpcSet: %v", err)
	}
	if err := dev1.Up(); err != nil {
		t.Fatalf("dev1 Up: %v", err)
	}

	if err := dev2.IpcSet(fmt.Sprintf("private_key=%s\nlisten_port=0\n", hex.EncodeToString(k2.Bytes()))); err != nil {
		t.Fatalf("dev2 IpcSet: %v", err)
	}
	if err := dev2.Up(); err != nil {
		t.Fatalf("dev2 Up: %v", err)
	}

	port1, err := getDeviceListenPort(dev1)
	if err != nil {
		t.Fatalf("getting port 1: %v", err)
	}
	port2, err := getDeviceListenPort(dev2)
	if err != nil {
		t.Fatalf("getting port 2: %v", err)
	}

	// Device 1 peer: Device 2
	peerCfg1 := fmt.Sprintf("public_key=%s\nendpoint=127.0.0.1:%d\nallowed_ip=%s/128\n",
		hex.EncodeToString(k2.PublicKey().Bytes()), port2, ip2.String())
	if err := dev1.IpcSet(peerCfg1); err != nil {
		t.Fatalf("dev1 add peer: %v", err)
	}

	// Device 2 peer: Device 1 with keepalive
	peerCfg2 := fmt.Sprintf("public_key=%s\nendpoint=127.0.0.1:%d\nallowed_ip=%s/128\npersistent_keepalive_interval=25\n",
		hex.EncodeToString(k1.PublicKey().Bytes()), port1, ip1.String())
	if err := dev2.IpcSet(peerCfg2); err != nil {
		t.Fatalf("dev2 add peer: %v", err)
	}

	// Listen TCP on dev2 netstack
	tcpAddr := &net.TCPAddr{IP: net.ParseIP(ip2.String()), Port: api.DefaultWireGuardTunnelPort}
	lis, err := net2.ListenTCP(tcpAddr)
	if err != nil {
		t.Fatalf("dev2 ListenTCP: %v", err)
	}
	defer lis.Close()

	// Server goroutine: echo
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}(c)
		}
	}()

	// Wait for handshake initiated by persistent keepalive
	deadline := time.Now().Add(2 * time.Second)
	var handshakeDone bool
	for time.Now().Before(deadline) {
		out, err := dev2.IpcGet()
		if err == nil {
			for _, line := range strings.Split(out, "\n") {
				if strings.HasPrefix(line, "last_handshake_time_sec=") {
					var sec int64
					fmt.Sscanf(line, "last_handshake_time_sec=%d", &sec)
					if sec > 0 {
						handshakeDone = true
						break
					}
				}
			}
		}
		if handshakeDone {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !handshakeDone {
		t.Fatal("handshake did not complete in time")
	}

	// Dial from dev1 netstack to dev2
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	conn, err := net1.DialContextTCP(ctx, tcpAddr)
	if err != nil {
		t.Fatalf("net1 DialContextTCP failed: %v", err)
	}
	defer conn.Close()

	msg := []byte("wireguard echo test message")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if string(buf) != string(msg) {
		t.Fatalf("got %q, want %q", string(buf), string(msg))
	}
}

// TestWireGuardTransport_PeerLifecycle verifies peer addition and removal on the frontend WireGuard device.
func TestWireGuardTransport_PeerLifecycle(t *testing.T) {
	tr, err := NewWireGuardTransport(WireGuardTransportConfig{
		ListenAddr: "127.0.0.1:0",
		Prefix:     "fd00::/64",
	})
	if err != nil {
		t.Fatalf("NewWireGuardTransport failed: %v", err)
	}
	defer tr.Close()

	clientPriv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generating client key: %v", err)
	}
	var clientPub [32]byte
	copy(clientPub[:], clientPriv.PublicKey().Bytes())
	peerAddr := netip.MustParseAddr("fd00::10")

	// 1. Register peer
	sessionID := "test-sess-123"
	if err := tr.RegisterPeer(sessionID, clientPub, peerAddr); err != nil {
		t.Fatalf("RegisterPeer failed: %v", err)
	}

	if !tr.HasSession(sessionID) {
		t.Fatalf("expected session %s to be registered", sessionID)
	}

	// Verify peer is present in device UAPI output
	out, err := tr.Device().IpcGet()
	if err != nil {
		t.Fatalf("IpcGet failed: %v", err)
	}
	clientPubHex := hex.EncodeToString(clientPub[:])
	if !strings.Contains(out, clientPubHex) {
		t.Fatalf("expected device to contain peer %s, got:\n%s", clientPubHex, out)
	}
	if !strings.Contains(out, "allowed_ip=fd00::10/128") {
		t.Fatalf("expected device to contain allowed_ip=fd00::10/128, got:\n%s", out)
	}

	// 2. Unregister session
	tr.UnregisterSession(sessionID)
	if tr.HasSession(sessionID) {
		t.Fatalf("expected session %s to be unregistered", sessionID)
	}

	// Verify peer is removed from device UAPI output
	outAfter, err := tr.Device().IpcGet()
	if err != nil {
		t.Fatalf("IpcGet after unregister failed: %v", err)
	}
	if strings.Contains(outAfter, clientPubHex) {
		t.Fatalf("expected device to no longer contain peer %s, got:\n%s", clientPubHex, outAfter)
	}

	// 3. Reject client key that equals frontend public key
	fePub := tr.PublicKey()
	if err := tr.RegisterPeer("sess-self", fePub, netip.MustParseAddr("fd00::20")); err == nil {
		t.Fatal("expected error when registering frontend's own public key, got nil")
	}

	// 4. Reject client key that is already bound to another session
	if err := tr.RegisterPeer("sess-1", clientPub, netip.MustParseAddr("fd00::30")); err != nil {
		t.Fatalf("registering sess-1: %v", err)
	}
	if err := tr.RegisterPeer("sess-2", clientPub, netip.MustParseAddr("fd00::40")); err == nil {
		t.Fatal("expected error when registering duplicate public key to different session, got nil")
	}

	// 5. Updating address on same session replaces allowed_ips (ensures exactly one /128)
	if err := tr.RegisterPeer("sess-1", clientPub, netip.MustParseAddr("fd00::31")); err != nil {
		t.Fatalf("updating sess-1 address: %v", err)
	}
	outUpdate, err := tr.Device().IpcGet()
	if err != nil {
		t.Fatalf("IpcGet after update failed: %v", err)
	}
	if strings.Contains(outUpdate, "allowed_ip=fd00::30/128") {
		t.Fatalf("expected old allowed_ip fd00::30/128 to be replaced, got:\n%s", outUpdate)
	}
	if !strings.Contains(outUpdate, "allowed_ip=fd00::31/128") {
		t.Fatalf("expected new allowed_ip fd00::31/128, got:\n%s", outUpdate)
	}

	// 6. TryGetConn skips peer whose handshake is zero without blocking
	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()
	conn, err := tr.TryGetConn(ctx, "sess-1")
	if err != nil {
		t.Fatalf("expected nil error for unestablished peer, got: %v", err)
	}
	if conn != nil {
		t.Fatal("expected nil conn for unestablished peer")
	}
}

// TestWireGuardTransport_TryGetConn_HandshakeLifecycle verifies that TryGetConn correctly skips
// unestablished peers, connects to peers with a fresh handshake, and skips peers with a handshake older than 180s.
func TestWireGuardTransport_TryGetConn_HandshakeLifecycle(t *testing.T) {
	// Fake clock
	var nowUnixNano atomic.Int64
	baseTime := time.Unix(1700000000, 0)
	nowUnixNano.Store(baseTime.UnixNano())
	nowFunc := func() time.Time {
		return time.Unix(0, nowUnixNano.Load())
	}
	advance := func(d time.Duration) {
		nowUnixNano.Add(d.Nanoseconds())
	}

	// 1. Setup frontend WireGuard transport with fake clock and short interval (20ms)
	tr, err := NewWireGuardTransport(WireGuardTransportConfig{
		ListenAddr:            "127.0.0.1:0",
		Prefix:                "fd00::/64",
		HandshakeSyncInterval: 20 * time.Millisecond,
		PerAttemptTimeout:     5 * time.Second,
		Now:                   nowFunc,
	})
	if err != nil {
		t.Fatalf("NewWireGuardTransport failed: %v", err)
	}
	defer tr.Close()

	// 2. Setup backend netstack device with loopback UDP
	backendPriv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generating backend key: %v", err)
	}
	var backendPub [32]byte
	copy(backendPub[:], backendPriv.PublicKey().Bytes())
	backendIP := netip.MustParseAddr("fd00::2")

	backendTun, backendNet, err := netstack.CreateNetTUN([]netip.Addr{backendIP}, nil, api.DefaultWireGuardMTU)
	if err != nil {
		t.Fatalf("backend CreateNetTUN: %v", err)
	}
	backendDev := device.NewDevice(backendTun, conn.NewDefaultBind(), device.NewLogger(device.LogLevelSilent, ""))
	defer backendDev.Close()

	if err := backendDev.IpcSet(fmt.Sprintf("private_key=%s\nlisten_port=0\n", hex.EncodeToString(backendPriv.Bytes()))); err != nil {
		t.Fatalf("backendDev IpcSet: %v", err)
	}
	if err := backendDev.Up(); err != nil {
		t.Fatalf("backendDev Up: %v", err)
	}

	// Listen TCP on backend netstack
	tcpAddr := &net.TCPAddr{IP: net.ParseIP(backendIP.String()), Port: api.DefaultWireGuardTunnelPort}
	lis, err := backendNet.ListenTCP(tcpAddr)
	if err != nil {
		t.Fatalf("backend ListenTCP: %v", err)
	}
	defer lis.Close()

	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}(c)
		}
	}()

	// Register peer on frontend
	sessionID := "test-lifecycle-sess"
	if err := tr.RegisterPeer(sessionID, backendPub, backendIP); err != nil {
		t.Fatalf("RegisterPeer failed: %v", err)
	}

	// Check 1: No handshake yet -> skipped
	ctx1, cancel1 := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel1()

	conn1, err := tr.TryGetConn(ctx1, sessionID)
	if err != nil {
		t.Fatalf("TryGetConn error: %v", err)
	}
	if conn1 != nil {
		conn1.Close()
		t.Fatal("expected nil conn when no handshake completed yet")
	}

	// Configure backend peer pointing to frontend UDP port with persistent keepalive to initiate handshake
	fePub := tr.PublicKey()
	peerCfg := fmt.Sprintf("public_key=%s\nendpoint=127.0.0.1:%d\nallowed_ip=fd00::1/128\npersistent_keepalive_interval=25\n",
		hex.EncodeToString(fePub[:]), tr.ActualPort())
	if err := backendDev.IpcSet(peerCfg); err != nil {
		t.Fatalf("backend add frontend peer: %v", err)
	}

	// Wait for handshake to complete and be picked up by background sync loop
	deadline := time.Now().Add(3 * time.Second)
	var hsCached time.Time
	for time.Now().Before(deadline) {
		hsCached = tr.PeerLastHandshake(sessionID)
		if !hsCached.IsZero() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if hsCached.IsZero() {
		t.Fatal("handshake was not cached within deadline")
	}

	// Align fake clock with real handshake time (fresh handshake)
	nowUnixNano.Store(hsCached.Add(5 * time.Second).UnixNano())

	// Check 2: Fresh handshake -> dialled
	ctx2, cancel2 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel2()

	conn2, err := tr.TryGetConn(ctx2, sessionID)
	if err != nil {
		t.Fatalf("TryGetConn failed for fresh handshake: %v", err)
	}
	if conn2 == nil {
		t.Fatal("expected non-nil conn for fresh handshake, got nil")
	}
	// Verify connection is functional via echo
	echoMsg := []byte("hello-wireguard-fresh")
	if _, err := conn2.Write(echoMsg); err != nil {
		conn2.Close()
		t.Fatalf("echo write failed: %v", err)
	}
	echoBuf := make([]byte, len(echoMsg))
	if _, err := io.ReadFull(conn2, echoBuf); err != nil {
		conn2.Close()
		t.Fatalf("echo read failed: %v", err)
	}
	if string(echoBuf) != string(echoMsg) {
		conn2.Close()
		t.Fatalf("echo mismatch: got %q, want %q", string(echoBuf), string(echoMsg))
	}
	conn2.Close()

	// Check 3: Handshake older than 180s -> skipped
	advance(181 * time.Second) // now is hsCached + 5s + 181s = hsCached + 186s > 180s
	ctx3, cancel3 := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel3()

	conn3, err := tr.TryGetConn(ctx3, sessionID)
	if err != nil {
		t.Fatalf("TryGetConn error on expired handshake: %v", err)
	}
	if conn3 != nil {
		conn3.Close()
		t.Fatal("expected nil conn for handshake older than 180s, got active connection")
	}
}

// TestWireGuardTransport_GetConnForSessions_WokenOnUsableHandshake verifies that a connection waiter
// waiting for an unestablished session is promptly woken up when the session's handshake completes.
func TestWireGuardTransport_GetConnForSessions_WokenOnUsableHandshake(t *testing.T) {
	tr, err := NewWireGuardTransport(WireGuardTransportConfig{
		ListenAddr:            "127.0.0.1:0",
		Prefix:                "fd00::/64",
		HandshakeSyncInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewWireGuardTransport failed: %v", err)
	}
	defer tr.Close()

	// Setup backend netstack device with loopback UDP
	backendPriv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generating backend key: %v", err)
	}
	var backendPub [32]byte
	copy(backendPub[:], backendPriv.PublicKey().Bytes())
	backendIP := netip.MustParseAddr("fd00::2")

	backendTun, backendNet, err := netstack.CreateNetTUN([]netip.Addr{backendIP}, nil, api.DefaultWireGuardMTU)
	if err != nil {
		t.Fatalf("backend CreateNetTUN: %v", err)
	}
	backendDev := device.NewDevice(backendTun, conn.NewDefaultBind(), device.NewLogger(device.LogLevelSilent, ""))
	defer backendDev.Close()

	if err := backendDev.IpcSet(fmt.Sprintf("private_key=%s\nlisten_port=0\n", hex.EncodeToString(backendPriv.Bytes()))); err != nil {
		t.Fatalf("backendDev IpcSet: %v", err)
	}
	if err := backendDev.Up(); err != nil {
		t.Fatalf("backendDev Up: %v", err)
	}

	// Listen TCP on backend netstack
	tcpAddr := &net.TCPAddr{IP: net.ParseIP(backendIP.String()), Port: api.DefaultWireGuardTunnelPort}
	lis, err := backendNet.ListenTCP(tcpAddr)
	if err != nil {
		t.Fatalf("backend ListenTCP: %v", err)
	}
	defer lis.Close()

	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}(c)
		}
	}()

	// Register peer on frontend (no handshake yet)
	sessionID := "test-wake-sess"
	if err := tr.RegisterPeer(sessionID, backendPub, backendIP); err != nil {
		t.Fatalf("RegisterPeer failed: %v", err)
	}

	// Start GetConnForSessions waiter in background with an 8s context
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()

	type result struct {
		conn net.Conn
		sid  string
		err  error
	}
	resCh := make(chan result, 1)
	go func() {
		c, sid, err := tr.GetConnForSessions(ctx, []string{sessionID})
		resCh <- result{conn: c, sid: sid, err: err}
	}()

	// Verify waiter is waiting (not immediately returning)
	time.Sleep(50 * time.Millisecond)
	select {
	case res := <-resCh:
		if res.conn != nil {
			res.conn.Close()
		}
		t.Fatalf("expected GetConnForSessions to block while handshake is unestablished, got: conn=%v, err=%v", res.conn, res.err)
	default:
	}

	// Now initiate handshake on backend
	fePub := tr.PublicKey()
	peerCfg := fmt.Sprintf("public_key=%s\nendpoint=127.0.0.1:%d\nallowed_ip=fd00::1/128\npersistent_keepalive_interval=25\n",
		hex.EncodeToString(fePub[:]), tr.ActualPort())
	if err := backendDev.IpcSet(peerCfg); err != nil {
		t.Fatalf("backend add frontend peer: %v", err)
	}

	// The waiter should wake up promptly (within 3s) once handshake completes
	select {
	case res := <-resCh:
		if res.err != nil {
			t.Fatalf("GetConnForSessions failed: %v", res.err)
		}
		if res.conn == nil {
			t.Fatal("expected non-nil conn")
		}
		defer res.conn.Close()
		if res.sid != sessionID {
			t.Fatalf("got session %q, want %q", res.sid, sessionID)
		}

		// Verify connection is working
		msg := []byte("handshake-wakeup-ok")
		if _, err := res.conn.Write(msg); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		buf := make([]byte, len(msg))
		if _, err := io.ReadFull(res.conn, buf); err != nil {
			t.Fatalf("read failed: %v", err)
		}
		if string(buf) != string(msg) {
			t.Fatalf("got %q, want %q", string(buf), string(msg))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("GetConnForSessions was not woken up promptly after handshake completed")
	}
}

// TestCompositeTransport_GetConnForSessions_DialRetryOnUsablePeer verifies that when dialing a usable
// peer fails (e.g. backend listener is not yet accepting), CompositeTransport retries after backoff
// rather than parking indefinitely on notification channels.
func TestCompositeTransport_GetConnForSessions_DialRetryOnUsablePeer(t *testing.T) {
	tr, err := NewWireGuardTransport(WireGuardTransportConfig{
		ListenAddr:            "127.0.0.1:0",
		Prefix:                "fd00::/64",
		HandshakeSyncInterval: 20 * time.Millisecond,
		PerAttemptTimeout:     50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewWireGuardTransport failed: %v", err)
	}
	defer tr.Close()

	// Setup backend netstack device with loopback UDP
	backendPriv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generating backend key: %v", err)
	}
	var backendPub [32]byte
	copy(backendPub[:], backendPriv.PublicKey().Bytes())
	backendIP := netip.MustParseAddr("fd00::2")

	backendTun, backendNet, err := netstack.CreateNetTUN([]netip.Addr{backendIP}, nil, api.DefaultWireGuardMTU)
	if err != nil {
		t.Fatalf("backend CreateNetTUN: %v", err)
	}
	backendDev := device.NewDevice(backendTun, conn.NewDefaultBind(), device.NewLogger(device.LogLevelSilent, ""))
	defer backendDev.Close()

	if err := backendDev.IpcSet(fmt.Sprintf("private_key=%s\nlisten_port=0\n", hex.EncodeToString(backendPriv.Bytes()))); err != nil {
		t.Fatalf("backendDev IpcSet: %v", err)
	}
	if err := backendDev.Up(); err != nil {
		t.Fatalf("backendDev Up: %v", err)
	}

	// Register peer on frontend
	sessionID := "test-dial-retry-sess"
	if err := tr.RegisterPeer(sessionID, backendPub, backendIP); err != nil {
		t.Fatalf("RegisterPeer failed: %v", err)
	}

	// Initiate handshake from backend to frontend
	fePub := tr.PublicKey()
	peerCfg := fmt.Sprintf("public_key=%s\nendpoint=127.0.0.1:%d\nallowed_ip=fd00::1/128\npersistent_keepalive_interval=25\n",
		hex.EncodeToString(fePub[:]), tr.ActualPort())
	if err := backendDev.IpcSet(peerCfg); err != nil {
		t.Fatalf("backend add frontend peer: %v", err)
	}

	// Wait for handshake to complete and be cached on frontend
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !tr.PeerLastHandshake(sessionID).IsZero() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if tr.PeerLastHandshake(sessionID).IsZero() {
		t.Fatal("handshake was not cached within deadline")
	}

	// Note: backend TCP listener is deliberately NOT started yet, so initial dial will fail.
	comp := NewCompositeTransport(nil, nil, nil)
	comp.SetWireGuard(tr)
	defer comp.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	type result struct {
		conn net.Conn
		sid  string
		err  error
	}
	resCh := make(chan result, 1)
	go func() {
		c, sid, err := comp.GetConnForSessions(ctx, []string{sessionID})
		resCh <- result{conn: c, sid: sid, err: err}
	}()

	// Wait a moment so the first dial attempt fails on the unopened port
	time.Sleep(100 * time.Millisecond)

	// Now start the backend TCP listener so subsequent retries can succeed
	tcpAddr := &net.TCPAddr{IP: net.ParseIP(backendIP.String()), Port: api.DefaultWireGuardTunnelPort}
	lis, err := backendNet.ListenTCP(tcpAddr)
	if err != nil {
		t.Fatalf("backend ListenTCP: %v", err)
	}
	defer lis.Close()

	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}(c)
		}
	}()

	// The waiter should retry after backoff and be served promptly (well before context deadline)
	select {
	case res := <-resCh:
		if res.err != nil {
			t.Fatalf("GetConnForSessions returned error: %v", res.err)
		}
		if res.conn == nil {
			t.Fatal("expected non-nil connection")
		}
		defer res.conn.Close()
		if res.sid != sessionID {
			t.Fatalf("got session %q, want %q", res.sid, sessionID)
		}

		// Verify connection works via echo
		msg := []byte("retry-success")
		if _, err := res.conn.Write(msg); err != nil {
			t.Fatalf("echo write failed: %v", err)
		}
		buf := make([]byte, len(msg))
		if _, err := io.ReadFull(res.conn, buf); err != nil {
			t.Fatalf("echo read failed: %v", err)
		}
		if string(buf) != string(msg) {
			t.Fatalf("got %q, want %q", string(buf), string(msg))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GetConnForSessions parked and did not retry failed dial to usable peer")
	}
}

// TestWireGuardTransport_PeerScaleIndependence verifies that TryGetConn latency is independent
// of the number of registered peers (e.g. 1 vs 1000 peers).
func TestWireGuardTransport_PeerScaleIndependence(t *testing.T) {
	tr, err := NewWireGuardTransport(WireGuardTransportConfig{
		ListenAddr: "127.0.0.1:0",
		Prefix:     "fd00::/64",
	})
	if err != nil {
		t.Fatalf("NewWireGuardTransport failed: %v", err)
	}
	defer tr.Close()

	// Register 1st peer
	var pk1 [32]byte
	binary.BigEndian.PutUint32(pk1[:], 1)
	if err := tr.RegisterPeer("sess-0", pk1, netip.MustParseAddr("fd00::100")); err != nil {
		t.Fatalf("RegisterPeer 0 failed: %v", err)
	}

	ctx := t.Context()

	// Warm up
	_, _ = tr.TryGetConn(ctx, "sess-0")

	// Measure cost for 1 peer over 10,000 iterations
	const iters = 10000
	start1 := time.Now()
	for i := 0; i < iters; i++ {
		_, _ = tr.TryGetConn(ctx, "sess-0")
	}
	dur1 := time.Since(start1)

	// Now register up to 1000 peers (or 100 if -short)
	numPeers := 1000
	if testing.Short() {
		numPeers = 100
	}
	for i := 1; i < numPeers; i++ {
		var pk [32]byte
		binary.BigEndian.PutUint32(pk[:], uint32(i+1))
		sid := fmt.Sprintf("sess-%d", i)
		addr := netip.MustParseAddr(fmt.Sprintf("fd00::%x", 0x100+i))
		if err := tr.RegisterPeer(sid, pk, addr); err != nil {
			t.Fatalf("RegisterPeer %d failed: %v", i, err)
		}
	}

	// Measure cost with registered peers over same iterations
	startScale := time.Now()
	for i := 0; i < iters; i++ {
		_, _ = tr.TryGetConn(ctx, "sess-0")
	}
	durScale := time.Since(startScale)

	t.Logf("1 peer: %v for %d calls (%v/call), %d peers: %v for %d calls (%v/call)",
		dur1, iters, dur1/iters, numPeers, durScale, iters, durScale/iters)

	// In the old implementation calling IpcGet, 1000 peers would be orders of magnitude slower
	// (each call serializing 1000 peers taking ~1ms, so 10,000 calls would take ~10s).
	// With cached timestamps, 10,000 calls takes only a few milliseconds in both cases.
	// We assert durScale per call is under 50 microseconds.
	if perCall := durScale / iters; perCall > 50*time.Microsecond {
		t.Fatalf("TryGetConn too slow with %d peers: %v per call (expected < 50µs)", numPeers, perCall)
	}
}

// BenchmarkWireGuardTransport_TryGetConn benchmarks TryGetConn with 1 vs 1000 peers.
func BenchmarkWireGuardTransport_TryGetConn(b *testing.B) {
	for _, numPeers := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("%d_peers", numPeers), func(b *testing.B) {
			tr, err := NewWireGuardTransport(WireGuardTransportConfig{
				ListenAddr: "127.0.0.1:0",
				Prefix:     "fd00::/64",
			})
			if err != nil {
				b.Fatalf("NewWireGuardTransport failed: %v", err)
			}
			defer tr.Close()

			for i := 0; i < numPeers; i++ {
				var pk [32]byte
				binary.BigEndian.PutUint32(pk[:], uint32(i+1))
				sid := fmt.Sprintf("sess-%d", i)
				addr := netip.MustParseAddr(fmt.Sprintf("fd00::%x", 0x100+i))
				if err := tr.RegisterPeer(sid, pk, addr); err != nil {
					b.Fatalf("RegisterPeer %d failed: %v", i, err)
				}
			}

			ctx := b.Context()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = tr.TryGetConn(ctx, "sess-0")
			}
		})
	}
}

// TestWireGuardTransport_CloseStopsBackgroundLoop verifies that tr.Close stops the background syncLoop
// and does not leak goroutines.
func TestWireGuardTransport_CloseStopsBackgroundLoop(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	tr, err := NewWireGuardTransport(WireGuardTransportConfig{
		ListenAddr:            "127.0.0.1:0",
		Prefix:                "fd00::/64",
		HandshakeSyncInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewWireGuardTransport failed: %v", err)
	}

	var pk [32]byte
	pk[0] = 42
	if err := tr.RegisterPeer("test-sess", pk, netip.MustParseAddr("fd00::42")); err != nil {
		t.Fatalf("RegisterPeer: %v", err)
	}

	// Close must stop the background loop
	if err := tr.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify doneCh is closed
	select {
	case <-tr.doneCh:
		// Background syncLoop stopped cleanly
	case <-time.After(1 * time.Second):
		t.Fatal("background sync loop did not stop within 1 second")
	}

	// Check goroutine count returns to baseline (allow small window for netstack goroutines if any)
	deadline := time.Now().Add(1 * time.Second)
	var finalGoroutines int
	for time.Now().Before(deadline) {
		runtime.GC()
		finalGoroutines = runtime.NumGoroutine()
		if finalGoroutines <= initialGoroutines+1 { // +1 tolerance for test runner background
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if finalGoroutines > initialGoroutines+2 {
		t.Fatalf("goroutine leak: started with %d, ended with %d", initialGoroutines, finalGoroutines)
	}
}

func TestParseIpcHandshakes(t *testing.T) {
	var k1, k2 [32]byte
	k1[0] = 0xaa
	k2[0] = 0xbb

	uapiOutput := fmt.Sprintf(`private_key=0000000000000000000000000000000000000000000000000000000000000001
listen_port=51820
public_key=%s
endpoint=192.0.2.1:51820
allowed_ip=fd00::10/128
last_handshake_time_sec=1700000000
last_handshake_time_nsec=500000
rx_bytes=100
tx_bytes=200
public_key=%s
allowed_ip=fd00::20/128
last_handshake_time_sec=0
last_handshake_time_nsec=0
`, hex.EncodeToString(k1[:]), hex.EncodeToString(k2[:]))

	parsed := parseIpcHandshakes(uapiOutput)
	if len(parsed) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(parsed))
	}

	t1, ok1 := parsed[k1]
	if !ok1 {
		t.Fatalf("expected key 1 to be parsed")
	}
	want1 := time.Unix(1700000000, 500000)
	if !t1.Equal(want1) {
		t.Fatalf("key 1 handshake: got %v, want %v", t1, want1)
	}

	t2, ok2 := parsed[k2]
	if !ok2 {
		t.Fatalf("expected key 2 to be parsed")
	}
	if !t2.IsZero() {
		t.Fatalf("key 2 handshake: expected zero time, got %v", t2)
	}
}
