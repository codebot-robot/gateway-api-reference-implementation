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
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strings"
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
