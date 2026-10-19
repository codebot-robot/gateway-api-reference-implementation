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
	"log"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"

	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/api"
	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun"
	"golang.zx2c4.com/wireguard/tun/netstack"
)

type wireguardPeer struct {
	sessionID string
	publicKey [32]byte
	peerAddr  netip.Addr
	tcpAddr   *net.TCPAddr
}

// WireGuardTransportConfig configures the frontend WireGuard transport device.
type WireGuardTransportConfig struct {
	ListenAddr string // UDP listen address, e.g. ":51820" or ":0"
	Prefix     string // IPv6 prefix, e.g. "fd00::/64"
	Endpoint   string // Optional advertised endpoint "<host:port>"
	TunnelPort int    // Target TCP port on controller netstack inside the tunnel
	MTU        int    // Tunnel MTU
	Logger     *device.Logger
}

// WireGuardTransport implements Transport forwarding client connections over WireGuard.
type WireGuardTransport struct {
	mu                 sync.RWMutex
	device             *device.Device
	netstackNet        *netstack.Net
	tunDev             tun.Device
	bind               conn.Bind
	privateKey         *ecdh.PrivateKey
	publicKey          [32]byte
	prefix             netip.Prefix
	frontendAddr       netip.Addr
	listenAddr         string
	actualPort         uint16
	advertisedEndpoint string
	tunnelPort         int
	mtu                int
	sessions           map[string]*wireguardPeer
	pubKeyToSession    map[[32]byte]string
	notifyCh           chan struct{}
	closed             bool
}

// NewWireGuardTransport creates and starts a new WireGuard transport on the frontend.
func NewWireGuardTransport(cfg WireGuardTransportConfig) (*WireGuardTransport, error) {
	if cfg.Prefix == "" {
		cfg.Prefix = api.DefaultWireGuardPrefix
	}
	prefix, err := netip.ParsePrefix(cfg.Prefix)
	if err != nil {
		return nil, fmt.Errorf("parsing wireguard prefix %q: %w", cfg.Prefix, err)
	}
	if !prefix.Addr().Is6() {
		return nil, fmt.Errorf("wireguard prefix %q must be IPv6", cfg.Prefix)
	}

	if cfg.TunnelPort <= 0 {
		cfg.TunnelPort = api.DefaultWireGuardTunnelPort
	}
	if cfg.MTU <= 0 {
		cfg.MTU = api.DefaultWireGuardMTU
	}

	// Frontend IP address is the first host address in prefix (e.g. fd00::1)
	baseBytes := prefix.Addr().As16()
	baseBytes[15] = 1
	frontendAddr := netip.AddrFrom16(baseBytes)

	tunDev, netstackNet, err := netstack.CreateNetTUN([]netip.Addr{frontendAddr}, nil, cfg.MTU)
	if err != nil {
		return nil, fmt.Errorf("creating netstack TUN device: %w", err)
	}

	logger := cfg.Logger
	if logger == nil {
		logger = device.NewLogger(device.LogLevelSilent, "")
	}

	privKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		if closeErr := tunDev.Close(); closeErr != nil {
			logger.Errorf("failed to close tun device: %v", closeErr)
		}
		return nil, fmt.Errorf("generating wireguard ephemeral key: %w", err)
	}
	pubKeyBytes := privKey.PublicKey().Bytes()
	var pubKey [32]byte
	copy(pubKey[:], pubKeyBytes)

	bind := conn.NewDefaultBind()
	dev := device.NewDevice(tunDev, bind, logger)

	listenPort := 0
	if cfg.ListenAddr != "" {
		pStr := cfg.ListenAddr
		if strings.Contains(pStr, ":") {
			_, portPart, err := net.SplitHostPort(cfg.ListenAddr)
			if err != nil {
				pStr = cfg.ListenAddr
			} else {
				pStr = portPart
			}
		}
		p, err := strconv.Atoi(pStr)
		if err != nil {
			listenPort = 0
		} else {
			listenPort = p
		}
	}

	uapiConf := fmt.Sprintf("private_key=%s\nlisten_port=%d\n",
		hex.EncodeToString(privKey.Bytes()), listenPort)
	if err := dev.IpcSet(uapiConf); err != nil {
		dev.Close()
		return nil, fmt.Errorf("setting initial wireguard device config: %w", err)
	}

	if err := dev.Up(); err != nil {
		dev.Close()
		return nil, fmt.Errorf("bringing wireguard device up: %w", err)
	}

	actualPort, err := getDeviceListenPort(dev)
	if err != nil {
		actualPort = uint16(listenPort)
	}

	tr := &WireGuardTransport{
		device:             dev,
		netstackNet:        netstackNet,
		tunDev:             tunDev,
		bind:               bind,
		privateKey:         privKey,
		publicKey:          pubKey,
		prefix:             prefix,
		frontendAddr:       frontendAddr,
		listenAddr:         cfg.ListenAddr,
		actualPort:         actualPort,
		advertisedEndpoint: cfg.Endpoint,
		tunnelPort:         cfg.TunnelPort,
		mtu:                cfg.MTU,
		sessions:           make(map[string]*wireguardPeer),
		pubKeyToSession:    make(map[[32]byte]string),
		notifyCh:           make(chan struct{}),
	}

	return tr, nil
}

func (w *WireGuardTransport) broadcastLocked() {
	close(w.notifyCh)
	w.notifyCh = make(chan struct{})
}

// PublicKey returns the frontend's 32-byte WireGuard public key.
func (w *WireGuardTransport) PublicKey() [32]byte {
	return w.publicKey
}

// FrontendAddress returns the frontend IPv6 address inside the tunnel.
func (w *WireGuardTransport) FrontendAddress() netip.Addr {
	return w.frontendAddr
}

// Prefix returns the configured IPv6 prefix.
func (w *WireGuardTransport) Prefix() netip.Prefix {
	return w.prefix
}

// ActualPort returns the UDP port wireguard is listening on.
func (w *WireGuardTransport) ActualPort() uint16 {
	return w.actualPort
}

// AdvertisedEndpoint returns the advertised endpoint, if set.
func (w *WireGuardTransport) AdvertisedEndpoint() string {
	return w.advertisedEndpoint
}

// MTU returns the configured MTU.
func (w *WireGuardTransport) MTU() int {
	return w.mtu
}

// TunnelPort returns the inner TCP port targeted on the peer netstack.
func (w *WireGuardTransport) TunnelPort() int {
	return w.tunnelPort
}

// Device returns the underlying wireguard-go device (for testing / inspection).
func (w *WireGuardTransport) Device() *device.Device {
	return w.device
}

// RegisterPeer adds or updates a WireGuard peer for sessionID with AllowedIPs exactly peerAddr/128.
func (w *WireGuardTransport) RegisterPeer(sessionID string, clientPubKey [32]byte, peerAddr netip.Addr) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return net.ErrClosed
	}

	if existing, ok := w.sessions[sessionID]; ok {
		if existing.publicKey == clientPubKey && existing.peerAddr == peerAddr {
			return nil
		}
		// If key changed, remove old peer first
		if err := w.device.IpcSet(fmt.Sprintf("public_key=%s\nremove=true\n", hex.EncodeToString(existing.publicKey[:]))); err != nil {
			return fmt.Errorf("removing previous wireguard peer key: %w", err)
		}
		delete(w.pubKeyToSession, existing.publicKey)
	}

	// Limits: AllowedIPs is exactly one /128 per peer; never a wider prefix.
	peerUAPI := fmt.Sprintf("public_key=%s\nallowed_ip=%s/128\n",
		hex.EncodeToString(clientPubKey[:]), peerAddr.String())
	if err := w.device.IpcSet(peerUAPI); err != nil {
		return fmt.Errorf("configuring wireguard peer for session %s: %w", sessionID, err)
	}

	tcpAddr := &net.TCPAddr{
		IP:   net.ParseIP(peerAddr.String()),
		Port: w.tunnelPort,
	}

	p := &wireguardPeer{
		sessionID: sessionID,
		publicKey: clientPubKey,
		peerAddr:  peerAddr,
		tcpAddr:   tcpAddr,
	}
	w.sessions[sessionID] = p
	w.pubKeyToSession[clientPubKey] = sessionID
	w.broadcastLocked()
	return nil
}

// UnregisterSession removes the peer from the wireguard device and frees session state.
func (w *WireGuardTransport) UnregisterSession(sessionID string) {
	w.mu.Lock()
	p, ok := w.sessions[sessionID]
	if !ok {
		w.mu.Unlock()
		return
	}
	delete(w.sessions, sessionID)
	delete(w.pubKeyToSession, p.publicKey)
	w.broadcastLocked()
	w.mu.Unlock()

	// Remove peer from wireguard-go device
	if err := w.device.IpcSet(fmt.Sprintf("public_key=%s\nremove=true\n", hex.EncodeToString(p.publicKey[:]))); err != nil {
		log.Printf("Frontend: failed to remove wireguard peer %s: %v", hex.EncodeToString(p.publicKey[:]), err)
	}
}

// HasSession returns true if the session is currently registered in the WireGuard transport.
func (w *WireGuardTransport) HasSession(sessionID string) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	_, ok := w.sessions[sessionID]
	return ok
}

// TryGetConn dials the backend netstack over WireGuard for sessionID.
func (w *WireGuardTransport) TryGetConn(ctx context.Context, sessionID string) (net.Conn, error) {
	w.mu.RLock()
	p, ok := w.sessions[sessionID]
	w.mu.RUnlock()

	if !ok {
		return nil, nil
	}

	conn, err := w.netstackNet.DialContextTCP(ctx, p.tcpAddr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// GetConn dials a connection to the specified backend session.
func (w *WireGuardTransport) GetConn(ctx context.Context, sessionID string) (net.Conn, error) {
	conn, _, err := w.GetConnForSessions(ctx, []string{sessionID})
	return conn, err
}

// GetConnForSessions dials the first candidate session that is registered and connects successfully.
func (w *WireGuardTransport) GetConnForSessions(ctx context.Context, sessionIDs []string) (net.Conn, string, error) {
	for {
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return nil, "", net.ErrClosed
		}
		notify := w.notifyCh
		w.mu.Unlock()

		for _, sid := range sessionIDs {
			if w.HasSession(sid) {
				conn, err := w.TryGetConn(ctx, sid)
				if err != nil {
					if ctx.Err() != nil {
						return nil, "", ctx.Err()
					}
					continue
				}
				if conn != nil {
					return conn, sid, nil
				}
			}
		}

		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-notify:
		}
	}
}

// Close closes the WireGuard transport device and associated network resources.
func (w *WireGuardTransport) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	w.broadcastLocked()
	w.mu.Unlock()

	w.device.Close()
	return nil
}

func getDeviceListenPort(d *device.Device) (uint16, error) {
	out, err := d.IpcGet()
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "listen_port=") {
			var p uint16
			_, err := fmt.Sscanf(line, "listen_port=%d", &p)
			if err != nil {
				continue
			}
			return p, nil
		}
	}
	return 0, fmt.Errorf("listen_port not found in device ipc output")
}
