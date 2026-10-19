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
	"errors"
	"fmt"
	"log"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/api"
	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun"
	"golang.zx2c4.com/wireguard/tun/netstack"
)

type wireguardPeer struct {
	sessionID     string
	publicKey     [32]byte
	peerAddr      netip.Addr
	tcpAddr       *net.TCPAddr
	registeredAt  time.Time
	lastHandshake time.Time
}

// WireGuardTransportConfig configures the frontend WireGuard transport device.
type WireGuardTransportConfig struct {
	ListenAddr            string // UDP listen address, e.g. ":51820" or ":0"
	Prefix                string // IPv6 prefix, e.g. "fd00::/64"
	Endpoint              string // Optional advertised endpoint "<host:port>"
	TunnelPort            int    // Target TCP port on controller netstack inside the tunnel
	MTU                   int    // Tunnel MTU
	PerAttemptTimeout     time.Duration
	HandshakeSyncInterval time.Duration    // Interval to refresh cached peer handshakes (default: 5s)
	Now                   func() time.Time // Clock function for tests (default: time.Now)
	Logger                *device.Logger
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
	perAttemptTimeout  time.Duration
	syncInterval       time.Duration
	now                func() time.Time
	sessions           map[string]*wireguardPeer
	pubKeyToSession    map[[32]byte]string
	notifyCh           chan struct{}
	syncCh             chan struct{}
	stopCh             chan struct{}
	doneCh             chan struct{}
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

	timeNow := cfg.Now
	if timeNow == nil {
		timeNow = time.Now
	}

	syncInterval := cfg.HandshakeSyncInterval
	if syncInterval <= 0 {
		syncInterval = 5 * time.Second
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
		perAttemptTimeout:  cfg.PerAttemptTimeout,
		syncInterval:       syncInterval,
		now:                timeNow,
		sessions:           make(map[string]*wireguardPeer),
		pubKeyToSession:    make(map[[32]byte]string),
		notifyCh:           make(chan struct{}),
		syncCh:             make(chan struct{}, 1),
		stopCh:             make(chan struct{}),
		doneCh:             make(chan struct{}),
	}

	go tr.syncLoop()

	return tr, nil
}

func (w *WireGuardTransport) broadcastLocked() {
	close(w.notifyCh)
	w.notifyCh = make(chan struct{})
}

// Notify returns a receive-only channel that is closed and recreated when session registrations change.
func (w *WireGuardTransport) Notify() <-chan struct{} {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.notifyCh
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

	if w.closed {
		w.mu.Unlock()
		return net.ErrClosed
	}

	// Reject if client public key equals frontend's own public key
	if clientPubKey == w.publicKey {
		w.mu.Unlock()
		return fmt.Errorf("client public key cannot equal frontend public key")
	}

	// Reject if public key is already bound to a different session
	if existingSession, ok := w.pubKeyToSession[clientPubKey]; ok && existingSession != sessionID {
		w.mu.Unlock()
		return fmt.Errorf("public key is already bound to session %s", existingSession)
	}

	if existing, ok := w.sessions[sessionID]; ok {
		if existing.publicKey == clientPubKey && existing.peerAddr == peerAddr {
			w.mu.Unlock()
			return nil
		}
		// If key changed, remove old peer first
		if err := w.device.IpcSet(fmt.Sprintf("public_key=%s\nremove=true\n", hex.EncodeToString(existing.publicKey[:]))); err != nil {
			w.mu.Unlock()
			return fmt.Errorf("removing previous wireguard peer key: %w", err)
		}
		delete(w.pubKeyToSession, existing.publicKey)
	}

	// Limits: AllowedIPs is exactly one /128 per peer; never a wider prefix.
	// replace_allowed_ips=true ensures no prior allowed IPs remain on the peer.
	peerUAPI := fmt.Sprintf("public_key=%s\nreplace_allowed_ips=true\nallowed_ip=%s/128\n",
		hex.EncodeToString(clientPubKey[:]), peerAddr.String())
	if err := w.device.IpcSet(peerUAPI); err != nil {
		w.mu.Unlock()
		return fmt.Errorf("configuring wireguard peer for session %s: %w", sessionID, err)
	}

	tcpAddr := &net.TCPAddr{
		IP:   net.ParseIP(peerAddr.String()),
		Port: w.tunnelPort,
	}

	p := &wireguardPeer{
		sessionID:    sessionID,
		publicKey:    clientPubKey,
		peerAddr:     peerAddr,
		tcpAddr:      tcpAddr,
		registeredAt: w.now(),
	}
	w.sessions[sessionID] = p
	w.pubKeyToSession[clientPubKey] = sessionID
	w.broadcastLocked()
	w.mu.Unlock()

	if err := w.syncHandshakes(); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Printf("Frontend: failed to sync wireguard handshakes after registering peer %s: %v", sessionID, err)
	}
	w.triggerSync()
	return nil
}

// UnregisterSession removes the peer from the wireguard device and frees session state.
func (w *WireGuardTransport) UnregisterSession(sessionID string) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
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
// It skips peers whose cached handshake is zero (not established) or older than 180s,
// and bounds the dial attempt to PerAttemptTimeout so other candidates are not starved.
func (w *WireGuardTransport) TryGetConn(ctx context.Context, sessionID string) (net.Conn, error) {
	w.mu.RLock()
	p, ok := w.sessions[sessionID]
	if !ok {
		w.mu.RUnlock()
		return nil, nil
	}
	hsTime := p.lastHandshake
	w.mu.RUnlock()

	// Skip peer if last handshake is zero (never completed) or older than 180s
	if hsTime.IsZero() || w.now().Sub(hsTime) > 180*time.Second {
		return nil, nil
	}

	attemptTimeout := w.perAttemptTimeout
	if attemptTimeout <= 0 {
		attemptTimeout = 500 * time.Millisecond
	}
	dialCtx, dialCancel := context.WithTimeout(ctx, attemptTimeout)
	defer dialCancel()

	conn, err := w.netstackNet.DialContextTCP(dialCtx, p.tcpAddr)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// Connection attempt timed out or failed on this candidate; return nil to try next
		return nil, nil
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
	close(w.stopCh)
	w.broadcastLocked()
	w.mu.Unlock()

	<-w.doneCh
	w.device.Close()
	return nil
}

func (w *WireGuardTransport) triggerSync() {
	select {
	case w.syncCh <- struct{}{}:
	default:
	}
}

func (w *WireGuardTransport) syncLoop() {
	defer close(w.doneCh)

	timer := time.NewTimer(w.syncInterval)
	defer timer.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-w.syncCh:
		case <-timer.C:
		}

		if err := w.syncHandshakes(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("Frontend: failed to sync wireguard handshakes: %v", err)
		}

		w.mu.RLock()
		hasZeroHandshake := false
		now := w.now()
		for _, p := range w.sessions {
			if p.lastHandshake.IsZero() && now.Sub(p.registeredAt) <= w.syncInterval {
				hasZeroHandshake = true
				break
			}
		}
		w.mu.RUnlock()

		nextInterval := w.syncInterval
		if hasZeroHandshake && nextInterval > 50*time.Millisecond {
			nextInterval = 50 * time.Millisecond
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(nextInterval)
	}
}

func parseIpcHandshakes(out string) map[[32]byte]time.Time {
	result := make(map[[32]byte]time.Time)
	var currentKey [32]byte
	var hasKey bool
	var sec, nsec int64

	saveCurrent := func() {
		if hasKey {
			if sec > 0 {
				result[currentKey] = time.Unix(sec, nsec)
			} else {
				result[currentKey] = time.Time{}
			}
		}
	}

	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "public_key=") {
			saveCurrent()
			pkHex := strings.TrimPrefix(line, "public_key=")
			b, err := hex.DecodeString(pkHex)
			if err == nil && len(b) == 32 {
				copy(currentKey[:], b)
				hasKey = true
			} else {
				hasKey = false
			}
			sec = 0
			nsec = 0
		} else if hasKey && strings.HasPrefix(line, "last_handshake_time_sec=") {
			if _, err := fmt.Sscanf(line, "last_handshake_time_sec=%d", &sec); err != nil {
				sec = 0
			}
		} else if hasKey && strings.HasPrefix(line, "last_handshake_time_nsec=") {
			if _, err := fmt.Sscanf(line, "last_handshake_time_nsec=%d", &nsec); err != nil {
				nsec = 0
			}
		}
	}
	saveCurrent()
	return result
}

func (w *WireGuardTransport) syncHandshakes() error {
	w.mu.RLock()
	if w.closed {
		w.mu.RUnlock()
		return net.ErrClosed
	}
	w.mu.RUnlock()

	out, err := w.device.IpcGet()
	if err != nil {
		return err
	}
	handshakes := parseIpcHandshakes(out)

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return net.ErrClosed
	}

	for pubKey, hsTime := range handshakes {
		if sid, ok := w.pubKeyToSession[pubKey]; ok {
			if p, ok := w.sessions[sid]; ok {
				p.lastHandshake = hsTime
			}
		}
	}
	return nil
}

// SyncHandshakes immediately queries the WireGuard device and updates cached handshake timestamps for all sessions.
func (w *WireGuardTransport) SyncHandshakes() error {
	return w.syncHandshakes()
}

// PeerLastHandshake returns the cached last-handshake time for sessionID, or zero time if not registered.
func (w *WireGuardTransport) PeerLastHandshake(sessionID string) time.Time {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if p, ok := w.sessions[sessionID]; ok {
		return p.lastHandshake
	}
	return time.Time{}
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
