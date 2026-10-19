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

package api

const (
	// DefaultInternalHostname is the default SNI hostname used for frontend mTLS API.
	DefaultInternalHostname = "snigateway.internal"

	// UpgradeProtocol is the protocol value used in HTTP Upgrade for reverse tunnels.
	UpgradeProtocol = "snigateway-tunnel"

	// UpgradeProtocolH2 is the protocol value used in HTTP Upgrade for HTTP/2 multiplexed reverse tunnels.
	UpgradeProtocolH2 = "snigateway-tunnel-h2"

	// HeaderSessionID is the HTTP header used to identify the backend session.
	HeaderSessionID = "X-Session-ID"

	// TunnelALPN is the ALPN identifier used for QUIC reverse tunnels.
	TunnelALPN = "snigateway-tunnel/1"

	// DefaultWireGuardPrefix is the default IPv6 ULA prefix for WireGuard tunnels.
	DefaultWireGuardPrefix = "fd00::/64"

	// DefaultWireGuardListenPort is the default UDP port for the frontend WireGuard device.
	DefaultWireGuardListenPort = 51820

	// DefaultWireGuardTunnelPort is the fixed TCP port backend listeners bind to inside the netstack tunnel.
	DefaultWireGuardTunnelPort = 51821

	// DefaultWireGuardMTU is the inner MTU for WireGuard tunnels.
	DefaultWireGuardMTU = 1280
)

// SessionInitRequest is the initial message sent on the QUIC control stream.
type SessionInitRequest struct {
	Type  string `json:"type"`
	Token string `json:"token,omitempty"`
}

// SessionResponse is returned upon establishing a backend session.
type SessionResponse struct {
	SessionID string `json:"sessionId"`
	Status    string `json:"status"`
}

// RegistrationRequest contains hostnames to be served by the session.
type RegistrationRequest struct {
	Type      string   `json:"type,omitempty"`
	Hostnames []string `json:"hostnames"`
	Token     string   `json:"token,omitempty"`
}

// RegistrationResponse is returned upon successful registration.
type RegistrationResponse struct {
	Status    string   `json:"status"`
	Hostnames []string `json:"hostnames"`
}

// WireGuardExchangeRequest is sent to initiate WireGuard key exchange for a session.
type WireGuardExchangeRequest struct {
	Type      string `json:"type,omitempty"`
	PublicKey string `json:"publicKey"`
	Token     string `json:"token,omitempty"`
}

// WireGuardExchangeResponse contains frontend WireGuard configuration for the session.
type WireGuardExchangeResponse struct {
	FrontendPublicKey string `json:"frontendPublicKey"`
	Endpoint          string `json:"endpoint"`
	PeerAddress       string `json:"peerAddress"`
	FrontendAddress   string `json:"frontendAddress"`
	MTU               int    `json:"mtu"`
	Status            string `json:"status,omitempty"`
}
