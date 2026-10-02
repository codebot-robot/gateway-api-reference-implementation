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

	// RegistrationPath is the path for registering hostnames.
	RegistrationPath = "/v1/registration"

	// ConnectionsPath is the path for the long-lived connections event stream.
	ConnectionsPath = "/v1/connections"

	// ConnectionsPrefix is the prefix for dialing back a connection by ID.
	ConnectionsPrefix = "/v1/connections/"

	// UpgradeHeader is the HTTP Upgrade header name.
	UpgradeHeader = "Upgrade"

	// UpgradeProtocol is the protocol value used in HTTP Upgrade for reverse tunnels.
	UpgradeProtocol = "snigateway-tunnel"
)

// RegistrationRequest contains hostnames to be served by the client.
type RegistrationRequest struct {
	Hostnames []string `json:"hostnames"`
}

// RegistrationResponse is returned upon successful registration.
type RegistrationResponse struct {
	Status    string   `json:"status"`
	Hostnames []string `json:"hostnames"`
}

// ConnectionEvent is streamed to the client over GET /v1/connections.
type ConnectionEvent struct {
	ID         string `json:"id"`
	Hostname   string `json:"hostname,omitempty"`
	RemoteAddr string `json:"remoteAddr,omitempty"`
}
