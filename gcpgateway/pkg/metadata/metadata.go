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

package metadata

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// NegMetadata represents the resource metadata stored as JSON in a NEG description.
type NegMetadata struct {
	ClusterID   string `json:"cluster-id"`
	Namespace   string `json:"namespace"`
	GatewayName string `json:"gateway-name"`
	Port        int    `json:"port"`
}

type rawNegMetadata struct {
	ClusterID      string `json:"cluster-id"`
	ClusterIDAlt   string `json:"clusterId"`
	Namespace      string `json:"namespace"`
	GatewayName    string `json:"gateway-name"`
	GatewayNameAlt string `json:"gatewayName"`
	Port           any    `json:"port"`
}

// Hash returns the 8-character lowercase hex SHA-256 hash of clusterID/namespace/gatewayName.
func Hash(clusterID, namespace, gatewayName string) string {
	raw := fmt.Sprintf("%s/%s/%s", clusterID, namespace, gatewayName)
	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:])[:8]
}

// ResourceName returns the full GCP resource name: <prefix>-<8-char-hash>-<suffix>.
func ResourceName(prefix, clusterID, namespace, gatewayName, suffix string) string {
	h := Hash(clusterID, namespace, gatewayName)
	if suffix != "" {
		return fmt.Sprintf("%s-%s-%s", prefix, h, suffix)
	}
	return fmt.Sprintf("%s-%s", prefix, h)
}

// Encode serializes the NegMetadata to a compact JSON string.
func (m *NegMetadata) Encode() (string, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// Parse extracts and validates NegMetadata from a JSON description string.
// Returns an error if the description is missing, empty, or unparseable.
func Parse(desc string) (*NegMetadata, error) {
	trimmed := strings.TrimSpace(desc)
	if trimmed == "" {
		return nil, fmt.Errorf("empty metadata description")
	}

	var raw rawNegMetadata
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return nil, fmt.Errorf("invalid metadata JSON: %w", err)
	}

	cid := raw.ClusterID
	if cid == "" {
		cid = raw.ClusterIDAlt
	}
	gw := raw.GatewayName
	if gw == "" {
		gw = raw.GatewayNameAlt
	}

	var port int
	switch v := raw.Port.(type) {
	case float64:
		port = int(v)
	case int:
		port = v
	case string:
		p, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return nil, fmt.Errorf("invalid port %q: %w", v, err)
		}
		port = p
	case nil:
		port = 0
	default:
		return nil, fmt.Errorf("unsupported port type %T", v)
	}

	if cid == "" || raw.Namespace == "" || gw == "" {
		return nil, fmt.Errorf("missing required metadata fields (cluster-id, namespace, gateway-name)")
	}

	return &NegMetadata{
		ClusterID:   cid,
		Namespace:   raw.Namespace,
		GatewayName: gw,
		Port:        port,
	}, nil
}

// Matches checks if the NegMetadata matches the expected cluster, namespace, gateway name, and optional port.
func (m *NegMetadata) Matches(clusterID, namespace, gatewayName string, port int) bool {
	if m == nil {
		return false
	}
	if m.ClusterID != clusterID || m.Namespace != namespace || m.GatewayName != gatewayName {
		return false
	}
	if port > 0 && m.Port != port {
		return false
	}
	return true
}
