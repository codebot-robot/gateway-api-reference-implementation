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
	"testing"
)

func TestHashAndResourceName(t *testing.T) {
	h := Hash("cluster-1", "default", "my-gateway")
	if len(h) != 8 {
		t.Fatalf("expected 8 characters, got %d (%q)", len(h), h)
	}

	name := ResourceName("gcpgw", "cluster-1", "default", "my-gateway", "8443")
	expected := "gcpgw-" + h + "-8443"
	if name != expected {
		t.Errorf("expected %q, got %q", expected, name)
	}

	// Name should not contain readable namespace or gateway name
	if len(name) > 63 {
		t.Errorf("name exceeds 63 characters: %d (%q)", len(name), name)
	}
}

func TestMetadataEncodeAndParse(t *testing.T) {
	orig := &NegMetadata{
		ClusterID:   "my-cluster",
		Namespace:   "prod",
		GatewayName: "api-gw",
		Port:        8443,
	}

	encoded, err := orig.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	parsed, err := Parse(encoded)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if !parsed.Matches("my-cluster", "prod", "api-gw", 8443) {
		t.Errorf("expected metadata to match, got %+v", parsed)
	}

	if parsed.Matches("other-cluster", "prod", "api-gw", 8443) {
		t.Errorf("expected mismatch on cluster-id")
	}
	if parsed.Matches("my-cluster", "other-ns", "api-gw", 8443) {
		t.Errorf("expected mismatch on namespace")
	}
	if parsed.Matches("my-cluster", "prod", "other-gw", 8443) {
		t.Errorf("expected mismatch on gateway name")
	}
	if parsed.Matches("my-cluster", "prod", "api-gw", 8080) {
		t.Errorf("expected mismatch on port")
	}
}

func TestMetadataParse_StringPortAndAltKeys(t *testing.T) {
	jsonStr := `{"clusterId":"cluster-1","namespace":"default","gatewayName":"gw","port":"8080"}`
	parsed, err := Parse(jsonStr)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if parsed.Port != 8080 {
		t.Errorf("expected port 8080, got %d", parsed.Port)
	}
	if !parsed.Matches("cluster-1", "default", "gw", 8080) {
		t.Errorf("expected match")
	}
}

func TestMetadataParse_Invalid(t *testing.T) {
	tests := []struct {
		name string
		json string
	}{
		{"empty", ""},
		{"malformed json", "{not json}"},
		{"missing cluster", `{"namespace":"default","gateway-name":"gw"}`},
		{"missing namespace", `{"cluster-id":"c1","gateway-name":"gw"}`},
		{"missing gateway", `{"cluster-id":"c1","namespace":"default"}`},
		{"invalid port string", `{"cluster-id":"c1","namespace":"d","gateway-name":"g","port":"abc"}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.json)
			if err == nil {
				t.Fatalf("expected error for %q, got nil", tc.json)
			}
		})
	}
}
