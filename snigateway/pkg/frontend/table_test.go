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
	"testing"
)

func TestRegistrationTable(t *testing.T) {
	tbl := NewRegistrationTable()

	// Register client A
	tbl.Register("client-a", []string{"foo.example.com", "*.wildcard.org"})
	// Register client B
	tbl.Register("client-b", []string{"bar.example.com", "*.example.com", "*.sub.wildcard.org"})

	tests := []struct {
		name       string
		hostname   string
		wantClient string
		wantFound  bool
	}{
		{
			name:       "exact match client A",
			hostname:   "foo.example.com",
			wantClient: "client-a",
			wantFound:  true,
		},
		{
			name:       "exact match client A with port and uppercase",
			hostname:   "FOO.EXAMPLE.COM:443",
			wantClient: "client-a",
			wantFound:  true,
		},
		{
			name:       "exact match client B",
			hostname:   "bar.example.com",
			wantClient: "client-b",
			wantFound:  true,
		},
		{
			name:       "wildcard match client B",
			hostname:   "other.example.com",
			wantClient: "client-b",
			wantFound:  true,
		},
		{
			name:       "longer wildcard match (*.sub.wildcard.org vs *.wildcard.org)",
			hostname:   "app.sub.wildcard.org",
			wantClient: "client-b",
			wantFound:  true,
		},
		{
			name:       "shorter wildcard match (*.wildcard.org)",
			hostname:   "other.wildcard.org",
			wantClient: "client-a",
			wantFound:  true,
		},
		{
			name:       "unknown domain",
			hostname:   "unregistered.net",
			wantClient: "",
			wantFound:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotClient, gotFound := tbl.Match(tt.hostname)
			if gotFound != tt.wantFound {
				t.Fatalf("Match(%q) gotFound = %v, want %v", tt.hostname, gotFound, tt.wantFound)
			}
			if gotClient != tt.wantClient {
				t.Fatalf("Match(%q) gotClient = %q, want %q", tt.hostname, gotClient, tt.wantClient)
			}
		})
	}

	// Test exact match wins over wildcard when both are registered
	tbl.Register("client-c", []string{"exact.example.com"})
	gotClient, gotFound := tbl.Match("exact.example.com")
	if !gotFound || gotClient != "client-c" {
		t.Fatalf("expected client-c for exact.example.com, got %s (found=%v)", gotClient, gotFound)
	}

	// Test unregister client-c, now wildcard (*.example.com -> client-b) takes over
	tbl.Unregister("client-c")
	gotClient, gotFound = tbl.Match("exact.example.com")
	if !gotFound || gotClient != "client-b" {
		t.Fatalf("expected fallback to client-b for exact.example.com, got %s (found=%v)", gotClient, gotFound)
	}

	// Test re-registration replaces previous hostnames
	tbl.Register("client-a", []string{"new.domain.com"})
	gotClient, gotFound = tbl.Match("foo.example.com")
	if gotFound && gotClient == "client-a" {
		t.Fatalf("foo.example.com should have been unregistered for client-a")
	}
	gotClient, gotFound = tbl.Match("new.domain.com")
	if !gotFound || gotClient != "client-a" {
		t.Fatalf("expected client-a for new.domain.com")
	}
}
