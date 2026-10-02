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
	"net"
	"strings"
	"sync"
)

// RegistrationTable maps registered hostnames to client identifiers.
// It supports exact hostnames and wildcard hostnames (*.example.com), with exact match taking precedence.
type RegistrationTable struct {
	mu          sync.RWMutex
	clientHosts map[string][]string // clientID -> registered host patterns
	exact       map[string]string   // normalized exact hostname -> clientID
	wildcard    map[string]string   // normalized wildcard pattern (*.example.com) -> clientID
}

// NewRegistrationTable creates a new empty RegistrationTable.
func NewRegistrationTable() *RegistrationTable {
	return &RegistrationTable{
		clientHosts: make(map[string][]string),
		exact:       make(map[string]string),
		wildcard:    make(map[string]string),
	}
}

// CleanHostname removes port if present and returns lowercase string.
func CleanHostname(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return strings.ToLower(strings.TrimSpace(host))
}

// Register sets the full list of hostnames served by clientID, replacing any previous list for that client.
func (t *RegistrationTable) Register(clientID string, hostnames []string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Clear previous registrations for this client
	t.unregisterLocked(clientID)

	var registered []string
	for _, rawHost := range hostnames {
		h := CleanHostname(rawHost)
		if h == "" {
			continue
		}
		registered = append(registered, h)
		if strings.HasPrefix(h, "*.") {
			t.wildcard[h] = clientID
		} else {
			t.exact[h] = clientID
		}
	}
	t.clientHosts[clientID] = registered
}

// Unregister removes all hostname registrations for clientID.
func (t *RegistrationTable) Unregister(clientID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.unregisterLocked(clientID)
}

func (t *RegistrationTable) unregisterLocked(clientID string) {
	if previous, exists := t.clientHosts[clientID]; exists {
		for _, h := range previous {
			if strings.HasPrefix(h, "*.") {
				if t.wildcard[h] == clientID {
					delete(t.wildcard, h)
				}
			} else {
				if t.exact[h] == clientID {
					delete(t.exact, h)
				}
			}
		}
		delete(t.clientHosts, clientID)
	}
}

// Match looks up the best matching clientID for a given incoming SNI hostname.
// Exact match wins. If no exact match, longest matching wildcard pattern wins.
func (t *RegistrationTable) Match(hostname string) (string, bool) {
	cleanHost := CleanHostname(hostname)
	if cleanHost == "" {
		return "", false
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	// 1. Exact match
	if clientID, ok := t.exact[cleanHost]; ok {
		return clientID, true
	}

	// 2. Wildcard match (longest matching wildcard suffix wins)
	var bestClientID string
	bestPatternLen := -1

	for pattern, clientID := range t.wildcard {
		suffix := pattern[1:] // e.g. .example.com
		if len(cleanHost) > len(suffix) && strings.HasSuffix(cleanHost, suffix) {
			if len(pattern) > bestPatternLen {
				bestPatternLen = len(pattern)
				bestClientID = clientID
			}
		}
	}

	if bestPatternLen >= 0 {
		return bestClientID, true
	}

	return "", false
}

// GetRegisteredHostnames returns the hostnames registered for a given clientID.
func (t *RegistrationTable) GetRegisteredHostnames(clientID string) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	hosts, ok := t.clientHosts[clientID]
	if !ok {
		return nil
	}
	res := make([]string, len(hosts))
	copy(res, hosts)
	return res
}
