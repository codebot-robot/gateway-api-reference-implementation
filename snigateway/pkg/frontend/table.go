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
// It supports multiple clients registering the same hostname, and supports exact hostnames
// as well as wildcard hostnames (*.example.com), with exact match taking precedence.
type RegistrationTable struct {
	mu          sync.RWMutex
	clientHosts map[string][]string // clientID -> registered host patterns
	exact       map[string][]string // normalized exact hostname -> slice of clientIDs
	wildcard    map[string][]string // normalized wildcard pattern (*.example.com) -> slice of clientIDs
	exactRR     map[string]int      // round-robin counter per exact hostname
	wildcardRR  map[string]int      // round-robin counter per wildcard pattern
}

// NewRegistrationTable creates a new empty RegistrationTable.
func NewRegistrationTable() *RegistrationTable {
	return &RegistrationTable{
		clientHosts: make(map[string][]string),
		exact:       make(map[string][]string),
		wildcard:    make(map[string][]string),
		exactRR:     make(map[string]int),
		wildcardRR:  make(map[string]int),
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
			t.wildcard[h] = append(t.wildcard[h], clientID)
		} else {
			t.exact[h] = append(t.exact[h], clientID)
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

func removeFromSlice(slice []string, val string) []string {
	var res []string
	for _, item := range slice {
		if item != val {
			res = append(res, item)
		}
	}
	return res
}

func (t *RegistrationTable) unregisterLocked(clientID string) {
	if previous, exists := t.clientHosts[clientID]; exists {
		for _, h := range previous {
			if strings.HasPrefix(h, "*.") {
				newSlice := removeFromSlice(t.wildcard[h], clientID)
				if len(newSlice) == 0 {
					delete(t.wildcard, h)
					delete(t.wildcardRR, h)
				} else {
					t.wildcard[h] = newSlice
				}
			} else {
				newSlice := removeFromSlice(t.exact[h], clientID)
				if len(newSlice) == 0 {
					delete(t.exact, h)
					delete(t.exactRR, h)
				} else {
					t.exact[h] = newSlice
				}
			}
		}
		delete(t.clientHosts, clientID)
	}
}

// Match looks up the best matching clientID for a given incoming SNI hostname.
// Exact match wins. If no exact match, longest matching wildcard pattern wins.
// If multiple clients are registered for the matched pattern, selection is round-robined.
func (t *RegistrationTable) Match(hostname string) (string, bool) {
	cleanHost := CleanHostname(hostname)
	if cleanHost == "" {
		return "", false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// 1. Exact match
	if clients, ok := t.exact[cleanHost]; ok && len(clients) > 0 {
		idx := t.exactRR[cleanHost] % len(clients)
		t.exactRR[cleanHost]++
		return clients[idx], true
	}

	// 2. Wildcard match (longest matching wildcard suffix wins)
	var bestPattern string
	bestPatternLen := -1

	for pattern, clients := range t.wildcard {
		if len(clients) == 0 {
			continue
		}
		suffix := pattern[1:] // e.g. .example.com
		if len(cleanHost) > len(suffix) && strings.HasSuffix(cleanHost, suffix) {
			if len(pattern) > bestPatternLen {
				bestPatternLen = len(pattern)
				bestPattern = pattern
			}
		}
	}

	if bestPatternLen >= 0 {
		clients := t.wildcard[bestPattern]
		idx := t.wildcardRR[bestPattern] % len(clients)
		t.wildcardRR[bestPattern]++
		return clients[idx], true
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
