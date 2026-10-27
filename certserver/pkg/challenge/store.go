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

package challenge

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"time"
)

var (
	// ErrNotFound is returned when a challenge token is not found or has expired.
	ErrNotFound = errors.New("challenge token not found or expired")

	// ErrHostMismatch is returned when the request host does not match the token's identifier.
	ErrHostMismatch = errors.New("request host does not match challenge identifier")
)

// ChallengeResolver provides a lookup method for HTTP-01 challenge responses,
// accepting a caller identity to support future authentication and challenge binding.
type ChallengeResolver interface {
	ResolveChallenge(ctx context.Context, caller string, host string, token string) (string, error)
}

// Store defines the interface for registering, resolving, and removing pending ACME challenges.
type Store interface {
	ChallengeResolver
	Put(ctx context.Context, token string, identifier string, keyAuth string, expiresAt time.Time) error
	Remove(ctx context.Context, token string) error
}

type challengeItem struct {
	token      string
	identifier string // normalized host
	keyAuth    string
	expiresAt  time.Time
}

// MemoryStore is an in-memory thread-safe implementation of Store.
type MemoryStore struct {
	mu         sync.RWMutex
	challenges map[string]*challengeItem
	timeNow    func() time.Time
}

// NewMemoryStore initializes a new in-memory challenge store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		challenges: make(map[string]*challengeItem),
		timeNow:    time.Now,
	}
}

// Put adds or updates a pending challenge.
func (s *MemoryStore) Put(ctx context.Context, token, identifier, keyAuth string, expiresAt time.Time) error {
	normHost := normalizeHost(identifier)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.challenges[token] = &challengeItem{
		token:      token,
		identifier: normHost,
		keyAuth:    keyAuth,
		expiresAt:  expiresAt,
	}
	return nil
}

// Remove deletes a challenge by token.
func (s *MemoryStore) Remove(ctx context.Context, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.challenges, token)
	return nil
}

// ResolveChallenge looks up the challenge response for the specified token and host,
// validating token existence, expiry, and identifier matching.
func (s *MemoryStore) ResolveChallenge(ctx context.Context, caller string, host string, token string) (string, error) {
	s.mu.Lock()
	item, ok := s.challenges[token]
	if !ok {
		s.mu.Unlock()
		return "", ErrNotFound
	}

	now := s.timeNow()
	if !item.expiresAt.IsZero() && now.After(item.expiresAt) {
		delete(s.challenges, token)
		s.mu.Unlock()
		return "", ErrNotFound
	}
	s.mu.Unlock()

	normReqHost := normalizeHost(host)
	if normReqHost != item.identifier {
		return "", ErrHostMismatch
	}

	return item.keyAuth, nil
}

// normalizeHost strips optional port and lowers the hostname string.
func normalizeHost(host string) string {
	h := strings.TrimSpace(host)
	if strings.Contains(h, ":") {
		if parsedHost, _, err := net.SplitHostPort(h); err == nil {
			h = parsedHost
		} else {
			// Fallback for malformed host:port strings
			h = strings.Split(h, ":")[0]
		}
	}
	return strings.ToLower(strings.Trim(h, "[]"))
}
