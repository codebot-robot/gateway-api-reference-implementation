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

package manager

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/acme"
	"golang.org/x/sync/singleflight"

	acmeclient "github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/acme"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/cache"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/challenge"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/metrics"
)

var (
	// ErrTooManyPendingOrders is returned when the number of concurrent pending orders exceeds the configured cap.
	ErrTooManyPendingOrders = errors.New("maximum number of pending ACME orders reached")
)

// Manager coordinates certificate issuance, caching, and background renewals.
type Manager struct {
	client         *acmeclient.Client
	cache          *cache.DiskCache
	challengeStore challenge.Store
	metrics        *metrics.Metrics
	opts           Options

	obtainSf      singleflight.Group
	renewSf       singleflight.Group
	pendingOrders atomic.Int64

	// mu guards renewals map and metrics updates.
	mu       sync.RWMutex
	renewals map[string]*renewalItem

	cancelLoop context.CancelFunc
	loopWg     sync.WaitGroup
}

// NewManager creates a new certificate Manager.
func NewManager(
	client *acmeclient.Client,
	diskCache *cache.DiskCache,
	challengeStore challenge.Store,
	m *metrics.Metrics,
	optFns ...func(*Options),
) *Manager {
	opts := DefaultOptions()
	for _, fn := range optFns {
		fn(&opts)
	}

	return &Manager{
		client:         client,
		cache:          diskCache,
		challengeStore: challengeStore,
		metrics:        m,
		opts:           opts,
		renewals:       make(map[string]*renewalItem),
	}
}

// Start loads certificates from disk cache, initializes renewal tracking, and starts the background renewal loop.
func (m *Manager) Start(ctx context.Context) error {
	if err := m.cache.Load(); err != nil {
		return fmt.Errorf("failed to load certificates from cache: %w", err)
	}

	m.mu.Lock()
	now := m.opts.Clock.Now()
	for _, cert := range m.cache.List() {
		m.trackCertificateLocked(cert, now)
	}
	m.mu.Unlock()

	loopCtx, cancel := context.WithCancel(ctx)
	m.cancelLoop = cancel

	m.loopWg.Add(1)
	go m.runRenewalLoop(loopCtx)

	return nil
}

// Stop terminates the background renewal loop and waits for it to exit.
func (m *Manager) Stop() {
	if m.cancelLoop != nil {
		m.cancelLoop()
	}
	m.loopWg.Wait()
}

// Cache returns the underlying disk cache.
func (m *Manager) Cache() *cache.DiskCache {
	return m.cache
}

// Obtain creates or reuses a certificate order for the normalized hostname set.
// Concurrent calls for the same set share one order via obtainSf.
func (m *Manager) Obtain(ctx context.Context, hostnames []string) (*cache.Certificate, error) {
	normalized, hostnamesKey, err := NormalizeHostnames(hostnames, m.opts.MaxHostnamesPerSet, m.opts.MaxHostnameLength)
	if err != nil {
		return nil, err
	}

	now := m.opts.Clock.Now()

	// 1. Fast-path: check if valid certificate already exists in cache
	if existing, err := m.cache.Get(hostnamesKey); err == nil && existing != nil && !existing.IsExpired(now) {
		return existing, nil
	}

	// 2. Bound pending orders
	if m.opts.MaxPendingOrders > 0 && m.pendingOrders.Load() >= m.opts.MaxPendingOrders {
		return nil, ErrTooManyPendingOrders
	}

	// 3. Singleflight execution for concurrent Obtain requests of the same set
	ch := m.obtainSf.DoChan(hostnamesKey, func() (any, error) {
		// Re-check cache under singleflight lock
		if existing, err := m.cache.Get(hostnamesKey); err == nil && existing != nil && !existing.IsExpired(m.opts.Clock.Now()) {
			return existing, nil
		}

		if m.opts.MaxPendingOrders > 0 {
			if curr := m.pendingOrders.Add(1); curr > m.opts.MaxPendingOrders {
				m.pendingOrders.Add(-1)
				return nil, ErrTooManyPendingOrders
			}
			defer m.pendingOrders.Add(-1)
		}

		// Detached context with independent timeout so that cancellation of the first caller
		// does not abort the shared order for other waiting callers.
		orderCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		cert, err := m.issueCertificate(orderCtx, normalized, hostnamesKey, false)
		if err != nil {
			return nil, err
		}
		return cert, nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*cache.Certificate), nil
	}
}

// Renew forces a certificate renewal for the normalized hostname set, creating a new ACME order.
// Concurrent renewals for the same set coalesce via renewSf and never return a cached certificate.
func (m *Manager) Renew(ctx context.Context, hostnames []string) (*cache.Certificate, error) {
	normalized, hostnamesKey, err := NormalizeHostnames(hostnames, m.opts.MaxHostnamesPerSet, m.opts.MaxHostnameLength)
	if err != nil {
		return nil, err
	}

	// 1. Bound pending orders
	if m.opts.MaxPendingOrders > 0 && m.pendingOrders.Load() >= m.opts.MaxPendingOrders {
		return nil, ErrTooManyPendingOrders
	}

	// 2. Singleflight execution for renewals of the same set. Always issues a new certificate.
	ch := m.renewSf.DoChan(hostnamesKey, func() (any, error) {
		if m.opts.MaxPendingOrders > 0 {
			if curr := m.pendingOrders.Add(1); curr > m.opts.MaxPendingOrders {
				m.pendingOrders.Add(-1)
				return nil, ErrTooManyPendingOrders
			}
			defer m.pendingOrders.Add(-1)
		}

		orderCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		cert, err := m.issueCertificate(orderCtx, normalized, hostnamesKey, true)
		if err != nil {
			return nil, err
		}
		return cert, nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*cache.Certificate), nil
	}
}

// issueCertificate executes the ACME order creation, HTTP-01 challenge completion, CSR submission, and caching.
func (m *Manager) issueCertificate(ctx context.Context, normalized []string, hostnamesKey string, isRenewal bool) (*cache.Certificate, error) {
	cert, err := m.executeACMEOrder(ctx, normalized, hostnamesKey)
	if err != nil {
		if isRenewal {
			m.metrics.RecordRenewal(false)
		} else {
			m.metrics.RecordIssuance(false)
		}
		return nil, err
	}

	if err := m.cache.Put(cert); err != nil {
		if isRenewal {
			m.metrics.RecordRenewal(false)
		} else {
			m.metrics.RecordIssuance(false)
		}
		return nil, fmt.Errorf("failed to cache issued certificate: %w", err)
	}

	m.mu.Lock()
	m.trackCertificateLocked(cert, m.opts.Clock.Now())
	m.mu.Unlock()

	if isRenewal {
		m.metrics.RecordRenewal(true)
	} else {
		m.metrics.RecordIssuance(true)
	}

	return cert, nil
}

func (m *Manager) executeACMEOrder(ctx context.Context, normalized []string, hostnamesKey string) (*cache.Certificate, error) {
	// Ensure ACME account is registered
	if _, err := m.client.EnsureAccount(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure ACME account: %w", err)
	}

	// 1. Authorize order
	order, err := m.client.AuthorizeOrder(ctx, acme.DomainIDs(normalized...))
	if err != nil {
		return nil, fmt.Errorf("failed to authorize ACME order: %w", err)
	}

	// 2. Fulfill pending authorizations
	for _, authzURL := range order.AuthzURLs {
		if err := m.fulfillAuthorization(ctx, authzURL); err != nil {
			return nil, err
		}
	}

	// 3. Wait for order to be ready
	order, err = m.client.WaitOrder(ctx, order.URI)
	if err != nil {
		return nil, fmt.Errorf("order failed while waiting for ready: %w", err)
	}

	// 4. Generate ECDSA P-256 certificate key
	certKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate certificate private key: %w", err)
	}

	// 5. Generate Certificate Signing Request (CSR)
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		DNSNames: normalized,
	}, certKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create CSR: %w", err)
	}

	// 6. Submit CSR and download certificate chain
	derChain, certURL, err := m.client.CreateCertFromOrder(ctx, order, csrDER, true)
	if err != nil {
		if order.Status == acme.StatusValid && order.CertURL != "" {
			derChain, err = m.client.FetchCert(ctx, order.CertURL, true)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to obtain certificate from order (%s): %w", certURL, err)
		}
	}

	if len(derChain) == 0 {
		return nil, errors.New("empty certificate chain returned from ACME server")
	}

	leaf, err := x509.ParseCertificate(derChain[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse leaf certificate: %w", err)
	}

	// Encode certificate chain to PEM
	var certPEM []byte
	for _, b := range derChain {
		certPEM = append(certPEM, pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: b,
		})...)
	}

	// Encode private key to PEM
	keyDER, err := x509.MarshalECPrivateKey(certKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal private key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: keyDER,
	})

	return &cache.Certificate{
		Hostnames:    normalized,
		HostnamesKey: hostnamesKey,
		CertPEM:      certPEM,
		KeyPEM:       keyPEM,
		Leaf:         leaf,
		IssuedAt:     leaf.NotBefore,
		ExpiresAt:    leaf.NotAfter,
	}, nil
}

func (m *Manager) fulfillAuthorization(ctx context.Context, authzURL string) error {
	authz, err := m.client.GetAuthorization(ctx, authzURL)
	if err != nil {
		return fmt.Errorf("failed to get authorization %s: %w", authzURL, err)
	}
	if authz.Status == acme.StatusValid {
		return nil
	}

	var chal *acme.Challenge
	for _, c := range authz.Challenges {
		if c.Type == "http-01" {
			chal = c
			break
		}
	}
	if chal == nil {
		return fmt.Errorf("no HTTP-01 challenge offered for %s", authz.Identifier.Value)
	}

	keyAuth, err := m.client.HTTP01ChallengeResponse(chal.Token)
	if err != nil {
		return fmt.Errorf("failed to compute HTTP-01 challenge response: %w", err)
	}

	expiresAt := authz.Expires
	if expiresAt.IsZero() {
		expiresAt = m.opts.Clock.Now().Add(10 * time.Minute)
	}

	if err := m.challengeStore.Put(ctx, chal.Token, authz.Identifier.Value, keyAuth, expiresAt); err != nil {
		return fmt.Errorf("failed to register challenge in store: %w", err)
	}
	defer func(token string) {
		if rmErr := m.challengeStore.Remove(context.Background(), token); rmErr != nil && !errors.Is(rmErr, challenge.ErrNotFound) {
			log.Printf("certserver: failed to remove challenge token %s: %v", token, rmErr)
		}
	}(chal.Token)

	if _, err := m.client.Accept(ctx, chal); err != nil {
		return fmt.Errorf("failed to accept challenge: %w", err)
	}

	if _, err := m.client.WaitAuthorization(ctx, authz.URI); err != nil {
		return fmt.Errorf("authorization failed for %s: %w", authz.Identifier.Value, err)
	}

	return nil
}
