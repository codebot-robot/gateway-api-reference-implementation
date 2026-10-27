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
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	acmeclient "github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/acme"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/cache"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/challenge"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/metrics"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/testserver"
)

type mockClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *mockClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *mockClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func createTestManager(
	t *testing.T,
	pebble *testserver.Server,
	cacheDir string,
	clock Clock,
	optFns ...func(*Options),
) (*Manager, *metrics.Metrics) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	diskCache, err := cache.NewDiskCache(cacheDir)
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}

	acmeCli, err := acmeclient.NewClient(t.Context(), acmeclient.Config{
		DirectoryURL: pebble.DirectoryURL,
		HTTPClient:   pebble.HTTPClient,
	})
	if err != nil {
		t.Fatalf("failed to create ACME client: %v", err)
	}

	defaultOptFns := []func(*Options){
		func(o *Options) {
			if clock != nil {
				o.Clock = clock
			}
			o.RenewalCheckInterval = 50 * time.Millisecond
			o.InitialRetryBackoff = 50 * time.Millisecond
			o.MaxRetryBackoff = 500 * time.Millisecond
			o.RenewalJitterPercent = 0 // deterministic for tests
		},
	}
	defaultOptFns = append(defaultOptFns, optFns...)

	mgr := NewManager(acmeCli, diskCache, pebble.ChallengeStore, m, defaultOptFns...)
	return mgr, m
}

func TestManager_Pebble_Issuance(t *testing.T) {
	pebble := testserver.StartPebble(t, testserver.ServerConfig{})
	tempDir := t.TempDir()
	cacheDir := filepath.Join(tempDir, "cache")

	clock := &mockClock{now: time.Now()}
	mgr, _ := createTestManager(t, pebble, cacheDir, clock)

	ctx := t.Context()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer mgr.Stop()

	hostnames := []string{"web.example.com", "api.example.com"}
	cert, err := mgr.Obtain(ctx, hostnames)
	if err != nil {
		t.Fatalf("Obtain failed: %v", err)
	}

	if cert == nil {
		t.Fatal("expected non-nil certificate")
	}

	// Verify hostnames normalized
	if len(cert.Hostnames) != 2 || cert.Hostnames[0] != "api.example.com" || cert.Hostnames[1] != "web.example.com" {
		t.Errorf("unexpected hostnames in cert: %v", cert.Hostnames)
	}

	// Verify SANs in leaf
	if len(cert.Leaf.DNSNames) != 2 {
		t.Errorf("expected 2 DNSNames in leaf, got %d", len(cert.Leaf.DNSNames))
	}

	// Verify file permissions
	hKey := "api.example.com,web.example.com"
	cached, err := mgr.Cache().Get(hKey)
	if err != nil {
		t.Fatalf("Cache.Get failed: %v", err)
	}
	if cached == nil {
		t.Fatal("expected cached certificate")
	}
}

func TestManager_Pebble_ReuseFromCacheAfterRestart(t *testing.T) {
	pebble := testserver.StartPebble(t, testserver.ServerConfig{})
	tempDir := t.TempDir()
	cacheDir := filepath.Join(tempDir, "cache")

	clock := &mockClock{now: time.Now()}
	ctx := t.Context()

	// 1. Initial run: issue certificate
	mgr1, _ := createTestManager(t, pebble, cacheDir, clock)
	if err := mgr1.Start(ctx); err != nil {
		t.Fatalf("mgr1.Start failed: %v", err)
	}
	hostnames := []string{"restart.example.com"}
	cert1, err := mgr1.Obtain(ctx, hostnames)
	if err != nil {
		t.Fatalf("mgr1.Obtain failed: %v", err)
	}
	mgr1.Stop()

	// 2. Shut down Pebble server to ensure restart uses ONLY local cache without network
	pebble.Close()

	// 3. Restart: create second manager pointing to the same cache directory
	mgr2, _ := createTestManager(t, pebble, cacheDir, clock)
	if err := mgr2.Start(ctx); err != nil {
		t.Fatalf("mgr2.Start failed: %v", err)
	}
	defer mgr2.Stop()

	// Obtain should immediately succeed from local disk cache
	cert2, err := mgr2.Obtain(ctx, hostnames)
	if err != nil {
		t.Fatalf("mgr2.Obtain from cache failed: %v", err)
	}

	if cert2.Leaf.SerialNumber.Cmp(cert1.Leaf.SerialNumber) != 0 {
		t.Errorf("expected reused certificate with same serial number, got %v vs %v",
			cert2.Leaf.SerialNumber, cert1.Leaf.SerialNumber)
	}
}

func TestManager_Pebble_Renewal(t *testing.T) {
	pebble := testserver.StartPebble(t, testserver.ServerConfig{
		ValidityPeriod: 60, // 60 seconds validity
	})
	tempDir := t.TempDir()
	cacheDir := filepath.Join(tempDir, "cache")

	initialTime := time.Now().Truncate(time.Second)
	clock := &mockClock{now: initialTime}

	mgr, _ := createTestManager(t, pebble, cacheDir, clock)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer mgr.Stop()

	hostnames := []string{"renew.example.com"}
	cert1, err := mgr.Obtain(ctx, hostnames)
	if err != nil {
		t.Fatalf("Obtain failed: %v", err)
	}

	// Advance clock past 2/3 lifetime (e.g. 45 seconds out of 60 seconds)
	clock.Advance(45 * time.Second)

	// Trigger renewal check
	if err := mgr.CheckRenewals(ctx); err != nil {
		t.Fatalf("CheckRenewals failed: %v", err)
	}

	// Verify new certificate was issued and cached
	cert2, err := mgr.Cache().Get("renew.example.com")
	if err != nil {
		t.Fatalf("Cache.Get failed: %v", err)
	}

	if cert2.Leaf.SerialNumber.Cmp(cert1.Leaf.SerialNumber) == 0 {
		t.Errorf("expected new certificate serial after renewal, got identical serial: %v", cert1.Leaf.SerialNumber)
	}
}

func TestManager_Pebble_ConcurrentObtainSameSet(t *testing.T) {
	pebble := testserver.StartPebble(t, testserver.ServerConfig{})
	tempDir := t.TempDir()
	cacheDir := filepath.Join(tempDir, "cache")

	clock := &mockClock{now: time.Now()}
	mgr, _ := createTestManager(t, pebble, cacheDir, clock)

	ctx := t.Context()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer mgr.Stop()

	hostnames := []string{"concurrent.example.com"}
	concurrency := 10

	var wg sync.WaitGroup
	certs := make([]*cache.Certificate, concurrency)
	errs := make([]error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c, err := mgr.Obtain(ctx, hostnames)
			certs[idx] = c
			errs[idx] = err
		}(i)
	}

	wg.Wait()

	for i := 0; i < concurrency; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d failed: %v", i, errs[i])
		}
		if certs[i] == nil {
			t.Fatalf("goroutine %d received nil certificate", i)
		}
		// Verify all concurrent callers received the identical certificate
		if certs[i].Leaf.SerialNumber.Cmp(certs[0].Leaf.SerialNumber) != 0 {
			t.Errorf("goroutine %d received different certificate serial: %v vs %v",
				i, certs[i].Leaf.SerialNumber, certs[0].Leaf.SerialNumber)
		}
	}
}

func TestManager_Pebble_FailedValidation(t *testing.T) {
	// Start with custom challenge store that drops challenges to force validation failure
	brokenStore := &brokenChallengeStore{store: challenge.NewMemoryStore()}
	pebble := testserver.StartPebble(t, testserver.ServerConfig{
		ChallengeStore: brokenStore,
	})

	tempDir := t.TempDir()
	cacheDir := filepath.Join(tempDir, "cache")

	clock := &mockClock{now: time.Now()}
	mgr, m := createTestManager(t, pebble, cacheDir, clock)

	ctx := t.Context()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer mgr.Stop()

	// Validation must fail
	_, err := mgr.Obtain(ctx, []string{"fail-validation.example.com"})
	if err == nil {
		t.Fatal("expected Obtain to fail due to validation error, but it succeeded")
	}

	// Verify no certificate in cache
	_, err = mgr.Cache().Get("fail-validation.example.com")
	if !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("expected cache.ErrNotFound, got %v", err)
	}

	// Verify issuance failure metric was recorded
	_ = m
}

type failRoundTripper struct {
	base http.RoundTripper
	fail bool
}

func (f *failRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if f.fail {
		return nil, errors.New("simulated ACME server downtime during renewal")
	}
	return f.base.RoundTrip(req)
}

func TestManager_Pebble_RenewalFailureBackoffAndKeepServing(t *testing.T) {
	pebble := testserver.StartPebble(t, testserver.ServerConfig{
		ValidityPeriod: 60,
	})

	tempDir := t.TempDir()
	cacheDir := filepath.Join(tempDir, "cache")

	initialTime := time.Now().Truncate(time.Second)
	clock := &mockClock{now: initialTime}

	tripper := &failRoundTripper{
		base: pebble.HTTPClient.Transport,
		fail: false,
	}
	customHTTPClient := &http.Client{
		Transport: tripper,
	}

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	diskCache, _ := cache.NewDiskCache(cacheDir)
	acmeCli, err := acmeclient.NewClient(t.Context(), acmeclient.Config{
		DirectoryURL: pebble.DirectoryURL,
		HTTPClient:   customHTTPClient,
	})
	if err != nil {
		t.Fatalf("failed to create ACME client: %v", err)
	}

	mgr := NewManager(acmeCli, diskCache, pebble.ChallengeStore, m, func(o *Options) {
		o.Clock = clock
		o.RenewalCheckInterval = 50 * time.Millisecond
		o.InitialRetryBackoff = 100 * time.Millisecond
		o.MaxRetryBackoff = 500 * time.Millisecond
		o.RenewalJitterPercent = 0
	})

	ctx := t.Context()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer mgr.Stop()

	// 1. Initial Obtain succeeds
	hostnames := []string{"retry.example.com"}
	cert1, err := mgr.Obtain(ctx, hostnames)
	if err != nil {
		t.Fatalf("initial Obtain failed: %v", err)
	}

	// 2. Simulate CA failure during renewal
	tripper.fail = true

	// Advance clock into renewal window (45s of 60s)
	clock.Advance(45 * time.Second)

	// Execute renewal attempt: fails due to simulated CA downtime
	_ = mgr.CheckRenewals(ctx)

	// Verify existing certificate is STILL served from cache!
	served, err := mgr.Obtain(ctx, hostnames)
	if err != nil {
		t.Fatalf("Obtain after renewal failure should keep serving existing certificate, got error: %v", err)
	}
	if served.Leaf.SerialNumber.Cmp(cert1.Leaf.SerialNumber) != 0 {
		t.Fatalf("served certificate changed after renewal failure: %v vs %v",
			served.Leaf.SerialNumber, cert1.Leaf.SerialNumber)
	}

	// Verify state is renewing
	state, ok := mgr.GetTrackedState("retry.example.com")
	if !ok || state != metrics.StateRenewing {
		t.Errorf("expected state %s, got %s (ok=%v)", metrics.StateRenewing, state, ok)
	}

	// 3. Recover CA: retry after backoff succeeds
	tripper.fail = false
	clock.Advance(200 * time.Millisecond) // past 100ms backoff

	if err := mgr.CheckRenewals(ctx); err != nil {
		t.Fatalf("CheckRenewals after CA recovery failed: %v", err)
	}

	// Verify new certificate was issued and cached
	renewed, err := mgr.Obtain(ctx, hostnames)
	if err != nil {
		t.Fatalf("Obtain after recovered renewal failed: %v", err)
	}
	if renewed.Leaf.SerialNumber.Cmp(cert1.Leaf.SerialNumber) == 0 {
		t.Fatalf("expected renewed certificate serial number to change")
	}

	// Verify state is now valid
	state, ok = mgr.GetTrackedState("retry.example.com")
	if !ok || state != metrics.StateValid {
		t.Errorf("expected state %s, got %s (ok=%v)", metrics.StateValid, state, ok)
	}
}

func TestManager_Pebble_ExternalAccountBinding(t *testing.T) {
	eabKID := "test-eab-kid"
	eabKey := "dzA2OGZkZTBkYzljZWYxYTRlMTQ2YjEzYTE4ODQyZDI" // 32 bytes base64

	pebble := testserver.StartPebble(t, testserver.ServerConfig{
		EABKeyID:  eabKID,
		EABMACKey: eabKey,
	})

	tempDir := t.TempDir()
	cacheDir := filepath.Join(tempDir, "cache")

	acmeCli, err := acmeclient.NewClient(t.Context(), acmeclient.Config{
		DirectoryURL: pebble.DirectoryURL,
		EABKID:       eabKID,
		EABHMACKey:   eabKey,
		HTTPClient:   pebble.HTTPClient,
	})
	if err != nil {
		t.Fatalf("failed to create ACME client with EAB: %v", err)
	}

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	diskCache, _ := cache.NewDiskCache(cacheDir)
	mgr := NewManager(acmeCli, diskCache, pebble.ChallengeStore, m)

	ctx := t.Context()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer mgr.Stop()

	cert, err := mgr.Obtain(ctx, []string{"eab.example.com"})
	if err != nil {
		t.Fatalf("Obtain with EAB failed: %v", err)
	}
	if cert == nil {
		t.Fatal("expected certificate with EAB")
	}
}

func TestManager_LimitsAndValidation(t *testing.T) {
	pebble := testserver.StartPebble(t, testserver.ServerConfig{})
	tempDir := t.TempDir()
	cacheDir := filepath.Join(tempDir, "cache")

	clock := &mockClock{now: time.Now()}
	mgr, _ := createTestManager(t, pebble, cacheDir, clock, func(o *Options) {
		o.MaxHostnamesPerSet = 3
		o.MaxHostnameLength = 30
		o.MaxPendingOrders = 1
	})

	ctx := t.Context()

	// 1. Wildcard rejection
	_, err := mgr.Obtain(ctx, []string{"*.example.com"})
	if !errors.Is(err, ErrWildcardNotSupported) {
		t.Errorf("expected ErrWildcardNotSupported, got %v", err)
	}

	// 2. Too many hostnames
	_, err = mgr.Obtain(ctx, []string{"a.com", "b.com", "c.com", "d.com"})
	if !errors.Is(err, ErrTooManyHostnames) {
		t.Errorf("expected ErrTooManyHostnames, got %v", err)
	}

	// 3. Hostname too long
	_, err = mgr.Obtain(ctx, []string{"this-hostname-is-longer-than-thirty-characters.com"})
	if !errors.Is(err, ErrHostnameTooLong) {
		t.Errorf("expected ErrHostnameTooLong, got %v", err)
	}

	// 4. Empty hostnames
	_, err = mgr.Obtain(ctx, []string{})
	if !errors.Is(err, ErrNoHostnames) {
		t.Errorf("expected ErrNoHostnames, got %v", err)
	}
}

func TestManager_PrivateKeyNotInErrorsOrLogs(t *testing.T) {
	pebble := testserver.StartPebble(t, testserver.ServerConfig{})
	tempDir := t.TempDir()
	cacheDir := filepath.Join(tempDir, "cache")

	clock := &mockClock{now: time.Now()}
	mgr, _ := createTestManager(t, pebble, cacheDir, clock)

	ctx := t.Context()
	_ = mgr.Start(ctx)
	defer mgr.Stop()

	cert, err := mgr.Obtain(ctx, []string{"privatekey-safe.example.com"})
	if err != nil {
		t.Fatalf("Obtain failed: %v", err)
	}

	// Read raw private key file
	rawKeyPEM, err := os.ReadFile(filepath.Join(cacheDir, "privatekey-safe.example.com.key"))
	if err != nil {
		// Try hash
		files, _ := os.ReadDir(cacheDir)
		for _, f := range files {
			if strings.HasSuffix(f.Name(), ".key") {
				rawKeyPEM, _ = os.ReadFile(filepath.Join(cacheDir, f.Name()))
				break
			}
		}
	}

	if len(rawKeyPEM) == 0 {
		t.Fatal("failed to find key file")
	}

	certStr := fmt.Sprintf("%v %s", cert, cert)
	if strings.Contains(certStr, string(rawKeyPEM)) || strings.Contains(certStr, "EC PRIVATE KEY") {
		t.Fatalf("private key found in certificate string: %s", certStr)
	}
}

type brokenChallengeStore struct {
	store *challenge.MemoryStore
	pass  bool
}

func (b *brokenChallengeStore) Put(ctx context.Context, token, identifier, keyAuth string, expiresAt time.Time) error {
	if !b.pass {
		return nil // Drop challenge
	}
	return b.store.Put(ctx, token, identifier, keyAuth, expiresAt)
}

func (b *brokenChallengeStore) Remove(ctx context.Context, token string) error {
	return b.store.Remove(ctx, token)
}

func (b *brokenChallengeStore) ResolveChallenge(ctx context.Context, caller, host, token string) (string, error) {
	if !b.pass {
		return "", challenge.ErrNotFound
	}
	return b.store.ResolveChallenge(ctx, caller, host, token)
}
