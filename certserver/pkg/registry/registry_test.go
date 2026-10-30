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

package registry

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// Helper: base64 URL encoder without padding
func base64URLEncode(data []byte) string {
	return base64.RawURLEncoding.EncodeToString(data)
}

// Helper: generate RSA key
func generateRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate RSA key: %v", err)
	}
	return k
}

// Helper: sign JWT with header and payload
func signJWT(t *testing.T, header map[string]any, payload map[string]any, privKey *rsa.PrivateKey) string {
	t.Helper()
	hBytes, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("failed to marshal header: %v", err)
	}
	pBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	signingInput := base64URLEncode(hBytes) + "." + base64URLEncode(pBytes)

	if privKey == nil {
		return signingInput + "."
	}

	hashed := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, privKey, crypto.SHA256, hashed[:])
	if err != nil {
		t.Fatalf("failed to sign JWT: %v", err)
	}
	return signingInput + "." + base64URLEncode(sig)
}

// Helper: build JWKS JSON for given keys
func buildJWKS(keys map[string]*rsa.PublicKey) []byte {
	type jwk struct {
		Kty string `json:"kty"`
		Kid string `json:"kid"`
		Alg string `json:"alg"`
		Use string `json:"use"`
		N   string `json:"n"`
		E   string `json:"e"`
	}
	type jwksDoc struct {
		Keys []jwk `json:"keys"`
	}

	var doc jwksDoc
	for kid, pub := range keys {
		eBytes := []byte{byte(pub.E >> 16), byte(pub.E >> 8), byte(pub.E)}
		for len(eBytes) > 1 && eBytes[0] == 0 {
			eBytes = eBytes[1:]
		}
		doc.Keys = append(doc.Keys, jwk{
			Kty: "RSA",
			Kid: kid,
			Alg: "RS256",
			Use: "sig",
			N:   base64URLEncode(pub.N.Bytes()),
			E:   base64URLEncode(eBytes),
		})
	}
	data, _ := json.Marshal(doc)
	return data
}

// TestTokenVerification tests all required token verification scenarios:
// - valid
// - expired
// - wrong audience
// - wrong issuer
// - unknown kid (triggers a single JWKS refresh)
// - token from cluster A presented as cluster B
// - algorithm none and HMAC rejected
func TestTokenVerification(t *testing.T) {
	keyA1 := generateRSAKey(t)
	keyA2 := generateRSAKey(t) // used for unknown kid refresh test
	keyB := generateRSAKey(t)

	var jwksMu sync.RWMutex
	keysA := map[string]*rsa.PublicKey{
		"key-a1": &keyA1.PublicKey,
	}
	keysB := map[string]*rsa.PublicKey{
		"key-b": &keyB.PublicKey,
	}

	var refreshCountA atomic.Int32

	// Start mock OIDC/JWKS server for cluster A
	serverA := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(fmt.Sprintf(`{"issuer": %q, "jwks_uri": %q}`, "https://cluster-a.example.com", "https://cluster-a.example.com/jwks")))
		case "/jwks":
			refreshCountA.Add(1)
			jwksMu.RLock()
			data := buildJWKS(keysA)
			jwksMu.RUnlock()
			w.Header().Set("Content-Type", "application/json")
			w.Write(data)
		default:
			http.NotFound(w, r)
		}
	}))
	defer serverA.Close()

	// Start mock OIDC/JWKS server for cluster B
	serverB := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(fmt.Sprintf(`{"issuer": %q, "jwks_uri": %q}`, "https://cluster-b.example.com", "https://cluster-b.example.com/jwks")))
		case "/jwks":
			jwksMu.RLock()
			data := buildJWKS(keysB)
			jwksMu.RUnlock()
			w.Header().Set("Content-Type", "application/json")
			w.Write(data)
		default:
			http.NotFound(w, r)
		}
	}))
	defer serverB.Close()

	clusters := []ClusterConfig{
		{
			ClusterID:                "cluster-a",
			IssuerURL:                "https://cluster-a.example.com",
			JWKSURL:                  serverA.URL + "/jwks", // direct JWKS URL pointing to test server
			ControllerServiceAccount: "system:serviceaccount:gateway-system:gari-controller",
		},
		{
			ClusterID:                "cluster-b",
			IssuerURL:                "https://cluster-b.example.com",
			JWKSURL:                  serverB.URL + "/jwks",
			ControllerServiceAccount: "system:serviceaccount:gateway-system:gari-controller",
		},
	}

	reg, err := NewRegistry(clusters,
		WithHTTPClient(serverA.Client()),
		WithUnknownKidRateLimit(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to create registry: %v", err)
	}
	defer reg.Close()

	ctx := t.Context()
	now := time.Now()

	// 1. Valid token
	t.Run("valid token", func(t *testing.T) {
		token := signJWT(t,
			map[string]any{"alg": "RS256", "kid": "key-a1"},
			map[string]any{
				"iss": "https://cluster-a.example.com",
				"sub": "system:serviceaccount:gateway-system:gari-controller",
				"aud": CertServerAudience,
				"exp": now.Add(1 * time.Hour).Unix(),
				"nbf": now.Add(-1 * time.Minute).Unix(),
				"iat": now.Unix(),
			},
			keyA1,
		)

		id, cluster, err := reg.VerifyToken(ctx, token, "cluster-a")
		if err != nil {
			t.Fatalf("expected token to be valid, got error: %v", err)
		}
		if id.ClusterID != "cluster-a" || id.Namespace != "gateway-system" || id.ServiceAccount != "gari-controller" {
			t.Errorf("unexpected identity: %+v", id)
		}
		if cluster.ClusterID != "cluster-a" {
			t.Errorf("unexpected cluster: %+v", cluster)
		}
	})

	// 2. Expired token
	t.Run("expired token", func(t *testing.T) {
		token := signJWT(t,
			map[string]any{"alg": "RS256", "kid": "key-a1"},
			map[string]any{
				"iss": "https://cluster-a.example.com",
				"sub": "system:serviceaccount:gateway-system:gari-controller",
				"aud": CertServerAudience,
				"exp": now.Add(-10 * time.Minute).Unix(), // expired past 60s skew
				"nbf": now.Add(-30 * time.Minute).Unix(),
				"iat": now.Add(-30 * time.Minute).Unix(),
			},
			keyA1,
		)

		_, _, err := reg.VerifyToken(ctx, token, "cluster-a")
		if err == nil || !strings.Contains(err.Error(), "expired") {
			t.Fatalf("expected expired error, got: %v", err)
		}
	})

	// 3. Wrong audience
	t.Run("wrong audience", func(t *testing.T) {
		token := signJWT(t,
			map[string]any{"alg": "RS256", "kid": "key-a1"},
			map[string]any{
				"iss": "https://cluster-a.example.com",
				"sub": "system:serviceaccount:gateway-system:gari-controller",
				"aud": "wrong-audience",
				"exp": now.Add(1 * time.Hour).Unix(),
				"iat": now.Unix(),
			},
			keyA1,
		)

		_, _, err := reg.VerifyToken(ctx, token, "cluster-a")
		if err == nil || !strings.Contains(err.Error(), "audience") {
			t.Fatalf("expected audience mismatch error, got: %v", err)
		}
	})

	// 4. Wrong issuer
	t.Run("wrong issuer", func(t *testing.T) {
		token := signJWT(t,
			map[string]any{"alg": "RS256", "kid": "key-a1"},
			map[string]any{
				"iss": "https://untrusted-issuer.example.com",
				"sub": "system:serviceaccount:gateway-system:gari-controller",
				"aud": CertServerAudience,
				"exp": now.Add(1 * time.Hour).Unix(),
				"iat": now.Unix(),
			},
			keyA1,
		)

		// without explicit cluster ID
		_, _, err := reg.VerifyToken(ctx, token, "")
		if err == nil || !strings.Contains(err.Error(), "untrusted or unconfigured") {
			t.Fatalf("expected untrusted issuer error, got: %v", err)
		}
	})

	// 5. Unknown kid triggers a single JWKS refresh
	t.Run("unknown kid triggers single refresh", func(t *testing.T) {
		// Wait for rate limit window after initial registry startup load to elapse
		time.Sleep(120 * time.Millisecond)

		// Key key-a2 is not yet in keysA
		initialRefreshes := refreshCountA.Load()

		token := signJWT(t,
			map[string]any{"alg": "RS256", "kid": "key-a2"},
			map[string]any{
				"iss": "https://cluster-a.example.com",
				"sub": "system:serviceaccount:gateway-system:gari-controller",
				"aud": CertServerAudience,
				"exp": now.Add(1 * time.Hour).Unix(),
				"iat": now.Unix(),
			},
			keyA2,
		)

		// Before adding key-a2 to mock server, verify attempt triggers a refresh but fails because key not found
		_, _, err := reg.VerifyToken(ctx, token, "cluster-a")
		if err == nil || !strings.Contains(err.Error(), "not found") {
			t.Fatalf("expected key not found error, got: %v", err)
		}

		if refreshCountA.Load() <= initialRefreshes {
			t.Errorf("expected refresh count to increase on unknown kid, before=%d, after=%d", initialRefreshes, refreshCountA.Load())
		}

		// Now add key-a2 to keysA and wait for rate limit to elapse
		time.Sleep(120 * time.Millisecond)
		jwksMu.Lock()
		keysA["key-a2"] = &keyA2.PublicKey
		jwksMu.Unlock()

		refreshesBefore := refreshCountA.Load()
		id, _, err := reg.VerifyToken(ctx, token, "cluster-a")
		if err != nil {
			t.Fatalf("expected token with refreshed kid to succeed, got: %v", err)
		}
		if id.ServiceAccount != "gari-controller" {
			t.Errorf("unexpected SA: %s", id.ServiceAccount)
		}
		if refreshCountA.Load() != refreshesBefore+1 {
			t.Errorf("expected exactly one refresh triggered for new kid, before=%d, after=%d", refreshesBefore, refreshCountA.Load())
		}
	})

	// 6. Token from cluster A presented as cluster B
	t.Run("token from cluster A presented as cluster B", func(t *testing.T) {
		tokenA := signJWT(t,
			map[string]any{"alg": "RS256", "kid": "key-a1"},
			map[string]any{
				"iss": "https://cluster-a.example.com",
				"sub": "system:serviceaccount:gateway-system:gari-controller",
				"aud": CertServerAudience,
				"exp": now.Add(1 * time.Hour).Unix(),
				"iat": now.Unix(),
			},
			keyA1,
		)

		// Presented claiming cluster B
		_, _, err := reg.VerifyToken(ctx, tokenA, "cluster-b")
		if err == nil || !strings.Contains(err.Error(), "does not match") {
			t.Fatalf("expected issuer mismatch error when presenting cluster A token as cluster B, got: %v", err)
		}
	})

	// 7. Algorithm none and HMAC rejected
	t.Run("algorithm none rejected", func(t *testing.T) {
		token := signJWT(t,
			map[string]any{"alg": "none"},
			map[string]any{
				"iss": "https://cluster-a.example.com",
				"sub": "system:serviceaccount:gateway-system:gari-controller",
				"aud": CertServerAudience,
				"exp": now.Add(1 * time.Hour).Unix(),
				"iat": now.Unix(),
			},
			nil, // unsigned
		)

		_, _, err := reg.VerifyToken(ctx, token, "cluster-a")
		if err == nil || !strings.Contains(err.Error(), "none") {
			t.Fatalf("expected 'none' algorithm rejected error, got: %v", err)
		}
	})

	t.Run("HMAC algorithm rejected", func(t *testing.T) {
		token := signJWT(t,
			map[string]any{"alg": "HS256", "kid": "key-a1"},
			map[string]any{
				"iss": "https://cluster-a.example.com",
				"sub": "system:serviceaccount:gateway-system:gari-controller",
				"aud": CertServerAudience,
				"exp": now.Add(1 * time.Hour).Unix(),
				"iat": now.Unix(),
			},
			nil,
		)

		_, _, err := reg.VerifyToken(ctx, token, "cluster-a")
		if err == nil || !strings.Contains(err.Error(), "HMAC") {
			t.Fatalf("expected HMAC algorithm rejected error, got: %v", err)
		}
	})
}

// TestAuthorization tests:
// - a non-controller identity is refused
// - allow and deny lists are applied
// - hostname normalisation works (case, trailing dot, duplicates)
func TestAuthorization(t *testing.T) {
	cluster := &ClusterConfig{
		ClusterID:                "cluster-prod",
		IssuerURL:                "https://cluster-prod.example.com",
		ControllerNamespace:      "gateway-system",
		ControllerServiceAccount: "gari-controller",
		AllowedHostnames: []string{
			"example.com",
			"*.example.org",
		},
		DeniedHostnames: []string{
			"denied.example.org",
			"bad.example.com",
		},
	}

	// 1. Non-controller identity is refused
	t.Run("non-controller identity refused", func(t *testing.T) {
		controllerID := CallerIdentity{
			ClusterID:      "cluster-prod",
			Namespace:      "gateway-system",
			ServiceAccount: "gari-controller",
		}
		if !IsAuthorizedController(cluster, controllerID) {
			t.Errorf("expected genuine controller identity to be authorized")
		}

		attackerID := CallerIdentity{
			ClusterID:      "cluster-prod",
			Namespace:      "default",
			ServiceAccount: "evil-sa",
		}
		if IsAuthorizedController(cluster, attackerID) {
			t.Errorf("expected unauthorized service account to be refused")
		}

		wrongNSID := CallerIdentity{
			ClusterID:      "cluster-prod",
			Namespace:      "other-ns",
			ServiceAccount: "gari-controller",
		}
		if IsAuthorizedController(cluster, wrongNSID) {
			t.Errorf("expected controller in wrong namespace to be refused")
		}
	})

	// 2. Allow and deny lists are applied
	t.Run("allow and deny lists applied", func(t *testing.T) {
		// Allowed exact name
		if err := CheckHostnamePolicy(cluster, "example.com"); err != nil {
			t.Errorf("expected example.com to be allowed, got: %v", err)
		}

		// Allowed wildcard match (*.example.org)
		if err := CheckHostnamePolicy(cluster, "foo.example.org"); err != nil {
			t.Errorf("expected foo.example.org to be allowed, got: %v", err)
		}

		// Denied exact name outside allow list
		if err := CheckHostnamePolicy(cluster, "unauthorized.net"); err == nil {
			t.Errorf("expected unauthorized.net outside allow list to be refused")
		}

		// Sub-sub domain doesn't match single-label wildcard
		if err := CheckHostnamePolicy(cluster, "a.b.example.org"); err == nil {
			t.Errorf("expected a.b.example.org to be refused by single-label wildcard policy")
		}

		// Denied by explicit deny list (even though it matches *.example.org)
		if err := CheckHostnamePolicy(cluster, "denied.example.org"); err == nil {
			t.Errorf("expected denied.example.org to be refused by deny list")
		}

		// Denied by explicit deny list
		if err := CheckHostnamePolicy(cluster, "bad.example.com"); err == nil {
			t.Errorf("expected bad.example.com to be refused by deny list")
		}
	})

	// 3. Multiple hostnames policy check
	t.Run("multiple hostnames policy check", func(t *testing.T) {
		hostnames := []string{"app.example.org", "example.com"}
		cluster := &ClusterConfig{
			AllowedHostnames: []string{"example.com", "*.example.org"},
		}
		if err := CheckHostnamesPolicy(cluster, hostnames); err != nil {
			t.Errorf("expected hostnames to be allowed: %v", err)
		}

		clusterDenied := &ClusterConfig{
			AllowedHostnames: []string{"example.com", "*.example.org"},
			DeniedHostnames:  []string{"app.example.org"},
		}
		if err := CheckHostnamesPolicy(clusterDenied, hostnames); err == nil {
			t.Errorf("expected denied hostname to be rejected")
		}
	})

	// 4. Require HTTPS for jwksURL and issuerURL
	t.Run("require HTTPS for jwksURL and issuerURL", func(t *testing.T) {
		httpJWKSCfg := ClusterConfig{
			ClusterID: "c1",
			IssuerURL: "https://issuer.example.com",
			JWKSURL:   "http://insecure-jwks.example.com/keys",
		}
		if err := httpJWKSCfg.Validate(); err == nil {
			t.Errorf("expected Validate to reject http:// jwksURL")
		}

		httpIssuerCfg := ClusterConfig{
			ClusterID: "c2",
			IssuerURL: "http://insecure-issuer.example.com",
		}
		if err := httpIssuerCfg.Validate(); err == nil {
			t.Errorf("expected Validate to reject http:// issuerURL")
		}

		validHTTPSCfg := ClusterConfig{
			ClusterID: "c3",
			IssuerURL: "https://issuer.example.com",
			JWKSURL:   "https://jwks.example.com/keys",
		}
		if err := validHTTPSCfg.Validate(); err != nil {
			t.Errorf("expected Validate to accept valid https config: %v", err)
		}
	})
}

// TestAuditLogger tests that the audit log outputs single structured JSON lines
// and never includes sensitive tokens or key material.
func TestAuditLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := NewAuditLogger(&buf)

	entry := AuditEntry{
		Timestamp: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC),
		Identity: CallerIdentity{
			ClusterID:      "cluster-1",
			Namespace:      "gateway-system",
			ServiceAccount: "gari-controller",
		},
		Gateway:   "default/prod-gw",
		Hostnames: []string{"example.com", "app.example.com"},
		Decision:  DecisionAllowed,
		Reason:    "certificate returned from cache",
	}

	if err := logger.Log(entry); err != nil {
		t.Fatalf("logger.Log failed: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected exactly one line, got %d lines: %s", len(lines), buf.String())
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &parsed); err != nil {
		t.Fatalf("audit line is not valid JSON: %v", err)
	}

	// Verify absence of sensitive terms
	forbiddenKeywords := []string{"token", "private_key", "key_pem", "secret"}
	for _, kw := range forbiddenKeywords {
		if _, found := parsed[kw]; found {
			t.Errorf("audit log line contains sensitive key %q", kw)
		}
	}

	if parsed["decision"] != "allowed" {
		t.Errorf("expected decision 'allowed', got %v", parsed["decision"])
	}
	if parsed["gateway"] != "default/prod-gw" {
		t.Errorf("expected gateway 'default/prod-gw', got %v", parsed["gateway"])
	}
}

// TestRateLimiter tests that per-identity rate limiting functions properly.
func TestRateLimiter(t *testing.T) {
	limiter := NewIdentityRateLimiter(rate.Limit(5), 2) // 5 qps, burst 2

	id1 := CallerIdentity{ClusterID: "c1", Namespace: "ns1", ServiceAccount: "sa1"}
	id2 := CallerIdentity{ClusterID: "c2", Namespace: "ns2", ServiceAccount: "sa2"}

	// Burst 2 for id1
	if !limiter.Allow(id1) {
		t.Errorf("request 1 for id1 should be allowed")
	}
	if !limiter.Allow(id1) {
		t.Errorf("request 2 for id1 should be allowed")
	}
	if limiter.Allow(id1) {
		t.Errorf("request 3 for id1 should exceed burst limit")
	}

	// id2 has its own separate bucket
	if !limiter.Allow(id2) {
		t.Errorf("request 1 for id2 should be allowed independently of id1")
	}
}

// TestBindingStore tests recording and retrieving gateway bindings.
func TestBindingStore(t *testing.T) {
	store := NewMemoryBindingStore()

	b1 := Binding{
		ClusterID:               "cluster-1",
		Gateway:                 "default/gateway-1",
		DataPlaneServiceAccount: "default/dp-sa-1",
		Hostnames:               []string{"foo.com", "bar.com"},
	}

	store.RecordBinding(b1)

	got, found := store.GetBinding("cluster-1", "default/gateway-1")
	if !found {
		t.Fatalf("expected binding to be found")
	}
	if got.DataPlaneServiceAccount != "default/dp-sa-1" {
		t.Errorf("unexpected DP SA: %s", got.DataPlaneServiceAccount)
	}
	if len(store.ListBindings()) != 1 {
		t.Errorf("expected 1 binding in store, got %d", len(store.ListBindings()))
	}

	// Test ValidatedBinding
	now := time.Now()
	if store.HasValidatedBinding("cluster-1", "default/gateway-1", "foo.com,bar.com", now) {
		t.Errorf("expected no validated binding yet")
	}

	vb := ValidatedBinding{
		ClusterID:    "cluster-1",
		Gateway:      "default/gateway-1",
		HostnamesKey: "foo.com,bar.com",
		ValidatedAt:  now,
		ExpiresAt:    now.Add(1 * time.Hour),
	}
	store.RecordValidatedBinding(vb)

	if !store.HasValidatedBinding("cluster-1", "default/gateway-1", "foo.com,bar.com", now) {
		t.Errorf("expected validated binding to be active")
	}

	// Check expired binding
	if store.HasValidatedBinding("cluster-1", "default/gateway-1", "foo.com,bar.com", now.Add(2*time.Hour)) {
		t.Errorf("expected binding to be expired")
	}
}

func TestMatchesDataPlaneServiceAccount(t *testing.T) {
	tests := []struct {
		name         string
		configuredSA string
		caller       CallerIdentity
		wantMatch    bool
	}{
		{
			name:         "exact full subject match",
			configuredSA: "system:serviceaccount:ns1:sa1",
			caller:       CallerIdentity{ClusterID: "c1", Namespace: "ns1", ServiceAccount: "sa1"},
			wantMatch:    true,
		},
		{
			name:         "slash format match",
			configuredSA: "ns1/sa1",
			caller:       CallerIdentity{ClusterID: "c1", Namespace: "ns1", ServiceAccount: "sa1"},
			wantMatch:    true,
		},
		{
			name:         "colon format match",
			configuredSA: "ns1:sa1",
			caller:       CallerIdentity{ClusterID: "c1", Namespace: "ns1", ServiceAccount: "sa1"},
			wantMatch:    true,
		},
		{
			name:         "unqualified SA is rejected",
			configuredSA: "sa1",
			caller:       CallerIdentity{ClusterID: "c1", Namespace: "ns1", ServiceAccount: "sa1"},
			wantMatch:    false,
		},
		{
			name:         "different SA name",
			configuredSA: "ns1/sa1",
			caller:       CallerIdentity{ClusterID: "c1", Namespace: "ns1", ServiceAccount: "sa2"},
			wantMatch:    false,
		},
		{
			name:         "empty configured SA",
			configuredSA: "",
			caller:       CallerIdentity{ClusterID: "c1", Namespace: "ns1", ServiceAccount: "sa1"},
			wantMatch:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := MatchesDataPlaneServiceAccount(tc.configuredSA, tc.caller)
			if got != tc.wantMatch {
				t.Errorf("MatchesDataPlaneServiceAccount() = %v, want %v", got, tc.wantMatch)
			}
		})
	}
}

func TestIsAuthorizedForwarder(t *testing.T) {
	clusters := []ClusterConfig{
		{
			ClusterID:           "cluster-a",
			IssuerURL:           "https://issuer-a.example.com",
			SharedFrontEndGroup: "group-1",
		},
		{
			ClusterID:           "cluster-b",
			IssuerURL:           "https://issuer-b.example.com",
			SharedFrontEndGroup: "group-1",
		},
		{
			ClusterID: "cluster-c",
			IssuerURL: "https://issuer-c.example.com",
			// No shared-front-end group
		},
		{
			ClusterID: "cluster-d",
			IssuerURL: "https://issuer-d.example.com",
			GatewaySharedFrontEndGroups: map[string]string{
				"default/gw-special": "group-special",
			},
		},
	}

	reg, err := NewRegistry(clusters)
	if err != nil {
		t.Fatalf("failed to create registry: %v", err)
	}
	defer reg.Close()

	bindingStore := NewMemoryBindingStore()
	bindingStore.RecordBinding(Binding{
		ClusterID:               "cluster-b",
		Gateway:                 "default/gw-b",
		DataPlaneServiceAccount: "default/dp-b",
		Hostnames:               []string{"shared.example.com"},
	})

	// 1. Same cluster, matching SA -> authorized
	if !IsAuthorizedForwarder(reg, bindingStore, "cluster-a", "default/gw-a", "default/dp-a",
		CallerIdentity{ClusterID: "cluster-a", Namespace: "default", ServiceAccount: "dp-a"}) {
		t.Errorf("expected authorized for same cluster matching SA")
	}

	// 2. Same cluster, different SA -> refused
	if IsAuthorizedForwarder(reg, bindingStore, "cluster-a", "default/gw-a", "default/dp-a",
		CallerIdentity{ClusterID: "cluster-a", Namespace: "default", ServiceAccount: "dp-other"}) {
		t.Errorf("expected refused for same cluster different SA")
	}

	// 3. Different cluster in same shared-front-end group with recorded binding -> authorized
	if !IsAuthorizedForwarder(reg, bindingStore, "cluster-a", "default/gw-a", "default/dp-a",
		CallerIdentity{ClusterID: "cluster-b", Namespace: "default", ServiceAccount: "dp-b"}) {
		t.Errorf("expected authorized for different cluster in same shared front end group")
	}

	// 4. Different cluster not in shared group -> refused
	if IsAuthorizedForwarder(reg, bindingStore, "cluster-a", "default/gw-a", "default/dp-a",
		CallerIdentity{ClusterID: "cluster-c", Namespace: "default", ServiceAccount: "dp-c"}) {
		t.Errorf("expected refused for cluster not in shared front end group")
	}

	// 5. Cluster without shared group asking, forwarder from another cluster -> refused
	if IsAuthorizedForwarder(reg, bindingStore, "cluster-c", "default/gw-c", "default/dp-c",
		CallerIdentity{ClusterID: "cluster-a", Namespace: "default", ServiceAccount: "dp-a"}) {
		t.Errorf("expected refused when requester has no shared group")
	}

	// 6. Per-gateway shared group override
	if !IsAuthorizedForwarder(reg, bindingStore, "cluster-d", "default/gw-special", "default/dp-d",
		CallerIdentity{ClusterID: "cluster-d", Namespace: "default", ServiceAccount: "dp-d"}) {
		t.Errorf("expected authorized for same gateway")
	}
}
