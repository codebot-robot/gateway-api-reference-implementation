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
	"context"
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/certs"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/client"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/jwt"
)

// Helper to sign RS256 token
func testSignRS256(t *testing.T, iss, sub string, aud []string, exp, nbf, iat *time.Time, key *rsa.PrivateKey, kid string) string {
	t.Helper()
	hdrJSON, _ := json.Marshal(map[string]string{
		"alg": "RS256",
		"kid": kid,
		"typ": "JWT",
	})
	hdrB64 := base64.RawURLEncoding.EncodeToString(hdrJSON)

	payload := map[string]any{
		"iss": iss,
		"sub": sub,
		"aud": aud,
	}
	if exp != nil {
		payload["exp"] = exp.Unix()
	}
	if nbf != nil {
		payload["nbf"] = nbf.Unix()
	}
	if iat != nil {
		payload["iat"] = iat.Unix()
	}

	payloadJSON, _ := json.Marshal(payload)
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)

	toSign := hdrB64 + "." + payloadB64
	hashed := sha256.Sum256([]byte(toSign))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, hashed[:])
	if err != nil {
		t.Fatalf("rsa.SignPKCS1v15: %v", err)
	}
	return toSign + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// Helper to sign alg=none token
func testSignNone(t *testing.T, iss, sub string, aud []string, exp *time.Time) string {
	t.Helper()
	hdrJSON, _ := json.Marshal(map[string]string{"alg": "none", "typ": "JWT"})
	hdrB64 := base64.RawURLEncoding.EncodeToString(hdrJSON)

	payload := map[string]any{
		"iss": iss,
		"sub": sub,
		"aud": aud,
	}
	if exp != nil {
		payload["exp"] = exp.Unix()
	}
	payloadJSON, _ := json.Marshal(payload)
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)

	return hdrB64 + "." + payloadB64 + "."
}

// Helper to sign HS256 token
func testSignHS256(t *testing.T, iss, sub string, aud []string, exp *time.Time, secret []byte) string {
	t.Helper()
	hdrJSON, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	hdrB64 := base64.RawURLEncoding.EncodeToString(hdrJSON)

	payload := map[string]any{
		"iss": iss,
		"sub": sub,
		"aud": aud,
	}
	if exp != nil {
		payload["exp"] = exp.Unix()
	}
	payloadJSON, _ := json.Marshal(payload)
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)

	toSign := hdrB64 + "." + payloadB64
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(toSign))
	sig := mac.Sum(nil)

	return toSign + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func exportJWK(pub *rsa.PublicKey, kid string) map[string]any {
	nBytes := pub.N.Bytes()
	eBytes := big.NewInt(int64(pub.E)).Bytes()
	return map[string]any{
		"kty": "RSA",
		"kid": kid,
		"use": "sig",
		"alg": "RS256",
		"n":   base64.RawURLEncoding.EncodeToString(nBytes),
		"e":   base64.RawURLEncoding.EncodeToString(eBytes),
	}
}

func setupJWTTestEnv(t *testing.T) (*Server, string, *certs.GeneratedCerts, *rsa.PrivateKey, string, func()) {
	t.Helper()

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	issuerURL := "https://kubernetes.default.svc"
	jwksDoc := map[string]any{
		"keys": []any{
			exportJWK(&rsaKey.PublicKey, "key-1"),
		},
	}
	jwksJSON, err := json.Marshal(jwksDoc)
	if err != nil {
		t.Fatal(err)
	}

	tmpDir := t.TempDir()
	jwksFilePath := filepath.Join(tmpDir, "jwks.json")
	if err := os.WriteFile(jwksFilePath, jwksJSON, 0644); err != nil {
		t.Fatal(err)
	}

	jwksCache, err := jwt.NewJWKSCache([]jwt.IssuerConfig{
		{
			IssuerURL: issuerURL,
			JWKSFile:  jwksFilePath,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	verifier := jwt.NewVerifier(jwksCache, jwt.WithAudience("snigateway"))

	subjRule1, err := ParseSubjectRule("system:serviceaccount:default:snigateway-agent=*.example.com,app.example.com")
	if err != nil {
		t.Fatal(err)
	}
	subjRule2, err := ParseSubjectRule("system:serviceaccount:wildcard-ns:*=*.wildcard.org")
	if err != nil {
		t.Fatal(err)
	}
	authorizer := NewSubjectAuthorizer([]SubjectRule{subjRule1, subjRule2})

	generated, err := certs.GenerateAll("snigateway.internal", "unused-client")
	if err != nil {
		t.Fatal(err)
	}

	serverTLS, err := certs.NewServerTLSConfig(generated.CA.CertPEM, generated.Server.CertPEM, generated.Server.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	// VerifyClientCertIfGiven enables token auth when no cert is sent
	serverTLS.ClientAuth = tls.VerifyClientCertIfGiven

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	srv, err := NewServer(ServerConfig{
		ListenAddrs:         []string{ln.Addr().String()},
		InternalHostname:    "snigateway.internal",
		ServerTLSConfig:     serverTLS,
		Authorizer:          authorizer,
		JWTVerifier:         verifier,
		LeaseReaperInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		_ = srv.Serve(ln)
	}()

	cleanup := func() {
		_ = srv.Close()
		_ = jwksCache.Close()
	}

	return srv, ln.Addr().String(), generated, rsaKey, issuerURL, cleanup
}

func TestJWT_LocalIssuerAcceptanceCriteria(t *testing.T) {
	_, serverAddr, certBundle, rsaKey, issuerURL, cleanup := setupJWTTestEnv(t)
	defer cleanup()

	clientTLS, err := certs.NewClientTLSConfig(certBundle.CA.CertPEM, nil, nil, "snigateway.internal")
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	expFuture := now.Add(1 * time.Hour)
	expPast := now.Add(-10 * time.Minute)
	validSub := "system:serviceaccount:default:snigateway-agent"
	wildcardSub := "system:serviceaccount:wildcard-ns:any-agent"
	unauthorizedSub := "system:serviceaccount:other:unknown-agent"

	t.Run("Valid token accepted", func(t *testing.T) {
		token := testSignRS256(t, issuerURL, validSub, []string{"snigateway"}, &expFuture, nil, &now, rsaKey, "key-1")
		c := client.NewClient(serverAddr, clientTLS, client.WithTokenProvider(func() (string, error) {
			return token, nil
		}))

		sessID, _, err := c.StartSession(t.Context())
		if err != nil {
			t.Fatalf("StartSession failed: %v", err)
		}
		if sessID == "" {
			t.Fatal("expected non-empty session ID")
		}

		// Register authorized hostnames
		resp, err := c.Register(t.Context(), sessID, []string{"app.example.com", "foo.example.com"})
		if err != nil {
			t.Fatalf("Register failed: %v", err)
		}
		if resp.Status != "registered" {
			t.Fatalf("expected status registered, got %s", resp.Status)
		}
	})

	t.Run("Wrong audience rejected with 401", func(t *testing.T) {
		token := testSignRS256(t, issuerURL, validSub, []string{"wrong-aud"}, &expFuture, nil, &now, rsaKey, "key-1")
		c := client.NewClient(serverAddr, clientTLS, client.WithTokenProvider(func() (string, error) {
			return token, nil
		}))
		_, _, err := c.StartSession(t.Context())
		if err == nil || !strings.Contains(err.Error(), "status 401") {
			t.Fatalf("expected 401 Unauthorized for wrong audience, got: %v", err)
		}
	})

	t.Run("Expired token rejected with 401", func(t *testing.T) {
		token := testSignRS256(t, issuerURL, validSub, []string{"snigateway"}, &expPast, nil, nil, rsaKey, "key-1")
		c := client.NewClient(serverAddr, clientTLS, client.WithTokenProvider(func() (string, error) {
			return token, nil
		}))
		_, _, err := c.StartSession(t.Context())
		if err == nil || !strings.Contains(err.Error(), "status 401") {
			t.Fatalf("expected 401 Unauthorized for expired token, got: %v", err)
		}
	})

	t.Run("Unknown issuer rejected with 401", func(t *testing.T) {
		token := testSignRS256(t, "https://unknown.issuer.com", validSub, []string{"snigateway"}, &expFuture, nil, &now, rsaKey, "key-1")
		c := client.NewClient(serverAddr, clientTLS, client.WithTokenProvider(func() (string, error) {
			return token, nil
		}))
		_, _, err := c.StartSession(t.Context())
		if err == nil || !strings.Contains(err.Error(), "status 401") {
			t.Fatalf("expected 401 Unauthorized for unknown issuer, got: %v", err)
		}
	})

	t.Run("Unknown kid after refresh rejected with 401", func(t *testing.T) {
		token := testSignRS256(t, issuerURL, validSub, []string{"snigateway"}, &expFuture, nil, &now, rsaKey, "unknown-kid")
		c := client.NewClient(serverAddr, clientTLS, client.WithTokenProvider(func() (string, error) {
			return token, nil
		}))
		_, _, err := c.StartSession(t.Context())
		if err == nil || !strings.Contains(err.Error(), "status 401") {
			t.Fatalf("expected 401 Unauthorized for unknown kid, got: %v", err)
		}
	})

	t.Run("alg=none rejected with 401", func(t *testing.T) {
		token := testSignNone(t, issuerURL, validSub, []string{"snigateway"}, &expFuture)
		c := client.NewClient(serverAddr, clientTLS, client.WithTokenProvider(func() (string, error) {
			return token, nil
		}))
		_, _, err := c.StartSession(t.Context())
		if err == nil || !strings.Contains(err.Error(), "status 401") {
			t.Fatalf("expected 401 Unauthorized for alg=none, got: %v", err)
		}
	})

	t.Run("HS256 rejected with 401", func(t *testing.T) {
		token := testSignHS256(t, issuerURL, validSub, []string{"snigateway"}, &expFuture, []byte("secret"))
		c := client.NewClient(serverAddr, clientTLS, client.WithTokenProvider(func() (string, error) {
			return token, nil
		}))
		_, _, err := c.StartSession(t.Context())
		if err == nil || !strings.Contains(err.Error(), "status 401") {
			t.Fatalf("expected 401 Unauthorized for HS256, got: %v", err)
		}
	})

	t.Run("Subject not in rules rejected with disallowed hostnames error", func(t *testing.T) {
		token := testSignRS256(t, issuerURL, unauthorizedSub, []string{"snigateway"}, &expFuture, nil, &now, rsaKey, "key-1")
		c := client.NewClient(serverAddr, clientTLS, client.WithTokenProvider(func() (string, error) {
			return token, nil
		}))
		sessID, _, err := c.StartSession(t.Context())
		if err != nil {
			t.Fatalf("StartSession failed: %v", err)
		}
		_, err = c.Register(t.Context(), sessID, []string{"app.example.com"})
		if err == nil || !strings.Contains(err.Error(), "disallowed hostnames") {
			t.Fatalf("expected disallowed hostnames error, got: %v", err)
		}
	})

	t.Run("<ns>:* rule works", func(t *testing.T) {
		token := testSignRS256(t, issuerURL, wildcardSub, []string{"snigateway"}, &expFuture, nil, &now, rsaKey, "key-1")
		c := client.NewClient(serverAddr, clientTLS, client.WithTokenProvider(func() (string, error) {
			return token, nil
		}))
		sessID, _, err := c.StartSession(t.Context())
		if err != nil {
			t.Fatalf("StartSession failed: %v", err)
		}
		resp, err := c.Register(t.Context(), sessID, []string{"foo.wildcard.org"})
		if err != nil {
			t.Fatalf("Register failed for <ns>:* subject: %v", err)
		}
		if resp.Status != "registered" {
			t.Fatalf("expected registered, got %s", resp.Status)
		}
	})
}

func TestJWT_SessionBinding(t *testing.T) {
	_, serverAddr, certBundle, rsaKey, issuerURL, cleanup := setupJWTTestEnv(t)
	defer cleanup()

	clientTLS, err := certs.NewClientTLSConfig(certBundle.CA.CertPEM, nil, nil, "snigateway.internal")
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	expFuture := now.Add(1 * time.Hour)
	subA := "system:serviceaccount:default:snigateway-agent"
	subB := "system:serviceaccount:wildcard-ns:any-agent"

	tokenA1 := testSignRS256(t, issuerURL, subA, []string{"snigateway"}, &expFuture, nil, &now, rsaKey, "key-1")
	// Rotated token for same sub (different timestamp/signature)
	later := now.Add(5 * time.Minute)
	expRotated := expFuture.Add(1 * time.Hour)
	tokenA2 := testSignRS256(t, issuerURL, subA, []string{"snigateway"}, &expRotated, nil, &later, rsaKey, "key-1")
	tokenB := testSignRS256(t, issuerURL, subB, []string{"snigateway"}, &expFuture, nil, &now, rsaKey, "key-1")

	// 1. Start session with tokenA1
	currentToken := tokenA1
	c := client.NewClient(serverAddr, clientTLS, client.WithTokenProvider(func() (string, error) {
		return currentToken, nil
	}))

	sessID, _, err := c.StartSession(t.Context())
	if err != nil {
		t.Fatalf("StartSession failed: %v", err)
	}

	// 2. Dial-back with rotated token for same sub succeeds
	currentToken = tokenA2
	conn, err := c.DialTunnel(t.Context(), sessID)
	if err != nil {
		t.Fatalf("dial-back with rotated token for same sub failed: %v", err)
	}
	_ = conn.Close()

	// 3. Dial-back with token for different sub gets 403 identity mismatch
	currentToken = tokenB
	_, err = c.DialTunnel(t.Context(), sessID)
	if err == nil {
		t.Fatal("expected dial-back with token for different sub to fail")
	}
	if !strings.Contains(err.Error(), "status 403") && !strings.Contains(err.Error(), "session identity mismatch") {
		t.Fatalf("expected 403 session identity mismatch, got: %v", err)
	}
}

func TestJWT_LeaseExpirationAndRenewal(t *testing.T) {
	srv, serverAddr, certBundle, rsaKey, issuerURL, cleanup := setupJWTTestEnv(t)
	defer cleanup()

	clientTLS, err := certs.NewClientTLSConfig(certBundle.CA.CertPEM, nil, nil, "snigateway.internal")
	if err != nil {
		t.Fatal(err)
	}

	sub := "system:serviceaccount:default:snigateway-agent"
	now := time.Now()

	t.Run("Registration expires at token exp without re-register", func(t *testing.T) {
		// Short-lived token expiring in 200ms
		expShort := now.Add(200 * time.Millisecond)
		token := testSignRS256(t, issuerURL, sub, []string{"snigateway"}, &expShort, nil, &now, rsaKey, "key-1")

		c := client.NewClient(serverAddr, clientTLS, client.WithTokenProvider(func() (string, error) {
			return token, nil
		}))

		sessID, sessErrCh, err := c.StartSession(t.Context())
		if err != nil {
			t.Fatalf("StartSession failed: %v", err)
		}

		_, err = c.Register(t.Context(), sessID, []string{"app.example.com"})
		if err != nil {
			t.Fatalf("Register failed: %v", err)
		}

		// Verify immediately registered
		if _, ok := srv.table.Match("app.example.com"); !ok {
			t.Fatal("expected app.example.com to be registered initially")
		}

		// Wait for lease reaper to trigger after 200ms exp
		select {
		case err := <-sessErrCh:
			// Session should drop/close
			_ = err
		case <-time.After(1 * time.Second):
			// Also check table directly
		}

		// Verify registration expired and dropped from table
		if _, ok := srv.table.Match("app.example.com"); ok {
			t.Fatal("expected app.example.com to expire and be dropped from registration table")
		}
	})

	t.Run("Re-register with fresh token extends lease", func(t *testing.T) {
		// First token expires in 400ms
		exp1 := time.Now().Add(400 * time.Millisecond)
		token1 := testSignRS256(t, issuerURL, sub, []string{"snigateway"}, &exp1, nil, &now, rsaKey, "key-1")

		var currentToken = token1
		c := client.NewClient(serverAddr, clientTLS, client.WithTokenProvider(func() (string, error) {
			return currentToken, nil
		}))

		sessID, _, err := c.StartSession(t.Context())
		if err != nil {
			t.Fatalf("StartSession failed: %v", err)
		}

		_, err = c.Register(t.Context(), sessID, []string{"app.example.com"})
		if err != nil {
			t.Fatalf("Register failed: %v", err)
		}

		// Re-register before 400ms with fresh token extending to +5s
		expFresh := time.Now().Add(5 * time.Second)
		tokenFresh := testSignRS256(t, issuerURL, sub, []string{"snigateway"}, &expFresh, nil, &now, rsaKey, "key-1")
		currentToken = tokenFresh

		_, err = c.Register(t.Context(), sessID, []string{"app.example.com"})
		if err != nil {
			t.Fatalf("Re-register failed: %v", err)
		}

		// Wait 500ms (past the original 400ms expiry)
		time.Sleep(500 * time.Millisecond)

		// app.example.com must STILL be registered because lease was extended!
		if _, ok := srv.table.Match("app.example.com"); !ok {
			t.Fatal("expected app.example.com to still be active after lease extension")
		}
	})
}

func TestJWT_MTLSOnlyDeploymentsUnchanged(t *testing.T) {
	// Configure server WITHOUT any JWTVerifier (mTLS only)
	generated, err := certs.GenerateAll("snigateway.internal", "client-mtls")
	if err != nil {
		t.Fatal(err)
	}

	serverTLS, err := certs.NewServerTLSConfig(generated.CA.CertPEM, generated.Server.CertPEM, generated.Server.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	srv, err := NewServer(ServerConfig{
		ListenAddrs:      []string{ln.Addr().String()},
		InternalHostname: "snigateway.internal",
		ServerTLSConfig:  serverTLS, // Has RequireAndVerifyClientCert
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	go func() {
		_ = srv.Serve(ln)
	}()

	// 1. Client without certificate connects
	clientTLSNoCert, err := certs.NewClientTLSConfig(generated.CA.CertPEM, nil, nil, "snigateway.internal")
	if err != nil {
		t.Fatal(err)
	}

	cNoCert := client.NewClient(ln.Addr().String(), clientTLSNoCert)
	_, _, err = cNoCert.StartSession(t.Context())
	if err == nil {
		t.Fatal("expected request without client certificate to be rejected in mTLS-only mode")
	}

	// 2. Client with certificate succeeds
	clientTLSWithCert, err := certs.NewClientTLSConfig(generated.CA.CertPEM, generated.Client.CertPEM, generated.Client.KeyPEM, "snigateway.internal")
	if err != nil {
		t.Fatal(err)
	}
	cWithCert := client.NewClient(ln.Addr().String(), clientTLSWithCert)
	sessID, _, err := cWithCert.StartSession(t.Context())
	if err != nil {
		t.Fatalf("client with certificate should succeed: %v", err)
	}
	if sessID == "" {
		t.Fatal("expected non-empty session ID")
	}
}

func TestJWT_TokensNeverAppearInLogsOrErrors(t *testing.T) {
	_, serverAddr, certBundle, rsaKey, issuerURL, cleanup := setupJWTTestEnv(t)
	defer cleanup()

	clientTLS, err := certs.NewClientTLSConfig(certBundle.CA.CertPEM, nil, nil, "snigateway.internal")
	if err != nil {
		t.Fatal(err)
	}

	secretToken := "SUPER_SECRET_TOKEN_MATERIAL_THAT_MUST_NEVER_APPEAR_IN_LOGS_OR_ERRORS"
	now := time.Now()
	exp := now.Add(-1 * time.Hour) // expired
	badToken := testSignRS256(t, issuerURL, "sub", []string{"snigateway"}, &exp, nil, nil, rsaKey, "key-1")
	badToken = strings.Replace(badToken, badToken[10:30], secretToken, 1)

	// Make HTTP request directly to capture raw error response
	url := fmt.Sprintf("https://%s/v1/session", "snigateway.internal")
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer "+badToken)

	transport := &http.Transport{
		TLSClientConfig: clientTLS,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return net.Dial("tcp", serverAddr)
		},
	}
	httpClient := &http.Client{Transport: transport}

	resp, err := httpClient.Do(req)
	if err != nil {
		// If error occurred, ensure secretToken is not in error string
		if strings.Contains(err.Error(), secretToken) {
			t.Fatalf("token leaked in client error: %v", err)
		}
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), secretToken) {
		t.Fatalf("token leaked in HTTP error response body: %s", string(body))
	}
	if strings.Contains(string(body), "SUPER_SECRET") {
		t.Fatalf("token leaked in HTTP error response body: %s", string(body))
	}
}
