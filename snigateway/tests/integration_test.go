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

package tests

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
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

	"github.com/gke-labs/gateway-api-reference-implementation/certserver/api/jwt"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/certs"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/client"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/frontend"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/tunnel"
)

func TestTunnelListenerHTTPSIntegration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "snigateway-integration-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 1. Generate mTLS infrastructure certificates
	if err := certs.GenerateAndWriteCertificates(tempDir, "snigateway.internal", "integration-client"); err != nil {
		t.Fatalf("GenerateAndWriteCertificates failed: %v", err)
	}

	caCertPEM, err := os.ReadFile(filepath.Join(tempDir, "ca.crt"))
	if err != nil {
		t.Fatalf("reading ca.crt: %v", err)
	}
	serverCertPEM, err := os.ReadFile(filepath.Join(tempDir, "server.crt"))
	if err != nil {
		t.Fatalf("reading server.crt: %v", err)
	}
	serverKeyPEM, err := os.ReadFile(filepath.Join(tempDir, "server.key"))
	if err != nil {
		t.Fatalf("reading server.key: %v", err)
	}
	clientCertPEM, err := os.ReadFile(filepath.Join(tempDir, "client.crt"))
	if err != nil {
		t.Fatalf("reading client.crt: %v", err)
	}
	clientKeyPEM, err := os.ReadFile(filepath.Join(tempDir, "client.key"))
	if err != nil {
		t.Fatalf("reading client.key: %v", err)
	}

	// 2. Start snigateway-frontend in-process
	serverTLS, err := certs.NewServerTLSConfig(caCertPEM, serverCertPEM, serverKeyPEM)
	if err != nil {
		t.Fatalf("NewServerTLSConfig failed: %v", err)
	}

	frontendListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	frontendAddr := frontendListener.Addr().String()

	frontendUDP, err := net.ListenPacket("udp", frontendAddr)
	if err != nil {
		t.Fatalf("net.ListenPacket failed: %v", err)
	}

	frontendServer, err := frontend.NewServer(frontend.ServerConfig{
		ServerTLSConfig:  serverTLS,
		InternalHostname: "snigateway.internal",
		ConnectTimeout:   5 * time.Second,
		ReadSNITimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer frontendServer.Close()

	go func() {
		_ = frontendServer.ServeAll([]net.Listener{frontendListener}, []net.PacketConn{frontendUDP})
	}()

	// 3. Start tunnel Manager with client connecting to frontend
	clientTLS, err := certs.NewClientTLSConfig(caCertPEM, clientCertPEM, clientKeyPEM, "snigateway.internal")
	if err != nil {
		t.Fatalf("NewClientTLSConfig failed: %v", err)
	}

	c := client.NewClient(frontendAddr, clientTLS)
	mgr := tunnel.NewManager(c, tunnel.WithBackoff(50*time.Millisecond, 200*time.Millisecond))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go func() {
		_ = mgr.Run(ctx)
	}()

	// 4. Generate backend application certificates and start HTTPS server on tunnel.Listener()
	backendCert, err := generateBackendCert("service.example.com", "updated.example.com")
	if err != nil {
		t.Fatalf("generateBackendCert failed: %v", err)
	}

	tlsLis := tls.NewListener(mgr.Listener(), &tls.Config{
		Certificates: []tls.Certificate{backendCert},
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Custom-Header", "ReverseTunnel")
		clientIP, _, _ := net.SplitHostPort(r.RemoteAddr)
		w.Header().Set("X-Real-Client-IP", clientIP)
		_, _ = fmt.Fprintf(w, "Hello from backend for host: %s, path: %s", r.Host, r.URL.Path)
	})

	httpServer := &http.Server{
		Handler: mux,
	}
	defer httpServer.Close()

	go func() {
		_ = httpServer.Serve(tlsLis)
	}()

	// 5. Register initial hostname: service.example.com
	mgr.UpdateHostnames([]string{"service.example.com"})

	// Helper function to send HTTPS request with specific SNI and Host
	sendRequest := func(sniHost string, hostHeader string, path string) (*http.Response, string, error) {
		clientTransport := &http.Transport{
			TLSClientConfig: &tls.Config{
				ServerName:         sniHost,
				InsecureSkipVerify: true,
			},
			DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return tls.Dial("tcp", frontendAddr, &tls.Config{
					ServerName:         sniHost,
					InsecureSkipVerify: true,
				})
			},
		}
		httpClient := &http.Client{
			Transport: clientTransport,
			Timeout:   3 * time.Second,
		}

		req, err := http.NewRequest(http.MethodGet, "https://"+sniHost+path, nil)
		if err != nil {
			return nil, "", err
		}
		if hostHeader != "" {
			req.Host = hostHeader
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, "", err
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, "", err
		}
		return resp, string(body), nil
	}

	// 6. Verify end-to-end HTTPS request with matching SNI
	t.Run("End-to-End HTTPS through tunnel listener", func(t *testing.T) {
		var resp *http.Response
		var body string
		var err error

		// Retry briefly while registration completes
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			resp, body, err = sendRequest("service.example.com", "service.example.com", "/hello")
			if err == nil && resp.StatusCode == http.StatusOK {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}

		if err != nil {
			t.Fatalf("failed to perform HTTPS request: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", resp.StatusCode)
		}
		if gotHeader := resp.Header.Get("X-Custom-Header"); gotHeader != "ReverseTunnel" {
			t.Fatalf("expected header 'ReverseTunnel', got %q", gotHeader)
		}
		if clientIP := resp.Header.Get("X-Real-Client-IP"); clientIP != "127.0.0.1" {
			t.Fatalf("expected real client IP 127.0.0.1 via PROXY protocol, got %q", clientIP)
		}
		expectedBody := "Hello from backend for host: service.example.com, path: /hello"
		if body != expectedBody {
			t.Fatalf("got body %q, want %q", body, expectedBody)
		}
	})

	// 7. Change registered hostnames dynamically: replace service.example.com with updated.example.com
	t.Run("Changing registered hostnames takes effect dynamically", func(t *testing.T) {
		mgr.UpdateHostnames([]string{"updated.example.com"})

		// Verify updated.example.com works
		var resp *http.Response
		var body string
		var err error

		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			resp, body, err = sendRequest("updated.example.com", "updated.example.com", "/hello")
			if err == nil && resp.StatusCode == http.StatusOK {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}

		if err != nil {
			t.Fatalf("request to updated.example.com failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200 for updated.example.com, got %d", resp.StatusCode)
		}
		expectedBody := "Hello from backend for host: updated.example.com, path: /hello"
		if body != expectedBody {
			t.Fatalf("got body %q, want %q", body, expectedBody)
		}

		// Verify old hostname service.example.com is now closed / rejected
		_, _, err = sendRequest("service.example.com", "service.example.com", "/hello")
		if err == nil {
			t.Fatalf("expected old hostname service.example.com to fail after update, but succeeded")
		}
	})
}

func TestTunnelListenerH2TransportIntegration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "snigateway-integration-h2-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	if err := certs.GenerateAndWriteCertificates(tempDir, "snigateway.internal", "integration-client"); err != nil {
		t.Fatalf("GenerateAndWriteCertificates failed: %v", err)
	}

	caCertPEM, _ := os.ReadFile(filepath.Join(tempDir, "ca.crt"))
	serverCertPEM, _ := os.ReadFile(filepath.Join(tempDir, "server.crt"))
	serverKeyPEM, _ := os.ReadFile(filepath.Join(tempDir, "server.key"))
	clientCertPEM, _ := os.ReadFile(filepath.Join(tempDir, "client.crt"))
	clientKeyPEM, _ := os.ReadFile(filepath.Join(tempDir, "client.key"))

	serverTLS, err := certs.NewServerTLSConfig(caCertPEM, serverCertPEM, serverKeyPEM)
	if err != nil {
		t.Fatalf("NewServerTLSConfig failed: %v", err)
	}

	frontendListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	frontendAddr := frontendListener.Addr().String()

	frontendServer, err := frontend.NewServer(frontend.ServerConfig{
		ServerTLSConfig:  serverTLS,
		InternalHostname: "snigateway.internal",
		ConnectTimeout:   5 * time.Second,
		ReadSNITimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer frontendServer.Close()

	go func() {
		_ = frontendServer.Serve(frontendListener)
	}()

	clientTLS, err := certs.NewClientTLSConfig(caCertPEM, clientCertPEM, clientKeyPEM, "snigateway.internal")
	if err != nil {
		t.Fatalf("NewClientTLSConfig failed: %v", err)
	}

	c := client.NewClient(frontendAddr, clientTLS)
	mgr := tunnel.NewManager(c, tunnel.WithTransport(tunnel.TransportModeH2), tunnel.WithBackoff(50*time.Millisecond, 200*time.Millisecond))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go func() {
		_ = mgr.Run(ctx)
	}()

	backendCert, err := generateBackendCert("h2-service.example.com")
	if err != nil {
		t.Fatalf("generateBackendCert failed: %v", err)
	}

	tlsLis := tls.NewListener(mgr.Listener(), &tls.Config{
		Certificates: []tls.Certificate{backendCert},
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/h2-test", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Tunnel-Transport", "h2")
		_, _ = fmt.Fprintf(w, "Hello via HTTP/2 CONNECT tunnel!")
	})

	httpServer := &http.Server{Handler: mux}
	defer httpServer.Close()

	go func() {
		_ = httpServer.Serve(tlsLis)
	}()

	mgr.UpdateHostnames([]string{"h2-service.example.com"})

	sendRequest := func(sniHost, path string) (*http.Response, string, error) {
		clientTransport := &http.Transport{
			TLSClientConfig: &tls.Config{
				ServerName:         sniHost,
				InsecureSkipVerify: true,
			},
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.Dial(network, frontendAddr)
			},
		}
		httpClient := &http.Client{
			Transport: clientTransport,
			Timeout:   5 * time.Second,
		}
		resp, err := httpClient.Get(fmt.Sprintf("https://%s%s", sniHost, path))
		if err != nil {
			return nil, "", err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, "", err
		}
		return resp, string(body), nil
	}

	deadline := time.Now().Add(5 * time.Second)
	var resp *http.Response
	var body string
	for time.Now().Before(deadline) {
		resp, body, err = sendRequest("h2-service.example.com", "/h2-test")
		if err == nil && resp.StatusCode == http.StatusOK {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if err != nil {
		t.Fatalf("request over h2 tunnel failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	if body != "Hello via HTTP/2 CONNECT tunnel!" {
		t.Fatalf("unexpected body: %q", body)
	}
	if resp.Header.Get("X-Tunnel-Transport") != "h2" {
		t.Errorf("expected X-Tunnel-Transport header 'h2', got %q", resp.Header.Get("X-Tunnel-Transport"))
	}
}

func signTestJWT(t *testing.T, iss, sub, aud string, exp time.Time, key *rsa.PrivateKey, kid string) string {
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
		"aud": []string{aud},
		"exp": exp.Unix(),
	}
	payloadJSON, _ := json.Marshal(payload)
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)

	toSign := hdrB64 + "." + payloadB64
	hashed := sha256.Sum256([]byte(toSign))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, hashed[:])
	if err != nil {
		t.Fatalf("rsa.SignPKCS1v15 failed: %v", err)
	}
	return toSign + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func TestTokenAuthentication_ProjectedVolumeAndRotationIntegration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "snigateway-token-integration-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 1. Generate RSA key for JWKS and tokens
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	issuerURL := "https://kubernetes.default.svc"
	nBytes := rsaKey.N.Bytes()
	eBytes := big.NewInt(int64(rsaKey.E)).Bytes()
	jwksDoc := map[string]any{
		"keys": []any{
			map[string]any{
				"kty": "RSA",
				"kid": "key-1",
				"use": "sig",
				"alg": "RS256",
				"n":   base64.RawURLEncoding.EncodeToString(nBytes),
				"e":   base64.RawURLEncoding.EncodeToString(eBytes),
			},
		},
	}
	jwksJSON, err := json.Marshal(jwksDoc)
	if err != nil {
		t.Fatal(err)
	}

	jwksFilePath := filepath.Join(tempDir, "jwks.json")
	if err := os.WriteFile(jwksFilePath, jwksJSON, 0644); err != nil {
		t.Fatal(err)
	}

	// 2. Generate server TLS certs
	certsBundle, err := certs.GenerateAll("snigateway.internal", "unused-client")
	if err != nil {
		t.Fatal(err)
	}

	jwksCache, err := jwt.NewJWKSCache([]jwt.IssuerConfig{
		{IssuerURL: issuerURL, JWKSFile: jwksFilePath},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer jwksCache.Close()

	verifier := jwt.NewVerifier(jwksCache, jwt.WithAudience("snigateway"))

	subjRule, err := frontend.ParseSubjectRule("system:serviceaccount:default:test-sa=service.example.com")
	if err != nil {
		t.Fatal(err)
	}
	authorizer := frontend.NewSubjectAuthorizer([]frontend.SubjectRule{subjRule})

	serverTLS, err := certs.NewServerTLSConfig(certsBundle.CA.CertPEM, certsBundle.Server.CertPEM, certsBundle.Server.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	serverTLS.ClientAuth = tls.VerifyClientCertIfGiven

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	frontendAddr := ln.Addr().String()

	frontendUDP, err := net.ListenPacket("udp", frontendAddr)
	if err != nil {
		t.Fatal(err)
	}

	frontendServer, err := frontend.NewServer(frontend.ServerConfig{
		ServerTLSConfig:     serverTLS,
		InternalHostname:    "snigateway.internal",
		Authorizer:          authorizer,
		JWTVerifier:         verifier,
		LeaseReaperInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer frontendServer.Close()

	go func() {
		_ = frontendServer.ServeAll([]net.Listener{ln}, []net.PacketConn{frontendUDP})
	}()

	// 3. Write initial token to projected token file
	tokenFilePath := filepath.Join(tempDir, "token")
	now := time.Now()
	exp1 := now.Add(1 * time.Hour)
	token1 := signTestJWT(t, issuerURL, "system:serviceaccount:default:test-sa", "snigateway", exp1, rsaKey, "key-1")
	if err := os.WriteFile(tokenFilePath, []byte(token1), 0600); err != nil {
		t.Fatal(err)
	}

	// 4. Client configured with CA cert only (no client cert) + token file
	clientTLS, err := certs.NewClientTLSConfig(certsBundle.CA.CertPEM, nil, nil, "snigateway.internal")
	if err != nil {
		t.Fatal(err)
	}

	c := client.NewClient(frontendAddr, clientTLS, client.WithTokenFile(tokenFilePath))
	mgr := tunnel.NewManager(c, tunnel.WithBackoff(50*time.Millisecond, 200*time.Millisecond))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go func() {
		_ = mgr.Run(ctx)
	}()

	// 5. Start HTTPS backend on mgr.Listener()
	backendCert, err := generateBackendCert("service.example.com")
	if err != nil {
		t.Fatal(err)
	}

	backendLis := tls.NewListener(mgr.Listener(), &tls.Config{
		Certificates: []tls.Certificate{backendCert},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/token-test", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, "Hello via token-authenticated tunnel!")
	})
	httpServer := &http.Server{Handler: mux}
	defer httpServer.Close()
	go func() {
		_ = httpServer.Serve(backendLis)
	}()

	mgr.UpdateHostnames([]string{"service.example.com"})

	// Verify request works
	sendRequest := func() (*http.Response, string, error) {
		clientTransport := &http.Transport{
			TLSClientConfig: &tls.Config{
				ServerName:         "service.example.com",
				InsecureSkipVerify: true,
			},
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.Dial(network, frontendAddr)
			},
		}
		httpClient := &http.Client{Transport: clientTransport, Timeout: 5 * time.Second}
		resp, err := httpClient.Get("https://service.example.com/token-test")
		if err != nil {
			return nil, "", err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		return resp, string(body), err
	}

	deadline := time.Now().Add(5 * time.Second)
	var resp *http.Response
	var body string
	for time.Now().Before(deadline) {
		resp, body, err = sendRequest()
		if err == nil && resp.StatusCode == http.StatusOK {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("first request failed: err=%v, status=%v", err, resp)
	}
	if body != "Hello via token-authenticated tunnel!" {
		t.Fatalf("unexpected body: %q", body)
	}

	// 6. Simulate Kubelet rotating the token file
	exp2 := time.Now().Add(2 * time.Hour)
	token2 := signTestJWT(t, issuerURL, "system:serviceaccount:default:test-sa", "snigateway", exp2, rsaKey, "key-1")
	if err := os.WriteFile(tokenFilePath, []byte(token2), 0600); err != nil {
		t.Fatal(err)
	}

	// Trigger registration or dial tunnel with rotated token
	mgr.UpdateHostnames([]string{"service.example.com"})
	time.Sleep(100 * time.Millisecond)

	// Second request after token rotation
	resp, body, err = sendRequest()
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("request after token rotation failed: err=%v, status=%v", err, resp)
	}
	if body != "Hello via token-authenticated tunnel!" {
		t.Fatalf("unexpected body after token rotation: %q", body)
	}
}

// TestTunnelListenerHTTPSIntegration_WireGuard verifies end-to-end registration,
// WireGuard key exchange, and HTTPS requests through the tunnel listener over
// --tunnel-transport=wireguard, confirming PROXY header client address preservation.
func TestTunnelListenerHTTPSIntegration_WireGuard(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "snigateway-wg-integration-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	if err := certs.GenerateAndWriteCertificates(tempDir, "snigateway.internal", "client-test"); err != nil {
		t.Fatalf("GenerateAndWriteCertificates failed: %v", err)
	}

	caCertPEM, err := os.ReadFile(filepath.Join(tempDir, "ca.crt"))
	if err != nil {
		t.Fatalf("reading ca.crt: %v", err)
	}
	serverCertPEM, err := os.ReadFile(filepath.Join(tempDir, "server.crt"))
	if err != nil {
		t.Fatalf("reading server.crt: %v", err)
	}
	serverKeyPEM, err := os.ReadFile(filepath.Join(tempDir, "server.key"))
	if err != nil {
		t.Fatalf("reading server.key: %v", err)
	}
	clientCertPEM, err := os.ReadFile(filepath.Join(tempDir, "client.crt"))
	if err != nil {
		t.Fatalf("reading client.crt: %v", err)
	}
	clientKeyPEM, err := os.ReadFile(filepath.Join(tempDir, "client.key"))
	if err != nil {
		t.Fatalf("reading client.key: %v", err)
	}

	serverTLS, err := certs.NewServerTLSConfig(caCertPEM, serverCertPEM, serverKeyPEM)
	if err != nil {
		t.Fatalf("NewServerTLSConfig failed: %v", err)
	}

	frontendListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	defer frontendListener.Close()
	frontendAddr := frontendListener.Addr().String()

	frontendUDP, err := net.ListenPacket("udp", frontendAddr)
	if err != nil {
		t.Fatalf("ListenPacket failed: %v", err)
	}
	defer frontendUDP.Close()

	frontendServer, err := frontend.NewServer(frontend.ServerConfig{
		ServerTLSConfig:     serverTLS,
		InternalHostname:    "snigateway.internal",
		ConnectTimeout:      5 * time.Second,
		ReadSNITimeout:      2 * time.Second,
		WireGuardListenAddr: "127.0.0.1:0",
		WireGuardPrefix:     "fd00::/64",
	})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer frontendServer.Close()

	go func() {
		_ = frontendServer.ServeAll([]net.Listener{frontendListener}, []net.PacketConn{frontendUDP})
	}()

	clientTLS, err := certs.NewClientTLSConfig(caCertPEM, clientCertPEM, clientKeyPEM, "snigateway.internal")
	if err != nil {
		t.Fatalf("NewClientTLSConfig failed: %v", err)
	}

	c := client.NewClient(frontendAddr, clientTLS)
	mgr := tunnel.NewManager(c,
		tunnel.WithTransport(tunnel.TransportModeWireGuard),
		tunnel.WithBackoff(50*time.Millisecond, 200*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go func() {
		_ = mgr.Run(ctx)
	}()

	backendCert, err := generateBackendCert("service.example.com", "updated.example.com")
	if err != nil {
		t.Fatalf("generateBackendCert failed: %v", err)
	}

	tlsLis := tls.NewListener(mgr.Listener(), &tls.Config{
		Certificates: []tls.Certificate{backendCert},
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Custom-Header", "WireGuardTunnel")
		clientIP, _, _ := net.SplitHostPort(r.RemoteAddr)
		w.Header().Set("X-Real-Client-IP", clientIP)
		_, _ = fmt.Fprintf(w, "Hello from backend via WireGuard for host: %s, path: %s", r.Host, r.URL.Path)
	})

	httpServer := &http.Server{Handler: mux}
	defer httpServer.Close()

	go func() {
		_ = httpServer.Serve(tlsLis)
	}()

	mgr.UpdateHostnames([]string{"service.example.com"})

	sendRequest := func(sniHost string, hostHeader string, path string) (*http.Response, string, error) {
		clientTransport := &http.Transport{
			TLSClientConfig: &tls.Config{
				ServerName:         sniHost,
				InsecureSkipVerify: true,
			},
			DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return tls.Dial("tcp", frontendAddr, &tls.Config{
					ServerName:         sniHost,
					InsecureSkipVerify: true,
				})
			},
		}
		httpClient := &http.Client{
			Transport: clientTransport,
			Timeout:   3 * time.Second,
		}

		req, err := http.NewRequest(http.MethodGet, "https://"+sniHost+path, nil)
		if err != nil {
			return nil, "", err
		}
		if hostHeader != "" {
			req.Host = hostHeader
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, "", err
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, "", err
		}
		return resp, string(body), nil
	}

	t.Run("End-to-End HTTPS through WireGuard tunnel", func(t *testing.T) {
		var resp *http.Response
		var body string
		var err error

		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			resp, body, err = sendRequest("service.example.com", "service.example.com", "/hello")
			if err == nil && resp.StatusCode == http.StatusOK {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}

		if err != nil {
			t.Fatalf("failed to perform HTTPS request over WireGuard: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", resp.StatusCode)
		}
		if gotHeader := resp.Header.Get("X-Custom-Header"); gotHeader != "WireGuardTunnel" {
			t.Fatalf("expected header 'WireGuardTunnel', got %q", gotHeader)
		}
		if clientIP := resp.Header.Get("X-Real-Client-IP"); clientIP != "127.0.0.1" {
			t.Fatalf("expected real client IP 127.0.0.1 preserved via PROXY protocol, got %q", clientIP)
		}
		expectedBody := "Hello from backend via WireGuard for host: service.example.com, path: /hello"
		if body != expectedBody {
			t.Fatalf("got body %q, want %q", body, expectedBody)
		}
	})

	t.Run("Dynamic hostname update with WireGuard transport", func(t *testing.T) {
		mgr.UpdateHostnames([]string{"updated.example.com"})

		var resp *http.Response
		var body string
		var err error

		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			resp, body, err = sendRequest("updated.example.com", "updated.example.com", "/hello")
			if err == nil && resp.StatusCode == http.StatusOK {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}

		if err != nil {
			t.Fatalf("failed to perform HTTPS request to updated hostname: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", resp.StatusCode)
		}
		expectedBody := "Hello from backend via WireGuard for host: updated.example.com, path: /hello"
		if body != expectedBody {
			t.Fatalf("got body %q, want %q", body, expectedBody)
		}

		// Verify old hostname is rejected
		resp, _, err = sendRequest("service.example.com", "service.example.com", "/hello")
		if err == nil && resp.StatusCode == http.StatusOK {
			t.Fatalf("expected old hostname to be rejected, got status %d", resp.StatusCode)
		}
	})
}

// TestWireGuard_PeerLifecycleIntegration verifies:
// 1. Peer removed from frontend device when session closes.
// 2. Key exchange for session owned by a different identity gets 403.
// 3. Two sessions never share an address.
func TestWireGuard_PeerLifecycleIntegration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "snigateway-wg-lifecycle-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	genA, err := certs.GenerateAll("snigateway.internal", "client-A")
	if err != nil {
		t.Fatalf("GenerateAll client-A failed: %v", err)
	}
	genB, err := certs.GenerateAll("snigateway.internal", "client-B")
	if err != nil {
		t.Fatalf("GenerateAll client-B failed: %v", err)
	}

	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(genA.CA.CertPEM)
	caPool.AppendCertsFromPEM(genB.CA.CertPEM)

	serverTLS, err := certs.NewServerTLSConfigWithCertPool(caPool, genA.Server.CertPEM, genA.Server.KeyPEM)
	if err != nil {
		t.Fatalf("NewServerTLSConfigWithCertPool failed: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	defer ln.Close()
	frontendAddr := ln.Addr().String()

	frontendServer, err := frontend.NewServer(frontend.ServerConfig{
		ServerTLSConfig:     serverTLS,
		InternalHostname:    "snigateway.internal",
		WireGuardListenAddr: "127.0.0.1:0",
		WireGuardPrefix:     "fd00::/64",
	})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer frontendServer.Close()

	go func() {
		_ = frontendServer.Serve(ln)
	}()

	clientTLSA, err := certs.NewClientTLSConfig(genA.CA.CertPEM, genA.Client.CertPEM, genA.Client.KeyPEM, "snigateway.internal")
	if err != nil {
		t.Fatalf("NewClientTLSConfig A: %v", err)
	}
	clientTLSB, err := certs.NewClientTLSConfig(genA.CA.CertPEM, genB.Client.CertPEM, genB.Client.KeyPEM, "snigateway.internal")
	if err != nil {
		t.Fatalf("NewClientTLSConfig B: %v", err)
	}

	clientA := client.NewClient(frontendAddr, clientTLSA)
	clientB := client.NewClient(frontendAddr, clientTLSB)

	// 1. Client A starts session 1
	sess1Ctx, sess1Cancel := context.WithCancel(t.Context())
	defer sess1Cancel()

	sess1ID, _, err := clientA.StartSession(sess1Ctx)
	if err != nil {
		t.Fatalf("clientA.StartSession failed: %v", err)
	}

	pubKeyA := "1111111111111111111111111111111111111111111="
	resp1, err := clientA.WireGuardExchange(sess1Ctx, sess1ID, pubKeyA)
	if err != nil {
		t.Fatalf("clientA.WireGuardExchange failed: %v", err)
	}

	// Verify peer exists in frontend WireGuard device
	out1, err := frontendServer.WireGuardTransport().Device().IpcGet()
	if err != nil {
		t.Fatalf("IpcGet failed: %v", err)
	}
	rawPubA, _ := base64.StdEncoding.DecodeString(pubKeyA)
	pubAHex := fmt.Sprintf("%x", rawPubA)
	if !strings.Contains(out1, pubAHex) {
		t.Fatalf("expected frontend device to have peer %s, got:\n%s", pubAHex, out1)
	}

	// 2. Client B attempts key exchange for session 1 owned by Client A: gets 403 Forbidden
	_, err = clientB.WireGuardExchange(t.Context(), sess1ID, "2222222222222222222222222222222222222222222=")
	if err == nil {
		t.Fatal("expected key exchange by different identity to fail, got nil")
	}
	if !strings.Contains(err.Error(), "403") {
		t.Fatalf("expected 403 Forbidden, got: %v", err)
	}

	// 3. Client B starts session 2, performs key exchange: two sessions never share an address
	sess2Ctx, sess2Cancel := context.WithCancel(t.Context())
	defer sess2Cancel()

	sess2ID, _, err := clientB.StartSession(sess2Ctx)
	if err != nil {
		t.Fatalf("clientB.StartSession failed: %v", err)
	}

	pubKeyB := "3333333333333333333333333333333333333333333="
	resp2, err := clientB.WireGuardExchange(sess2Ctx, sess2ID, pubKeyB)
	if err != nil {
		t.Fatalf("clientB.WireGuardExchange failed: %v", err)
	}

	if resp1.PeerAddress == resp2.PeerAddress {
		t.Fatalf("expected distinct peer addresses, both got %q", resp1.PeerAddress)
	}

	// 4. Session 1 closes: peer removed from frontend device
	sess1Cancel()

	// Wait for peer removal
	deadline := time.Now().Add(2 * time.Second)
	var peerRemoved bool
	for time.Now().Before(deadline) {
		out, err := frontendServer.WireGuardTransport().Device().IpcGet()
		if err == nil && !strings.Contains(out, pubAHex) {
			peerRemoved = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !peerRemoved {
		out, _ := frontendServer.WireGuardTransport().Device().IpcGet()
		t.Fatalf("expected peer %s to be removed from device after session close, got:\n%s", pubAHex, out)
	}
}

// TestWireGuard_HandshakeTimeout verifies:
// When the UDP endpoint is unreachable, the controller reports the failure
// within the bound and does not leave a peer behind on the frontend.
func TestWireGuard_HandshakeTimeout(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "snigateway-wg-timeout-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	if err := certs.GenerateAndWriteCertificates(tempDir, "snigateway.internal", "client-test"); err != nil {
		t.Fatalf("GenerateAndWriteCertificates failed: %v", err)
	}

	caCertPEM, _ := os.ReadFile(filepath.Join(tempDir, "ca.crt"))
	serverCertPEM, _ := os.ReadFile(filepath.Join(tempDir, "server.crt"))
	serverKeyPEM, _ := os.ReadFile(filepath.Join(tempDir, "server.key"))
	clientCertPEM, _ := os.ReadFile(filepath.Join(tempDir, "client.crt"))
	clientKeyPEM, _ := os.ReadFile(filepath.Join(tempDir, "client.key"))

	serverTLS, err := certs.NewServerTLSConfig(caCertPEM, serverCertPEM, serverKeyPEM)
	if err != nil {
		t.Fatalf("NewServerTLSConfig failed: %v", err)
	}

	frontendListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	defer frontendListener.Close()
	frontendAddr := frontendListener.Addr().String()

	// Frontend configured with unreachable endpoint advertised to clients
	frontendServer, err := frontend.NewServer(frontend.ServerConfig{
		ServerTLSConfig:     serverTLS,
		InternalHostname:    "snigateway.internal",
		WireGuardListenAddr: "127.0.0.1:0",
		WireGuardEndpoint:   "127.0.0.1:59999", // Unreachable port
		WireGuardPrefix:     "fd00::/64",
	})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer frontendServer.Close()

	go func() {
		_ = frontendServer.Serve(frontendListener)
	}()

	clientTLS, err := certs.NewClientTLSConfig(caCertPEM, clientCertPEM, clientKeyPEM, "snigateway.internal")
	if err != nil {
		t.Fatalf("NewClientTLSConfig failed: %v", err)
	}

	c := client.NewClient(frontendAddr, clientTLS)
	handshakeTimeout := 500 * time.Millisecond
	mgr := tunnel.NewManager(c,
		tunnel.WithTransport(tunnel.TransportModeWireGuard),
		tunnel.WithWireGuardHandshakeTimeout(handshakeTimeout),
		tunnel.WithBackoff(50*time.Millisecond, 100*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	go func() {
		_ = mgr.Run(ctx)
	}()

	// Since endpoint is unreachable, manager should never become connected.
	// Poll to confirm it remains disconnected past the handshake timeout.
	deadline := time.Now().Add(handshakeTimeout + 500*time.Millisecond)
	for time.Now().Before(deadline) {
		if mgr.IsConnected() {
			t.Fatal("expected manager not to be connected with unreachable endpoint")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Stop manager
	cancel()

	// Poll until no peer remains on the frontend device
	cleanDeadline := time.Now().Add(2 * time.Second)
	var peerCleaned bool
	for time.Now().Before(cleanDeadline) {
		out, err := frontendServer.WireGuardTransport().Device().IpcGet()
		if err == nil && !strings.Contains(out, "public_key=") {
			peerCleaned = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !peerCleaned {
		out, _ := frontendServer.WireGuardTransport().Device().IpcGet()
		t.Fatalf("expected no peer left on frontend device after timeout, got:\n%s", out)
	}
}
