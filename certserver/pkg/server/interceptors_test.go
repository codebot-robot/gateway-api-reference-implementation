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

package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	certserverv1 "github.com/gke-labs/gateway-api-reference-implementation/certserver/api/v1"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/audit"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/cache"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/manager"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/registry"
)

func createTestCertificate(t *testing.T, hostname string, keyOverride []byte) *cache.Certificate {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"test"},
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:              []string{hostname},
		BasicConstraintsValid: true,
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("failed to create cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(derBytes)
	if err != nil {
		t.Fatalf("failed to parse cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	if len(keyOverride) > 0 {
		keyPEM = keyOverride
	}
	return cache.NewCertificate([]string{hostname}, hostname, certPEM, keyPEM, leaf, time.Now(), time.Now().Add(24*time.Hour))
}

type interceptorTestHarness struct {
	server       *Server
	grpcClient   certserverv1.CertificateServiceClient
	conn         *grpc.ClientConn
	auditBuf     *bytes.Buffer
	clusterKey   *rsa.PrivateKey
	kid          string
	issuerURL    string
	jwksServer   *httptest.Server
	reg          *registry.Registry
	bindingStore registry.BindingStore
	diskCache    *cache.DiskCache
}

func setupInterceptorTestHarness(t *testing.T) *interceptorTestHarness {
	t.Helper()

	clusterKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate cluster RSA key: %v", err)
	}
	kid := "test-key-1"

	jwksServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(buildTestJWKS(map[string]*rsa.PublicKey{kid: &clusterKey.PublicKey}))
	}))
	t.Cleanup(jwksServer.Close)

	clusterID := "test-cluster"
	issuerURL := "https://kubernetes.default.svc"
	reg, err := registry.NewRegistry([]registry.ClusterConfig{
		{
			ClusterID:                clusterID,
			IssuerURL:                issuerURL,
			JWKSURL:                  jwksServer.URL,
			ControllerNamespace:      "gateway-system",
			ControllerServiceAccount: "gari-controller",
			AllowedHostnames:         []string{"*.example.com", "example.com"},
		},
	}, registry.WithHTTPClient(jwksServer.Client()))
	if err != nil {
		t.Fatalf("failed to create registry: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })

	var auditBuf bytes.Buffer
	auditLogger := registry.NewAuditLogger(&auditBuf)
	bindingStore := registry.NewMemoryBindingStore()

	tempDir := t.TempDir()
	diskCache, err := cache.NewDiskCache(tempDir)
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}

	opts := Options{
		ListenAddr:        "127.0.0.1:0",
		TLSConfig:         generateSelfSignedCert(t),
		ReleaseMode:       true,
		MaxHostnames:      10,
		MaxHostnameLength: 253,
		RateLimitQPS:      100,
		RateLimitBurst:    100,
	}

	mgr := manager.NewManager(nil, diskCache, nil, nil)
	srv, err := NewServer(opts, reg, mgr, bindingStore, auditLogger)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	if err := srv.Start(t.Context()); err != nil {
		t.Fatalf("srv.Start failed: %v", err)
	}
	t.Cleanup(srv.Stop)

	creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})
	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatalf("failed to connect to gRPC server: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := certserverv1.NewCertificateServiceClient(conn)

	return &interceptorTestHarness{
		server:       srv,
		grpcClient:   client,
		conn:         conn,
		auditBuf:     &auditBuf,
		clusterKey:   clusterKey,
		kid:          kid,
		issuerURL:    issuerURL,
		jwksServer:   jwksServer,
		reg:          reg,
		bindingStore: bindingStore,
		diskCache:    diskCache,
	}
}

// 1. Tests: an unauthenticated RPC is rejected generically and audited as refused, with the reason
func TestInterceptors_UnauthenticatedRPCRejectedAndAuditedAsRefused(t *testing.T) {
	h := setupInterceptorTestHarness(t)

	req := &certserverv1.GetCertificateRequest{
		Gateway:                 "default/gw-1",
		DataPlaneServiceAccount: "default/dp-sa-1",
		Hostnames:               []string{"test.example.com"},
	}

	// Subtest A: Missing token
	t.Run("missing_token", func(t *testing.T) {
		h.auditBuf.Reset()

		_, err := h.grpcClient.GetCertificate(t.Context(), req)
		if err == nil {
			t.Fatalf("expected unauthenticated call to fail")
		}

		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.Unauthenticated {
			t.Fatalf("expected codes.Unauthenticated, got: %v", err)
		}
		if st.Message() != "authentication failed" {
			t.Errorf("expected generic error message 'authentication failed', got %q", st.Message())
		}

		lines := strings.Split(strings.TrimSpace(h.auditBuf.String()), "\n")
		if len(lines) != 1 {
			t.Fatalf("expected exactly 1 audit line, got %d: %s", len(lines), h.auditBuf.String())
		}

		var entry registry.AuditEntry
		if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
			t.Fatalf("failed to unmarshal audit entry: %v", err)
		}

		if entry.Decision != registry.DecisionRefused {
			t.Errorf("expected decision refused, got %s", entry.Decision)
		}
		if entry.Reason != "missing authorization token" {
			t.Errorf("expected reason 'missing authorization token', got %q", entry.Reason)
		}
		if entry.StatusCode != "Unauthenticated" {
			t.Errorf("expected status code 'Unauthenticated', got %q", entry.StatusCode)
		}
		if entry.Method != "/certserver.v1.CertificateService/GetCertificate" {
			t.Errorf("expected method '/certserver.v1.CertificateService/GetCertificate', got %q", entry.Method)
		}
		if entry.Duration <= 0 {
			t.Errorf("expected positive duration, got %v", entry.Duration)
		}
	})

	// Subtest B: Invalid / malformed token
	t.Run("invalid_token", func(t *testing.T) {
		h.auditBuf.Reset()

		badCtx := metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer malformed.bad.jwt")
		_, err := h.grpcClient.GetCertificate(badCtx, req)
		if err == nil {
			t.Fatalf("expected call with invalid token to fail")
		}

		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.Unauthenticated {
			t.Fatalf("expected codes.Unauthenticated, got: %v", err)
		}
		if st.Message() != "authentication failed" {
			t.Errorf("expected generic error message 'authentication failed', got %q", st.Message())
		}

		lines := strings.Split(strings.TrimSpace(h.auditBuf.String()), "\n")
		if len(lines) != 1 {
			t.Fatalf("expected exactly 1 audit line, got %d: %s", len(lines), h.auditBuf.String())
		}

		var entry registry.AuditEntry
		if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
			t.Fatalf("failed to unmarshal audit entry: %v", err)
		}

		if entry.Decision != registry.DecisionRefused {
			t.Errorf("expected decision refused, got %s", entry.Decision)
		}
		if !strings.Contains(entry.Reason, "token verification failed") {
			t.Errorf("expected reason to contain 'token verification failed', got %q", entry.Reason)
		}
		if entry.StatusCode != "Unauthenticated" {
			t.Errorf("expected status code 'Unauthenticated', got %q", entry.StatusCode)
		}
	})
}

// 2. Tests: a successful GetCertificate is audited once, with the handler's fields
func TestInterceptors_SuccessfulGetCertificateAuditedOnceWithFields(t *testing.T) {
	h := setupInterceptorTestHarness(t)

	// Pre-populate cache with a certificate
	testHost := "valid.example.com"
	certData := createTestCertificate(t, testHost, nil)
	if err := h.diskCache.Put(certData); err != nil {
		t.Fatalf("failed to put certificate in cache: %v", err)
	}

	// Pre-record validated binding
	h.bindingStore.RecordValidatedBinding(registry.ValidatedBinding{
		ClusterID:    "test-cluster",
		Gateway:      "default/gw-1",
		HostnamesKey: testHost,
		ValidatedAt:  time.Now(),
		ExpiresAt:    time.Now().Add(1 * time.Hour),
	})

	token := signControllerToken(t, h.issuerURL, "system:serviceaccount:gateway-system:gari-controller", h.clusterKey, h.kid)
	callCtx := metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer "+token)

	h.auditBuf.Reset()

	req := &certserverv1.GetCertificateRequest{
		Gateway:                 "default/gw-1",
		DataPlaneServiceAccount: "default/dp-sa-1",
		Hostnames:               []string{testHost},
		ClusterId:               "test-cluster",
	}

	resp, err := h.grpcClient.GetCertificate(callCtx, req)
	if err != nil {
		t.Fatalf("GetCertificate failed: %v", err)
	}
	if len(resp.CertificateChain) == 0 {
		t.Fatalf("empty certificate chain returned")
	}

	lines := strings.Split(strings.TrimSpace(h.auditBuf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected exactly 1 audit line, got %d: %s", len(lines), h.auditBuf.String())
	}

	var entry registry.AuditEntry
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("failed to unmarshal audit entry: %v", err)
	}

	if entry.Method != "/certserver.v1.CertificateService/GetCertificate" {
		t.Errorf("unexpected method: %s", entry.Method)
	}
	if entry.StatusCode != "OK" {
		t.Errorf("unexpected status code: %s", entry.StatusCode)
	}
	if entry.Decision != registry.DecisionAllowed {
		t.Errorf("unexpected decision: %s", entry.Decision)
	}
	if entry.Gateway != "default/gw-1" {
		t.Errorf("unexpected gateway: %s", entry.Gateway)
	}
	if len(entry.Hostnames) != 1 || entry.Hostnames[0] != testHost {
		t.Errorf("unexpected hostnames: %v", entry.Hostnames)
	}
	if entry.Reason != "certificate returned from cache" {
		t.Errorf("unexpected reason: %s", entry.Reason)
	}
	if entry.Identity.ClusterID != "test-cluster" || entry.Identity.ServiceAccount != "gari-controller" {
		t.Errorf("unexpected identity: %+v", entry.Identity)
	}
	if entry.Duration <= 0 {
		t.Errorf("expected positive duration, got %v", entry.Duration)
	}
}

// 3. Tests: a handler error, and a handler panic, each produce exactly one audit line
func TestInterceptors_HandlerErrorAndPanicProduceExactlyOneAuditLine(t *testing.T) {
	// Subtest A: Handler error produces exactly one audit line
	t.Run("handler_error", func(t *testing.T) {
		var buf bytes.Buffer
		logger := registry.NewAuditLogger(&buf)
		interceptor := AuditUnaryInterceptor(logger)

		handlerErr := status.Errorf(codes.InvalidArgument, "invalid argument provided")
		handler := func(ctx context.Context, req any) (any, error) {
			audit.FromContext(ctx).Set("test-gw", []string{"foo.com"}, registry.DecisionRefused, "argument invalid")
			return nil, handlerErr
		}

		info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/ErrorMethod"}
		_, err := interceptor(t.Context(), "req", info, handler)
		if err != handlerErr {
			t.Fatalf("expected handler error, got %v", err)
		}

		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		if len(lines) != 1 {
			t.Fatalf("expected exactly 1 audit line, got %d: %s", len(lines), buf.String())
		}

		var entry registry.AuditEntry
		if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
			t.Fatalf("failed to unmarshal audit entry: %v", err)
		}

		if entry.StatusCode != "InvalidArgument" {
			t.Errorf("expected status code InvalidArgument, got %s", entry.StatusCode)
		}
		if entry.Decision != registry.DecisionRefused {
			t.Errorf("expected decision refused, got %s", entry.Decision)
		}
		if entry.Reason != "argument invalid" {
			t.Errorf("expected reason 'argument invalid', got %s", entry.Reason)
		}
		if entry.Gateway != "test-gw" {
			t.Errorf("expected gateway 'test-gw', got %s", entry.Gateway)
		}
	})

	// Subtest B: Handler panic produces exactly one audit line
	t.Run("handler_panic", func(t *testing.T) {
		var buf bytes.Buffer
		logger := registry.NewAuditLogger(&buf)
		interceptor := AuditUnaryInterceptor(logger)

		handler := func(ctx context.Context, req any) (any, error) {
			audit.FromContext(ctx).Set("panic-gw", []string{"panic.com"}, registry.DecisionRefused, "pre-panic annotation")
			panic("unexpected handler panic")
		}

		info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/PanicMethod"}

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic to propagate")
			}

			lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
			if len(lines) != 1 {
				t.Fatalf("expected exactly 1 audit line on panic, got %d: %s", len(lines), buf.String())
			}

			var entry registry.AuditEntry
			if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
				t.Fatalf("failed to unmarshal audit entry: %v", err)
			}

			if entry.StatusCode != "Internal" {
				t.Errorf("expected status code Internal, got %s", entry.StatusCode)
			}
			if entry.Decision != registry.DecisionRefused {
				t.Errorf("expected decision refused, got %s", entry.Decision)
			}
			if entry.Reason != "panic" {
				t.Errorf("expected reason 'panic', got %s", entry.Reason)
			}
			if entry.Gateway != "panic-gw" {
				t.Errorf("expected gateway panic-gw, got %s", entry.Gateway)
			}
		}()

		_, _ = interceptor(t.Context(), "req", info, handler)
	})
}

// 4. Tests: audit output never contains key material or tokens (check with a fixture key)
func TestInterceptors_AuditOutputNeverContainsKeyMaterialOrTokens(t *testing.T) {
	h := setupInterceptorTestHarness(t)

	const fixturePrivateKey = "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA0FixturePrivateKeySecret1234567890\n-----END RSA PRIVATE KEY-----\n"

	testHost := "secure.example.com"
	certData := createTestCertificate(t, testHost, []byte(fixturePrivateKey))
	if err := h.diskCache.Put(certData); err != nil {
		t.Fatalf("failed to put certificate in cache: %v", err)
	}

	h.bindingStore.RecordValidatedBinding(registry.ValidatedBinding{
		ClusterID:    "test-cluster",
		Gateway:      "default/secure-gw",
		HostnamesKey: testHost,
		ValidatedAt:  time.Now(),
		ExpiresAt:    time.Now().Add(1 * time.Hour),
	})

	fixtureToken := signControllerToken(t, h.issuerURL, "system:serviceaccount:gateway-system:gari-controller", h.clusterKey, h.kid)
	callCtx := metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer "+fixtureToken)

	h.auditBuf.Reset()

	req := &certserverv1.GetCertificateRequest{
		Gateway:                 "default/secure-gw",
		DataPlaneServiceAccount: "default/dp-sa-1",
		Hostnames:               []string{testHost},
		ClusterId:               "test-cluster",
		Token:                   fixtureToken, // also passed in request field to check redaction
	}

	resp, err := h.grpcClient.GetCertificate(callCtx, req)
	if err != nil {
		t.Fatalf("GetCertificate failed: %v", err)
	}

	if string(resp.PrivateKey) != fixturePrivateKey {
		t.Fatalf("expected response to contain fixture private key")
	}

	// Inspect audit log
	capturedLogs := h.auditBuf.String()
	if strings.Contains(capturedLogs, fixturePrivateKey) {
		t.Errorf("CRITICAL: audit log leaked fixture private key!")
	}
	if strings.Contains(capturedLogs, "0FixturePrivateKeySecret1234567890") {
		t.Errorf("CRITICAL: audit log leaked fixture private key fragment!")
	}
	if strings.Contains(capturedLogs, fixtureToken) {
		t.Errorf("CRITICAL: audit log leaked fixture bearer token!")
	}

	lines := strings.Split(strings.TrimSpace(capturedLogs), "\n")
	for i, line := range lines {
		var parsed map[string]any
		if err := json.Unmarshal([]byte(line), &parsed); err != nil {
			t.Fatalf("line %d invalid JSON: %v", i, err)
		}

		forbiddenKeys := []string{"token", "private_key", "key_pem", "secret", "certificate_chain"}
		for _, k := range forbiddenKeys {
			if _, found := parsed[k]; found {
				t.Errorf("line %d contained forbidden field %q", i, k)
			}
		}
	}
}

// 5. Tests: the HTTP middleware writes the same format
func TestInterceptors_HTTPMiddlewareWritesSameFormat(t *testing.T) {
	var gRPCBuf bytes.Buffer
	gRPCLogger := registry.NewAuditLogger(&gRPCBuf)
	unaryInterceptor := AuditUnaryInterceptor(gRPCLogger)

	// Simulate gRPC call
	info := &grpc.UnaryServerInfo{FullMethod: "/certserver.v1.CertificateService/GetCertificate"}
	_, _ = unaryInterceptor(t.Context(), "req", info, func(ctx context.Context, req any) (any, error) {
		rec := audit.FromContext(ctx)
		rec.SetIdentity(registry.CallerIdentity{
			ClusterID:      "test-cluster",
			Namespace:      "test-ns",
			ServiceAccount: "test-sa",
		})
		rec.Set("default/test-gw", []string{"http.example.com"}, registry.DecisionAllowed, "grpc success")
		return "resp", nil
	})

	var httpBuf bytes.Buffer
	httpLogger := audit.NewLogger(&httpBuf)
	httpHandler := audit.WrapHTTP(httpLogger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := audit.FromContext(r.Context())
		rec.SetIdentity(registry.CallerIdentity{
			ClusterID:      "test-cluster",
			Namespace:      "test-ns",
			ServiceAccount: "test-sa",
		})
		rec.Set("default/test-gw", []string{"http.example.com"}, registry.DecisionAllowed, "http success")
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/.well-known/acme-challenge/sample", nil)
	rr := httptest.NewRecorder()
	httpHandler.ServeHTTP(rr, req)

	// Compare both logs
	gRPCLine := strings.TrimSpace(gRPCBuf.String())
	httpLine := strings.TrimSpace(httpBuf.String())

	var gRPCEntry registry.AuditEntry
	if err := json.Unmarshal([]byte(gRPCLine), &gRPCEntry); err != nil {
		t.Fatalf("failed to unmarshal gRPC entry: %v", err)
	}

	var httpEntry registry.AuditEntry
	if err := json.Unmarshal([]byte(httpLine), &httpEntry); err != nil {
		t.Fatalf("failed to unmarshal HTTP entry: %v", err)
	}

	// Verify both entries have identical JSON schema keys
	var gRPCMap map[string]any
	json.Unmarshal([]byte(gRPCLine), &gRPCMap)

	var httpMap map[string]any
	json.Unmarshal([]byte(httpLine), &httpMap)

	expectedKeys := []string{"timestamp", "method", "identity", "status_code", "duration", "gateway", "hostnames", "decision", "reason"}
	for _, k := range expectedKeys {
		if _, ok := gRPCMap[k]; !ok {
			t.Errorf("gRPC log missing expected key %q", k)
		}
		if _, ok := httpMap[k]; !ok {
			t.Errorf("HTTP log missing expected key %q", k)
		}
	}

	if gRPCEntry.StatusCode != "OK" || httpEntry.StatusCode != "OK" {
		t.Errorf("expected both status codes to be 'OK', got gRPC=%q HTTP=%q", gRPCEntry.StatusCode, httpEntry.StatusCode)
	}
	if gRPCEntry.Decision != registry.DecisionAllowed || httpEntry.Decision != registry.DecisionAllowed {
		t.Errorf("expected both decisions to be 'allowed', got gRPC=%q HTTP=%q", gRPCEntry.Decision, httpEntry.Decision)
	}
	if gRPCEntry.Duration <= 0 || httpEntry.Duration <= 0 {
		t.Errorf("expected both durations to be > 0, got gRPC=%v HTTP=%v", gRPCEntry.Duration, httpEntry.Duration)
	}
}

// TestStreamInterceptors tests the stream interceptors for audit and auth
func TestStreamInterceptors(t *testing.T) {
	clusterKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate cluster RSA key: %v", err)
	}
	kid := "test-stream-key"
	jwksServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(buildTestJWKS(map[string]*rsa.PublicKey{kid: &clusterKey.PublicKey}))
	}))
	defer jwksServer.Close()

	issuerURL := "https://kubernetes.default.svc"
	reg, err := registry.NewRegistry([]registry.ClusterConfig{
		{
			ClusterID:                "stream-cluster",
			IssuerURL:                issuerURL,
			JWKSURL:                  jwksServer.URL,
			ControllerNamespace:      "gateway-system",
			ControllerServiceAccount: "stream-controller",
		},
	}, registry.WithHTTPClient(jwksServer.Client()))
	if err != nil {
		t.Fatalf("failed to create registry: %v", err)
	}
	defer reg.Close()

	var buf bytes.Buffer
	logger := registry.NewAuditLogger(&buf)

	auditStream := AuditStreamInterceptor(logger)
	authStream := AuthStreamInterceptor(reg)

	// Subtest 1: Stream unauthenticated
	t.Run("unauthenticated_stream", func(t *testing.T) {
		buf.Reset()
		mockSS := &mockServerStream{ctx: t.Context()}
		info := &grpc.StreamServerInfo{FullMethod: "/test.Service/StreamMethod"}

		err := auditStream(nil, mockSS, info, func(srv any, ss grpc.ServerStream) error {
			return authStream(srv, ss, info, func(srv any, ss grpc.ServerStream) error {
				return nil
			})
		})

		if err == nil {
			t.Fatalf("expected stream auth to fail")
		}
		st, _ := status.FromError(err)
		if st.Code() != codes.Unauthenticated {
			t.Errorf("expected Unauthenticated, got %v", st.Code())
		}
		if st.Message() != "authentication failed" {
			t.Errorf("expected 'authentication failed', got %q", st.Message())
		}

		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		if len(lines) != 1 {
			t.Fatalf("expected 1 audit line, got %d: %s", len(lines), buf.String())
		}

		var entry registry.AuditEntry
		json.Unmarshal([]byte(lines[0]), &entry)
		if entry.Decision != registry.DecisionRefused {
			t.Errorf("expected refused, got %s", entry.Decision)
		}
		if entry.Reason != "missing authorization token" {
			t.Errorf("expected 'missing authorization token', got %q", entry.Reason)
		}
	})

	// Subtest 2: Stream authenticated success
	t.Run("authenticated_stream", func(t *testing.T) {
		buf.Reset()
		token := signControllerToken(t, issuerURL, "system:serviceaccount:gateway-system:stream-controller", clusterKey, kid)
		ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer "+token))
		mockSS := &mockServerStream{ctx: ctx}
		info := &grpc.StreamServerInfo{FullMethod: "/test.Service/StreamMethod"}

		err := auditStream(nil, mockSS, info, func(srv any, ss grpc.ServerStream) error {
			return authStream(srv, ss, info, func(srv any, ss grpc.ServerStream) error {
				id, ok := CallerIdentityFromContext(ss.Context())
				if !ok || id.ServiceAccount != "stream-controller" {
					t.Errorf("unexpected identity in stream context: %+v", id)
				}
				audit.FromContext(ss.Context()).Set("stream-gw", []string{"stream.example.com"}, registry.DecisionAllowed, "stream success")
				return nil
			})
		})

		if err != nil {
			t.Fatalf("expected stream to succeed, got %v", err)
		}

		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		if len(lines) != 1 {
			t.Fatalf("expected 1 audit line, got %d: %s", len(lines), buf.String())
		}

		var entry registry.AuditEntry
		json.Unmarshal([]byte(lines[0]), &entry)
		if entry.StatusCode != "OK" {
			t.Errorf("expected OK, got %s", entry.StatusCode)
		}
		if entry.Decision != registry.DecisionAllowed {
			t.Errorf("expected allowed, got %s", entry.Decision)
		}
		if entry.Identity.ServiceAccount != "stream-controller" {
			t.Errorf("expected stream-controller, got %s", entry.Identity.ServiceAccount)
		}
	})
}

type mockServerStream struct {
	ctx context.Context
}

func (m *mockServerStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockServerStream) SendHeader(metadata.MD) error { return nil }
func (m *mockServerStream) SetTrailer(metadata.MD)       {}
func (m *mockServerStream) Context() context.Context     { return m.ctx }
func (m *mockServerStream) SendMsg(any) error            { return nil }
func (m *mockServerStream) RecvMsg(any) error            { return nil }
