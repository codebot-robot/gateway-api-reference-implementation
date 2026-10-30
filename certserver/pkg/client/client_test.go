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

package client

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	acmeclient "github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/acme"
	certserverv1 "github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/api/v1"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/cache"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/challenge"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/manager"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/metrics"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/registry"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/server"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/testserver"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/proxy"
)

func generateSelfSignedCert(t *testing.T) (*tls.Config, []byte, []byte) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate private key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"certserver test"},
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost", "127.0.0.1"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	cert := tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  priv,
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, certPEM, keyPEM
}

func signControllerToken(t *testing.T, iss, sub string, privKey *rsa.PrivateKey, kid string) string {
	t.Helper()
	hBytes, _ := json.Marshal(map[string]any{
		"alg": "RS256",
		"kid": kid,
	})
	now := time.Now()
	pBytes, _ := json.Marshal(map[string]any{
		"iss": iss,
		"sub": sub,
		"aud": registry.CertServerAudience,
		"exp": now.Add(1 * time.Hour).Unix(),
		"nbf": now.Add(-1 * time.Minute).Unix(),
		"iat": now.Unix(),
	})

	input := base64.RawURLEncoding.EncodeToString(hBytes) + "." + base64.RawURLEncoding.EncodeToString(pBytes)
	hashed := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, privKey, crypto.SHA256, hashed[:])
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func buildTestJWKS(keys map[string]*rsa.PublicKey) []byte {
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
			N:   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
			E:   base64.RawURLEncoding.EncodeToString(eBytes),
		})
	}
	data, _ := json.Marshal(doc)
	return data
}

type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// TestClient_RealCertServerWithPebble runs the client against a real certserver
// backed by Pebble ACME and a fake cluster JWKS:
// - Verifies pending on initial cache miss.
// - Background worker fetches the certificate.
// - State recompute trigger is called when the certificate is issued.
// - Subsequent ResolveCertificates call returns the issued certificate.
// - Subsequent calls return the certificate from local in-memory cache.
// - Verifies token rotation: updating the token file is reflected in subsequent calls.
// - Verifies no certificate private keys or bearer tokens are ever logged.
func TestClient_RealCertServerWithPebble(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()

	// Intercept log output to assert no tokens or keys are logged
	var logBuf safeBuffer
	origLogWriter := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(origLogWriter)

	// 1. Start mock cluster JWKS server
	clusterKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate cluster key: %v", err)
	}
	const kid = "test-cluster-key"

	jwksServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(buildTestJWKS(map[string]*rsa.PublicKey{kid: &clusterKey.PublicKey}))
	}))
	defer jwksServer.Close()

	// 2. Configure certserver cluster registry
	const clusterID = "test-cluster"
	issuerURL := jwksServer.URL

	clusters := []registry.ClusterConfig{
		{
			ClusterID:                clusterID,
			IssuerURL:                issuerURL,
			JWKSURL:                  jwksServer.URL,
			ControllerNamespace:      "gateway-system",
			ControllerServiceAccount: "gcpgateway-controller",
			AllowedHostnames: []string{
				"client-test.example.com",
				"rotated.example.com",
			},
		},
	}

	reg, err := registry.NewRegistry(clusters, registry.WithHTTPClient(jwksServer.Client()))
	if err != nil {
		t.Fatalf("failed to create registry: %v", err)
	}
	defer reg.Close()

	bindingStore := registry.NewMemoryBindingStore()
	auditLogger := registry.NewAuditLogger(nil)
	m := metrics.New(prometheus.NewRegistry())

	challengeStore := challenge.NewMemoryStore()
	challengeHandler := challenge.NewHandler(
		challengeStore,
		challenge.WithRegistry(reg),
		challenge.WithBindingStore(bindingStore),
		challenge.WithAuditLogger(auditLogger),
		challenge.WithMetrics(m),
	)

	// Challenge server runs over HTTPS with test TLS certificate
	challengeTLS, challengeCertPEM, _ := generateSelfSignedCert(t)
	challengeSrv := httptest.NewUnstartedServer(challengeHandler)
	challengeSrv.TLS = challengeTLS
	challengeSrv.StartTLS()
	defer challengeSrv.Close()

	tempDir := t.TempDir()

	// Data plane token written to projected token file on disk
	dpToken := signControllerToken(t, issuerURL, "system:serviceaccount:default:test-gateway-gari", clusterKey, kid)
	dpTokenFile := filepath.Join(tempDir, "dp-token")
	if err := os.WriteFile(dpTokenFile, []byte(dpToken), 0600); err != nil {
		t.Fatalf("failed to write data-plane token file: %v", err)
	}

	// Real GARI ACME challenge forwarder from pkg/proxy, reading token from file
	// and verifying upstream certserver HTTPS connection with test CA bundle.
	forwarder, err := proxy.NewACMEChallengeForwarder(proxy.ACMEChallengeConfig{
		UpstreamURL: challengeSrv.URL,
		CABundlePEM: string(challengeCertPEM),
		TokenPath:   dpTokenFile,
	}, proxy.NewFileTokenSource(dpTokenFile))
	if err != nil {
		t.Fatalf("failed to create ACME challenge forwarder: %v", err)
	}

	// 3. Start Pebble ACME test server pointed at real GARI forwarder
	pebble := testserver.StartPebble(t, testserver.ServerConfig{
		ValidityPeriod:   86400,
		ChallengeStore:   challengeStore,
		ChallengeHandler: forwarder,
	})

	// 4. Initialize Manager with Pebble ACME client and temp cache
	cacheDir := filepath.Join(tempDir, "cache")
	diskCache, err := cache.NewDiskCache(cacheDir)
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}
	accountKey, err := diskCache.LoadOrGenerateAccountKey()
	if err != nil {
		t.Fatalf("failed to generate account key: %v", err)
	}

	acmeCli, err := acmeclient.NewClient(ctx, acmeclient.Config{
		DirectoryURL: pebble.DirectoryURL,
		AccountKey:   accountKey,
		HTTPClient:   pebble.HTTPClient,
		UserAgent:    "client-e2e-test",
	})
	if err != nil {
		t.Fatalf("failed to create acme client: %v", err)
	}

	mgr := manager.NewManager(acmeCli, diskCache, challengeStore, m, func(o *manager.Options) {
		o.BindingStore = bindingStore
	})
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}
	defer mgr.Stop()

	// 5. Start certserver gRPC server over TLS
	srvTLS, srvCertPEM, _ := generateSelfSignedCert(t)
	serverOpts := server.Options{
		ListenAddr:   "127.0.0.1:0",
		TLSConfig:    srvTLS,
		ReleaseMode:  true,
		MaxHostnames: 10,
	}

	srv, err := server.NewServer(serverOpts, reg, mgr, bindingStore, auditLogger)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Stop()

	// 6. Write controller token to projected token file
	token := signControllerToken(t, issuerURL, "system:serviceaccount:gateway-system:gcpgateway-controller", clusterKey, kid)
	tokenFile := filepath.Join(tempDir, "token")
	if err := os.WriteFile(tokenFile, []byte(token), 0600); err != nil {
		t.Fatalf("failed to write token file: %v", err)
	}

	caBundleFile := filepath.Join(tempDir, "ca.crt")
	if err := os.WriteFile(caBundleFile, srvCertPEM, 0644); err != nil {
		t.Fatalf("failed to write CA bundle file: %v", err)
	}

	// 7. Initialize certserver client with CA bundle and token path
	cli, err := New(Config{
		ServerAddress: srv.Addr(),
		CABundlePath:  caBundleFile,
		TokenPath:     tokenFile,
		MinBackoff:    100 * time.Millisecond,
		MaxBackoff:    500 * time.Millisecond,
		RPCTimeout:    10 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Stop() // Test Close / Stop

	// 8. Setup trigger tracking
	recomputeCh := make(chan struct{}, 10)
	var triggerCalls atomic.Int32
	cli.SetRecomputeTrigger(func() {
		triggerCalls.Add(1)
		select {
		case recomputeCh <- struct{}{}:
		default:
		}
	})

	// 9. Create test Gateway and Listener
	hostname := gatewayv1.Hostname("client-test.example.com")
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-gateway",
		},
	}
	listener := &gatewayv1.Listener{
		Name:     "https",
		Port:     443,
		Protocol: gatewayv1.HTTPSProtocolType,
		Hostname: &hostname,
	}

	// 10. Call ResolveCertificates: Initial call must return Pending immediately (non-blocking)
	startCall := time.Now()
	res, err := cli.ResolveCertificates(gw, listener)
	callDuration := time.Since(startCall)
	if err != nil {
		t.Fatalf("unexpected error from ResolveCertificates: %v", err)
	}
	if !res.Pending {
		t.Fatalf("expected initial call to return Pending=true, got %+v", res)
	}
	if len(res.Certificates) != 0 {
		t.Fatalf("expected 0 certificates on initial pending call, got %d", len(res.Certificates))
	}
	if callDuration > 50*time.Millisecond {
		t.Errorf("ResolveCertificates took %v, must be strictly non-blocking", callDuration)
	}

	// 11. Wait for background issuance to complete and trigger to be called
	select {
	case <-recomputeCh:
		// Trigger was successfully called by background worker!
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for background issuance trigger to be called")
	}

	// 12. Call ResolveCertificates again: Must return the newly issued certificate!
	res2, err := cli.ResolveCertificates(gw, listener)
	if err != nil {
		t.Fatalf("ResolveCertificates failed on recompute: %v", err)
	}
	if res2.Pending {
		t.Fatalf("expected Pending=false after trigger, but still pending")
	}
	if len(res2.Certificates) != 1 {
		t.Fatalf("expected exactly 1 certificate, got %d", len(res2.Certificates))
	}
	cert := res2.Certificates[0]
	if cert == nil || len(cert.Certificate) == 0 {
		t.Fatalf("returned certificate chain is empty")
	}
	if cert.PrivateKey == nil {
		t.Fatalf("returned certificate private key is missing (release mode)")
	}
	parsedLeaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("failed to parse leaf certificate: %v", err)
	}
	if len(parsedLeaf.DNSNames) != 1 || parsedLeaf.DNSNames[0] != "client-test.example.com" {
		t.Errorf("expected DNSNames [client-test.example.com], got %v", parsedLeaf.DNSNames)
	}

	// 13. Call ResolveCertificates a third time: Must serve from the local in-memory cache
	startCall3 := time.Now()
	res3, err := cli.ResolveCertificates(gw, listener)
	call3Duration := time.Since(startCall3)
	if err != nil {
		t.Fatalf("ResolveCertificates failed on third call: %v", err)
	}
	if res3.Pending || len(res3.Certificates) != 1 {
		t.Fatalf("expected served from cache, got %+v", res3)
	}
	if call3Duration > 5*time.Millisecond {
		t.Errorf("cache hit took %v, expected <5ms", call3Duration)
	}

	// 14. Verify token rotation: write a rotated token to disk and request a new hostname
	rotatedToken := signControllerToken(t, issuerURL, "system:serviceaccount:gateway-system:gcpgateway-controller", clusterKey, kid)
	if err := os.WriteFile(tokenFile, []byte(rotatedToken), 0600); err != nil {
		t.Fatalf("failed to update token file: %v", err)
	}

	rotatedHostname := gatewayv1.Hostname("rotated.example.com")
	rotatedListener := &gatewayv1.Listener{
		Name:     "https-rotated",
		Port:     443,
		Protocol: gatewayv1.HTTPSProtocolType,
		Hostname: &rotatedHostname,
	}

	resRot, err := cli.ResolveCertificates(gw, rotatedListener)
	if err != nil || !resRot.Pending {
		t.Fatalf("expected Pending=true for new hostname, got res=%+v err=%v", resRot, err)
	}

	select {
	case <-recomputeCh:
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for rotated certificate issuance")
	}

	resRot2, err := cli.ResolveCertificates(gw, rotatedListener)
	if err != nil || len(resRot2.Certificates) != 1 {
		t.Fatalf("expected certificate for rotated listener, got %+v (err=%v)", resRot2, err)
	}

	// 15. Verify that no tokens or certificate private keys were ever logged
	capturedLogs := logBuf.String()
	if strings.Contains(capturedLogs, token) {
		t.Errorf("security violation: bearer token was logged!")
	}
	if strings.Contains(capturedLogs, rotatedToken) {
		t.Errorf("security violation: rotated bearer token was logged!")
	}
	if strings.Contains(capturedLogs, "BEGIN RSA PRIVATE KEY") || strings.Contains(capturedLogs, "BEGIN PRIVATE KEY") {
		t.Errorf("security violation: certificate private key was logged!")
	}
}

// Stop alias for Close for test convenience
func (c *Client) Stop() {
	_ = c.Close()
}

func TestClient_InputValidation(t *testing.T) {
	tempDir := t.TempDir()
	tokenFile := filepath.Join(tempDir, "token")
	if err := os.WriteFile(tokenFile, []byte("tok"), 0600); err != nil {
		t.Fatalf("failed to write token: %v", err)
	}

	cli, err := New(Config{
		ServerAddress: "localhost:1234",
		TokenPath:     tokenFile,
		TLSConfig:     &tls.Config{InsecureSkipVerify: true},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Stop()

	// 1. Nil gateway / listener
	res, err := cli.ResolveCertificates(nil, nil)
	if err != nil || res.FailedReason != gatewayv1.ListenerReasonInvalidCertificateRef {
		t.Errorf("expected InvalidCertificateRef for nil gw/listener, got %+v (err=%v)", res, err)
	}

	// 2. Empty hostname
	emptyHost := gatewayv1.Hostname("")
	gw := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "gw"}}
	res, err = cli.ResolveCertificates(gw, &gatewayv1.Listener{Hostname: &emptyHost})
	if err != nil || res.FailedReason != gatewayv1.ListenerReasonInvalidCertificateRef {
		t.Errorf("expected InvalidCertificateRef for empty hostname, got %+v (err=%v)", res, err)
	}

	// 3. Wildcard hostname
	wildcard := gatewayv1.Hostname("*.example.com")
	res, err = cli.ResolveCertificates(gw, &gatewayv1.Listener{Hostname: &wildcard})
	if err != nil || res.FailedReason != gatewayv1.ListenerReasonInvalidCertificateRef {
		t.Errorf("expected InvalidCertificateRef for wildcard hostname, got %+v (err=%v)", res, err)
	}
}

type mockCertServiceClient struct {
	certserverv1.CertificateServiceClient
	getCertFunc func(ctx context.Context, req *certserverv1.GetCertificateRequest, opts ...grpc.CallOption) (*certserverv1.GetCertificateResponse, error)
}

func (m *mockCertServiceClient) GetCertificate(ctx context.Context, req *certserverv1.GetCertificateRequest, opts ...grpc.CallOption) (*certserverv1.GetCertificateResponse, error) {
	return m.getCertFunc(ctx, req, opts...)
}

func TestClient_NamespaceQualifiedDataPlaneServiceAccount(t *testing.T) {
	tempDir := t.TempDir()
	tokenFile := filepath.Join(tempDir, "token")
	if err := os.WriteFile(tokenFile, []byte("valid-token"), 0600); err != nil {
		t.Fatalf("failed to write token: %v", err)
	}

	reqCh := make(chan *certserverv1.GetCertificateRequest, 1)
	mockGRPC := &mockCertServiceClient{
		getCertFunc: func(ctx context.Context, req *certserverv1.GetCertificateRequest, opts ...grpc.CallOption) (*certserverv1.GetCertificateResponse, error) {
			select {
			case reqCh <- req:
			default:
			}
			return nil, status.Errorf(codes.Unavailable, "issuance in progress")
		},
	}

	cli, err := New(Config{
		TokenPath:  tokenFile,
		GRPCClient: mockGRPC,
		MinBackoff: 10 * time.Millisecond,
		MaxBackoff: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Stop()

	host := gatewayv1.Hostname("app.example.com")
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "traffic-system",
			Name:      "web-gateway",
		},
	}
	listener := &gatewayv1.Listener{Hostname: &host}

	_, err = cli.ResolveCertificates(gw, listener)
	if err != nil {
		t.Fatalf("unexpected error calling ResolveCertificates: %v", err)
	}

	var capturedReq *certserverv1.GetCertificateRequest
	select {
	case capturedReq = <-reqCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for GetCertificate request")
	}

	wantDPSA := "traffic-system/web-gateway-gari"
	if got := capturedReq.GetDataPlaneServiceAccount(); got != wantDPSA {
		t.Errorf("GetDataPlaneServiceAccount() = %q, want namespace-qualified %q", got, wantDPSA)
	}

	wantGateway := "traffic-system/web-gateway"
	if got := capturedReq.GetGateway(); got != wantGateway {
		t.Errorf("GetGateway() = %q, want %q", got, wantGateway)
	}
}

func TestClient_NonRetryableError(t *testing.T) {
	tempDir := t.TempDir()
	tokenFile := filepath.Join(tempDir, "token")
	if err := os.WriteFile(tokenFile, []byte("valid-token"), 0600); err != nil {
		t.Fatalf("failed to write token: %v", err)
	}

	mockGRPC := &mockCertServiceClient{
		getCertFunc: func(ctx context.Context, req *certserverv1.GetCertificateRequest, opts ...grpc.CallOption) (*certserverv1.GetCertificateResponse, error) {
			return nil, status.Errorf(codes.PermissionDenied, "hostname policy refused: domain denied")
		},
	}

	cli, err := New(Config{
		TokenPath:  tokenFile,
		GRPCClient: mockGRPC,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Stop()

	recomputeCh := make(chan struct{}, 10)
	cli.SetRecomputeTrigger(func() {
		recomputeCh <- struct{}{}
	})

	host := gatewayv1.Hostname("denied.example.com")
	gw := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "gw"}}
	listener := &gatewayv1.Listener{Hostname: &host}

	// First call -> pending
	res, err := cli.ResolveCertificates(gw, listener)
	if err != nil || !res.Pending {
		t.Fatalf("expected Pending=true on initial call, got res=%+v err=%v", res, err)
	}

	// Wait for trigger indicating failure
	select {
	case <-recomputeCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for trigger on permanent error")
	}

	// Second call -> FailedReason reported with generic status message
	res2, err := cli.ResolveCertificates(gw, listener)
	if err != nil {
		t.Fatalf("unexpected error on second call: %v", err)
	}
	if res2.FailedReason != gatewayv1.ListenerReasonInvalidCertificateRef {
		t.Errorf("expected InvalidCertificateRef on permanent error, got %v", res2.FailedReason)
	}
	if !strings.Contains(strings.ToLower(res2.FailedMessage), "permission denied") {
		t.Errorf("expected generic permission denied message, got %q", res2.FailedMessage)
	}
}

func TestClient_NonRetryableError_Recovery(t *testing.T) {
	tempDir := t.TempDir()
	tokenFile := filepath.Join(tempDir, "token")
	if err := os.WriteFile(tokenFile, []byte("valid-token"), 0600); err != nil {
		t.Fatalf("failed to write token: %v", err)
	}

	_, certPEM, keyPEM := generateSelfSignedCert(t)

	var calls atomic.Int32
	mockGRPC := &mockCertServiceClient{
		getCertFunc: func(ctx context.Context, req *certserverv1.GetCertificateRequest, opts ...grpc.CallOption) (*certserverv1.GetCertificateResponse, error) {
			idx := calls.Add(1)
			if idx == 1 {
				return nil, status.Errorf(codes.PermissionDenied, "temporary policy denial")
			}
			return &certserverv1.GetCertificateResponse{
				CertificateChain: certPEM,
				PrivateKey:       keyPEM,
			}, nil
		},
	}

	cli, err := New(Config{
		TokenPath:  tokenFile,
		GRPCClient: mockGRPC,
		MinBackoff: 10 * time.Millisecond,
		MaxBackoff: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Stop()

	recomputeCh := make(chan struct{}, 10)
	cli.SetRecomputeTrigger(func() {
		select {
		case recomputeCh <- struct{}{}:
		default:
		}
	})

	host := gatewayv1.Hostname("recover.example.com")
	gw := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "gw"}}
	listener := &gatewayv1.Listener{Hostname: &host}

	// 1. Initial call -> pending
	res, err := cli.ResolveCertificates(gw, listener)
	if err != nil || !res.Pending {
		t.Fatalf("expected Pending=true, got %+v", res)
	}

	// 2. Wait for first failure trigger
	select {
	case <-recomputeCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for initial failure trigger")
	}

	resFail, err := cli.ResolveCertificates(gw, listener)
	if err != nil || resFail.FailedReason != gatewayv1.ListenerReasonInvalidCertificateRef {
		t.Fatalf("expected InvalidCertificateRef, got %+v", resFail)
	}
	if !strings.Contains(strings.ToLower(resFail.FailedMessage), "permission denied") {
		t.Errorf("expected generic permission denied message, got %q", resFail.FailedMessage)
	}

	// 3. Client retried with capped backoff in background and succeeded!
	select {
	case <-recomputeCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for recovery trigger")
	}

	resSuccess, err := cli.ResolveCertificates(gw, listener)
	if err != nil || resSuccess.Pending || len(resSuccess.Certificates) != 1 {
		t.Fatalf("expected recovered certificate, got %+v (err=%v)", resSuccess, err)
	}
}

func TestClient_RefusedRefreshPreservesValidCertificate(t *testing.T) {
	tempDir := t.TempDir()
	tokenFile := filepath.Join(tempDir, "token")
	if err := os.WriteFile(tokenFile, []byte("valid-token"), 0600); err != nil {
		t.Fatalf("failed to write token: %v", err)
	}

	_, certPEM, keyPEM := generateSelfSignedCert(t)

	var calls atomic.Int32
	mockGRPC := &mockCertServiceClient{
		getCertFunc: func(ctx context.Context, req *certserverv1.GetCertificateRequest, opts ...grpc.CallOption) (*certserverv1.GetCertificateResponse, error) {
			idx := calls.Add(1)
			if idx == 1 {
				return &certserverv1.GetCertificateResponse{
					CertificateChain: certPEM,
					PrivateKey:       keyPEM,
				}, nil
			}
			return nil, status.Errorf(codes.PermissionDenied, "renewal policy refusal")
		},
	}

	simulatedTime := time.Now()
	cli, err := New(Config{
		TokenPath:      tokenFile,
		GRPCClient:     mockGRPC,
		MinBackoff:     10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		CheckInterval:  50 * time.Millisecond,
		EvictionPeriod: 48 * time.Hour,
		TimeNow: func() time.Time {
			return simulatedTime
		},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Stop()

	recomputeCh := make(chan struct{}, 10)
	cli.SetRecomputeTrigger(func() {
		select {
		case recomputeCh <- struct{}{}:
		default:
		}
	})

	host := gatewayv1.Hostname("refresh.example.com")
	gw := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "gw"}}
	listener := &gatewayv1.Listener{Hostname: &host}

	// 1. Initial call -> pending
	res, err := cli.ResolveCertificates(gw, listener)
	if err != nil || !res.Pending {
		t.Fatalf("expected Pending=true, got %+v", res)
	}

	// 2. Wait for initial certificate issuance
	select {
	case <-recomputeCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for issuance trigger")
	}

	resCert, err := cli.ResolveCertificates(gw, listener)
	if err != nil || len(resCert.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %+v", resCert)
	}

	// 3. Advance time into renewal window, but before expiration (cert valid 24h, renewal window is 8h before expiry)
	simulatedTime = simulatedTime.Add(20 * time.Hour)

	// Trigger renewal check
	cli.checkAndRefresh()

	// Wait for background renewal attempt (which returns PermissionDenied)
	time.Sleep(100 * time.Millisecond)

	// 4. ResolveCertificates MUST STILL return the valid unexpired certificate!
	resStillValid, err := cli.ResolveCertificates(gw, listener)
	if err != nil {
		t.Fatalf("ResolveCertificates returned error: %v", err)
	}
	if len(resStillValid.Certificates) != 1 {
		t.Fatalf("expected valid certificate to be retained during failed refresh, got %+v", resStillValid)
	}
	if resStillValid.FailedReason != "" {
		t.Errorf("expected no FailedReason while cert is unexpired, got %v", resStillValid.FailedReason)
	}
}

func TestClient_Eviction(t *testing.T) {
	tempDir := t.TempDir()
	tokenFile := filepath.Join(tempDir, "token")
	if err := os.WriteFile(tokenFile, []byte("valid-token"), 0600); err != nil {
		t.Fatalf("failed to write token: %v", err)
	}

	_, certPEM, keyPEM := generateSelfSignedCert(t)

	mockGRPC := &mockCertServiceClient{
		getCertFunc: func(ctx context.Context, req *certserverv1.GetCertificateRequest, opts ...grpc.CallOption) (*certserverv1.GetCertificateResponse, error) {
			return &certserverv1.GetCertificateResponse{
				CertificateChain: certPEM,
				PrivateKey:       keyPEM,
			}, nil
		},
	}

	simulatedTime := time.Now()
	cli, err := New(Config{
		TokenPath:      tokenFile,
		GRPCClient:     mockGRPC,
		EvictionPeriod: 100 * time.Millisecond,
		TimeNow: func() time.Time {
			return simulatedTime
		},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Stop()

	recomputeCh := make(chan struct{}, 10)
	cli.SetRecomputeTrigger(func() {
		select {
		case recomputeCh <- struct{}{}:
		default:
		}
	})

	host := gatewayv1.Hostname("evict.example.com")
	gw := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "gw"}}
	listener := &gatewayv1.Listener{Hostname: &host}

	// Call ResolveCertificates to insert into cache
	_, _ = cli.ResolveCertificates(gw, listener)

	// Wait for initial fetch to finish so inFlight is false
	select {
	case <-recomputeCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for fetch")
	}

	cli.mu.RLock()
	cacheLenBefore := len(cli.cache)
	cli.mu.RUnlock()
	if cacheLenBefore != 1 {
		t.Fatalf("expected 1 entry in cache, got %d", cacheLenBefore)
	}

	// Advance time past EvictionPeriod without calling ResolveCertificates
	simulatedTime = simulatedTime.Add(200 * time.Millisecond)

	cli.checkAndRefresh()

	cli.mu.RLock()
	cacheLenAfter := len(cli.cache)
	cli.mu.RUnlock()
	if cacheLenAfter != 0 {
		t.Fatalf("expected inactive entry to be evicted from cache, got %d entries", cacheLenAfter)
	}
}

func TestClient_Eviction_FailingLoopExits(t *testing.T) {
	tempDir := t.TempDir()
	tokenFile := filepath.Join(tempDir, "token")
	if err := os.WriteFile(tokenFile, []byte("valid-token"), 0600); err != nil {
		t.Fatalf("failed to write token: %v", err)
	}

	var rpcCalls atomic.Int32
	mockGRPC := &mockCertServiceClient{
		getCertFunc: func(ctx context.Context, req *certserverv1.GetCertificateRequest, opts ...grpc.CallOption) (*certserverv1.GetCertificateResponse, error) {
			rpcCalls.Add(1)
			return nil, status.Errorf(codes.Unavailable, "issuance permanently broken in test")
		},
	}

	cli, err := New(Config{
		TokenPath:      tokenFile,
		GRPCClient:     mockGRPC,
		MinBackoff:     10 * time.Millisecond,
		MaxBackoff:     20 * time.Millisecond,
		EvictionPeriod: 100 * time.Millisecond,
		CheckInterval:  50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Stop()

	host := gatewayv1.Hostname("failing.example.com")
	gw := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "gw"}}
	listener := &gatewayv1.Listener{Hostname: &host}

	// 1. Initial call triggers background fetchLoop
	res, err := cli.ResolveCertificates(gw, listener)
	if err != nil || !res.Pending {
		t.Fatalf("expected initial call to return Pending=true, got %+v (err=%v)", res, err)
	}

	// 2. Stop calling ResolveCertificates and wait for EvictionPeriod to elapse.
	// fetchLoop should evict the entry on lastRequested alone and exit.
	time.Sleep(300 * time.Millisecond)

	cli.mu.RLock()
	cacheLen := len(cli.cache)
	cli.mu.RUnlock()
	if cacheLen != 0 {
		t.Fatalf("expected failing entry to be evicted after EvictionPeriod, but %d entries remain", cacheLen)
	}

	// 3. Record RPC count, wait longer, and ensure no further RPCs are made
	callsAtEviction := rpcCalls.Load()
	if callsAtEviction == 0 {
		t.Fatalf("expected at least one RPC call before eviction")
	}

	time.Sleep(150 * time.Millisecond)

	callsAfter := rpcCalls.Load()
	if callsAfter != callsAtEviction {
		t.Errorf("expected fetchLoop to have exited with no further RPCs, but calls increased from %d to %d", callsAtEviction, callsAfter)
	}
}
