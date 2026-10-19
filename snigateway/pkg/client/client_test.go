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
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/certs"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/frontend"
)

func TestClient_Registration(t *testing.T) {
	generated, err := certs.GenerateAll("snigateway.internal", "client-test")
	if err != nil {
		t.Fatalf("GenerateAll failed: %v", err)
	}

	serverTLS, err := certs.NewServerTLSConfig(generated.CA.CertPEM, generated.Server.CertPEM, generated.Server.KeyPEM)
	if err != nil {
		t.Fatalf("NewServerTLSConfig failed: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	defer ln.Close()

	srv, err := frontend.NewServer(frontend.ServerConfig{
		ServerTLSConfig: serverTLS,
	})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer srv.Close()

	go func() {
		_ = srv.Serve(ln)
	}()

	clientTLS, err := certs.NewClientTLSConfig(generated.CA.CertPEM, generated.Client.CertPEM, generated.Client.KeyPEM, "snigateway.internal")
	if err != nil {
		t.Fatalf("NewClientTLSConfig failed: %v", err)
	}

	c := NewClient(ln.Addr().String(), clientTLS)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	sessID, _, err := c.StartSession(ctx)
	if err != nil {
		t.Fatalf("c.StartSession failed: %v", err)
	}

	resp, err := c.Register(ctx, sessID, []string{"foo.example.com", "*.bar.com"})
	if err != nil {
		t.Fatalf("c.Register failed: %v", err)
	}

	if resp.Status != "registered" {
		t.Fatalf("expected status registered, got %s", resp.Status)
	}

	if len(resp.Hostnames) != 2 {
		t.Fatalf("expected 2 hostnames, got %d (%v)", len(resp.Hostnames), resp.Hostnames)
	}
}

func TestTLSSessionResumption_TunnelDial(t *testing.T) {
	generated, err := certs.GenerateAll("snigateway.internal", "client-test")
	if err != nil {
		t.Fatalf("GenerateAll failed: %v", err)
	}

	serverTLS, err := certs.NewServerTLSConfig(generated.CA.CertPEM, generated.Server.CertPEM, generated.Server.KeyPEM)
	if err != nil {
		t.Fatalf("NewServerTLSConfig failed: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	defer ln.Close()

	srv, err := frontend.NewServer(frontend.ServerConfig{
		ServerTLSConfig: serverTLS,
	})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer srv.Close()

	go func() {
		_ = srv.Serve(ln)
	}()

	clientTLS, err := certs.NewClientTLSConfig(generated.CA.CertPEM, generated.Client.CertPEM, generated.Client.KeyPEM, "snigateway.internal")
	if err != nil {
		t.Fatalf("NewClientTLSConfig failed: %v", err)
	}

	c := NewClient(ln.Addr().String(), clientTLS)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	type connectionStater interface {
		ConnectionState() tls.ConnectionState
	}

	// Dial 1: full TLS handshake to frontend
	rawConn1, err := tls.Dial("tcp", ln.Addr().String(), clientTLS)
	if err != nil {
		t.Fatalf("tls.Dial failed: %v", err)
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://snigateway.internal/healthz", nil)
	_ = req.Write(rawConn1)
	if rawConn1.ConnectionState().DidResume {
		t.Errorf("dial 1 expected DidResume=false, got true")
	}
	rawConn1.Close()

	sessID, _, err := c.StartSession(ctx)
	if err != nil {
		t.Fatalf("c.StartSession failed: %v", err)
	}

	// Tunnel dial: resumed TLS handshake
	conn2, err := c.DialTunnel(ctx, sessID)
	if err != nil {
		t.Fatalf("c.DialTunnel 2 failed: %v", err)
	}
	cs2, ok := conn2.(connectionStater)
	if !ok {
		t.Fatalf("conn2 does not implement ConnectionState()")
	}
	if !cs2.ConnectionState().DidResume {
		t.Errorf("dial 2 expected DidResume=true, got false")
	}
	conn2.Close()
}

func TestClient_WireGuardExchange(t *testing.T) {
	generated, err := certs.GenerateAll("snigateway.internal", "client-test")
	if err != nil {
		t.Fatalf("GenerateAll failed: %v", err)
	}

	otherGen, err := certs.GenerateAll("snigateway.internal", "other-client")
	if err != nil {
		t.Fatalf("GenerateAll other failed: %v", err)
	}

	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(generated.CA.CertPEM)
	caPool.AppendCertsFromPEM(otherGen.CA.CertPEM)

	serverTLS, err := certs.NewServerTLSConfigWithCertPool(caPool, generated.Server.CertPEM, generated.Server.KeyPEM)
	if err != nil {
		t.Fatalf("NewServerTLSConfigWithCertPool failed: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	defer ln.Close()

	srv, err := frontend.NewServer(frontend.ServerConfig{
		ServerTLSConfig:     serverTLS,
		WireGuardListenAddr: "127.0.0.1:0",
		WireGuardPrefix:     "fd00::/64",
	})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer srv.Close()

	go func() {
		_ = srv.Serve(ln)
	}()

	clientTLS, err := certs.NewClientTLSConfig(generated.CA.CertPEM, generated.Client.CertPEM, generated.Client.KeyPEM, "snigateway.internal")
	if err != nil {
		t.Fatalf("NewClientTLSConfig failed: %v", err)
	}

	c := NewClient(ln.Addr().String(), clientTLS)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	sessID, _, err := c.StartSession(ctx)
	if err != nil {
		t.Fatalf("c.StartSession failed: %v", err)
	}

	// Fake client WireGuard public key (32 bytes base64)
	clientPubKey := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
	resp, err := c.WireGuardExchange(ctx, sessID, clientPubKey)
	if err != nil {
		t.Fatalf("WireGuardExchange failed: %v", err)
	}

	if resp.FrontendPublicKey == "" {
		t.Error("expected non-empty FrontendPublicKey")
	}
	if resp.Endpoint == "" {
		t.Error("expected non-empty Endpoint")
	}
	if resp.PeerAddress == "" {
		t.Error("expected non-empty PeerAddress")
	}
	if resp.FrontendAddress == "" {
		t.Error("expected non-empty FrontendAddress")
	}
	if resp.MTU != 1280 {
		t.Errorf("expected MTU 1280, got %d", resp.MTU)
	}

	// Key exchange with another client identity gets 403 Forbidden
	otherTLS, err := certs.NewClientTLSConfig(generated.CA.CertPEM, otherGen.Client.CertPEM, otherGen.Client.KeyPEM, "snigateway.internal")
	if err != nil {
		t.Fatalf("NewClientTLSConfig other failed: %v", err)
	}
	otherClient := NewClient(ln.Addr().String(), otherTLS)
	_, err = otherClient.WireGuardExchange(ctx, sessID, clientPubKey)
	if err == nil {
		t.Fatal("expected error for mismatched identity, got nil")
	}
	if !strings.Contains(err.Error(), "403") {
		t.Fatalf("expected 403 Forbidden for mismatched identity, got %v", err)
	}
}
