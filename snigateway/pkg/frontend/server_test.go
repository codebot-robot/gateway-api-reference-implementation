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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/api"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/certs"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/client"
)

// generateTestBackendCert creates a self-signed cert for the fake backend application served over reverse tunnel.
func generateTestBackendCert(hosts ...string) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(42),
		Subject: pkix.Name{
			CommonName: hosts[0],
		},
		DNSNames:              hosts,
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
	return tls.X509KeyPair(certPEM, keyPEM)
}

func setupTestServerAndClient(t *testing.T) (*Server, string, *certs.GeneratedCerts, func()) {
	t.Helper()

	generated, err := certs.GenerateAll("snigateway.internal", "test-cluster-client")
	if err != nil {
		t.Fatalf("GenerateAll certs failed: %v", err)
	}

	serverTLS, err := certs.NewServerTLSConfig(generated.CA.CertPEM, generated.Server.CertPEM, generated.Server.KeyPEM)
	if err != nil {
		t.Fatalf("NewServerTLSConfig failed: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}

	serverAddr := ln.Addr().String()

	srv, err := NewServer(ServerConfig{
		ServerTLSConfig:  serverTLS,
		InternalHostname: "snigateway.internal",
		ConnectTimeout:   5 * time.Second,
		ReadSNITimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	go func() {
		_ = srv.Serve(ln)
	}()

	cleanup := func() {
		_ = srv.Close()
	}

	return srv, serverAddr, generated, cleanup
}

func TestEndToEndReverseTunnel(t *testing.T) {
	_, serverAddr, generated, cleanup := setupTestServerAndClient(t)
	defer cleanup()

	clientTLS, err := certs.NewClientTLSConfig(generated.CA.CertPEM, generated.Client.CertPEM, generated.Client.KeyPEM, "snigateway.internal")
	if err != nil {
		t.Fatalf("NewClientTLSConfig failed: %v", err)
	}

	c := client.NewClient(serverAddr, clientTLS)

	// Generate backend TLS certificate for target service
	backendCert, err := generateTestBackendCert("app.example.com", "*.wildcard.org")
	if err != nil {
		t.Fatalf("generateTestBackendCert failed: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Backend handler for incoming tunnels
	serveConn := func(ctx context.Context, connID string, hostname string, tunnel net.Conn) {
		defer tunnel.Close()

		tlsServer := tls.Server(tunnel, &tls.Config{
			Certificates: []tls.Certificate{backendCert},
		})
		defer tlsServer.Close()

		buf := make([]byte, 1024)
		n, err := tlsServer.Read(buf)
		if err != nil {
			return
		}
		reqStr := string(buf[:n])
		respStr := fmt.Sprintf("ECHO from backend for host %s: %s", hostname, reqStr)
		_, _ = tlsServer.Write([]byte(respStr))
	}

	// Start client loop serving hostnames
	go func() {
		_ = c.Run(ctx, []string{"app.example.com", "*.wildcard.org"}, serveConn)
	}()

	// Wait for registration to complete
	time.Sleep(100 * time.Millisecond)

	// 1. Test Exact Match End-to-End
	t.Run("Exact Match End to End", func(t *testing.T) {
		tlsConn, err := tls.Dial("tcp", serverAddr, &tls.Config{
			ServerName:         "app.example.com",
			InsecureSkipVerify: true,
		})
		if err != nil {
			t.Fatalf("tls.Dial to app.example.com failed: %v", err)
		}
		defer tlsConn.Close()

		payload := "Hello via reverse tunnel!"
		if _, err := tlsConn.Write([]byte(payload)); err != nil {
			t.Fatalf("Write to tlsConn failed: %v", err)
		}

		reply := make([]byte, 1024)
		n, err := tlsConn.Read(reply)
		if err != nil {
			t.Fatalf("Read from tlsConn failed: %v", err)
		}

		got := string(reply[:n])
		want := "ECHO from backend for host app.example.com: " + payload
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})

	// 2. Test Wildcard Match End-to-End
	t.Run("Wildcard Match End to End", func(t *testing.T) {
		tlsConn, err := tls.Dial("tcp", serverAddr, &tls.Config{
			ServerName:         "service-1.wildcard.org",
			InsecureSkipVerify: true,
		})
		if err != nil {
			t.Fatalf("tls.Dial to service-1.wildcard.org failed: %v", err)
		}
		defer tlsConn.Close()

		payload := "Wildcard hello!"
		if _, err := tlsConn.Write([]byte(payload)); err != nil {
			t.Fatalf("Write to tlsConn failed: %v", err)
		}

		reply := make([]byte, 1024)
		n, err := tlsConn.Read(reply)
		if err != nil {
			t.Fatalf("Read from tlsConn failed: %v", err)
		}

		got := string(reply[:n])
		want := "ECHO from backend for host service-1.wildcard.org: " + payload
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})

	// 3. Test Unknown Hostname - should be closed by frontend
	t.Run("Unknown Hostname Closes Connection", func(t *testing.T) {
		conn, err := tls.Dial("tcp", serverAddr, &tls.Config{
			ServerName:         "unregistered.domain.com",
			InsecureSkipVerify: true,
		})
		if err == nil {
			// If handshake initiated, read should return EOF immediately
			buf := make([]byte, 10)
			_, readErr := conn.Read(buf)
			_ = conn.Close()
			if readErr == nil {
				t.Fatalf("expected connection to be closed for unknown hostname, but read succeeded")
			}
		}
	})
}

func TestRejectionWithoutValidClientCertificate(t *testing.T) {
	_, serverAddr, generated, cleanup := setupTestServerAndClient(t)
	defer cleanup()

	// 1. Connection with no client certificate
	t.Run("No client certificate rejected", func(t *testing.T) {
		caPool := x509.NewCertPool()
		caPool.AppendCertsFromPEM(generated.CA.CertPEM)

		tlsConfig := &tls.Config{
			RootCAs:    caPool,
			ServerName: "snigateway.internal",
		}

		client := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
				DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					dialer := &tls.Dialer{Config: tlsConfig}
					return dialer.DialContext(ctx, "tcp", serverAddr)
				},
			},
		}

		_, err := client.Get("https://snigateway.internal/v1/connections")
		if err == nil {
			t.Fatal("expected request without client cert to fail, got nil err")
		}
	})

	// 2. Connection with client certificate signed by untrusted/different CA
	t.Run("Untrusted client certificate rejected", func(t *testing.T) {
		otherCerts, err := certs.GenerateAll("snigateway.internal", "rogue-client")
		if err != nil {
			t.Fatalf("GenerateAll rogue certs failed: %v", err)
		}

		caPool := x509.NewCertPool()
		caPool.AppendCertsFromPEM(generated.CA.CertPEM)

		rogueClientCert, err := tls.X509KeyPair(otherCerts.Client.CertPEM, otherCerts.Client.KeyPEM)
		if err != nil {
			t.Fatalf("parsing rogue cert: %v", err)
		}

		tlsConfig := &tls.Config{
			RootCAs:      caPool,
			Certificates: []tls.Certificate{rogueClientCert},
			ServerName:   "snigateway.internal",
		}

		client := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
				DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					dialer := &tls.Dialer{Config: tlsConfig}
					return dialer.DialContext(ctx, "tcp", serverAddr)
				},
			},
		}

		_, err = client.Get("https://snigateway.internal/v1/connections")
		if err == nil {
			t.Fatal("expected request with untrusted client cert to fail, got nil err")
		}
	})
}

func TestStreamReconnectKeepsRegistration(t *testing.T) {
	srv, serverAddr, generated, cleanup := setupTestServerAndClient(t)
	defer cleanup()

	clientTLS, err := certs.NewClientTLSConfig(generated.CA.CertPEM, generated.Client.CertPEM, generated.Client.KeyPEM, "snigateway.internal")
	if err != nil {
		t.Fatalf("NewClientTLSConfig failed: %v", err)
	}

	c := client.NewClient(serverAddr, clientTLS)

	ctx1, cancel1 := context.WithCancel(t.Context())
	ready1 := make(chan struct{})

	go func() {
		_ = c.StreamConnectionsWithReady(ctx1, ready1, func(ctx context.Context, event *api.ConnectionEvent) error {
			return nil
		})
	}()

	select {
	case <-ready1:
	case <-time.After(3 * time.Second):
		t.Fatal("stream 1 timed out connecting")
	}

	// Register hostnames with frontend
	if _, err := c.Register(ctx1, []string{"app.example.com"}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if registered := srv.RegistrationTable().GetRegisteredHostnames("test-cluster-client"); len(registered) != 1 || registered[0] != "app.example.com" {
		t.Fatalf("expected [app.example.com] registered, got %v", registered)
	}

	// Connect stream 2 (reconnect) before stream 1 completes
	ctx2, cancel2 := context.WithCancel(t.Context())
	ready2 := make(chan struct{})

	go func() {
		_ = c.StreamConnectionsWithReady(ctx2, ready2, func(ctx context.Context, event *api.ConnectionEvent) error {
			return nil
		})
	}()

	select {
	case <-ready2:
	case <-time.After(3 * time.Second):
		t.Fatal("stream 2 timed out connecting")
	}

	// Now close stream 1
	cancel1()
	time.Sleep(100 * time.Millisecond)

	// Stream 1 exiting must NOT unregister the hostnames because stream 2 is active
	if registered := srv.RegistrationTable().GetRegisteredHostnames("test-cluster-client"); len(registered) != 1 || registered[0] != "app.example.com" {
		t.Fatalf("expected hostnames to remain registered after stream 1 closed, got %v", registered)
	}

	// Now close stream 2 (the active stream)
	cancel2()
	time.Sleep(100 * time.Millisecond)

	// Active stream 2 closing with no replacement must unregister
	if registered := srv.RegistrationTable().GetRegisteredHostnames("test-cluster-client"); len(registered) != 0 {
		t.Fatalf("expected hostnames to be unregistered after active stream 2 closed, got %v", registered)
	}
}
