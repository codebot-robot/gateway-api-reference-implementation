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

package tunnel

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/api"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/certs"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/client"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/frontend"
)

func TestManager_EndToEnd(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "snigateway-mgr-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	if err := certs.GenerateAndWriteCertificates(tempDir, "snigateway.internal", "mgr-client"); err != nil {
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

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	serverAddr := ln.Addr().String()

	srv, err := frontend.NewServer(frontend.ServerConfig{
		ServerTLSConfig:  serverTLS,
		InternalHostname: "snigateway.internal",
		ConnectTimeout:   5 * time.Second,
		ReadSNITimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer srv.Close()

	go func() {
		_ = srv.Serve(ln)
	}()

	clientTLS, err := certs.NewClientTLSConfig(caCertPEM, clientCertPEM, clientKeyPEM, "snigateway.internal")
	if err != nil {
		t.Fatalf("NewClientTLSConfig failed: %v", err)
	}

	c := client.NewClient(serverAddr, clientTLS)
	mgr := NewManager(c, WithBackoff(50*time.Millisecond, 200*time.Millisecond))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go func() {
		_ = mgr.Run(ctx)
	}()

	// 1. Update hostnames on manager
	mgr.UpdateHostnames([]string{"app1.example.com", "*.wildcard.org"})

	// Poll until registration table reflects hostnames
	var regHosts []string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		regHosts = srv.RegistrationTable().GetRegisteredHostnames("mgr-client")
		slices.Sort(regHosts)
		if slices.Equal(regHosts, []string{"*.wildcard.org", "app1.example.com"}) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !slices.Equal(regHosts, []string{"*.wildcard.org", "app1.example.com"}) {
		t.Fatalf("registered hostnames mismatch, got %v", regHosts)
	}

	// 2. Start an HTTPS server on mgr.Listener()
	tlsCert, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	if err != nil {
		t.Fatalf("tls.X509KeyPair: %v", err)
	}

	tlsLis := tls.NewListener(mgr.Listener(), &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
	})

	httpSrv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("response from " + r.Host))
		}),
	}
	defer httpSrv.Close()

	go func() {
		_ = httpSrv.Serve(tlsLis)
	}()

	// 3. Connect as external client with matching SNI
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				ServerName:         "app1.example.com",
				InsecureSkipVerify: true,
			},
			DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return tls.Dial("tcp", serverAddr, &tls.Config{
					ServerName:         "app1.example.com",
					InsecureSkipVerify: true,
				})
			},
		},
		Timeout: 5 * time.Second,
	}

	resp, err := httpClient.Get("https://app1.example.com/test")
	if err != nil {
		t.Fatalf("HTTP GET failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	// 4. Update hostnames dynamically: remove app1.example.com, add app2.example.com
	mgr.UpdateHostnames([]string{"app2.example.com"})

	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		regHosts = srv.RegistrationTable().GetRegisteredHostnames("mgr-client")
		if slices.Equal(regHosts, []string{"app2.example.com"}) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !slices.Equal(regHosts, []string{"app2.example.com"}) {
		t.Fatalf("registered hostnames mismatch after update, got %v", regHosts)
	}

	// app2.example.com should now succeed
	httpApp2Client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				ServerName:         "app2.example.com",
				InsecureSkipVerify: true,
			},
			DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return tls.Dial("tcp", serverAddr, &tls.Config{
					ServerName:         "app2.example.com",
					InsecureSkipVerify: true,
				})
			},
		},
		Timeout: 5 * time.Second,
	}

	resp2, err := httpApp2Client.Get("https://app2.example.com/test")
	if err != nil {
		t.Fatalf("HTTP GET app2 failed: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 for app2, got %d", resp2.StatusCode)
	}

	// app1.example.com should now fail / be closed
	_, err = httpClient.Get("https://app1.example.com/test")
	if err == nil {
		t.Fatalf("expected GET app1 to fail after unregistration, but succeeded")
	}
}

func TestManager_StreamReconnect(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "snigateway-mgr-reconn-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	if err := certs.GenerateAndWriteCertificates(tempDir, "snigateway.internal", "mgr-client"); err != nil {
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

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	serverAddr := ln.Addr().String()

	srv, err := frontend.NewServer(frontend.ServerConfig{
		ServerTLSConfig:  serverTLS,
		InternalHostname: "snigateway.internal",
		ConnectTimeout:   5 * time.Second,
		ReadSNITimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer srv.Close()

	go func() {
		_ = srv.Serve(ln)
	}()

	clientTLS, err := certs.NewClientTLSConfig(caCertPEM, clientCertPEM, clientKeyPEM, "snigateway.internal")
	if err != nil {
		t.Fatalf("NewClientTLSConfig failed: %v", err)
	}

	c := client.NewClient(serverAddr, clientTLS)
	mgr := NewManager(c, WithBackoff(50*time.Millisecond, 100*time.Millisecond))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go func() {
		_ = mgr.Run(ctx)
	}()

	mgr.UpdateHostnames([]string{"echo.snigateway.test"})

	tlsCert, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	if err != nil {
		t.Fatalf("tls.X509KeyPair: %v", err)
	}

	tlsLis := tls.NewListener(mgr.Listener(), &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
	})

	httpSrv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("response from " + r.Host))
		}),
	}
	defer httpSrv.Close()

	go func() {
		_ = httpSrv.Serve(tlsLis)
	}()

	// Wait for initial registration
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		regHosts := srv.RegistrationTable().GetRegisteredHostnames("mgr-client")
		if slices.Equal(regHosts, []string{"echo.snigateway.test"}) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				ServerName:         "echo.snigateway.test",
				InsecureSkipVerify: true,
			},
			DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return tls.Dial("tcp", serverAddr, &tls.Config{
					ServerName:         "echo.snigateway.test",
					InsecureSkipVerify: true,
				})
			},
		},
		Timeout: 5 * time.Second,
	}

	resp, err := httpClient.Get("https://echo.snigateway.test/test")
	if err != nil {
		t.Fatalf("initial HTTP GET failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	// Connect an overlapping second stream with the same client certificate to simulate reconnect / old stream displacement
	ctxOverlap, cancelOverlap := context.WithCancel(t.Context())
	overlapReady := make(chan struct{})
	overlapClient := client.NewClient(serverAddr, clientTLS)
	go func() {
		_ = overlapClient.StreamConnectionsWithReady(ctxOverlap, overlapReady, func(ctx context.Context, event *api.ConnectionEvent) error {
			return nil
		})
	}()

	select {
	case <-overlapReady:
	case <-time.After(3 * time.Second):
		t.Fatal("overlap stream timed out connecting")
	}

	// Close the overlapping stream; Manager should recover and re-register
	cancelOverlap()

	// Wait for Manager to reconnect and re-register
	deadline = time.Now().Add(5 * time.Second)
	var regHosts []string
	for time.Now().Before(deadline) {
		regHosts = srv.RegistrationTable().GetRegisteredHostnames("mgr-client")
		if slices.Equal(regHosts, []string{"echo.snigateway.test"}) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !slices.Equal(regHosts, []string{"echo.snigateway.test"}) {
		t.Fatalf("hostnames not re-registered after stream reconnect, got: %v", regHosts)
	}

	// Verify traffic still works
	resp2, err := httpClient.Get("https://echo.snigateway.test/test")
	if err != nil {
		t.Fatalf("HTTP GET after reconnect failed: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 after reconnect, got %d", resp2.StatusCode)
	}
}
