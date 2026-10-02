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

package certs

import (
	"crypto/tls"
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateAndWriteCertificates(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "certs-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	err = GenerateAndWriteCertificates(tempDir, "snigateway.internal", "snigateway-client")
	if err != nil {
		t.Fatalf("GenerateAndWriteCertificates failed: %v", err)
	}

	files := []string{
		"ca.crt",
		"ca.key",
		"server.crt",
		"server.key",
		"client.crt",
		"client.key",
	}

	for _, f := range files {
		p := filepath.Join(tempDir, f)
		if _, err := os.Stat(p); err != nil {
			t.Errorf("expected file %s to exist, stat err: %v", f, err)
		}
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

	serverTLS, err := NewServerTLSConfig(caCertPEM, serverCertPEM, serverKeyPEM)
	if err != nil {
		t.Fatalf("NewServerTLSConfig failed: %v", err)
	}

	clientTLS, err := NewClientTLSConfig(caCertPEM, clientCertPEM, clientKeyPEM, "snigateway.internal")
	if err != nil {
		t.Fatalf("NewClientTLSConfig failed: %v", err)
	}

	// Test mTLS handshake between in-memory listener and client
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	defer ln.Close()

	tlsLn := tls.NewListener(ln, serverTLS)
	defer tlsLn.Close()

	errCh := make(chan error, 1)
	go func() {
		conn, err := tlsLn.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		tlsConn, ok := conn.(*tls.Conn)
		if !ok {
			return
		}
		if err := tlsConn.Handshake(); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	clientConn, err := tls.Dial("tcp", ln.Addr().String(), clientTLS)
	if err != nil {
		t.Fatalf("tls.Dial failed: %v", err)
	}
	defer clientConn.Close()

	if err := clientConn.Handshake(); err != nil {
		t.Fatalf("client Handshake failed: %v", err)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("server Handshake failed: %v", err)
	}
}
