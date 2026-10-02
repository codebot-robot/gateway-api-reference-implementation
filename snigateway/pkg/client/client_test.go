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
	"net"
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

	resp, err := c.Register(ctx, []string{"foo.example.com", "*.bar.com"})
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
