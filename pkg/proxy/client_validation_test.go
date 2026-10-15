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

package proxy

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
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func generateTestCAAndClientCert(t testing.TB, commonName string) ([]byte, tls.Certificate) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: commonName + " CA"},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(1 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	caBytes, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create CA certificate: %v", err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caBytes})
	caCert, err := x509.ParseCertificate(caBytes)
	if err != nil {
		t.Fatalf("failed to parse CA certificate: %v", err)
	}

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate client key: %v", err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: commonName + " Client"},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(1 * time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientBytes, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create client certificate: %v", err)
	}
	clientPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientBytes})
	clientKeyBytes, err := x509.MarshalECPrivateKey(clientKey)
	if err != nil {
		t.Fatalf("failed to marshal client key: %v", err)
	}
	clientKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: clientKeyBytes})

	clientCert, err := tls.X509KeyPair(clientPEM, clientKeyPEM)
	if err != nil {
		t.Fatalf("failed to create client TLS key pair: %v", err)
	}

	return caPEM, clientCert
}

func TestClientCertificateValidation_RealSockets(t *testing.T) {
	// 1. Backend server
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("backend-response-ok"))
	}))
	defer backendServer.Close()

	backendURL, err := url.Parse(backendServer.URL)
	if err != nil {
		t.Fatalf("failed to parse backend URL: %v", err)
	}
	backendHost, backendPortStr, _ := net.SplitHostPort(backendURL.Host)
	backendPort, _ := strconv.Atoi(backendPortStr)

	// 2. Generate CAs and client certs
	ca1PEM, client1Cert := generateTestCAAndClientCert(t, "CA-1")
	ca2PEM, client2Cert := generateTestCAAndClientCert(t, "CA-2")
	_, clientOtherCert := generateTestCAAndClientCert(t, "CA-Other")

	// 3. Generate server certificates sharing a CA
	serverCAPEM, serverCert := generateTestCAAndCert(t, []string{"a.example.com", "b.example.com", "c.example.com"}, nil)

	serverCAPool := x509.NewCertPool()
	serverCAPool.AppendCertsFromPEM(serverCAPEM)

	// 4. Initialize Proxy
	p := NewProxy()
	p.UpdateCertificates(map[string]*tls.Certificate{
		"a.example.com": &serverCert,
		"b.example.com": &serverCert,
		"c.example.com": &serverCert,
	}, &serverCert)

	routeA := state.InternalRoute{
		Hostnames: []string{"a.example.com"},
		Rules: []state.InternalRule{
			{
				Matches:  []state.InternalMatch{{Path: &state.InternalPathMatch{Type: gatewayv1.PathMatchPathPrefix, Value: "/"}}},
				Backends: []state.InternalBackend{{Host: backendHost, Port: int32(backendPort), Weight: 1}},
			},
		},
	}
	routeB := state.InternalRoute{
		Hostnames: []string{"b.example.com"},
		Rules: []state.InternalRule{
			{
				Matches:  []state.InternalMatch{{Path: &state.InternalPathMatch{Type: gatewayv1.PathMatchPathPrefix, Value: "/"}}},
				Backends: []state.InternalBackend{{Host: backendHost, Port: int32(backendPort), Weight: 1}},
			},
		},
	}
	routeC := state.InternalRoute{
		Hostnames: []string{"c.example.com"},
		Rules: []state.InternalRule{
			{
				Matches:  []state.InternalMatch{{Path: &state.InternalPathMatch{Type: gatewayv1.PathMatchPathPrefix, Value: "/"}}},
				Backends: []state.InternalBackend{{Host: backendHost, Port: int32(backendPort), Weight: 1}},
			},
		},
	}
	routeD := state.InternalRoute{
		Hostnames: []string{"d.example.com"},
		Rules: []state.InternalRule{
			{
				Matches:  []state.InternalMatch{{Path: &state.InternalPathMatch{Type: gatewayv1.PathMatchPathPrefix, Value: "/"}}},
				Backends: []state.InternalBackend{{Host: backendHost, Port: int32(backendPort), Weight: 1}},
			},
		},
	}

	// Configure listeners:
	// - a.example.com: HTTPS listener on port 8443 with CA1 validation (AllowValidOnly)
	// - b.example.com: HTTPS listener on port 8443 with CA2 validation (AllowValidOnly)
	// - c.example.com: HTTPS listener on port 8443 with NO client validation
	// - d.example.com: HTTP listener on port 8000
	listeners := []state.InternalListener{
		{
			Name:     "https-a",
			Protocol: gatewayv1.HTTPSProtocolType,
			Port:     8443,
			Hostname: "a.example.com",
			Routes:   []state.InternalRoute{routeA},
			ClientValidation: &state.InternalClientValidation{
				CACertPEMs: [][]byte{ca1PEM},
				Mode:       gatewayv1.AllowValidOnly,
			},
		},
		{
			Name:     "https-b",
			Protocol: gatewayv1.HTTPSProtocolType,
			Port:     8443,
			Hostname: "b.example.com",
			Routes:   []state.InternalRoute{routeB},
			ClientValidation: &state.InternalClientValidation{
				CACertPEMs: [][]byte{ca2PEM},
				Mode:       gatewayv1.AllowValidOnly,
			},
		},
		{
			Name:     "https-c-unvalidated",
			Protocol: gatewayv1.HTTPSProtocolType,
			Port:     8443,
			Hostname: "c.example.com",
			Routes:   []state.InternalRoute{routeC},
		},
		{
			Name:     "http-d",
			Protocol: gatewayv1.HTTPProtocolType,
			Port:     8000,
			Hostname: "d.example.com",
			Routes:   []state.InternalRoute{routeD},
		},
	}

	routes := []state.InternalRoute{routeA, routeB, routeC, routeD}
	p.UpdateConfig(listeners, routes)

	// 5. Start HTTPS server with SNI listener and TLSConfig
	rawLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on TCP: %v", err)
	}
	defer rawLis.Close()

	sniLis := p.NewSNIListener(rawLis)
	defer sniLis.Close()

	tlsConfig := p.TLSConfig()
	httpsSrv := &http.Server{
		Handler:   p,
		TLSConfig: tlsConfig,
	}
	tlsLis := tls.NewListener(sniLis, tlsConfig)
	go func() {
		_ = httpsSrv.Serve(tlsLis)
	}()
	defer httpsSrv.Close()

	httpsAddr := rawLis.Addr().String()

	// 6. Start plain HTTP server
	httpRawLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen for HTTP: %v", err)
	}
	defer httpRawLis.Close()

	httpSrv := &http.Server{Handler: p}
	go func() {
		_ = httpSrv.Serve(httpRawLis)
	}()
	defer httpSrv.Close()

	httpAddr := httpRawLis.Addr().String()

	makeHTTPSRequest := func(serverName string, clientCert *tls.Certificate) (*http.Response, error) {
		var certs []tls.Certificate
		if clientCert != nil {
			certs = []tls.Certificate{*clientCert}
		}

		tr := &http.Transport{
			TLSClientConfig: &tls.Config{
				ServerName:   serverName,
				RootCAs:      serverCAPool,
				Certificates: certs,
			},
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, network, httpsAddr)
			},
		}
		defer tr.CloseIdleConnections()

		client := &http.Client{
			Transport: tr,
			Timeout:   3 * time.Second,
		}

		req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("https://%s:8443/", serverName), nil)
		if err != nil {
			return nil, err
		}
		req.Host = fmt.Sprintf("%s:8443", serverName)
		return client.Do(req)
	}

	t.Run("Valid client cert accepted", func(t *testing.T) {
		resp, err := makeHTTPSRequest("a.example.com", &client1Cert)
		if err != nil {
			t.Fatalf("expected request to succeed, got: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200 OK, got: %d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "backend-response-ok" {
			t.Errorf("expected body 'backend-response-ok', got %q", string(body))
		}
	})

	t.Run("Cert from another CA rejected at handshake", func(t *testing.T) {
		_, err := makeHTTPSRequest("a.example.com", &clientOtherCert)
		if err == nil {
			t.Fatalf("expected handshake failure with cert from other CA, but request succeeded")
		}
	})

	t.Run("No cert rejected at handshake", func(t *testing.T) {
		_, err := makeHTTPSRequest("a.example.com", nil)
		if err == nil {
			t.Fatalf("expected handshake failure when no client cert is provided, but request succeeded")
		}
	})

	t.Run("Two listeners with different CAs on one port distinguished by SNI", func(t *testing.T) {
		// a.example.com with CA1 -> success
		respA1, err := makeHTTPSRequest("a.example.com", &client1Cert)
		if err != nil {
			t.Fatalf("a.example.com with CA1 should succeed: %v", err)
		}
		respA1.Body.Close()
		if respA1.StatusCode != http.StatusOK {
			t.Errorf("a.example.com expected 200, got: %d", respA1.StatusCode)
		}

		// a.example.com with CA2 -> handshake rejected
		if _, err := makeHTTPSRequest("a.example.com", &client2Cert); err == nil {
			t.Fatalf("a.example.com with CA2 cert should be rejected at handshake")
		}

		// b.example.com with CA2 -> success
		respB2, err := makeHTTPSRequest("b.example.com", &client2Cert)
		if err != nil {
			t.Fatalf("b.example.com with CA2 should succeed: %v", err)
		}
		respB2.Body.Close()
		if respB2.StatusCode != http.StatusOK {
			t.Errorf("b.example.com expected 200, got: %d", respB2.StatusCode)
		}

		// b.example.com with CA1 -> handshake rejected
		if _, err := makeHTTPSRequest("b.example.com", &client1Cert); err == nil {
			t.Fatalf("b.example.com with CA1 cert should be rejected at handshake")
		}
	})

	t.Run("An HTTP listener and an HTTPS listener without validation untouched", func(t *testing.T) {
		// HTTPS listener without validation (c.example.com) succeeds with NO client cert
		respC, err := makeHTTPSRequest("c.example.com", nil)
		if err != nil {
			t.Fatalf("c.example.com without client cert failed: %v", err)
		}
		defer respC.Body.Close()
		if respC.StatusCode != http.StatusOK {
			t.Errorf("c.example.com expected 200, got: %d", respC.StatusCode)
		}

		// Plain HTTP listener (d.example.com) succeeds untouched
		req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/", httpAddr), nil)
		if err != nil {
			t.Fatalf("failed to create HTTP request: %v", err)
		}
		req.Host = "d.example.com"
		httpClient := &http.Client{Timeout: 3 * time.Second}
		respD, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("HTTP request to d.example.com failed: %v", err)
		}
		defer respD.Body.Close()
		if respD.StatusCode != http.StatusOK {
			t.Errorf("HTTP request to d.example.com expected 200, got: %d", respD.StatusCode)
		}
	})

	t.Run("Dynamic config update swaps CA atomically", func(t *testing.T) {
		// Swap: a.example.com now requires CA2 instead of CA1
		updatedListeners := []state.InternalListener{
			{
				Name:     "https-a",
				Protocol: gatewayv1.HTTPSProtocolType,
				Port:     8443,
				Hostname: "a.example.com",
				Routes:   []state.InternalRoute{routeA},
				ClientValidation: &state.InternalClientValidation{
					CACertPEMs: [][]byte{ca2PEM},
					Mode:       gatewayv1.AllowValidOnly,
				},
			},
		}
		p.UpdateConfig(updatedListeners, routes)

		// Now client1Cert should fail on a.example.com
		if _, err := makeHTTPSRequest("a.example.com", &client1Cert); err == nil {
			t.Fatalf("expected client1Cert to fail after CA swap")
		}

		// And client2Cert should succeed on a.example.com
		resp, err := makeHTTPSRequest("a.example.com", &client2Cert)
		if err != nil {
			t.Fatalf("expected client2Cert to succeed after CA swap: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200 OK after CA swap, got: %d", resp.StatusCode)
		}
	})
}
