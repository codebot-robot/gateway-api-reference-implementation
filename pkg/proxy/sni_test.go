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
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/sni"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestSNIListener_PassthroughAndHTTPSAndFailClosed(t *testing.T) {
	// 1. Backend TLS server for passthrough route
	backendCAPEM, backendTLSCert := generateTestCAAndCert(t, []string{"passthrough.example.com"}, nil)
	backendServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("backend-passthrough-response"))
	}))
	backendServer.TLS = &tls.Config{Certificates: []tls.Certificate{backendTLSCert}}
	backendServer.StartTLS()
	defer backendServer.Close()

	backendURL, err := url.Parse(backendServer.URL)
	if err != nil {
		t.Fatalf("failed to parse backend server URL: %v", err)
	}
	backendHost, backendPortStr, err := net.SplitHostPort(backendURL.Host)
	if err != nil {
		t.Fatalf("failed to split backend host port: %v", err)
	}

	// 2. HTTPS terminating backend for proxy HTTPRoute
	httpsTermBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("https-terminated-response"))
	}))
	defer httpsTermBackend.Close()

	termURL, err := url.Parse(httpsTermBackend.URL)
	if err != nil {
		t.Fatalf("failed to parse term backend URL: %v", err)
	}
	termHost, termPortStr, _ := net.SplitHostPort(termURL.Host)
	termPort, _ := strconv.Atoi(termPortStr)

	// 3. Proxy certificates
	proxyCAPEM, proxyTLSCert := generateTestCAAndCert(t, []string{"https.example.com"}, nil)

	p := NewProxy()
	p.UpdateCertificates(map[string]*tls.Certificate{
		"https.example.com": &proxyTLSCert,
	}, &proxyTLSCert)

	// 4. Configure listeners on proxy:
	// - One TLS Passthrough listener for passthrough.example.com
	// - One HTTPS listener for https.example.com
	passthroughMode := gatewayv1.TLSModePassthrough
	httpsRoutes := []state.InternalRoute{
		{
			Hostnames: []string{"https.example.com"},
			Rules: []state.InternalRule{
				{
					Matches: []state.InternalMatch{
						{
							Path: &state.InternalPathMatch{
								Type:  gatewayv1.PathMatchPathPrefix,
								Value: "/",
							},
						},
					},
					Backends: []state.InternalBackend{
						{
							Host:   termHost,
							Port:   int32(termPort),
							Weight: 1,
						},
					},
				},
			},
		},
	}

	p.UpdateConfig(
		[]state.InternalListener{
			{
				Name:        "tls-passthrough",
				Protocol:    gatewayv1.TLSProtocolType,
				Port:        443,
				Hostname:    "passthrough.example.com",
				TLSMode:     &passthroughMode,
				TLSBackends: map[string][]string{"passthrough.example.com": {net.JoinHostPort(backendHost, backendPortStr)}},
			},
			{
				Name:     "https-terminating",
				Protocol: gatewayv1.HTTPSProtocolType,
				Port:     443,
				Hostname: "https.example.com",
				Routes:   httpsRoutes,
			},
		},
		httpsRoutes,
	)

	// 5. Start raw TCP listener with SNI front
	rawLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on TCP: %v", err)
	}
	defer rawLis.Close()

	sniLis := p.NewSNIListener(rawLis)
	defer sniLis.Close()

	tlsConfig := &tls.Config{
		GetCertificate: p.GetCertificate,
	}
	httpsSrv := &http.Server{
		Handler:   p,
		TLSConfig: tlsConfig,
	}
	tlsLis := tls.NewListener(sniLis, tlsConfig)
	go func() {
		_ = httpsSrv.Serve(tlsLis)
	}()
	defer httpsSrv.Close()

	addr := rawLis.Addr().String()

	// 6. Test Passthrough Route
	t.Run("Passthrough route presents backend cert unterminated", func(t *testing.T) {
		backendCertPool := x509.NewCertPool()
		backendCertPool.AppendCertsFromPEM(backendCAPEM)

		conn, err := tls.Dial("tcp", addr, &tls.Config{
			ServerName: "passthrough.example.com",
			RootCAs:    backendCertPool,
		})
		if err != nil {
			t.Fatalf("failed to connect to passthrough SNI: %v", err)
		}
		defer conn.Close()

		// Verify backend's certificate was presented
		state := conn.ConnectionState()
		if len(state.PeerCertificates) == 0 {
			t.Fatal("expected peer certificates from backend")
		}
		leaf := state.PeerCertificates[0]
		foundDNS := false
		for _, dns := range leaf.DNSNames {
			if dns == "passthrough.example.com" {
				foundDNS = true
				break
			}
		}
		if !foundDNS {
			t.Errorf("expected peer cert to have DNS passthrough.example.com, got %v", leaf.DNSNames)
		}

		// Make HTTP request over the connection
		req := "GET / HTTP/1.1\r\nHost: passthrough.example.com\r\nConnection: close\r\n\r\n"
		if _, err := conn.Write([]byte(req)); err != nil {
			t.Fatalf("failed to write HTTP request: %v", err)
		}
		respBytes, err := io.ReadAll(conn)
		if err != nil {
			t.Fatalf("failed to read HTTP response: %v", err)
		}
		if !strings.Contains(string(respBytes), "backend-passthrough-response") {
			t.Errorf("expected response from passthrough backend, got %q", string(respBytes))
		}
	})

	// 7. Test HTTPS Terminating Route
	t.Run("HTTPS route terminates and serves with proxy cert", func(t *testing.T) {
		proxyCertPool := x509.NewCertPool()
		proxyCertPool.AppendCertsFromPEM(proxyCAPEM)

		client := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					ServerName: "https.example.com",
					RootCAs:    proxyCertPool,
				},
				DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "tcp", addr)
				},
			},
			Timeout: 2 * time.Second,
		}

		resp, err := client.Get("https://https.example.com/")
		if err != nil {
			t.Fatalf("failed to GET from HTTPS listener: %v", err)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected status 200, got %d", resp.StatusCode)
		}
		if string(body) != "https-terminated-response" {
			t.Errorf("expected response %q, got %q", "https-terminated-response", string(body))
		}
	})

	// 8. Test Non-matching SNI is closed without response (Fail closed)
	t.Run("Non-matching SNI is closed without response", func(t *testing.T) {
		dialer := &net.Dialer{Timeout: 2 * time.Second}
		conn, err := tls.DialWithDialer(dialer, "tcp", addr, &tls.Config{
			ServerName:         "unknown.example.com",
			InsecureSkipVerify: true,
		})
		if err == nil {
			conn.Close()
			t.Fatal("expected connection rejection for unknown SNI, but connection succeeded")
		}
	})
}

func TestSNIListener_ShutdownMidHandshakeRace(t *testing.T) {
	_, proxyTLSCert := generateTestCAAndCert(t, []string{"https.example.com"}, nil)

	p := NewProxy()
	p.UpdateCertificates(map[string]*tls.Certificate{
		"https.example.com": &proxyTLSCert,
	}, &proxyTLSCert)

	p.UpdateConfig(
		[]state.InternalListener{
			{
				Name:     "https",
				Protocol: gatewayv1.HTTPSProtocolType,
				Port:     443,
				Hostname: "https.example.com",
			},
		},
		nil,
	)

	rawLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	sniLis := p.NewSNIListener(rawLis)
	addr := rawLis.Addr().String()

	var wg sync.WaitGroup

	// Background worker accepting connections from sniLis
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			conn, err := sniLis.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	// Spawn multiple clients that initiate TLS handshakes concurrently
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dialer := &net.Dialer{Timeout: 1 * time.Second}
			conn, err := tls.DialWithDialer(dialer, "tcp", addr, &tls.Config{
				ServerName:         "https.example.com",
				InsecureSkipVerify: true,
			})
			if err == nil {
				_ = conn.Close()
			}
		}()
	}

	// Close the listener while handshakes are mid-flight
	time.Sleep(2 * time.Millisecond)
	_ = sniLis.Close()

	wg.Wait()
	<-acceptDone
}

func TestSNIListener_PassthroughNoResolvableBackendClosed(t *testing.T) {
	passthroughMode := gatewayv1.TLSModePassthrough

	p := NewProxy()
	p.UpdateConfig(
		[]state.InternalListener{
			{
				Name:        "tls-passthrough",
				Protocol:    gatewayv1.TLSProtocolType,
				Port:        443,
				Hostname:    "passthrough.example.com",
				TLSMode:     &passthroughMode,
				TLSBackends: nil, // Passthrough listener with no resolvable backend
			},
		},
		nil,
	)

	rawLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on TCP: %v", err)
	}
	defer rawLis.Close()

	sniLis := p.NewSNIListener(rawLis)
	defer sniLis.Close()

	addr := rawLis.Addr().String()

	// Dial TLS with SNI matching the Passthrough listener
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	conn, err := tls.DialWithDialer(dialer, "tcp", addr, &tls.Config{
		ServerName:         "passthrough.example.com",
		InsecureSkipVerify: true,
	})
	if err == nil {
		conn.Close()
		t.Fatal("expected connection rejection when passthrough listener has no resolvable backend, but connection succeeded")
	}
}

func TestSNIListener_BypassToggle(t *testing.T) {
	_, proxyTLSCert := generateTestCAAndCert(t, []string{"https.example.com"}, nil)

	p := NewProxy()
	p.UpdateCertificates(map[string]*tls.Certificate{
		"https.example.com": &proxyTLSCert,
	}, &proxyTLSCert)

	httpsListener := state.InternalListener{
		Name:     "https-listener",
		Protocol: gatewayv1.HTTPSProtocolType,
		Port:     443,
		Hostname: "https.example.com",
	}

	backendCAPEM, backendTLSCert := generateTestCAAndCert(t, []string{"passthrough.example.com"}, nil)
	backendServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("passthrough-ok"))
	}))
	backendServer.TLS = &tls.Config{Certificates: []tls.Certificate{backendTLSCert}}
	backendServer.StartTLS()
	defer backendServer.Close()

	backendURL, err := url.Parse(backendServer.URL)
	if err != nil {
		t.Fatalf("failed to parse backend server URL: %v", err)
	}
	backendHost, backendPortStr, err := net.SplitHostPort(backendURL.Host)
	if err != nil {
		t.Fatalf("failed to split backend host port: %v", err)
	}

	passthroughMode := gatewayv1.TLSModePassthrough
	passthroughListener := state.InternalListener{
		Name:        "tls-passthrough",
		Protocol:    gatewayv1.TLSProtocolType,
		Port:        443,
		Hostname:    "passthrough.example.com",
		TLSMode:     &passthroughMode,
		TLSBackends: map[string][]string{"passthrough.example.com": {net.JoinHostPort(backendHost, backendPortStr)}},
	}

	// 1. Initial config: no Passthrough listeners (HTTPS only)
	p.UpdateConfig([]state.InternalListener{httpsListener}, nil)

	rawLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer rawLis.Close()

	sniLis := p.NewSNIListener(rawLis)
	defer sniLis.Close()

	addr := rawLis.Addr().String()

	// Step 1: With no Passthrough listeners, Accept returns raw connection (not *sni.PeekedConn)
	t.Run("Initially no Passthrough: returns raw connection", func(t *testing.T) {
		clientConn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("failed to dial: %v", err)
		}
		defer clientConn.Close()

		acceptedConn, err := sniLis.Accept()
		if err != nil {
			t.Fatalf("Accept failed: %v", err)
		}
		defer acceptedConn.Close()

		if _, ok := acceptedConn.(*sni.PeekedConn); ok {
			t.Fatalf("expected raw connection, got *sni.PeekedConn")
		}
	})

	// Step 2: Add Passthrough listener via UpdateConfig: switches path ON without restarting listener
	t.Run("Add Passthrough: switches path ON", func(t *testing.T) {
		p.UpdateConfig([]state.InternalListener{httpsListener, passthroughListener}, nil)

		// First, verify passthrough traffic is spliced to backend
		backendCertPool := x509.NewCertPool()
		backendCertPool.AppendCertsFromPEM(backendCAPEM)
		ptConn, err := tls.Dial("tcp", addr, &tls.Config{
			ServerName: "passthrough.example.com",
			RootCAs:    backendCertPool,
		})
		if err != nil {
			t.Fatalf("failed to dial passthrough SNI: %v", err)
		}
		defer ptConn.Close()

		req := "GET / HTTP/1.1\r\nHost: passthrough.example.com\r\nConnection: close\r\n\r\n"
		if _, err := ptConn.Write([]byte(req)); err != nil {
			t.Fatalf("failed to write request: %v", err)
		}
		resp, err := io.ReadAll(ptConn)
		if err != nil {
			t.Fatalf("failed to read response: %v", err)
		}
		if !strings.Contains(string(resp), "passthrough-ok") {
			t.Errorf("expected passthrough-ok response, got %q", string(resp))
		}

		// Second, verify HTTPS traffic is intercepted, sniffed, and returned as *sni.PeekedConn
		go func() {
			dialer := &net.Dialer{Timeout: 2 * time.Second}
			c, err := tls.DialWithDialer(dialer, "tcp", addr, &tls.Config{
				ServerName:         "https.example.com",
				InsecureSkipVerify: true,
			})
			if err == nil {
				_ = c.Close()
			}
		}()

		acceptedConn, err := sniLis.Accept()
		if err != nil {
			t.Fatalf("Accept failed: %v", err)
		}
		defer acceptedConn.Close()

		if _, ok := acceptedConn.(*sni.PeekedConn); !ok {
			t.Fatalf("expected *sni.PeekedConn when passthrough is active, got %T", acceptedConn)
		}
	})

	// Step 3: Remove Passthrough listener via UpdateConfig: switches path OFF again
	t.Run("Remove Passthrough: switches path OFF", func(t *testing.T) {
		p.UpdateConfig([]state.InternalListener{httpsListener}, nil)

		clientConn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("failed to dial: %v", err)
		}
		defer clientConn.Close()

		acceptedConn, err := sniLis.Accept()
		if err != nil {
			t.Fatalf("Accept failed: %v", err)
		}
		defer acceptedConn.Close()

		if _, ok := acceptedConn.(*sni.PeekedConn); ok {
			t.Fatalf("expected raw connection after removing passthrough, got *sni.PeekedConn")
		}

		// Subsequent connection should also be raw
		clientConn2, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("failed to dial 2nd connection: %v", err)
		}
		defer clientConn2.Close()

		acceptedConn2, err := sniLis.Accept()
		if err != nil {
			t.Fatalf("Accept 2nd failed: %v", err)
		}
		defer acceptedConn2.Close()

		if _, ok := acceptedConn2.(*sni.PeekedConn); ok {
			t.Fatalf("expected raw connection on subsequent accept, got *sni.PeekedConn")
		}
	})
}

func BenchmarkSNIListener_HTTPSOnly(b *testing.B) {
	_, proxyTLSCert := generateTestCAAndCert(b, []string{"https.example.com"}, nil)

	p := NewProxy()
	p.UpdateCertificates(map[string]*tls.Certificate{
		"https.example.com": &proxyTLSCert,
	}, &proxyTLSCert)

	httpsRoutes := []state.InternalRoute{
		{
			Hostnames: []string{"https.example.com"},
			Rules: []state.InternalRule{
				{
					Backends: []state.InternalBackend{
						{Host: "127.0.0.1", Port: 8080, Weight: 1},
					},
				},
			},
		},
	}
	p.UpdateConfig(
		[]state.InternalListener{
			{
				Name:     "https",
				Protocol: gatewayv1.HTTPSProtocolType,
				Port:     443,
				Hostname: "https.example.com",
				Routes:   httpsRoutes,
			},
		},
		httpsRoutes,
	)

	rawLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to listen: %v", err)
	}
	defer rawLis.Close()

	sniLis := p.NewSNIListener(rawLis)
	defer sniLis.Close()

	tlsConfig := &tls.Config{
		GetCertificate: p.GetCertificate,
	}
	tlsLis := tls.NewListener(sniLis, tlsConfig)
	defer tlsLis.Close()

	go func() {
		for {
			conn, err := tlsLis.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				var buf [1]byte
				_, _ = c.Read(buf[:])
			}(conn)
		}
	}()

	addr := rawLis.Addr().String()
	clientTLSConfig := &tls.Config{
		ServerName:         "https.example.com",
		InsecureSkipVerify: true,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn, err := tls.Dial("tcp", addr, clientTLSConfig)
		if err != nil {
			b.Fatalf("dial failed: %v", err)
		}
		_, _ = conn.Write([]byte("x"))
		_ = conn.Close()
	}
}
