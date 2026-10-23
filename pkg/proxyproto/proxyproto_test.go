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

package proxyproto

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestProxyProtocolV1_TCP4(t *testing.T) {
	raw := "PROXY TCP4 192.0.2.1 198.51.100.1 12345 80\r\n"
	hdr, err := Decode(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if hdr.Version != 1 {
		t.Errorf("expected Version 1, got %d", hdr.Version)
	}
	if hdr.Command != CommandProxy {
		t.Errorf("expected CommandProxy, got %d", hdr.Command)
	}
	if hdr.SrcAddr == nil || hdr.SrcAddr.String() != "192.0.2.1:12345" {
		t.Errorf("expected SrcAddr 192.0.2.1:12345, got %v", hdr.SrcAddr)
	}
	if hdr.DstAddr == nil || hdr.DstAddr.String() != "198.51.100.1:80" {
		t.Errorf("expected DstAddr 198.51.100.1:80, got %v", hdr.DstAddr)
	}

	formatted, err := hdr.Format()
	if err != nil {
		t.Fatalf("Format failed: %v", err)
	}
	if string(formatted) != raw {
		t.Errorf("expected formatted %q, got %q", raw, string(formatted))
	}
}

func TestProxyProtocolV1_TCP6(t *testing.T) {
	raw := "PROXY TCP6 2001:db8::1 2001:db8::2 54321 443\r\n"
	hdr, err := Decode(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if hdr.Version != 1 {
		t.Errorf("expected Version 1, got %d", hdr.Version)
	}
	if hdr.Command != CommandProxy {
		t.Errorf("expected CommandProxy, got %d", hdr.Command)
	}
	if hdr.SrcAddr == nil || hdr.SrcAddr.String() != "[2001:db8::1]:54321" {
		t.Errorf("expected SrcAddr [2001:db8::1]:54321, got %v", hdr.SrcAddr)
	}
	if hdr.DstAddr == nil || hdr.DstAddr.String() != "[2001:db8::2]:443" {
		t.Errorf("expected DstAddr [2001:db8::2]:443, got %v", hdr.DstAddr)
	}

	formatted, err := hdr.Format()
	if err != nil {
		t.Fatalf("Format failed: %v", err)
	}
	if string(formatted) != raw {
		t.Errorf("expected formatted %q, got %q", raw, string(formatted))
	}
}

func TestProxyProtocolV1_UNKNOWN(t *testing.T) {
	tests := []string{
		"PROXY UNKNOWN\r\n",
		"PROXY UNKNOWN something else here\r\n",
	}

	for _, raw := range tests {
		hdr, err := Decode(strings.NewReader(raw))
		if err != nil {
			t.Fatalf("Decode(%q) failed: %v", raw, err)
		}
		if hdr.Version != 1 {
			t.Errorf("expected Version 1, got %d", hdr.Version)
		}
		if hdr.Command != CommandLocal {
			t.Errorf("expected CommandLocal, got %d", hdr.Command)
		}
		if hdr.SrcAddr != nil {
			t.Errorf("expected nil SrcAddr, got %v", hdr.SrcAddr)
		}
		formatted, err := hdr.Format()
		if err != nil {
			t.Fatalf("Format failed: %v", err)
		}
		if string(formatted) != "PROXY UNKNOWN\r\n" {
			t.Errorf("expected 'PROXY UNKNOWN\\r\\n', got %q", string(formatted))
		}
	}
}

func TestProxyProtocolV1_InvalidHeaders(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{"missing CRLF", "PROXY TCP4 1.2.3.4 5.6.7.8 1 2"},
		{"over 107 bytes", "PROXY TCP4 " + strings.Repeat("a", 110) + "\r\n"},
		{"invalid proto", "PROXY UDP4 1.2.3.4 5.6.7.8 1 2\r\n"},
		{"invalid src IP", "PROXY TCP4 999.999.999.999 5.6.7.8 1 2\r\n"},
		{"invalid port", "PROXY TCP4 1.2.3.4 5.6.7.8 99999 2\r\n"},
		{"non-numeric port", "PROXY TCP4 1.2.3.4 5.6.7.8 abc 2\r\n"},
		{"missing tokens", "PROXY TCP4 1.2.3.4 5.6.7.8\r\n"},
		{"truncated", "PROXY "},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Decode(strings.NewReader(tc.raw))
			if err == nil {
				t.Fatalf("expected error for %q, got nil", tc.raw)
			}
		})
	}
}

func TestProxyProtocolV2_IPv4(t *testing.T) {
	src := &net.TCPAddr{
		IP:   net.ParseIP("192.0.2.1"),
		Port: 12345,
	}
	dst := &net.TCPAddr{
		IP:   net.ParseIP("198.51.100.1"),
		Port: 443,
	}

	orig := &Header{
		Command:   CommandProxy,
		SrcAddr:   src,
		DstAddr:   dst,
		Authority: "echo.example.com",
	}

	data, err := orig.Format()
	if err != nil {
		t.Fatalf("Format failed: %v", err)
	}

	parsed, err := Decode(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if parsed.Version != 2 {
		t.Fatalf("expected version 2, got %d", parsed.Version)
	}
	if parsed.Command != CommandProxy {
		t.Fatalf("expected CommandProxy, got %d", parsed.Command)
	}
	if parsed.SrcAddr.String() != src.String() {
		t.Fatalf("expected SrcAddr %s, got %s", src.String(), parsed.SrcAddr.String())
	}
	if parsed.DstAddr.String() != dst.String() {
		t.Fatalf("expected DstAddr %s, got %s", dst.String(), parsed.DstAddr.String())
	}
	if parsed.Authority != "echo.example.com" {
		t.Fatalf("expected Authority echo.example.com, got %s", parsed.Authority)
	}
}

func TestProxyProtocolV2_IPv6(t *testing.T) {
	src := &net.TCPAddr{
		IP:   net.ParseIP("2001:db8::1"),
		Port: 54321,
	}
	dst := &net.TCPAddr{
		IP:   net.ParseIP("2001:db8::2"),
		Port: 8443,
	}

	orig := &Header{
		Command:   CommandProxy,
		SrcAddr:   src,
		DstAddr:   dst,
		Authority: "ipv6.example.org",
	}

	data, err := orig.Format()
	if err != nil {
		t.Fatalf("Format failed: %v", err)
	}

	parsed, err := Decode(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if parsed.SrcAddr.String() != src.String() {
		t.Fatalf("expected SrcAddr %s, got %s", src.String(), parsed.SrcAddr.String())
	}
	if parsed.DstAddr.String() != dst.String() {
		t.Fatalf("expected DstAddr %s, got %s", dst.String(), parsed.DstAddr.String())
	}
	if parsed.Authority != "ipv6.example.org" {
		t.Fatalf("expected Authority ipv6.example.org, got %s", parsed.Authority)
	}
}

func TestProxyProtocolV2_Unspec(t *testing.T) {
	orig := &Header{
		Command:   CommandProxy,
		Authority: "sni.only.com",
	}

	data, err := orig.Format()
	if err != nil {
		t.Fatalf("Format failed: %v", err)
	}

	parsed, err := Decode(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if parsed.Authority != "sni.only.com" {
		t.Fatalf("expected Authority sni.only.com, got %s", parsed.Authority)
	}
}

func TestProxyProtocolV2_InvalidSignature(t *testing.T) {
	invalid := []byte("INVALID_PROXY_HEADER_1234567890")
	_, err := Decode(bytes.NewReader(invalid))
	if err == nil {
		t.Fatalf("expected error for invalid signature, got nil")
	}
}

type failingWriter struct {
	failAfter int
	written   int
}

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.written >= w.failAfter {
		return 0, errors.New("writer failure")
	}
	remaining := w.failAfter - w.written
	if len(p) > remaining {
		w.written += remaining
		return remaining, errors.New("writer failure")
	}
	w.written += len(p)
	return len(p), nil
}

func TestProxyProtocolV2_EncodeError(t *testing.T) {
	orig := &Header{
		Command:   CommandProxy,
		Authority: "echo.example.com",
	}

	fw := &failingWriter{failAfter: 5}
	if err := Encode(fw, orig); err == nil {
		t.Fatalf("expected error from Encode with failing writer, got nil")
	}
}

func generateTestCert(t *testing.T) tls.Certificate {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate private key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"GARI Test"},
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost", "test.example.com"},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  priv,
	}
}

func TestListener_AllowedSourceCIDRs_RejectsBeforeTLS(t *testing.T) {
	rawLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	defer rawLis.Close()

	// Only allow 10.0.0.0/8 (127.0.0.1 is rejected)
	filterLis, err := NewListener(rawLis, ListenerOptions{
		AllowedSourceCIDRs: []string{"10.0.0.0/8"},
	})
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}

	serverCert := generateTestCert(t)
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
	}
	tlsLis := tls.NewListener(filterLis, tlsConfig)
	defer tlsLis.Close()

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := tlsLis.Accept()
		if err == nil {
			conn.Close()
			t.Errorf("expected tlsLis.Accept() not to accept disallowed connection")
		}
	}()

	// Connect from 127.0.0.1 (not in 10.0.0.0/8)
	clientConn, err := net.Dial("tcp", rawLis.Addr().String())
	if err != nil {
		t.Fatalf("net.Dial failed: %v", err)
	}
	defer clientConn.Close()

	// Read from connection: should be immediately closed by server (EOF) without any TLS bytes
	buf := make([]byte, 100)
	n, readErr := clientConn.Read(buf)
	if n != 0 || readErr != io.EOF {
		t.Errorf("expected immediate EOF before TLS bytes, got n=%d, err=%v", n, readErr)
	}

	tlsLis.Close()
	<-serverDone
}

func TestListener_TrustedSource_PROXYHonoured_HTTPRemoteAddr(t *testing.T) {
	rawLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	defer rawLis.Close()

	// 127.0.0.1 is in allowed and trusted CIDRs
	filterLis, err := NewListener(rawLis, ListenerOptions{
		AllowedSourceCIDRs:        []string{"127.0.0.0/8"},
		ProxyProtocolTrustedCIDRs: []string{"127.0.0.0/8"},
		ProxyProtocol:             "enabled",
	})
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}

	var recordedRemoteAddr string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recordedRemoteAddr = r.RemoteAddr
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{Handler: handler}
	go func() {
		_ = srv.Serve(filterLis)
	}()
	defer srv.Close()

	conn, err := net.Dial("tcp", rawLis.Addr().String())
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	// Send PROXY v1 header followed by HTTP request
	proxyHeader := "PROXY TCP4 203.0.113.195 198.51.100.1 54321 80\r\n"
	httpRequest := "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
	if _, err := conn.Write([]byte(proxyHeader + httpRequest)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("ReadResponse failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	expectedPrefix := "203.0.113.195:"
	if !strings.HasPrefix(recordedRemoteAddr, expectedPrefix) {
		t.Errorf("expected RemoteAddr to start with %q, got %q", expectedPrefix, recordedRemoteAddr)
	}
}

func TestListener_UntrustedSource_PROXYNotHonoured(t *testing.T) {
	rawLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	defer rawLis.Close()

	// 127.0.0.1 is in allowed CIDRs, but NOT in trusted CIDRs
	filterLis, err := NewListener(rawLis, ListenerOptions{
		AllowedSourceCIDRs:        []string{"127.0.0.0/8"},
		ProxyProtocolTrustedCIDRs: []string{"10.0.0.0/8"}, // 127.0.0.1 is NOT trusted!
		ProxyProtocol:             "enabled",
	})
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}

	var recordedRemoteAddr string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recordedRemoteAddr = r.RemoteAddr
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{Handler: handler}
	go func() {
		_ = srv.Serve(filterLis)
	}()
	defer srv.Close()

	// 1. Send normal HTTP request without PROXY header: should succeed with real 127.0.0.1 address
	conn, err := net.Dial("tcp", rawLis.Addr().String())
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	httpRequest := "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
	if _, err := conn.Write([]byte(httpRequest)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("ReadResponse failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	if !strings.HasPrefix(recordedRemoteAddr, "127.0.0.1:") {
		t.Errorf("expected RemoteAddr to start with 127.0.0.1:, got %q", recordedRemoteAddr)
	}

	// 2. Send spoofed PROXY header from untrusted source:
	// Listener does NOT strip PROXY header, so HTTP server receives "PROXY TCP4..." as HTTP request line,
	// which is invalid HTTP (returns 400 Bad Request) and does NOT rewrite client IP!
	conn2, err := net.Dial("tcp", rawLis.Addr().String())
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn2.Close()

	spoofedHeader := "PROXY TCP4 203.0.113.195 198.51.100.1 54321 80\r\n"
	if _, err := conn2.Write([]byte(spoofedHeader + httpRequest)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	resp2, err := http.ReadResponse(bufio.NewReader(conn2), nil)
	if err != nil {
		// Connection closed or 400 is fine
		return
	}
	resp2.Body.Close()
	if resp2.StatusCode == http.StatusOK {
		t.Errorf("untrusted PROXY header should not produce HTTP 200 OK")
	}
}

func TestListener_IdleTrustedConnectionDoesNotDelayAccept(t *testing.T) {
	rawLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	defer rawLis.Close()

	filterLis, err := NewListener(rawLis, ListenerOptions{
		AllowedSourceCIDRs:        []string{"127.0.0.0/8"},
		ProxyProtocolTrustedCIDRs: []string{"127.0.0.0/8"},
		ProxyProtocol:             "enabled",
	})
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer filterLis.Close()

	// Connect Client 1 and send NO bytes (idle connection)
	client1, err := net.Dial("tcp", rawLis.Addr().String())
	if err != nil {
		t.Fatalf("client1 dial failed: %v", err)
	}
	defer client1.Close()

	// Accept Client 1 immediately
	startAccept1 := time.Now()
	conn1, err := filterLis.Accept()
	if err != nil {
		t.Fatalf("Accept client1 failed: %v", err)
	}
	defer conn1.Close()
	if dur := time.Since(startAccept1); dur > 500*time.Millisecond {
		t.Errorf("Accept client1 was delayed: %v", dur)
	}

	// Connect Client 2 while Client 1 is still completely idle
	client2, err := net.Dial("tcp", rawLis.Addr().String())
	if err != nil {
		t.Fatalf("client2 dial failed: %v", err)
	}
	defer client2.Close()

	// Accept Client 2 must return immediately without waiting for Client 1's header!
	startAccept2 := time.Now()
	conn2, err := filterLis.Accept()
	if err != nil {
		t.Fatalf("Accept client2 failed: %v", err)
	}
	defer conn2.Close()
	if dur := time.Since(startAccept2); dur > 500*time.Millisecond {
		t.Errorf("Accept client2 was delayed by idle client1: %v", dur)
	}

	// Client 2 sends PROXY header and payload
	go func() {
		_, _ = client2.Write([]byte("PROXY TCP4 203.0.113.1 127.0.0.1 12345 80\r\nhello"))
	}()

	buf := make([]byte, 5)
	n, err := conn2.Read(buf)
	if err != nil || string(buf[:n]) != "hello" {
		t.Errorf("expected 'hello' from conn2, got %q, err=%v", string(buf[:n]), err)
	}
	if !strings.HasPrefix(conn2.RemoteAddr().String(), "203.0.113.1:12345") {
		t.Errorf("expected RemoteAddr 203.0.113.1:12345, got %v", conn2.RemoteAddr())
	}
}

func TestListener_UpdateOptionsClearing(t *testing.T) {
	rawLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	defer rawLis.Close()

	// Initially only allow 10.0.0.0/8 (127.0.0.1 is rejected)
	filterLis, err := NewListener(rawLis, ListenerOptions{
		AllowedSourceCIDRs: []string{"10.0.0.0/8"},
	})
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer filterLis.Close()

	serverAccepted := make(chan net.Conn, 1)
	go func() {
		for {
			c, err := filterLis.Accept()
			if err != nil {
				return
			}
			serverAccepted <- c
		}
	}()

	// Dial from 127.0.0.1: should be closed immediately by Accept() loop
	client1, err := net.Dial("tcp", rawLis.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer client1.Close()

	buf := make([]byte, 10)
	n, readErr := client1.Read(buf)
	if n != 0 || readErr != io.EOF {
		t.Errorf("expected EOF for disallowed source, got n=%d, err=%v", n, readErr)
	}

	// Clear allowedSourceCIDRs by updating with empty list
	if err := filterLis.UpdateOptions(ListenerOptions{}); err != nil {
		t.Fatalf("UpdateOptions clearing failed: %v", err)
	}

	// Dial from 127.0.0.1 again: now it should be accepted!
	client2, err := net.Dial("tcp", rawLis.Addr().String())
	if err != nil {
		t.Fatalf("dial after clear failed: %v", err)
	}
	defer client2.Close()

	select {
	case sConn := <-serverAccepted:
		if sConn == nil {
			t.Fatalf("expected accepted connection after clearing allowlist, got nil")
		}
		sConn.Close()
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for accept after clearing allowlist")
	}
}
