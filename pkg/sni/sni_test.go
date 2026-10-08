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

package sni

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"
)

func TestSniffSNI(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	expectedSNI := "test.example.com"

	errCh := make(chan error, 1)
	sniCh := make(chan string, 1)
	peekedConnCh := make(chan net.Conn, 1)

	go func() {
		sni, peekedConn, err := SniffSNI(serverConn, 2*time.Second)
		if err != nil {
			errCh <- err
			return
		}
		sniCh <- sni
		peekedConnCh <- peekedConn
		errCh <- nil
	}()

	tlsClient := tls.Client(clientConn, &tls.Config{
		ServerName:         expectedSNI,
		InsecureSkipVerify: true,
	})

	// Start handshake in goroutine
	go func() {
		_ = tlsClient.Handshake()
	}()

	if err := <-errCh; err != nil {
		t.Fatalf("SniffSNI failed: %v", err)
	}

	gotSNI := <-sniCh
	if gotSNI != expectedSNI {
		t.Fatalf("expected SNI %q, got %q", expectedSNI, gotSNI)
	}

	peekedConn := <-peekedConnCh

	// Verify that peekedConn can be used to complete TLS handshake on server side!
	serverCert, err := tls.X509KeyPair(testCertPEM, testKeyPEM)
	if err != nil {
		t.Fatalf("parsing test cert/key: %v", err)
	}

	tlsServer := tls.Server(peekedConn, &tls.Config{
		Certificates: []tls.Certificate{serverCert},
	})

	serverHandshakeErr := make(chan error, 1)
	go func() {
		serverHandshakeErr <- tlsServer.Handshake()
	}()

	select {
	case err := <-serverHandshakeErr:
		if err != nil {
			t.Fatalf("server Handshake failed on peekedConn: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server handshake on peekedConn")
	}
}

func TestSniffSNI_NotTLS(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	go func() {
		_, _ = clientConn.Write([]byte("GET / HTTP/1.1\r\n\r\n"))
	}()

	_, peekedConn, err := SniffSNI(serverConn, 1*time.Second)
	if err == nil {
		t.Fatal("expected error for non-TLS connection, got nil")
	}

	buf := make([]byte, 16)
	n, _ := peekedConn.Read(buf)
	if string(buf[:n]) != "GET / HTTP/1.1\r\n" && string(buf[:n]) != "GET /" {
		if n == 0 {
			t.Fatal("expected peekedConn to preserve non-TLS bytes")
		}
	}
}

// In-memory self-signed cert/key for tests
var (
	testCertPEM = []byte(`-----BEGIN CERTIFICATE-----
MIIBFjCBvaADAgECAgEBMAoGCCqGSM49BAMCMBMxETAPBgNVBAMMCHNuaS10ZXN0
MB4XDTI2MDEwMTAwMDAwMFoXDTM2MDEwMTAwMDAwMFowEzERMA8GA1UEAwwIc25p
LXRlc3QwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAARdY0hI8j9gB2i9zQJz8gHz
8+qI3p2kF8p6HlZzW+aV1m0qI5c8Kz1N+9O+7qB0m1c5d0e5f6g7h8i9j0k1l2mA
MAoGCCqGSM49BAMCA0kAMEYCIQD/s6F0z2K4m8Q1f4a9b+xK5L7m1V3o5t7e1a3o
5s7t2AIhAM4w8o8a9v6c4z1x7k2b8m9o3v7a2e4c6k8l0n1p2q3r
-----END CERTIFICATE-----`)
	testKeyPEM = []byte(`-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIEt+6o8v5l7c3m9k2j8h1f4d7b0z2x5v8t1r4p7m0k3goAoGCCqGSM49
BAMCoAMDSwAwSAJBAIxdY0hI8j9gB2i9zQJz8gHz8+qI3p2kF8p6HlZzW+aV1m0q
I5c8Kz1N+9O+7qB0m1c5d0e5f6g7h8i9j0k1l2kCAQEB
-----END EC PRIVATE KEY-----`)
)

func init() {
	// Generate valid ephemeral cert pair so tests don't depend on static mock PEM
	cert, err := generateSelfSignedCert()
	if err == nil {
		testCertPEM = cert.certPEM
		testKeyPEM = cert.keyPEM
	}
}

type ephemeralCert struct {
	certPEM []byte
	keyPEM  []byte
}

func generateSelfSignedCert() (*ephemeralCert, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "test.example.com",
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"test.example.com"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		return nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	privBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privBytes})
	return &ephemeralCert{
		certPEM: certPEM,
		keyPEM:  keyPEM,
	}, nil
}
