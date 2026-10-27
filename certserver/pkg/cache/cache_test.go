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

package cache

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func generateSelfSignedCert(t *testing.T, hostnames []string) ([]byte, []byte, *x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("failed to generate serial number: %v", err)
	}

	now := time.Now().Truncate(time.Second)
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: hostnames[0],
		},
		DNSNames:              hostnames,
		NotBefore:             now,
		NotAfter:              now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("failed to parse certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: der,
	})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("failed to marshal key: %v", err)
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: keyDER,
	})

	return certPEM, keyPEM, leaf, key
}

func TestDiskCache_PermissionsAndLifecycle(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "certserver-cache-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tempDir)

	cacheDir := filepath.Join(tempDir, "certs")
	c, err := NewDiskCache(cacheDir)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	// Verify directory permissions 0700
	fi, err := os.Stat(cacheDir)
	if err != nil {
		t.Fatalf("Stat cache dir failed: %v", err)
	}
	if fi.Mode().Perm() != 0700 {
		t.Fatalf("expected cache dir perm 0700, got %o", fi.Mode().Perm())
	}

	hostnames := []string{"app.example.com", "example.com"}
	hostnamesKey := "app.example.com,example.com"
	certPEM, keyPEM, leaf, _ := generateSelfSignedCert(t, hostnames)

	cert := &Certificate{
		Hostnames:    hostnames,
		HostnamesKey: hostnamesKey,
		CertPEM:      certPEM,
		KeyPEM:       keyPEM,
		Leaf:         leaf,
		IssuedAt:     leaf.NotBefore,
		ExpiresAt:    leaf.NotAfter,
	}

	// 1. Put certificate
	if err := c.Put(cert); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify file permissions 0600 on disk
	h := hashKey(hostnamesKey)
	for _, ext := range []string{".crt", ".key", ".meta"} {
		fPath := filepath.Join(cacheDir, h+ext)
		info, err := os.Stat(fPath)
		if err != nil {
			t.Fatalf("Stat %s failed: %v", fPath, err)
		}
		if info.Mode().Perm() != 0600 {
			t.Errorf("file %s expected perm 0600, got %o", fPath, info.Mode().Perm())
		}
	}

	// 2. Get certificate
	got, err := c.Get(hostnamesKey)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.HostnamesKey != hostnamesKey {
		t.Errorf("expected hostnamesKey %s, got %s", hostnamesKey, got.HostnamesKey)
	}

	// 3. Restart simulation: create a new DiskCache instance and Load()
	c2, err := NewDiskCache(cacheDir)
	if err != nil {
		t.Fatalf("NewDiskCache for restart failed: %v", err)
	}
	if err := c2.Load(); err != nil {
		t.Fatalf("Load on restart failed: %v", err)
	}

	gotAfterRestart, err := c2.Get(hostnamesKey)
	if err != nil {
		t.Fatalf("Get after restart failed: %v", err)
	}
	if gotAfterRestart.HostnamesKey != hostnamesKey {
		t.Errorf("expected %s, got %s", hostnamesKey, gotAfterRestart.HostnamesKey)
	}
	if gotAfterRestart.Leaf.SerialNumber.Cmp(leaf.SerialNumber) != 0 {
		t.Errorf("serial numbers do not match after restart")
	}

	// 4. Delete
	if err := c2.Delete(hostnamesKey); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, err = c2.Get(hostnamesKey)
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after Delete, got %v", err)
	}
}

func TestCertificate_PrivateKeySafety(t *testing.T) {
	hostnames := []string{"safe.example.com"}
	certPEM, keyPEM, leaf, _ := generateSelfSignedCert(t, hostnames)

	cert := &Certificate{
		Hostnames:    hostnames,
		HostnamesKey: "safe.example.com",
		CertPEM:      certPEM,
		KeyPEM:       keyPEM,
		Leaf:         leaf,
		IssuedAt:     leaf.NotBefore,
		ExpiresAt:    leaf.NotAfter,
	}

	str := cert.String()
	goStr := fmt.Sprintf("%#v", cert)

	// Verify private key is never in String() or GoString()
	if strings.Contains(str, "PRIVATE KEY") || strings.Contains(str, string(keyPEM)) {
		t.Fatalf("private key found in String(): %s", str)
	}
	if strings.Contains(goStr, "PRIVATE KEY") || strings.Contains(goStr, string(keyPEM)) {
		t.Fatalf("private key found in GoString(): %s", goStr)
	}
}
