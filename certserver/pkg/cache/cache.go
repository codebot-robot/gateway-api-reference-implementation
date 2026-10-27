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
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	// ErrNotFound indicates that the requested certificate was not found in cache.
	ErrNotFound = errors.New("certificate not found in cache")

	// ErrKeyMismatch indicates the private key does not match the certificate's public key.
	ErrKeyMismatch = errors.New("private key does not match certificate public key")
)

// Certificate represents a managed TLS certificate chain and its private key.
type Certificate struct {
	Hostnames    []string          `json:"hostnames"`
	HostnamesKey string            `json:"hostnames_key"`
	CertPEM      []byte            `json:"cert_pem"`
	KeyPEM       []byte            `json:"-"` // Never serialized in metadata
	Leaf         *x509.Certificate `json:"-"`
	IssuedAt     time.Time         `json:"issued_at"`
	ExpiresAt    time.Time         `json:"expires_at"`
}

// String implements fmt.Stringer, ensuring private keys are never printed in logs or errors.
func (c *Certificate) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Certificate(Hostnames=%v, IssuedAt=%s, ExpiresAt=%s)", c.Hostnames, c.IssuedAt.Format(time.RFC3339), c.ExpiresAt.Format(time.RFC3339))
}

// GoString implements fmt.GoStringer to prevent key exposure via %#v.
func (c *Certificate) GoString() string {
	return c.String()
}

// IsExpired reports whether the certificate is expired at the given time.
func (c *Certificate) IsExpired(now time.Time) bool {
	if c == nil || c.Leaf == nil {
		return true
	}
	return now.After(c.ExpiresAt)
}

// certMetadata is saved to <hash>.meta at mode 0600.
type certMetadata struct {
	Hostnames    []string  `json:"hostnames"`
	HostnamesKey string    `json:"hostnames_key"`
	IssuedAt     time.Time `json:"issued_at"`
	ExpiresAt    time.Time `json:"expires_at"`
}

// DiskCache implements certificate caching on local disk.
type DiskCache struct {
	dir     string
	mu      sync.RWMutex
	entries map[string]*Certificate
}

// NewDiskCache creates a disk-backed certificate cache in the specified directory.
// The directory is ensured to have permissions 0700.
func NewDiskCache(dir string) (*DiskCache, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to set cache directory permissions: %w", err)
	}

	dc := &DiskCache{
		dir:     dir,
		entries: make(map[string]*Certificate),
	}
	return dc, nil
}

// Dir returns the directory path used by the disk cache.
func (dc *DiskCache) Dir() string {
	return dc.dir
}

// hashKey returns the hex-encoded SHA-256 hash of a normalized hostnames key.
func hashKey(hostnamesKey string) string {
	sum := sha256.Sum256([]byte(hostnamesKey))
	return hex.EncodeToString(sum[:])
}

// Put saves the certificate chain and private key to disk at mode 0600, and updates the in-memory cache.
func (dc *DiskCache) Put(cert *Certificate) error {
	if cert == nil || cert.HostnamesKey == "" {
		return errors.New("cannot store nil or empty certificate")
	}

	h := hashKey(cert.HostnamesKey)
	crtPath := filepath.Join(dc.dir, h+".crt")
	keyPath := filepath.Join(dc.dir, h+".key")
	metaPath := filepath.Join(dc.dir, h+".meta")

	// Ensure directory permissions remain 0700
	if err := os.Chmod(dc.dir, 0700); err != nil {
		return fmt.Errorf("failed to ensure cache directory permissions 0700: %w", err)
	}

	// Write files with mode 0600
	if err := os.WriteFile(crtPath, cert.CertPEM, 0600); err != nil {
		return fmt.Errorf("failed to write certificate file: %w", err)
	}
	if err := os.Chmod(crtPath, 0600); err != nil {
		return fmt.Errorf("failed to set certificate file mode: %w", err)
	}

	if err := os.WriteFile(keyPath, cert.KeyPEM, 0600); err != nil {
		if rmErr := os.Remove(crtPath); rmErr != nil && !os.IsNotExist(rmErr) {
			return fmt.Errorf("failed to write key file: %w (cleanup error: %v)", err, rmErr)
		}
		return fmt.Errorf("failed to write key file: %w", err)
	}
	if err := os.Chmod(keyPath, 0600); err != nil {
		return fmt.Errorf("failed to set key file mode: %w", err)
	}

	meta := certMetadata{
		Hostnames:    cert.Hostnames,
		HostnamesKey: cert.HostnamesKey,
		IssuedAt:     cert.IssuedAt,
		ExpiresAt:    cert.ExpiresAt,
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		if rmErr := os.Remove(crtPath); rmErr != nil && !os.IsNotExist(rmErr) {
			return fmt.Errorf("failed to marshal certificate metadata: %w (cleanup crt error: %v)", err, rmErr)
		}
		if rmErr := os.Remove(keyPath); rmErr != nil && !os.IsNotExist(rmErr) {
			return fmt.Errorf("failed to marshal certificate metadata: %w (cleanup key error: %v)", err, rmErr)
		}
		return fmt.Errorf("failed to marshal certificate metadata: %w", err)
	}
	if err := os.WriteFile(metaPath, metaBytes, 0600); err != nil {
		if rmErr := os.Remove(crtPath); rmErr != nil && !os.IsNotExist(rmErr) {
			return fmt.Errorf("failed to write metadata file: %w (cleanup crt error: %v)", err, rmErr)
		}
		if rmErr := os.Remove(keyPath); rmErr != nil && !os.IsNotExist(rmErr) {
			return fmt.Errorf("failed to write metadata file: %w (cleanup key error: %v)", err, rmErr)
		}
		return fmt.Errorf("failed to write metadata file: %w", err)
	}
	if err := os.Chmod(metaPath, 0600); err != nil {
		return fmt.Errorf("failed to set metadata file mode: %w", err)
	}

	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.entries[cert.HostnamesKey] = cert
	return nil
}

// Get returns the cached certificate for a hostnames key.
func (dc *DiskCache) Get(hostnamesKey string) (*Certificate, error) {
	dc.mu.RLock()
	defer dc.mu.RUnlock()
	cert, ok := dc.entries[hostnamesKey]
	if !ok {
		return nil, ErrNotFound
	}
	return cert, nil
}

// Delete removes the certificate from disk and in-memory cache.
func (dc *DiskCache) Delete(hostnamesKey string) error {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	delete(dc.entries, hostnamesKey)

	h := hashKey(hostnamesKey)
	var errs []error
	for _, ext := range []string{".crt", ".key", ".meta"} {
		fPath := filepath.Join(dc.dir, h+ext)
		if err := os.Remove(fPath); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors occurred during certificate deletion: %v", errs)
	}
	return nil
}

// List returns a snapshot of all certificates loaded in memory.
func (dc *DiskCache) List() []*Certificate {
	dc.mu.RLock()
	defer dc.mu.RUnlock()
	list := make([]*Certificate, 0, len(dc.entries))
	for _, c := range dc.entries {
		list = append(list, c)
	}
	return list
}

// Load reads all certificate chains and keys from disk, validates them, and populates the in-memory cache.
func (dc *DiskCache) Load() error {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	// Enforce directory permissions 0700
	if err := os.Chmod(dc.dir, 0700); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to enforce cache directory permissions 0700: %w", err)
	}

	files, err := os.ReadDir(dc.dir)
	if err != nil {
		return fmt.Errorf("failed to read cache directory: %w", err)
	}

	for _, f := range files {
		if f.IsDir() || filepath.Ext(f.Name()) != ".crt" {
			continue
		}
		base := f.Name()[:len(f.Name())-4]
		crtPath := filepath.Join(dc.dir, base+".crt")
		keyPath := filepath.Join(dc.dir, base+".key")
		metaPath := filepath.Join(dc.dir, base+".meta")

		// Enforce mode 0600 on files
		if err := os.Chmod(crtPath, 0600); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to set %s permissions: %w", crtPath, err)
		}
		if err := os.Chmod(keyPath, 0600); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to set %s permissions: %w", keyPath, err)
		}
		if err := os.Chmod(metaPath, 0600); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to set %s permissions: %w", metaPath, err)
		}

		certPEM, err := os.ReadFile(crtPath)
		if err != nil {
			continue
		}
		keyPEM, err := os.ReadFile(keyPath)
		if err != nil {
			continue
		}

		leaf, err := parseLeafCert(certPEM)
		if err != nil {
			continue
		}

		key, err := parseECPrivateKey(keyPEM)
		if err != nil {
			continue
		}

		// Verify key corresponds to leaf certificate public key
		leafPubKey, ok := leaf.PublicKey.(*ecdsa.PublicKey)
		if !ok || !leafPubKey.Equal(key.Public()) {
			continue
		}

		var hostnames []string
		var hostnamesKey string
		metaBytes, err := os.ReadFile(metaPath)
		if err == nil {
			var meta certMetadata
			if err := json.Unmarshal(metaBytes, &meta); err == nil && meta.HostnamesKey != "" {
				hostnames = meta.Hostnames
				hostnamesKey = meta.HostnamesKey
			}
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("failed to read %s: %w", metaPath, err)
		}

		if hostnamesKey == "" {
			hostnames = leaf.DNSNames
			hostnamesKey = leaf.Subject.CommonName
		}

		cert := &Certificate{
			Hostnames:    hostnames,
			HostnamesKey: hostnamesKey,
			CertPEM:      certPEM,
			KeyPEM:       keyPEM,
			Leaf:         leaf,
			IssuedAt:     leaf.NotBefore,
			ExpiresAt:    leaf.NotAfter,
		}

		dc.entries[hostnamesKey] = cert
	}

	return nil
}

// parseLeafCert parses the first certificate PEM block in data.
func parseLeafCert(certPEM []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errors.New("failed to decode certificate PEM")
	}
	return x509.ParseCertificate(block.Bytes)
}

// parseECPrivateKey parses an ECDSA private key from PEM bytes.
func parseECPrivateKey(keyPEM []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, errors.New("failed to decode key PEM")
	}
	if block.Type == "EC PRIVATE KEY" {
		return x509.ParseECPrivateKey(block.Bytes)
	}
	parsedKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	ecKey, ok := parsedKey.(*ecdsa.PrivateKey)
	if !ok {
		return nil, errors.New("key is not an ECDSA private key")
	}
	return ecKey, nil
}
