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
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	accountKeyFileName = "account.key"
)

var (
	// ErrNotFound indicates that the requested certificate was not found in cache.
	ErrNotFound = errors.New("certificate not found in cache")

	// ErrKeyMismatch indicates the private key does not match the certificate's public key.
	ErrKeyMismatch = errors.New("private key does not match certificate public key")
)

// Certificate represents a managed TLS certificate chain and its private key.
type Certificate struct {
	Hostnames               []string  `json:"hostnames"`
	HostnamesKey            string    `json:"hostnames_key"`
	CertPEM                 []byte    `json:"cert_pem"`
	KeyPEM                  []byte    `json:"key_pem"`
	IssuedAt                time.Time `json:"issued_at"`
	ExpiresAt               time.Time `json:"expires_at"`
	ClusterID               string    `json:"cluster_id,omitempty"`
	Gateway                 string    `json:"gateway,omitempty"`
	DataPlaneServiceAccount string    `json:"data_plane_service_account,omitempty"`

	leaf *x509.Certificate
}

// NewCertificate constructs a Certificate with a parsed leaf and metadata.
func NewCertificate(hostnames []string, hostnamesKey string, certPEM, keyPEM []byte, leaf *x509.Certificate, issuedAt, expiresAt time.Time) *Certificate {
	return &Certificate{
		Hostnames:    hostnames,
		HostnamesKey: hostnamesKey,
		CertPEM:      certPEM,
		KeyPEM:       keyPEM,
		leaf:         leaf,
		IssuedAt:     issuedAt,
		ExpiresAt:    expiresAt,
	}
}

// Leaf returns the parsed x509 leaf certificate.
func (c *Certificate) Leaf() *x509.Certificate {
	if c == nil {
		return nil
	}
	return c.leaf
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
	if c == nil || c.leaf == nil {
		return true
	}
	return now.After(c.ExpiresAt)
}

// certBundle stores a complete certificate chain and key atomically in a single file per set.
type certBundle struct {
	Hostnames               []string  `json:"hostnames"`
	HostnamesKey            string    `json:"hostnames_key"`
	CertPEM                 []byte    `json:"cert_pem"`
	KeyPEM                  []byte    `json:"key_pem"`
	IssuedAt                time.Time `json:"issued_at"`
	ExpiresAt               time.Time `json:"expires_at"`
	ClusterID               string    `json:"cluster_id,omitempty"`
	Gateway                 string    `json:"gateway,omitempty"`
	DataPlaneServiceAccount string    `json:"data_plane_service_account,omitempty"`
}

// DiskCache implements certificate caching on local disk.
type DiskCache struct {
	dir string

	// mu guards in-memory cache entries.
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
// Storing each certificate bundle under a deterministic hash gives a stable,
// collision-resistant 1:1 filename per hostname set on disk.
func hashKey(hostnamesKey string) string {
	sum := sha256.Sum256([]byte(hostnamesKey))
	return hex.EncodeToString(sum[:])
}

// writeAtomic writes data to a temporary file in the cache directory, fsyncs it,
// sets mode 0600, renames it into place, and fsyncs the directory.
func (dc *DiskCache) writeAtomic(filename string, data []byte) error {
	destPath := filepath.Join(dc.dir, filename)
	tmpPath := filepath.Join(dc.dir, fmt.Sprintf(".%s.tmp.%d", filename, time.Now().UnixNano()))

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	cleanup := true
	defer func() {
		if cleanup {
			if closeErr := f.Close(); closeErr != nil && !errors.Is(closeErr, os.ErrClosed) {
				log.Printf("certserver: error closing temp file %s: %v", tmpPath, closeErr)
			}
			if rmErr := os.Remove(tmpPath); rmErr != nil && !os.IsNotExist(rmErr) {
				log.Printf("certserver: error removing temp file %s: %v", tmpPath, rmErr)
			}
		}
	}()

	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to fsync temp file: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	if err := os.Chmod(tmpPath, 0600); err != nil {
		return fmt.Errorf("failed to set temp file permissions: %w", err)
	}

	if err := os.Rename(tmpPath, destPath); err != nil {
		return fmt.Errorf("failed to rename %s to %s: %w", tmpPath, destPath, err)
	}

	cleanup = false
	return dc.syncDir()
}

// syncDir fsyncs the cache directory to commit directory entries to storage.
func (dc *DiskCache) syncDir() error {
	dirFile, err := os.Open(dc.dir)
	if err != nil {
		return fmt.Errorf("failed to open cache dir for sync: %w", err)
	}
	defer func() {
		if closeErr := dirFile.Close(); closeErr != nil {
			log.Printf("certserver: error closing cache dir %s: %v", dc.dir, closeErr)
		}
	}()

	if err := dirFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync cache dir: %w", err)
	}
	return nil
}

// LoadOrGenerateAccountKey loads the ECDSA P-256 account key from account.key in the cache directory,
// or generates, fsyncs, and stores a new one at mode 0600 if none exists.
func (dc *DiskCache) LoadOrGenerateAccountKey() (*ecdsa.PrivateKey, error) {
	keyPath := filepath.Join(dc.dir, accountKeyFileName)

	keyPEM, err := os.ReadFile(keyPath)
	if err == nil {
		key, parseErr := parseECPrivateKey(keyPEM)
		if parseErr != nil {
			return nil, fmt.Errorf("failed to parse existing account key from %s: %w", keyPath, parseErr)
		}
		return key, nil
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to read account key file %s: %w", keyPath, err)
	}

	// Generate new ECDSA P-256 account key
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate account key: %w", err)
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal generated account key: %w", err)
	}

	pemBytes := pem.EncodeToMemory(&pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: keyDER,
	})

	if err := dc.writeAtomic(accountKeyFileName, pemBytes); err != nil {
		return nil, fmt.Errorf("failed to persist account key to %s: %w", keyPath, err)
	}

	return key, nil
}

// Put saves the certificate chain and private key to disk as a single atomic bundle file at mode 0600.
func (dc *DiskCache) Put(cert *Certificate) error {
	if cert == nil || cert.HostnamesKey == "" {
		return errors.New("cannot store nil or empty certificate")
	}

	bundle := certBundle{
		Hostnames:               cert.Hostnames,
		HostnamesKey:            cert.HostnamesKey,
		CertPEM:                 cert.CertPEM,
		KeyPEM:                  cert.KeyPEM,
		IssuedAt:                cert.IssuedAt,
		ExpiresAt:               cert.ExpiresAt,
		ClusterID:               cert.ClusterID,
		Gateway:                 cert.Gateway,
		DataPlaneServiceAccount: cert.DataPlaneServiceAccount,
	}

	bundleBytes, err := json.Marshal(bundle)
	if err != nil {
		return fmt.Errorf("failed to marshal certificate bundle: %w", err)
	}

	h := hashKey(cert.HostnamesKey)
	bundleFileName := h + ".bundle"

	if err := dc.writeAtomic(bundleFileName, bundleBytes); err != nil {
		return fmt.Errorf("failed to write certificate bundle: %w", err)
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

// Delete removes the certificate bundle from disk and in-memory cache.
func (dc *DiskCache) Delete(hostnamesKey string) error {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	delete(dc.entries, hostnamesKey)

	h := hashKey(hostnamesKey)
	fPath := filepath.Join(dc.dir, h+".bundle")
	if err := os.Remove(fPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove certificate bundle %s: %w", fPath, err)
	}
	return dc.syncDir()
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

// Load reads all certificate bundles from disk, validates them, and populates the in-memory cache.
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
		if f.IsDir() || filepath.Ext(f.Name()) != ".bundle" {
			continue
		}
		bundlePath := filepath.Join(dc.dir, f.Name())

		// Enforce mode 0600 on bundle file
		if err := os.Chmod(bundlePath, 0600); err != nil && !os.IsNotExist(err) {
			log.Printf("certserver: failed to set permissions on %s: %v", bundlePath, err)
			continue
		}

		bundleBytes, err := os.ReadFile(bundlePath)
		if err != nil {
			log.Printf("certserver: failed to read certificate bundle %s: %v", bundlePath, err)
			continue
		}

		var bundle certBundle
		if err := json.Unmarshal(bundleBytes, &bundle); err != nil {
			log.Printf("certserver: failed to unmarshal certificate bundle from %s: %v", bundlePath, err)
			continue
		}

		leaf, err := parseLeafCert(bundle.CertPEM)
		if err != nil {
			log.Printf("certserver: failed to parse leaf certificate from %s: %v", bundlePath, err)
			continue
		}

		key, err := parseECPrivateKey(bundle.KeyPEM)
		if err != nil {
			log.Printf("certserver: failed to parse private key from %s: %v", bundlePath, err)
			continue
		}

		// Verify key corresponds to leaf certificate public key
		leafPubKey, ok := leaf.PublicKey.(*ecdsa.PublicKey)
		if !ok || !leafPubKey.Equal(key.Public()) {
			log.Printf("certserver: public key mismatch for certificate bundle %s", bundlePath)
			continue
		}

		cert := NewCertificate(
			bundle.Hostnames,
			bundle.HostnamesKey,
			bundle.CertPEM,
			bundle.KeyPEM,
			leaf,
			bundle.IssuedAt,
			bundle.ExpiresAt,
		)
		cert.ClusterID = bundle.ClusterID
		cert.Gateway = bundle.Gateway
		cert.DataPlaneServiceAccount = bundle.DataPlaneServiceAccount

		dc.entries[bundle.HostnamesKey] = cert
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
