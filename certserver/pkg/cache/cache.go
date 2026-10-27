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

	if _, err := f.Write(data); err != nil {
		closeErr := f.Close()
		if rmErr := os.Remove(tmpPath); rmErr != nil && !os.IsNotExist(rmErr) {
			log.Printf("certserver: error removing temp file %s: %v", tmpPath, rmErr)
		}
		if closeErr != nil {
			return fmt.Errorf("failed to write temp file (%w) and close failed: %v", err, closeErr)
		}
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	if err := f.Sync(); err != nil {
		closeErr := f.Close()
		if rmErr := os.Remove(tmpPath); rmErr != nil && !os.IsNotExist(rmErr) {
			log.Printf("certserver: error removing temp file %s: %v", tmpPath, rmErr)
		}
		if closeErr != nil {
			return fmt.Errorf("failed to fsync temp file (%w) and close failed: %v", err, closeErr)
		}
		return fmt.Errorf("failed to fsync temp file: %w", err)
	}

	if err := f.Close(); err != nil {
		if rmErr := os.Remove(tmpPath); rmErr != nil && !os.IsNotExist(rmErr) {
			log.Printf("certserver: error removing temp file %s: %v", tmpPath, rmErr)
		}
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	if err := os.Chmod(tmpPath, 0600); err != nil {
		if rmErr := os.Remove(tmpPath); rmErr != nil && !os.IsNotExist(rmErr) {
			log.Printf("certserver: error removing temp file %s: %v", tmpPath, rmErr)
		}
		return fmt.Errorf("failed to set temp file permissions: %w", err)
	}

	if err := os.Rename(tmpPath, destPath); err != nil {
		if rmErr := os.Remove(tmpPath); rmErr != nil && !os.IsNotExist(rmErr) {
			log.Printf("certserver: error removing temp file %s: %v", tmpPath, rmErr)
		}
		return fmt.Errorf("failed to rename %s to %s: %w", tmpPath, destPath, err)
	}

	return dc.syncDir()
}

// syncDir fsyncs the cache directory to commit directory entries to storage.
func (dc *DiskCache) syncDir() error {
	dirFile, err := os.Open(dc.dir)
	if err != nil {
		return fmt.Errorf("failed to open cache dir for sync: %w", err)
	}

	syncErr := dirFile.Sync()
	closeErr := dirFile.Close()
	if syncErr != nil {
		return fmt.Errorf("failed to sync cache dir: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("failed to close cache dir: %w", closeErr)
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

// Put saves the certificate chain and private key to disk at mode 0600 atomically,
// ensuring the certificate and metadata are placed before the key is renamed into place.
func (dc *DiskCache) Put(cert *Certificate) error {
	if cert == nil || cert.HostnamesKey == "" {
		return errors.New("cannot store nil or empty certificate")
	}

	h := hashKey(cert.HostnamesKey)
	crtFile := h + ".crt"
	keyFile := h + ".key"
	metaFile := h + ".meta"

	// 1. Write certificate chain atomically
	if err := dc.writeAtomic(crtFile, cert.CertPEM); err != nil {
		return fmt.Errorf("failed to write certificate file: %w", err)
	}

	// 2. Write metadata atomically
	meta := certMetadata{
		Hostnames:    cert.Hostnames,
		HostnamesKey: cert.HostnamesKey,
		IssuedAt:     cert.IssuedAt,
		ExpiresAt:    cert.ExpiresAt,
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to marshal certificate metadata: %w", err)
	}
	if err := dc.writeAtomic(metaFile, metaBytes); err != nil {
		return fmt.Errorf("failed to write metadata file: %w", err)
	}

	// 3. Write private key into place last
	if err := dc.writeAtomic(keyFile, cert.KeyPEM); err != nil {
		return fmt.Errorf("failed to write key file: %w", err)
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
			log.Printf("certserver: failed to set permissions on %s: %v", crtPath, err)
			continue
		}
		if err := os.Chmod(keyPath, 0600); err != nil && !os.IsNotExist(err) {
			log.Printf("certserver: failed to set permissions on %s: %v", keyPath, err)
			continue
		}
		if err := os.Chmod(metaPath, 0600); err != nil && !os.IsNotExist(err) {
			log.Printf("certserver: failed to set permissions on %s: %v", metaPath, err)
			continue
		}

		certPEM, err := os.ReadFile(crtPath)
		if err != nil {
			log.Printf("certserver: failed to read certificate file %s: %v", crtPath, err)
			continue
		}
		keyPEM, err := os.ReadFile(keyPath)
		if err != nil {
			log.Printf("certserver: failed to read key file %s: %v", keyPath, err)
			continue
		}

		leaf, err := parseLeafCert(certPEM)
		if err != nil {
			log.Printf("certserver: failed to parse leaf certificate from %s: %v", crtPath, err)
			continue
		}

		key, err := parseECPrivateKey(keyPEM)
		if err != nil {
			log.Printf("certserver: failed to parse private key from %s: %v", keyPath, err)
			continue
		}

		// Verify key corresponds to leaf certificate public key
		leafPubKey, ok := leaf.PublicKey.(*ecdsa.PublicKey)
		if !ok || !leafPubKey.Equal(key.Public()) {
			log.Printf("certserver: public key mismatch for certificate %s and key %s", crtPath, keyPath)
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
			} else {
				log.Printf("certserver: failed to unmarshal metadata from %s", metaPath)
			}
		} else if !os.IsNotExist(err) {
			log.Printf("certserver: failed to read metadata file %s: %v", metaPath, err)
			continue
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
