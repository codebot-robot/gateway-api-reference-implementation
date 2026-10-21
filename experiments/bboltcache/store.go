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

package bboltcache

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/bbolt"
)

// StoreOptions contains configuration for creating a BBoltStore.
type StoreOptions struct {
	FilePath   string
	AutoDelete bool
	Cipher     Cipher
	Codec      ObjectCodec
	BoltOpts   *bbolt.Options
}

// StoreOption is a functional option for configuring a BBoltStore.
type StoreOption func(*StoreOptions)

// WithFilePath sets a specific file path for the bbolt database.
func WithFilePath(path string) StoreOption {
	return func(o *StoreOptions) {
		o.FilePath = path
	}
}

// WithAutoDelete specifies whether to delete the database file when closed.
func WithAutoDelete(autoDelete bool) StoreOption {
	return func(o *StoreOptions) {
		o.AutoDelete = autoDelete
	}
}

// WithCipher sets the encryption cipher for the store.
func WithCipher(cipher Cipher) StoreOption {
	return func(o *StoreOptions) {
		o.Cipher = cipher
	}
}

// WithCodec sets the object codec for serialization.
func WithCodec(codec ObjectCodec) StoreOption {
	return func(o *StoreOptions) {
		o.Codec = codec
	}
}

// WithBoltOptions sets custom bbolt Options.
func WithBoltOptions(opts *bbolt.Options) StoreOption {
	return func(o *StoreOptions) {
		o.BoltOpts = opts
	}
}

// Store wraps a bbolt key-value database for caching Kubernetes objects on disk.
type Store struct {
	db         *bbolt.DB
	path       string
	isTemp     bool
	autoDelete bool
	cipher     Cipher
	codec      ObjectCodec
	mu         sync.RWMutex
	closed     atomic.Bool
}

// NewStore initializes a new bbolt-backed Store.
// By default, if no FilePath is provided, a temporary scratch database is created
// with AutoDelete enabled.
func NewStore(opts ...StoreOption) (*Store, error) {
	options := StoreOptions{
		Cipher:     NoopCipher{},
		Codec:      NewJSONCodec(),
		AutoDelete: true,
		BoltOpts: &bbolt.Options{
			Timeout:         1 * time.Second,
			FreelistType:    bbolt.FreelistMapType,
			InitialMmapSize: 64 * 1024 * 1024,
		},
	}
	for _, opt := range opts {
		opt(&options)
	}
	if options.BoltOpts != nil && options.BoltOpts.InitialMmapSize == 0 {
		options.BoltOpts.InitialMmapSize = 64 * 1024 * 1024
	}

	path := options.FilePath
	isTemp := false
	if path == "" {
		tmpFile, err := os.CreateTemp("", "gari-bbolt-*.db")
		if err != nil {
			return nil, fmt.Errorf("failed to create temporary bbolt file: %w", err)
		}
		path = tmpFile.Name()
		if closeErr := tmpFile.Close(); closeErr != nil {
			if rmErr := os.Remove(path); rmErr != nil && !os.IsNotExist(rmErr) {
				return nil, fmt.Errorf("failed to close temporary bbolt file (%w) and failed to cleanup: %w", closeErr, rmErr)
			}
			return nil, fmt.Errorf("failed to close temporary bbolt file: %w", closeErr)
		}
		// Remove empty file so bbolt initializes its header cleanly.
		if rmErr := os.Remove(path); rmErr != nil && !os.IsNotExist(rmErr) {
			return nil, fmt.Errorf("failed to remove temporary file placeholder: %w", rmErr)
		}
		isTemp = true
		options.AutoDelete = true
	}

	db, err := bbolt.Open(path, 0600, options.BoltOpts)
	if err != nil {
		if isTemp {
			if rmErr := os.Remove(path); rmErr != nil && !os.IsNotExist(rmErr) {
				return nil, fmt.Errorf("failed to open bbolt database at %s (%w) and failed to cleanup temp file: %w", path, err, rmErr)
			}
		}
		return nil, fmt.Errorf("failed to open bbolt database at %s: %w", path, err)
	}

	return &Store{
		db:         db,
		path:       path,
		isTemp:     isTemp,
		autoDelete: options.AutoDelete,
		cipher:     options.Cipher,
		codec:      options.Codec,
	}, nil
}

// DB returns the underlying bbolt.DB instance.
func (s *Store) DB() *bbolt.DB {
	return s.db
}

// Path returns the database file path.
func (s *Store) Path() string {
	return s.path
}

// Cipher returns the store's configured cipher.
func (s *Store) Cipher() Cipher {
	return s.cipher
}

// Codec returns the store's configured codec.
func (s *Store) Codec() ObjectCodec {
	return s.codec
}

// Close closes the bbolt database and cleans up temporary files if configured.
func (s *Store) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var err error
	if s.db != nil {
		err = s.db.Close()
	}
	if s.autoDelete && s.path != "" {
		if rmErr := os.Remove(s.path); rmErr != nil && !os.IsNotExist(rmErr) && err == nil {
			err = rmErr
		}
	}
	return err
}

// EnsureBucket creates the given bucket if it does not already exist.
func (s *Store) EnsureBucket(bucketName string) error {
	if s.closed.Load() {
		return errors.New("store is closed")
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
}

// Put writes raw bytes for a key in the specified bucket.
func (s *Store) Put(bucketName string, key string, data []byte) error {
	if s.closed.Load() {
		return errors.New("store is closed")
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		return b.Put([]byte(key), data)
	})
}

// Get retrieves raw bytes for a key in the specified bucket.
// Returns a cloned byte slice safe for use after the read transaction finishes.
func (s *Store) Get(bucketName string, key string) ([]byte, bool, error) {
	if s.closed.Load() {
		return nil, false, errors.New("store is closed")
	}
	var result []byte
	var exists bool
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(key))
		if v != nil {
			result = bytes.Clone(v)
			exists = true
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return result, exists, nil
}

// Delete removes a key from the specified bucket.
func (s *Store) Delete(bucketName string, key string) error {
	if s.closed.Load() {
		return errors.New("store is closed")
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return nil
		}
		return b.Delete([]byte(key))
	})
}

// ListKeys returns all keys currently present in the specified bucket without decoding values.
func (s *Store) ListKeys(bucketName string) ([]string, error) {
	if s.closed.Load() {
		return nil, errors.New("store is closed")
	}
	var keys []string
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, _ []byte) error {
			keys = append(keys, string(k))
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// BeginTx starts a transaction with the underlying bbolt database.
// If writable is false, a consistent point-in-time read transaction is started.
func (s *Store) BeginTx(writable bool) (*bbolt.Tx, error) {
	if s.closed.Load() {
		return nil, errors.New("store is closed")
	}
	return s.db.Begin(writable)
}
