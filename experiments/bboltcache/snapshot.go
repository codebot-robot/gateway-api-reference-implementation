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
	"sync"

	"go.etcd.io/bbolt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// Snapshot represents a consistent point-in-time view of the bbolt store backed by a read transaction.
// A read transaction holds an immutable snapshot without blocking writers.
type Snapshot struct {
	tx     *bbolt.Tx
	store  *Store
	closed bool
	mu     sync.Mutex
}

// NewSnapshot opens a consistent point-in-time read transaction on the store.
func (s *Store) NewSnapshot() (*Snapshot, error) {
	if s.closed.Load() {
		return nil, errors.New("store is closed")
	}
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin read transaction: %w", err)
	}
	return &Snapshot{
		tx:    tx,
		store: s,
	}, nil
}

// Close finishes the read transaction and releases transaction resources.
func (s *Snapshot) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.tx.Rollback()
}

// GetRaw retrieves raw encrypted bytes from a bucket for the given key in this snapshot.
func (s *Snapshot) GetRaw(bucketName, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false, errors.New("snapshot is closed")
	}

	b := s.tx.Bucket([]byte(bucketName))
	if b == nil {
		return nil, false, nil
	}
	v := b.Get([]byte(key))
	if v == nil {
		return nil, false, nil
	}
	return bytes.Clone(v), true, nil
}

// GetObject decodes and decrypts an object from a bucket into the provided prototype.
func (s *Snapshot) GetObject(bucketName, key string, into runtime.Object, cipher Cipher) (bool, error) {
	raw, exists, err := s.GetRaw(bucketName, key)
	if err != nil || !exists {
		return exists, err
	}

	if cipher == nil {
		cipher = s.store.Cipher()
	}
	ad := AdditionalData(bucketName, key)
	decrypted, err := cipher.Decrypt(raw, ad)
	if err != nil {
		return false, fmt.Errorf("failed to decrypt %s: %w", key, err)
	}

	if err := s.store.Codec().Decode(decrypted, into); err != nil {
		return false, fmt.Errorf("failed to decode %s: %w", key, err)
	}
	return true, nil
}

// GetConfigMap retrieves and decodes a ConfigMap from the snapshot.
func (s *Snapshot) GetConfigMap(bucketName, key string, cipher Cipher) (*corev1.ConfigMap, bool, error) {
	cm := &corev1.ConfigMap{}
	exists, err := s.GetObject(bucketName, key, cm, cipher)
	if err != nil || !exists {
		return nil, exists, err
	}
	return cm, true, nil
}

// GetSecret retrieves, decrypts, and decodes a Secret from the snapshot.
func (s *Snapshot) GetSecret(bucketName, key string, cipher Cipher) (*corev1.Secret, bool, error) {
	sec := &corev1.Secret{}
	exists, err := s.GetObject(bucketName, key, sec, cipher)
	if err != nil || !exists {
		return nil, exists, err
	}
	return sec, true, nil
}

// LazyResolver memoizes decoded objects across recomputes by key@resourceVersion.
// This ensures that unreferenced objects are never decoded, and referenced objects
// are only decoded once until their resourceVersion changes.
type LazyResolver struct {
	mu            sync.RWMutex
	memo          map[string]any
	touched       map[string]struct{}
	cmBucketName  string
	secBucketName string
	cipher        Cipher
}

// NewLazyResolver creates a new LazyResolver.
func NewLazyResolver(cmBucketName, secBucketName string, cipher Cipher) *LazyResolver {
	if cmBucketName == "" {
		cmBucketName = "*v1.ConfigMap"
	}
	if secBucketName == "" {
		secBucketName = "*v1.Secret"
	}
	if cipher == nil {
		cipher = NoopCipher{}
	}
	return &LazyResolver{
		memo:          make(map[string]any),
		cmBucketName:  cmBucketName,
		secBucketName: secBucketName,
		cipher:        cipher,
	}
}

func memoKey(kind string, key types.NamespacedName, rv string) string {
	return fmt.Sprintf("%s:%s/%s@%s", kind, key.Namespace, key.Name, rv)
}

// StartRecompute begins a new recompute cycle, tracking touched entries.
func (r *LazyResolver) StartRecompute() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.touched = make(map[string]struct{})
}

// FinishRecompute ends the recompute cycle and evicts all memoized entries that were not
// touched in this cycle. This ensures old versions and unreferenced objects are reclaimed.
func (r *LazyResolver) FinishRecompute() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.touched == nil {
		return
	}
	for k := range r.memo {
		if _, ok := r.touched[k]; !ok {
			delete(r.memo, k)
		}
	}
	r.touched = nil
}

// ResolveConfigMap resolves a ConfigMap from the snapshot, returning the memoized instance
// if the resourceVersion matches.
func (r *LazyResolver) ResolveConfigMap(snap *Snapshot, key types.NamespacedName, rv string) (*corev1.ConfigMap, bool, error) {
	strKey := key.String()
	mKey := ""
	if rv != "" {
		mKey = memoKey("ConfigMap", key, rv)
		r.mu.RLock()
		val, ok := r.memo[mKey]
		r.mu.RUnlock()
		if ok {
			r.mu.Lock()
			if r.touched != nil {
				r.touched[mKey] = struct{}{}
			}
			r.mu.Unlock()
			return val.(*corev1.ConfigMap), true, nil
		}
	}

	cm, exists, err := snap.GetConfigMap(r.cmBucketName, strKey, r.cipher)
	if err != nil || !exists {
		return nil, exists, err
	}

	actualRV := cm.ResourceVersion
	if actualRV != "" {
		newMKey := memoKey("ConfigMap", key, actualRV)
		r.mu.Lock()
		r.memo[newMKey] = cm
		if r.touched != nil {
			r.touched[newMKey] = struct{}{}
		}
		r.mu.Unlock()
	}
	return cm, true, nil
}

// ResolveSecret resolves a Secret from the snapshot, returning the memoized instance
// if the resourceVersion matches.
func (r *LazyResolver) ResolveSecret(snap *Snapshot, key types.NamespacedName, rv string) (*corev1.Secret, bool, error) {
	strKey := key.String()
	mKey := ""
	if rv != "" {
		mKey = memoKey("Secret", key, rv)
		r.mu.RLock()
		val, ok := r.memo[mKey]
		r.mu.RUnlock()
		if ok {
			r.mu.Lock()
			if r.touched != nil {
				r.touched[mKey] = struct{}{}
			}
			r.mu.Unlock()
			return val.(*corev1.Secret), true, nil
		}
	}

	sec, exists, err := snap.GetSecret(r.secBucketName, strKey, r.cipher)
	if err != nil || !exists {
		return nil, exists, err
	}

	actualRV := sec.ResourceVersion
	if actualRV != "" {
		newMKey := memoKey("Secret", key, actualRV)
		r.mu.Lock()
		r.memo[newMKey] = sec
		if r.touched != nil {
			r.touched[newMKey] = struct{}{}
		}
		r.mu.Unlock()
	}
	return sec, true, nil
}

// EvictStale removes memoized entries whose resourceVersion no longer matches the current version.
func (r *LazyResolver) EvictStale(activeKeys map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for k := range r.memo {
		if activeRV, exists := activeKeys[k]; !exists || activeRV == "" {
			delete(r.memo, k)
		}
	}
}

// Clear purges all memoized objects.
func (r *LazyResolver) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.memo = make(map[string]any)
}

// Len returns the number of memoized objects.
func (r *LazyResolver) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.memo)
}
