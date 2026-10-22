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

package diskstore

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// IndexerOptions configures a DiskIndexer.
type IndexerOptions struct {
	BucketName string
	KeyFunc    toolscache.KeyFunc
	Indexers   toolscache.Indexers
	Cipher     Cipher
	Codec      ObjectCodec
}

// IndexerOption is a functional option for configuring a DiskIndexer.
type IndexerOption func(*IndexerOptions)

// WithBucketName sets the bucket name.
func WithBucketName(name string) IndexerOption {
	return func(o *IndexerOptions) {
		o.BucketName = name
	}
}

// WithKeyFunc sets the key function.
func WithKeyFunc(kf toolscache.KeyFunc) IndexerOption {
	return func(o *IndexerOptions) {
		o.KeyFunc = kf
	}
}

// WithIndexers sets secondary indexers.
func WithIndexers(indexers toolscache.Indexers) IndexerOption {
	return func(o *IndexerOptions) {
		o.Indexers = indexers
	}
}

// WithIndexerCipher overrides the cipher.
func WithIndexerCipher(cipher Cipher) IndexerOption {
	return func(o *IndexerOptions) {
		o.Cipher = cipher
	}
}

// WithIndexerCodec overrides the codec.
func WithIndexerCodec(codec ObjectCodec) IndexerOption {
	return func(o *IndexerOptions) {
		o.Codec = codec
	}
}

func callerInfo() string {
	for skip := 2; skip <= 6; skip++ {
		pc, file, line, ok := runtime.Caller(skip)
		if !ok {
			break
		}
		fn := runtime.FuncForPC(pc)
		if fn == nil {
			continue
		}
		fnName := fn.Name()
		if strings.Contains(fnName, "diskstore") {
			continue
		}
		return fmt.Sprintf("%s (%s:%d)", fnName, filepath.Base(file), line)
	}
	return "unknown caller"
}

// DiskIndexer implements k8s.io/client-go/tools/cache.Indexer backed by a DiskStore.
type DiskIndexer struct {
	store      *DiskStore
	bucketName string
	example    k8sruntime.Object
	keyFunc    toolscache.KeyFunc
	cipher     Cipher
	codec      ObjectCodec

	mu         sync.RWMutex
	indexers   toolscache.Indexers
	indices    map[string]map[string]map[string]struct{} // indexName -> indexVal -> set of keys
	revIndices map[string]map[string][]string            // key -> indexName -> []indexVal
	lastSyncRV string
}

var _ toolscache.Indexer = (*DiskIndexer)(nil)

// NewDiskIndexer creates a new Indexer for the given example object type backed by store.
func NewDiskIndexer(store *DiskStore, example k8sruntime.Object, opts ...IndexerOption) (*DiskIndexer, error) {
	if store == nil {
		return nil, errors.New("store cannot be nil")
	}
	if example == nil {
		return nil, errors.New("example object cannot be nil")
	}

	options := IndexerOptions{
		BucketName: fmt.Sprintf("%T", example),
		KeyFunc:    toolscache.DeletionHandlingMetaNamespaceKeyFunc,
		Indexers:   toolscache.Indexers{},
		Cipher:     store.Cipher(),
		Codec:      store.Codec(),
	}
	for _, opt := range opts {
		opt(&options)
	}

	indexer := &DiskIndexer{
		store:      store,
		bucketName: options.BucketName,
		example:    example,
		keyFunc:    options.KeyFunc,
		cipher:     options.Cipher,
		codec:      options.Codec,
		indexers:   make(toolscache.Indexers),
		indices:    make(map[string]map[string]map[string]struct{}),
		revIndices: make(map[string]map[string][]string),
	}

	for k, v := range options.Indexers {
		indexer.indexers[k] = v
		indexer.indices[k] = make(map[string]map[string]struct{})
	}

	return indexer, nil
}

// Add inserts or updates an object.
func (d *DiskIndexer) Add(obj any) error {
	return d.Update(obj)
}

// Update updates an existing object or adds it if not present.
func (d *DiskIndexer) Update(obj any) error {
	key, err := d.keyFunc(obj)
	if err != nil {
		return toolscache.KeyError{Obj: obj, Err: err}
	}

	runtimeObj, ok := obj.(k8sruntime.Object)
	if !ok {
		return fmt.Errorf("object %T does not implement runtime.Object", obj)
	}

	encoded, err := d.codec.Encode(runtimeObj)
	if err != nil {
		return fmt.Errorf("failed to encode object: %w", err)
	}

	ad := AdditionalData(d.bucketName, key)
	encrypted, err := d.cipher.Encrypt(encoded, ad)
	if err != nil {
		return fmt.Errorf("failed to encrypt object: %w", err)
	}

	// Update secondary indices
	d.mu.Lock()
	for indexName, indexFunc := range d.indexers {
		newVals, err := indexFunc(obj)
		if err != nil {
			d.mu.Unlock()
			return fmt.Errorf("indexing error for %s: %w", indexName, err)
		}

		// Remove old entries
		if oldMap, ok := d.revIndices[key]; ok {
			if oldVals, ok := oldMap[indexName]; ok {
				for _, oldVal := range oldVals {
					if d.indices[indexName] != nil && d.indices[indexName][oldVal] != nil {
						delete(d.indices[indexName][oldVal], key)
					}
				}
			}
		}

		// Add new entries
		if d.indices[indexName] == nil {
			d.indices[indexName] = make(map[string]map[string]struct{})
		}
		for _, newVal := range newVals {
			if d.indices[indexName][newVal] == nil {
				d.indices[indexName][newVal] = make(map[string]struct{})
			}
			d.indices[indexName][newVal][key] = struct{}{}
		}

		if d.revIndices[key] == nil {
			d.revIndices[key] = make(map[string][]string)
		}
		d.revIndices[key][indexName] = newVals
	}
	d.mu.Unlock()

	return d.store.Put(d.bucketName, key, encrypted, d.lastSyncRV)
}

// Delete removes an object from the store and secondary indices.
func (d *DiskIndexer) Delete(obj any) error {
	key, err := d.keyFunc(obj)
	if err != nil {
		return toolscache.KeyError{Obj: obj, Err: err}
	}

	d.mu.Lock()
	if oldMap, ok := d.revIndices[key]; ok {
		for indexName, oldVals := range oldMap {
			for _, oldVal := range oldVals {
				if d.indices[indexName] != nil && d.indices[indexName][oldVal] != nil {
					delete(d.indices[indexName][oldVal], key)
				}
			}
		}
		delete(d.revIndices, key)
	}
	d.mu.Unlock()

	return d.store.Delete(d.bucketName, key)
}

// Get returns the stored object matching the key of obj.
func (d *DiskIndexer) Get(obj any) (item any, exists bool, err error) {
	key, err := d.keyFunc(obj)
	if err != nil {
		return nil, false, toolscache.KeyError{Obj: obj, Err: err}
	}
	return d.GetByKey(key)
}

// GetByKey retrieves, decrypts, and decodes an object by key.
func (d *DiskIndexer) GetByKey(key string) (item any, exists bool, err error) {
	encrypted, exists, err := d.store.Get(d.bucketName, key)
	if err != nil || !exists {
		return nil, exists, err
	}

	ad := AdditionalData(d.bucketName, key)
	decrypted, err := d.cipher.Decrypt(encrypted, ad)
	if err != nil {
		return nil, false, fmt.Errorf("failed to decrypt %s: %w", key, err)
	}

	target := d.example.DeepCopyObject()
	if err := d.codec.Decode(decrypted, target); err != nil {
		return nil, false, fmt.Errorf("failed to decode %s: %w", key, err)
	}

	return target, true, nil
}

// List returns a point-in-time consistent view of all objects in the bucket.
func (d *DiskIndexer) List() []any {
	rawItems, err := d.store.List(d.bucketName)
	if err != nil {
		return nil
	}

	var items []any
	for _, raw := range rawItems {
		ad := AdditionalData(d.bucketName, raw.Key)
		decrypted, err := d.cipher.Decrypt(raw.Payload, ad)
		if err != nil {
			continue
		}
		target := d.example.DeepCopyObject()
		if err := d.codec.Decode(decrypted, target); err != nil {
			continue
		}
		items = append(items, target)
	}

	klog.Infof("diskstore: broad read invoked on bucket %q: List() decoded %d objects (caller: %s)",
		d.bucketName, len(items), callerInfo())
	return items
}

// ListKeys returns all keys currently stored without decoding payloads.
func (d *DiskIndexer) ListKeys() []string {
	return d.store.ListKeys()
}

// Replace replaces all store contents with list.
func (d *DiskIndexer) Replace(list []any, rv string) error {
	d.mu.Lock()
	d.lastSyncRV = rv
	// Reset indices
	d.indices = make(map[string]map[string]map[string]struct{})
	for k := range d.indexers {
		d.indices[k] = make(map[string]map[string]struct{})
	}
	d.revIndices = make(map[string]map[string][]string)
	d.mu.Unlock()

	// Clear existing items in store
	for _, k := range d.store.ListKeys() {
		_ = d.store.Delete(d.bucketName, k)
	}

	for _, item := range list {
		if err := d.Update(item); err != nil {
			return err
		}
	}
	return nil
}

// Resync is a no-op matching convention.
func (d *DiskIndexer) Resync() error {
	return nil
}

// LastStoreSyncResourceVersion returns the last recorded sync resourceVersion.
func (d *DiskIndexer) LastStoreSyncResourceVersion() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.lastSyncRV != "" {
		return d.lastSyncRV
	}
	return d.store.LastRecoveredResourceVersion()
}

// Bookmark records the resourceVersion from a watch bookmark.
func (d *DiskIndexer) Bookmark(rv string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastSyncRV = rv
}

// AddIndexers registers new secondary indexers.
func (d *DiskIndexer) AddIndexers(newIndexers toolscache.Indexers) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for k, v := range newIndexers {
		if _, exists := d.indexers[k]; exists {
			return fmt.Errorf("indexer %q already exists", k)
		}
		d.indexers[k] = v
		d.indices[k] = make(map[string]map[string]struct{})
	}
	return nil
}

// GetIndexers returns the registered secondary indexers.
func (d *DiskIndexer) GetIndexers() toolscache.Indexers {
	d.mu.RLock()
	defer d.mu.RUnlock()

	res := make(toolscache.Indexers, len(d.indexers))
	for k, v := range d.indexers {
		res[k] = v
	}
	return res
}

// Index returns objects matching the index values of obj for indexName.
func (d *DiskIndexer) Index(indexName string, obj any) ([]any, error) {
	d.mu.RLock()
	indexFunc, exists := d.indexers[indexName]
	d.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("index with name %s does not exist", indexName)
	}

	indexedValues, err := indexFunc(obj)
	if err != nil {
		return nil, err
	}

	keySet := make(map[string]struct{})
	for _, val := range indexedValues {
		keys, err := d.IndexKeys(indexName, val)
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			keySet[k] = struct{}{}
		}
	}

	var items []any
	for k := range keySet {
		item, exists, err := d.GetByKey(k)
		if err != nil {
			return nil, err
		}
		if exists {
			items = append(items, item)
		}
	}
	return items, nil
}

// IndexKeys returns storage keys matching indexedValue for indexName.
func (d *DiskIndexer) IndexKeys(indexName, indexedValue string) ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if _, exists := d.indexers[indexName]; !exists {
		return nil, fmt.Errorf("index with name %s does not exist", indexName)
	}

	valMap, ok := d.indices[indexName]
	if !ok {
		return nil, nil
	}
	keySet, ok := valMap[indexedValue]
	if !ok {
		return nil, nil
	}

	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, nil
}

// ByIndex returns objects matching indexedValue for indexName.
func (d *DiskIndexer) ByIndex(indexName, indexedValue string) ([]any, error) {
	keys, err := d.IndexKeys(indexName, indexedValue)
	if err != nil {
		return nil, err
	}

	var items []any
	for _, key := range keys {
		item, exists, err := d.GetByKey(key)
		if err != nil {
			return nil, err
		}
		if exists {
			items = append(items, item)
		}
	}
	return items, nil
}

// ListIndexFuncValues returns all distinct indexed values for indexName.
func (d *DiskIndexer) ListIndexFuncValues(indexName string) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	valMap, ok := d.indices[indexName]
	if !ok {
		return nil
	}

	values := make([]string, 0, len(valMap))
	for v := range valMap {
		values = append(values, v)
	}
	sort.Strings(values)
	return values
}
