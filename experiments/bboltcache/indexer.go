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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"go.etcd.io/bbolt"
	"k8s.io/apimachinery/pkg/runtime"
	toolscache "k8s.io/client-go/tools/cache"
)

// IndexerOptions configures a BBoltIndexer.
type IndexerOptions struct {
	BucketName string
	KeyFunc    toolscache.KeyFunc
	Indexers   toolscache.Indexers
	Cipher     Cipher
	Codec      ObjectCodec
}

// IndexerOption is a functional option for configuring a BBoltIndexer.
type IndexerOption func(*IndexerOptions)

// WithBucketName overrides the default bucket name.
func WithBucketName(name string) IndexerOption {
	return func(o *IndexerOptions) {
		o.BucketName = name
	}
}

// WithKeyFunc overrides the key calculation function.
func WithKeyFunc(kf toolscache.KeyFunc) IndexerOption {
	return func(o *IndexerOptions) {
		o.KeyFunc = kf
	}
}

// WithIndexers provides initial indexers.
func WithIndexers(indexers toolscache.Indexers) IndexerOption {
	return func(o *IndexerOptions) {
		o.Indexers = indexers
	}
}

// WithIndexerCipher overrides the store cipher for this indexer.
func WithIndexerCipher(cipher Cipher) IndexerOption {
	return func(o *IndexerOptions) {
		o.Cipher = cipher
	}
}

// WithIndexerCodec overrides the store codec for this indexer.
func WithIndexerCodec(codec ObjectCodec) IndexerOption {
	return func(o *IndexerOptions) {
		o.Codec = codec
	}
}

// BBoltIndexer implements k8s.io/client-go/tools/cache.Indexer backed by a bbolt database.
// Stored objects are serialized and encrypted on disk, living in the page cache rather than Go heap.
type BBoltIndexer struct {
	store      *Store
	bucketName string
	example    runtime.Object
	keyFunc    toolscache.KeyFunc
	cipher     Cipher
	codec      ObjectCodec
	mu         sync.RWMutex
	indexers   toolscache.Indexers
	lastSyncRV string
}

var _ toolscache.Indexer = (*BBoltIndexer)(nil)

// NewBBoltIndexer creates a new Indexer for the given example object type.
func NewBBoltIndexer(store *Store, example runtime.Object, opts ...IndexerOption) (*BBoltIndexer, error) {
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

	indexer := &BBoltIndexer{
		store:      store,
		bucketName: options.BucketName,
		example:    example,
		keyFunc:    options.KeyFunc,
		cipher:     options.Cipher,
		codec:      options.Codec,
		indexers:   make(toolscache.Indexers),
	}
	for k, v := range options.Indexers {
		indexer.indexers[k] = v
	}

	if err := store.EnsureBucket(indexer.bucketName); err != nil {
		return nil, fmt.Errorf("failed to create bucket %s: %w", indexer.bucketName, err)
	}

	return indexer, nil
}

func (b *BBoltIndexer) indexBucketName(indexName string) string {
	return fmt.Sprintf("_idx_%s_%s", b.bucketName, indexName)
}

func (b *BBoltIndexer) revIndexBucketName(indexName string) string {
	return fmt.Sprintf("_rev_%s_%s", b.bucketName, indexName)
}

// Add inserts an object into the bbolt store and updates secondary indices.
func (b *BBoltIndexer) Add(obj any) error {
	return b.Update(obj)
}

// Update updates an existing object or adds it if not present.
func (b *BBoltIndexer) Update(obj any) error {
	key, err := b.keyFunc(obj)
	if err != nil {
		return toolscache.KeyError{Obj: obj, Err: err}
	}

	runtimeObj, ok := obj.(runtime.Object)
	if !ok {
		return fmt.Errorf("object %T does not implement runtime.Object", obj)
	}

	encoded, err := b.codec.Encode(runtimeObj)
	if err != nil {
		return fmt.Errorf("failed to encode object: %w", err)
	}

	encrypted, err := b.cipher.Encrypt(encoded)
	if err != nil {
		return fmt.Errorf("failed to encrypt object: %w", err)
	}

	b.mu.RLock()
	indexers := make(toolscache.Indexers, len(b.indexers))
	for k, v := range b.indexers {
		indexers[k] = v
	}
	b.mu.RUnlock()

	return b.store.DB().Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(b.bucketName))
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte(key), encrypted); err != nil {
			return err
		}

		// Update secondary indices
		for indexName, indexFunc := range indexers {
			newVals, err := indexFunc(obj)
			if err != nil {
				return fmt.Errorf("indexing error for %s: %w", indexName, err)
			}

			idxBucket, err := tx.CreateBucketIfNotExists([]byte(b.indexBucketName(indexName)))
			if err != nil {
				return err
			}
			revBucket, err := tx.CreateBucketIfNotExists([]byte(b.revIndexBucketName(indexName)))
			if err != nil {
				return err
			}

			// Clean up previous indexed entries for this object
			if oldValsBytes := revBucket.Get([]byte(key)); oldValsBytes != nil {
				var oldVals []string
				if err := json.Unmarshal(oldValsBytes, &oldVals); err != nil {
					return err
				}
				for _, oldVal := range oldVals {
					idxKey := fmt.Sprintf("%s/%s", oldVal, key)
					if err := idxBucket.Delete([]byte(idxKey)); err != nil {
						return err
					}
				}
			}

			// Insert new indexed entries
			for _, newVal := range newVals {
				idxKey := fmt.Sprintf("%s/%s", newVal, key)
				if err := idxBucket.Put([]byte(idxKey), []byte{1}); err != nil {
					return err
				}
			}

			newValsBytes, err := json.Marshal(newVals)
			if err != nil {
				return err
			}
			if err := revBucket.Put([]byte(key), newValsBytes); err != nil {
				return err
			}
		}

		return nil
	})
}

// Delete removes an object from the bbolt store and secondary indices.
func (b *BBoltIndexer) Delete(obj any) error {
	key, err := b.keyFunc(obj)
	if err != nil {
		return toolscache.KeyError{Obj: obj, Err: err}
	}

	b.mu.RLock()
	indexers := make(toolscache.Indexers, len(b.indexers))
	for k, v := range b.indexers {
		indexers[k] = v
	}
	b.mu.RUnlock()

	return b.store.DB().Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(b.bucketName))
		if bucket != nil {
			if err := bucket.Delete([]byte(key)); err != nil {
				return err
			}
		}

		// Clean up secondary indices
		for indexName := range indexers {
			idxBucket := tx.Bucket([]byte(b.indexBucketName(indexName)))
			revBucket := tx.Bucket([]byte(b.revIndexBucketName(indexName)))
			if revBucket != nil {
				if oldValsBytes := revBucket.Get([]byte(key)); oldValsBytes != nil {
					var oldVals []string
					if err := json.Unmarshal(oldValsBytes, &oldVals); err != nil {
						return err
					}
					if idxBucket != nil {
						for _, oldVal := range oldVals {
							idxKey := fmt.Sprintf("%s/%s", oldVal, key)
							if err := idxBucket.Delete([]byte(idxKey)); err != nil {
								return err
							}
						}
					}
					if err := revBucket.Delete([]byte(key)); err != nil {
						return err
					}
				}
			}
		}

		return nil
	})
}

// Get returns the stored object matching the key of the given object.
func (b *BBoltIndexer) Get(obj any) (item any, exists bool, err error) {
	key, err := b.keyFunc(obj)
	if err != nil {
		return nil, false, toolscache.KeyError{Obj: obj, Err: err}
	}
	return b.GetByKey(key)
}

// GetByKey retrieves an object from disk, decrypts and deserializes it.
func (b *BBoltIndexer) GetByKey(key string) (item any, exists bool, err error) {
	var encrypted []byte
	viewErr := b.store.DB().View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(b.bucketName))
		if bucket == nil {
			return nil
		}
		val := bucket.Get([]byte(key))
		if val != nil {
			encrypted = bytes.Clone(val)
			exists = true
		}
		return nil
	})
	if viewErr != nil {
		return nil, false, viewErr
	}
	if !exists {
		return nil, false, nil
	}

	decrypted, err := b.cipher.Decrypt(encrypted)
	if err != nil {
		return nil, false, fmt.Errorf("failed to decrypt %s: %w", key, err)
	}

	target := b.example.DeepCopyObject()
	if err := b.codec.Decode(decrypted, target); err != nil {
		return nil, false, fmt.Errorf("failed to decode %s: %w", key, err)
	}

	return target, true, nil
}

// List returns all objects stored in the bucket.
func (b *BBoltIndexer) List() []any {
	var items []any
	err := b.store.DB().View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(b.bucketName))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(_, val []byte) error {
			decrypted, err := b.cipher.Decrypt(val)
			if err != nil {
				return nil
			}
			target := b.example.DeepCopyObject()
			if err := b.codec.Decode(decrypted, target); err != nil {
				return nil
			}
			items = append(items, target)
			return nil
		})
	})
	if err != nil {
		return nil
	}
	return items
}

// ListKeys returns all keys in the store without decoding payloads.
func (b *BBoltIndexer) ListKeys() []string {
	keys, err := b.store.ListKeys(b.bucketName)
	if err != nil {
		return nil
	}
	return keys
}

// Replace atomically replaces the entire store contents with the provided list.
func (b *BBoltIndexer) Replace(list []any, rv string) error {
	b.mu.Lock()
	b.lastSyncRV = rv
	indexers := make(toolscache.Indexers, len(b.indexers))
	for k, v := range b.indexers {
		indexers[k] = v
	}
	b.mu.Unlock()

	return b.store.DB().Update(func(tx *bbolt.Tx) error {
		// Recreate primary bucket
		if err := tx.DeleteBucket([]byte(b.bucketName)); err != nil && !errors.Is(err, bbolt.ErrBucketNotFound) {
			return err
		}
		bucket, err := tx.CreateBucketIfNotExists([]byte(b.bucketName))
		if err != nil {
			return err
		}

		// Recreate index buckets
		for indexName := range indexers {
			if err := tx.DeleteBucket([]byte(b.indexBucketName(indexName))); err != nil && !errors.Is(err, bbolt.ErrBucketNotFound) {
				return err
			}
			if err := tx.DeleteBucket([]byte(b.revIndexBucketName(indexName))); err != nil && !errors.Is(err, bbolt.ErrBucketNotFound) {
				return err
			}
			if _, err := tx.CreateBucketIfNotExists([]byte(b.indexBucketName(indexName))); err != nil {
				return err
			}
			if _, err := tx.CreateBucketIfNotExists([]byte(b.revIndexBucketName(indexName))); err != nil {
				return err
			}
		}

		for _, item := range list {
			key, err := b.keyFunc(item)
			if err != nil {
				return toolscache.KeyError{Obj: item, Err: err}
			}

			runtimeObj, ok := item.(runtime.Object)
			if !ok {
				return fmt.Errorf("object %T does not implement runtime.Object", item)
			}

			encoded, err := b.codec.Encode(runtimeObj)
			if err != nil {
				return err
			}

			encrypted, err := b.cipher.Encrypt(encoded)
			if err != nil {
				return err
			}

			if err := bucket.Put([]byte(key), encrypted); err != nil {
				return err
			}

			for indexName, indexFunc := range indexers {
				newVals, err := indexFunc(item)
				if err != nil {
					return err
				}

				idxBucket := tx.Bucket([]byte(b.indexBucketName(indexName)))
				revBucket := tx.Bucket([]byte(b.revIndexBucketName(indexName)))

				for _, newVal := range newVals {
					idxKey := fmt.Sprintf("%s/%s", newVal, key)
					if err := idxBucket.Put([]byte(idxKey), []byte{1}); err != nil {
						return err
					}
				}

				newValsBytes, err := json.Marshal(newVals)
				if err != nil {
					return err
				}
				if err := revBucket.Put([]byte(key), newValsBytes); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// Resync is a no-op matching client-go Store interface convention.
func (b *BBoltIndexer) Resync() error {
	return nil
}

// LastStoreSyncResourceVersion returns the last resource version observed by Replace or Bookmark.
func (b *BBoltIndexer) LastStoreSyncResourceVersion() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.lastSyncRV
}

// Bookmark records the resource version from a watch bookmark.
func (b *BBoltIndexer) Bookmark(rv string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastSyncRV = rv
}

// AddIndexers adds new secondary indexers to the store.
func (b *BBoltIndexer) AddIndexers(newIndexers toolscache.Indexers) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for k, v := range newIndexers {
		if _, exists := b.indexers[k]; exists {
			return fmt.Errorf("indexer %q already exists", k)
		}
		b.indexers[k] = v
	}
	return nil
}

// GetIndexers returns the registered indexers.
func (b *BBoltIndexer) GetIndexers() toolscache.Indexers {
	b.mu.RLock()
	defer b.mu.RUnlock()

	res := make(toolscache.Indexers, len(b.indexers))
	for k, v := range b.indexers {
		res[k] = v
	}
	return res
}

// Index returns stored objects matching the index values of obj for indexName.
func (b *BBoltIndexer) Index(indexName string, obj any) ([]any, error) {
	b.mu.RLock()
	indexFunc, exists := b.indexers[indexName]
	b.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("index with name %s does not exist", indexName)
	}

	indexedValues, err := indexFunc(obj)
	if err != nil {
		return nil, err
	}

	keySet := make(map[string]struct{})
	for _, val := range indexedValues {
		keys, err := b.IndexKeys(indexName, val)
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			keySet[k] = struct{}{}
		}
	}

	var items []any
	for k := range keySet {
		item, exists, err := b.GetByKey(k)
		if err != nil {
			return nil, err
		}
		if exists {
			items = append(items, item)
		}
	}
	return items, nil
}

// IndexKeys returns the storage keys of objects with indexedValue for indexName.
func (b *BBoltIndexer) IndexKeys(indexName, indexedValue string) ([]string, error) {
	b.mu.RLock()
	_, exists := b.indexers[indexName]
	b.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("index with name %s does not exist", indexName)
	}

	var keys []string
	prefix := []byte(indexedValue + "/")
	err := b.store.DB().View(func(tx *bbolt.Tx) error {
		idxBucket := tx.Bucket([]byte(b.indexBucketName(indexName)))
		if idxBucket == nil {
			return nil
		}
		c := idxBucket.Cursor()
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			storageKey := string(k[len(prefix):])
			keys = append(keys, storageKey)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// ByIndex returns stored objects that match indexedValue for indexName.
func (b *BBoltIndexer) ByIndex(indexName, indexedValue string) ([]any, error) {
	keys, err := b.IndexKeys(indexName, indexedValue)
	if err != nil {
		return nil, err
	}
	var items []any
	for _, key := range keys {
		item, exists, err := b.GetByKey(key)
		if err != nil {
			return nil, err
		}
		if exists {
			items = append(items, item)
		}
	}
	return items, nil
}

// ListIndexFuncValues returns all distinct index values for indexName.
func (b *BBoltIndexer) ListIndexFuncValues(indexName string) []string {
	var values []string
	valSet := make(map[string]struct{})

	err := b.store.DB().View(func(tx *bbolt.Tx) error {
		idxBucket := tx.Bucket([]byte(b.indexBucketName(indexName)))
		if idxBucket == nil {
			return nil
		}
		return idxBucket.ForEach(func(k, _ []byte) error {
			parts := strings.SplitN(string(k), "/", 2)
			if len(parts) > 0 {
				valSet[parts[0]] = struct{}{}
			}
			return nil
		})
	})
	if err != nil {
		return nil
	}

	for val := range valSet {
		values = append(values, val)
	}
	return values
}
