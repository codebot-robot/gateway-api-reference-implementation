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
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestStore_TempFileLifecycle(t *testing.T) {
	store, err := NewStore()
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	path := store.Path()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected db file to exist at %s: %v", path, err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("failed to close store: %v", err)
	}

	// Because it is a temp file with AutoDelete, it should be deleted
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected db file at %s to be deleted on Close, but it exists", path)
	}
}

func TestStore_ExplicitFilePersistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "persist.db")

	store1, err := NewStore(WithFilePath(dbPath), WithAutoDelete(false))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	if err := store1.Put("bucketA", "k1", []byte("val1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := store1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen
	store2, err := NewStore(WithFilePath(dbPath), WithAutoDelete(false))
	if err != nil {
		t.Fatalf("reopening store failed: %v", err)
	}
	defer store2.Close()

	val, exists, err := store2.Get("bucketA", "k1")
	if err != nil || !exists {
		t.Fatalf("expected k1 to exist in reopened store, exists=%v, err=%v", exists, err)
	}
	if !bytes.Equal(val, []byte("val1")) {
		t.Fatalf("got %q, expected %q", val, "val1")
	}
}

func TestStore_PutGetDeleteList(t *testing.T) {
	store, err := NewStore()
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	bucket := "testBucket"
	if err := store.Put(bucket, "k1", []byte("v1")); err != nil {
		t.Fatalf("Put k1 failed: %v", err)
	}
	if err := store.Put(bucket, "k2", []byte("v2")); err != nil {
		t.Fatalf("Put k2 failed: %v", err)
	}

	val, exists, err := store.Get(bucket, "k1")
	if err != nil || !exists || !bytes.Equal(val, []byte("v1")) {
		t.Fatalf("Get k1 failed: exists=%v, val=%s, err=%v", exists, val, err)
	}

	keys, err := store.ListKeys(bucket)
	if err != nil || len(keys) != 2 {
		t.Fatalf("ListKeys failed: keys=%v, err=%v", keys, err)
	}

	if err := store.Delete(bucket, "k1"); err != nil {
		t.Fatalf("Delete k1 failed: %v", err)
	}

	_, exists, err = store.Get(bucket, "k1")
	if err != nil || exists {
		t.Fatalf("expected k1 deleted, exists=%v, err=%v", exists, err)
	}
}

func TestStore_ConcurrentAccess(t *testing.T) {
	store, err := NewStore()
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	bucket := "concurrentBucket"
	var wg sync.WaitGroup

	// 10 writers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = store.Put(bucket, "key", []byte("value"))
			}
		}(i)
	}

	// 20 readers
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, _, _ = store.Get(bucket, "key")
			}
		}()
	}

	wg.Wait()
}
