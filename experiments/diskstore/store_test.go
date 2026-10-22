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
	"bytes"
	"errors"
	"testing"
)

func TestDiskStore_BasicCRUD(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)

	store, err := OpenStore(WithFS(faultFS), WithDir("test_store"))
	if err != nil {
		t.Fatalf("OpenStore failed: %v", err)
	}
	defer store.Close()

	// Put
	payload := []byte("hello-diskstore-world")
	if err := store.Put("ConfigMap", "default/cm-1", payload, "1001"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get
	got, exists, err := store.Get("ConfigMap", "default/cm-1")
	if err != nil || !exists {
		t.Fatalf("Get failed: exists=%v, err=%v", exists, err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %s, want %s", string(got), string(payload))
	}

	// Delete
	if err := store.Delete("ConfigMap", "default/cm-1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, exists, err = store.Get("ConfigMap", "default/cm-1")
	if err != nil || exists {
		t.Fatalf("expected deleted, exists=%v, err=%v", exists, err)
	}
}

func TestDiskStore_OversizeObject(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)

	store, err := OpenStore(WithFS(faultFS), WithDir("test_oversize_store"))
	if err != nil {
		t.Fatalf("OpenStore failed: %v", err)
	}
	defer store.Close()

	initialCount := OversizeCount()

	// 3 MiB payload (> MaxPayloadCapacity ~2 MiB)
	oversizePayload := make([]byte, 3*1024*1024)
	copy(oversizePayload, []byte("large-payload-prefix-data"))

	// Write must NOT fail
	if err := store.Put("ConfigMap", "default/large-cm", oversizePayload, "1002"); err != nil {
		t.Fatalf("Put oversize object failed: %v", err)
	}

	// Metric must increment
	if OversizeCount() != initialCount+1 {
		t.Fatalf("expected OversizeCount %d, got %d", initialCount+1, OversizeCount())
	}

	// Get should retrieve intact
	got, exists, err := store.Get("ConfigMap", "default/large-cm")
	if err != nil || !exists {
		t.Fatalf("Get oversize failed: exists=%v, err=%v", exists, err)
	}
	if !bytes.Equal(got, oversizePayload) {
		t.Fatalf("oversize payload mismatch")
	}

	// Delete
	if err := store.Delete("ConfigMap", "default/large-cm"); err != nil {
		t.Fatalf("Delete oversize failed: %v", err)
	}

	// Metric must decrement
	if OversizeCount() != initialCount {
		t.Fatalf("expected OversizeCount %d after delete, got %d", initialCount, OversizeCount())
	}
}

func TestDiskStore_FailedStateOnErrors(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)

	store, err := OpenStore(WithFS(faultFS), WithDir("test_fail_store"))
	if err != nil {
		t.Fatalf("OpenStore failed: %v", err)
	}
	defer store.Close()

	_ = store.Put("ConfigMap", "default/cm-1", []byte("data-1"), "1")

	// Inject write error into filesystem
	injectedErr := errors.New("simulated unrecoverable disk error")
	faultFS.SetWriteError(injectedErr)

	// Next Put will fail and transition store into failed state
	err = store.Put("ConfigMap", "default/cm-2", []byte("data-2"), "2")
	if err == nil {
		t.Fatalf("expected error on injected write failure")
	}

	// Invariant: Failed store returns error rather than stale data on ANY subsequent read
	_, _, err = store.Get("ConfigMap", "default/cm-1")
	if !errors.Is(err, ErrStoreFailed) {
		t.Fatalf("expected ErrStoreFailed on read after failure, got %v", err)
	}
}

func TestDiskStore_StaleReadDetection(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)

	store, err := OpenStore(WithFS(faultFS), WithDir("test_stale_store"))
	if err != nil {
		t.Fatalf("OpenStore failed: %v", err)
	}
	defer store.Close()

	// Put version 1
	_ = store.Put("ConfigMap", "default/cm-stale", []byte("val-v1"), "1")

	// Put version 2 (LSN increments)
	_ = store.Put("ConfigMap", "default/cm-stale", []byte("val-v2"), "2")

	// Artificially modify the key index LSN to expect higher LSN (simulating page roll-back / substitution)
	store.keyMu.Lock()
	entry := store.keyIndex["default/cm-stale"]
	entry.LSN = 999 // Mismatch!
	store.keyIndex["default/cm-stale"] = entry
	store.keyMu.Unlock()

	// Read must detect mismatch, report corruption, and put store in failed state
	_, _, err = store.Get("ConfigMap", "default/cm-stale")
	if !errors.Is(err, ErrStoreFailed) {
		t.Fatalf("expected ErrStoreFailed on stale read detection, got %v", err)
	}
}
