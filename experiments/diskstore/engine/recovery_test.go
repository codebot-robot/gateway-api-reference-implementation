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

package engine

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestRecovery_TornMultiBlockPage(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)

	var dbID [16]byte
	copy(dbID[:], []byte("rec-torn-db-0001"))

	wal, _ := OpenWAL(faultFS, "wal_dir", dbID, 0)
	heapFile, _ := faultFS.Create("heap.db")
	buddy := NewBuddyAllocator(512)
	bp := NewBufferPool(heapFile, wal, buddy, dbID, BufferPoolOptions{NumFrames: 16})

	// 1. Write Version 1 of a multi-block page (LSN 10) and make it durable
	payloadV1 := bytes.Repeat([]byte("v1-payload-"), 1000) // ~11 KB -> 4 blocks
	_ = wal.Append(Record{Type: RecordTypePut, LSN: 10, Key: "large-obj", Payload: payloadV1, ResourceVersion: "10"})
	_ = wal.Sync()

	mbV1, err := NewMultiBlockPage(1, 10, "large-obj", payloadV1)
	if err != nil {
		t.Fatalf("NewMultiBlockPage v1 failed: %v", err)
	}
	locV1, _ := buddy.Allocate(2) // 4 blocks
	_ = NewDirectIO().Writev(heapFile, int64(locV1*BlockSize), mbV1.Blocks)
	_ = heapFile.Sync()

	// 2. Write Version 2 of the multi-block page (LSN 20) to a new location, but TEAR one of its blocks
	payloadV2 := bytes.Repeat([]byte("v2-payload-"), 1000)
	_ = wal.Append(Record{Type: RecordTypePut, LSN: 20, Key: "large-obj", Payload: payloadV2, ResourceVersion: "20"})
	_ = wal.Sync()

	mbV2, err := NewMultiBlockPage(1, 20, "large-obj", payloadV2)
	if err != nil {
		t.Fatalf("NewMultiBlockPage v2 failed: %v", err)
	}

	// Corrupt block 2 of V2 (simulate torn write)
	mbV2.Blocks[2][BlockHeaderSize+10] ^= 0xff
	locV2, _ := buddy.Allocate(2)
	_ = NewDirectIO().Writev(heapFile, int64(locV2*BlockSize), mbV2.Blocks)
	_ = heapFile.Sync()

	_ = bp.Close()
	_ = heapFile.Close()
	_ = wal.Close()

	// 3. Run Recovery
	reopenedHeap, _ := faultFS.OpenFile("heap.db", 0, 0)
	defer reopenedHeap.Close()

	res, err := Recover(faultFS, reopenedHeap, "wal_dir", dbID)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// Recovery must fall back to Version 1 (LSN 10)
	entry, exists := res.KeyIndex["large-obj"]
	if !exists {
		t.Fatalf("expected large-obj to exist after recovery")
	}
	if entry.LSN != 10 {
		t.Fatalf("expected fallback to LSN 10, got %d", entry.LSN)
	}
}

func TestRecovery_InvalidMagicBlocks(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)
	faultFS.SetMaxFsyncedLSN(100)

	var dbID [16]byte
	copy(dbID[:], []byte("rec-foreign-db01"))

	heapFile, _ := faultFS.Create("heap.db")

	// Write a valid block
	b1 := make([]byte, BlockSize)
	EncodeBlockHeader(BlockHeader{LogicalPage: 1, LSN: 5, NumBlocks: 1}, b1)
	FinalizeBlockCRC(b1)
	_, _ = heapFile.WriteAt(b1, 0)

	// Write an invalid block (corrupt magic)
	b2 := make([]byte, BlockSize)
	EncodeBlockHeader(BlockHeader{LogicalPage: 2, LSN: 10, NumBlocks: 1}, b2)
	b2[0] = 0xff // invalid magic
	FinalizeBlockCRC(b2)
	_, _ = heapFile.WriteAt(b2, BlockSize)
	_ = heapFile.Sync()

	// Run recovery
	res, err := Recover(faultFS, heapFile, "wal_dir", dbID)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// Page 1 should be recovered; invalid magic page 2 must be ignored
	if _, ok := res.PageDescs[1]; !ok {
		t.Fatalf("page 1 should be recovered")
	}
	if _, ok := res.PageDescs[2]; ok {
		t.Fatalf("invalid page 2 should NOT be recovered")
	}
}

func TestRecovery_NoResurrection(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)

	var dbID [16]byte
	copy(dbID[:], []byte("rec-no-res-db-01"))

	wal, _ := OpenWAL(faultFS, "wal_dir", dbID, 0)
	heapFile, _ := faultFS.Create("heap.db")
	buddy := NewBuddyAllocator(512)
	bp := NewBufferPool(heapFile, wal, buddy, dbID, BufferPoolOptions{NumFrames: 16})
	cp := NewCheckpointer(bp, heapFile, wal)

	// Add key-1, checkpoint, then delete key-1 and checkpoint again
	_ = wal.Append(Record{Type: RecordTypePut, LSN: 1, Key: "key-1", Payload: []byte("val-1"), ResourceVersion: "1"})
	h1, _ := bp.AllocateNewPage(1, 1, 1)
	slotted, _ := WrapSlottedPage(h1.Block(0))
	_, _ = slotted.Put("key-1", []byte("val-1"), 1)
	slotted.Finalize()
	h1.Unpin()

	_, _ = cp.Checkpoint()

	// Now delete key-1
	_ = wal.Append(Record{Type: RecordTypeDelete, LSN: 3, Key: "key-1", ResourceVersion: "3"})
	h1Reload, _ := bp.PinPage(1)
	slottedReload, _ := WrapSlottedPage(h1Reload.Block(0))
	_ = slottedReload.Delete(0, 3)
	slottedReload.Finalize()
	bp.MarkDirty(1, 3)
	h1Reload.Unpin()

	_, _ = cp.Checkpoint()

	_ = bp.Close()
	_ = heapFile.Close()
	_ = wal.Close()

	// Recovery
	reopenedHeap, _ := faultFS.OpenFile("heap.db", 0, 0)
	defer reopenedHeap.Close()

	res, err := Recover(faultFS, reopenedHeap, "wal_dir", dbID)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// Invariant: deleted key-1 must NOT resurrect
	if _, exists := res.KeyIndex["key-1"]; exists {
		t.Fatalf("deleted object resurrected after recovery!")
	}
}

func TestRecovery_ForgedHeaders(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)

	var dbID [16]byte
	copy(dbID[:], []byte("rec-forge-db-001"))

	wal, _ := OpenWAL(faultFS, "wal_dir", dbID, 0)
	heapFile, _ := faultFS.Create("heap.db")
	buddy := NewBuddyAllocator(512)
	bp := NewBufferPool(heapFile, wal, buddy, dbID, BufferPoolOptions{NumFrames: 16})

	// Construct an untrusted payload that contains forged block headers with valid CRCs,
	// high LSN (9999), and target logical page placed at EVERY offset.
	forgedHeader := make([]byte, BlockSize)
	EncodeBlockHeader(BlockHeader{
		LogicalPage: 1,
		LSN:         9999, // higher than any real LSN
		BlockIndex:  0,
		NumBlocks:   1,
	}, forgedHeader)
	FinalizeBlockCRC(forgedHeader)

	// Combine into a multi-block object payload
	var maliciousPayload []byte
	for offset := 0; offset < 20000; offset += 500 {
		maliciousPayload = append(maliciousPayload, forgedHeader...)
	}

	// Store real legitimate object first at LSN 5
	_ = wal.Append(Record{Type: RecordTypePut, LSN: 5, Key: "legit-key", Payload: []byte("legit-val"), ResourceVersion: "5"})
	h1, _ := bp.AllocateNewPage(1, 1, 5)
	s1, _ := WrapSlottedPage(h1.Block(0))
	_, _ = s1.Put("legit-key", []byte("legit-val"), 5)
	s1.Finalize()
	h1.Unpin()
	_ = bp.FlushPage(1)

	// Now store the malicious object containing forged headers at LSN 10
	_ = wal.Append(Record{Type: RecordTypePut, LSN: 10, Key: "malicious-key", Payload: maliciousPayload, ResourceVersion: "10"})
	_ = wal.Sync()
	mbPage, err := NewMultiBlockPage(2, 10, "malicious-key", maliciousPayload)
	if err != nil {
		t.Fatalf("NewMultiBlockPage failed: %v", err)
	}

	loc2, _ := buddy.Allocate(CalculateOrder(len(mbPage.Blocks)))
	_ = NewDirectIO().Writev(heapFile, int64(loc2*BlockSize), mbPage.Blocks)
	_ = heapFile.Sync()

	_ = bp.Close()
	_ = heapFile.Close()
	_ = wal.Close()

	// Run Recovery
	reopenedHeap, _ := faultFS.OpenFile("heap.db", 0, 0)
	defer reopenedHeap.Close()

	res, err := Recover(faultFS, reopenedHeap, "wal_dir", dbID)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// Invariant: The forged header with LSN 9999 must NOT be accepted!
	// Logical page 1 must have LSN 5, and legit-key must be intact!
	legitEntry, exists := res.KeyIndex["legit-key"]
	if !exists {
		t.Fatalf("legit-key missing after recovery")
	}
	if legitEntry.LSN != 5 {
		t.Fatalf("forged header accepted! LSN is %d, expected 5", legitEntry.LSN)
	}
}

func TestRecovery_CrashConsistency(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)
	dir := "test_crash_consistency_store"

	// Model mapping key -> expected payload value
	model := make(map[string]string)

	// Cycle 1: Open fresh store, put initial keys and a large multi-block object, checkpoint
	store, err := OpenStore(WithFS(faultFS), WithDir(dir))
	if err != nil {
		t.Fatalf("Cycle 1 OpenStore failed: %v", err)
	}

	largeVal := bytes.Repeat([]byte("large-payload-bytes-"), 500) // ~10 KB
	_ = store.Put("ConfigMap", "key1", []byte("val1-initial"), "1")
	model["key1"] = "val1-initial"

	_ = store.Put("ConfigMap", "key2", []byte("val2-initial"), "2")
	model["key2"] = "val2-initial"

	_ = store.Put("ConfigMap", "key-large", largeVal, "3")
	model["key-large"] = string(largeVal)

	chk1, err := store.Checkpoint()
	if err != nil {
		t.Fatalf("Checkpoint 1 failed: %v", err)
	}

	// Update key1 and put key3
	_ = store.Put("ConfigMap", "key1", []byte("val1-updated"), "4")
	model["key1"] = "val1-updated"

	_ = store.Put("ConfigMap", "key3", []byte("val3-initial"), "5")
	model["key3"] = "val3-initial"

	// Checkpoint 2
	chk2, err := store.Checkpoint()
	if err != nil {
		t.Fatalf("Checkpoint 2 failed: %v", err)
	}
	if chk2 <= chk1 {
		t.Fatalf("checkpoint LSN did not advance: chk2=%d <= chk1=%d", chk2, chk1)
	}

	// Un-checkpointed writes before crash
	_ = store.Put("ConfigMap", "key4", []byte("val4-uncheckpointed"), "6")
	model["key4"] = "val4-uncheckpointed"

	// Simulate crash with tearing of un-fsynced writes
	faultFS.Crash(false, 0.5)
	_ = store.Close()

	// Cycle 2: Reopen store and verify full contents against model
	store2, err := OpenStore(WithFS(faultFS), WithDir(dir))
	if err != nil {
		t.Fatalf("Cycle 2 OpenStore failed: %v", err)
	}

	for k, expectedVal := range model {
		val, exists, err := store2.Get("ConfigMap", k)
		if err != nil {
			t.Fatalf("Cycle 2 Get(%s) failed: %v", k, err)
		}
		if !exists {
			t.Fatalf("Cycle 2 Get(%s) missing after recovery", k)
		}
		if string(val) != expectedVal {
			t.Fatalf("Cycle 2 Get(%s) payload mismatch: got %q, want %q", k, string(val), expectedVal)
		}
	}

	// In Cycle 2: Delete key1, update key2, put key5, and Checkpoint 3
	if err := store2.Delete("ConfigMap", "key1"); err != nil {
		t.Fatalf("Delete key1 failed: %v", err)
	}
	delete(model, "key1")

	_ = store2.Put("ConfigMap", "key2", []byte("val2-cycle2"), "7")
	model["key2"] = "val2-cycle2"

	_ = store2.Put("ConfigMap", "key5", []byte("val5-cycle2"), "8")
	model["key5"] = "val5-cycle2"

	_, err = store2.Checkpoint()
	if err != nil {
		t.Fatalf("Checkpoint 3 failed: %v", err)
	}

	// Crash again with drop of unsynced writes (power loss simulation)
	faultFS.Crash(true, 0)
	_ = store2.Close()

	// Cycle 3: Reopen store again and verify full contents against updated model
	store3, err := OpenStore(WithFS(faultFS), WithDir(dir))
	if err != nil {
		t.Fatalf("Cycle 3 OpenStore failed: %v", err)
	}
	defer store3.Close()

	// key1 was deleted; must not exist
	if _, exists, _ := store3.Get("ConfigMap", "key1"); exists {
		t.Fatalf("deleted key1 resurrected after cycle 3 recovery!")
	}

	for k, expectedVal := range model {
		val, exists, err := store3.Get("ConfigMap", k)
		if err != nil {
			t.Fatalf("Cycle 3 Get(%s) failed: %v", k, err)
		}
		if !exists {
			t.Fatalf("Cycle 3 Get(%s) missing after recovery", k)
		}
		if string(val) != expectedVal {
			t.Fatalf("Cycle 3 Get(%s) payload mismatch: got %q, want %q", k, string(val), expectedVal)
		}
	}
}

// TestRegression_Item1_LSNReopenContinuation asserts LSNs continue from recovered max and start a new segment.
func TestRegression_Item1_LSNReopenContinuation(t *testing.T) {
	memFS := NewMemFS()
	dir := "test_reopen_lsn_continuation"

	store, err := OpenStore(WithFS(memFS), WithDir(dir))
	if err != nil {
		t.Fatalf("OpenStore failed: %v", err)
	}

	_ = store.Put("ConfigMap", "k1", []byte("v1"), "1")
	_ = store.Put("ConfigMap", "k2", []byte("v2"), "2")
	chkLSN, _ := store.Checkpoint()
	_ = store.Close()

	// Reopen store
	store2, err := OpenStore(WithFS(memFS), WithDir(dir))
	if err != nil {
		t.Fatalf("Reopen OpenStore failed: %v", err)
	}
	defer store2.Close()

	// First Put after reopen must NOT fail with "segment already used" and must have LSN > chkLSN
	if err := store2.Put("ConfigMap", "k3", []byte("v3"), "3"); err != nil {
		t.Fatalf("first Put after reopen failed: %v", err)
	}

	store2.keyMu.RLock()
	entryK3 := store2.keyIndex[qualifyKey("ConfigMap", "k3")]
	store2.keyMu.RUnlock()

	if entryK3.LSN <= chkLSN {
		t.Fatalf("LSN went backwards after reopen: k3 LSN=%d <= checkpoint LSN=%d", entryK3.LSN, chkLSN)
	}
}

// TestRegression_Item2_TornTailAdvance asserts WAL iterator advances across segments after a torn tail.
func TestRegression_Item2_TornTailAdvance(t *testing.T) {
	memFS := NewMemFS()
	var dbID [16]byte
	copy(dbID[:], []byte("wal-torn-test-01"))

	wal, err := OpenWAL(memFS, "wal_dir", dbID, 0, WALOptions{MaxSegmentSize: 4096})
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	// Write records to segment 1 (LSN 1..5)
	for i := uint64(1); i <= 5; i++ {
		_ = wal.Append(Record{Type: RecordTypePut, LSN: i, Key: fmt.Sprintf("k%d", i), Payload: []byte("v")})
	}
	_ = wal.Sync()

	// Write records to segment 2 (LSN 6..10)
	for i := uint64(6); i <= 10; i++ {
		_ = wal.Append(Record{Type: RecordTypePut, LSN: i, Key: fmt.Sprintf("k%d", i), Payload: []byte("v")})
	}
	_ = wal.Sync()
	_ = wal.Close()

	// Corrupt tail of segment 1 (simulate torn crash at tail of seg 1)
	entries, _ := memFS.ReadDir("wal_dir")
	seg1File, _ := memFS.OpenFile(filepath.Join("wal_dir", entries[0].Name()), os.O_RDWR, 0644)
	buf := make([]byte, 200)
	_, _ = seg1File.ReadAt(buf, 200)
	buf[50] ^= 0xff // corrupt record in seg 1
	_, _ = seg1File.WriteAt(buf, 200)
	_ = seg1File.Close()

	// Iterator with expected continuation should advance to segment 2
	it, err := NewWALIterator(memFS, "wal_dir", dbID, 1)
	if err != nil {
		t.Fatalf("NewWALIterator failed: %v", err)
	}
	defer it.Close()

	var replayed []uint64
	for {
		rec, err := it.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("unexpected iterator error: %v", err)
		}
		replayed = append(replayed, rec.LSN)
	}

	// Must have read records from segment 2
	hasSeg2 := false
	for _, lsn := range replayed {
		if lsn >= 6 {
			hasSeg2 = true
			break
		}
	}
	if !hasSeg2 {
		t.Fatalf("iterator failed to advance to segment 2 after torn tail; replayed: %v", replayed)
	}
}

// TestRegression_Item3_RedoAppliesPayload asserts un-checkpointed WAL writes apply to page buffers during redo.
func TestRegression_Item3_RedoAppliesPayload(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)
	dir := "test_redo_applies_payload"

	store, err := OpenStore(WithFS(faultFS), WithDir(dir))
	if err != nil {
		t.Fatalf("OpenStore failed: %v", err)
	}

	// Write keys without checkpointing
	_ = store.Put("ConfigMap", "key-redo-1", []byte("payload-redo-1"), "1")
	_ = store.Put("ConfigMap", "key-redo-2", []byte("payload-redo-2"), "2")

	// Sync WAL so records are durable, but do NOT sync heap
	_ = store.wal.Sync()

	// Simulate crash
	_ = store.Close()

	// Reopen store: recovery must redo the puts onto page buffers
	store2, err := OpenStore(WithFS(faultFS), WithDir(dir))
	if err != nil {
		t.Fatalf("Reopen OpenStore failed: %v", err)
	}
	defer store2.Close()

	val1, exists, err := store2.Get("ConfigMap", "key-redo-1")
	if err != nil || !exists || string(val1) != "payload-redo-1" {
		t.Fatalf("key-redo-1 not recovered: exists=%v, val=%q, err=%v", exists, string(val1), err)
	}

	val2, exists, err := store2.Get("ConfigMap", "key-redo-2")
	if err != nil || !exists || string(val2) != "payload-redo-2" {
		t.Fatalf("key-redo-2 not recovered: exists=%v, val=%q, err=%v", exists, string(val2), err)
	}
}

// TestRegression_Item4_HighestLSNKeySelection asserts recovery always picks the copy with the highest LSN.
func TestRegression_Item4_HighestLSNKeySelection(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)
	faultFS.SetMaxFsyncedLSN(100)

	var dbID [16]byte
	copy(dbID[:], []byte("rec-highest-lsn-"))

	wal, _ := OpenWAL(faultFS, "wal_dir", dbID, 0)
	heapFile, _ := faultFS.Create("heap.db")
	buddy := NewBuddyAllocator(512)
	bp := NewBufferPool(heapFile, wal, buddy, dbID, BufferPoolOptions{NumFrames: 16})

	// Page 1: holds key at LSN 10
	h1, _ := bp.AllocateNewPage(1, 1, 10)
	s1, _ := WrapSlottedPage(h1.Block(0))
	_, _ = s1.Put("target-key", []byte("older-val"), 10)
	s1.Finalize()
	h1.Unpin()
	_ = bp.FlushPage(1)

	// Page 2: holds key at LSN 20 (newer!)
	h2, _ := bp.AllocateNewPage(2, 1, 20)
	s2, _ := WrapSlottedPage(h2.Block(0))
	_, _ = s2.Put("target-key", []byte("newer-val"), 20)
	s2.Finalize()
	h2.Unpin()
	_ = bp.FlushPage(2)

	_ = heapFile.Sync()
	_ = bp.Close()
	_ = heapFile.Close()
	_ = wal.Close()

	reopenedHeap, _ := faultFS.OpenFile("heap.db", 0, 0)
	defer reopenedHeap.Close()

	res, err := Recover(faultFS, reopenedHeap, "wal_dir", dbID)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	entry, exists := res.KeyIndex["target-key"]
	if !exists {
		t.Fatalf("target-key missing from recovered index")
	}
	if entry.LSN != 20 {
		t.Fatalf("recovery chose stale LSN %d instead of highest LSN 20", entry.LSN)
	}
}

func CalculateOrder(numBlocks int) int {
	order := 0
	for (1 << order) < numBlocks {
		order++
	}
	return order
}
