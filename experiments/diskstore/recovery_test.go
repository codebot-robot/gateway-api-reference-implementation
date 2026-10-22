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

	var dbID [16]byte
	copy(dbID[:], []byte("rec-crash-db-001"))

	wal, err := OpenWAL(faultFS, "wal_dir", dbID, 0)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}
	heapFile, err := faultFS.Create("heap.db")
	if err != nil {
		t.Fatalf("Create heap failed: %v", err)
	}
	buddy := NewBuddyAllocator(512)
	bp := NewBufferPool(heapFile, wal, buddy, dbID, BufferPoolOptions{NumFrames: 16})
	cp := NewCheckpointer(bp, heapFile, wal)

	// Step 1: Put k1, k2, then Checkpoint
	_ = wal.Append(Record{Type: RecordTypePut, LSN: 1, Key: "k1", Payload: []byte("v1"), ResourceVersion: "rv1"})
	h1, _ := bp.AllocateNewPage(1, 1, 1)
	s1, _ := WrapSlottedPage(h1.Block(0))
	_, _ = s1.Put("k1", []byte("v1"), 1)
	s1.Finalize()
	h1.Unpin()

	_ = wal.Append(Record{Type: RecordTypePut, LSN: 2, Key: "k2", Payload: []byte("v2"), ResourceVersion: "rv2"})
	h2, _ := bp.AllocateNewPage(2, 1, 2)
	s2, _ := WrapSlottedPage(h2.Block(0))
	_, _ = s2.Put("k2", []byte("v2"), 2)
	s2.Finalize()
	h2.Unpin()

	chk1LSN, err := cp.Checkpoint()
	if err != nil {
		t.Fatalf("Checkpoint 1 failed: %v", err)
	}

	// Step 2: Put k3, Delete k1
	_ = wal.Append(Record{Type: RecordTypePut, LSN: chk1LSN + 1, Key: "k3", Payload: []byte("v3"), ResourceVersion: "rv3"})
	h3, _ := bp.AllocateNewPage(3, 1, chk1LSN+1)
	s3, _ := WrapSlottedPage(h3.Block(0))
	_, _ = s3.Put("k3", []byte("v3"), chk1LSN+1)
	s3.Finalize()
	h3.Unpin()

	// Step 3: Simulate crash with random tearing of un-fsynced writes
	faultFS.Crash(false, 0.5)

	_ = bp.Close()
	_ = heapFile.Close()
	_ = wal.Close()

	// Step 4: Run recovery
	reopenedHeap, _ := faultFS.OpenFile("heap.db", 0, 0)
	defer reopenedHeap.Close()

	res, err := Recover(faultFS, reopenedHeap, "wal_dir", dbID)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// State must be at least as recent as Checkpoint 1 (chk1LSN)
	if res.MaxLSN < chk1LSN {
		t.Fatalf("recovered MaxLSN %d < checkpoint 1 LSN %d", res.MaxLSN, chk1LSN)
	}

	// Both k1 and k2 must exist and be valid as of checkpoint 1
	if _, ok := res.KeyIndex["k1"]; !ok {
		t.Fatalf("k1 missing after recovery")
	}
	if _, ok := res.KeyIndex["k2"]; !ok {
		t.Fatalf("k2 missing after recovery")
	}

	// Reported resourceVersion must not be empty
	if res.LastResourceVersion == "" {
		t.Fatalf("reported resourceVersion is empty")
	}
}

func CalculateOrder(numBlocks int) int {
	order := 0
	for (1 << order) < numBlocks {
		order++
	}
	return order
}
