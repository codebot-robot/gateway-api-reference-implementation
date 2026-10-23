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

func TestBufferPool_PinAndEviction(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)

	var dbID [16]byte
	copy(dbID[:], []byte("bp-test-db-00001"))

	wal, err := OpenWAL(faultFS, "wal_dir", dbID, 0)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}
	defer wal.Close()

	heapFile, err := faultFS.Create("heap.db")
	if err != nil {
		t.Fatalf("Create heap file failed: %v", err)
	}
	defer heapFile.Close()

	buddy := NewBuddyAllocator(512)

	// Small buffer pool of 4 frames to test frame eviction under pressure
	bp := NewBufferPool(heapFile, wal, buddy, dbID, BufferPoolOptions{NumFrames: 4})
	defer bp.Close()

	// Append WAL record for LSN 10
	_ = wal.Append(Record{Type: RecordTypePut, LSN: 10, Key: "k1", Payload: []byte("val1")})

	// Allocate page 1 (1 block)
	h1, err := bp.AllocateNewPage(1, 1, 10)
	if err != nil {
		t.Fatalf("AllocateNewPage failed: %v", err)
	}
	copy(h1.Block(0)[BlockHeaderSize:], []byte("page-1-data"))
	h1.Unpin()

	// Flush page 1 -> writes to disk under write-ahead rule
	if err := bp.FlushPage(1); err != nil {
		t.Fatalf("FlushPage failed: %v", err)
	}

	// Verify page was written to disk and can be read back after eviction
	if err := heapFile.Sync(); err != nil {
		t.Fatalf("heapFile.Sync failed: %v", err)
	}
	if err := bp.PromoteFlushedToDurable(); err != nil {
		t.Fatalf("PromoteFlushedToDurable failed: %v", err)
	}

	// Allocate 4 new pages (pages 2, 3, 4, 5) to force eviction of page 1 from the 4-frame pool
	for i := uint64(2); i <= 5; i++ {
		_ = wal.Append(Record{Type: RecordTypePut, LSN: 10 + i, Key: "k", Payload: []byte("v")})
		h, err := bp.AllocateNewPage(i, 1, 10+i)
		if err != nil {
			t.Fatalf("AllocateNewPage %d failed: %v", i, err)
		}
		h.Unpin()
	}

	// Re-pin page 1 from disk
	loc := h1.Desc.DurableLoc
	h1Reloaded, err := bp.PinPageWithLocation(1, loc, 1)
	if err != nil {
		t.Fatalf("PinPageWithLocation failed: %v", err)
	}
	defer h1Reloaded.Unpin()

	if !bytes.HasPrefix(h1Reloaded.Block(0)[BlockHeaderSize:], []byte("page-1-data")) {
		t.Fatalf("reloaded page data mismatch: %s", string(h1Reloaded.Block(0)[BlockHeaderSize:BlockHeaderSize+12]))
	}
}

func TestBufferPool_MultiBlockPageNonAdjacentFrames(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)

	var dbID [16]byte
	copy(dbID[:], []byte("bp-test-db-00002"))

	wal, _ := OpenWAL(faultFS, "wal_dir", dbID, 0)
	defer wal.Close()

	heapFile, _ := faultFS.Create("heap.db")
	defer heapFile.Close()

	buddy := NewBuddyAllocator(512)

	// Buffer pool of 16 frames
	bp := NewBufferPool(heapFile, wal, buddy, dbID, BufferPoolOptions{NumFrames: 16})
	defer bp.Close()

	// Allocate some single block pages first, then free some to create non-adjacent free frames
	_ = wal.Append(Record{Type: RecordTypePut, LSN: 50, Key: "k", Payload: []byte("v")})
	hA, _ := bp.AllocateNewPage(10, 1, 50)
	hB, _ := bp.AllocateNewPage(11, 1, 50)
	hC, _ := bp.AllocateNewPage(12, 1, 50)
	hA.Unpin()
	hB.Unpin()
	hC.Unpin()

	// Allocate a 4-block page (multi-block)
	hMulti, err := bp.AllocateNewPage(20, 4, 50)
	if err != nil {
		t.Fatalf("Allocate multi-block page failed: %v", err)
	}
	defer hMulti.Unpin()

	if len(hMulti.Blocks()) != 4 {
		t.Fatalf("expected 4 blocks, got %d", len(hMulti.Blocks()))
	}

	for i := 0; i < 4; i++ {
		copy(hMulti.Block(i)[BlockHeaderSize:], []byte("block-content"))
	}

	// Flush multi-block page
	if err := bp.FlushPage(20); err != nil {
		t.Fatalf("FlushPage failed: %v", err)
	}
}
