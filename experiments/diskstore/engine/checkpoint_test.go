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
	"testing"
)

func TestCheckpointer_BasicCheckpoint(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)

	var dbID [16]byte
	copy(dbID[:], []byte("chk-test-db-0001"))

	wal, err := OpenWAL(faultFS, "wal_dir", dbID, 0, WALOptions{MaxSegmentSize: 100})
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
	bp := NewBufferPool(heapFile, wal, buddy, dbID, BufferPoolOptions{NumFrames: 16})
	defer bp.Close()

	checkpointer := NewCheckpointer(bp, heapFile, wal)

	// Write 5 records to WAL and dirty 2 pages
	for i := uint64(1); i <= 5; i++ {
		_ = wal.Append(Record{Type: RecordTypePut, LSN: i, Key: "k", Payload: []byte("val")})
	}

	h1, _ := bp.AllocateNewPage(1, 1, 3)
	copy(h1.Block(0)[BlockHeaderSize:], []byte("page1-data"))
	h1.Unpin()

	h2, _ := bp.AllocateNewPage(2, 1, 5)
	copy(h2.Block(0)[BlockHeaderSize:], []byte("page2-data"))
	h2.Unpin()

	// Checkpoint
	chkLSN, err := checkpointer.Checkpoint()
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if chkLSN <= 5 {
		t.Fatalf("expected checkpoint LSN > 5, got %d", chkLSN)
	}

	// Verify both pages are now durable and not dirty
	if h1.Desc.Dirty || h2.Desc.Dirty {
		t.Fatalf("pages should not be dirty after checkpoint")
	}
	if h1.Desc.DurableLoc == NoBlock || h2.Desc.DurableLoc == NoBlock {
		t.Fatalf("pages should have durable locations after checkpoint")
	}
}
