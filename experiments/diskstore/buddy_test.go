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
	"testing"
)

func TestBuddyAllocator_AllocateAndFree(t *testing.T) {
	alloc := NewBuddyAllocator(512)

	// Allocate order 0 (1 block)
	b0, err := alloc.Allocate(0)
	if err != nil || b0 != 0 {
		t.Fatalf("expected block 0, got %d, err=%v", b0, err)
	}

	// Allocate order 0 again -> should be block 1
	b1, err := alloc.Allocate(0)
	if err != nil || b1 != 1 {
		t.Fatalf("expected block 1, got %d, err=%v", b1, err)
	}

	// Allocate order 2 (4 blocks) -> aligned to 4 -> block 4
	b2, err := alloc.Allocate(2)
	if err != nil || b2%4 != 0 {
		t.Fatalf("expected 4-aligned block, got %d, err=%v", b2, err)
	}

	// Free b0
	if err := alloc.Free(b0); err != nil {
		t.Fatalf("Free b0 failed: %v", err)
	}

	// Next order 0 allocation must prefer lowest free location (reuse block 0)
	b0Reused, err := alloc.Allocate(0)
	if err != nil || b0Reused != 0 {
		t.Fatalf("expected reuse of lowest free block 0, got %d", b0Reused)
	}

	// Free b0Reused and b1 -> should coalesce into order 1 at block 0
	_ = alloc.Free(b0Reused)
	_ = alloc.Free(b1)

	// Now allocate order 1 (2 blocks) -> should get block 0
	bOrder1, err := alloc.Allocate(1)
	if err != nil || bOrder1 != 0 {
		t.Fatalf("expected coalesced order 1 at block 0, got %d", bOrder1)
	}

	_ = alloc.Free(bOrder1)
	_ = alloc.Free(b2)
}

func TestBuddyAllocator_MarkAllocated(t *testing.T) {
	alloc := NewBuddyAllocator(512)

	// Simulate recovery: mark block 16 as allocated (order 2 = 4 blocks, [16..20))
	if err := alloc.MarkAllocated(16, 2); err != nil {
		t.Fatalf("MarkAllocated failed: %v", err)
	}

	// Allocate order 2: should get block 0 (lowest available)
	b0, err := alloc.Allocate(2)
	if err != nil || b0 != 0 {
		t.Fatalf("expected block 0, got %d", b0)
	}

	// Keep allocating until we reach 16: block 16 must NOT be returned!
	for i := 0; i < 10; i++ {
		blk, err := alloc.Allocate(2)
		if err != nil {
			t.Fatalf("Allocate failed: %v", err)
		}
		if blk == 16 {
			t.Fatalf("allocator returned pre-allocated block 16!")
		}
	}
}

func TestBuddyAllocator_TailTruncation(t *testing.T) {
	memFS := NewMemFS()
	file, _ := memFS.Create("heap.db")
	defer file.Close()

	alloc := NewBuddyAllocator(1024)

	// Allocate blocks up to block 10
	b0, _ := alloc.Allocate(0)
	b1, _ := alloc.Allocate(1)

	// Tail truncation should shrink capacity from 1024 down to 512
	newSize, err := alloc.TruncateTail(file)
	if err != nil {
		t.Fatalf("TruncateTail failed: %v", err)
	}

	if newSize != 512*BlockSize {
		t.Fatalf("expected truncated size %d, got %d", 512*BlockSize, newSize)
	}

	_ = alloc.Free(b0)
	_ = alloc.Free(b1)
}
