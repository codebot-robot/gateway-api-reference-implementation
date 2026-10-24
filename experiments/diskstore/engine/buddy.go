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
	"fmt"
	"sort"
	"sync"
)

const (
	// MinOrder is 0 (1 block = 4 KiB).
	MinOrder = 0

	// MaxOrder is 9 (512 blocks = 2 MiB).
	MaxOrder = 9

	// DefaultGrowBlocks is 512 blocks (2 MiB).
	DefaultGrowBlocks = 512
)

// BuddyAllocator manages physical blocks in the heap file using a buddy allocation system.
// It prioritizes the lowest free block locations so that the tail of the file empties and can be truncated.
type BuddyAllocator struct {
	mu sync.Mutex

	// freeLists holds sorted lists of free block start indices for each order (0..MaxOrder).
	freeLists [MaxOrder + 1][]uint64

	// allocated tracks allocated block index -> order.
	allocated map[uint64]int

	// totalBlocks is the current physical capacity of the heap file in blocks.
	totalBlocks uint64
}

// NewBuddyAllocator creates a buddy allocator with initial capacity in blocks.
func NewBuddyAllocator(initialBlocks uint64) *BuddyAllocator {
	alloc := &BuddyAllocator{
		allocated: make(map[uint64]int),
	}
	if initialBlocks > 0 {
		alloc.growLocked(initialBlocks)
	}
	return alloc
}

// TotalBlocks returns the total capacity in blocks.
func (b *BuddyAllocator) TotalBlocks() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.totalBlocks
}

// AllocatedCount returns the number of currently allocated blocks.
func (b *BuddyAllocator) AllocatedCount() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var count uint64
	for _, order := range b.allocated {
		count += 1 << order
	}
	return count
}

func (b *BuddyAllocator) insertFree(order int, block uint64) {
	list := b.freeLists[order]
	idx := sort.Search(len(list), func(i int) bool {
		return list[i] >= block
	})
	if idx < len(list) && list[idx] == block {
		return // already present
	}
	list = append(list, 0)
	copy(list[idx+1:], list[idx:])
	list[idx] = block
	b.freeLists[order] = list
}

func (b *BuddyAllocator) removeFree(order int, block uint64) bool {
	list := b.freeLists[order]
	idx := sort.Search(len(list), func(i int) bool {
		return list[i] >= block
	})
	if idx < len(list) && list[idx] == block {
		b.freeLists[order] = append(list[:idx], list[idx+1:]...)
		return true
	}
	return false
}

func (b *BuddyAllocator) growLocked(growBlocks uint64) {
	// Round up to multiple of MaxOrder (512 blocks) to ensure natural alignment
	unit := uint64(1 << MaxOrder)
	if growBlocks%unit != 0 {
		growBlocks = ((growBlocks + unit - 1) / unit) * unit
	}

	start := b.totalBlocks
	end := start + growBlocks
	b.totalBlocks = end

	// Add new blocks as MaxOrder chunks
	for blk := start; blk < end; blk += unit {
		b.insertFree(MaxOrder, blk)
	}
}

// Allocate reserves 2^order contiguous blocks, preferring the lowest available block index.
func (b *BuddyAllocator) Allocate(order int) (uint64, error) {
	if order < MinOrder || order > MaxOrder {
		return 0, fmt.Errorf("invalid order %d (must be between %d and %d)", order, MinOrder, MaxOrder)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	return b.allocateLocked(order)
}

func (b *BuddyAllocator) allocateLocked(order int) (uint64, error) {
	// Find order >= order with the lowest available block index
	targetOrder := -1
	var lowestBlock uint64 = ^uint64(0)

	for o := order; o <= MaxOrder; o++ {
		if len(b.freeLists[o]) > 0 {
			blk := b.freeLists[o][0]
			if blk < lowestBlock {
				lowestBlock = blk
				targetOrder = o
			}
		}
	}

	// If no suitable block, grow allocator
	if targetOrder == -1 {
		needed := uint64(1 << order)
		growSize := uint64(DefaultGrowBlocks)
		if needed > growSize {
			growSize = needed
		}
		b.growLocked(growSize)
		for o := order; o <= MaxOrder; o++ {
			if len(b.freeLists[o]) > 0 {
				blk := b.freeLists[o][0]
				if blk < lowestBlock {
					lowestBlock = blk
					targetOrder = o
				}
			}
		}
	}

	// Lowest free block at targetOrder
	block := b.freeLists[targetOrder][0]
	b.removeFree(targetOrder, block)

	// Split down to requested order
	for o := targetOrder; o > order; o-- {
		buddy := block + uint64(1<<(o-1))
		b.insertFree(o-1, buddy)
	}

	b.allocated[block] = order
	return block, nil
}

func (b *BuddyAllocator) insertAndCoalesceFreeLocked(order int, block uint64) {
	currBlock := block
	currOrder := order
	for currOrder < MaxOrder {
		buddy := currBlock ^ uint64(1<<currOrder)
		// Check if buddy is free and aligned
		if b.removeFree(currOrder, buddy) {
			// Merge with buddy
			if buddy < currBlock {
				currBlock = buddy
			}
			currOrder++
		} else {
			break
		}
	}

	b.insertFree(currOrder, currBlock)
}

func (b *BuddyAllocator) returnFreeRangeLocked(start, count uint64) {
	curr := start
	end := start + count
	for curr < end {
		order := 0
		for order < MaxOrder {
			nextOrder := order + 1
			blockSize := uint64(1 << nextOrder)
			if curr+blockSize <= end && curr%blockSize == 0 {
				order = nextOrder
			} else {
				break
			}
		}
		b.insertAndCoalesceFreeLocked(order, curr)
		curr += uint64(1 << order)
	}
}

// Free releases previously allocated blocks starting at block and merges buddies.
func (b *BuddyAllocator) Free(block uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	order, exists := b.allocated[block]
	if !exists {
		return fmt.Errorf("block %d not allocated", block)
	}
	delete(b.allocated, block)

	b.insertAndCoalesceFreeLocked(order, block)
	return nil
}

// AllocateBatch reserves a contiguous extent of physical blocks for a batch of pages,
// recording each individual page allocation in the allocator.
// It sorts orders internally (largest first) so all pages are naturally aligned,
// and returns the allocated starting block for each page in the caller's original input order.
func (b *BuddyAllocator) AllocateBatch(orders []int) ([]uint64, error) {
	if len(orders) == 0 {
		return nil, nil
	}
	totalBlocks := 0
	for _, o := range orders {
		if o < MinOrder || o > MaxOrder {
			return nil, fmt.Errorf("invalid order %d in batch", o)
		}
		totalBlocks += 1 << o
	}
	if totalBlocks > (1 << MaxOrder) {
		return nil, fmt.Errorf("batch total blocks %d exceeds maximum extent size %d", totalBlocks, 1<<MaxOrder)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	return b.allocateBatchLocked(orders, totalBlocks)
}

func (b *BuddyAllocator) allocateBatchLocked(orders []int, totalBlocks int) ([]uint64, error) {
	minOrder := 0
	for (1 << minOrder) < totalBlocks {
		minOrder++
	}

	targetOrder := -1
	var lowestBlock uint64 = ^uint64(0)
	for o := minOrder; o <= MaxOrder; o++ {
		if len(b.freeLists[o]) > 0 {
			blk := b.freeLists[o][0]
			if blk < lowestBlock {
				lowestBlock = blk
				targetOrder = o
			}
		}
	}

	if targetOrder == -1 {
		b.growLocked(DefaultGrowBlocks)
		for o := minOrder; o <= MaxOrder; o++ {
			if len(b.freeLists[o]) > 0 {
				blk := b.freeLists[o][0]
				if blk < lowestBlock {
					lowestBlock = blk
					targetOrder = o
				}
			}
		}
	}

	startBlock := b.freeLists[targetOrder][0]
	b.removeFree(targetOrder, startBlock)
	extentSize := uint64(1 << targetOrder)
	if rem := extentSize - uint64(totalBlocks); rem > 0 {
		b.returnFreeRangeLocked(startBlock+uint64(totalBlocks), rem)
	}

	type batchItem struct {
		origIdx int
		order   int
	}
	items := make([]batchItem, len(orders))
	for i, o := range orders {
		items[i] = batchItem{origIdx: i, order: o}
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].order > items[j].order
	})

	locs := make([]uint64, len(orders))
	curr := startBlock
	for _, it := range items {
		locs[it.origIdx] = curr
		b.allocated[curr] = it.order
		curr += uint64(1 << it.order)
	}
	return locs, nil
}

// MarkAllocated marks specific blocks as allocated during crash recovery.
func (b *BuddyAllocator) MarkAllocated(block uint64, order int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	size := uint64(1 << order)
	end := block + size
	if end > b.totalBlocks {
		b.growLocked(end - b.totalBlocks)
	}

	// Remove from free lists if present
	// Need to split any higher order block covering this range
	b.removeRangeFromFreeLocked(block, order)

	b.allocated[block] = order
	return nil
}

func (b *BuddyAllocator) removeRangeFromFreeLocked(block uint64, order int) {
	// Check each order
	for o := order; o <= MaxOrder; o++ {
		blockSize := uint64(1 << o)
		alignedStart := (block / blockSize) * blockSize
		if b.removeFree(o, alignedStart) {
			// Split and reinsert parts not covering block
			b.splitAndReinsertLocked(alignedStart, o, block, order)
			return
		}
	}
}

func (b *BuddyAllocator) splitAndReinsertLocked(start uint64, fromOrder int, targetBlock uint64, targetOrder int) {
	if fromOrder == targetOrder {
		return
	}
	childOrder := fromOrder - 1
	left := start
	right := start + uint64(1<<childOrder)

	if targetBlock < right {
		// Target is in left child; right child is free
		b.insertFree(childOrder, right)
		b.splitAndReinsertLocked(left, childOrder, targetBlock, targetOrder)
	} else {
		// Target is in right child; left child is free
		b.insertFree(childOrder, left)
		b.splitAndReinsertLocked(right, childOrder, targetBlock, targetOrder)
	}
}

// TruncateTail truncates unused blocks at the tail of the heap file.
// Returns the new size of the file in bytes.
func (b *BuddyAllocator) TruncateTail(file File) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var highestBlock uint64
	for blk, order := range b.allocated {
		end := blk + uint64(1<<order)
		if end > highestBlock {
			highestBlock = end
		}
	}

	// Round up to multiple of 512 blocks (2 MiB)
	unit := uint64(1 << MaxOrder)
	newBlocks := ((highestBlock + unit - 1) / unit) * unit
	if newBlocks < unit {
		newBlocks = unit
	}

	if newBlocks >= b.totalBlocks {
		return int64(b.totalBlocks * BlockSize), nil
	}

	// Rebuild free lists removing blocks >= newBlocks
	for o := 0; o <= MaxOrder; o++ {
		var filtered []uint64
		for _, blk := range b.freeLists[o] {
			if blk < newBlocks {
				filtered = append(filtered, blk)
			}
		}
		b.freeLists[o] = filtered
	}

	b.totalBlocks = newBlocks
	newSizeBytes := int64(newBlocks * BlockSize)

	if file != nil {
		if err := file.Truncate(newSizeBytes); err != nil {
			return 0, fmt.Errorf("failed to truncate heap file to %d bytes: %w", newSizeBytes, err)
		}
	}

	return newSizeBytes, nil
}
