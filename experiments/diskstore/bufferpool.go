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
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	// DefaultBufferPoolFrames is 1024 frames = 4 MiB.
	DefaultBufferPoolFrames = 1024
)

var (
	// ErrBufferPoolFull is returned when all frames are pinned and cannot be evicted.
	ErrBufferPoolFull = errors.New("buffer pool is full of pinned pages")

	// ErrPageNotFound indicates the page is neither in memory nor has a location on disk.
	ErrPageNotFound = errors.New("page not found")
)

// PageDesc holds metadata and frame mappings for a logical page in the buffer pool.
type PageDesc struct {
	LogicalPage uint64
	NumBlocks   int
	FrameIDs    []int // Non-adjacent frames in the arena

	PinCount atomic.Int32
	Dirty    bool
	DirtyLSN uint64 // ARIES: first LSN that dirtied page since last flush
	PageLSN  uint64 // latest LSN that modified this page

	PhysicalBlock uint64 // currently known disk location
	FlushedLoc    uint64 // location written since last heap fsync
	DurableLoc    uint64 // location confirmed durable by heap fsync

	Mu sync.RWMutex // Per-page latch for safe read/write access to frame data
}

// PageHandle is held by callers while accessing a pinned page.
type PageHandle struct {
	pool *BufferPool
	Desc *PageDesc
}

// Blocks returns byte slices for each block in this page.
func (h *PageHandle) Blocks() [][]byte {
	blocks := make([][]byte, len(h.Desc.FrameIDs))
	for i, fID := range h.Desc.FrameIDs {
		start := fID * BlockSize
		blocks[i] = h.pool.arena[start : start+BlockSize]
	}
	return blocks
}

// Block returns the byte slice for a specific block index.
func (h *PageHandle) Block(i int) []byte {
	fID := h.Desc.FrameIDs[i]
	start := fID * BlockSize
	return h.pool.arena[start : start+BlockSize]
}

// Unpin releases the caller's pin on the page.
func (h *PageHandle) Unpin() {
	h.Desc.PinCount.Add(-1)
}

// BufferPool manages a fixed-size arena of 4 KiB frames with direct I/O, out-of-place writes,
// and write-ahead rule enforcement.
type BufferPool struct {
	file     File
	wal      *WAL
	buddy    *BuddyAllocator
	directIO DirectIO
	dbID     [16]byte
	mu       sync.Mutex

	numFrames  int
	arena      []byte // Pointer-free byte arena aligned to 4096 bytes
	freeFrames []int

	pageTable map[uint64]*PageDesc // logicalPage -> PageDesc

	bgStop chan struct{}
	bgDone chan struct{}
}

// BufferPoolOptions configures the BufferPool.
type BufferPoolOptions struct {
	NumFrames int
}

// NewBufferPool initializes a fixed-size buffer pool backed by a single pointer-free byte arena.
func NewBufferPool(file File, wal *WAL, buddy *BuddyAllocator, dbID [16]byte, opts ...BufferPoolOptions) *BufferPool {
	numFrames := DefaultBufferPoolFrames
	if len(opts) > 0 && opts[0].NumFrames > 0 {
		numFrames = opts[0].NumFrames
	}

	// Allocate pointer-free byte arena aligned to 4096 bytes without mmap
	totalBytes := numFrames * BlockSize
	raw := make([]byte, totalBytes+BlockSize)
	rawPtr := uintptr(unsafe.Pointer(&raw[0]))
	alignedPtr := (rawPtr + uintptr(BlockSize-1)) &^ uintptr(BlockSize-1)
	offset := int(alignedPtr - rawPtr)
	arena := raw[offset : offset+totalBytes]

	freeFrames := make([]int, numFrames)
	for i := 0; i < numFrames; i++ {
		freeFrames[i] = i
	}

	bp := &BufferPool{
		file:       file,
		wal:        wal,
		buddy:      buddy,
		directIO:   NewDirectIO(),
		dbID:       dbID,
		numFrames:  numFrames,
		arena:      arena,
		freeFrames: freeFrames,
		pageTable:  make(map[uint64]*PageDesc),
		bgStop:     make(chan struct{}),
		bgDone:     make(chan struct{}),
	}

	go bp.backgroundWriter()
	return bp
}

func (bp *BufferPool) backgroundWriter() {
	defer close(bp.bgDone)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-bp.bgStop:
			return
		case <-ticker.C:
			bp.flushCleanCandidates()
		}
	}
}

func (bp *BufferPool) flushCleanCandidates() {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for _, desc := range bp.pageTable {
		if desc.Dirty && desc.PinCount.Load() == 0 {
			_ = bp.flushPageLocked(desc)
		}
	}
}

// PinPage pins an existing page in the buffer pool, loading it from disk if necessary.
func (bp *BufferPool) PinPage(logicalPage uint64) (*PageHandle, error) {
	bp.mu.Lock()
	desc, exists := bp.pageTable[logicalPage]
	if exists {
		desc.PinCount.Add(1)
		bp.mu.Unlock()
		return &PageHandle{pool: bp, Desc: desc}, nil
	}

	// Page is not in memory; cannot load without known physical location or numBlocks
	bp.mu.Unlock()
	return nil, ErrPageNotFound
}

// PinPageWithLocation pins a page, loading from disk at physicalBlock if not currently in memory.
func (bp *BufferPool) PinPageWithLocation(logicalPage uint64, physicalBlock uint64, numBlocks int) (*PageHandle, error) {
	bp.mu.Lock()
	desc, exists := bp.pageTable[logicalPage]
	if exists {
		desc.PinCount.Add(1)
		bp.mu.Unlock()
		return &PageHandle{pool: bp, Desc: desc}, nil
	}

	// Allocate frames for loading page
	frameIDs, err := bp.acquireFramesLocked(numBlocks)
	if err != nil {
		bp.mu.Unlock()
		return nil, err
	}

	desc = &PageDesc{
		LogicalPage:   logicalPage,
		NumBlocks:     numBlocks,
		FrameIDs:      frameIDs,
		PhysicalBlock: physicalBlock,
		DurableLoc:    physicalBlock,
	}
	desc.PinCount.Store(1)

	// Prepare iovec and read from disk
	iov := make([][]byte, numBlocks)
	for i, fID := range frameIDs {
		start := fID * BlockSize
		iov[i] = bp.arena[start : start+BlockSize]
	}

	if err := bp.directIO.Readv(bp.file, int64(physicalBlock*BlockSize), iov); err != nil {
		// Release frames on error
		bp.freeFrames = append(bp.freeFrames, frameIDs...)
		bp.mu.Unlock()
		return nil, fmt.Errorf("failed to read page %d from disk block %d: %w", logicalPage, physicalBlock, err)
	}

	// Verify block headers
	for i := 0; i < numBlocks; i++ {
		hdr, err := DecodeBlockHeader(iov[i], bp.dbID)
		if err != nil {
			bp.freeFrames = append(bp.freeFrames, frameIDs...)
			bp.mu.Unlock()
			return nil, fmt.Errorf("corrupt block %d in page %d: %w", i, logicalPage, err)
		}
		if hdr.LogicalPage != logicalPage || hdr.BlockIndex != uint32(i) {
			bp.freeFrames = append(bp.freeFrames, frameIDs...)
			bp.mu.Unlock()
			return nil, fmt.Errorf("block header mismatch in page %d", logicalPage)
		}
		desc.PageLSN = hdr.LSN
	}

	bp.pageTable[logicalPage] = desc
	bp.mu.Unlock()

	return &PageHandle{pool: bp, Desc: desc}, nil
}

// AllocateNewPage creates a new page in the buffer pool with freshly acquired frames.
func (bp *BufferPool) AllocateNewPage(logicalPage uint64, numBlocks int, lsn uint64) (*PageHandle, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	frameIDs, err := bp.acquireFramesLocked(numBlocks)
	if err != nil {
		return nil, err
	}

	desc := &PageDesc{
		LogicalPage: logicalPage,
		NumBlocks:   numBlocks,
		FrameIDs:    frameIDs,
		Dirty:       true,
		DirtyLSN:    lsn,
		PageLSN:     lsn,
	}
	desc.PinCount.Store(1)

	// Clear frames
	for i, fID := range frameIDs {
		start := fID * BlockSize
		blk := bp.arena[start : start+BlockSize]
		clear(blk)
		hdr := BlockHeader{
			DBID:        bp.dbID,
			LogicalPage: logicalPage,
			LSN:         lsn,
			BlockIndex:  uint32(i),
			NumBlocks:   uint32(numBlocks),
		}
		EncodeBlockHeader(hdr, blk)
	}

	bp.pageTable[logicalPage] = desc
	return &PageHandle{pool: bp, Desc: desc}, nil
}

func clear(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

func (bp *BufferPool) acquireFramesLocked(count int) ([]int, error) {
	for len(bp.freeFrames) < count {
		// Evict an unpinned page
		evicted := false
		for pageID, desc := range bp.pageTable {
			if desc.PinCount.Load() == 0 {
				if desc.Dirty {
					if err := bp.flushPageLocked(desc); err != nil {
						return nil, err
					}
				}
				// Reclaim frames
				bp.freeFrames = append(bp.freeFrames, desc.FrameIDs...)
				delete(bp.pageTable, pageID)
				evicted = true
				break
			}
		}
		if !evicted {
			return nil, ErrBufferPoolFull
		}
	}

	allocated := make([]int, count)
	for i := 0; i < count; i++ {
		allocated[i] = bp.freeFrames[len(bp.freeFrames)-1]
		bp.freeFrames = bp.freeFrames[:len(bp.freeFrames)-1]
	}
	return allocated, nil
}

// MarkDirty flags a page as modified with the given LSN.
func (bp *BufferPool) MarkDirty(logicalPage uint64, lsn uint64) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if desc, exists := bp.pageTable[logicalPage]; exists {
		if !desc.Dirty {
			desc.Dirty = true
			desc.DirtyLSN = lsn
		}
		desc.PageLSN = lsn
	}
}

// FlushPage flushes a dirty page to disk under the write-ahead rule and out-of-place write rule.
func (bp *BufferPool) FlushPage(logicalPage uint64) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	desc, exists := bp.pageTable[logicalPage]
	if !exists || !desc.Dirty {
		return nil
	}
	return bp.flushPageLocked(desc)
}

func (bp *BufferPool) flushPageLocked(desc *PageDesc) error {
	if !desc.Dirty {
		return nil
	}

	// Write-ahead rule: before writing any page whose LSN is L, log must be fsynced through L.
	if bp.wal != nil {
		if err := bp.wal.SyncThrough(desc.PageLSN); err != nil {
			return fmt.Errorf("write-ahead sync failed: %w", err)
		}
	}

	// Write elsewhere / Reuse rule:
	// Allocate new location from buddy allocator.
	// The location holding the page's newest durable (fsynced) copy must never be overwritten.
	order := 0
	for (1 << order) < desc.NumBlocks {
		order++
	}

	newLoc, err := bp.buddy.Allocate(order)
	if err != nil {
		return fmt.Errorf("failed to allocate disk block for page %d: %w", desc.LogicalPage, err)
	}

	// If desc was already written to a location since last heap fsync, that un-fsynced location can be freed immediately
	if desc.FlushedLoc != 0 && desc.FlushedLoc != desc.DurableLoc {
		_ = bp.buddy.Free(desc.FlushedLoc)
	}
	desc.FlushedLoc = newLoc
	desc.PhysicalBlock = newLoc

	// Finalize block headers and CRCs
	iov := make([][]byte, desc.NumBlocks)
	for i, fID := range desc.FrameIDs {
		start := fID * BlockSize
		blk := bp.arena[start : start+BlockSize]
		// Update header LSN and DBID
		hdr := BlockHeader{
			DBID:        bp.dbID,
			LogicalPage: desc.LogicalPage,
			LSN:         desc.PageLSN,
			BlockIndex:  uint32(i),
			NumBlocks:   uint32(desc.NumBlocks),
		}
		EncodeBlockHeader(hdr, blk)
		FinalizeBlockCRC(blk)
		iov[i] = blk
	}

	// Write blocks using direct I/O
	if err := bp.directIO.Writev(bp.file, int64(newLoc*BlockSize), iov); err != nil {
		return fmt.Errorf("failed to write page %d to block %d: %w", desc.LogicalPage, newLoc, err)
	}

	desc.Dirty = false
	return nil
}

// FlushAllDirty flushes all dirty pages currently in the buffer pool.
func (bp *BufferPool) FlushAllDirty() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for _, desc := range bp.pageTable {
		if desc.Dirty {
			if err := bp.flushPageLocked(desc); err != nil {
				return err
			}
		}
	}
	return nil
}

// PromoteFlushedToDurable promotes flushed locations to durable locations after a heap fsync.
// Frees any superseded durable locations to the buddy allocator according to the Reuse Rule.
func (bp *BufferPool) PromoteFlushedToDurable() {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for _, desc := range bp.pageTable {
		if desc.FlushedLoc != 0 {
			if desc.DurableLoc != 0 && desc.DurableLoc != desc.FlushedLoc {
				// Old durable copy is now superseded and can be safely reused
				_ = bp.buddy.Free(desc.DurableLoc)
			}
			desc.DurableLoc = desc.FlushedLoc
			desc.FlushedLoc = 0
		}
	}
}

// DirtyPageTable returns a map of logicalPage -> dirtyLSN for all currently dirty pages.
func (bp *BufferPool) DirtyPageTable() map[uint64]uint64 {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	dpt := make(map[uint64]uint64)
	for pID, desc := range bp.pageTable {
		if desc.Dirty && desc.DirtyLSN > 0 {
			dpt[pID] = desc.DirtyLSN
		}
	}
	return dpt
}

// Close stops the background writer and flushes dirty pages.
func (bp *BufferPool) Close() error {
	close(bp.bgStop)
	<-bp.bgDone

	return bp.FlushAllDirty()
}
