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

	// NoBlock is a sentinel representing an unassigned block location.
	NoBlock uint64 = ^uint64(0)
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

	PinCount  atomic.Int32
	Dirty     bool
	DirtyLSN  uint64        // ARIES: first LSN that dirtied page since last flush
	PageLSN   atomic.Uint64 // latest LSN that modified this page
	DirtyTime time.Time     // Timestamp when page became dirty

	PhysicalBlock     uint64 // currently known disk location
	FlushedLoc        uint64 // location written since last heap fsync
	FlushedBeforeSync uint64 // location written before current heap fsync started
	DurableLoc        uint64 // location confirmed durable by heap fsync

	InFlight bool // true when a direct I/O flush is active for this page

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
// Lock hierarchy: bp.mu (pool-wide mutex) -> desc.Mu (per-page latch). Never acquire bp.mu while holding desc.Mu.
type BufferPool struct {
	file      File
	wal       *WAL
	buddy     *BuddyAllocator
	directIO  DirectIO
	cipher    Cipher
	dbID      [16]byte
	mu        sync.Mutex
	onFailure func(error)

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
	Cipher    Cipher
	OnFailure func(error)
}

// NewBufferPool initializes a fixed-size buffer pool backed by a single pointer-free byte arena.
func NewBufferPool(file File, wal *WAL, buddy *BuddyAllocator, dbID [16]byte, opts ...BufferPoolOptions) *BufferPool {
	numFrames := DefaultBufferPoolFrames
	var cipher Cipher
	var onFailure func(error)
	if len(opts) > 0 {
		if opts[0].NumFrames > 0 {
			numFrames = opts[0].NumFrames
		}
		cipher = opts[0].Cipher
		onFailure = opts[0].OnFailure
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
		cipher:     cipher,
		dbID:       dbID,
		numFrames:  numFrames,
		arena:      arena,
		freeFrames: freeFrames,
		pageTable:  make(map[uint64]*PageDesc),
		onFailure:  onFailure,
		bgStop:     make(chan struct{}),
		bgDone:     make(chan struct{}),
	}

	go bp.backgroundIdleSweep()
	return bp
}

func (bp *BufferPool) SetOnFailure(fn func(error)) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.onFailure = fn
}

func (bp *BufferPool) fail(err error) {
	if bp.onFailure != nil {
		bp.onFailure(err)
	}
}

// backgroundIdleSweep sweeps pages that have been dirty and idle for > 1 minute.
// Runs every 10 seconds (no 20ms busy flushing).
func (bp *BufferPool) backgroundIdleSweep() {
	defer close(bp.bgDone)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-bp.bgStop:
			return
		case <-ticker.C:
			bp.sweepIdleDirtyPages()
		}
	}
}

func (bp *BufferPool) sweepIdleDirtyPages() {
	bp.mu.Lock()
	now := time.Now()
	var toFlush []*PageDesc
	for _, desc := range bp.pageTable {
		if desc.Dirty && desc.PinCount.Load() == 0 && !desc.InFlight && now.Sub(desc.DirtyTime) >= time.Minute {
			toFlush = append(toFlush, desc)
		}
	}
	bp.mu.Unlock()

	for _, desc := range toFlush {
		if err := bp.flushPage(desc); err != nil {
			bp.fail(fmt.Errorf("background idle sweep failed: %w", err))
			return
		}
	}
}

// PinPage pins an existing page in the buffer pool, loading it from disk if necessary.
func (bp *BufferPool) PinPage(logicalPage uint64) (*PageHandle, error) {
	bp.mu.Lock()
	desc, exists := bp.pageTable[logicalPage]
	if !exists {
		bp.mu.Unlock()
		return nil, ErrPageNotFound
	}

	if len(desc.FrameIDs) > 0 {
		desc.PinCount.Add(1)
		bp.mu.Unlock()
		return &PageHandle{pool: bp, Desc: desc}, nil
	}

	// Page was evicted from memory frames; reload from disk
	readBlock := desc.PhysicalBlock
	if readBlock == NoBlock {
		readBlock = desc.DurableLoc
	}
	if readBlock == NoBlock {
		bp.mu.Unlock()
		return nil, ErrPageNotFound
	}

	frameIDs, err := bp.acquireFramesLocked(desc.NumBlocks)
	if err != nil {
		bp.mu.Unlock()
		return nil, err
	}

	desc.FrameIDs = frameIDs
	desc.PinCount.Store(1)

	iov := make([][]byte, desc.NumBlocks)
	for i, fID := range frameIDs {
		start := fID * BlockSize
		iov[i] = bp.arena[start : start+BlockSize]
	}

	if err := bp.directIO.Readv(bp.file, int64(readBlock*BlockSize), iov); err != nil {
		bp.freeFrames = append(bp.freeFrames, frameIDs...)
		desc.FrameIDs = nil
		desc.PinCount.Store(0)
		bp.mu.Unlock()
		return nil, fmt.Errorf("failed to read page %d from disk block %d: %w", logicalPage, readBlock, err)
	}

	// Verify block headers
	for i := 0; i < desc.NumBlocks; i++ {
		hdr, err := DecodeBlockHeader(iov[i])
		if err != nil || hdr.LogicalPage != logicalPage || hdr.BlockIndex != uint32(i) {
			bp.freeFrames = append(bp.freeFrames, frameIDs...)
			desc.FrameIDs = nil
			desc.PinCount.Store(0)
			bp.mu.Unlock()
			return nil, fmt.Errorf("corrupt block in page %d", logicalPage)
		}
		desc.PageLSN.Store(hdr.LSN)
	}

	bp.mu.Unlock()
	return &PageHandle{pool: bp, Desc: desc}, nil
}

// LoadPageDescs registers recovered page descriptors into the page table.
func (bp *BufferPool) LoadPageDescs(descs map[uint64]*PageDesc) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	for k, v := range descs {
		bp.pageTable[k] = v
	}
}

// PinPageWithLocation pins a page, loading from disk at physicalBlock if not currently in memory.
func (bp *BufferPool) PinPageWithLocation(logicalPage uint64, physicalBlock uint64, numBlocks int) (*PageHandle, error) {
	bp.mu.Lock()
	desc, exists := bp.pageTable[logicalPage]
	if !exists {
		desc = &PageDesc{
			LogicalPage:   logicalPage,
			NumBlocks:     numBlocks,
			PhysicalBlock: physicalBlock,
			DurableLoc:    physicalBlock,
		}
		bp.pageTable[logicalPage] = desc
	}
	bp.mu.Unlock()
	return bp.PinPage(logicalPage)
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
		LogicalPage:   logicalPage,
		NumBlocks:     numBlocks,
		FrameIDs:      frameIDs,
		Dirty:         true,
		DirtyLSN:      lsn,
		DirtyTime:     time.Now(),
		PhysicalBlock: NoBlock,
		FlushedLoc:    NoBlock,
		DurableLoc:    NoBlock,
	}
	desc.PageLSN.Store(lsn)
	desc.PinCount.Store(1)

	// Clear frames and encode initial block headers
	for i, fID := range frameIDs {
		start := fID * BlockSize
		blk := bp.arena[start : start+BlockSize]
		clearBytes(blk)
		hdr := BlockHeader{
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

// acquireFramesLocked acquires count frames, evicting clean pages first, then flushing dirty pages.
func (bp *BufferPool) acquireFramesLocked(count int) ([]int, error) {
	for len(bp.freeFrames) < count {
		// Pass 1: Evict clean, unpinned pages first (zero disk I/O)
		evictedClean := false
		for _, desc := range bp.pageTable {
			if len(desc.FrameIDs) > 0 && desc.PinCount.Load() == 0 && !desc.Dirty && !desc.InFlight {
				bp.freeFrames = append(bp.freeFrames, desc.FrameIDs...)
				desc.FrameIDs = nil
				evictedClean = true
				break
			}
		}
		if evictedClean {
			continue
		}

		// Pass 2: Evict unpinned dirty page
		evictedDirty := false
		for _, desc := range bp.pageTable {
			if len(desc.FrameIDs) > 0 && desc.PinCount.Load() == 0 && desc.Dirty && !desc.InFlight {
				if err := bp.flushPageLocked(desc); err != nil {
					return nil, err
				}
				bp.freeFrames = append(bp.freeFrames, desc.FrameIDs...)
				desc.FrameIDs = nil
				evictedDirty = true
				break
			}
		}
		if !evictedDirty {
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
// Lock hierarchy: called outside desc.Mu or after desc.Mu is released.
func (bp *BufferPool) MarkDirty(logicalPage uint64, lsn uint64) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if desc, exists := bp.pageTable[logicalPage]; exists {
		if !desc.Dirty {
			desc.Dirty = true
			desc.DirtyLSN = lsn
			desc.DirtyTime = time.Now()
		}
		desc.PageLSN.Store(lsn)
	}
}

// FlushPage flushes a dirty page to disk under the write-ahead rule and out-of-place write rule.
func (bp *BufferPool) FlushPage(logicalPage uint64) error {
	bp.mu.Lock()
	desc, exists := bp.pageTable[logicalPage]
	if !exists || !desc.Dirty {
		bp.mu.Unlock()
		return nil
	}
	bp.mu.Unlock()
	return bp.flushPage(desc)
}

func (bp *BufferPool) flushPage(desc *PageDesc) error {
	bp.mu.Lock()
	if !desc.Dirty || desc.InFlight {
		bp.mu.Unlock()
		return nil
	}
	err := bp.flushPageLocked(desc)
	bp.mu.Unlock()
	return err
}

func (bp *BufferPool) flushPageLocked(desc *PageDesc) error {
	if !desc.Dirty {
		return nil
	}

	pageLSN := desc.PageLSN.Load()

	// Write-ahead rule: before writing any page whose LSN is L, log must be fsynced through L.
	if bp.wal != nil {
		if err := bp.wal.SyncThrough(pageLSN); err != nil {
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
	if desc.FlushedLoc != NoBlock && desc.FlushedLoc != desc.DurableLoc && desc.FlushedLoc != desc.FlushedBeforeSync {
		if err := bp.buddy.Free(desc.FlushedLoc); err != nil {
			return fmt.Errorf("failed to free un-fsynced block %d: %w", desc.FlushedLoc, err)
		}
	}
	desc.FlushedLoc = newLoc
	desc.PhysicalBlock = newLoc

	desc.Mu.Lock()
	// Finalize block headers and CRCs
	iov := make([][]byte, desc.NumBlocks)
	for i, fID := range desc.FrameIDs {
		start := fID * BlockSize
		blk := bp.arena[start : start+BlockSize]
		hdr := BlockHeader{
			LogicalPage: desc.LogicalPage,
			LSN:         pageLSN,
			BlockIndex:  uint32(i),
			NumBlocks:   uint32(desc.NumBlocks),
		}
		EncodeBlockHeader(hdr, blk)
		FinalizeBlockCRC(blk)
		iov[i] = blk
	}
	desc.Mu.Unlock()

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
	var toFlush []*PageDesc
	for _, desc := range bp.pageTable {
		if desc.Dirty && len(desc.FrameIDs) > 0 {
			toFlush = append(toFlush, desc)
		}
	}
	bp.mu.Unlock()

	for _, desc := range toFlush {
		if err := bp.flushPage(desc); err != nil {
			return err
		}
	}
	return nil
}

// SnapshotFlushedForSync records all currently flushed locations before a heap fsync starts.
// This fulfills the Reuse Rule: only locations written before the fsync began may be promoted to durable.
func (bp *BufferPool) SnapshotFlushedForSync() {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for _, desc := range bp.pageTable {
		if desc.FlushedLoc != NoBlock {
			desc.FlushedBeforeSync = desc.FlushedLoc
		}
	}
}

// PromoteFlushedToDurable promotes only locations that were flushed before the heap fsync began.
// Frees any superseded durable locations to the buddy allocator according to the Reuse Rule.
func (bp *BufferPool) PromoteFlushedToDurable() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	var errs []error
	for _, desc := range bp.pageTable {
		if desc.FlushedBeforeSync != NoBlock {
			if desc.DurableLoc != NoBlock && desc.DurableLoc != desc.FlushedBeforeSync {
				// Old durable copy is now superseded and can be safely reused
				if err := bp.buddy.Free(desc.DurableLoc); err != nil {
					errs = append(errs, err)
				}
			}
			desc.DurableLoc = desc.FlushedBeforeSync
			if desc.FlushedLoc == desc.FlushedBeforeSync {
				desc.FlushedLoc = NoBlock
			}
			desc.FlushedBeforeSync = NoBlock
		}
	}
	return errors.Join(errs...)
}

// DirtyPageTableAndLSN returns a map of logicalPage -> dirtyLSN and the current max LSN at snapshot time.
func (bp *BufferPool) DirtyPageTableAndLSN() (map[uint64]uint64, uint64) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	dpt := make(map[uint64]uint64)
	for pID, desc := range bp.pageTable {
		if desc.Dirty && desc.DirtyLSN > 0 {
			dpt[pID] = desc.DirtyLSN
		}
	}
	maxLSN := uint64(0)
	if bp.wal != nil {
		maxLSN = bp.wal.MaxLSN()
	}
	return dpt, maxLSN
}

// Close stops the background writer and flushes dirty pages.
func (bp *BufferPool) Close() error {
	close(bp.bgStop)
	<-bp.bgDone

	return bp.FlushAllDirty()
}
