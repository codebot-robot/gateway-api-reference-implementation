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
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

var (
	// ErrReuseRuleViolation is returned when a write violates the reuse rule.
	ErrReuseRuleViolation = errors.New("reuse rule violation: attempt to overwrite newest durable copy of a page")

	// ErrWriteAheadRuleViolation is returned when a heap write violates the write-ahead rule.
	ErrWriteAheadRuleViolation = errors.New("write-ahead rule violation: page written before WAL fsynced through page LSN")
)

const (
	// DefaultBlockSize is 4 KiB.
	DefaultBlockSize = 4096

	// BlockMagicValue is "GARIDSTR" in big endian uint64.
	BlockMagicValue uint64 = 0x4741524944535452
)

// DurablePageRange tracks the physical block range and LSN of the newest durable copy of a logical page.
type DurablePageRange struct {
	StartBlock uint64
	NumBlocks  uint32
	LSN        uint64
}

// FaultFS wraps an FS to intercept writes, assert storage invariants, and simulate crashes.
type FaultFS struct {
	underlying FS
	mu         sync.RWMutex
	files      map[string]*FaultFile

	maxFsyncedLSN atomic.Uint64

	assertReuseRule      bool
	assertWriteAheadRule bool

	writeErr error
	syncErr  error
}

// NewFaultFS creates a new FaultFS wrapping an underlying FS.
func NewFaultFS(underlying FS) *FaultFS {
	return &FaultFS{
		underlying:           underlying,
		files:                make(map[string]*FaultFile),
		assertReuseRule:      true,
		assertWriteAheadRule: true,
	}
}

// SetMaxFsyncedLSN sets the highest LSN that has been fsynced in the WAL.
func (f *FaultFS) SetMaxFsyncedLSN(lsn uint64) {
	for {
		curr := f.maxFsyncedLSN.Load()
		if lsn <= curr || f.maxFsyncedLSN.CompareAndSwap(curr, lsn) {
			break
		}
	}
}

// MaxFsyncedLSN returns the highest LSN that has been fsynced in the WAL.
func (f *FaultFS) MaxFsyncedLSN() uint64 {
	return f.maxFsyncedLSN.Load()
}

// SetAssertReuseRule enables or disables the reuse rule assertion.
func (f *FaultFS) SetAssertReuseRule(enabled bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.assertReuseRule = enabled
}

// SetAssertWriteAheadRule enables or disables the write-ahead rule assertion.
func (f *FaultFS) SetAssertWriteAheadRule(enabled bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.assertWriteAheadRule = enabled
}

// SetWriteError sets an error to be returned on subsequent writes.
func (f *FaultFS) SetWriteError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeErr = err
}

// SetSyncError sets an error to be returned on subsequent syncs.
func (f *FaultFS) SetSyncError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncErr = err
}

func (f *FaultFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	cleaned := cleanPath(name)
	file, err := f.underlying.OpenFile(cleaned, flag, perm)
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	ff, exists := f.files[cleaned]
	if !exists {
		ff = newFaultFile(f, cleaned, file)
		f.files[cleaned] = ff
	} else {
		ff.underlying = file
	}
	return ff, nil
}

func (f *FaultFS) Create(name string) (File, error) {
	return f.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
}

func (f *FaultFS) Open(name string) (File, error) {
	return f.OpenFile(name, os.O_RDONLY, 0)
}

func (f *FaultFS) Remove(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.files, cleanPath(name))
	return f.underlying.Remove(name)
}

func (f *FaultFS) MkdirAll(path string, perm os.FileMode) error {
	return f.underlying.MkdirAll(path, perm)
}

func (f *FaultFS) ReadDir(dirname string) ([]os.DirEntry, error) {
	return f.underlying.ReadDir(dirname)
}

func (f *FaultFS) SyncDir(dirname string) error {
	return f.underlying.SyncDir(dirname)
}

// Crash simulates a crash across all files in this filesystem.
// If dropUnsynced is true, all un-fsynced writes are discarded.
// If tearFraction > 0, randomly tears un-fsynced blocks.
func (f *FaultFS) Crash(dropUnsynced bool, tearFraction float64) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, ff := range f.files {
		ff.Crash(dropUnsynced, tearFraction)
	}
}

// FaultFile intercepts reads, writes, and syncs on a specific file.
type FaultFile struct {
	fs         *FaultFS
	name       string
	underlying File
	mu         sync.RWMutex

	durableData   []byte
	pendingData   []byte
	pendingBlocks map[uint64]bool

	// Map of logicalPage -> newest durable range (heap files only)
	durablePages map[uint64]DurablePageRange

	isHeapFile bool
	isWALFile  bool
}

func newFaultFile(fs *FaultFS, name string, underlying File) *FaultFile {
	base := filepath.Base(name)
	isHeap := filepath.Ext(name) == ".db" || filepath.Ext(name) == ".heap" || base == "heap.db"
	isWAL := filepath.Ext(name) == ".wal"

	// Read initial contents from underlying file
	size, _ := underlying.Size()
	initial := make([]byte, size)
	if size > 0 {
		_, _ = underlying.ReadAt(initial, 0)
	}

	ff := &FaultFile{
		fs:            fs,
		name:          name,
		underlying:    underlying,
		durableData:   bytes.Clone(initial),
		pendingData:   bytes.Clone(initial),
		pendingBlocks: make(map[uint64]bool),
		durablePages:  make(map[uint64]DurablePageRange),
		isHeapFile:    isHeap,
		isWALFile:     isWAL,
	}

	if isHeap && len(initial) > 0 {
		ff.rebuildDurablePagesLocked()
	}
	return ff
}

func (f *FaultFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if off < 0 {
		return 0, errors.New("negative offset")
	}
	if off >= int64(len(f.pendingData)) {
		return 0, os.ErrNotExist
	}
	n := copy(p, f.pendingData[off:])
	return n, nil
}

func (f *FaultFile) WriteAt(p []byte, off int64) (int, error) {
	f.fs.mu.RLock()
	writeErr := f.fs.writeErr
	assertReuse := f.fs.assertReuseRule
	assertWAL := f.fs.assertWriteAheadRule
	maxFsyncedLSN := f.fs.MaxFsyncedLSN()
	f.fs.mu.RUnlock()

	if writeErr != nil {
		return 0, writeErr
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if off < 0 {
		return 0, errors.New("negative offset")
	}

	// If this is a heap file, assert Reuse Rule and Write-Ahead Rule
	if f.isHeapFile {
		startBlock := uint64(off / DefaultBlockSize)
		numBlocksInWrite := (len(p) + DefaultBlockSize - 1) / DefaultBlockSize

		for i := 0; i < numBlocksInWrite; i++ {
			bIdx := startBlock + uint64(i)
			blockOff := i * DefaultBlockSize
			blockEnd := blockOff + DefaultBlockSize
			if blockEnd > len(p) {
				blockEnd = len(p)
			}
			blockBytes := p[blockOff:blockEnd]

			// Parse block header if at least HeaderSize bytes
			if len(blockBytes) >= 64 {
				magic := binary.BigEndian.Uint64(blockBytes[0:8])
				if magic == BlockMagicValue {
					logicalPage := binary.BigEndian.Uint64(blockBytes[32:40])
					lsn := binary.BigEndian.Uint64(blockBytes[40:48])
					numBlocks := binary.BigEndian.Uint32(blockBytes[52:56])

					_ = logicalPage
					_ = numBlocks

					// Assert Write-Ahead Rule:
					// Before writing any page whose LSN is L, log must be fsynced through L.
					if assertWAL && lsn > maxFsyncedLSN {
						panic(fmt.Sprintf("%v: page LSN %d > max fsynced WAL LSN %d (block %d)",
							ErrWriteAheadRuleViolation, lsn, maxFsyncedLSN, bIdx))
					}
				}
			}

			// Assert Reuse Rule:
			// The location holding a page's newest durable (fsynced) copy must never be overwritten.
			if assertReuse {
				for pageID, durable := range f.durablePages {
					if bIdx >= durable.StartBlock && bIdx < durable.StartBlock+uint64(durable.NumBlocks) {
						panic(fmt.Sprintf("%v: writing to block %d which holds durable copy of page %d (LSN %d, blocks [%d..%d))",
							ErrReuseRuleViolation, bIdx, pageID, durable.LSN, durable.StartBlock, durable.StartBlock+uint64(durable.NumBlocks)))
					}
				}
			}

			f.pendingBlocks[bIdx] = true
		}
	}

	reqLen := int(off) + len(p)
	if reqLen > len(f.pendingData) {
		expanded := make([]byte, reqLen)
		copy(expanded, f.pendingData)
		f.pendingData = expanded
	}
	copy(f.pendingData[off:], p)

	// Write through to underlying
	_, err := f.underlying.WriteAt(p, off)
	if err != nil {
		return 0, err
	}

	return len(p), nil
}

func (f *FaultFile) Sync() error {
	f.fs.mu.RLock()
	syncErr := f.fs.syncErr
	f.fs.mu.RUnlock()

	if syncErr != nil {
		return syncErr
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Commit pending writes to durable state
	f.durableData = bytes.Clone(f.pendingData)
	f.pendingBlocks = make(map[uint64]bool)

	// Update durable pages index if heap file
	if f.isHeapFile {
		f.rebuildDurablePagesLocked()
	}

	// Update MaxFsyncedLSN if WAL file
	if f.isWALFile {
		f.updateMaxFsyncedWAL_LSNLocked()
	}

	return f.underlying.Sync()
}

func (f *FaultFile) rebuildDurablePagesLocked() {
	f.durablePages = make(map[uint64]DurablePageRange)
	totalBlocks := len(f.durableData) / DefaultBlockSize

	type candidatePage struct {
		startBlock  uint64
		logicalPage uint64
		lsn         uint64
		numBlocks   uint32
	}

	// Scan durable blocks for valid page copies
	b := 0
	for b < totalBlocks {
		blockStart := b * DefaultBlockSize
		blockBytes := f.durableData[blockStart : blockStart+DefaultBlockSize]
		magic := binary.BigEndian.Uint64(blockBytes[0:8])
		if magic != BlockMagicValue {
			b++
			continue
		}

		logicalPage := binary.BigEndian.Uint64(blockBytes[32:40])
		lsn := binary.BigEndian.Uint64(blockBytes[40:48])
		blockIndex := binary.BigEndian.Uint32(blockBytes[48:52])
		numBlocks := binary.BigEndian.Uint32(blockBytes[52:56])

		// Must be the first block of a page
		if blockIndex != 0 || numBlocks == 0 || b+int(numBlocks) > totalBlocks {
			b++
			continue
		}

		// Verify candidate page run
		valid := true
		for k := 0; k < int(numBlocks); k++ {
			kbStart := (b + k) * DefaultBlockSize
			kbBytes := f.durableData[kbStart : kbStart+DefaultBlockSize]

			kMagic := binary.BigEndian.Uint64(kbBytes[0:8])
			kLogical := binary.BigEndian.Uint64(kbBytes[32:40])
			kLSN := binary.BigEndian.Uint64(kbBytes[40:48])
			kIndex := binary.BigEndian.Uint32(kbBytes[48:52])
			kNum := binary.BigEndian.Uint32(kbBytes[52:56])
			kCRC := binary.BigEndian.Uint32(kbBytes[56:60])

			if kMagic != BlockMagicValue || kLogical != logicalPage || kLSN != lsn ||
				kIndex != uint32(k) || kNum != numBlocks {
				valid = false
				break
			}

			// Verify CRC
			calcBuf := make([]byte, DefaultBlockSize)
			copy(calcBuf, kbBytes)
			calcBuf[56] = 0
			calcBuf[57] = 0
			calcBuf[58] = 0
			calcBuf[59] = 0
			expectedCRC := crc32.Checksum(calcBuf, crc32.MakeTable(crc32.Castagnoli))
			if kCRC != expectedCRC {
				valid = false
				break
			}
		}

		if valid {
			cand := candidatePage{
				startBlock:  uint64(b),
				logicalPage: logicalPage,
				lsn:         lsn,
				numBlocks:   numBlocks,
			}
			existing, exists := f.durablePages[logicalPage]
			if !exists || cand.lsn > existing.LSN {
				f.durablePages[logicalPage] = DurablePageRange{
					StartBlock: cand.startBlock,
					NumBlocks:  cand.numBlocks,
					LSN:        cand.lsn,
				}
			}
			b += int(numBlocks)
		} else {
			b++
		}
	}
}

func (f *FaultFile) updateMaxFsyncedWAL_LSNLocked() {
	// Scan WAL records in durableData to find max LSN
	// WAL records: Header (24 bytes) at file start, then records.
	// Record: Type (1), LSN (8), RedoStart (8), KeyLen (4), ValLen (4), RVLen (4), CRC (4)
	if len(f.durableData) < 24 {
		return
	}
	offset := 24
	var maxLSN uint64
	for offset+33 <= len(f.durableData) {
		recLSN := binary.BigEndian.Uint64(f.durableData[offset+1 : offset+9])
		keyLen := binary.BigEndian.Uint32(f.durableData[offset+17 : offset+21])
		valLen := binary.BigEndian.Uint32(f.durableData[offset+21 : offset+25])
		rvLen := binary.BigEndian.Uint32(f.durableData[offset+25 : offset+29])

		totalLen := 33 + int(keyLen) + int(valLen) + int(rvLen)
		if offset+totalLen > len(f.durableData) {
			break
		}

		// Verify record CRC
		storedCRC := binary.BigEndian.Uint32(f.durableData[offset+29 : offset+33])
		headerCopy := make([]byte, 33)
		copy(headerCopy, f.durableData[offset:offset+33])
		headerCopy[29] = 0
		headerCopy[30] = 0
		headerCopy[31] = 0
		headerCopy[32] = 0

		h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
		_, _ = h.Write(headerCopy)
		_, _ = h.Write(f.durableData[offset+33 : offset+totalLen])
		if h.Sum32() != storedCRC {
			break
		}

		if recLSN > maxLSN {
			maxLSN = recLSN
		}
		offset += totalLen
	}

	if maxLSN > 0 {
		f.fs.SetMaxFsyncedLSN(maxLSN)
	}
}

// Crash simulates a crash on this file.
func (f *FaultFile) Crash(dropUnsynced bool, tearFraction float64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if dropUnsynced {
		// Discard pending writes; revert to durable data
		f.pendingData = bytes.Clone(f.durableData)
		f.pendingBlocks = make(map[uint64]bool)
		_ = f.underlying.Truncate(int64(len(f.durableData)))
		_, _ = f.underlying.WriteAt(f.durableData, 0)
		_ = f.underlying.Sync()
		return
	}

	if tearFraction > 0 && len(f.pendingBlocks) > 0 {
		// Partially write or tear random pending blocks
		for bIdx := range f.pendingBlocks {
			r := rand.Float64()
			if r < tearFraction {
				// Tear block: corrupt random bytes or write only half block
				start := int(bIdx) * DefaultBlockSize
				if start+DefaultBlockSize <= len(f.pendingData) {
					// Corrupt last 128 bytes of block
					for k := 0; k < 128; k++ {
						f.pendingData[start+DefaultBlockSize-1-k] ^= 0xff
					}
				}
			}
		}
	}

	// Write current crashed image to underlying and sync
	_ = f.underlying.Truncate(int64(len(f.pendingData)))
	_, _ = f.underlying.WriteAt(f.pendingData, 0)
	_ = f.underlying.Sync()
}

func (f *FaultFile) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if size < 0 {
		return errors.New("negative size")
	}
	if int(size) < len(f.pendingData) {
		f.pendingData = f.pendingData[:size]
	} else if int(size) > len(f.pendingData) {
		expanded := make([]byte, size)
		copy(expanded, f.pendingData)
		f.pendingData = expanded
	}
	return f.underlying.Truncate(size)
}

func (f *FaultFile) Close() error {
	return f.underlying.Close()
}

func (f *FaultFile) Size() (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return int64(len(f.pendingData)), nil
}

func (f *FaultFile) SyscallConn() (any, error) {
	return f.underlying.SyscallConn()
}
