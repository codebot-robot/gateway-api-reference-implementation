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
	"io"
)

// KeyIndexEntry maps an object key to its location in a logical page.
type KeyIndexEntry struct {
	LogicalPage uint64
	Slot        uint32
	LSN         uint64
	IsOversize  bool
}

// RecoveryResult contains the state reconstructed after a crash scan and log replay.
type RecoveryResult struct {
	DBID                [16]byte
	PageDescs           map[uint64]*PageDesc
	Buddy               *BuddyAllocator
	KeyIndex            map[string]KeyIndexEntry
	EmptyLogicalPages   []uint64
	LastResourceVersion string
	MaxLSN              uint64
	NextLogicalPage     uint64
}

type candidatePageCopy struct {
	logicalPage uint64
	lsn         uint64
	startBlock  uint64
	numBlocks   int
}

// Recover scans the heap file, filters out foreign and torn copies, replays the WAL from
// the last checkpoint's redo start, and rebuilds the in-memory indexes and allocator state.
func Recover(fs FS, heapFile File, walDir string, dbID [16]byte) (*RecoveryResult, error) {
	size, err := heapFile.Size()
	if err != nil {
		return nil, fmt.Errorf("failed to get heap file size: %w", err)
	}

	totalBlocks := uint64(size / BlockSize)
	buddy := NewBuddyAllocator(totalBlocks)

	// Step 1 & 2: Scan physical blocks in heap file to find valid candidate page copies
	bestCandidates := make(map[uint64]candidatePageCopy) // logicalPage -> candidatePageCopy

	b := uint64(0)
	for b < totalBlocks {
		blockBuf := make([]byte, BlockSize)
		_, err := heapFile.ReadAt(blockBuf, int64(b*BlockSize))
		if err != nil {
			break
		}

		hdr, err := DecodeBlockHeader(blockBuf, dbID)
		if err != nil || hdr.BlockIndex != 0 || hdr.NumBlocks == 0 {
			b++
			continue
		}

		numBlocks := int(hdr.NumBlocks)
		if b%uint64(numBlocks) != 0 || b+uint64(numBlocks) > totalBlocks {
			b++
			continue
		}

		// Verify candidate page run
		valid := true
		for k := 0; k < numBlocks; k++ {
			kbBuf := make([]byte, BlockSize)
			_, err := heapFile.ReadAt(kbBuf, int64((b+uint64(k))*BlockSize))
			if err != nil {
				valid = false
				break
			}

			kHdr, err := DecodeBlockHeader(kbBuf, dbID)
			if err != nil || kHdr.LogicalPage != hdr.LogicalPage || kHdr.LSN != hdr.LSN ||
				kHdr.BlockIndex != uint32(k) || kHdr.NumBlocks != hdr.NumBlocks {
				valid = false
				break
			}
		}

		if valid {
			cand := candidatePageCopy{
				logicalPage: hdr.LogicalPage,
				lsn:         hdr.LSN,
				startBlock:  b,
				numBlocks:   numBlocks,
			}
			existing, exists := bestCandidates[hdr.LogicalPage]
			if !exists || cand.lsn > existing.lsn {
				bestCandidates[hdr.LogicalPage] = cand
			}
			b += uint64(numBlocks)
		} else {
			b++
		}
	}

	// Step 3: Mark blocks of winning candidates as allocated in the buddy allocator
	pageDescs := make(map[uint64]*PageDesc)
	var maxRecoveredLogicalPage uint64

	for logicalPage, cand := range bestCandidates {
		order := 0
		for (1 << order) < cand.numBlocks {
			order++
		}
		if err := buddy.MarkAllocated(cand.startBlock, order); err != nil {
			return nil, fmt.Errorf("failed to mark block %d allocated during recovery: %w", cand.startBlock, err)
		}

		desc := &PageDesc{
			LogicalPage:   logicalPage,
			NumBlocks:     cand.numBlocks,
			PhysicalBlock: cand.startBlock,
			DurableLoc:    cand.startBlock,
			PageLSN:       cand.lsn,
		}
		pageDescs[logicalPage] = desc

		if logicalPage > maxRecoveredLogicalPage {
			maxRecoveredLogicalPage = logicalPage
		}
	}

	// Step 4: Scan recovered pages to rebuild key index and empty page list
	keyIndex := make(map[string]KeyIndexEntry)
	var emptyPages []uint64

	for logicalPage, cand := range bestCandidates {
		blocks := make([][]byte, cand.numBlocks)
		for i := 0; i < cand.numBlocks; i++ {
			blocks[i] = make([]byte, BlockSize)
			_, _ = heapFile.ReadAt(blocks[i], int64((cand.startBlock+uint64(i))*BlockSize))
		}

		if cand.numBlocks == 1 {
			slotted, err := WrapSlottedPage(blocks[0])
			if err == nil {
				if slotted.ActiveSlotsCount() == 0 {
					emptyPages = append(emptyPages, logicalPage)
				} else {
					for s := uint32(0); s < uint32(slotted.NumSlots()); s++ {
						k, _, recLSN, err := slotted.Get(s)
						if err == nil {
							keyIndex[k] = KeyIndexEntry{
								LogicalPage: logicalPage,
								Slot:        s,
								LSN:         recLSN,
							}
						}
					}
				}
			}
		} else {
			k, _, recLSN, err := DecodeMultiBlockPage(blocks)
			if err == nil {
				keyIndex[k] = KeyIndexEntry{
					LogicalPage: logicalPage,
					Slot:        0,
					LSN:         recLSN,
				}
			}
		}
	}

	// Step 5: Scan WAL to find the last checkpoint's redo start LSN
	var redoStartLSN uint64 = 1
	var lastResourceVersion string
	var maxWAL_LSN uint64

	walScanIt, err := NewWALIterator(fs, walDir, dbID, 1)
	if err == nil {
		for {
			rec, err := walScanIt.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				break
			}
			if rec.LSN > maxWAL_LSN {
				maxWAL_LSN = rec.LSN
			}
			if rec.ResourceVersion != "" {
				lastResourceVersion = rec.ResourceVersion
			}
			if rec.Type == RecordTypeCheckpoint && rec.RedoStartLSN > 0 {
				redoStartLSN = rec.RedoStartLSN
			}
		}
		_ = walScanIt.Close()
	}

	// Step 6: Replay WAL from redoStartLSN
	walRedoIt, err := NewWALIterator(fs, walDir, dbID, redoStartLSN)
	if err == nil {
		for {
			rec, err := walRedoIt.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				break
			}
			if rec.LSN > maxWAL_LSN {
				maxWAL_LSN = rec.LSN
			}
			if rec.ResourceVersion != "" {
				lastResourceVersion = rec.ResourceVersion
			}

			// Apply Put
			if rec.Type == RecordTypePut {
				existing, inIndex := keyIndex[rec.Key]
				if inIndex {
					desc, inPages := pageDescs[existing.LogicalPage]
					if inPages && desc.PageLSN >= rec.LSN {
						continue // Target page already reflects this or newer write
					}
					// Update key index entry with new LSN
					existing.LSN = rec.LSN
					keyIndex[rec.Key] = existing
				} else {
					// Put new key: assign logical page
					var targetPage uint64
					if len(emptyPages) > 0 {
						targetPage = emptyPages[len(emptyPages)-1]
						emptyPages = emptyPages[:len(emptyPages)-1]
					} else {
						maxRecoveredLogicalPage++
						targetPage = maxRecoveredLogicalPage
					}

					keyIndex[rec.Key] = KeyIndexEntry{
						LogicalPage: targetPage,
						Slot:        0,
						LSN:         rec.LSN,
					}
					pageDescs[targetPage] = &PageDesc{
						LogicalPage: targetPage,
						NumBlocks:   1,
						PageLSN:     rec.LSN,
					}
				}
			}

			// Apply Delete
			if rec.Type == RecordTypeDelete {
				existing, inIndex := keyIndex[rec.Key]
				if inIndex {
					desc, inPages := pageDescs[existing.LogicalPage]
					if inPages && desc.PageLSN >= rec.LSN {
						continue // Already durable
					}
					delete(keyIndex, rec.Key)
				}
			}
		}
		_ = walRedoIt.Close()
	}

	return &RecoveryResult{
		DBID:                dbID,
		PageDescs:           pageDescs,
		Buddy:               buddy,
		KeyIndex:            keyIndex,
		EmptyLogicalPages:   emptyPages,
		LastResourceVersion: lastResourceVersion,
		MaxLSN:              maxWAL_LSN,
		NextLogicalPage:     maxRecoveredLogicalPage + 1,
	}, nil
}
