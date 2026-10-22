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
	"fmt"
	"sync"
)

// Checkpointer coordinates non-blocking checkpoints between the buffer pool, heap file, and WAL.
type Checkpointer struct {
	bp       *BufferPool
	heapFile File
	wal      *WAL
	mu       sync.Mutex
}

// NewCheckpointer creates a new Checkpointer.
func NewCheckpointer(bp *BufferPool, heapFile File, wal *WAL) *Checkpointer {
	return &Checkpointer{
		bp:       bp,
		heapFile: heapFile,
		wal:      wal,
	}
}

// Checkpoint performs a non-blocking checkpoint:
// 1. Snapshot dirty pages and determine redo start LSN (min of dirtyLSN).
// 2. Flush dirty pages to new physical locations (out-of-place write).
// 3. Fsync heap file.
// 4. Promote flushed locations to durable; free older superseded durable locations.
// 5. Append checkpoint record to WAL giving redo start LSN.
// 6. Fsync WAL.
// 7. Truncate eligible WAL segments strictly prior to redo start LSN.
func (c *Checkpointer) Checkpoint() (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 1. Determine redo start LSN from dirty page table before flush
	dpt := c.bp.DirtyPageTable()
	var redoStartLSN uint64

	for _, dirtyLSN := range dpt {
		if redoStartLSN == 0 || dirtyLSN < redoStartLSN {
			redoStartLSN = dirtyLSN
		}
	}

	// 2. Flush dirty pages to disk under write-ahead rule
	if err := c.bp.FlushAllDirty(); err != nil {
		return 0, fmt.Errorf("checkpoint failed to flush dirty pages: %w", err)
	}

	// 3. Fsync heap file
	if err := c.heapFile.Sync(); err != nil {
		return 0, fmt.Errorf("checkpoint failed to fsync heap file: %w", err)
	}

	// 4. Promote flushed locations to durable (frees superseded locations)
	c.bp.PromoteFlushedToDurable()

	chkLSN := c.wal.NextLSN()
	if redoStartLSN == 0 {
		redoStartLSN = chkLSN
	}

	// 5. Append checkpoint record to WAL
	chkRec := Record{
		Type:         RecordTypeCheckpoint,
		LSN:          chkLSN,
		RedoStartLSN: redoStartLSN,
	}
	if err := c.wal.Append(chkRec); err != nil {
		return 0, fmt.Errorf("checkpoint failed to write WAL record: %w", err)
	}

	// 6. Fsync WAL
	if err := c.wal.Sync(); err != nil {
		return 0, fmt.Errorf("checkpoint failed to fsync WAL: %w", err)
	}

	// 7. Truncate finished WAL segments prior to redoStartLSN
	if err := c.wal.TruncateBefore(redoStartLSN); err != nil {
		return 0, fmt.Errorf("checkpoint failed to truncate WAL: %w", err)
	}

	return chkLSN, nil
}
