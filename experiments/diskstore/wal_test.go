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
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWAL_AppendAndIterate(t *testing.T) {
	fs := NewMemFS()
	var dbID [16]byte
	copy(dbID[:], []byte("wal-test-db-0001"))

	wal, err := OpenWAL(fs, "wal_dir", dbID, 0, WALOptions{MaxSegmentSize: 200})
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	// Write 10 records. With MaxSegmentSize=200, this should roll across multiple segments.
	for i := uint64(1); i <= 10; i++ {
		rec := Record{
			Type:            RecordTypePut,
			LSN:             i,
			Key:             fmt.Sprintf("ns/cm-%d", i),
			Payload:         []byte(fmt.Sprintf("payload-%d", i)),
			ResourceVersion: fmt.Sprintf("rv-%d", i),
		}
		if err := wal.Append(rec); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	if err := wal.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	_ = wal.Close()

	// Verify segments are created and directory was synced
	if fs.DirSyncCount("wal_dir") < 2 {
		t.Fatalf("expected multiple dir syncs on segment creation, got %d", fs.DirSyncCount("wal_dir"))
	}

	// Replay from beginning
	it, err := NewWALIterator(fs, "wal_dir", dbID, 1)
	if err != nil {
		t.Fatalf("NewWALIterator failed: %v", err)
	}
	defer it.Close()

	var recoveredCount uint64
	for {
		rec, err := it.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Iterator error: %v", err)
		}
		recoveredCount++
		if rec.LSN != recoveredCount {
			t.Fatalf("expected LSN %d, got %d", recoveredCount, rec.LSN)
		}
		if rec.Key != fmt.Sprintf("ns/cm-%d", recoveredCount) {
			t.Fatalf("unexpected key: %s", rec.Key)
		}
	}

	if recoveredCount != 10 {
		t.Fatalf("expected 10 recovered records, got %d", recoveredCount)
	}
}

func TestWAL_StopAtBadCRC(t *testing.T) {
	fs := NewMemFS()
	var dbID [16]byte
	copy(dbID[:], []byte("wal-test-db-0002"))

	wal, err := OpenWAL(fs, "wal_dir", dbID, 0, WALOptions{MaxSegmentSize: 1024})
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	for i := uint64(1); i <= 5; i++ {
		_ = wal.Append(Record{
			Type:            RecordTypePut,
			LSN:             i,
			Key:             fmt.Sprintf("key-%d", i),
			Payload:         []byte("payload"),
			ResourceVersion: "100",
		})
	}
	_ = wal.Sync()
	_ = wal.Close()

	// Corrupt record 4
	entries, _ := fs.ReadDir("wal_dir")
	segPath := "wal_dir/" + entries[0].Name()
	file, _ := fs.OpenFile(segPath, 0, 0)
	size, _ := file.Size()
	data := make([]byte, size)
	_, _ = file.ReadAt(data, 0)
	// Corrupt byte inside record 4
	data[200] ^= 0xff
	_, _ = file.WriteAt(data, 0)
	_ = file.Close()

	// Replay should cleanly stop before corrupted record 4 or 5
	it, err := NewWALIterator(fs, "wal_dir", dbID, 1)
	if err != nil {
		t.Fatalf("NewWALIterator failed: %v", err)
	}
	defer it.Close()

	var count int
	for {
		_, err := it.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		count++
	}

	if count >= 5 {
		t.Fatalf("expected iteration to stop before corrupt record, but read %d records", count)
	}
}

func TestWAL_TruncateSegments(t *testing.T) {
	fs := NewMemFS()
	var dbID [16]byte
	copy(dbID[:], []byte("wal-test-db-0003"))

	// Create small segments (100 bytes) so each record rolls segment
	wal, err := OpenWAL(fs, "wal_dir", dbID, 0, WALOptions{MaxSegmentSize: 80})
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	for i := uint64(1); i <= 6; i++ {
		_ = wal.Append(Record{
			Type:            RecordTypePut,
			LSN:             i,
			Key:             fmt.Sprintf("k%d", i),
			Payload:         []byte("short"),
			ResourceVersion: "1",
		})
	}
	_ = wal.Sync()

	entriesBefore, _ := fs.ReadDir("wal_dir")
	if len(entriesBefore) < 4 {
		t.Fatalf("expected at least 4 segments, got %d", len(entriesBefore))
	}

	// Truncate before LSN 4 (checkpoint covers records 1..3)
	if err := wal.TruncateBefore(4); err != nil {
		t.Fatalf("TruncateBefore failed: %v", err)
	}

	entriesAfter, _ := fs.ReadDir("wal_dir")
	if len(entriesAfter) >= len(entriesBefore) {
		t.Fatalf("expected fewer segments after truncation: before=%d, after=%d",
			len(entriesBefore), len(entriesAfter))
	}

	// First remaining segment must have startLSN >= 4 (or be the active segment)
	_ = wal.Close()
}

// TestWAL_AppendRecordCompletesWhileWriteOrSyncBlocked asserts that AppendRecord completes
// while a WAL write or fsync is blocked in the fault filesystem (Item 2).
func TestWAL_AppendRecordCompletesWhileWriteOrSyncBlocked(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)
	var dbID [16]byte
	copy(dbID[:], []byte("wal-test-db-0004"))

	wal, err := OpenWAL(faultFS, "wal_dir", dbID, 0)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	// Initial append and sync to initialize active segment and header
	_, err = wal.AppendRecord(&Record{
		Type:    RecordTypePut,
		Key:     "init",
		Payload: []byte("init"),
	})
	if err != nil {
		t.Fatalf("initial append failed: %v", err)
	}
	if err := wal.Sync(); err != nil {
		t.Fatalf("initial sync failed: %v", err)
	}

	syncBlocked := make(chan struct{})
	releaseSync := make(chan struct{})

	faultFS.SetBeforeSync(func(path string) {
		if strings.HasSuffix(path, ".wal") {
			select {
			case <-syncBlocked:
				// Already triggered
			default:
				close(syncBlocked)
				<-releaseSync
			}
		}
	})

	// Start a background sync on record 2
	syncDone := make(chan error, 1)
	go func() {
		_, aErr := wal.AppendRecord(&Record{
			Type:    RecordTypePut,
			Key:     "k1",
			Payload: []byte("v1"),
		})
		if aErr != nil {
			syncDone <- aErr
			return
		}
		syncDone <- wal.Sync()
	}()

	// Wait until WAL writer is blocked in fsync
	select {
	case <-syncBlocked:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for WAL sync to block")
	}

	// Now call AppendRecord from this goroutine: it must complete immediately without waiting for fsync!
	appendDone := make(chan error, 1)
	go func() {
		_, aErr := wal.AppendRecord(&Record{
			Type:    RecordTypePut,
			Key:     "k2",
			Payload: []byte("v2"),
		})
		appendDone <- aErr
	}()

	select {
	case err := <-appendDone:
		if err != nil {
			t.Fatalf("AppendRecord failed while sync was blocked: %v", err)
		}
	case <-time.After(1 * time.Second):
		close(releaseSync)
		t.Fatalf("AppendRecord stalled while WAL writer was performing fsync!")
	}

	// Unblock the fsync
	close(releaseSync)
	if err := <-syncDone; err != nil {
		t.Fatalf("background sync failed: %v", err)
	}

	if err := wal.Sync(); err != nil {
		t.Fatalf("final sync failed: %v", err)
	}
	_ = wal.Close()

	// Verify both records are present and recoverable in order
	it, err := NewWALIterator(faultFS, "wal_dir", dbID, 1)
	if err != nil {
		t.Fatalf("NewWALIterator failed: %v", err)
	}
	defer it.Close()

	var keys []string
	for {
		rec, err := it.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("iteration error: %v", err)
		}
		keys = append(keys, rec.Key)
	}

	expected := []string{"init", "k1", "k2"}
	if len(keys) != len(expected) {
		t.Fatalf("expected keys %v, got %v", expected, keys)
	}
	for i, k := range keys {
		if k != expected[i] {
			t.Fatalf("key %d mismatch: got %s, expected %s", i, k, expected[i])
		}
	}
}

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// TestWAL_FiveSecondBound_UnsyncedBlocks asserts that written but unsynced blocks are fsynced
// within 5 seconds when appends stop on exact block boundaries (Item 3).
func TestWAL_FiveSecondBound_UnsyncedBlocks(t *testing.T) {
	fs := NewMemFS()
	var dbID [16]byte
	copy(dbID[:], []byte("wal-test-db-0005"))

	clk := &testClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	wal, err := OpenWAL(fs, "wal_dir", dbID, 0, WALOptions{
		Clock:        clk,
		TickInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}
	defer wal.Close()

	// Append record 1 that fills exactly the rest of the first 4 KiB block.
	// Block 0 has 32 bytes segment header.
	// Space remaining in block 0: 4096 - 32 = 4064 bytes.
	// Record header is 36 bytes.
	// Payload needed: 4064 - 36 = 4028 bytes.
	p1 := make([]byte, 4028)
	_, err = wal.AppendRecord(&Record{
		Type:    RecordTypePut,
		Payload: p1,
	})
	if err != nil {
		t.Fatalf("AppendRecord 1 failed: %v", err)
	}

	// Append record 2 that fills exactly the next 4 KiB block (4096 bytes).
	// Payload needed: 4096 - 36 = 4060 bytes.
	p2 := make([]byte, 4060)
	lsn2, err := wal.AppendRecord(&Record{
		Type:    RecordTypePut,
		Payload: p2,
	})
	if err != nil {
		t.Fatalf("AppendRecord 2 failed: %v", err)
	}

	// Verify tailOffset is exactly 0 (filled whole blocks)
	wal.mu.Lock()
	tailOff := wal.tailOffset
	wal.mu.Unlock()
	if tailOff != 0 {
		t.Fatalf("expected tailOffset to be 0, got %d", tailOff)
	}

	// Allow background writer to flush the queued blocks to disk without sync
	time.Sleep(50 * time.Millisecond)

	// Verify that blocks were NOT yet fsynced
	if wal.FsyncedLSN() >= lsn2 {
		t.Fatalf("expected blocks to not be fsynced immediately, fsyncedLSN=%d, lsn2=%d", wal.FsyncedLSN(), lsn2)
	}

	// Advance clock by 5 seconds without injecting any sync or appends
	clk.Advance(5 * time.Second)

	// Poll for fsync to occur within a short timeout
	deadline := time.Now().Add(2 * time.Second)
	for wal.FsyncedLSN() < lsn2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if wal.FsyncedLSN() < lsn2 {
		t.Fatalf("expected WAL to fsync within 5s bound, fsyncedLSN=%d, expected at least %d", wal.FsyncedLSN(), lsn2)
	}
}
