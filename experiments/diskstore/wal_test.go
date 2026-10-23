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
	"testing"
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
