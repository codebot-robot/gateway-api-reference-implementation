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
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	// WALMagic is "WAL1" in big endian.
	WALMagic uint32 = 0x57414C31

	// WALVersion is 1.
	WALVersion uint32 = 1

	// SegmentHeaderSize is 32 bytes: Magic(4) + Version(4) + Reserved(8) + DBID(16).
	SegmentHeaderSize = 32

	// DefaultMaxSegmentSize is 16 MiB.
	DefaultMaxSegmentSize = 16 * 1024 * 1024
)

// WAL manages appending, syncing, and segment lifecycle of the write-ahead log.
type WAL struct {
	fs   FS
	dir  string
	dbID [16]byte
	mu   sync.Mutex

	activeFile     File
	activePath     string
	activeStartLSN uint64
	activeSize     int64

	maxLSN      atomic.Uint64
	fsyncedLSN  atomic.Uint64
	maxSegSize  int64
	createdLSNs map[uint64]bool // Asserts never reuse/recycle segment file names
}

// WALOptions configures a WAL instance.
type WALOptions struct {
	MaxSegmentSize int64
}

// OpenWAL opens or creates a WAL in the given directory.
func OpenWAL(fs FS, dir string, dbID [16]byte, initialLSN uint64, opts ...WALOptions) (*WAL, error) {
	if err := fs.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}
	if err := fs.SyncDir(dir); err != nil {
		return nil, fmt.Errorf("failed to sync WAL directory: %w", err)
	}

	maxSegSize := int64(DefaultMaxSegmentSize)
	if len(opts) > 0 && opts[0].MaxSegmentSize > 0 {
		maxSegSize = opts[0].MaxSegmentSize
	}

	w := &WAL{
		fs:          fs,
		dir:         dir,
		dbID:        dbID,
		maxSegSize:  maxSegSize,
		createdLSNs: make(map[uint64]bool),
	}

	// Scan existing segments to find highest LSN
	segments, err := w.listSegments()
	if err != nil {
		return nil, err
	}

	for _, seg := range segments {
		w.createdLSNs[seg.startLSN] = true
	}

	if initialLSN > 0 {
		w.maxLSN.Store(initialLSN)
		w.fsyncedLSN.Store(initialLSN)
	}

	return w, nil
}

type segmentMeta struct {
	path     string
	name     string
	startLSN uint64
	size     int64
}

func (w *WAL) listSegments() ([]segmentMeta, error) {
	entries, err := w.fs.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}

	var segments []segmentMeta
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}
		base := strings.TrimSuffix(entry.Name(), ".wal")
		lsn, err := strconv.ParseUint(base, 10, 64)
		if err != nil {
			continue // ignore non-matching names
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		segments = append(segments, segmentMeta{
			path:     filepath.Join(w.dir, entry.Name()),
			name:     entry.Name(),
			startLSN: lsn,
			size:     info.Size(),
		})
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].startLSN < segments[j].startLSN
	})
	return segments, nil
}

func segmentFileName(startLSN uint64) string {
	return fmt.Sprintf("%020d.wal", startLSN)
}

// createSegment creates a new immutable WAL segment named by starting LSN.
// Invariant: Never reuse or recycle a segment file name. Fsync the directory after creation.
func (w *WAL) createSegmentLocked(startLSN uint64) error {
	if w.createdLSNs[startLSN] {
		return fmt.Errorf("invariant violation: segment file name for LSN %d already used", startLSN)
	}

	name := segmentFileName(startLSN)
	path := filepath.Join(w.dir, name)

	f, err := w.fs.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create WAL segment %s: %w", name, err)
	}

	// Write segment header: Magic(4) + Version(4) + Reserved(8) + DBID(16)
	header := make([]byte, SegmentHeaderSize)
	binary.BigEndian.PutUint32(header[0:4], WALMagic)
	binary.BigEndian.PutUint32(header[4:8], WALVersion)
	// header[8:16] is zero padding
	copy(header[16:32], w.dbID[:])

	if _, err := f.WriteAt(header, 0); err != nil {
		return errors.Join(fmt.Errorf("failed to write segment header: %w", err), f.Close())
	}

	// Fsync the segment file
	if err := f.Sync(); err != nil {
		return errors.Join(fmt.Errorf("failed to sync new segment file: %w", err), f.Close())
	}

	// Fsync the directory after creating a segment
	if err := w.fs.SyncDir(w.dir); err != nil {
		return errors.Join(fmt.Errorf("failed to fsync directory after segment creation: %w", err), f.Close())
	}

	if w.activeFile != nil {
		// Existing segment becomes finished & immutable; close it
		syncErr := w.activeFile.Sync()
		closeErr := w.activeFile.Close()
		if err := errors.Join(syncErr, closeErr); err != nil {
			return fmt.Errorf("failed to sync and close active segment: %w", err)
		}
	}

	w.activeFile = f
	w.activePath = path
	w.activeStartLSN = startLSN
	w.activeSize = int64(SegmentHeaderSize)
	w.createdLSNs[startLSN] = true
	return nil
}

// NextLSN increments and returns the next 64-bit monotonic LSN.
func (w *WAL) NextLSN() uint64 {
	return w.maxLSN.Add(1)
}

// MaxLSN returns the current maximum allocated LSN.
func (w *WAL) MaxLSN() uint64 {
	return w.maxLSN.Load()
}

// FsyncedLSN returns the maximum LSN that has been fsynced.
func (w *WAL) FsyncedLSN() uint64 {
	return w.fsyncedLSN.Load()
}

// Append appends a record to the WAL.
// No fsync is performed on commit.
func (w *WAL) Append(r Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	encoded := EncodeRecord(r)

	// If no active file or file exceeds max size, roll to a new segment
	if w.activeFile == nil || w.activeSize+int64(len(encoded)) > w.maxSegSize {
		if err := w.createSegmentLocked(r.LSN); err != nil {
			return err
		}
	}

	n, err := w.activeFile.WriteAt(encoded, w.activeSize)
	if err != nil {
		return fmt.Errorf("failed to write WAL record: %w", err)
	}
	w.activeSize += int64(n)

	if r.LSN > w.maxLSN.Load() {
		w.maxLSN.Store(r.LSN)
	}
	return nil
}

// Sync flushes and fsyncs the active WAL file.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.activeFile == nil {
		return nil
	}
	if err := w.activeFile.Sync(); err != nil {
		return err
	}
	w.fsyncedLSN.Store(w.maxLSN.Load())
	return nil
}

// SyncThrough ensures that the WAL is fsynced at least through the given LSN.
// This fulfills the write-ahead rule: before writing any page with LSN L, the log must be fsynced through L.
func (w *WAL) SyncThrough(lsn uint64) error {
	if w.fsyncedLSN.Load() >= lsn {
		return nil
	}
	return w.Sync()
}

// TruncateBefore removes whole completed segment files whose records are all strictly prior to oldestNeededLSN.
// Finished segments are deleted only when fully covered by a completed checkpoint.
func (w *WAL) TruncateBefore(oldestNeededLSN uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	segments, err := w.listSegments()
	if err != nil {
		return err
	}

	// Never delete the active segment
	var deletedAny bool
	for i := 0; i < len(segments)-1; i++ {
		nextStart := segments[i+1].startLSN
		// If next segment starts at or before oldestNeededLSN, this whole segment is strictly before oldestNeededLSN
		if nextStart <= oldestNeededLSN {
			if err := w.fs.Remove(segments[i].path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("failed to delete truncated segment %s: %w", segments[i].name, err)
			}
			deletedAny = true
		}
	}

	if deletedAny {
		return w.fs.SyncDir(w.dir)
	}
	return nil
}

// Close closes the WAL and flushes remaining writes.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.activeFile != nil {
		syncErr := w.activeFile.Sync()
		closeErr := w.activeFile.Close()
		w.activeFile = nil
		return errors.Join(syncErr, closeErr)
	}
	return nil
}

// WALIterator replays records across segment files starting from a given LSN.
type WALIterator struct {
	fs          FS
	segments    []segmentMeta
	dbID        [16]byte
	currSegIdx  int
	currFile    File
	currOffset  int64
	expectedLSN uint64
	done        bool
}

// NewWALIterator creates an iterator to replay records from startLSN.
func NewWALIterator(fs FS, dir string, dbID [16]byte, startLSN uint64) (*WALIterator, error) {
	entries, err := fs.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var segments []segmentMeta
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}
		base := strings.TrimSuffix(entry.Name(), ".wal")
		lsn, err := strconv.ParseUint(base, 10, 64)
		if err != nil {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		segments = append(segments, segmentMeta{
			path:     filepath.Join(dir, entry.Name()),
			name:     entry.Name(),
			startLSN: lsn,
			size:     info.Size(),
		})
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].startLSN < segments[j].startLSN
	})

	it := &WALIterator{
		fs:          fs,
		segments:    segments,
		dbID:        dbID,
		currSegIdx:  -1,
		expectedLSN: 0,
	}

	// Advance to first valid segment
	if err := it.nextSegment(); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return it, nil
}

func (it *WALIterator) nextSegment() error {
	if it.currFile != nil {
		if err := it.currFile.Close(); err != nil {
			return err
		}
		it.currFile = nil
	}

	it.currSegIdx++
	if it.currSegIdx >= len(it.segments) {
		it.done = true
		return io.EOF
	}

	seg := it.segments[it.currSegIdx]
	f, err := it.fs.Open(seg.path)
	if err != nil {
		return err
	}

	// Read and verify segment header: Magic(4) + Version(4) + Reserved(8) + DBID(16)
	header := make([]byte, SegmentHeaderSize)
	if _, err := f.ReadAt(header, 0); err != nil {
		return errors.Join(fmt.Errorf("corrupt segment header in %s: %w", seg.name, err), f.Close())
	}

	magic := binary.BigEndian.Uint32(header[0:4])
	ver := binary.BigEndian.Uint32(header[4:8])
	if magic != WALMagic || ver != WALVersion || !bytes.Equal(header[16:32], it.dbID[:]) {
		return errors.Join(fmt.Errorf("foreign or invalid segment header in %s", seg.name), f.Close())
	}

	it.currFile = f
	it.currOffset = int64(SegmentHeaderSize)
	return nil
}

// Next reads the next valid record in sequence.
// Stops and returns io.EOF at first record with a bad CRC or non-contiguous LSN.
func (it *WALIterator) Next() (Record, error) {
	if it.done {
		return Record{}, io.EOF
	}

	for {
		if it.currFile == nil {
			return Record{}, io.EOF
		}

		// Read record header at currOffset
		hdr := make([]byte, RecordHeaderSize)
		n, err := it.currFile.ReadAt(hdr, it.currOffset)
		if err == io.EOF && n == 0 {
			// Try advancing to next segment
			if segErr := it.nextSegment(); segErr != nil {
				return Record{}, io.EOF
			}
			continue
		}
		if err != nil || n < RecordHeaderSize {
			// Torn / incomplete header -> stop recovery
			it.done = true
			return Record{}, io.EOF
		}

		keyLen := binary.BigEndian.Uint32(hdr[20:24])
		payloadLen := binary.BigEndian.Uint32(hdr[24:28])
		rvLen := binary.BigEndian.Uint32(hdr[28:32])
		totalRecordLen := RecordHeaderSize + int(keyLen) + int(payloadLen) + int(rvLen)

		recordBytes := make([]byte, totalRecordLen)
		n, err = it.currFile.ReadAt(recordBytes, it.currOffset)
		if err != nil && err != io.EOF {
			it.done = true
			return Record{}, io.EOF
		}
		if n < totalRecordLen {
			// Torn record body -> stop recovery
			it.done = true
			return Record{}, io.EOF
		}

		rec, decodeErr := DecodeRecord(bytes.NewReader(recordBytes))
		if decodeErr != nil {
			// Stop at first record with bad CRC or corruption
			it.done = true
			return Record{}, io.EOF
		}

		// Check LSN contiguity
		if it.expectedLSN > 0 && rec.LSN != it.expectedLSN {
			// Stop at first non-contiguous LSN
			it.done = true
			return Record{}, io.EOF
		}

		it.expectedLSN = rec.LSN + 1
		it.currOffset += int64(totalRecordLen)
		return rec, nil
	}
}

// Close closes the iterator.
func (it *WALIterator) Close() error {
	if it.currFile != nil {
		err := it.currFile.Close()
		it.currFile = nil
		return err
	}
	return nil
}
