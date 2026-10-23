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
	"time"

	"k8s.io/klog/v2"
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

	// WALBlockSize is 4 KiB block alignment for direct I/O WAL writes.
	WALBlockSize = 4096
)

type pendingWALBlock struct {
	file   File
	offset int64
	block  []byte
	maxLSN uint64
}

// WAL manages appending, syncing, and segment lifecycle of the write-ahead log.
type WAL struct {
	fs   FS
	dir  string
	dbID [16]byte
	mu   sync.Mutex

	activeFile     File
	activePath     string
	activeStartLSN uint64
	activeSize     int64 // Total bytes written/committed to current segment file

	tailBlock         []byte // Aligned 4 KiB buffer for current block
	tailOffset        int    // Current offset within tailBlock (0..4096)
	blockOffset       int64  // Physical file offset where tailBlock starts
	tailBlockOpenTime time.Time

	nextLSN         uint64 // Next monotonic LSN to allocate under mu
	lastAppendedLSN uint64 // Highest LSN successfully written to buffer
	maxLSN          atomic.Uint64
	fsyncedLSN      atomic.Uint64 // Highest LSN covered by fdatasync
	maxSegSize      int64
	createdLSNs     map[uint64]bool // Asserts never reuse/recycle segment file names

	queueCond      *sync.Cond
	syncCond       *sync.Cond
	queue          []pendingWALBlock
	maxQueueBlocks int

	wakeCh        chan struct{}
	bgStop        chan struct{}
	bgDone        chan struct{}
	stickyErr     error
	onFailure     func(error)
	closed        bool
	syncRequested bool
}

// WALOptions configures a WAL instance.
type WALOptions struct {
	MaxSegmentSize int64
	OnFailure      func(error)
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
	var onFailure func(error)
	if len(opts) > 0 {
		if opts[0].MaxSegmentSize > 0 {
			maxSegSize = opts[0].MaxSegmentSize
		}
		onFailure = opts[0].OnFailure
	}

	w := &WAL{
		fs:              fs,
		dir:             dir,
		dbID:            dbID,
		maxSegSize:      maxSegSize,
		createdLSNs:     make(map[uint64]bool),
		tailBlock:       AllocateAlignedBlock(WALBlockSize),
		nextLSN:         initialLSN + 1,
		lastAppendedLSN: initialLSN,
		maxQueueBlocks:  1024,
		wakeCh:          make(chan struct{}, 1),
		bgStop:          make(chan struct{}),
		bgDone:          make(chan struct{}),
		onFailure:       onFailure,
	}
	w.queueCond = sync.NewCond(&w.mu)
	w.syncCond = sync.NewCond(&w.mu)

	// Scan existing segments to record used start LSNs
	segments, err := w.listSegments()
	if err != nil {
		return nil, err
	}

	for _, seg := range segments {
		w.createdLSNs[seg.startLSN] = true
	}

	w.maxLSN.Store(initialLSN)
	w.fsyncedLSN.Store(initialLSN)

	go w.bgWriter()

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

func (w *WAL) bgWriter() {
	defer close(w.bgDone)

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-w.bgStop:
			w.mu.Lock()
			if err := w.flushQueueAndSyncLocked(true); err != nil {
				klog.V(4).ErrorS(err, "failed to flush queue and sync on bgStop")
			}
			w.mu.Unlock()
			return
		case <-w.wakeCh:
			w.mu.Lock()
			if err := w.flushQueueAndSyncLocked(w.syncRequested); err != nil {
				klog.V(4).ErrorS(err, "failed to flush queue and sync on wake")
			}
			w.mu.Unlock()
		case <-ticker.C:
			w.mu.Lock()
			shouldSync := w.syncRequested || (w.tailOffset > 0 && !w.tailBlockOpenTime.IsZero() && time.Since(w.tailBlockOpenTime) >= 5*time.Second)
			if len(w.queue) > 0 || shouldSync {
				if err := w.flushQueueAndSyncLocked(shouldSync); err != nil {
					klog.V(4).ErrorS(err, "failed to flush queue and sync on ticker")
				}
			}
			w.mu.Unlock()
		}
	}
}

func (w *WAL) setStickyErrLocked(err error) {
	if w.stickyErr == nil && err != nil {
		w.stickyErr = err
		if w.onFailure != nil {
			w.onFailure(err)
		}
		w.syncCond.Broadcast()
		w.queueCond.Broadcast()
	}
}

func (w *WAL) flushQueueAndSyncLocked(doSync bool) error {
	if w.stickyErr != nil {
		return w.stickyErr
	}

	for len(w.queue) > 0 {
		p := w.queue[0]
		w.queue = w.queue[1:]
		w.queueCond.Signal()

		if _, err := p.file.WriteAt(p.block, p.offset); err != nil {
			w.setStickyErrLocked(fmt.Errorf("failed to write WAL block at offset %d: %w", p.offset, err))
			return w.stickyErr
		}
	}

	if doSync {
		if w.activeFile != nil {
			if w.tailOffset > 0 {
				for i := w.tailOffset; i < WALBlockSize; i++ {
					w.tailBlock[i] = 0
				}
				if _, err := w.activeFile.WriteAt(w.tailBlock, w.blockOffset); err != nil {
					w.setStickyErrLocked(fmt.Errorf("failed to write padded WAL block on sync: %w", err))
					return w.stickyErr
				}
				w.blockOffset += WALBlockSize
				w.activeSize = w.blockOffset
				w.tailOffset = 0
				w.tailBlockOpenTime = time.Time{}
				w.tailBlock = AllocateAlignedBlock(WALBlockSize)
			}

			if err := w.activeFile.Sync(); err != nil {
				w.setStickyErrLocked(fmt.Errorf("failed to fsync WAL segment: %w", err))
				return w.stickyErr
			}
		}
		w.fsyncedLSN.Store(w.lastAppendedLSN)
		w.syncRequested = false
		w.syncCond.Broadcast()
	}
	return nil
}

// createSegmentLocked creates a new immutable WAL segment named by starting LSN.
// Invariant: Never reuse or recycle a segment file name. Fsync the directory after creation.
func (w *WAL) createSegmentLocked(startLSN uint64) error {
	if w.createdLSNs[startLSN] {
		return fmt.Errorf("invariant violation: segment file name for LSN %d already used", startLSN)
	}

	// Flush any queued blocks and sync existing active segment before rolling
	if w.activeFile != nil {
		if err := w.flushQueueAndSyncLocked(true); err != nil {
			return err
		}
	}

	name := segmentFileName(startLSN)
	path := filepath.Join(w.dir, name)

	openFlags := os.O_RDWR | os.O_CREATE | DirectIOOpenFlag()
	f, err := w.fs.OpenFile(path, openFlags, 0644)
	if err != nil {
		if DirectIOOpenFlag() != 0 {
			klog.Warningf("diskstore: WAL segment %s failed to open with direct I/O: %v, falling back to buffered I/O", name, err)
			GlobalMetrics.DirectIOFallbackCount.Add(1)
		}
		// Fallback without DirectIOOpenFlag if not supported by FS
		f, err = w.fs.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return fmt.Errorf("failed to create WAL segment %s: %w", name, err)
		}
	}

	// Prepare segment header in tail block
	clearBytes(w.tailBlock)
	binary.BigEndian.PutUint32(w.tailBlock[0:4], WALMagic)
	binary.BigEndian.PutUint32(w.tailBlock[4:8], WALVersion)
	// bytes 8..16 reserved zero padding
	copy(w.tailBlock[16:32], w.dbID[:])

	// Write initial header block
	if _, err := f.WriteAt(w.tailBlock, 0); err != nil {
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
		closeErr := w.activeFile.Close()
		if closeErr != nil {
			return fmt.Errorf("failed to close superseded active segment: %w", closeErr)
		}
	}

	w.activeFile = f
	w.activePath = path
	w.activeStartLSN = startLSN
	w.blockOffset = 0
	w.tailOffset = SegmentHeaderSize
	w.activeSize = int64(SegmentHeaderSize)
	w.createdLSNs[startLSN] = true
	return nil
}

// NextLSN increments and returns the next 64-bit monotonic LSN.
func (w *WAL) NextLSN() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	lsn := w.nextLSN
	w.nextLSN++
	if lsn > w.maxLSN.Load() {
		w.maxLSN.Store(lsn)
	}
	return lsn
}

// MaxLSN returns the current maximum allocated LSN.
func (w *WAL) MaxLSN() uint64 {
	return w.maxLSN.Load()
}

// FsyncedLSN returns the maximum LSN that has been fsynced.
func (w *WAL) FsyncedLSN() uint64 {
	return w.fsyncedLSN.Load()
}

// DurableLSN returns the maximum LSN confirmed durable by fdatasync.
func (w *WAL) DurableLSN() uint64 {
	return w.fsyncedLSN.Load()
}

// Append appends a record to the WAL.
// No fsync is performed on commit.
func (w *WAL) Append(r Record) error {
	_, err := w.AppendRecord(&r)
	return err
}

// AppendRecord appends a record to the WAL, assigning LSN under WAL lock if LSN == 0.
func (w *WAL) AppendRecord(r *Record) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.stickyErr != nil {
		return 0, w.stickyErr
	}
	if w.closed {
		return 0, errors.New("wal is closed")
	}

	if r.LSN == 0 {
		r.LSN = w.nextLSN
		w.nextLSN++
	} else if r.LSN >= w.nextLSN {
		w.nextLSN = r.LSN + 1
	}
	if r.LSN > w.maxLSN.Load() {
		w.maxLSN.Store(r.LSN)
	}

	encoded := EncodeRecord(*r)

	// Block if queue is full (bounded buffer)
	for len(w.queue) >= w.maxQueueBlocks {
		if w.stickyErr != nil {
			return 0, w.stickyErr
		}
		w.queueCond.Wait()
	}

	// If no active file or file exceeds max size, roll to a new segment
	if w.activeFile == nil || w.blockOffset+int64(w.tailOffset)+int64(len(encoded)) > w.maxSegSize {
		if err := w.createSegmentLocked(r.LSN); err != nil {
			return 0, err
		}
	}

	if w.tailOffset == 0 && w.tailBlockOpenTime.IsZero() {
		w.tailBlockOpenTime = time.Now()
	}

	written := 0
	for written < len(encoded) {
		space := WALBlockSize - w.tailOffset
		toCopy := len(encoded) - written
		if toCopy > space {
			toCopy = space
		}
		copy(w.tailBlock[w.tailOffset:w.tailOffset+toCopy], encoded[written:written+toCopy])
		w.tailOffset += toCopy
		written += toCopy

		if w.tailOffset == WALBlockSize {
			// Enqueue full 4 KiB block for background direct I/O writing
			w.queue = append(w.queue, pendingWALBlock{
				file:   w.activeFile,
				offset: w.blockOffset,
				block:  w.tailBlock,
				maxLSN: r.LSN,
			})
			w.blockOffset += WALBlockSize
			w.activeSize = w.blockOffset
			w.tailOffset = 0
			w.tailBlockOpenTime = time.Time{}
			w.tailBlock = AllocateAlignedBlock(WALBlockSize)

			select {
			case w.wakeCh <- struct{}{}:
			default:
			}
		}
	}

	if w.tailOffset > 0 && w.tailBlockOpenTime.IsZero() {
		w.tailBlockOpenTime = time.Now()
	}

	w.lastAppendedLSN = r.LSN
	return r.LSN, nil
}

// Sync flushes and fsyncs the active WAL file.
func (w *WAL) Sync() error {
	w.mu.Lock()
	targetLSN := w.lastAppendedLSN
	w.mu.Unlock()
	return w.SyncThrough(targetLSN)
}

// SyncThrough ensures that the WAL is fsynced at least through the given LSN.
// This fulfills the write-ahead rule: before writing any page with LSN L, the log must be fsynced through L.
func (w *WAL) SyncThrough(lsn uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for {
		if w.stickyErr != nil {
			return w.stickyErr
		}
		if w.fsyncedLSN.Load() >= lsn {
			return nil
		}
		if w.fsyncedLSN.Load() >= w.lastAppendedLSN {
			// All appended WAL records are already fsynced
			return nil
		}
		w.syncRequested = true
		select {
		case w.wakeCh <- struct{}{}:
		default:
		}
		w.syncCond.Wait()
	}
}

// Flush flushes all buffered records and fsyncs the WAL.
func (w *WAL) Flush() error {
	w.mu.Lock()
	targetLSN := w.lastAppendedLSN
	w.mu.Unlock()
	return w.SyncThrough(targetLSN)
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
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	close(w.bgStop)
	w.mu.Unlock()

	<-w.bgDone

	w.mu.Lock()
	defer w.mu.Unlock()

	var errs []error
	if w.activeFile != nil {
		syncErr := w.activeFile.Sync()
		closeErr := w.activeFile.Close()
		w.activeFile = nil
		if syncErr != nil || closeErr != nil {
			errs = append(errs, errors.Join(syncErr, closeErr))
		}
	}
	if w.stickyErr != nil {
		errs = append(errs, w.stickyErr)
	}
	return errors.Join(errs...)
}

// WALIterator replays records across segment files starting from a given LSN.
type WALIterator struct {
	fs          FS
	segments    []segmentMeta
	dbID        [16]byte
	currSegIdx  int
	currFile    File
	currOffset  int64
	startLSN    uint64
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

	// Find the segment that could contain startLSN
	startIdx := 0
	for i, seg := range segments {
		if seg.startLSN <= startLSN {
			startIdx = i
		} else {
			break
		}
	}

	it := &WALIterator{
		fs:          fs,
		segments:    segments,
		dbID:        dbID,
		currSegIdx:  startIdx - 1,
		startLSN:    startLSN,
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
// Honors startLSN and properly handles torn tails when later segments exist.
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

		// Check if at clean EOF of segment
		if err == io.EOF && n == 0 {
			if segErr := it.nextSegment(); segErr != nil {
				return Record{}, io.EOF
			}
			continue
		}

		// Check for real read I/O error
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			it.done = true
			return Record{}, fmt.Errorf("read error reading record header at offset %d: %w", it.currOffset, err)
		}

		// Check for zero-padded block or torn header
		isZeroPadding := true
		for _, b := range hdr[:n] {
			if b != 0 {
				isZeroPadding = false
				break
			}
		}

		if isZeroPadding {
			// Check if we can skip zero padding to the next 4 KiB block boundary within this segment
			nextBlock := ((it.currOffset / WALBlockSize) + 1) * WALBlockSize
			size, sizeErr := it.currFile.Size()
			if sizeErr == nil && nextBlock < size {
				it.currOffset = nextBlock
				continue
			}
			// Reached end of blocks in this segment: check if next segment starts at expected LSN
			if it.canAdvanceToNextSegment() {
				if segErr := it.nextSegment(); segErr != nil {
					it.done = true
					return Record{}, io.EOF
				}
				continue
			}
			it.done = true
			return Record{}, io.EOF
		}

		if n < RecordHeaderSize {
			// Torn header: check if next segment starts at expected LSN
			if it.canAdvanceToNextSegment() {
				if segErr := it.nextSegment(); segErr != nil {
					it.done = true
					return Record{}, io.EOF
				}
				continue
			}
			// Otherwise, this is the true end of the log
			it.done = true
			return Record{}, io.EOF
		}

		keyLen := binary.BigEndian.Uint32(hdr[20:24])
		payloadLen := binary.BigEndian.Uint32(hdr[24:28])
		rvLen := binary.BigEndian.Uint32(hdr[28:32])
		totalRecordLen := RecordHeaderSize + int(keyLen) + int(payloadLen) + int(rvLen)

		recordBytes := make([]byte, totalRecordLen)
		n, err = it.currFile.ReadAt(recordBytes, it.currOffset)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			it.done = true
			return Record{}, fmt.Errorf("read error reading record body at offset %d: %w", it.currOffset, err)
		}

		if n < totalRecordLen {
			// Torn record body: check if next segment starts at expected LSN
			if it.canAdvanceToNextSegment() {
				if segErr := it.nextSegment(); segErr != nil {
					it.done = true
					return Record{}, io.EOF
				}
				continue
			}
			it.done = true
			return Record{}, io.EOF
		}

		rec, decodeErr := DecodeRecord(bytes.NewReader(recordBytes))
		if decodeErr != nil {
			// Corrupt record or bad CRC: check if next segment starts at expected LSN
			if it.canAdvanceToNextSegment() {
				if segErr := it.nextSegment(); segErr != nil {
					it.done = true
					return Record{}, io.EOF
				}
				continue
			}
			it.done = true
			return Record{}, io.EOF
		}

		// If startLSN was specified, skip records prior to startLSN
		if it.startLSN > 0 && rec.LSN < it.startLSN {
			it.currOffset += int64(totalRecordLen)
			continue
		}

		// Check LSN contiguity
		if it.expectedLSN > 0 && rec.LSN != it.expectedLSN {
			// LSN gap: check if next segment can continue
			if it.canAdvanceToNextSegment() {
				if segErr := it.nextSegment(); segErr != nil {
					it.done = true
					return Record{}, io.EOF
				}
				continue
			}
			it.done = true
			return Record{}, io.EOF
		}

		it.expectedLSN = rec.LSN + 1
		it.currOffset += int64(totalRecordLen)
		return rec, nil
	}
}

// canAdvanceToNextSegment checks if a later segment exists starting at expectedLSN.
func (it *WALIterator) canAdvanceToNextSegment() bool {
	if it.currSegIdx+1 < len(it.segments) {
		nextSeg := it.segments[it.currSegIdx+1]
		if it.expectedLSN == 0 || nextSeg.startLSN == it.expectedLSN {
			return true
		}
	}
	return false
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

func clearBytes(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
