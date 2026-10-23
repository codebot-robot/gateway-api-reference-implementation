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
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"k8s.io/klog/v2"
)

var (
	// ErrStoreFailed is returned when the store has entered the failed state due to corruption or I/O error.
	ErrStoreFailed = errors.New("diskstore has entered a failed state and cannot serve requests")

	// ErrKeyNotFound indicates the requested key does not exist.
	ErrKeyNotFound = errors.New("key not found")
)

func qualifyKey(bucketName, key string) string {
	return bucketName + "\x00" + key
}

func unqualifyKey(qualified string) (string, string) {
	parts := strings.SplitN(qualified, "\x00", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", qualified
}

// StoreOptions configures a DiskStore instance.
type StoreOptions struct {
	Dir        string
	FS         FS
	Cipher     Cipher
	Codec      ObjectCodec
	NumFrames  int
	AutoDelete bool
	Clock      Clock
}

// StoreOption is a functional option for configuring a DiskStore.
type StoreOption func(*StoreOptions)

// WithClock sets the Clock used for WAL timing.
func WithClock(clock Clock) StoreOption {
	return func(o *StoreOptions) {
		o.Clock = clock
	}
}

// WithDir sets the directory for store files.
func WithDir(dir string) StoreOption {
	return func(o *StoreOptions) {
		o.Dir = dir
	}
}

// WithFS sets the filesystem implementation (e.g. for fault injection).
func WithFS(fs FS) StoreOption {
	return func(o *StoreOptions) {
		o.FS = fs
	}
}

// WithCipher sets the cipher for encryption.
func WithCipher(cipher Cipher) StoreOption {
	return func(o *StoreOptions) {
		o.Cipher = cipher
	}
}

// WithCodec sets the object codec.
func WithCodec(codec ObjectCodec) StoreOption {
	return func(o *StoreOptions) {
		o.Codec = codec
	}
}

// WithNumFrames sets the buffer pool frame count.
func WithNumFrames(n int) StoreOption {
	return func(o *StoreOptions) {
		o.NumFrames = n
	}
}

// WithAutoDelete specifies whether to delete the directory on Close.
func WithAutoDelete(autoDelete bool) StoreOption {
	return func(o *StoreOptions) {
		o.AutoDelete = autoDelete
	}
}

// DiskStore coordinates the write-ahead log, heap file, buffer pool, buddy allocator,
// in-memory index, and crash consistency mechanisms.
type DiskStore struct {
	fs         FS
	dir        string
	dbID       [16]byte
	isTemp     bool
	autoDelete bool

	cipher Cipher
	codec  ObjectCodec

	heapFile File
	wal      *WAL
	buddy    *BuddyAllocator
	bp       *BufferPool
	cp       *Checkpointer

	writerMu sync.RWMutex // Single writer: serializes Put, Delete; RLock for point-in-time List

	keyMu           sync.RWMutex
	keyIndex        map[string]KeyIndexEntry // bucketQualifiedKey -> KeyIndexEntry
	oversizeObjects map[string][]byte        // bucketQualifiedKey -> payload

	emptyLogicalPages []uint64
	nextLogicalPage   uint64
	activeSlottedPage uint64

	failed    atomic.Bool
	failedErr error
	failMu    sync.RWMutex

	lastRecoveredRV         string
	lastRecoveredRVByBucket map[string]string
	closed                  atomic.Bool
}

// OpenStore opens or initializes a DiskStore in dir.
func OpenStore(opts ...StoreOption) (*DiskStore, error) {
	options := StoreOptions{
		FS:         NewOSFS(),
		Cipher:     NoopCipher{},
		Codec:      NewHybridCodec(),
		NumFrames:  DefaultBufferPoolFrames,
		AutoDelete: false,
	}
	for _, opt := range opts {
		opt(&options)
	}

	dir := options.Dir
	isTemp := false
	if dir == "" {
		tmpDir, err := os.MkdirTemp("", "gari-diskstore-*")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp diskstore dir: %w", err)
		}
		dir = tmpDir
		isTemp = true
		options.AutoDelete = true
	}

	if err := options.FS.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create store dir: %w", err)
	}
	if err := options.FS.SyncDir(dir); err != nil {
		return nil, fmt.Errorf("failed to sync store dir: %w", err)
	}

	metaPath := filepath.Join(dir, "meta.db")
	heapPath := filepath.Join(dir, "heap.db")
	walDir := filepath.Join(dir, "wal")
	oversizeDir := filepath.Join(dir, "oversize")

	if err := options.FS.MkdirAll(oversizeDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create oversize directory: %w", err)
	}

	var dbID [16]byte
	isNewStore := false

	// Read or generate database ID
	metaFile, err := options.FS.OpenFile(metaPath, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) || errors.Is(err, os.ErrNotExist) {
			isNewStore = true
			if _, randErr := io.ReadFull(rand.Reader, dbID[:]); randErr != nil {
				return nil, fmt.Errorf("failed to generate DB ID: %w", randErr)
			}
			metaFile, err = options.FS.Create(metaPath)
			if err != nil {
				return nil, fmt.Errorf("failed to create meta file: %w", err)
			}
			if _, wErr := metaFile.WriteAt(dbID[:], 0); wErr != nil {
				closeErr := metaFile.Close()
				return nil, errors.Join(wErr, closeErr)
			}
			if err := metaFile.Sync(); err != nil {
				closeErr := metaFile.Close()
				return nil, errors.Join(fmt.Errorf("failed to sync meta file: %w", err), closeErr)
			}
		} else {
			return nil, err
		}
	} else {
		if _, rErr := metaFile.ReadAt(dbID[:], 0); rErr != nil {
			closeErr := metaFile.Close()
			return nil, errors.Join(fmt.Errorf("failed to read DB ID from meta: %w", rErr), closeErr)
		}
	}
	if err := metaFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close meta file: %w", err)
	}
	if err := options.FS.SyncDir(dir); err != nil {
		return nil, fmt.Errorf("failed to sync directory after meta file creation: %w", err)
	}

	// Open heap file with direct I/O flag
	openFlags := os.O_RDWR | os.O_CREATE | DirectIOOpenFlag()
	heapFile, err := options.FS.OpenFile(heapPath, openFlags, 0644)
	if err != nil {
		if DirectIOOpenFlag() != 0 {
			klog.Warningf("diskstore: heap file failed to open with direct I/O: %v, falling back to buffered I/O", err)
			GlobalMetrics.DirectIOFallbackCount.Add(1)
		}
		// Fallback without O_DIRECT if filesystem doesn't support it (e.g. tmpfs)
		heapFile, err = options.FS.OpenFile(heapPath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open heap file: %w", err)
		}
	}

	// Apply platform specific flags (e.g. F_NOCACHE on Darwin)
	conn, connErr := heapFile.SyscallConn()
	if connErr == nil && conn != nil {
		if rawConn, ok := conn.(interface{ Control(func(uintptr)) error }); ok {
			var ctrlErr error
			if err := rawConn.Control(func(fd uintptr) {
				ctrlErr = applyPlatformDirectIOFlags(fd)
			}); err != nil {
				klog.V(4).ErrorS(err, "failed to apply direct I/O control")
			} else if ctrlErr != nil {
				klog.V(4).ErrorS(ctrlErr, "failed to apply platform direct I/O flags")
			}
		}
	}

	s := &DiskStore{
		fs:                      options.FS,
		dir:                     dir,
		dbID:                    dbID,
		isTemp:                  isTemp,
		autoDelete:              options.AutoDelete,
		cipher:                  options.Cipher,
		codec:                   options.Codec,
		heapFile:                heapFile,
		keyIndex:                make(map[string]KeyIndexEntry),
		oversizeObjects:         make(map[string][]byte),
		emptyLogicalPages:       make([]uint64, 0),
		lastRecoveredRVByBucket: make(map[string]string),
		nextLogicalPage:         1,
	}

	if isNewStore {
		wal, err := OpenWAL(options.FS, walDir, dbID, 0, WALOptions{
			OnFailure: s.setFailed,
			Clock:     options.Clock,
		})
		if err != nil {
			closeErr := heapFile.Close()
			return nil, errors.Join(fmt.Errorf("failed to open WAL: %w", err), closeErr)
		}
		s.wal = wal
		s.buddy = NewBuddyAllocator(512)
		s.bp = NewBufferPool(heapFile, wal, s.buddy, dbID, BufferPoolOptions{
			NumFrames: options.NumFrames,
			Cipher:    options.Cipher,
			OnFailure: s.setFailed,
		})
		s.cp = NewCheckpointer(s.bp, heapFile, wal)
	} else {
		// Existing store: perform crash recovery scan and WAL redo replay
		recoveryRes, err := Recover(options.FS, heapFile, walDir, dbID)
		if err != nil {
			closeErr := heapFile.Close()
			return nil, errors.Join(fmt.Errorf("recovery failed: %w", err), closeErr)
		}

		s.buddy = recoveryRes.Buddy
		s.keyIndex = recoveryRes.KeyIndex
		s.emptyLogicalPages = recoveryRes.EmptyLogicalPages
		s.lastRecoveredRV = recoveryRes.LastResourceVersion
		if recoveryRes.LastResourceVersionByBucket != nil {
			s.lastRecoveredRVByBucket = recoveryRes.LastResourceVersionByBucket
		}
		s.nextLogicalPage = recoveryRes.NextLogicalPage
		if s.nextLogicalPage == 0 {
			s.nextLogicalPage = 1
		}

		// Item 1: LSNs continue from recovered maximum, new segment starts at maxLSN + 1
		wal, err := OpenWAL(options.FS, walDir, dbID, recoveryRes.MaxLSN, WALOptions{
			OnFailure: s.setFailed,
			Clock:     options.Clock,
		})
		if err != nil {
			closeErr := heapFile.Close()
			return nil, errors.Join(fmt.Errorf("failed to open WAL: %w", err), closeErr)
		}
		s.wal = wal

		s.bp = NewBufferPool(heapFile, wal, s.buddy, dbID, BufferPoolOptions{
			NumFrames: options.NumFrames,
			Cipher:    options.Cipher,
			OnFailure: s.setFailed,
		})
		s.bp.LoadPageDescs(recoveryRes.PageDescs)
		s.cp = NewCheckpointer(s.bp, heapFile, wal)

		// Item 17: Load persisted oversize objects
		if err := s.loadOversizeObjects(); err != nil {
			closeErr := heapFile.Close()
			return nil, errors.Join(fmt.Errorf("failed to load oversize objects: %w", err), closeErr)
		}
	}

	return s, nil
}

const (
	// OversizeMagic identifies valid oversize object files ("OVRS").
	OversizeMagic uint32 = 0x4F565253

	// OversizeVersion is 1.
	OversizeVersion uint32 = 1

	// OversizeHeaderSize is 32 bytes.
	OversizeHeaderSize = 32
)

func oversizeFileName(qKey string) string {
	h := sha256.Sum256([]byte(qKey))
	return fmt.Sprintf("%x.obj", h)
}

func (s *DiskStore) oversizeFilePath(qKey string) string {
	return filepath.Join(s.dir, "oversize", oversizeFileName(qKey))
}

func (s *DiskStore) removeOversizeFile(qKey string) error {
	p := s.oversizeFilePath(qKey)
	if err := s.fs.Remove(p); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove oversize file %s: %w", p, err)
	}
	if err := s.fs.SyncDir(filepath.Join(s.dir, "oversize")); err != nil {
		return fmt.Errorf("failed to sync oversize dir: %w", err)
	}
	return nil
}

func encodeOversizeFile(qKey string, lsn uint64, payload []byte) []byte {
	keyBytes := []byte(qKey)
	totalLen := OversizeHeaderSize + len(keyBytes) + len(payload)
	buf := make([]byte, totalLen)

	binary.BigEndian.PutUint32(buf[0:4], OversizeMagic)
	binary.BigEndian.PutUint32(buf[4:8], OversizeVersion)
	binary.BigEndian.PutUint64(buf[8:16], lsn)
	// bytes 16..20 zeroed for CRC
	binary.BigEndian.PutUint32(buf[20:24], uint32(len(keyBytes)))
	binary.BigEndian.PutUint32(buf[24:28], uint32(len(payload)))

	copy(buf[OversizeHeaderSize:], keyBytes)
	copy(buf[OversizeHeaderSize+len(keyBytes):], payload)

	crc := crc32.Checksum(buf, castagnoliTable)
	binary.BigEndian.PutUint32(buf[16:20], crc)
	return buf
}

func decodeOversizeFile(data []byte) (string, uint64, []byte, error) {
	if len(data) < OversizeHeaderSize {
		return "", 0, nil, errors.New("oversize file too small")
	}
	magic := binary.BigEndian.Uint32(data[0:4])
	ver := binary.BigEndian.Uint32(data[4:8])
	if magic != OversizeMagic || ver != OversizeVersion {
		return "", 0, nil, fmt.Errorf("invalid oversize magic or version: magic 0x%08x, ver %d", magic, ver)
	}
	lsn := binary.BigEndian.Uint64(data[8:16])
	storedCRC := binary.BigEndian.Uint32(data[16:20])
	keyLen := binary.BigEndian.Uint32(data[20:24])
	payloadLen := binary.BigEndian.Uint32(data[24:28])

	if int(OversizeHeaderSize)+int(keyLen)+int(payloadLen) != len(data) {
		return "", 0, nil, fmt.Errorf("oversize file size mismatch: header specifies %d bytes, file is %d bytes", int(OversizeHeaderSize)+int(keyLen)+int(payloadLen), len(data))
	}

	// Verify CRC over bytes with CRC zeroed
	var zeroCRC [4]byte
	crc := crc32.Update(0, castagnoliTable, data[:16])
	crc = crc32.Update(crc, castagnoliTable, zeroCRC[:])
	actualCRC := crc32.Update(crc, castagnoliTable, data[20:])
	if storedCRC != actualCRC {
		return "", 0, nil, ErrBlockCRCMismatch
	}

	qKey := string(data[OversizeHeaderSize : OversizeHeaderSize+keyLen])
	payload := data[OversizeHeaderSize+keyLen:]
	return qKey, lsn, payload, nil
}

func (s *DiskStore) writeOversizeFile(qKey string, lsn uint64, payload []byte) error {
	oversizeDir := filepath.Join(s.dir, "oversize")
	finalPath := s.oversizeFilePath(qKey)
	tmpName := fmt.Sprintf(".tmp-%d-%s", lsn, oversizeFileName(qKey))
	tmpPath := filepath.Join(oversizeDir, tmpName)

	fileData := encodeOversizeFile(qKey, lsn, payload)

	of, err := s.fs.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create temp oversize file: %w", err)
	}

	if _, err := of.WriteAt(fileData, 0); err != nil {
		closeErr := of.Close()
		remErr := s.fs.Remove(tmpPath)
		return errors.Join(fmt.Errorf("failed to write temp oversize file: %w", err), closeErr, remErr)
	}

	if err := of.Sync(); err != nil {
		closeErr := of.Close()
		remErr := s.fs.Remove(tmpPath)
		return errors.Join(fmt.Errorf("failed to sync temp oversize file: %w", err), closeErr, remErr)
	}

	if err := of.Close(); err != nil {
		remErr := s.fs.Remove(tmpPath)
		return errors.Join(fmt.Errorf("failed to close temp oversize file: %w", err), remErr)
	}

	if err := s.fs.Rename(tmpPath, finalPath); err != nil {
		remErr := s.fs.Remove(tmpPath)
		return errors.Join(fmt.Errorf("failed to rename temp oversize file: %w", err), remErr)
	}

	if err := s.fs.SyncDir(oversizeDir); err != nil {
		return fmt.Errorf("failed to sync oversize dir: %w", err)
	}
	return nil
}

func (s *DiskStore) loadOversizeObjects() error {
	oversizeDir := filepath.Join(s.dir, "oversize")
	entries, err := s.fs.ReadDir(oversizeDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		filePath := filepath.Join(oversizeDir, name)

		if strings.HasPrefix(name, ".tmp-") {
			// Clean up abandoned temporary files from previous crashes
			if remErr := s.fs.Remove(filePath); remErr != nil {
				klog.V(4).ErrorS(remErr, "failed to remove abandoned temp oversize file", "file", filePath)
			}
			continue
		}

		f, err := s.fs.Open(filePath)
		if err != nil {
			return fmt.Errorf("failed to open oversize file %s: %w", name, err)
		}
		info, err := entry.Info()
		if err != nil {
			if closeErr := f.Close(); closeErr != nil {
				klog.V(4).ErrorS(closeErr, "failed to close oversize file", "file", filePath)
			}
			return fmt.Errorf("failed to stat oversize file %s: %w", name, err)
		}
		data := make([]byte, info.Size())
		if _, rErr := f.ReadAt(data, 0); rErr != nil {
			if closeErr := f.Close(); closeErr != nil {
				klog.V(4).ErrorS(closeErr, "failed to close unreadable oversize file", "file", filePath)
			}
			return fmt.Errorf("failed to read oversize file %s: %w", name, rErr)
		}
		if cErr := f.Close(); cErr != nil {
			return fmt.Errorf("failed to close oversize file %s: %w", name, cErr)
		}

		qKey, lsn, payload, err := decodeOversizeFile(data)
		if err != nil {
			return fmt.Errorf("corrupt oversize file %s: %w", name, err)
		}

		// Item 2: Loading must respect LSNs
		if existing, ok := s.keyIndex[qKey]; ok {
			if !existing.IsOversize || existing.LSN > lsn {
				// Object was updated to a normal-sized object or newer version; file is stale
				if remErr := s.fs.Remove(filePath); remErr != nil {
					return fmt.Errorf("failed to remove stale oversize file %s: %w", filePath, remErr)
				}
				if syncErr := s.fs.SyncDir(oversizeDir); syncErr != nil {
					return fmt.Errorf("failed to sync oversize dir: %w", syncErr)
				}
				continue
			}
		}

		s.oversizeObjects[qKey] = payload
		s.keyIndex[qKey] = KeyIndexEntry{
			LogicalPage: 0,
			Slot:        0,
			LSN:         lsn,
			IsOversize:  true,
		}
		GlobalMetrics.OversizeObjectsCount.Add(1)
	}
	return nil
}

// setFailed transitions the store into the permanent failed state (Item 18).
func (s *DiskStore) setFailed(err error) {
	if err == nil {
		return
	}
	s.failMu.Lock()
	defer s.failMu.Unlock()
	if !s.failed.Load() {
		s.failedErr = fmt.Errorf("%w: %v", ErrStoreFailed, err)
		s.failed.Store(true)
	}
}

func (s *DiskStore) isFailed() bool {
	return s.failed.Load()
}

func (s *DiskStore) failErr() error {
	s.failMu.RLock()
	defer s.failMu.RUnlock()
	if s.failedErr != nil {
		return s.failedErr
	}
	return ErrStoreFailed
}

// LastRecoveredResourceVersion returns the resourceVersion of the last record processed during recovery.
// If bucketName is specified, it returns the recovered resourceVersion for that bucket.
func (s *DiskStore) LastRecoveredResourceVersion(bucketName ...string) string {
	s.keyMu.RLock()
	defer s.keyMu.RUnlock()
	if len(bucketName) > 0 && bucketName[0] != "" {
		if rv, ok := s.lastRecoveredRVByBucket[bucketName[0]]; ok && rv != "" {
			return rv
		}
	}
	return s.lastRecoveredRV
}

// RecordBookmark records a watch bookmark in the WAL without creating a key/value object.
func (s *DiskStore) RecordBookmark(bucketName, rv string) error {
	if s.isFailed() {
		return s.failErr()
	}

	rec := Record{
		Type:            RecordTypeBookmark,
		Key:             qualifyKey(bucketName, ""),
		ResourceVersion: rv,
	}
	if _, err := s.wal.AppendRecord(&rec); err != nil {
		s.setFailed(err)
		return s.failErr()
	}

	s.keyMu.Lock()
	s.lastRecoveredRVByBucket[bucketName] = rv
	s.keyMu.Unlock()
	return nil
}

// DBID returns the store's unique database ID.
func (s *DiskStore) DBID() [16]byte {
	return s.dbID
}

// Cipher returns the configured cipher.
func (s *DiskStore) Cipher() Cipher {
	return s.cipher
}

// Codec returns the configured codec.
func (s *DiskStore) Codec() ObjectCodec {
	return s.codec
}

// Path returns the store directory.
func (s *DiskStore) Path() string {
	return s.dir
}

// Put writes an object to the store.
func (s *DiskStore) Put(bucketName, key string, payload []byte, rv string) error {
	if s.isFailed() {
		return s.failErr()
	}

	s.writerMu.Lock()
	defer s.writerMu.Unlock()

	qKey := qualifyKey(bucketName, key)

	s.keyMu.RLock()
	existing, inIndex := s.keyIndex[qKey]
	s.keyMu.RUnlock()

	// Append record to WAL, assigning LSN under WAL lock
	rec := Record{
		Type:            RecordTypePut,
		Key:             qKey,
		Payload:         payload,
		ResourceVersion: rv,
	}
	lsn, err := s.wal.AppendRecord(&rec)
	if err != nil {
		s.setFailed(err)
		return s.failErr()
	}

	// Handle oversize object (> 2 MiB): persist to disk under oversize directory (Item 17)
	if len(payload) > MaxPayloadCapacity {
		if inIndex {
			if !existing.IsOversize {
				if existing.NumBlocks == 1 {
					if err := s.deleteSlotFromPageLocked(existing.LogicalPage, existing.Slot, lsn); err != nil {
						s.setFailed(err)
						return s.failErr()
					}
				} else {
					if err := s.clearLogicalPageLocked(existing.LogicalPage, lsn); err != nil {
						s.setFailed(err)
						return s.failErr()
					}
				}
			}
		}

		// Persist oversize file atomically
		if err := s.writeOversizeFile(qKey, lsn, payload); err != nil {
			s.setFailed(err)
			return s.failErr()
		}

		s.keyMu.Lock()
		s.oversizeObjects[qKey] = bytes.Clone(payload)
		s.keyIndex[qKey] = KeyIndexEntry{
			LogicalPage: 0,
			Slot:        0,
			LSN:         lsn,
			IsOversize:  true,
		}
		s.keyMu.Unlock()

		if !inIndex || !existing.IsOversize {
			GlobalMetrics.OversizeObjectsCount.Add(1)
		}
		return nil
	}

	// If transitioning from oversize to normal-sized object (Item 2)
	if inIndex && existing.IsOversize {
		if err := s.removeOversizeFile(qKey); err != nil {
			s.setFailed(err)
			return s.failErr()
		}
		s.keyMu.Lock()
		delete(s.oversizeObjects, qKey)
		s.keyMu.Unlock()
		GlobalMetrics.OversizeObjectsCount.Add(-1)
	}

	// Check if this object fits in a 1-block slotted page
	if len(payload) < BlockUsableSize-64 {
		// Use or allocate a slotted page
		var targetPage uint64
		if inIndex && !existing.IsOversize && existing.NumBlocks == 1 {
			targetPage = existing.LogicalPage
		} else if s.activeSlottedPage != 0 {
			targetPage = s.activeSlottedPage
		} else if len(s.emptyLogicalPages) > 0 {
			targetPage = s.emptyLogicalPages[len(s.emptyLogicalPages)-1]
			s.emptyLogicalPages = s.emptyLogicalPages[:len(s.emptyLogicalPages)-1]
			s.activeSlottedPage = targetPage
		} else {
			targetPage = s.nextLogicalPage
			s.nextLogicalPage++
			s.activeSlottedPage = targetPage
		}

		handle, err := s.bp.PinPage(targetPage)
		if err != nil {
			handle, err = s.bp.AllocateNewPage(targetPage, 1, lsn)
			if err != nil {
				s.setFailed(err)
				return s.failErr()
			}
		}

		handle.Desc.Mu.Lock()
		slotted, err := WrapSlottedPage(handle.Block(0))
		if err != nil {
			handle.Desc.Mu.Unlock()
			handle.Unpin()
			s.setFailed(err)
			return s.failErr()
		}

		slotIdx, err := slotted.Put(qKey, payload, lsn)
		if err == nil {
			s.keyMu.Lock()
			s.keyIndex[qKey] = KeyIndexEntry{
				LogicalPage: targetPage,
				Slot:        slotIdx,
				LSN:         lsn,
				NumBlocks:   1,
			}
			s.keyMu.Unlock()

			if inIndex && !existing.IsOversize && targetPage == existing.LogicalPage && slotIdx != existing.Slot {
				if delErr := slotted.Delete(existing.Slot, lsn); delErr != nil && !errors.Is(delErr, ErrSlotNotFound) {
					handle.Desc.Mu.Unlock()
					handle.Unpin()
					s.setFailed(delErr)
					return s.failErr()
				}
			}
		} else {
			// Page is full; allocate a fresh slotted page and update activeSlottedPage
			handle.Desc.Mu.Unlock()
			handle.Unpin()

			// If old record was on another page, delete it from old page
			if inIndex && !existing.IsOversize && existing.NumBlocks == 1 {
				if err := s.deleteSlotFromPageLocked(existing.LogicalPage, existing.Slot, lsn); err != nil {
					s.setFailed(err)
					return s.failErr()
				}
			} else if inIndex && !existing.IsOversize && existing.NumBlocks > 1 {
				if err := s.clearLogicalPageLocked(existing.LogicalPage, lsn); err != nil {
					s.setFailed(err)
					return s.failErr()
				}
			}

			if len(s.emptyLogicalPages) > 0 {
				targetPage = s.emptyLogicalPages[len(s.emptyLogicalPages)-1]
				s.emptyLogicalPages = s.emptyLogicalPages[:len(s.emptyLogicalPages)-1]
			} else {
				targetPage = s.nextLogicalPage
				s.nextLogicalPage++
			}
			s.activeSlottedPage = targetPage
			handle, err = s.bp.AllocateNewPage(targetPage, 1, lsn)
			if err != nil {
				s.setFailed(err)
				return s.failErr()
			}
			handle.Desc.Mu.Lock()
			slotted, err = WrapSlottedPage(handle.Block(0))
			if err != nil {
				handle.Desc.Mu.Unlock()
				handle.Unpin()
				s.setFailed(err)
				return s.failErr()
			}
			slotIdx, err = slotted.Put(qKey, payload, lsn)
			if err != nil {
				handle.Desc.Mu.Unlock()
				handle.Unpin()
				s.setFailed(err)
				return s.failErr()
			}
		}

		slotted.Finalize()
		handle.Desc.Mu.Unlock()
		s.bp.MarkDirty(targetPage, lsn)
		handle.Unpin()

		s.keyMu.Lock()
		s.keyIndex[qKey] = KeyIndexEntry{
			LogicalPage: targetPage,
			Slot:        slotIdx,
			LSN:         lsn,
			NumBlocks:   1,
		}
		s.keyMu.Unlock()
	} else {
		// Multi-block page
		numBlocks, err := CalculateMultiBlockCount(len(payload))
		if err != nil {
			s.setFailed(err)
			return s.failErr()
		}

		// If replacing an existing slotted or multi-block record, clean up old location (Item 15)
		if inIndex {
			if !existing.IsOversize && existing.NumBlocks == 1 {
				if err := s.deleteSlotFromPageLocked(existing.LogicalPage, existing.Slot, lsn); err != nil {
					s.setFailed(err)
					return s.failErr()
				}
			} else if !existing.IsOversize && existing.NumBlocks > 1 {
				if err := s.clearLogicalPageLocked(existing.LogicalPage, lsn); err != nil {
					s.setFailed(err)
					return s.failErr()
				}
			}
		}

		var targetPage uint64
		if len(s.emptyLogicalPages) > 0 {
			targetPage = s.emptyLogicalPages[len(s.emptyLogicalPages)-1]
			s.emptyLogicalPages = s.emptyLogicalPages[:len(s.emptyLogicalPages)-1]
		} else {
			targetPage = s.nextLogicalPage
			s.nextLogicalPage++
		}

		mbPage, err := NewMultiBlockPage(targetPage, lsn, qKey, payload)
		if err != nil {
			s.setFailed(err)
			return s.failErr()
		}

		handle, err := s.bp.AllocateNewPage(targetPage, numBlocks, lsn)
		if err != nil {
			s.setFailed(err)
			return s.failErr()
		}

		handle.Desc.Mu.Lock()
		for i, b := range mbPage.Blocks {
			FinalizeBlockCRC(b)
			copy(handle.Block(i), b)
		}
		handle.Desc.Mu.Unlock()
		s.bp.MarkDirty(targetPage, lsn)
		handle.Unpin()

		s.keyMu.Lock()
		s.keyIndex[qKey] = KeyIndexEntry{
			LogicalPage: targetPage,
			Slot:        0,
			LSN:         lsn,
			NumBlocks:   numBlocks,
		}
		s.keyMu.Unlock()
	}

	return nil
}

func (s *DiskStore) deleteSlotFromPageLocked(pageID uint64, slotIdx uint32, lsn uint64) error {
	handle, err := s.bp.PinPage(pageID)
	if err != nil {
		return fmt.Errorf("failed to pin page %d for slot deletion: %w", pageID, err)
	}
	handle.Desc.Mu.Lock()
	slotted, wrapErr := WrapSlottedPage(handle.Block(0))
	if wrapErr != nil {
		handle.Desc.Mu.Unlock()
		handle.Unpin()
		return fmt.Errorf("failed to wrap page %d: %w", pageID, wrapErr)
	}
	if delErr := slotted.Delete(slotIdx, lsn); delErr != nil && !errors.Is(delErr, ErrSlotNotFound) {
		handle.Desc.Mu.Unlock()
		handle.Unpin()
		return fmt.Errorf("failed to delete slot %d from page %d: %w", slotIdx, pageID, delErr)
	}
	slotted.Finalize()
	if slotted.ActiveSlotsCount() == 0 {
		s.emptyLogicalPages = append(s.emptyLogicalPages, pageID)
		if s.activeSlottedPage == pageID {
			s.activeSlottedPage = 0
		}
	}
	handle.Desc.Mu.Unlock()
	s.bp.MarkDirty(pageID, lsn)
	handle.Unpin()
	return nil
}

// clearLogicalPageLocked writes a tombstone to clear a superseded or deleted multi-block page (Items 15, 16).
func (s *DiskStore) clearLogicalPageLocked(logicalPage uint64, lsn uint64) error {
	handle, err := s.bp.PinPage(logicalPage)
	if err != nil {
		return fmt.Errorf("failed to pin multi-block page %d for clearing: %w", logicalPage, err)
	}
	handle.Desc.Mu.Lock()
	tombstone := make([]byte, BlockSize)
	hdr := BlockHeader{LogicalPage: logicalPage, LSN: lsn, NumBlocks: 1}
	EncodeBlockHeader(hdr, tombstone)
	FinalizeBlockCRC(tombstone)
	copy(handle.Block(0), tombstone)
	handle.Desc.Mu.Unlock()
	s.bp.MarkDirty(logicalPage, lsn)
	handle.Unpin()
	s.emptyLogicalPages = append(s.emptyLogicalPages, logicalPage)
	return nil
}

// Get retrieves an object by key, validating expected key and LSN.
// Supports optimistic retry on concurrent updates (Item 8) and locks in-flight payload clone (Item 9).
func (s *DiskStore) Get(bucketName, key string) ([]byte, bool, error) {
	if s.isFailed() {
		return nil, false, s.failErr()
	}

	qKey := qualifyKey(bucketName, key)

	for attempt := 0; attempt < 5; attempt++ {
		s.keyMu.RLock()
		entry, exists := s.keyIndex[qKey]
		s.keyMu.RUnlock()

		if !exists {
			return nil, false, nil
		}

		if entry.IsOversize {
			s.keyMu.RLock()
			data, ok := s.oversizeObjects[qKey]
			s.keyMu.RUnlock()
			if !ok {
				return nil, false, nil
			}
			return bytes.Clone(data), true, nil
		}

		handle, err := s.bp.PinPage(entry.LogicalPage)
		if err != nil {
			s.keyMu.RLock()
			curEntry, stillExists := s.keyIndex[qKey]
			s.keyMu.RUnlock()
			if !stillExists || curEntry.LSN != entry.LSN || curEntry.LogicalPage != entry.LogicalPage {
				continue // Concurrent modification, retry!
			}
			s.setFailed(fmt.Errorf("failed to pin page %d for key %s: %w", entry.LogicalPage, key, err))
			return nil, false, s.failErr()
		}

		handle.Desc.Mu.RLock()
		pageLSN := handle.Desc.PageLSN.Load()
		var recKey string
		var recPayload []byte
		var recLSN uint64

		if handle.Desc.NumBlocks == 1 {
			slotted, wrapErr := WrapSlottedPage(handle.Block(0))
			if wrapErr == nil {
				var raw []byte
				recKey, raw, recLSN, err = slotted.Get(entry.Slot)
				if err == nil {
					recPayload = bytes.Clone(raw) // Item 9: clone payload while holding RLock!
				}
			} else {
				err = wrapErr
			}
		} else {
			var raw []byte
			recKey, raw, recLSN, err = DecodeMultiBlockPage(handle.Blocks())
			if err == nil {
				recPayload = bytes.Clone(raw) // Item 9: clone payload while holding RLock!
			}
		}
		handle.Desc.Mu.RUnlock()
		handle.Unpin()

		// Verify key and LSN
		if err != nil || recKey != qKey || recLSN != entry.LSN {
			s.keyMu.RLock()
			curEntry, stillExists := s.keyIndex[qKey]
			s.keyMu.RUnlock()
			if !stillExists || curEntry.LSN != entry.LSN || curEntry.LogicalPage != entry.LogicalPage || pageLSN > entry.LSN || attempt < 4 {
				runtime.Gosched()
				continue // Concurrent modification, retry!
			}
			s.setFailed(fmt.Errorf("corruption/stale read detected for %s: expected LSN %d, got LSN %d (recKey=%s, err=%v)",
				key, entry.LSN, recLSN, recKey, err))
			return nil, false, s.failErr()
		}

		return recPayload, true, nil
	}

	return nil, false, errors.New("read conflict retry limit reached")
}

// Delete removes an object by key.
func (s *DiskStore) Delete(bucketName, key string) error {
	if s.isFailed() {
		return s.failErr()
	}

	s.writerMu.Lock()
	defer s.writerMu.Unlock()

	qKey := qualifyKey(bucketName, key)

	s.keyMu.Lock()
	entry, exists := s.keyIndex[qKey]
	if !exists {
		s.keyMu.Unlock()
		return nil
	}
	delete(s.keyIndex, qKey)
	if entry.IsOversize {
		delete(s.oversizeObjects, qKey)
		GlobalMetrics.OversizeObjectsCount.Add(-1)
	}
	s.keyMu.Unlock()

	rec := Record{
		Type: RecordTypeDelete,
		Key:  qKey,
	}
	lsn, err := s.wal.AppendRecord(&rec)
	if err != nil {
		s.setFailed(err)
		return s.failErr()
	}

	if entry.IsOversize {
		if err := s.removeOversizeFile(qKey); err != nil {
			s.setFailed(err)
			return s.failErr()
		}
	} else {
		if entry.NumBlocks == 1 {
			if err := s.deleteSlotFromPageLocked(entry.LogicalPage, entry.Slot, lsn); err != nil {
				s.setFailed(err)
				return s.failErr()
			}
		} else {
			// Item 16: When deleting a multi-block object, write an empty tombstone page
			if err := s.clearLogicalPageLocked(entry.LogicalPage, lsn); err != nil {
				s.setFailed(err)
				return s.failErr()
			}
		}
	}

	return nil
}

// ListKeys returns all keys currently present in the key index for a specific bucket.
func (s *DiskStore) ListKeys() []string {
	if s.isFailed() {
		return nil
	}

	s.keyMu.RLock()
	defer s.keyMu.RUnlock()

	keys := make([]string, 0, len(s.keyIndex))
	for k := range s.keyIndex {
		_, userKey := unqualifyKey(k)
		keys = append(keys, userKey)
	}
	return keys
}

// RecordItem holds a key and payload returned from List().
type RecordItem struct {
	Key     string
	Payload []byte
}

// List returns a point-in-time consistent view of all objects in the bucket (Item 9, 10, 19).
func (s *DiskStore) List(bucketName string) ([]RecordItem, error) {
	if s.isFailed() {
		return nil, s.failErr()
	}

	// Hold writerMu read lock to ensure point-in-time consistency: no writes/deletes can mutate
	// pages or index underneath this read (Item 9)
	s.writerMu.RLock()
	defer s.writerMu.RUnlock()

	s.keyMu.RLock()
	prefix := bucketName + "\x00"
	snapshot := make(map[string]KeyIndexEntry)
	for qKey, entry := range s.keyIndex {
		if strings.HasPrefix(qKey, prefix) {
			snapshot[qKey] = entry
		}
	}
	s.keyMu.RUnlock()

	var items []RecordItem
	for qKey := range snapshot {
		_, userKey := unqualifyKey(qKey)
		payload, exists, err := s.Get(bucketName, userKey)
		if err != nil {
			return nil, err
		}
		if exists {
			items = append(items, RecordItem{Key: userKey, Payload: payload})
		}
	}
	return items, nil
}

// Sync ensures that all appended WAL records are flushed to disk and fsynced.
func (s *DiskStore) Sync() error {
	if s.isFailed() {
		return s.failErr()
	}
	if err := s.wal.Sync(); err != nil {
		s.setFailed(err)
		return s.failErr()
	}
	return nil
}

// Flush flushes all buffered WAL records to disk and fsynced.
func (s *DiskStore) Flush() error {
	if s.isFailed() {
		return s.failErr()
	}
	if err := s.wal.Flush(); err != nil {
		s.setFailed(err)
		return s.failErr()
	}
	return nil
}

// Checkpoint performs a non-blocking checkpoint (Item 7).
func (s *DiskStore) Checkpoint() (uint64, error) {
	if s.isFailed() {
		return 0, s.failErr()
	}

	chkLSN, err := s.cp.Checkpoint()
	if err != nil {
		s.setFailed(err)
		return 0, s.failErr()
	}
	return chkLSN, nil
}

// Close flushes and shuts down store components.
func (s *DiskStore) Close() error {
	if s.closed.Swap(true) {
		return nil
	}

	var errs []error
	// If not failed, checkpoint on close to truncate WAL and speed up reopen
	if !s.isFailed() && s.cp != nil {
		if _, err := s.cp.Checkpoint(); err != nil {
			errs = append(errs, err)
		}
	}

	if s.bp != nil {
		if err := s.bp.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.heapFile != nil {
		if err := s.heapFile.Sync(); err != nil {
			errs = append(errs, err)
		}
		if err := s.heapFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.wal != nil {
		if err := s.wal.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if s.autoDelete {
		if err := os.RemoveAll(s.dir); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
