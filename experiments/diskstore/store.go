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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

// StoreOptions configures a DiskStore instance.
type StoreOptions struct {
	Dir        string
	FS         FS
	Cipher     Cipher
	Codec      ObjectCodec
	NumFrames  int
	AutoDelete bool
}

// StoreOption is a functional option for configuring a DiskStore.
type StoreOption func(*StoreOptions)

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

	writerMu sync.Mutex // Single writer

	keyMu           sync.RWMutex
	keyIndex        map[string]KeyIndexEntry
	oversizeObjects map[string][]byte

	emptyLogicalPages []uint64
	nextLogicalPage   uint64
	activeSlottedPage uint64

	failed    atomic.Bool
	failedErr error
	failMu    sync.RWMutex

	lastRecoveredRV string
	closed          atomic.Bool
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

	metaPath := filepath.Join(dir, "meta.db")
	heapPath := filepath.Join(dir, "heap.db")
	walDir := filepath.Join(dir, "wal")

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

	// Open heap file with direct I/O flag
	openFlags := os.O_RDWR | os.O_CREATE | DirectIOOpenFlag()
	heapFile, err := options.FS.OpenFile(heapPath, openFlags, 0644)
	if err != nil {
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

	wal, err := OpenWAL(options.FS, walDir, dbID, 0)
	if err != nil {
		if closeErr := heapFile.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	s := &DiskStore{
		fs:                options.FS,
		dir:               dir,
		dbID:              dbID,
		isTemp:            isTemp,
		autoDelete:        options.AutoDelete,
		cipher:            options.Cipher,
		codec:             options.Codec,
		heapFile:          heapFile,
		wal:               wal,
		keyIndex:          make(map[string]KeyIndexEntry),
		oversizeObjects:   make(map[string][]byte),
		emptyLogicalPages: make([]uint64, 0),
		nextLogicalPage:   1,
	}

	if isNewStore {
		s.buddy = NewBuddyAllocator(512)
		s.bp = NewBufferPool(heapFile, wal, s.buddy, dbID, BufferPoolOptions{NumFrames: options.NumFrames})
		s.cp = NewCheckpointer(s.bp, heapFile, wal)
	} else {
		// Existing store: perform crash recovery scan and WAL redo replay
		recoveryRes, err := Recover(options.FS, heapFile, walDir, dbID)
		if err != nil {
			closeErr1 := heapFile.Close()
			closeErr2 := wal.Close()
			return nil, errors.Join(fmt.Errorf("recovery failed: %w", err), closeErr1, closeErr2)
		}

		s.buddy = recoveryRes.Buddy
		s.keyIndex = recoveryRes.KeyIndex
		s.emptyLogicalPages = recoveryRes.EmptyLogicalPages
		s.lastRecoveredRV = recoveryRes.LastResourceVersion
		s.nextLogicalPage = recoveryRes.NextLogicalPage
		if s.nextLogicalPage == 0 {
			s.nextLogicalPage = 1
		}

		s.bp = NewBufferPool(heapFile, wal, s.buddy, dbID, BufferPoolOptions{NumFrames: options.NumFrames})
		s.bp.LoadPageDescs(recoveryRes.PageDescs)
		s.cp = NewCheckpointer(s.bp, heapFile, wal)
	}

	return s, nil
}

// setFailed transitions the store into the permanent failed state.
func (s *DiskStore) setFailed(err error) {
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
func (s *DiskStore) LastRecoveredResourceVersion() string {
	return s.lastRecoveredRV
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

	lsn := s.wal.NextLSN()

	// Handle oversize object (> 2 MiB): kept in memory only and counted in metric
	if len(payload) > MaxPayloadCapacity {
		GlobalMetrics.OversizeObjectsCount.Add(1)
		s.keyMu.Lock()
		s.oversizeObjects[key] = bytes.Clone(payload)
		s.keyIndex[key] = KeyIndexEntry{
			LogicalPage: 0,
			Slot:        0,
			LSN:         lsn,
			IsOversize:  true,
		}
		s.keyMu.Unlock()

		// Append to WAL
		rec := Record{
			Type:            RecordTypePut,
			LSN:             lsn,
			Key:             key,
			Payload:         payload,
			ResourceVersion: rv,
		}
		if err := s.wal.Append(rec); err != nil {
			s.setFailed(err)
			return s.failErr()
		}
		return nil
	}

	// Append record to WAL
	rec := Record{
		Type:            RecordTypePut,
		LSN:             lsn,
		Key:             key,
		Payload:         payload,
		ResourceVersion: rv,
	}
	if err := s.wal.Append(rec); err != nil {
		s.setFailed(err)
		return s.failErr()
	}

	// Check if this object fits in a 1-block slotted page
	if len(payload) < BlockUsableSize-64 {
		// Use or allocate a slotted page
		var targetPage uint64
		s.keyMu.RLock()
		existing, inIndex := s.keyIndex[key]
		s.keyMu.RUnlock()

		if inIndex && !existing.IsOversize {
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

		slotIdx, err := slotted.Put(key, payload, lsn)
		if err == nil {
			if inIndex && targetPage == existing.LogicalPage && slotIdx != existing.Slot {
				if delErr := slotted.Delete(existing.Slot, lsn); delErr != nil {
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
			slotIdx, err = slotted.Put(key, payload, lsn)
			if err != nil {
				handle.Desc.Mu.Unlock()
				handle.Unpin()
				s.setFailed(err)
				return s.failErr()
			}
		}

		slotted.Finalize()
		s.bp.MarkDirty(targetPage, lsn)
		handle.Desc.Mu.Unlock()
		handle.Unpin()

		s.keyMu.Lock()
		s.keyIndex[key] = KeyIndexEntry{
			LogicalPage: targetPage,
			Slot:        slotIdx,
			LSN:         lsn,
		}
		s.keyMu.Unlock()
	} else {
		// Multi-block page
		numBlocks, err := CalculateMultiBlockCount(len(payload))
		if err != nil {
			s.setFailed(err)
			return s.failErr()
		}

		var targetPage uint64
		if len(s.emptyLogicalPages) > 0 {
			targetPage = s.emptyLogicalPages[len(s.emptyLogicalPages)-1]
			s.emptyLogicalPages = s.emptyLogicalPages[:len(s.emptyLogicalPages)-1]
		} else {
			targetPage = s.nextLogicalPage
			s.nextLogicalPage++
		}

		mbPage, err := NewMultiBlockPage(targetPage, lsn, key, payload)
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
		// Copy generated blocks into buffer pool frames
		for i, b := range mbPage.Blocks {
			copy(handle.Block(i), b)
		}
		s.bp.MarkDirty(targetPage, lsn)
		handle.Desc.Mu.Unlock()
		handle.Unpin()

		s.keyMu.Lock()
		s.keyIndex[key] = KeyIndexEntry{
			LogicalPage: targetPage,
			Slot:        0,
			LSN:         lsn,
		}
		s.keyMu.Unlock()
	}

	return nil
}

// Get retrieves an object by key, validating expected key and LSN.
// Any mismatch is treated as corruption and puts the store into a failed state.
func (s *DiskStore) Get(bucketName, key string) ([]byte, bool, error) {
	if s.isFailed() {
		return nil, false, s.failErr()
	}

	s.keyMu.RLock()
	entry, exists := s.keyIndex[key]
	s.keyMu.RUnlock()

	if !exists {
		return nil, false, nil
	}

	if entry.IsOversize {
		s.keyMu.RLock()
		data, ok := s.oversizeObjects[key]
		s.keyMu.RUnlock()
		if !ok {
			return nil, false, nil
		}
		return bytes.Clone(data), true, nil
	}

	handle, err := s.bp.PinPage(entry.LogicalPage)
	if err != nil {
		s.setFailed(fmt.Errorf("failed to pin page %d for key %s: %w", entry.LogicalPage, key, err))
		return nil, false, s.failErr()
	}
	defer handle.Unpin()

	handle.Desc.Mu.RLock()
	var recKey string
	var recPayload []byte
	var recLSN uint64

	if handle.Desc.NumBlocks == 1 {
		slotted, wrapErr := WrapSlottedPage(handle.Block(0))
		if wrapErr != nil {
			handle.Desc.Mu.RUnlock()
			s.setFailed(wrapErr)
			return nil, false, s.failErr()
		}
		recKey, recPayload, recLSN, err = slotted.Get(entry.Slot)
	} else {
		recKey, recPayload, recLSN, err = DecodeMultiBlockPage(handle.Blocks())
	}
	handle.Desc.Mu.RUnlock()

	// Invariant: Verify key and LSN match expected. Treat mismatch as corruption.
	if err != nil || recKey != key || recLSN != entry.LSN {
		s.setFailed(fmt.Errorf("corruption/stale read detected for %s: expected LSN %d, got LSN %d (recKey=%s, err=%v)",
			key, entry.LSN, recLSN, recKey, err))
		return nil, false, s.failErr()
	}

	return bytes.Clone(recPayload), true, nil
}

// Delete removes an object by key.
func (s *DiskStore) Delete(bucketName, key string) error {
	if s.isFailed() {
		return s.failErr()
	}

	s.writerMu.Lock()
	defer s.writerMu.Unlock()

	s.keyMu.Lock()
	entry, exists := s.keyIndex[key]
	if !exists {
		s.keyMu.Unlock()
		return nil
	}
	delete(s.keyIndex, key)
	if entry.IsOversize {
		delete(s.oversizeObjects, key)
		GlobalMetrics.OversizeObjectsCount.Add(-1)
	}
	s.keyMu.Unlock()

	lsn := s.wal.NextLSN()
	rec := Record{
		Type: RecordTypeDelete,
		LSN:  lsn,
		Key:  key,
	}
	if err := s.wal.Append(rec); err != nil {
		s.setFailed(err)
		return s.failErr()
	}

	if !entry.IsOversize {
		handle, err := s.bp.PinPage(entry.LogicalPage)
		if err != nil {
			if !errors.Is(err, ErrPageNotFound) {
				s.setFailed(err)
				return s.failErr()
			}
		} else {
			defer handle.Unpin()
			handle.Desc.Mu.Lock()
			if handle.Desc.NumBlocks == 1 {
				slotted, err := WrapSlottedPage(handle.Block(0))
				if err != nil {
					handle.Desc.Mu.Unlock()
					s.setFailed(err)
					return s.failErr()
				}
				if err := slotted.Delete(entry.Slot, lsn); err != nil {
					handle.Desc.Mu.Unlock()
					s.setFailed(err)
					return s.failErr()
				}
				slotted.Finalize()
				s.bp.MarkDirty(entry.LogicalPage, lsn)
				if slotted.ActiveSlotsCount() == 0 {
					s.emptyLogicalPages = append(s.emptyLogicalPages, entry.LogicalPage)
					if s.activeSlottedPage == entry.LogicalPage {
						s.activeSlottedPage = 0
					}
				}
			} else {
				s.emptyLogicalPages = append(s.emptyLogicalPages, entry.LogicalPage)
			}
			handle.Desc.Mu.Unlock()
		}
	}

	return nil
}

// ListKeys returns all keys currently present in the key index.
func (s *DiskStore) ListKeys() []string {
	if s.isFailed() {
		return nil
	}

	s.keyMu.RLock()
	defer s.keyMu.RUnlock()

	keys := make([]string, 0, len(s.keyIndex))
	for k := range s.keyIndex {
		keys = append(keys, k)
	}
	return keys
}

// RecordItem holds a key and payload returned from List().
type RecordItem struct {
	Key     string
	Payload []byte
}

// List returns a point-in-time consistent view of all objects in the store.
func (s *DiskStore) List(bucketName string) ([]RecordItem, error) {
	if s.isFailed() {
		return nil, s.failErr()
	}

	// Capture consistent snapshot of key index
	s.keyMu.RLock()
	snapshot := make(map[string]KeyIndexEntry, len(s.keyIndex))
	for k, v := range s.keyIndex {
		snapshot[k] = v
	}
	s.keyMu.RUnlock()

	var items []RecordItem
	for key := range snapshot {
		payload, exists, err := s.Get(bucketName, key)
		if err != nil {
			return nil, err
		}
		if exists {
			items = append(items, RecordItem{Key: key, Payload: payload})
		}
	}
	return items, nil
}

// Checkpoint performs a checkpoint.
func (s *DiskStore) Checkpoint() (uint64, error) {
	if s.isFailed() {
		return 0, s.failErr()
	}
	return s.cp.Checkpoint()
}

// Close flushes and shuts down store components.
func (s *DiskStore) Close() error {
	if s.closed.Swap(true) {
		return nil
	}

	var errs []error
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
