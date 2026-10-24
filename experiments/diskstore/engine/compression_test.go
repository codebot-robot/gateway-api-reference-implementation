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

package engine

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

func makeRepetitivePayload(size int) []byte {
	pattern := []byte("ConfigMap data with structured fields: key=value, metadata=informer; ")
	b := make([]byte, size)
	for i := 0; i < size; i += len(pattern) {
		copy(b[i:], pattern)
	}
	return b
}

func makeRandomPayload(size int) []byte {
	b := make([]byte, size)
	_, _ = rand.Read(b)
	return b
}

// TestCompression_ThresholdAndRawFallback tests that values above threshold are stored compressed
// with codec and uncompressed length recorded, while incompressible values or values below threshold are stored raw.
func TestCompression_ThresholdAndRawFallback(t *testing.T) {
	memFS := NewMemFS()
	store, err := Open(WithFS(memFS), WithDir("test_compress_threshold"), WithCompressionCodec(CodecZstd))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// 1. Compressible value above threshold (8000 bytes > 4000)
	compressibleVal := makeRepetitivePayload(8000)
	if err := store.Put("ConfigMap", "cm-compressed", compressibleVal, "1"); err != nil {
		t.Fatalf("Put compressible failed: %v", err)
	}

	qKey := qualifyKey("ConfigMap", "cm-compressed")
	entry := store.keyIndex[qKey]
	h, err := store.bp.PinPage(entry.LogicalPage)
	if err != nil {
		t.Fatalf("PinPage failed: %v", err)
	}
	var storedPayload []byte
	if entry.NumBlocks == 1 {
		sp, _ := WrapSlottedPage(h.Block(0))
		_, storedPayload, _, _ = sp.Get(entry.Slot)
	} else {
		_, storedPayload, _, _ = DecodeMultiBlockPage(h.Blocks())
	}
	h.Unpin()

	if len(storedPayload) < compressionHeaderSize {
		t.Fatalf("stored payload too short: %d", len(storedPayload))
	}
	if storedPayload[0] != byte(CodecZstd) {
		t.Fatalf("expected codec byte %d (CodecZstd), got %d", CodecZstd, storedPayload[0])
	}
	recordedLen := binary.BigEndian.Uint32(storedPayload[1:5])
	if recordedLen != 8000 {
		t.Fatalf("expected recorded uncompressed length 8000, got %d", recordedLen)
	}
	if len(storedPayload) >= 8000 {
		t.Fatalf("expected compressed size to be smaller than 8000, got %d", len(storedPayload))
	}

	got, exists, err := store.Get("ConfigMap", "cm-compressed")
	if err != nil || !exists || !bytes.Equal(got, compressibleVal) {
		t.Fatalf("Get returned invalid payload")
	}

	// 2. Incompressible value above threshold (5000 random bytes)
	incompressibleVal := makeRandomPayload(5000)
	if err := store.Put("ConfigMap", "cm-raw-above-thresh", incompressibleVal, "2"); err != nil {
		t.Fatalf("Put incompressible failed: %v", err)
	}

	qKeyRaw := qualifyKey("ConfigMap", "cm-raw-above-thresh")
	entryRaw := store.keyIndex[qKeyRaw]
	hRaw, err := store.bp.PinPage(entryRaw.LogicalPage)
	if err != nil {
		t.Fatalf("PinPage failed: %v", err)
	}
	var storedRawPayload []byte
	if entryRaw.NumBlocks == 1 {
		sp, _ := WrapSlottedPage(hRaw.Block(0))
		_, storedRawPayload, _, _ = sp.Get(entryRaw.Slot)
	} else {
		_, storedRawPayload, _, _ = DecodeMultiBlockPage(hRaw.Blocks())
	}
	hRaw.Unpin()

	if storedRawPayload[0] != byte(CodecNone) {
		t.Fatalf("expected codec byte %d (CodecNone) for incompressible data, got %d", CodecNone, storedRawPayload[0])
	}
	if !bytes.Equal(storedRawPayload[1:], incompressibleVal) {
		t.Fatalf("stored raw payload mismatch")
	}

	gotRaw, exists, err := store.Get("ConfigMap", "cm-raw-above-thresh")
	if err != nil || !exists || !bytes.Equal(gotRaw, incompressibleVal) {
		t.Fatalf("Get incompressible returned invalid payload")
	}

	// 3. Compressible value below threshold (2000 bytes <= 4000)
	smallVal := makeRepetitivePayload(2000)
	if err := store.Put("ConfigMap", "cm-small", smallVal, "3"); err != nil {
		t.Fatalf("Put small failed: %v", err)
	}

	qKeySmall := qualifyKey("ConfigMap", "cm-small")
	entrySmall := store.keyIndex[qKeySmall]
	hSmall, err := store.bp.PinPage(entrySmall.LogicalPage)
	if err != nil {
		t.Fatalf("PinPage failed: %v", err)
	}
	var storedSmallPayload []byte
	spSmall, _ := WrapSlottedPage(hSmall.Block(0))
	_, storedSmallPayload, _, _ = spSmall.Get(entrySmall.Slot)
	hSmall.Unpin()

	if storedSmallPayload[0] != byte(CodecNone) {
		t.Fatalf("expected codec byte %d (CodecNone) for small data, got %d", CodecNone, storedSmallPayload[0])
	}
	if !bytes.Equal(storedSmallPayload[1:], smallVal) {
		t.Fatalf("stored small payload mismatch")
	}

	gotSmall, exists, err := store.Get("ConfigMap", "cm-small")
	if err != nil || !exists || !bytes.Equal(gotSmall, smallVal) {
		t.Fatalf("Get small returned invalid payload")
	}
}

// TestCompression_RoundTripAllCodecs tests round-trips for CodecNone, CodecZstd, and CodecS2
// at sizes on both sides of the threshold and of every page-class boundary.
func TestCompression_RoundTripAllCodecs(t *testing.T) {
	codecs := []struct {
		name  string
		codec CompressionCodec
	}{
		{"none", CodecNone},
		{"zstd", CodecZstd},
		{"s2", CodecS2},
		{"snappy", CodecSnappy},
	}

	// Test boundary sizes:
	// Below threshold: 500, 3990
	// At/above threshold: 4000, 4010
	// 2-block vs 4-block boundary (8050 bytes): 8040, 8060
	// 4-block vs 8-block boundary (16178 bytes): 16170, 16190
	// 8-block vs 16-block boundary (32434 bytes): 32420, 32450
	// 16-block vs 32-block (64946 bytes): 64940, 64960
	// 32-block vs 64-block (129970 bytes): 129960, 129980
	// 64-block vs 128-block (260018 bytes): 260000, 260030
	// 128-block vs 256-block (520114 bytes): 520100, 520130
	// 256-block vs 512-block (1040306 bytes): 1040300, 1040320
	// 512-block vs oversize (2080704 bytes): 2080000, 2100000
	testSizes := []int{
		500, 3990, 4000, 4010,
		8040, 8060,
		16170, 16190,
		32420, 32450,
		64940, 64960,
		129960, 129980,
		260000, 260030,
		520100, 520130,
		1040300, 1040320,
		2080000, 2100000,
	}

	cipher, err := NewEphemeralAESGCMCipher()
	if err != nil {
		t.Fatalf("cipher creation failed: %v", err)
	}

	for _, tc := range codecs {
		t.Run(tc.name, func(t *testing.T) {
			memFS := NewMemFS()
			store, err := Open(
				WithFS(memFS),
				WithDir("test_roundtrip_"+tc.name),
				WithCompressionCodec(tc.codec),
				WithCipher(cipher),
			)
			if err != nil {
				t.Fatalf("Open failed: %v", err)
			}
			defer store.Close()

			for _, size := range testSizes {
				key := fmt.Sprintf("key-size-%d", size)
				payload := makeRepetitivePayload(size)

				if err := store.Put("Bucket", key, payload, "rv"); err != nil {
					t.Fatalf("Put failed for size %d (codec %s): %v", size, tc.name, err)
				}

				got, exists, err := store.Get("Bucket", key)
				if err != nil || !exists {
					t.Fatalf("Get failed for size %d (codec %s): exists=%v, err=%v", size, tc.name, exists, err)
				}
				if len(got) != size || !bytes.Equal(got, payload) {
					t.Fatalf("round-trip payload mismatch for size %d (codec %s)", size, tc.name)
				}
			}
		})
	}
}

// TestCompression_PageClassReduction verifies that an object moves to a smaller page class
// when compressed.
func TestCompression_PageClassReduction(t *testing.T) {
	const objectSize = 10240 // 10 KB ConfigMap
	payload := makeRepetitivePayload(objectSize)

	// 1. Uncompressed store
	memFSNone := NewMemFS()
	storeNone, err := Open(WithFS(memFSNone), WithDir("test_pageclass_none"), WithCompressionCodec(CodecNone))
	if err != nil {
		t.Fatalf("Open none failed: %v", err)
	}
	defer storeNone.Close()

	if err := storeNone.Put("ConfigMap", "cm-10k", payload, "1"); err != nil {
		t.Fatalf("Put none failed: %v", err)
	}
	entryNone := storeNone.keyIndex[qualifyKey("ConfigMap", "cm-10k")]

	// 2. Compressed store (zstd)
	memFSZstd := NewMemFS()
	storeZstd, err := Open(WithFS(memFSZstd), WithDir("test_pageclass_zstd"), WithCompressionCodec(CodecZstd))
	if err != nil {
		t.Fatalf("Open zstd failed: %v", err)
	}
	defer storeZstd.Close()

	if err := storeZstd.Put("ConfigMap", "cm-10k", payload, "1"); err != nil {
		t.Fatalf("Put zstd failed: %v", err)
	}
	entryZstd := storeZstd.keyIndex[qualifyKey("ConfigMap", "cm-10k")]

	t.Logf("10 KB object: uncompressed NumBlocks=%d, zstd NumBlocks=%d", entryNone.NumBlocks, entryZstd.NumBlocks)

	// 10 KB uncompressed requires a 4-block page (16 KiB)
	if entryNone.NumBlocks != 4 {
		t.Fatalf("expected uncompressed 10 KB object to require 4 blocks, got %d", entryNone.NumBlocks)
	}

	// 10 KB repetitive payload compresses to ~250 bytes, fitting in a 1-block slotted page
	if entryZstd.NumBlocks >= entryNone.NumBlocks {
		t.Fatalf("expected compressed object to move to smaller page class: got %d blocks, uncompressed %d blocks",
			entryZstd.NumBlocks, entryNone.NumBlocks)
	}
	if entryZstd.NumBlocks != 1 {
		t.Fatalf("expected compressed 10 KB object to fit in 1 block, got %d", entryZstd.NumBlocks)
	}

	// Verify Get returns exact original 10 KB
	got, exists, err := storeZstd.Get("ConfigMap", "cm-10k")
	if err != nil || !exists || !bytes.Equal(got, payload) {
		t.Fatalf("Get returned invalid payload")
	}
}

// TestCompression_RecoveryWALReplay verifies that recovery replays WAL records of compressed objects
// without decompressing, reconstructing identical pages and producing identical Get results.
func TestCompression_RecoveryWALReplay(t *testing.T) {
	memFS := NewMemFS()
	dir := "test_compress_recovery"

	store, err := Open(WithFS(memFS), WithDir(dir), WithCompressionCodec(CodecZstd))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Put several compressed records
	p1 := makeRepetitivePayload(8000)
	p2 := makeRepetitivePayload(16000)
	p3 := makeRepetitivePayload(2000)

	_ = store.Put("ConfigMap", "cm-1", p1, "1")
	_ = store.Put("ConfigMap", "cm-2", p2, "2")
	_ = store.Put("ConfigMap", "cm-3", p3, "3")

	// Ensure WAL records are durable, but do not checkpoint
	if err := store.wal.Sync(); err != nil {
		t.Fatalf("wal.Sync failed: %v", err)
	}

	// Simulate crash
	_ = store.Close()

	// Reopen store: crash recovery replays WAL redo onto page buffers
	storeRecovered, err := Open(WithFS(memFS), WithDir(dir), WithCompressionCodec(CodecZstd))
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer storeRecovered.Close()

	for key, want := range map[string][]byte{"cm-1": p1, "cm-2": p2, "cm-3": p3} {
		got, exists, err := storeRecovered.Get("ConfigMap", key)
		if err != nil || !exists {
			t.Fatalf("Get %s failed: exists=%v, err=%v", key, exists, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("payload mismatch for %s after recovery redo", key)
		}
	}
}

// TestCompression_Rejection_Direct tests rejection of corrupt decompression inputs directly.
func TestCompression_Rejection_Direct(t *testing.T) {
	const maxObjSize = 1000

	// 1. Recorded length exceeds maxObjectSize
	corruptLen := make([]byte, 10)
	corruptLen[0] = byte(CodecZstd)
	binary.BigEndian.PutUint32(corruptLen[1:5], uint32(maxObjSize+1))
	_, err := decompressValue(corruptLen, maxObjSize)
	if !errors.Is(err, ErrObjectTooLarge) {
		t.Fatalf("expected ErrObjectTooLarge, got %v", err)
	}

	// 2. Mismatch between recorded and actual length
	// Valid zstd frame that decompresses to 10 bytes, but recorded length claims 20 bytes
	enc := zstdEncoderPool.Get().(*zstd.Encoder)
	compressed10 := enc.EncodeAll([]byte("0123456789"), nil)
	zstdEncoderPool.Put(enc)

	mismatchBuf := make([]byte, 5+len(compressed10))
	mismatchBuf[0] = byte(CodecZstd)
	binary.BigEndian.PutUint32(mismatchBuf[1:5], 20) // Claims 20 bytes, actual is 10
	copy(mismatchBuf[5:], compressed10)
	_, err = decompressValue(mismatchBuf, maxObjSize)
	if !errors.Is(err, ErrCorruptCompression) {
		t.Fatalf("expected ErrCorruptCompression for length mismatch, got %v", err)
	}

	// S2 length mismatch check
	s2Compressed10 := s2.Encode(nil, []byte("0123456789"))
	s2MismatchBuf := make([]byte, 5+len(s2Compressed10))
	s2MismatchBuf[0] = byte(CodecS2)
	binary.BigEndian.PutUint32(s2MismatchBuf[1:5], 20)
	copy(s2MismatchBuf[5:], s2Compressed10)
	_, err = decompressValue(s2MismatchBuf, maxObjSize)
	if !errors.Is(err, ErrCorruptCompression) {
		t.Fatalf("expected ErrCorruptCompression for S2 length mismatch, got %v", err)
	}

	// Snappy length mismatch check
	snappyCompressed10 := snappy.Encode(nil, []byte("0123456789"))
	snappyMismatchBuf := make([]byte, 5+len(snappyCompressed10))
	snappyMismatchBuf[0] = byte(CodecSnappy)
	binary.BigEndian.PutUint32(snappyMismatchBuf[1:5], 20)
	copy(snappyMismatchBuf[5:], snappyCompressed10)
	_, err = decompressValue(snappyMismatchBuf, maxObjSize)
	if !errors.Is(err, ErrCorruptCompression) {
		t.Fatalf("expected ErrCorruptCompression for Snappy length mismatch, got %v", err)
	}

	// 3. Truncated data
	_, err = decompressValue([]byte{}, maxObjSize)
	if !errors.Is(err, ErrCorruptCompression) {
		t.Fatalf("expected ErrCorruptCompression for empty data, got %v", err)
	}

	_, err = decompressValue([]byte{byte(CodecZstd), 0, 0}, maxObjSize)
	if !errors.Is(err, ErrCorruptCompression) {
		t.Fatalf("expected ErrCorruptCompression for header truncated, got %v", err)
	}

	// Truncated zstd stream
	truncatedZstd := make([]byte, 5+len(compressed10)/2)
	truncatedZstd[0] = byte(CodecZstd)
	binary.BigEndian.PutUint32(truncatedZstd[1:5], 10)
	copy(truncatedZstd[5:], compressed10[:len(compressed10)/2])
	_, err = decompressValue(truncatedZstd, maxObjSize)
	if !errors.Is(err, ErrCorruptCompression) {
		t.Fatalf("expected ErrCorruptCompression for truncated stream, got %v", err)
	}

	// 4. Unknown codec byte
	unknownCodec := []byte{0x77, 0x01, 0x02}
	_, err = decompressValue(unknownCodec, maxObjSize)
	if !errors.Is(err, ErrCorruptCompression) {
		t.Fatalf("expected ErrCorruptCompression for unknown codec, got %v", err)
	}
}

// TestCompression_Rejection_StoreState verifies that encountering corruption during Get
// puts the store into the failed state without panicking or allocating large buffers.
func TestCompression_Rejection_StoreState(t *testing.T) {
	testCases := []struct {
		name          string
		corruptBuffer func() []byte
	}{
		{
			name: "length_exceeds_max",
			corruptBuffer: func() []byte {
				buf := make([]byte, 10)
				buf[0] = byte(CodecZstd)
				binary.BigEndian.PutUint32(buf[1:5], uint32(DefaultMaxObjectSize+1000))
				return buf
			},
		},
		{
			name: "length_mismatch",
			corruptBuffer: func() []byte {
				enc := zstdEncoderPool.Get().(*zstd.Encoder)
				compressed := enc.EncodeAll([]byte("hello world"), nil)
				zstdEncoderPool.Put(enc)
				buf := make([]byte, 5+len(compressed))
				buf[0] = byte(CodecZstd)
				binary.BigEndian.PutUint32(buf[1:5], 9999) // Claims 9999, actual is 11
				copy(buf[5:], compressed)
				return buf
			},
		},
		{
			name: "truncated_data",
			corruptBuffer: func() []byte {
				return []byte{byte(CodecZstd), 0x00, 0x10}
			},
		},
		{
			name: "unknown_codec",
			corruptBuffer: func() []byte {
				return []byte{0xcc, 0x01, 0x02, 0x03}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			memFS := NewMemFS()
			store, err := Open(WithFS(memFS), WithDir("test_reject_"+tc.name))
			if err != nil {
				t.Fatalf("Open failed: %v", err)
			}
			defer store.Close()

			// Write a normal record first
			_ = store.Put("ConfigMap", "corrupt-cm", []byte("placeholder"), "1")

			// Delete existing slot and put corruptPayload with matching key and LSN
			entry := store.keyIndex[qualifyKey("ConfigMap", "corrupt-cm")]
			handle, err := store.bp.PinPage(entry.LogicalPage)
			if err != nil {
				t.Fatalf("PinPage failed: %v", err)
			}
			handle.Desc.Mu.Lock()
			sp, _ := WrapSlottedPage(handle.Block(0))
			_ = sp.Delete(entry.Slot, entry.LSN)
			corruptPayload := tc.corruptBuffer()
			slotIdx, putErr := sp.Put(qualifyKey("ConfigMap", "corrupt-cm"), corruptPayload, entry.LSN)
			if putErr != nil {
				t.Fatalf("sp.Put failed: %v", putErr)
			}
			handle.Desc.Mu.Unlock()
			handle.Unpin()

			store.keyIndex[qualifyKey("ConfigMap", "corrupt-cm")] = KeyIndexEntry{
				LogicalPage: entry.LogicalPage,
				Slot:        slotIdx,
				LSN:         entry.LSN,
				NumBlocks:   1,
			}

			// Get must fail and transition store to failed state
			_, _, err = store.Get("ConfigMap", "corrupt-cm")
			if err == nil {
				t.Fatalf("expected Get to fail on corruption, but succeeded")
			}
			if !errors.Is(err, ErrFailed) {
				t.Fatalf("expected ErrFailed, got %v", err)
			}

			// Store must be in failed state
			if !store.isFailed() {
				t.Fatalf("store is not in failed state")
			}
			select {
			case <-store.Failed():
			default:
				t.Fatalf("store.Failed() channel is not closed")
			}

			// Subsequent operations must return ErrFailed
			if err := store.Put("ConfigMap", "new-key", []byte("val"), "2"); !errors.Is(err, ErrFailed) {
				t.Fatalf("expected ErrFailed from Put on failed store, got %v", err)
			}
		})
	}
}

// TestCompression_ConfigurableThreshold verifies that WithCompressionThreshold changes
// the size boundary at which compression is attempted.
func TestCompression_ConfigurableThreshold(t *testing.T) {
	memFS := NewMemFS()
	store, err := Open(
		WithFS(memFS),
		WithDir("test_custom_thresh"),
		WithCompressionCodec(CodecZstd),
		WithCompressionThreshold(100), // Low threshold: compress values > 100 bytes
	)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// 500 bytes (> 100) should be compressed
	payload500 := makeRepetitivePayload(500)
	if err := store.Put("ConfigMap", "cm-500", payload500, "1"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	entry := store.keyIndex[qualifyKey("ConfigMap", "cm-500")]
	h, _ := store.bp.PinPage(entry.LogicalPage)
	sp, _ := WrapSlottedPage(h.Block(0))
	_, stored, _, _ := sp.Get(entry.Slot)
	h.Unpin()

	if stored[0] != byte(CodecZstd) {
		t.Fatalf("expected codec %d (CodecZstd) for payload > custom threshold 100, got %d", CodecZstd, stored[0])
	}

	got, exists, err := store.Get("ConfigMap", "cm-500")
	if err != nil || !exists || !bytes.Equal(got, payload500) {
		t.Fatalf("Get returned invalid payload")
	}
}
