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
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestSlottedPage_CRUDAndStableSlots(t *testing.T) {
	page := NewSlottedPage(1, 100)

	// Insert 3 records
	slot0, err := page.Put("key-0", []byte("val-0"), 100)
	if err != nil || slot0 != 0 {
		t.Fatalf("Put slot0 failed: slot=%d, err=%v", slot0, err)
	}

	slot1, err := page.Put("key-1", []byte("val-1"), 101)
	if err != nil || slot1 != 1 {
		t.Fatalf("Put slot1 failed: slot=%d, err=%v", slot1, err)
	}

	slot2, err := page.Put("key-2", []byte("val-2"), 102)
	if err != nil || slot2 != 2 {
		t.Fatalf("Put slot2 failed: slot=%d, err=%v", slot2, err)
	}

	// Read back
	k, v, lsn, err := page.Get(slot1)
	if err != nil || k != "key-1" || string(v) != "val-1" || lsn != 101 {
		t.Fatalf("Get slot1 mismatch: k=%s v=%s lsn=%d err=%v", k, string(v), lsn, err)
	}

	// Delete slot 1
	if err := page.Delete(slot1, 103); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, _, _, err = page.Get(slot1)
	if !errors.Is(err, ErrSlotNotFound) {
		t.Fatalf("expected ErrSlotNotFound for deleted slot, got %v", err)
	}

	// Slots 0 and 2 must remain readable
	k0, v0, _, _ := page.Get(slot0)
	k2, v2, _, _ := page.Get(slot2)
	if k0 != "key-0" || string(v0) != "val-0" || k2 != "key-2" || string(v2) != "val-2" {
		t.Fatalf("adjacent slots damaged after delete")
	}

	// Insert a new record: must reuse slot 1 (stable slot numbers)
	slotReuse, err := page.Put("key-reused", []byte("val-reused"), 104)
	if err != nil || slotReuse != slot1 {
		t.Fatalf("expected reuse of slot 1, got %d, err=%v", slotReuse, err)
	}

	kReused, vReused, lsnReused, err := page.Get(slotReuse)
	if err != nil || kReused != "key-reused" || string(vReused) != "val-reused" || lsnReused != 104 {
		t.Fatalf("reused slot content mismatch: %v", err)
	}
}

func TestSlottedPage_CompactionPreservesSlotIDs(t *testing.T) {
	page := NewSlottedPage(2, 200)

	// Fill page with multiple records
	var slots []uint32
	for i := 0; i < 10; i++ {
		slot, err := page.Put(fmt.Sprintf("key-%02d", i), make([]byte, 200), uint64(200+i))
		if err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
		slots = append(slots, slot)
	}

	// Delete even slots
	for i := 0; i < len(slots); i += 2 {
		_ = page.Delete(slots[i], 300)
	}

	// Trigger compaction
	page.Compact()

	// Invariant: odd slots must remain valid at their exact original slot indices!
	for i := 1; i < len(slots); i += 2 {
		expectedKey := fmt.Sprintf("key-%02d", i)
		k, v, _, err := page.Get(slots[i])
		if err != nil || k != expectedKey || len(v) != 200 {
			t.Fatalf("slot %d corrupted after compaction: k=%s len=%d err=%v", slots[i], k, len(v), err)
		}
	}

	// Deleted even slots must still report not found
	for i := 0; i < len(slots); i += 2 {
		_, _, _, err := page.Get(slots[i])
		if !errors.Is(err, ErrSlotNotFound) {
			t.Fatalf("expected slot %d to be not found, got %v", slots[i], err)
		}
	}
}

func TestMultiBlockPage_EncodeDecode(t *testing.T) {
	// 10 KB payload (requires 4 blocks)
	payload := make([]byte, 10240)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	mbPage, err := NewMultiBlockPage(10, 500, "default/large-configmap", payload)
	if err != nil {
		t.Fatalf("NewMultiBlockPage failed: %v", err)
	}

	if len(mbPage.Blocks) != 4 {
		t.Fatalf("expected 4 blocks for 10 KB payload, got %d", len(mbPage.Blocks))
	}

	// Verify each block has a valid header and valid CRC
	for i, blk := range mbPage.Blocks {
		hdr, err := DecodeBlockHeader(blk)
		if err != nil {
			t.Fatalf("block %d header decode failed: %v", i, err)
		}
		if hdr.BlockIndex != uint32(i) || hdr.NumBlocks != 4 || hdr.LogicalPage != 10 || hdr.LSN != 500 {
			t.Fatalf("block %d header mismatch: %+v", i, hdr)
		}
	}

	// Decode multi-block page
	k, resPayload, lsn, err := DecodeMultiBlockPage(mbPage.Blocks)
	if err != nil {
		t.Fatalf("DecodeMultiBlockPage failed: %v", err)
	}

	if k != "default/large-configmap" || lsn != 500 || !bytes.Equal(resPayload, payload) {
		t.Fatalf("decoded multi-block record mismatch")
	}
}

func TestMultiBlockPage_OversizeObject(t *testing.T) {
	// Payload > MaxPayloadCapacity (~2 MiB)
	oversize := make([]byte, 3*1024*1024)

	_, err := NewMultiBlockPage(1, 1, "oversize", oversize)
	if !errors.Is(err, ErrObjectOversize) {
		t.Fatalf("expected ErrObjectOversize, got %v", err)
	}
}

func TestCalculateMultiBlockCount(t *testing.T) {
	// Key len + payload len within 2 blocks
	blocks, err := CalculateMultiBlockCount(10, 100)
	if err != nil || blocks != 2 {
		t.Fatalf("expected 2 blocks, got %d, err %v", blocks, err)
	}

	// Boundary around 2 blocks: capacity = 2 * 4064 = 8128. Overhead = 14.
	// For keyLen = 300, max payload in 2 blocks is 8128 - 14 - 300 = 7814.
	blocks, err = CalculateMultiBlockCount(300, 7814)
	if err != nil || blocks != 2 {
		t.Fatalf("expected 2 blocks for payload 7814, got %d, err %v", blocks, err)
	}
	// 7815 bytes exceeds 2 blocks, requires 4 blocks
	blocks, err = CalculateMultiBlockCount(300, 7815)
	if err != nil || blocks != 4 {
		t.Fatalf("expected 4 blocks for payload 7815, got %d, err %v", blocks, err)
	}

	// Exceeds 512 blocks
	maxPayload512 := MaxMultiBlockCount*BlockUsableSize - PageRecordHeaderSize - 300
	blocks, err = CalculateMultiBlockCount(300, maxPayload512)
	if err != nil || blocks != 512 {
		t.Fatalf("expected 512 blocks for payload %d, got %d, err %v", maxPayload512, blocks, err)
	}
	_, err = CalculateMultiBlockCount(300, maxPayload512+1)
	if !errors.Is(err, ErrObjectOversize) {
		t.Fatalf("expected ErrObjectOversize, got %v", err)
	}

	// Key too large (> 65535)
	_, err = CalculateMultiBlockCount(65536, 10)
	if !errors.Is(err, ErrObjectOversize) {
		t.Fatalf("expected ErrObjectOversize for key > 65535, got %v", err)
	}
}

func TestNewMultiBlockPage_CantWritePastBlocks(t *testing.T) {
	// 1. Symptom 2 case: 300-byte key with payload around 7.8-8.0 KiB (8050 bytes).
	// Must not panic, must successfully encode with 4 blocks.
	key300 := strings.Repeat("k", 300)
	payload8K := bytes.Repeat([]byte("p"), 8050)
	mbPage, err := NewMultiBlockPage(1, 100, key300, payload8K)
	if err != nil {
		t.Fatalf("NewMultiBlockPage failed: %v", err)
	}
	if len(mbPage.Blocks) != 4 {
		t.Fatalf("expected 4 blocks, got %d", len(mbPage.Blocks))
	}
	decKey, decPayload, decLSN, err := DecodeMultiBlockPage(mbPage.Blocks)
	if err != nil || decKey != key300 || decLSN != 100 || !bytes.Equal(decPayload, payload8K) {
		t.Fatalf("decoded multi-block record mismatch: err=%v", err)
	}

	// 2. 65535-byte key with small payload: must encode and decode cleanly.
	key65K := strings.Repeat("x", 65535)
	mb65K, err := NewMultiBlockPage(2, 200, key65K, []byte("small-val"))
	if err != nil {
		t.Fatalf("NewMultiBlockPage with 65535-byte key failed: %v", err)
	}
	decKey65, decPayload65, decLSN65, err := DecodeMultiBlockPage(mb65K.Blocks)
	if err != nil || decKey65 != key65K || decLSN65 != 200 || string(decPayload65) != "small-val" {
		t.Fatalf("decoded 65535-byte key record mismatch: err=%v", err)
	}

	// 3. Key exceeding 65535 bytes: must return error, not panic.
	key65536 := strings.Repeat("x", 65536)
	_, err = NewMultiBlockPage(3, 300, key65536, []byte("val"))
	if !errors.Is(err, ErrObjectOversize) {
		t.Fatalf("expected ErrObjectOversize for 65536-byte key, got %v", err)
	}

	// 4. Record exceeding 512-block capacity: must return error, not panic.
	hugePayload := make([]byte, MaxMultiBlockCount*BlockUsableSize)
	_, err = NewMultiBlockPage(4, 400, "k", hugePayload)
	if !errors.Is(err, ErrObjectOversize) {
		t.Fatalf("expected ErrObjectOversize for record exceeding 512 blocks, got %v", err)
	}
}
