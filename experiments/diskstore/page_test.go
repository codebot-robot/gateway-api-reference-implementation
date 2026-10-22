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
	"errors"
	"fmt"
	"testing"
)

func TestSlottedPage_CRUDAndStableSlots(t *testing.T) {
	var dbID [16]byte
	copy(dbID[:], []byte("slotted-page-001"))

	page := NewSlottedPage(1, 100, dbID)

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
	var dbID [16]byte
	copy(dbID[:], []byte("slotted-page-002"))

	page := NewSlottedPage(2, 200, dbID)

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
	var dbID [16]byte
	copy(dbID[:], []byte("multi-block-db-1"))

	// 10 KB payload (requires 4 blocks)
	payload := make([]byte, 10240)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	mbPage, err := NewMultiBlockPage(10, 500, dbID, "default/large-configmap", payload)
	if err != nil {
		t.Fatalf("NewMultiBlockPage failed: %v", err)
	}

	if len(mbPage.Blocks) != 4 {
		t.Fatalf("expected 4 blocks for 10 KB payload, got %d", len(mbPage.Blocks))
	}

	// Verify each block has a valid header and valid CRC
	for i, blk := range mbPage.Blocks {
		hdr, err := DecodeBlockHeader(blk, dbID)
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
	var dbID [16]byte

	_, err := NewMultiBlockPage(1, 1, dbID, "oversize", oversize)
	if !errors.Is(err, ErrObjectOversize) {
		t.Fatalf("expected ErrObjectOversize, got %v", err)
	}
}
