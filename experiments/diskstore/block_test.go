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
	"testing"
)

func TestBlock_HeaderEncodeDecode(t *testing.T) {
	var dbID [16]byte
	copy(dbID[:], []byte("block-test-db-01"))

	hdr := BlockHeader{
		DBID:        dbID,
		LogicalPage: 42,
		LSN:         1001,
		BlockIndex:  3,
		NumBlocks:   8,
	}

	buf := make([]byte, BlockSize)
	EncodeBlockHeader(hdr, buf)
	FinalizeBlockCRC(buf)

	decoded, err := DecodeBlockHeader(buf, dbID)
	if err != nil {
		t.Fatalf("DecodeBlockHeader failed: %v", err)
	}

	if decoded.LogicalPage != hdr.LogicalPage ||
		decoded.LSN != hdr.LSN ||
		decoded.BlockIndex != hdr.BlockIndex ||
		decoded.NumBlocks != hdr.NumBlocks ||
		decoded.DBID != hdr.DBID {
		t.Fatalf("decoded header mismatch: got %+v, want %+v", decoded, hdr)
	}
}

func TestBlock_ForeignDBID(t *testing.T) {
	var dbID1, dbID2 [16]byte
	copy(dbID1[:], []byte("db-one-111111111"))
	copy(dbID2[:], []byte("db-two-222222222"))

	buf := make([]byte, BlockSize)
	EncodeBlockHeader(BlockHeader{DBID: dbID1, LogicalPage: 1, LSN: 10, NumBlocks: 1}, buf)
	FinalizeBlockCRC(buf)

	_, err := DecodeBlockHeader(buf, dbID2)
	if !errors.Is(err, ErrForeignBlock) {
		t.Fatalf("expected ErrForeignBlock, got %v", err)
	}
}

func TestBlock_CRCMismatch(t *testing.T) {
	var dbID [16]byte
	copy(dbID[:], []byte("db-crc-test-0001"))

	buf := make([]byte, BlockSize)
	EncodeBlockHeader(BlockHeader{DBID: dbID, LogicalPage: 1, LSN: 10, NumBlocks: 1}, buf)
	FinalizeBlockCRC(buf)

	// Corrupt a byte in payload area
	buf[100] ^= 0xff

	_, err := DecodeBlockHeader(buf, dbID)
	if !errors.Is(err, ErrBlockCRCMismatch) {
		t.Fatalf("expected ErrBlockCRCMismatch, got %v", err)
	}
}
