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
	"errors"
	"testing"
)

func TestBlock_HeaderEncodeDecode(t *testing.T) {
	hdr := BlockHeader{
		LogicalPage: 42,
		LSN:         1001,
		BlockIndex:  3,
		NumBlocks:   8,
	}

	buf := make([]byte, BlockSize)
	EncodeBlockHeader(hdr, buf)
	FinalizeBlockCRC(buf)

	decoded, err := DecodeBlockHeader(buf)
	if err != nil {
		t.Fatalf("DecodeBlockHeader failed: %v", err)
	}

	if decoded.LogicalPage != hdr.LogicalPage ||
		decoded.LSN != hdr.LSN ||
		decoded.BlockIndex != hdr.BlockIndex ||
		decoded.NumBlocks != hdr.NumBlocks {
		t.Fatalf("decoded header mismatch: got %+v, want %+v", decoded, hdr)
	}
}

func TestBlock_InvalidMagicOrVersion(t *testing.T) {
	buf := make([]byte, BlockSize)
	EncodeBlockHeader(BlockHeader{LogicalPage: 1, LSN: 10, NumBlocks: 1}, buf)
	FinalizeBlockCRC(buf)

	// Invalidate magic
	buf[0] = 0xff
	FinalizeBlockCRC(buf)
	_, err := DecodeBlockHeader(buf)
	if !errors.Is(err, ErrInvalidBlockMagic) {
		t.Fatalf("expected ErrInvalidBlockMagic, got %v", err)
	}

	// Invalidate version
	buf[0] = BlockMagic
	buf[1] = 0x99
	FinalizeBlockCRC(buf)
	_, err = DecodeBlockHeader(buf)
	if !errors.Is(err, ErrInvalidBlockVersion) {
		t.Fatalf("expected ErrInvalidBlockVersion, got %v", err)
	}
}

func TestBlock_CRCMismatch(t *testing.T) {
	buf := make([]byte, BlockSize)
	EncodeBlockHeader(BlockHeader{LogicalPage: 1, LSN: 10, NumBlocks: 1}, buf)
	FinalizeBlockCRC(buf)

	// Corrupt a byte in payload area
	buf[100] ^= 0xff

	_, err := DecodeBlockHeader(buf)
	if !errors.Is(err, ErrBlockCRCMismatch) {
		t.Fatalf("expected ErrBlockCRCMismatch, got %v", err)
	}
}
