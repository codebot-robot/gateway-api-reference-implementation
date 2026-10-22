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
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	// BlockSize is the fixed 4 KiB size of every heap block.
	BlockSize = 4096

	// BlockHeaderSize is the fixed 64-byte header size at the beginning of every block.
	BlockHeaderSize = 64

	// BlockUsableSize is the usable payload capacity per block (4032 bytes).
	BlockUsableSize = BlockSize - BlockHeaderSize

	// MagicGARIDSTR is the magic number identifying valid diskstore blocks ("GARIDSTR").
	MagicGARIDSTR uint64 = 0x4741524944535452

	// FormatVersion is 1.
	FormatVersion uint32 = 1
)

var (
	// ErrInvalidBlockMagic indicates the block magic number does not match.
	ErrInvalidBlockMagic = errors.New("invalid block magic number")

	// ErrInvalidBlockVersion indicates an unsupported block format version.
	ErrInvalidBlockVersion = errors.New("unsupported block format version")

	// ErrBlockCRCMismatch indicates block CRC32C verification failed.
	ErrBlockCRCMismatch = errors.New("block CRC32C checksum mismatch")

	// ErrForeignBlock indicates the block belongs to a different database ID.
	ErrForeignBlock = errors.New("foreign block: database ID mismatch")
)

// BlockHeader represents the self-describing 64-byte header on every 4 KiB block.
type BlockHeader struct {
	Magic       uint64   // 8 bytes (0x4741524944535452)
	Version     uint32   // 4 bytes (1)
	Reserved1   uint32   // 4 bytes
	DBID        [16]byte // 16 bytes (store database ID)
	LogicalPage uint64   // 8 bytes (logical page number)
	LSN         uint64   // 8 bytes (log sequence number)
	BlockIndex  uint32   // 4 bytes (block index within its page: 0..NumBlocks-1)
	NumBlocks   uint32   // 4 bytes (total blocks in this page: power of 2 from 1 to 512)
	CRC32C      uint32   // 4 bytes (CRC32C over entire 4096 bytes with CRC field zeroed)
	Reserved2   [8]byte  // 8 bytes padding
}

// EncodeBlockHeader writes the header into the first 64 bytes of blockBuf.
// It zeroes the CRC field (bytes 56..60) so CRC32C can be computed over blockBuf.
func EncodeBlockHeader(hdr BlockHeader, blockBuf []byte) {
	if len(blockBuf) < BlockHeaderSize {
		panic("block buffer too short for header")
	}

	binary.BigEndian.PutUint64(blockBuf[0:8], MagicGARIDSTR)
	binary.BigEndian.PutUint32(blockBuf[8:12], FormatVersion)
	binary.BigEndian.PutUint32(blockBuf[12:16], 0)
	copy(blockBuf[16:32], hdr.DBID[:])
	binary.BigEndian.PutUint64(blockBuf[32:40], hdr.LogicalPage)
	binary.BigEndian.PutUint64(blockBuf[40:48], hdr.LSN)
	binary.BigEndian.PutUint32(blockBuf[48:52], hdr.BlockIndex)
	binary.BigEndian.PutUint32(blockBuf[52:56], hdr.NumBlocks)

	// Zero CRC field
	blockBuf[56] = 0
	blockBuf[57] = 0
	blockBuf[58] = 0
	blockBuf[59] = 0

	copy(blockBuf[60:64], hdr.Reserved2[:4])
}

// FinalizeBlockCRC computes CRC32C over the entire 4096 bytes of blockBuf and stores it at bytes 56..60.
func FinalizeBlockCRC(blockBuf []byte) uint32 {
	if len(blockBuf) != BlockSize {
		panic(fmt.Sprintf("invalid block size for CRC finalization: %d", len(blockBuf)))
	}
	blockBuf[56] = 0
	blockBuf[57] = 0
	blockBuf[58] = 0
	blockBuf[59] = 0

	crc := crc32.Checksum(blockBuf, crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(blockBuf[56:60], crc)
	return crc
}

// DecodeBlockHeader parses and validates the 64-byte header of blockBuf.
// It verifies magic, version, expectedDBID (if not all zeroes), and CRC32C.
func DecodeBlockHeader(blockBuf []byte, expectedDBID [16]byte) (BlockHeader, error) {
	if len(blockBuf) < BlockSize {
		return BlockHeader{}, errors.New("block buffer shorter than 4096 bytes")
	}

	magic := binary.BigEndian.Uint64(blockBuf[0:8])
	if magic != MagicGARIDSTR {
		return BlockHeader{}, ErrInvalidBlockMagic
	}

	ver := binary.BigEndian.Uint32(blockBuf[8:12])
	if ver != FormatVersion {
		return BlockHeader{}, ErrInvalidBlockVersion
	}

	var dbID [16]byte
	copy(dbID[:], blockBuf[16:32])

	var zeroDBID [16]byte
	if expectedDBID != zeroDBID && dbID != expectedDBID {
		return BlockHeader{}, ErrForeignBlock
	}

	logicalPage := binary.BigEndian.Uint64(blockBuf[32:40])
	lsn := binary.BigEndian.Uint64(blockBuf[40:48])
	blockIndex := binary.BigEndian.Uint32(blockBuf[48:52])
	numBlocks := binary.BigEndian.Uint32(blockBuf[52:56])
	storedCRC := binary.BigEndian.Uint32(blockBuf[56:60])

	// Verify CRC
	calcBuf := make([]byte, BlockSize)
	copy(calcBuf, blockBuf[:BlockSize])
	calcBuf[56] = 0
	calcBuf[57] = 0
	calcBuf[58] = 0
	calcBuf[59] = 0

	actualCRC := crc32.Checksum(calcBuf, crc32.MakeTable(crc32.Castagnoli))
	if storedCRC != actualCRC {
		return BlockHeader{}, ErrBlockCRCMismatch
	}

	return BlockHeader{
		Magic:       magic,
		Version:     ver,
		DBID:        dbID,
		LogicalPage: logicalPage,
		LSN:         lsn,
		BlockIndex:  blockIndex,
		NumBlocks:   numBlocks,
		CRC32C:      storedCRC,
	}, nil
}
