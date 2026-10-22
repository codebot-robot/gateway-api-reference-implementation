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
	"io"
)

var (
	// ErrBadCRC indicates a record failed CRC32C verification.
	ErrBadCRC = errors.New("bad CRC32C checksum in record")

	// ErrNonContiguousLSN indicates a record LSN did not match the expected sequential LSN.
	ErrNonContiguousLSN = errors.New("non-contiguous LSN encountered during replay")

	// ErrCorruptRecord indicates malformed record fields.
	ErrCorruptRecord = errors.New("corrupt record data")
)

// RecordType defines the operation type of a WAL log record.
type RecordType uint8

const (
	RecordTypePut        RecordType = 1
	RecordTypeDelete     RecordType = 2
	RecordTypeCheckpoint RecordType = 3
)

// Record represents a single write-ahead log record.
type Record struct {
	Type            RecordType
	LSN             uint64
	RedoStartLSN    uint64 // Redo start LSN for Checkpoint records
	Key             string
	Payload         []byte
	ResourceVersion string
	CRC32C          uint32
}

const (
	// RecordHeaderSize is 33 bytes:
	// Type(1) + LSN(8) + RedoStartLSN(8) + KeyLen(4) + PayloadLen(4) + RVLen(4) + CRC32C(4)
	RecordHeaderSize = 33

	// MaxRecordFieldLength is a sanity check limit (64 MiB).
	MaxRecordFieldLength = 64 * 1024 * 1024
)

// EncodeRecord serializes a record into binary format with a valid CRC32C.
func EncodeRecord(r Record) []byte {
	keyBytes := []byte(r.Key)
	rvBytes := []byte(r.ResourceVersion)

	totalLen := RecordHeaderSize + len(keyBytes) + len(r.Payload) + len(rvBytes)
	buf := make([]byte, totalLen)

	buf[0] = byte(r.Type)
	binary.BigEndian.PutUint64(buf[1:9], r.LSN)
	binary.BigEndian.PutUint64(buf[9:17], r.RedoStartLSN)
	binary.BigEndian.PutUint32(buf[17:21], uint32(len(keyBytes)))
	binary.BigEndian.PutUint32(buf[21:25], uint32(len(r.Payload)))
	binary.BigEndian.PutUint32(buf[25:29], uint32(len(rvBytes)))

	// Zero CRC field before calculation
	buf[29] = 0
	buf[30] = 0
	buf[31] = 0
	buf[32] = 0

	off := RecordHeaderSize
	copy(buf[off:], keyBytes)
	off += len(keyBytes)
	copy(buf[off:], r.Payload)
	off += len(r.Payload)
	copy(buf[off:], rvBytes)

	// Compute CRC32C over header + body
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	_, _ = h.Write(buf)
	crc := h.Sum32()

	binary.BigEndian.PutUint32(buf[29:33], crc)
	return buf
}

// DecodeRecord reads and validates a single record from reader.
func DecodeRecord(r io.Reader) (Record, error) {
	header := make([]byte, RecordHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return Record{}, err
	}

	recType := RecordType(header[0])
	lsn := binary.BigEndian.Uint64(header[1:9])
	redoStartLSN := binary.BigEndian.Uint64(header[9:17])
	keyLen := binary.BigEndian.Uint32(header[17:21])
	payloadLen := binary.BigEndian.Uint32(header[21:25])
	rvLen := binary.BigEndian.Uint32(header[25:29])
	storedCRC := binary.BigEndian.Uint32(header[29:33])

	if keyLen > MaxRecordFieldLength || payloadLen > MaxRecordFieldLength || rvLen > MaxRecordFieldLength {
		return Record{}, fmt.Errorf("%w: field length exceeds limit", ErrCorruptRecord)
	}

	bodyLen := int(keyLen) + int(payloadLen) + int(rvLen)
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return Record{}, fmt.Errorf("%w: incomplete record body", ErrCorruptRecord)
	}

	// Verify CRC
	headerCopy := make([]byte, RecordHeaderSize)
	copy(headerCopy, header)
	headerCopy[29] = 0
	headerCopy[30] = 0
	headerCopy[31] = 0
	headerCopy[32] = 0

	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	_, _ = h.Write(headerCopy)
	_, _ = h.Write(body)
	actualCRC := h.Sum32()

	if storedCRC != actualCRC {
		return Record{}, ErrBadCRC
	}

	key := string(body[:keyLen])
	payload := body[keyLen : keyLen+payloadLen]
	rv := string(body[keyLen+payloadLen:])

	return Record{
		Type:            recType,
		LSN:             lsn,
		RedoStartLSN:    redoStartLSN,
		Key:             key,
		Payload:         payload,
		ResourceVersion: rv,
		CRC32C:          storedCRC,
	}, nil
}
