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
	RecordTypeBookmark   RecordType = 4
)

const (
	// RecordMagic identifies valid log records ('R').
	RecordMagic byte = 0x52

	// RecordFormatVersion is 1.
	RecordFormatVersion byte = 1

	// RecordHeaderSize is 36 bytes:
	// Magic(1) + Version(1) + Type(1) + Reserved(1) + LSN(8) + RedoStartLSN(8) + KeyLen(4) + PayloadLen(4) + RVLen(4) + CRC32C(4)
	RecordHeaderSize = 36

	// MaxRecordFieldLength is a sanity check limit (64 MiB).
	MaxRecordFieldLength = 64 * 1024 * 1024
)

// Record represents a single write-ahead log record.
type Record struct {
	Type            RecordType
	LSN             uint64
	RedoStartLSN    uint64 // Redo start LSN for Checkpoint records
	Key             string
	Payload         []byte
	Token           string // Opaque per-bucket progress token (persisted in RV field on disk)
	ResourceVersion string // Alias / compatibility mapping to Token
	CRC32C          uint32
}

// EncodeRecord serializes a record into binary format with a valid CRC32C.
func EncodeRecord(r Record) []byte {
	token := r.Token
	if token == "" {
		token = r.ResourceVersion
	}
	keyBytes := []byte(r.Key)
	rvBytes := []byte(token)

	totalLen := RecordHeaderSize + len(keyBytes) + len(r.Payload) + len(rvBytes)
	buf := make([]byte, totalLen)

	buf[0] = RecordMagic
	buf[1] = RecordFormatVersion
	buf[2] = byte(r.Type)
	buf[3] = 0 // reserved

	binary.BigEndian.PutUint64(buf[4:12], r.LSN)
	binary.BigEndian.PutUint64(buf[12:20], r.RedoStartLSN)
	binary.BigEndian.PutUint32(buf[20:24], uint32(len(keyBytes)))
	binary.BigEndian.PutUint32(buf[24:28], uint32(len(r.Payload)))
	binary.BigEndian.PutUint32(buf[28:32], uint32(len(rvBytes)))

	// Zero CRC field before calculation
	buf[32] = 0
	buf[33] = 0
	buf[34] = 0
	buf[35] = 0

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

	binary.BigEndian.PutUint32(buf[32:36], crc)
	return buf
}

// DecodeRecord reads and validates a single record from reader.
func DecodeRecord(r io.Reader) (Record, error) {
	header := make([]byte, RecordHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return Record{}, err
	}

	magic := header[0]
	ver := header[1]
	if magic != RecordMagic || ver != RecordFormatVersion {
		return Record{}, fmt.Errorf("%w: invalid record magic or version (got magic 0x%02x, ver 0x%02x)", ErrCorruptRecord, magic, ver)
	}

	recType := RecordType(header[2])
	lsn := binary.BigEndian.Uint64(header[4:12])
	redoStartLSN := binary.BigEndian.Uint64(header[12:20])
	keyLen := binary.BigEndian.Uint32(header[20:24])
	payloadLen := binary.BigEndian.Uint32(header[24:28])
	rvLen := binary.BigEndian.Uint32(header[28:32])
	storedCRC := binary.BigEndian.Uint32(header[32:36])

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
	headerCopy[32] = 0
	headerCopy[33] = 0
	headerCopy[34] = 0
	headerCopy[35] = 0

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
		Token:           rv,
		ResourceVersion: rv,
		CRC32C:          storedCRC,
	}, nil
}
