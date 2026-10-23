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
	"testing"
)

func TestRecord_EncodeDecode(t *testing.T) {
	orig := Record{
		Type:         RecordTypePut,
		LSN:          105,
		RedoStartLSN: 90,
		Key:          "default/my-configmap",
		Payload:      []byte("sample payload data 12345"),
		Token:        "123456",
	}

	encoded := EncodeRecord(orig)
	decoded, err := DecodeRecord(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("DecodeRecord failed: %v", err)
	}

	if decoded.Type != orig.Type ||
		decoded.LSN != orig.LSN ||
		decoded.RedoStartLSN != orig.RedoStartLSN ||
		decoded.Key != orig.Key ||
		!bytes.Equal(decoded.Payload, orig.Payload) ||
		decoded.Token != orig.Token {
		t.Fatalf("decoded record does not match original: got %+v, want %+v", decoded, orig)
	}
}

func TestRecord_BadCRC(t *testing.T) {
	orig := Record{
		Type:    RecordTypePut,
		LSN:     1,
		Key:     "test/key",
		Payload: []byte("data"),
		Token:   "10",
	}

	encoded := EncodeRecord(orig)
	// Corrupt payload byte
	encoded[len(encoded)-1] ^= 0xff

	_, err := DecodeRecord(bytes.NewReader(encoded))
	if !errors.Is(err, ErrBadCRC) {
		t.Fatalf("expected ErrBadCRC, got %v", err)
	}
}

func TestRecord_TornRecord(t *testing.T) {
	orig := Record{
		Type:  RecordTypeDelete,
		LSN:   2,
		Key:   "test/key",
		Token: "11",
	}

	encoded := EncodeRecord(orig)
	// Truncate encoded bytes
	short := encoded[:len(encoded)-5]

	_, err := DecodeRecord(bytes.NewReader(short))
	if err == nil {
		t.Fatalf("expected error on truncated record, got nil")
	}
}
