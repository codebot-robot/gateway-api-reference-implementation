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
	"encoding/binary"
	"errors"
	"hash/crc32"
	"strings"
	"testing"
)

func makeTestBlock(logicalPage, lsn uint64, blockIndex, numBlocks uint32) []byte {
	buf := make([]byte, DefaultBlockSize)
	hdr := BlockHeader{
		LogicalPage: logicalPage,
		LSN:         lsn,
		BlockIndex:  blockIndex,
		NumBlocks:   numBlocks,
	}
	EncodeBlockHeader(hdr, buf)
	FinalizeBlockCRC(buf)
	return buf
}

func TestFaultFile_WriteAndSync(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)
	faultFS.SetMaxFsyncedLSN(100)

	file, err := faultFS.Create("heap.db")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer file.Close()

	// Write block 0 with LSN 50
	b0 := makeTestBlock(1, 50, 0, 1)
	if _, err := file.WriteAt(b0, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Verify reading un-synced data works
	readBuf := make([]byte, DefaultBlockSize)
	if _, err := file.ReadAt(readBuf, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(readBuf, b0) {
		t.Fatalf("ReadAt data mismatch")
	}

	// Sync
	if err := file.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
}

func TestFaultFile_CrashDropUnsynced(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)
	faultFS.SetMaxFsyncedLSN(100)

	file, err := faultFS.Create("heap.db")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer file.Close()

	// Durable write
	b0 := makeTestBlock(1, 50, 0, 1)
	_, _ = file.WriteAt(b0, 0)
	_ = file.Sync()

	// Unsynced write to block 1
	b1 := makeTestBlock(2, 60, 0, 1)
	_, _ = file.WriteAt(b1, DefaultBlockSize)

	// Simulate crash dropping unsynced writes
	faultFS.Crash(true, 0)

	// File size should be 1 block (4096 bytes)
	size, _ := file.Size()
	if size != DefaultBlockSize {
		t.Fatalf("expected size %d after crash drop, got %d", DefaultBlockSize, size)
	}

	// Block 0 should still be intact
	readBuf := make([]byte, DefaultBlockSize)
	_, _ = file.ReadAt(readBuf, 0)
	if !bytes.Equal(readBuf, b0) {
		t.Fatalf("durable block corrupted after crash")
	}
}

func TestFaultFile_CrashTearBlocks(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)
	faultFS.SetMaxFsyncedLSN(100)

	file, _ := faultFS.Create("heap.db")
	defer file.Close()

	// Unsynced write to block 0
	b0 := makeTestBlock(1, 50, 0, 1)
	_, _ = file.WriteAt(b0, 0)

	// Crash with tear fraction 1.0 (always tear pending blocks)
	faultFS.Crash(false, 1.0)

	readBuf := make([]byte, DefaultBlockSize)
	_, _ = file.ReadAt(readBuf, 0)

	// Block should now fail CRC
	storedCRC := binary.BigEndian.Uint32(readBuf[56:60])
	calcBuf := make([]byte, DefaultBlockSize)
	copy(calcBuf, readBuf)
	calcBuf[56] = 0
	calcBuf[57] = 0
	calcBuf[58] = 0
	calcBuf[59] = 0
	actualCRC := crc32.Checksum(calcBuf, crc32.MakeTable(crc32.Castagnoli))

	if storedCRC == actualCRC {
		t.Fatalf("expected torn block to fail CRC verification")
	}
}

func TestFaultFile_WriteAheadRuleAssertion(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)
	faultFS.SetMaxFsyncedLSN(50) // WAL only synced up to 50

	file, _ := faultFS.Create("heap.db")
	defer file.Close()

	// Write block with LSN 60 (exceeds 50) -> should panic with write-ahead rule violation
	b0 := makeTestBlock(1, 60, 0, 1)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic on write-ahead rule violation")
		}
		errMsg := ""
		if err, ok := r.(error); ok {
			errMsg = err.Error()
		} else if s, ok := r.(string); ok {
			errMsg = s
		}
		if !strings.Contains(errMsg, "write-ahead rule violation") {
			t.Fatalf("unexpected panic message: %v", r)
		}
	}()

	_, _ = file.WriteAt(b0, 0)
}

func TestFaultFile_ReuseRuleAssertion(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)
	faultFS.SetMaxFsyncedLSN(200)

	file, _ := faultFS.Create("heap.db")
	defer file.Close()

	// Write block 5 as page 10, LSN 100, and Sync it so it becomes durable
	b5 := makeTestBlock(10, 100, 0, 1)
	_, _ = file.WriteAt(b5, 5*DefaultBlockSize)
	_ = file.Sync()

	// Now try to overwrite block 5 while it is the newest durable copy of page 10
	b5New := makeTestBlock(20, 110, 0, 1)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic on reuse rule violation")
		}
		errMsg := ""
		if err, ok := r.(error); ok {
			errMsg = err.Error()
		} else if s, ok := r.(string); ok {
			errMsg = s
		}
		if !strings.Contains(errMsg, "reuse rule violation") {
			t.Fatalf("unexpected panic message: %v", r)
		}
	}()

	_, _ = file.WriteAt(b5New, 5*DefaultBlockSize)
}

func TestFaultFile_InjectedErrors(t *testing.T) {
	memFS := NewMemFS()
	faultFS := NewFaultFS(memFS)

	file, _ := faultFS.Create("heap.db")
	defer file.Close()

	injectedErr := errors.New("injected disk failure")
	faultFS.SetWriteError(injectedErr)

	_, err := file.WriteAt([]byte("data"), 0)
	if !errors.Is(err, injectedErr) {
		t.Fatalf("expected injected write error, got %v", err)
	}

	faultFS.SetWriteError(nil)
	faultFS.SetSyncError(injectedErr)

	err = file.Sync()
	if !errors.Is(err, injectedErr) {
		t.Fatalf("expected injected sync error, got %v", err)
	}
}
