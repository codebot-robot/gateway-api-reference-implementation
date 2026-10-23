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
	"io"
	"os"
	"testing"
)

func TestMemFS_BasicCRUD(t *testing.T) {
	fs := NewMemFS()

	if err := fs.MkdirAll("testdir/subdir", 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	file, err := fs.Create("testdir/subdir/test.txt")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	payload := []byte("hello world diskstore")
	n, err := file.WriteAt(payload, 0)
	if err != nil || n != len(payload) {
		t.Fatalf("WriteAt failed: n=%d err=%v", n, err)
	}

	size, err := file.Size()
	if err != nil || size != int64(len(payload)) {
		t.Fatalf("Size mismatch: size=%d err=%v", size, err)
	}

	buf := make([]byte, len(payload))
	n, err = file.ReadAt(buf, 0)
	if err != nil || !bytes.Equal(buf, payload) {
		t.Fatalf("ReadAt mismatch: buf=%s err=%v", string(buf), err)
	}

	if err := file.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify reading closed file fails
	_, err = file.ReadAt(buf, 0)
	if err == nil {
		t.Fatalf("expected error reading closed file")
	}

	// Re-open
	f2, err := fs.Open("testdir/subdir/test.txt")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer f2.Close()

	n, err = f2.ReadAt(buf, 0)
	if err != nil || !bytes.Equal(buf, payload) {
		t.Fatalf("Re-opened ReadAt mismatch: buf=%s err=%v", string(buf), err)
	}

	// Truncate
	if err := f2.Truncate(5); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}
	size, _ = f2.Size()
	if size != 5 {
		t.Fatalf("expected size 5 after truncate, got %d", size)
	}

	// Read beyond EOF
	buf2 := make([]byte, 10)
	_, err = f2.ReadAt(buf2, 10)
	if err != io.EOF && !os.IsNotExist(err) {
		t.Fatalf("expected EOF reading beyond size, got %v", err)
	}
}

func TestMemFS_DirectoryOperations(t *testing.T) {
	fs := NewMemFS()
	_ = fs.MkdirAll("dir1", 0755)

	f1, _ := fs.Create("dir1/file1.txt")
	_, _ = f1.WriteAt([]byte("a"), 0)
	_ = f1.Close()

	f2, _ := fs.Create("dir1/file2.txt")
	_, _ = f2.WriteAt([]byte("bb"), 0)
	_ = f2.Close()

	if err := fs.SyncDir("dir1"); err != nil {
		t.Fatalf("SyncDir failed: %v", err)
	}
	if fs.DirSyncCount("dir1") != 1 {
		t.Fatalf("expected DirSyncCount=1, got %d", fs.DirSyncCount("dir1"))
	}

	entries, err := fs.ReadDir("dir1")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 2 || entries[0].Name() != "file1.txt" || entries[1].Name() != "file2.txt" {
		t.Fatalf("unexpected entries: %+v", entries)
	}

	if err := fs.Remove("dir1/file1.txt"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	entries, _ = fs.ReadDir("dir1")
	if len(entries) != 1 || entries[0].Name() != "file2.txt" {
		t.Fatalf("expected only file2.txt, got %+v", entries)
	}
}
