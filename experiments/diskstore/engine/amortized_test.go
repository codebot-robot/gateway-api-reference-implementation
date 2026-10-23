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

package engine_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/gke-labs/gateway-api-reference-implementation/experiments/diskstore/engine"
)

func TestMemFS_AmortizedGrowth(t *testing.T) {
	memFS := engine.NewMemFS()
	f, err := memFS.OpenFile("test.dat", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	defer f.Close()

	chunk := bytes.Repeat([]byte("A"), 128)
	// Perform multiple sequential extending writes
	for i := 0; i < 1000; i++ {
		off := int64(i * len(chunk))
		n, err := f.WriteAt(chunk, off)
		if err != nil {
			t.Fatalf("WriteAt iteration %d failed: %v", i, err)
		}
		if n != len(chunk) {
			t.Fatalf("WriteAt iteration %d short write: %d", i, n)
		}
	}

	size, err := f.Size()
	if err != nil {
		t.Fatalf("Size failed: %v", err)
	}
	expectedSize := int64(1000 * len(chunk))
	if size != expectedSize {
		t.Fatalf("expected size %d, got %d", expectedSize, size)
	}

	// Verify reading back data
	readBuf := make([]byte, len(chunk))
	for i := 0; i < 1000; i++ {
		off := int64(i * len(chunk))
		n, err := f.ReadAt(readBuf, off)
		if err != nil {
			t.Fatalf("ReadAt iteration %d failed: %v", i, err)
		}
		if n != len(chunk) || !bytes.Equal(readBuf, chunk) {
			t.Fatalf("ReadAt iteration %d data mismatch", i)
		}
	}
}
