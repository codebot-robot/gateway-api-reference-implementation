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
	"testing"
)

func TestDirectIO_WritevAndReadv(t *testing.T) {
	memFS := NewMemFS()
	file, err := memFS.Create("directio_test.db")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer file.Close()

	dio := NewDirectIO()

	// 4 blocks
	iovWrite := make([][]byte, 4)
	for i := 0; i < 4; i++ {
		iovWrite[i] = make([]byte, BlockSize)
		iovWrite[i][0] = byte(i + 1)
		iovWrite[i][BlockSize-1] = byte(i + 10)
	}

	// Write at offset 4096 (block 1)
	if err := dio.Writev(file, BlockSize, iovWrite); err != nil {
		t.Fatalf("Writev failed: %v", err)
	}

	// Read back
	iovRead := make([][]byte, 4)
	for i := 0; i < 4; i++ {
		iovRead[i] = make([]byte, BlockSize)
	}

	if err := dio.Readv(file, BlockSize, iovRead); err != nil {
		t.Fatalf("Readv failed: %v", err)
	}

	for i := 0; i < 4; i++ {
		if !bytes.Equal(iovRead[i], iovWrite[i]) {
			t.Fatalf("block %d mismatch", i)
		}
	}
}
