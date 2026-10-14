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

package api

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

func TestH2ChunkQueue_ProbeImmediate(t *testing.T) {
	q := NewH2ChunkQueue(256 * 1024)
	defer q.Close()

	// 13-byte write without waiting
	probe := []byte("hello 13 byte")
	n, err := q.Write(probe)
	if err != nil || n != len(probe) {
		t.Fatalf("Write failed: n=%d err=%v", n, err)
	}

	buf := make([]byte, 1024)
	t0 := time.Now()
	nr, err := q.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if time.Since(t0) > 10*time.Millisecond {
		t.Fatalf("Read took too long: %v", time.Since(t0))
	}
	if nr != len(probe) || string(buf[:nr]) != string(probe) {
		t.Fatalf("Read mismatch: got %q", string(buf[:nr]))
	}
}

func TestH2ChunkQueue_CoalesceRead(t *testing.T) {
	q := NewH2ChunkQueue(256 * 1024)
	defer q.Close()

	// Enqueue 4 chunks of 16 KiB
	chunkSize := 16 * 1024
	for i := 0; i < 4; i++ {
		b := bytes.Repeat([]byte{byte('a' + i)}, chunkSize)
		n, err := q.Write(b)
		if err != nil || n != chunkSize {
			t.Fatalf("Write failed: %v", err)
		}
	}

	if q.QueuedBytes() != 4*chunkSize {
		t.Fatalf("QueuedBytes expected %d, got %d", 4*chunkSize, q.QueuedBytes())
	}

	// Drain in one Read into a 128 KiB buffer
	buf := make([]byte, 128*1024)
	nr, err := q.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if nr != 4*chunkSize {
		t.Fatalf("expected %d bytes drained, got %d", 4*chunkSize, nr)
	}

	for i := 0; i < 4; i++ {
		expected := bytes.Repeat([]byte{byte('a' + i)}, chunkSize)
		got := buf[i*chunkSize : (i+1)*chunkSize]
		if !bytes.Equal(got, expected) {
			t.Fatalf("chunk %d mismatch", i)
		}
	}
}

func TestH2ChunkQueue_TakeAllCoalesce(t *testing.T) {
	q := NewH2ChunkQueue(256 * 1024)
	defer q.Close()

	chunkSize := 16 * 1024
	for i := 0; i < 4; i++ {
		b := bytes.Repeat([]byte{byte('0' + i)}, chunkSize)
		_, _ = q.Write(b)
	}

	var drainBuf []byte
	n, err := q.TakeAll(&drainBuf)
	if err != nil {
		t.Fatalf("TakeAll failed: %v", err)
	}
	if n != 4*chunkSize || len(drainBuf) != 4*chunkSize {
		t.Fatalf("TakeAll mismatch: n=%d len=%d", n, len(drainBuf))
	}
	if q.QueuedBytes() != 0 {
		t.Fatalf("expected queue to be empty, got %d", q.QueuedBytes())
	}
}

func TestH2ChunkQueue_PartialReads(t *testing.T) {
	q := NewH2ChunkQueue(256 * 1024)
	defer q.Close()

	payload := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	_, _ = q.Write(payload)

	var result []byte
	smallBuf := make([]byte, 5)
	for len(result) < len(payload) {
		nr, err := q.Read(smallBuf)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		result = append(result, smallBuf[:nr]...)
	}

	if !bytes.Equal(result, payload) {
		t.Fatalf("result mismatch: got %q, want %q", result, payload)
	}
}

func TestH2ChunkQueue_BackpressureAndCloseUnblock(t *testing.T) {
	bound := 64 * 1024 // 64 KiB
	q := NewH2ChunkQueue(bound)

	// Fill queue to capacity
	b := make([]byte, 16*1024)
	for i := 0; i < 4; i++ {
		n, err := q.Write(b)
		if err != nil || n != len(b) {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}

	// 5th write should block
	blocked := make(chan error, 1)
	go func() {
		_, err := q.Write(b)
		blocked <- err
	}()

	select {
	case err := <-blocked:
		t.Fatalf("expected write to block, returned: %v", err)
	case <-time.After(50 * time.Millisecond):
		// correctly blocked
	}

	// Close queue should unblock writer
	q.Abort(net.ErrClosed)

	select {
	case err := <-blocked:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("expected net.ErrClosed, got: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for writer to unblock")
	}
}

func TestH2ChunkQueue_Deadlines(t *testing.T) {
	q := NewH2ChunkQueue(256 * 1024)

	// Write deadline in past
	q.SetWriteDeadline(time.Unix(1, 0))
	_, err := q.Write([]byte("test"))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("expected os.ErrDeadlineExceeded, got: %v", err)
	}

	// Clear write deadline
	q.SetWriteDeadline(time.Time{})
	n, err := q.Write([]byte("test"))
	if err != nil || n != 4 {
		t.Fatalf("Write failed after clearing deadline: %v", err)
	}

	// Read data
	buf := make([]byte, 10)
	_, err = q.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	// Read deadline in past
	q.SetReadDeadline(time.Unix(1, 0))
	_, err = q.Read(buf)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("expected os.ErrDeadlineExceeded, got: %v", err)
	}
}

func TestH2ChunkQueue_ConcurrentStreaming(t *testing.T) {
	q := NewH2ChunkQueue(64 * 1024)

	totalBytes := 2 * 1024 * 1024 // 2 MB
	chunkSize := 8 * 1024

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer q.Close()

		buf := make([]byte, chunkSize)
		written := 0
		for written < totalBytes {
			toWrite := min(chunkSize, totalBytes-written)
			_, err := q.Write(buf[:toWrite])
			if err != nil {
				t.Errorf("Write error: %v", err)
				return
			}
			written += toWrite
		}
	}()

	var readBytes int
	go func() {
		defer wg.Done()

		var drainBuf []byte
		for {
			n, err := q.TakeAll(&drainBuf)
			readBytes += n
			if err != nil {
				if err != io.EOF {
					t.Errorf("TakeAll error: %v", err)
				}
				break
			}
		}
	}()

	wg.Wait()
	if readBytes != totalBytes {
		t.Fatalf("expected %d read bytes, got %d", totalBytes, readBytes)
	}
}

func TestH2ChunkQueue_TakeAllIdleWithWriteDeadline(t *testing.T) {
	q := NewH2ChunkQueue(256 * 1024)
	defer q.Close()

	// Set write deadline in the past on an empty queue
	q.SetWriteDeadline(time.Now().Add(-1 * time.Second))

	// TakeAll should NOT return ErrDeadlineExceeded when idle
	drainDone := make(chan error, 1)
	go func() {
		var drainBuf []byte
		n, err := q.TakeAll(&drainBuf)
		if err != nil {
			drainDone <- err
			return
		}
		if string(drainBuf[:n]) != "payload" {
			drainDone <- fmt.Errorf("unexpected payload: %q", string(drainBuf[:n]))
			return
		}
		drainDone <- nil
	}()

	select {
	case err := <-drainDone:
		t.Fatalf("TakeAll should not return on idle with write deadline, got: %v", err)
	case <-time.After(50 * time.Millisecond):
		// correctly still waiting
	}

	// Clearing write deadline allows Write to succeed
	q.SetWriteDeadline(time.Time{})
	if _, err := q.Write([]byte("payload")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("TakeAll failed: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for TakeAll to drain payload")
	}
}

func TestH2ChunkQueue_SmallChunkMemoryBound(t *testing.T) {
	q := NewH2ChunkQueue(256 * 1024)
	defer q.Close()

	// 1 KiB chunk should allocate exact slice without poolBuf
	chunk1k := make([]byte, 1024)
	c := q.allocChunk(chunk1k)
	if c.poolBuf != nil {
		t.Errorf("expected 1 KiB chunk to not use pooled buffer, got poolBuf!=nil")
	}
	if len(c.data) != 1024 || cap(c.data) != 1024 {
		t.Errorf("expected 1 KiB chunk cap=1024, got %d", cap(c.data))
	}

	// 16 KiB chunk (e.g. TLS record) should use pool32k
	chunk16k := make([]byte, 16*1024)
	c16 := q.allocChunk(chunk16k)
	if c16.poolBuf == nil {
		t.Errorf("expected 16 KiB chunk to use pool32k, got poolBuf=nil")
	}
	if c16.poolBuf != nil {
		q.putPoolBuf(c16.poolBuf)
	}
}
