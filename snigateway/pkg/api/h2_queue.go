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
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

const (
	// DefaultH2StreamQueueBound is the maximum bytes queued per direction (256 KiB).
	DefaultH2StreamQueueBound = 256 * 1024
)

var (
	pool32k = sync.Pool{
		New: func() any {
			b := make([]byte, 32*1024)
			return &b
		},
	}
	pool64k = sync.Pool{
		New: func() any {
			b := make([]byte, 64*1024)
			return &b
		},
	}
)

type chunk struct {
	data    []byte
	poolBuf *[]byte
}

// H2ChunkQueue is a bounded byte queue for HTTP/2 tunnel streams.
//
// Writers append chunks (copying data so the caller can reuse buffers).
// If the queue has reached maxBytes, Write blocks until space is freed (backpressure).
// Readers (Read or TakeAll) drain whatever chunks are already queued into contiguous
// frames without artificial delay or timers.
type H2ChunkQueue struct {
	maxBytes int

	mu            sync.Mutex
	cond          *sync.Cond
	chunks        []chunk
	headOffset    int
	queuedBytes   int
	closed        bool
	closeErr      error
	readDeadline  time.Time
	writeDeadline time.Time

	// OnWriteAccepted, if non-nil, is invoked after bytes have been accepted into the queue
	// before Write returns.
	OnWriteAccepted func(n int)
}

// NewH2ChunkQueue constructs an H2ChunkQueue with the given byte bound.
func NewH2ChunkQueue(maxBytes int) *H2ChunkQueue {
	if maxBytes <= 0 {
		maxBytes = DefaultH2StreamQueueBound
	}
	q := &H2ChunkQueue{
		maxBytes: maxBytes,
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// SetWriteDeadline updates the write deadline and notifies blocked writers.
func (q *H2ChunkQueue) SetWriteDeadline(t time.Time) {
	q.mu.Lock()
	q.writeDeadline = t
	q.cond.Broadcast()
	q.mu.Unlock()
}

// SetReadDeadline updates the read deadline and notifies blocked readers.
func (q *H2ChunkQueue) SetReadDeadline(t time.Time) {
	q.mu.Lock()
	q.readDeadline = t
	q.cond.Broadcast()
	q.mu.Unlock()
}

// Wake broadcasts on the condition variable to re-evaluate deadlines or conditions.
func (q *H2ChunkQueue) Wake() {
	q.mu.Lock()
	q.cond.Broadcast()
	q.mu.Unlock()
}

// allocChunk copies b into a buffer, taking from 32 KiB or 64 KiB pools when applicable.
func (q *H2ChunkQueue) allocChunk(b []byte) chunk {
	n := len(b)
	if n >= 16*1024 && n <= 32*1024 {
		bp := pool32k.Get().(*[]byte)
		buf := (*bp)[:cap(*bp)]
		copy(buf, b)
		return chunk{data: buf[:n], poolBuf: bp}
	}
	if n > 32*1024 && n <= 64*1024 {
		bp := pool64k.Get().(*[]byte)
		buf := (*bp)[:cap(*bp)]
		copy(buf, b)
		return chunk{data: buf[:n], poolBuf: bp}
	}
	buf := make([]byte, n)
	copy(buf, b)
	return chunk{data: buf}
}

func (q *H2ChunkQueue) putPoolBuf(bp *[]byte) {
	if bp == nil {
		return
	}
	if cap(*bp) == 32*1024 {
		pool32k.Put(bp)
	} else if cap(*bp) == 64*1024 {
		pool64k.Put(bp)
	}
}

// Write appends b to the queue. If the queue is at capacity, it blocks until space is freed,
// the write deadline passes, or the queue is closed.
func (q *H2ChunkQueue) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	q.mu.Lock()

	for {
		if q.closed {
			err := q.closeErr
			if err == nil {
				err = net.ErrClosed
			}
			q.mu.Unlock()
			return 0, err
		}
		if !q.writeDeadline.IsZero() && !time.Now().Before(q.writeDeadline) {
			q.mu.Unlock()
			return 0, os.ErrDeadlineExceeded
		}
		if q.queuedBytes == 0 || q.queuedBytes+len(b) <= q.maxBytes {
			break
		}
		q.cond.Wait()
	}

	wasEmpty := q.queuedBytes == 0

	c := q.allocChunk(b)
	q.chunks = append(q.chunks, c)
	q.queuedBytes += len(b)

	if wasEmpty {
		q.cond.Signal()
	}
	hook := q.OnWriteAccepted
	q.mu.Unlock()

	if hook != nil {
		hook(len(b))
	}
	return len(b), nil
}

// Read copies queued data from the head of the queue into p until p is full or the queue is empty.
// Implements io.Reader (used as HTTP request body).
func (q *H2ChunkQueue) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for q.queuedBytes == 0 {
		if q.closed {
			if q.closeErr != nil && !errors.Is(q.closeErr, io.EOF) && !errors.Is(q.closeErr, net.ErrClosed) {
				return 0, q.closeErr
			}
			return 0, io.EOF
		}
		if !q.readDeadline.IsZero() && !time.Now().Before(q.readDeadline) {
			return 0, os.ErrDeadlineExceeded
		}
		q.cond.Wait()
	}

	n := 0
	for len(q.chunks) > 0 && n < len(p) {
		c := &q.chunks[0]
		avail := len(c.data) - q.headOffset
		toCopy := min(avail, len(p)-n)
		copy(p[n:n+toCopy], c.data[q.headOffset:q.headOffset+toCopy])
		n += toCopy
		q.headOffset += toCopy
		q.queuedBytes -= toCopy

		if q.headOffset >= len(c.data) {
			if c.poolBuf != nil {
				q.putPoolBuf(c.poolBuf)
			}
			q.chunks[0] = chunk{}
			q.chunks = q.chunks[1:]
			q.headOffset = 0
		}
	}

	if len(q.chunks) == 0 {
		q.chunks = q.chunks[:0]
		q.headOffset = 0
	}

	q.cond.Broadcast()
	return n, nil
}

// TakeAll drains all queued bytes into *into, growing it to the backlog size if needed.
// Used by the backend drainer to coalesce all currently queued writes into one frame.
// Waits only for data or close so write deadlines on idle streams do not cause teardown.
func (q *H2ChunkQueue) TakeAll(into *[]byte) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.queuedBytes == 0 {
		if q.closed {
			if q.closeErr != nil && !errors.Is(q.closeErr, io.EOF) && !errors.Is(q.closeErr, net.ErrClosed) {
				return 0, q.closeErr
			}
			return 0, io.EOF
		}
		q.cond.Wait()
	}

	total := q.queuedBytes
	if cap(*into) < total {
		*into = make([]byte, total)
	} else {
		*into = (*into)[:total]
	}

	dst := *into
	off := 0
	for i := range q.chunks {
		c := &q.chunks[i]
		src := c.data[q.headOffset:]
		copy(dst[off:off+len(src)], src)
		off += len(src)
		q.headOffset = 0
		if c.poolBuf != nil {
			q.putPoolBuf(c.poolBuf)
		}
		*c = chunk{}
	}

	q.chunks = q.chunks[:0]
	q.headOffset = 0
	q.queuedBytes = 0

	q.cond.Broadcast()
	return total, nil
}

// Close closes the queue for writing. Remaining queued bytes can still be read.
// Once drained, subsequent reads return io.EOF.
func (q *H2ChunkQueue) Close() error {
	return q.CloseWithError(nil)
}

// CloseWithError marks the queue closed with an error.
func (q *H2ChunkQueue) CloseWithError(err error) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	q.closeErr = err
	q.cond.Broadcast()
	return nil
}

// Abort immediately discards all queued chunks, frees any pooled buffers,
// and unblocks any waiting writers or readers with err.
func (q *H2ChunkQueue) Abort(err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	if err == nil {
		err = net.ErrClosed
	}
	q.closeErr = err
	for i := range q.chunks {
		if q.chunks[i].poolBuf != nil {
			q.putPoolBuf(q.chunks[i].poolBuf)
		}
		q.chunks[i] = chunk{}
	}
	q.chunks = nil
	q.headOffset = 0
	q.queuedBytes = 0
	q.cond.Broadcast()
}

// QueuedBytes returns the total unread bytes in the queue.
func (q *H2ChunkQueue) QueuedBytes() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queuedBytes
}

// IsClosed reports whether the queue has been closed or aborted.
func (q *H2ChunkQueue) IsClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}
