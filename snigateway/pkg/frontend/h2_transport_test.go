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

package frontend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/api"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/tunnel"
	"golang.org/x/net/http2"
)

// helper to start a mock backend H2 server on a pipe or net.Listener
func startMockBackendH2(t *testing.T, maxConcurrentStreams uint32, handler http.Handler) (net.Listener, func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}

	h2Srv := api.DefaultH2Server()
	if maxConcurrentStreams > 0 {
		h2Srv.MaxConcurrentStreams = maxConcurrentStreams
	}

	stopCh := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				h2Srv.ServeConn(c, &http2.ServeConnOpts{
					Handler: handler,
				})
			}(conn)
		}
	}()

	cleanup := func() {
		close(stopCh)
		_ = ln.Close()
	}
	return ln, cleanup
}

func TestH2Transport_LeastLoadedSelection(t *testing.T) {
	var conn1Streams, conn2Streams atomic.Int32

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		target := r.Header.Get("X-Mock-Backend")
		if target == "conn1" {
			conn1Streams.Add(1)
			defer conn1Streams.Add(-1)
		} else {
			conn2Streams.Add(1)
			defer conn2Streams.Add(-1)
		}

		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}

		// Hold stream open until client closes it
		io.Copy(io.Discard, r.Body)
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	h2Tr := NewH2Transport()
	defer h2Tr.Close()

	sessID := "sess-least-loaded"
	h2Tr.RegisterSession(sessID)

	// Connect 2 pooled connections to the mock backend
	c1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c1 failed: %v", err)
	}
	defer c1.Close()
	if err := h2Tr.AddConn(sessID, c1); err != nil {
		t.Fatalf("AddConn c1 failed: %v", err)
	}

	c2, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c2 failed: %v", err)
	}
	defer c2.Close()
	if err := h2Tr.AddConn(sessID, c2); err != nil {
		t.Fatalf("AddConn c2 failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Open 4 streams concurrently
	var activeStreams []net.Conn
	defer func() {
		for _, s := range activeStreams {
			_ = s.Close()
		}
	}()

	for i := 0; i < 4; i++ {
		s, err := h2Tr.GetConn(ctx, sessID)
		if err != nil {
			t.Fatalf("GetConn %d failed: %v", i, err)
		}
		activeStreams = append(activeStreams, s)
	}

	// Verify least-loaded balanced the connections (2 streams per connection)
	conns := h2Tr.sessions[sessID]
	if len(conns) != 2 {
		t.Fatalf("expected 2 pooled conns, got %d", len(conns))
	}
	s0 := conns[0].Load()
	s1 := conns[1].Load()
	if s0 != 2 || s1 != 2 {
		t.Errorf("expected balanced streams (2, 2), got (%d, %d)", s0, s1)
	}
}

type delayedFailConn struct {
	net.Conn
	mu          sync.Mutex
	failTrigger bool
	headersSeen chan struct{}
	headersDone sync.Once
	readErrSeen chan struct{}
	readErrOnce sync.Once
	selected    atomic.Bool
}

func newDelayedFailConn(c net.Conn) *delayedFailConn {
	return &delayedFailConn{
		Conn:        c,
		headersSeen: make(chan struct{}),
		readErrSeen: make(chan struct{}),
	}
}

func (d *delayedFailConn) triggerFailAfterHeaders() {
	d.mu.Lock()
	d.failTrigger = true
	d.mu.Unlock()
}

func (d *delayedFailConn) triggerFailNow() {
	d.mu.Lock()
	d.failTrigger = true
	d.headersDone.Do(func() {
		close(d.headersSeen)
	})
	d.mu.Unlock()
	_ = d.Conn.Close()
}

func (d *delayedFailConn) Read(b []byte) (int, error) {
	d.mu.Lock()
	triggered := d.failTrigger
	d.mu.Unlock()

	if triggered {
		<-d.headersSeen
		d.readErrOnce.Do(func() {
			close(d.readErrSeen)
		})
		return 0, net.ErrClosed
	}

	n, err := d.Conn.Read(b)
	if err != nil {
		d.readErrOnce.Do(func() {
			close(d.readErrSeen)
		})
	}
	return n, err
}

func (d *delayedFailConn) Write(b []byte) (int, error) {
	d.mu.Lock()
	triggered := d.failTrigger
	d.mu.Unlock()

	if triggered {
		d.selected.Store(true)
		// Let the CONNECT request HEADERS frame be written successfully to wire
		n, err := d.Conn.Write(b)
		// Trigger the failure right as headers are written so 0-RTT returns streamConn on c1
		d.headersDone.Do(func() {
			close(d.headersSeen)
			_ = d.Conn.Close()
		})
		return n, err
	}

	return d.Conn.Write(b)
}

func TestH2Transport_FailoverBeforeFirstByte(t *testing.T) {
	// Mock handler that echoes data
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		buf := make([]byte, 1024)
		n, _ := r.Body.Read(buf)
		if n > 0 {
			w.Write(buf[:n])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	h2Tr := NewH2Transport()
	defer h2Tr.Close()

	sessID := "sess-failover"
	h2Tr.RegisterSession(sessID)

	// Dial 2 pooled connections
	rawC1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c1 failed: %v", err)
	}
	defer rawC1.Close()
	c1 := newDelayedFailConn(rawC1)
	if err := h2Tr.AddConn(sessID, c1); err != nil {
		t.Fatalf("AddConn c1 failed: %v", err)
	}

	c2, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c2 failed: %v", err)
	}
	defer c2.Close()
	if err := h2Tr.AddConn(sessID, c2); err != nil {
		t.Fatalf("AddConn c2 failed: %v", err)
	}

	// Trigger delayed failure on c1: socket is closed, but read error detection is delayed
	// until after GetConn selects c1 and writes CONNECT headers.
	c1.triggerFailAfterHeaders()

	// Immediately request a connection mid-burst; GetConn should failover
	// to c2 before first byte without failing
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	stream, err := h2Tr.GetConn(ctx, sessID)
	if err != nil {
		t.Fatalf("expected failover to succeed, got error: %v", err)
	}
	defer stream.Close()

	// Wait for c1's socket closure error to surface to its client conn
	<-c1.readErrSeen
	for _, c := range h2Tr.SessionConns(sessID) {
		if c.rawConn == c1 {
			for !c.cc.State().Closed {
				runtime.Gosched()
			}
		}
	}

	// Write and read first byte to verify stream is fully functional
	if _, err := stream.Write([]byte("ping")); err != nil {
		t.Fatalf("stream.Write failed: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(stream, buf); err != nil {
		t.Fatalf("stream.Read failed: %v", err)
	}
	if string(buf) != "ping" {
		t.Fatalf("expected ping echo, got %s", string(buf))
	}
	if !c1.selected.Load() {
		t.Fatal("expected c1 to be initially selected before failover")
	}
}

func TestH2Transport_FailoverBeforeFirstByte_ReadFirst(t *testing.T) {
	// Mock handler that greets on connect
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		_, _ = w.Write([]byte("welcome"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	h2Tr := NewH2Transport()
	defer h2Tr.Close()

	sessID := "sess-failover-read"
	h2Tr.RegisterSession(sessID)

	// Dial 2 pooled connections
	rawC1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c1 failed: %v", err)
	}
	defer rawC1.Close()
	c1 := newDelayedFailConn(rawC1)
	if err := h2Tr.AddConn(sessID, c1); err != nil {
		t.Fatalf("AddConn c1 failed: %v", err)
	}

	c2, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c2 failed: %v", err)
	}
	defer c2.Close()
	if err := h2Tr.AddConn(sessID, c2); err != nil {
		t.Fatalf("AddConn c2 failed: %v", err)
	}

	c1.triggerFailAfterHeaders()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	stream, err := h2Tr.GetConn(ctx, sessID)
	if err != nil {
		t.Fatalf("expected failover to succeed, got error: %v", err)
	}
	defer stream.Close()

	// Read first without writing anything: verify transparent reopen on c2
	buf := make([]byte, 7)
	if _, err := io.ReadFull(stream, buf); err != nil {
		t.Fatalf("stream.Read failed: %v", err)
	}
	if string(buf) != "welcome" {
		t.Fatalf("expected welcome, got %s", string(buf))
	}
	if !c1.selected.Load() {
		t.Fatal("expected c1 to be initially selected before failover")
	}
}

func TestH2Transport_CommittedStreamSurfacesError(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		buf := make([]byte, 1024)
		n, _ := r.Body.Read(buf)
		if n > 0 {
			_, _ = w.Write(buf[:n])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	h2Tr := NewH2Transport()
	defer h2Tr.Close()

	sessID := "sess-committed"
	h2Tr.RegisterSession(sessID)

	rawC1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c1 failed: %v", err)
	}
	defer rawC1.Close()
	c1 := newDelayedFailConn(rawC1)
	if err := h2Tr.AddConn(sessID, c1); err != nil {
		t.Fatalf("AddConn c1 failed: %v", err)
	}

	c2, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c2 failed: %v", err)
	}
	defer c2.Close()
	if err := h2Tr.AddConn(sessID, c2); err != nil {
		t.Fatalf("AddConn c2 failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	stream, err := h2Tr.GetConn(ctx, sessID)
	if err != nil {
		t.Fatalf("GetConn failed: %v", err)
	}
	defer stream.Close()

	// Perform initial write and read to commit stream and confirm 200 OK
	if _, err := stream.Write([]byte("ping")); err != nil {
		t.Fatalf("stream.Write failed: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(stream, buf); err != nil {
		t.Fatalf("stream.Read failed: %v", err)
	}
	if string(buf) != "ping" {
		t.Fatalf("got %s, want ping", string(buf))
	}

	// Trigger failure on c1's socket AFTER body bytes have been queued and stream committed
	c1.triggerFailNow()
	<-c1.readErrSeen
	for _, c := range h2Tr.SessionConns(sessID) {
		if c.rawConn == c1 {
			for !c.cc.State().Closed {
				runtime.Gosched()
			}
		}
	}

	// Further writes or reads on this stream must surface the error, not reopen
	_, writeErr := stream.Write([]byte("more-data"))
	_, readErr := stream.Read(buf)
	if writeErr == nil && readErr == nil {
		t.Error("expected error on stream after underlying connection closed post-commit, but both Write and Read succeeded")
	}

	// Verify c2 was not used for a reopen
	for _, c := range h2Tr.SessionConns(sessID) {
		if c.rawConn == c2 {
			if load := c.Load(); load != 0 {
				t.Errorf("expected c2 to have 0 streams, got %d", load)
			}
		}
	}
}

func TestH2Transport_Non200ResponseDoesNotDrainOrReopen(t *testing.T) {
	var connectCount atomic.Int32

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			connectCount.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			return
		}
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	h2Tr := NewH2Transport()
	defer h2Tr.Close()

	sessID := "sess-non200"
	h2Tr.RegisterSession(sessID)

	// Add 3 pooled connections
	var pooledConns []net.Conn
	defer func() {
		for _, c := range pooledConns {
			_ = c.Close()
		}
	}()
	for i := 0; i < 3; i++ {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatalf("Dial %d failed: %v", i, err)
		}
		pooledConns = append(pooledConns, c)
		if err := h2Tr.AddConn(sessID, c); err != nil {
			t.Fatalf("AddConn %d failed: %v", i, err)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	stream, err := h2Tr.GetConn(ctx, sessID)
	if err == nil {
		defer stream.Close()
		buf := make([]byte, 16)
		_, err = stream.Read(buf)
	}

	if err == nil {
		t.Fatal("expected non-200 error from CONNECT, got nil")
	}

	// Assert exactly 1 CONNECT was sent (no reopen attempts)
	if count := connectCount.Load(); count != 1 {
		t.Fatalf("expected exactly 1 CONNECT request, got %d", count)
	}

	// Assert pool size remains 3 (no connection was drained)
	conns := h2Tr.SessionConns(sessID)
	if len(conns) != 3 {
		t.Fatalf("expected pool size to remain 3, got %d", len(conns))
	}
}

func TestH2Transport_FailedReopenReturnsErrorAndDoesNotHang(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	h2Tr := NewH2Transport()
	defer h2Tr.Close()

	sessID := "sess-failed-reopen"
	h2Tr.RegisterSession(sessID)

	rawC1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c1 failed: %v", err)
	}
	defer rawC1.Close()
	c1 := newDelayedFailConn(rawC1)
	if err := h2Tr.AddConn(sessID, c1); err != nil {
		t.Fatalf("AddConn c1 failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// GetConn returns streamConn on c1 with 0-RTT
	stream, err := h2Tr.GetConn(ctx, sessID)
	if err != nil {
		t.Fatalf("GetConn failed: %v", err)
	}
	defer stream.Close()

	// Kill c1 before any body bytes are queued
	c1.triggerFailNow()
	<-c1.readErrSeen
	for _, c := range h2Tr.SessionConns(sessID) {
		if c.rawConn == c1 {
			for !c.cc.State().Closed {
				runtime.Gosched()
			}
		}
	}

	// Set a short read deadline so reopen fails fast (no other pooled conns exist)
	_ = stream.SetReadDeadline(time.Now().Add(25 * time.Millisecond))

	buf := make([]byte, 10)
	_, rerr := stream.Read(buf)
	if rerr == nil {
		t.Fatal("expected error from failed reopen, got nil")
	}

	// Clear deadline: verify a second Read does not hang waiting on respCh
	_ = stream.SetReadDeadline(time.Time{})
	secondReadDone := make(chan error, 1)
	go func() {
		_, err := stream.Read(buf)
		secondReadDone <- err
	}()

	select {
	case err := <-secondReadDone:
		if err == nil {
			t.Fatal("expected second Read to return error, got nil")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("second stream.Read hung indefinitely waiting on respCh")
	}
}

func TestH2Transport_ConcurrentFirstReadAndWrite_ResultArrivesWhileWaiting(t *testing.T) {
	resultGate := make(chan struct{})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		// Hold response until both Read and Write are waiting
		<-resultGate
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		buf := make([]byte, 1024)
		n, _ := r.Body.Read(buf)
		if n > 0 {
			_, _ = w.Write(buf[:n])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	h2Tr := NewH2Transport()
	defer h2Tr.Close()

	sessID := "sess-concurrent-waiting"
	h2Tr.RegisterSession(sessID)

	c1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c1 failed: %v", err)
	}
	defer c1.Close()
	if err := h2Tr.AddConn(sessID, c1); err != nil {
		t.Fatalf("AddConn c1 failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	stream, err := h2Tr.GetConn(ctx, sessID)
	if err != nil {
		t.Fatalf("GetConn failed: %v", err)
	}
	defer stream.Close()

	readDone := make(chan error, 1)
	readStarted := make(chan struct{})
	buf := make([]byte, 4)
	go func() {
		close(readStarted)
		_, rerr := io.ReadFull(stream, buf)
		readDone <- rerr
	}()

	writeDone := make(chan error, 1)
	writeStarted := make(chan struct{})
	go func() {
		close(writeStarted)
		_, werr := stream.Write([]byte("ping"))
		writeDone <- werr
	}()

	<-readStarted
	<-writeStarted

	// Give both goroutines time to reach their wait on att.done / pollResult
	time.Sleep(10 * time.Millisecond)

	// Release the response: att.done will close while both Read and Write are waiting
	close(resultGate)

	select {
	case werr := <-writeDone:
		if werr != nil {
			t.Fatalf("stream.Write failed: %v", werr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream.Write hung waiting for response")
	}

	select {
	case rerr := <-readDone:
		if rerr != nil {
			t.Fatalf("stream.Read failed: %v", rerr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream.Read hung waiting for response")
	}

	if string(buf) != "ping" {
		t.Fatalf("expected ping echo, got %s", string(buf))
	}
}

func TestH2Transport_QueueClosedBeforeFirstWrite_Reopens(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		buf := make([]byte, 1024)
		n, _ := r.Body.Read(buf)
		if n > 0 {
			_, _ = w.Write(buf[:n])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	h2Tr := NewH2Transport()
	defer h2Tr.Close()

	sessID := "sess-queue-closed-first-write"
	h2Tr.RegisterSession(sessID)

	c1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c1 failed: %v", err)
	}
	defer c1.Close()
	if err := h2Tr.AddConn(sessID, c1); err != nil {
		t.Fatalf("AddConn c1 failed: %v", err)
	}

	c2, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c2 failed: %v", err)
	}
	defer c2.Close()
	if err := h2Tr.AddConn(sessID, c2); err != nil {
		t.Fatalf("AddConn c2 failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	stream, err := h2Tr.GetConn(ctx, sessID)
	if err != nil {
		t.Fatalf("GetConn failed: %v", err)
	}
	defer stream.Close()

	// Simulate queue being closed/aborted right before the first queued write
	// when the connection has failed: zero bytes have been accepted into the queue
	h2sc := stream.(*h2StreamConn)
	att := h2sc.attempt.Load()
	att.hc.closed.Store(true)
	att.queue.Abort(net.ErrClosed)

	// Write must recognize that 0 bytes were accepted and transparently reopen on c2
	if _, err := stream.Write([]byte("ping")); err != nil {
		t.Fatalf("stream.Write failed to reopen: %v", err)
	}

	buf := make([]byte, 4)
	if _, err := io.ReadFull(stream, buf); err != nil {
		t.Fatalf("stream.Read failed: %v", err)
	}
	if string(buf) != "ping" {
		t.Fatalf("expected ping echo from reopened connection, got %s", string(buf))
	}

	// Verify the active attempt was swapped to a new attempt
	if h2sc.attempt.Load() == att {
		t.Fatal("expected attempt to be replaced after transparent reopen")
	}
}

func testConcurrentFirstReadAndWriteFailover(t *testing.T, dieBefore200 bool) {
	var c1Requests atomic.Int32
	c1Gate := make(chan struct{})
	c1Sent200 := make(chan struct{})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		reqNum := c1Requests.Add(1)
		if reqNum == 1 && dieBefore200 {
			// Hold the response on c1 until c1 is killed
			<-c1Gate
		}
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		if reqNum == 1 && !dieBefore200 {
			close(c1Sent200)
		}
		buf := make([]byte, 1024)
		n, _ := r.Body.Read(buf)
		if n > 0 {
			_, _ = w.Write(buf[:n])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	h2Tr := NewH2Transport()
	defer h2Tr.Close()

	sessID := fmt.Sprintf("sess-concurrent-failover-%v", dieBefore200)
	h2Tr.RegisterSession(sessID)

	rawC1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c1 failed: %v", err)
	}
	defer rawC1.Close()
	c1 := newDelayedFailConn(rawC1)
	if err := h2Tr.AddConn(sessID, c1); err != nil {
		t.Fatalf("AddConn c1 failed: %v", err)
	}

	c2, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c2 failed: %v", err)
	}
	defer c2.Close()
	if err := h2Tr.AddConn(sessID, c2); err != nil {
		t.Fatalf("AddConn c2 failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	stream, err := h2Tr.GetConn(ctx, sessID)
	if err != nil {
		t.Fatalf("GetConn failed: %v", err)
	}
	defer stream.Close()

	h2sc := stream.(*h2StreamConn)
	att := h2sc.attempt.Load()

	if !dieBefore200 {
		// Ordering 2: Gate to ensure the 200 arrives at client before c1 dies
		<-c1Sent200
		<-att.done
	}

	// Kill c1 before first byte
	c1.triggerFailNow()
	if dieBefore200 {
		close(c1Gate)
	}
	<-c1.readErrSeen
	for _, c := range h2Tr.SessionConns(sessID) {
		if c.rawConn == c1 {
			for !c.cc.State().Closed {
				runtime.Gosched()
			}
		}
	}

	readDone := make(chan error, 1)
	buf := make([]byte, 4)
	go func() {
		_, rerr := io.ReadFull(stream, buf)
		readDone <- rerr
	}()

	writeDone := make(chan error, 1)
	go func() {
		_, werr := stream.Write([]byte("ping"))
		writeDone <- werr
	}()

	select {
	case werr := <-writeDone:
		if werr != nil {
			t.Fatalf("stream.Write failed during concurrent failover: %v", werr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream.Write hung during concurrent failover")
	}

	select {
	case rerr := <-readDone:
		if rerr != nil {
			t.Fatalf("stream.Read failed during concurrent failover: %v", rerr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream.Read hung during concurrent failover")
	}

	if string(buf) != "ping" {
		t.Fatalf("expected ping echo, got %s", string(buf))
	}
}

func TestH2Transport_ConcurrentFirstReadAndWrite_Failover(t *testing.T) {
	t.Run("DiesBefore200", func(t *testing.T) {
		testConcurrentFirstReadAndWriteFailover(t, true)
	})
	t.Run("DiesAfter200BeforeBytes", func(t *testing.T) {
		testConcurrentFirstReadAndWriteFailover(t, false)
	})
}

func TestH2Transport_ResponseBytesDelivered_DoesNotReopen(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Server-first protocol: send greeting immediately
		_, _ = w.Write([]byte("greeting"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	h2Tr := NewH2Transport()
	defer h2Tr.Close()

	sessID := "sess-response-delivered-no-reopen"
	h2Tr.RegisterSession(sessID)

	rawC1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c1 failed: %v", err)
	}
	defer rawC1.Close()
	c1 := newDelayedFailConn(rawC1)
	if err := h2Tr.AddConn(sessID, c1); err != nil {
		t.Fatalf("AddConn c1 failed: %v", err)
	}

	c2, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c2 failed: %v", err)
	}
	defer c2.Close()
	if err := h2Tr.AddConn(sessID, c2); err != nil {
		t.Fatalf("AddConn c2 failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	stream, err := h2Tr.GetConn(ctx, sessID)
	if err != nil {
		t.Fatalf("GetConn failed: %v", err)
	}
	defer stream.Close()

	h2sc := stream.(*h2StreamConn)
	att := h2sc.attempt.Load()

	// Read greeting from server: delivers response bytes to caller
	buf := make([]byte, 8)
	n, err := io.ReadFull(stream, buf)
	if err != nil {
		t.Fatalf("initial ReadFull failed: %v", err)
	}
	if string(buf[:n]) != "greeting" {
		t.Fatalf("got %q, want greeting", string(buf[:n]))
	}

	// Verify responseDelivered is set to true
	h2sc.mu.Lock()
	delivered := h2sc.responseDelivered
	h2sc.mu.Unlock()
	if !delivered {
		t.Fatal("expected responseDelivered to be true after reading response bytes")
	}

	// Now fail c1's transport
	c1.triggerFailNow()
	<-c1.readErrSeen
	for _, c := range h2Tr.SessionConns(sessID) {
		if c.rawConn == c1 {
			for !c.cc.State().Closed {
				runtime.Gosched()
			}
		}
	}

	// Subsequent Read or Write must fail and must NOT reopen on c2
	readBuf := make([]byte, 10)
	_, readErr := stream.Read(readBuf)
	if readErr == nil {
		t.Fatal("expected Read after connection failure to return error, got nil")
	}

	// Assert c2 has 0 load (no reopen was attempted)
	for _, c := range h2Tr.SessionConns(sessID) {
		if c.rawConn == c2 {
			if load := c.Load(); load != 0 {
				t.Fatalf("expected c2 load to be 0 (no reopen), got %d", load)
			}
		}
	}

	// Assert the stream attempt is still att (not replaced!)
	if h2sc.attempt.Load() != att {
		t.Fatal("stream attempt was unexpectedly replaced; reopen occurred after response bytes were delivered")
	}
}

func TestH2Transport_BufferedResponseDrainsAfterConnectionDies(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		_, _ = w.Write([]byte("abcdef"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	h2Tr := NewH2Transport()
	defer h2Tr.Close()

	sessID := "sess-buffered-response-drain"
	h2Tr.RegisterSession(sessID)

	rawC1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c1 failed: %v", err)
	}
	defer rawC1.Close()
	c1 := newDelayedFailConn(rawC1)
	if err := h2Tr.AddConn(sessID, c1); err != nil {
		t.Fatalf("AddConn c1 failed: %v", err)
	}

	c2, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c2 failed: %v", err)
	}
	defer c2.Close()
	if err := h2Tr.AddConn(sessID, c2); err != nil {
		t.Fatalf("AddConn c2 failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	stream, err := h2Tr.GetConn(ctx, sessID)
	if err != nil {
		t.Fatalf("GetConn failed: %v", err)
	}
	defer stream.Close()

	// Read one byte
	buf := make([]byte, 1)
	n, err := stream.Read(buf)
	if err != nil {
		t.Fatalf("first Read failed: %v", err)
	}
	if n != 1 || string(buf[:n]) != "a" {
		t.Fatalf("first Read got %q (len %d), want %q", string(buf[:n]), n, "a")
	}

	// Kill c1 after the first byte was read. The remaining bytes "bcdef" are already buffered in the client response body.
	c1.triggerFailNow()
	<-c1.readErrSeen
	for _, c := range h2Tr.SessionConns(sessID) {
		if c.rawConn == c1 {
			for !c.cc.State().Closed {
				runtime.Gosched()
			}
		}
	}

	// Second Read must return 'b', not ErrClosed
	n, err = stream.Read(buf)
	if err != nil {
		t.Fatalf("second Read failed: %v", err)
	}
	if n != 1 || string(buf[:n]) != "b" {
		t.Fatalf("second Read got %q (len %d), want %q", string(buf[:n]), n, "b")
	}

	// Remaining buffered bytes must drain completely
	rest := make([]byte, 10)
	n, err = io.ReadFull(stream, rest[:4])
	if err != nil {
		t.Fatalf("reading rest of buffered bytes failed: %v", err)
	}
	if string(rest[:n]) != "cdef" {
		t.Fatalf("got %q, want %q", string(rest[:n]), "cdef")
	}

	// Once buffered data is drained, subsequent Read must surface the transport error
	_, readErr := stream.Read(rest)
	if readErr == nil {
		t.Fatal("expected Read after draining buffer to return error, got nil")
	}

	// Verify c2 was not reopened
	for _, c := range h2Tr.SessionConns(sessID) {
		if c.rawConn == c2 {
			if load := c.Load(); load != 0 {
				t.Fatalf("expected c2 load to be 0 (no reopen), got %d", load)
			}
		}
	}
}

func TestH2Transport_CleanEOF_DoesNotDrainOrReopen(t *testing.T) {
	var connectCount atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		connectCount.Add(1)
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Return immediately: clean EOF with 0 body bytes
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	h2Tr := NewH2Transport()
	defer h2Tr.Close()

	sessID := "sess-clean-eof"
	h2Tr.RegisterSession(sessID)

	var rawConns1 []net.Conn
	defer func() {
		for _, c := range rawConns1 {
			c.Close()
		}
	}()
	for i := 1; i <= 3; i++ {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatalf("Dial c%d failed: %v", i, err)
		}
		rawConns1 = append(rawConns1, c)
		if err := h2Tr.AddConn(sessID, c); err != nil {
			t.Fatalf("AddConn c%d failed: %v", i, err)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	stream, err := h2Tr.GetConn(ctx, sessID)
	if err != nil {
		t.Fatalf("GetConn failed: %v", err)
	}
	defer stream.Close()

	buf := make([]byte, 10)
	n, rerr := stream.Read(buf)
	if n != 0 {
		t.Fatalf("expected 0 bytes read on EOF, got %d", n)
	}
	if !errors.Is(rerr, io.EOF) {
		t.Fatalf("expected io.EOF error, got %v", rerr)
	}

	// Assert exactly 1 CONNECT was sent (no transparent reopen was attempted)
	if cnt := connectCount.Load(); cnt != 1 {
		t.Fatalf("expected 1 CONNECT request, got %d", cnt)
	}

	// Assert pool size is still 3 (no healthy connection was drained)
	if poolSize := len(h2Tr.SessionConns(sessID)); poolSize != 3 {
		t.Fatalf("expected pool size of 3, got %d", poolSize)
	}
}

func TestH2Transport_StreamReset_DoesNotDrainOrReopen(t *testing.T) {
	var connectCount atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		connectCount.Add(1)
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Abort handler causes server to send RST_STREAM
		panic(http.ErrAbortHandler)
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	h2Tr := NewH2Transport()
	defer h2Tr.Close()

	sessID := "sess-stream-reset"
	h2Tr.RegisterSession(sessID)

	var rawConns2 []net.Conn
	defer func() {
		for _, c := range rawConns2 {
			c.Close()
		}
	}()
	for i := 1; i <= 3; i++ {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatalf("Dial c%d failed: %v", i, err)
		}
		rawConns2 = append(rawConns2, c)
		if err := h2Tr.AddConn(sessID, c); err != nil {
			t.Fatalf("AddConn c%d failed: %v", i, err)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	stream, err := h2Tr.GetConn(ctx, sessID)
	if err != nil {
		t.Fatalf("GetConn failed: %v", err)
	}
	defer stream.Close()

	buf := make([]byte, 10)
	n, rerr := stream.Read(buf)
	if n != 0 {
		t.Fatalf("expected 0 bytes read on stream reset, got %d", n)
	}
	var se http2.StreamError
	if !errors.As(rerr, &se) {
		t.Fatalf("expected http2.StreamError, got %v (%T)", rerr, rerr)
	}

	// Assert exactly 1 CONNECT was sent (no transparent reopen was attempted)
	if cnt := connectCount.Load(); cnt != 1 {
		t.Fatalf("expected 1 CONNECT request, got %d", cnt)
	}

	// Assert pool size is still 3 (no healthy connection was drained)
	if poolSize := len(h2Tr.SessionConns(sessID)); poolSize != 3 {
		t.Fatalf("expected pool size of 3, got %d", poolSize)
	}
}

func TestH2Transport_CleanEOF_WriteFirst_DoesNotDrainOrReopen(t *testing.T) {
	var connectCount atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		connectCount.Add(1)
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Return immediately: clean EOF with 0 body bytes
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	h2Tr := NewH2Transport()
	defer h2Tr.Close()

	sessID := "sess-clean-eof-write-first"
	h2Tr.RegisterSession(sessID)

	var rawConns []net.Conn
	defer func() {
		for _, c := range rawConns {
			c.Close()
		}
	}()
	for i := 1; i <= 3; i++ {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatalf("Dial c%d failed: %v", i, err)
		}
		rawConns = append(rawConns, c)
		if err := h2Tr.AddConn(sessID, c); err != nil {
			t.Fatalf("AddConn c%d failed: %v", i, err)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	stream, err := h2Tr.GetConn(ctx, sessID)
	if err != nil {
		t.Fatalf("GetConn failed: %v", err)
	}
	defer stream.Close()

	h2sc := stream.(*h2StreamConn)
	att := h2sc.attempt.Load()
	<-att.done

	// Wait until the stream end has closed the queue before the first write
	for {
		att.queue.Close()
		break
	}

	n, werr := stream.Write([]byte("ping"))
	if n != 0 {
		t.Fatalf("expected 0 bytes written on closed stream, got %d", n)
	}
	if werr == nil {
		t.Fatal("expected error on write to closed stream, got nil")
	}

	// Assert exactly 1 CONNECT was sent (no transparent reopen was attempted)
	if cnt := connectCount.Load(); cnt != 1 {
		t.Fatalf("expected 1 CONNECT request, got %d", cnt)
	}

	// Assert pool size is still 3 (no healthy connection was drained)
	if poolSize := len(h2Tr.SessionConns(sessID)); poolSize != 3 {
		t.Fatalf("expected pool size of 3, got %d", poolSize)
	}
}

func TestH2Transport_StreamReset_WriteFirst_DoesNotDrainOrReopen(t *testing.T) {
	var connectCount atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		connectCount.Add(1)
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Abort handler causes server to send RST_STREAM
		panic(http.ErrAbortHandler)
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	h2Tr := NewH2Transport()
	defer h2Tr.Close()

	sessID := "sess-stream-reset-write-first"
	h2Tr.RegisterSession(sessID)

	var rawConns []net.Conn
	defer func() {
		for _, c := range rawConns {
			c.Close()
		}
	}()
	for i := 1; i <= 3; i++ {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatalf("Dial c%d failed: %v", i, err)
		}
		rawConns = append(rawConns, c)
		if err := h2Tr.AddConn(sessID, c); err != nil {
			t.Fatalf("AddConn c%d failed: %v", i, err)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	stream, err := h2Tr.GetConn(ctx, sessID)
	if err != nil {
		t.Fatalf("GetConn failed: %v", err)
	}
	defer stream.Close()

	h2sc := stream.(*h2StreamConn)
	att := h2sc.attempt.Load()
	<-att.done

	// The stream reset before the first write closes the request queue
	att.queue.Abort(net.ErrClosed)

	n, werr := stream.Write([]byte("ping"))
	if n != 0 {
		t.Fatalf("expected 0 bytes written on reset stream, got %d", n)
	}
	if werr == nil {
		t.Fatal("expected error on write to reset stream, got nil")
	}

	// Assert exactly 1 CONNECT was sent (no transparent reopen was attempted)
	if cnt := connectCount.Load(); cnt != 1 {
		t.Fatalf("expected 1 CONNECT request, got %d", cnt)
	}

	// Assert pool size is still 3 (no healthy connection was drained)
	if poolSize := len(h2Tr.SessionConns(sessID)); poolSize != 3 {
		t.Fatalf("expected pool size of 3, got %d", poolSize)
	}
}

func TestH2Transport_ConcurrentFirstWriteAccepted_ReadDoesNotReopen(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	h2Tr := NewH2Transport()
	defer h2Tr.Close()

	sessID := "sess-write-accepted-no-reopen"
	h2Tr.RegisterSession(sessID)

	rawC1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c1 failed: %v", err)
	}
	defer rawC1.Close()
	c1 := newDelayedFailConn(rawC1)
	if err := h2Tr.AddConn(sessID, c1); err != nil {
		t.Fatalf("AddConn c1 failed: %v", err)
	}

	c2, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c2 failed: %v", err)
	}
	defer c2.Close()
	if err := h2Tr.AddConn(sessID, c2); err != nil {
		t.Fatalf("AddConn c2 failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	stream, err := h2Tr.GetConn(ctx, sessID)
	if err != nil {
		t.Fatalf("GetConn failed: %v", err)
	}
	defer stream.Close()

	h2sc := stream.(*h2StreamConn)
	att := h2sc.attempt.Load()

	bytesAccepted := make(chan struct{})
	releaseWrite := make(chan struct{})
	att.queue.OnWriteAccepted = func(n int) {
		close(bytesAccepted)
		<-releaseWrite
	}

	writeDone := make(chan error, 1)
	go func() {
		_, werr := stream.Write([]byte("ping"))
		writeDone <- werr
	}()

	// Wait until the write has accepted bytes into the queue and is held
	<-bytesAccepted

	// Fail the transport on c1 while the first write is held inside the queue
	c1.triggerFailNow()
	<-c1.readErrSeen
	for _, c := range h2Tr.SessionConns(sessID) {
		if c.rawConn == c1 {
			for !c.cc.State().Closed {
				runtime.Gosched()
			}
		}
	}

	// Concurrent Read runs while first write is held: it must wait for the write
	// and must NOT reopen because bytes were already accepted.
	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 10)
		_, rerr := stream.Read(buf)
		readDone <- rerr
	}()

	// Small pause to ensure Read has reached awaitResponse and is waiting on firstWriting
	time.Sleep(10 * time.Millisecond)

	// Release the held write
	close(releaseWrite)

	// Write must complete
	select {
	case werr := <-writeDone:
		_ = werr
	case <-time.After(2 * time.Second):
		t.Fatal("stream.Write hung")
	}

	// Read must return an error and MUST NOT have reopened on c2
	select {
	case rerr := <-readDone:
		if rerr == nil {
			t.Fatal("expected Read to fail with error, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream.Read hung")
	}

	// Assert c2 has 0 streams (no reopen was attempted!)
	for _, c := range h2Tr.SessionConns(sessID) {
		if c.rawConn == c2 {
			if load := c.Load(); load != 0 {
				t.Fatalf("expected c2 load to be 0 (no reopen), got %d", load)
			}
		}
	}

	// Assert the stream attempt is still att (not replaced!)
	if h2sc.attempt.Load() != att {
		t.Fatal("stream attempt was unexpectedly replaced; reopen occurred after bytes were accepted")
	}
}

func TestH2Transport_GOAWAYDrain(t *testing.T) {
	var inFlightDone sync.WaitGroup
	inFlightDone.Add(1)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}

		// Read client data
		buf := make([]byte, 1024)
		n, _ := r.Body.Read(buf)
		w.Write([]byte(fmt.Sprintf("echo: %s", string(buf[:n]))))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		inFlightDone.Done()
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	h2Tr := NewH2Transport(WithH2DrainTimeout(2 * time.Second))
	defer h2Tr.Close()

	sessID := "sess-goaway-drain"
	h2Tr.RegisterSession(sessID)

	c1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c1 failed: %v", err)
	}
	defer c1.Close()

	if err := h2Tr.AddConn(sessID, c1); err != nil {
		t.Fatalf("AddConn c1 failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Start in-flight stream
	stream, err := h2Tr.GetConn(ctx, sessID)
	if err != nil {
		t.Fatalf("GetConn failed: %v", err)
	}
	defer stream.Close()

	conns := h2Tr.sessions[sessID]
	if len(conns) != 1 {
		t.Fatalf("expected 1 conn, got %d", len(conns))
	}
	hc := conns[0]

	// Initiate drain on connection
	h2Tr.drainConn(hc)

	// Verify connection is marked draining and cannot take new requests
	if hc.CanTakeNewRequest() {
		t.Errorf("expected CanTakeNewRequest() == false on draining conn")
	}

	// Verify new GetConn on this session does NOT pick the draining conn (queues or times out)
	shortCtx, shortCancel := context.WithTimeout(t.Context(), 15*time.Millisecond)
	_, err = h2Tr.GetConn(shortCtx, sessID)
	shortCancel()
	if err == nil {
		t.Errorf("expected GetConn to block on draining conn without capacity, but succeeded")
	}

	// Complete the in-flight stream and verify it finishes successfully
	if _, err := stream.Write([]byte("in-flight")); err != nil {
		t.Fatalf("stream.Write failed: %v", err)
	}

	buf := make([]byte, 1024)
	n, err := stream.Read(buf)
	if err != nil {
		t.Fatalf("stream.Read failed: %v", err)
	}
	if string(buf[:n]) != "echo: in-flight" {
		t.Errorf("expected echo: in-flight, got %s", string(buf[:n]))
	}

	inFlightDone.Wait()
}

func TestH2Transport_MaxConcurrentStreamsQueuing(t *testing.T) {
	// Mock backend with MaxConcurrentStreams = 2
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Hold until closed
		io.Copy(io.Discard, r.Body)
	})

	ln, cleanup := startMockBackendH2(t, 2, handler)
	defer cleanup()

	h2Tr := NewH2Transport()
	defer h2Tr.Close()

	sessID := "sess-stream-cap"
	h2Tr.RegisterSession(sessID)

	c1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c1 failed: %v", err)
	}
	defer c1.Close()
	if err := h2Tr.AddConn(sessID, c1); err != nil {
		t.Fatalf("AddConn c1 failed: %v", err)
	}

	// Wait for server SETTINGS frame to take effect
	for {
		h2Tr.mu.Lock()
		conns := h2Tr.sessions[sessID]
		h2Tr.mu.Unlock()
		if len(conns) > 0 && conns[0].cc.State().MaxConcurrentStreams == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Fill capacity (2 concurrent streams)
	s1, err := h2Tr.GetConn(ctx, sessID)
	if err != nil {
		t.Fatalf("s1 GetConn failed: %v", err)
	}
	defer s1.Close()

	s2, err := h2Tr.GetConn(ctx, sessID)
	if err != nil {
		t.Fatalf("s2 GetConn failed: %v", err)
	}
	defer s2.Close()

	// 3rd stream must queue until capacity becomes available
	queuedDone := make(chan struct{})
	var s3 net.Conn
	var s3Err error

	go func() {
		defer close(queuedDone)
		s3, s3Err = h2Tr.GetConn(ctx, sessID)
	}()

	select {
	case <-queuedDone:
		t.Fatalf("3rd stream should have queued behind MaxConcurrentStreams limit, but returned immediately")
	case <-time.After(20 * time.Millisecond):
		// Expected to queue
	}

	// Close s1, freeing capacity
	_ = s1.Close()

	select {
	case <-queuedDone:
		if s3Err != nil {
			t.Fatalf("queued stream failed: %v", s3Err)
		}
		if s3 == nil {
			t.Fatalf("queued stream returned nil conn")
		}
		defer s3.Close()
	case <-time.After(1 * time.Second):
		t.Fatalf("queued stream did not unblock after stream closure")
	}
}

func TestH2Transport_IdlePing(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	// Use very short ping interval for test
	h2Tr := NewH2Transport(WithH2PingInterval(10 * time.Millisecond))
	defer h2Tr.Close()

	sessID := "sess-idle-ping"
	h2Tr.RegisterSession(sessID)

	c1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c1 failed: %v", err)
	}
	defer c1.Close()
	if err := h2Tr.AddConn(sessID, c1); err != nil {
		t.Fatalf("AddConn c1 failed: %v", err)
	}

	// Allow several pings to execute on idle connection
	time.Sleep(30 * time.Millisecond)

	conns := h2Tr.sessions[sessID]
	if len(conns) != 1 {
		t.Fatalf("expected 1 conn still healthy after idle pings, got %d", len(conns))
	}
	if !conns[0].CanTakeNewRequest() {
		t.Errorf("connection should still be healthy and able to take requests")
	}
}

func TestH2StreamConn_WriteToAndReadFrom(t *testing.T) {
	queue := api.NewH2ChunkQueue(api.DefaultH2StreamQueueBound)
	go func() {
		_, _ = io.Copy(io.Discard, queue)
	}()

	prRes, pwRes := io.Pipe()

	streamCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	conn := newH2StreamConn(queue, cancel, &net.TCPAddr{Port: 1234}, &net.TCPAddr{Port: 5678}, nil, nil)
	att := conn.attempt.Load()
	att.result = &rtResult{
		res: &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       prRes,
		},
	}
	att.resBody = prRes
	close(att.done)
	defer conn.Close()

	// Verify WriteTo
	go func() {
		_, _ = pwRes.Write([]byte("hello from response"))
		_ = pwRes.Close()
	}()

	wt, ok := any(conn).(io.WriterTo)
	if !ok {
		t.Fatalf("expected h2StreamConn to implement io.WriterTo")
	}
	var out bytes.Buffer
	written, err := wt.WriteTo(&out)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}
	if out.String() != "hello from response" || written != int64(len("hello from response")) {
		t.Errorf("WriteTo mismatch: got %q, written=%d", out.String(), written)
	}

	// Verify ReadFrom
	rf, ok := any(conn).(io.ReaderFrom)
	if !ok {
		t.Fatalf("expected h2StreamConn to implement io.ReaderFrom")
	}

	pipeReader, pipeWriter := io.Pipe()
	go func() {
		_, _ = pipeWriter.Write([]byte("request data"))
		_ = pipeWriter.Close()
	}()

	written, err = rf.ReadFrom(pipeReader)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if written != int64(len("request data")) {
		t.Errorf("ReadFrom mismatch: written=%d", written)
	}

	// Verify SetReadDeadline non-destructive behavior
	aLongTimeAgo := time.Unix(1, 0)
	if err := conn.SetReadDeadline(aLongTimeAgo); err != nil {
		t.Fatalf("SetReadDeadline failed: %v", err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear SetReadDeadline failed: %v", err)
	}
	_ = streamCtx
}

func TestH2StreamConn_StreamingProbe(t *testing.T) {
	queue := api.NewH2ChunkQueue(api.DefaultH2StreamQueueBound)

	_, cancel := context.WithCancel(t.Context())
	defer cancel()

	conn := newH2StreamConn(queue, cancel, nil, nil, nil, nil)
	defer conn.Close()

	// Probe: a 13-byte Write without concurrent Read is delivered within a few ms
	probe := []byte("hello 13 byte")
	t0 := time.Now()
	n, err := conn.Write(probe)
	if err != nil || n != len(probe) {
		t.Fatalf("Write failed: n=%d err=%v", n, err)
	}

	buf := make([]byte, 1024)
	nr, err := queue.Read(buf)
	if err != nil {
		t.Fatalf("queue.Read failed: %v", err)
	}
	if time.Since(t0) > 20*time.Millisecond {
		t.Fatalf("Probe delivery took too long: %v", time.Since(t0))
	}
	if string(buf[:nr]) != string(probe) {
		t.Fatalf("Probe payload mismatch: got %q", string(buf[:nr]))
	}
}

func TestH2StreamConn_BackpressureAndClose(t *testing.T) {
	bound := 64 * 1024
	queue := api.NewH2ChunkQueue(bound)

	_, cancel := context.WithCancel(t.Context())
	defer cancel()

	conn := newH2StreamConn(queue, cancel, nil, nil, nil, nil)

	// Write up to queue bound
	chunk := make([]byte, 16*1024)
	for i := 0; i < 4; i++ {
		n, err := conn.Write(chunk)
		if err != nil || n != len(chunk) {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}

	// 5th write against stalled reader must block
	writeErrCh := make(chan error, 1)
	go func() {
		_, err := conn.Write(chunk)
		writeErrCh <- err
	}()

	select {
	case err := <-writeErrCh:
		t.Fatalf("expected write to block, but returned: %v", err)
	case <-time.After(50 * time.Millisecond):
		// writer is blocked on backpressure
	}

	// Close unblocks it
	_ = conn.Close()

	select {
	case err := <-writeErrCh:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("expected net.ErrClosed, got: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timeout waiting for Close to unblock writer")
	}
}

func TestH2Transport_ReadsNeverWaitOnWrites(t *testing.T) {
	writeStalled := make(chan struct{})
	readDone := make(chan []byte, 1)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}

		_, cancelReq := context.WithCancel(r.Context())
		defer cancelReq()
		sConn := tunnel.NewServerStreamConn(w, r.Body, cancelReq, nil)
		var drainDone sync.WaitGroup
		drainDone.Add(1)
		defer func() {
			sConn.Close()
			drainDone.Wait()
		}()
		go func() {
			defer drainDone.Done()
			sConn.RunDrainLoop()
		}()

		// Background writer: continuously write until writes stall (flow-control window + queue filled)
		go func() {
			bigChunk := make([]byte, 64*1024)
			for {
				_ = sConn.SetWriteDeadline(time.Now().Add(25 * time.Millisecond))
				_, err := sConn.Write(bigChunk)
				if err != nil {
					// Write stalled on flow control / deadline!
					select {
					case <-writeStalled:
					default:
						close(writeStalled)
					}
					return
				}
			}
		}()

		// Wait until write has stalled
		<-writeStalled

		// While write is stalled, read client upload
		uploadBuf := make([]byte, 1024)
		_ = sConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		nr, err := sConn.Read(uploadBuf)
		if err != nil {
			t.Errorf("Read failed while write stalled: %v", err)
			return
		}
		readDone <- uploadBuf[:nr]
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	h2Tr := NewH2Transport()
	defer h2Tr.Close()

	sessID := "sess-flow-control-test"
	h2Tr.RegisterSession(sessID)

	c1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer c1.Close()
	if err := h2Tr.AddConn(sessID, c1); err != nil {
		t.Fatalf("AddConn failed: %v", err)
	}

	streamConn, err := h2Tr.GetConn(t.Context(), sessID)
	if err != nil {
		t.Fatalf("GetConn failed: %v", err)
	}
	defer streamConn.Close()

	// Frontend intentionally DOES NOT read from streamConn (non-draining frontend)
	// Wait for backend writer to stall
	select {
	case <-writeStalled:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for backend write to stall")
	}

	// Now client uploads data to backend while backend write is stalled
	uploadData := []byte("client upload while write is stalled")
	t0 := time.Now()
	nw, err := streamConn.Write(uploadData)
	if err != nil || nw != len(uploadData) {
		t.Fatalf("streamConn.Write failed: %v", err)
	}

	select {
	case data := <-readDone:
		if time.Since(t0) > 500*time.Millisecond {
			t.Fatalf("Read took too long: %v", time.Since(t0))
		}
		if string(data) != string(uploadData) {
			t.Fatalf("Read data mismatch: got %q, want %q", string(data), string(uploadData))
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for backend to read upload while write is stalled")
	}
}

func TestH2Transport_PrunesClosedConnections(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		io.Copy(io.Discard, r.Body)
	})

	ln, cleanup := startMockBackendH2(t, 250, handler)
	defer cleanup()

	h2Tr := NewH2Transport()
	defer h2Tr.Close()

	sessID := "sess-prune-test"
	h2Tr.RegisterSession(sessID)

	c1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c1 failed: %v", err)
	}
	if err := h2Tr.AddConn(sessID, c1); err != nil {
		t.Fatalf("AddConn c1 failed: %v", err)
	}

	c2, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial c2 failed: %v", err)
	}
	if err := h2Tr.AddConn(sessID, c2); err != nil {
		t.Fatalf("AddConn c2 failed: %v", err)
	}

	conns := h2Tr.SessionConns(sessID)
	if len(conns) != 2 {
		t.Fatalf("expected 2 active conns, got %d", len(conns))
	}

	// Close c1's socket directly
	_ = c1.Close()

	// Wait for c1's State().Closed to become true
	deadline := time.Now().Add(2 * time.Second)
	var closed bool
	for time.Now().Before(deadline) {
		if conns[0].ClientConn().State().Closed {
			closed = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !closed {
		t.Fatalf("expected c1 to be recognized as Closed")
	}

	// Trigger selection / pruning
	remaining := h2Tr.SessionConns(sessID)
	if len(remaining) != 1 {
		t.Fatalf("expected 1 connection remaining after prune, got %d", len(remaining))
	}
	if remaining[0].ClientConn() == conns[0].ClientConn() {
		t.Fatalf("expected c1 to be pruned, but c1 was still present")
	}
}
