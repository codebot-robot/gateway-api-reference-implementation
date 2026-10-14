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

	// Kill c1 abruptly by closing its raw socket
	_ = c1.Close()

	// Immediately request a connection mid-burst; GetConn should failover
	// to c2 before first byte without failing
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	stream, err := h2Tr.GetConn(ctx, sessID)
	if err != nil {
		t.Fatalf("expected failover to succeed, got error: %v", err)
	}
	defer stream.Close()

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
	shortCtx, shortCancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
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
	case <-time.After(50 * time.Millisecond):
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
	h2Tr := NewH2Transport(WithH2PingInterval(50 * time.Millisecond))
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
	time.Sleep(200 * time.Millisecond)

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

	respCh := make(chan rtResult, 1)
	prRes, pwRes := io.Pipe()

	respCh <- rtResult{
		res: &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       prRes,
		},
	}

	streamCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	conn := newH2StreamConn(queue, respCh, cancel, &net.TCPAddr{Port: 1234}, &net.TCPAddr{Port: 5678}, nil)
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
	respCh := make(chan rtResult, 1)

	_, cancel := context.WithCancel(t.Context())
	defer cancel()

	conn := newH2StreamConn(queue, respCh, cancel, nil, nil, nil)
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
	respCh := make(chan rtResult, 1)

	_, cancel := context.WithCancel(t.Context())
	defer cancel()

	conn := newH2StreamConn(queue, respCh, cancel, nil, nil, nil)

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
		defer sConn.Close()
		go sConn.RunDrainLoop()

		// Background writer: continuously write until writes stall (flow-control window + queue filled)
		go func() {
			bigChunk := make([]byte, 64*1024)
			for {
				_ = sConn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
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
