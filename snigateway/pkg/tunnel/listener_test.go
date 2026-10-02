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

package tunnel

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func TestListener_AcceptAndEnqueue(t *testing.T) {
	lis := NewListener()
	defer lis.Close()

	if lis.Addr().Network() != "snigateway-tunnel" {
		t.Fatalf("expected network snigateway-tunnel, got %s", lis.Addr().Network())
	}
	if lis.Addr().String() != "snigateway.internal" {
		t.Fatalf("expected address snigateway.internal, got %s", lis.Addr().String())
	}

	c1, c2 := net.Pipe()
	defer c1.Close()

	go func() {
		if err := lis.Enqueue(c2); err != nil {
			t.Errorf("Enqueue failed: %v", err)
		}
	}()

	accepted, err := lis.Accept()
	if err != nil {
		t.Fatalf("Accept failed: %v", err)
	}
	defer accepted.Close()

	// Verify data can flow through accepted connection
	go func() {
		_, _ = c1.Write([]byte("hello"))
	}()

	buf := make([]byte, 5)
	n, err := accepted.Read(buf)
	if err != nil || string(buf[:n]) != "hello" {
		t.Fatalf("unexpected read: %s, err: %v", string(buf[:n]), err)
	}
}

func TestListener_CloseUnblocksAccept(t *testing.T) {
	lis := NewListener()

	acceptErrCh := make(chan error, 1)
	go func() {
		_, err := lis.Accept()
		acceptErrCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	if err := lis.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	select {
	case err := <-acceptErrCh:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("expected net.ErrClosed, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Accept did not unblock after Close")
	}

	// Subsequent Accept returns net.ErrClosed immediately
	_, err := lis.Accept()
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("expected net.ErrClosed on subsequent Accept, got %v", err)
	}
}

func TestListener_EnqueueAfterClose(t *testing.T) {
	lis := NewListener()
	if err := lis.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	c1, c2 := net.Pipe()
	defer c1.Close()

	err := lis.Enqueue(c2)
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("expected net.ErrClosed from Enqueue after Close, got %v", err)
	}

	// c2 should have been closed
	buf := make([]byte, 10)
	_, readErr := c1.Read(buf)
	if readErr == nil {
		t.Fatalf("expected pipe to be closed by Enqueue after Close")
	}
}

func TestListener_CloseDrainsBufferedConnections(t *testing.T) {
	lis := NewListener()

	c1, c2 := net.Pipe()
	defer c1.Close()
	c3, c4 := net.Pipe()
	defer c3.Close()

	if err := lis.Enqueue(c2); err != nil {
		t.Fatalf("Enqueue c2 failed: %v", err)
	}
	if err := lis.Enqueue(c4); err != nil {
		t.Fatalf("Enqueue c4 failed: %v", err)
	}

	if err := lis.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Buffered pipes should now be closed
	buf := make([]byte, 10)
	if _, err := c1.Read(buf); err == nil {
		t.Fatalf("expected c1 to receive EOF/closed pipe")
	}
	if _, err := c3.Read(buf); err == nil {
		t.Fatalf("expected c3 to receive EOF/closed pipe")
	}
}

func TestListener_ConcurrentClose(t *testing.T) {
	lis := NewListener()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = lis.Close()
		}()
	}
	wg.Wait()
}
