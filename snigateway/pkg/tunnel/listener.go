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
	"net"
	"sync"
)

// Addr represents the network address for a reverse tunnel listener.
type Addr struct{}

// Network returns the address network name.
func (a Addr) Network() string {
	return "snigateway-tunnel"
}

// String returns the string format of the address.
func (a Addr) String() string {
	return "snigateway.internal"
}

// Listener implements net.Listener, delivering inbound connections received over reverse tunnels.
type Listener struct {
	addr    net.Addr
	connCh  chan net.Conn
	closeCh chan struct{}
	closed  bool
	mu      sync.Mutex
}

// NewListener creates a new reverse tunnel Listener.
func NewListener() *Listener {
	return &Listener{
		addr:    Addr{},
		connCh:  make(chan net.Conn, 128),
		closeCh: make(chan struct{}),
	}
}

// Accept waits for and returns the next connection from the reverse tunnel.
// If the listener is closed, Accept returns net.ErrClosed.
func (l *Listener) Accept() (net.Conn, error) {
	select {
	case <-l.closeCh:
		return nil, net.ErrClosed
	case conn, ok := <-l.connCh:
		if !ok {
			return nil, net.ErrClosed
		}
		return conn, nil
	}
}

// Close closes the listener and unblocks any pending Accept calls.
func (l *Listener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}
	l.closed = true
	close(l.closeCh)

	// Drain and close any remaining buffered connections
	for {
		select {
		case c := <-l.connCh:
			_ = c.Close()
		default:
			return nil
		}
	}
}

// Addr returns the listener's network address.
func (l *Listener) Addr() net.Addr {
	return l.addr
}

// Enqueue adds a connection to the listener queue for Accept to return.
// If the listener is already closed, the connection is closed immediately and net.ErrClosed is returned.
func (l *Listener) Enqueue(conn net.Conn) error {
	l.mu.Lock()
	closed := l.closed
	l.mu.Unlock()

	if closed {
		_ = conn.Close()
		return net.ErrClosed
	}

	select {
	case <-l.closeCh:
		_ = conn.Close()
		return net.ErrClosed
	case l.connCh <- conn:
		return nil
	}
}
