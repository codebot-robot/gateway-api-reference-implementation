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
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/api"
)

// Transport represents the data-plane connection provider between the frontend and backends.
type Transport interface {
	// GetConn retrieves or establishes a tunnel connection to the specified backend.
	GetConn(ctx context.Context, backendID string, hostname string, clientAddr net.Addr) (net.Conn, error)
	// Close closes the transport and any resources associated with it.
	Close() error
}

// DialbackTransport implements Transport using on-demand dialback over the connection event stream.
type DialbackTransport struct {
	mu        sync.RWMutex
	clients   map[string][]chan *api.ConnectionEvent
	clientsRR map[string]int
	pending   map[string]chan net.Conn
	timeout   time.Duration
	closed    bool
}

// NewDialbackTransport creates a new DialbackTransport.
func NewDialbackTransport(connectTimeout time.Duration) *DialbackTransport {
	if connectTimeout <= 0 {
		connectTimeout = 10 * time.Second
	}
	return &DialbackTransport{
		clients:   make(map[string][]chan *api.ConnectionEvent),
		clientsRR: make(map[string]int),
		pending:   make(map[string]chan net.Conn),
		timeout:   connectTimeout,
	}
}

// AddClientStream registers an event stream channel for a client.
func (t *DialbackTransport) AddClientStream(clientID string, ch chan *api.ConnectionEvent) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.clients[clientID] = append(t.clients[clientID], ch)
	return len(t.clients[clientID])
}

// RemoveClientStream removes an event stream channel for a client.
func (t *DialbackTransport) RemoveClientStream(clientID string, ch chan *api.ConnectionEvent) (remaining int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var kept []chan *api.ConnectionEvent
	for _, c := range t.clients[clientID] {
		if c != ch {
			kept = append(kept, c)
		}
	}
	if len(kept) == 0 {
		delete(t.clients, clientID)
		delete(t.clientsRR, clientID)
		return 0
	}
	t.clients[clientID] = kept
	return len(kept)
}

// RegisterPending registers a pending connection ID for dialback.
func (t *DialbackTransport) RegisterPending(connID string, ch chan net.Conn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pending[connID] = ch
}

// ClaimPending claims a pending connection channel by ID.
func (t *DialbackTransport) ClaimPending(connID string) (chan net.Conn, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ch, ok := t.pending[connID]
	return ch, ok
}

// RemovePending removes a pending connection channel.
func (t *DialbackTransport) RemovePending(connID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.pending, connID)
}

// GetConn implements Transport.
func (t *DialbackTransport) GetConn(ctx context.Context, backendID string, hostname string, clientAddr net.Addr) (net.Conn, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, net.ErrClosed
	}

	var eventCh chan *api.ConnectionEvent
	if streams, ok := t.clients[backendID]; ok && len(streams) > 0 {
		idx := t.clientsRR[backendID] % len(streams)
		t.clientsRR[backendID]++
		eventCh = streams[idx]
	}
	t.mu.Unlock()

	if eventCh == nil {
		return nil, fmt.Errorf("client %q has no active event stream", backendID)
	}

	connID := generateConnID()
	tunnelCh := make(chan net.Conn, 1)

	t.RegisterPending(connID, tunnelCh)
	defer t.RemovePending(connID)

	event := &api.ConnectionEvent{
		ID:         connID,
		Hostname:   hostname,
		RemoteAddr: clientAddr.String(),
	}

	timeout := t.timeout
	if d, ok := ctx.Deadline(); ok {
		remaining := time.Until(d)
		if remaining < timeout {
			timeout = remaining
		}
	}

	select {
	case eventCh <- event:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout sending event for conn %s to %s", connID, backendID)
	}

	select {
	case dialedConn := <-tunnelCh:
		if dialedConn == nil {
			return nil, errors.New("dialed connection closed")
		}
		return dialedConn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for dialback for conn %s from %s", connID, backendID)
	}
}

// Close implements Transport.
func (t *DialbackTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return nil
}

func generateConnID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
