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
	"fmt"
	"net"
	"sync"
)

// Transport represents the data-plane connection provider between the frontend and backends.
type Transport interface {
	// GetConn retrieves or establishes a tunnel connection to the specified backend session.
	GetConn(ctx context.Context, sessionID string) (net.Conn, error)
	// Close closes the transport and any resources associated with it.
	Close() error
}

// PoolTransport implements Transport using a warm pool of pre-dialed reverse tunnel connections.
type PoolTransport struct {
	mu         sync.Mutex
	pools      map[string][]net.Conn
	notifyCh   chan struct{}
	closed     bool
	closedChan chan struct{}
}

// NewPoolTransport creates a new PoolTransport.
func NewPoolTransport() *PoolTransport {
	return &PoolTransport{
		pools:      make(map[string][]net.Conn),
		notifyCh:   make(chan struct{}, 1),
		closedChan: make(chan struct{}),
	}
}

// RegisterSession registers a new backend session in the transport.
func (p *PoolTransport) RegisterSession(sessionID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.pools[sessionID]; !exists {
		p.pools[sessionID] = make([]net.Conn, 0, 8)
	}
}

// UnregisterSession removes a session and closes any idle pooled connections for it.
func (p *PoolTransport) UnregisterSession(sessionID string) {
	p.mu.Lock()
	conns := p.pools[sessionID]
	delete(p.pools, sessionID)
	p.mu.Unlock()

	for _, c := range conns {
		_ = c.Close()
	}
	p.notify()
}

// AddConn adds a newly dialed tunnel connection to the idle pool of sessionID.
func (p *PoolTransport) AddConn(sessionID string, conn net.Conn) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = conn.Close()
		return net.ErrClosed
	}
	if _, exists := p.pools[sessionID]; !exists {
		p.mu.Unlock()
		_ = conn.Close()
		return fmt.Errorf("unknown or closed session: %s", sessionID)
	}
	p.pools[sessionID] = append(p.pools[sessionID], conn)
	p.mu.Unlock()

	p.notify()
	return nil
}

func (p *PoolTransport) notify() {
	select {
	case p.notifyCh <- struct{}{}:
	default:
	}
}

// GetConn takes an idle connection for sessionID. If the pool is empty, it waits until a connection arrives or ctx is done.
func (p *PoolTransport) GetConn(ctx context.Context, sessionID string) (net.Conn, error) {
	conn, _, err := p.GetConnForSessions(ctx, []string{sessionID})
	return conn, err
}

// GetConnForSessions takes an idle connection from one of the candidate sessions in order.
// Sessions with no idle connections are skipped. If none currently have an idle connection,
// it waits until any candidate session receives an idle connection or ctx is done.
func (p *PoolTransport) GetConnForSessions(ctx context.Context, sessionIDs []string) (net.Conn, string, error) {
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, "", net.ErrClosed
		}

		// Try to pop an idle connection from the candidate sessions in order
		for _, sid := range sessionIDs {
			conns := p.pools[sid]
			if len(conns) > 0 {
				conn := conns[0]
				p.pools[sid] = conns[1:]
				p.mu.Unlock()
				return conn, sid, nil
			}
		}
		p.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-p.closedChan:
			return nil, "", net.ErrClosed
		case <-p.notifyCh:
			// A connection was added or session updated, retry
		}
	}
}

// Close closes all pooled connections across all sessions.
func (p *PoolTransport) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.closedChan)

	var allConns []net.Conn
	for sid, conns := range p.pools {
		allConns = append(allConns, conns...)
		delete(p.pools, sid)
	}
	p.mu.Unlock()

	for _, c := range allConns {
		_ = c.Close()
	}
	p.notify()
	return nil
}

func generateConnID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
