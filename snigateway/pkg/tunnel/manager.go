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
	"bufio"
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/client"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/proxyproto"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// DefaultPoolSize is the default number of idle pooled reverse tunnel connections.
const DefaultPoolSize = 4

// ManagerOption configures a Manager.
type ManagerOption func(*Manager)

// WithListener sets a custom Listener for the Manager.
func WithListener(lis *Listener) ManagerOption {
	return func(m *Manager) {
		m.listener = lis
	}
}

// WithPoolSize sets the number of idle pooled reverse tunnel connections to maintain.
func WithPoolSize(size int) ManagerOption {
	return func(m *Manager) {
		if size > 0 {
			m.poolSize = size
		}
	}
}

// WithDialTimeout sets the timeout for dialing reverse tunnels.
func WithDialTimeout(d time.Duration) ManagerOption {
	return func(m *Manager) {
		m.dialTimeout = d
	}
}

// WithRegisterTimeout sets the timeout for registration API calls.
func WithRegisterTimeout(d time.Duration) ManagerOption {
	return func(m *Manager) {
		m.registerTimeout = d
	}
}

// WithBackoff sets the minimum and maximum backoff durations for reconnecting.
func WithBackoff(min, max time.Duration) ManagerOption {
	return func(m *Manager) {
		m.backoffMin = min
		m.backoffMax = max
	}
}

// Manager manages communication with the snigateway frontend, maintaining a warm pool
// of pre-dialed reverse tunnel connections and announcing SNI hostnames.
type Manager struct {
	client          *client.Client
	listener        *Listener
	poolSize        int
	dialTimeout     time.Duration
	registerTimeout time.Duration
	backoffMin      time.Duration
	backoffMax      time.Duration

	mu         sync.Mutex
	hostnames  []string
	sessionID  string
	connected  bool
	generation uint64
	updateCh   chan struct{}
}

// NewManager creates a new Manager using the given client.
func NewManager(c *client.Client, opts ...ManagerOption) *Manager {
	m := &Manager{
		client:          c,
		listener:        NewListener(),
		poolSize:        DefaultPoolSize,
		dialTimeout:     10 * time.Second,
		registerTimeout: 5 * time.Second,
		backoffMin:      200 * time.Millisecond,
		backoffMax:      5 * time.Second,
		updateCh:        make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Listener returns the tunnel Listener used to accept reverse-tunnelled connections.
func (m *Manager) Listener() *Listener {
	return m.listener
}

// UpdateGateways extracts SNI hostnames from the provided Gateways and updates the manager.
func (m *Manager) UpdateGateways(gateways []*gatewayv1.Gateway) {
	hostnames := ExtractHostnames(gateways)
	m.UpdateHostnames(hostnames)
}

// UpdateHostnames updates the set of hostnames to announce to the frontend.
// It coalesces rapid updates and notifies the background registration worker non-blockingly.
func (m *Manager) UpdateHostnames(hostnames []string) {
	m.mu.Lock()
	if slices.Equal(m.hostnames, hostnames) {
		m.mu.Unlock()
		return
	}
	m.hostnames = slices.Clone(hostnames)
	m.mu.Unlock()

	m.triggerRegistration()
}

func (m *Manager) triggerRegistration() {
	select {
	case m.updateCh <- struct{}{}:
	default:
	}
}

// Run starts the session, pool, and registration loops, maintaining connectivity to the frontend
// and feeding connections into the tunnel Listener until ctx is canceled.
func (m *Manager) Run(ctx context.Context) error {
	defer m.listener.Close()

	g, ctx := errgroup.WithContext(ctx)

	// Background registration worker
	g.Go(func() error {
		var lastRegistered []string
		var lastGen uint64
		var hasRegistered bool
		var lastSession string

		for {
			select {
			case <-ctx.Done():
				return nil
			case <-m.updateCh:
				m.mu.Lock()
				connected := m.connected
				sessID := m.sessionID
				gen := m.generation
				curr := slices.Clone(m.hostnames)
				m.mu.Unlock()

				if !connected || sessID == "" {
					continue
				}

				if hasRegistered && lastGen == gen && lastSession == sessID && slices.Equal(lastRegistered, curr) {
					continue
				}

				regCtx, cancel := context.WithTimeout(ctx, m.registerTimeout)
				resp, err := m.client.Register(regCtx, sessID, curr)
				cancel()

				if err != nil {
					if !errors.Is(err, context.Canceled) && ctx.Err() == nil {
						klog.Errorf("Failed to register hostnames %v with frontend (session %s): %v", curr, sessID, err)
						go func() {
							select {
							case <-ctx.Done():
							case <-time.After(500 * time.Millisecond):
								m.triggerRegistration()
							}
						}()
					}
				} else {
					klog.Infof("Successfully registered hostnames with frontend: %v (session: %s)", resp.Hostnames, sessID)
					lastRegistered = curr
					lastGen = gen
					lastSession = sessID
					hasRegistered = true
				}
			}
		}
	})

	// Session & Pool loop with reconnect and exponential backoff
	g.Go(func() error {
		backoff := m.backoffMin

		for {
			if ctx.Err() != nil {
				return nil
			}

			sessCtx, sessCancel := context.WithCancel(ctx)
			sessID, sessErrCh, err := m.client.StartSession(sessCtx)
			if err != nil {
				sessCancel()
				if ctx.Err() != nil {
					return nil
				}
				klog.Warningf("Failed to start session with frontend: %v. Reconnecting in %v...", err, backoff)
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(backoff):
				}
				backoff *= 2
				if backoff > m.backoffMax {
					backoff = m.backoffMax
				}
				continue
			}

			// Reset backoff on successful session creation
			backoff = m.backoffMin

			m.mu.Lock()
			m.connected = true
			m.sessionID = sessID
			m.generation++
			m.mu.Unlock()

			klog.Infof("Established session %s with frontend", sessID)
			m.triggerRegistration()

			// Run pool worker for this session
			m.runPool(sessCtx, sessID, sessErrCh)

			sessCancel()

			m.mu.Lock()
			m.connected = false
			m.sessionID = ""
			m.mu.Unlock()

			if ctx.Err() != nil {
				return nil
			}

			klog.Warningf("Session %s ended. Reconnecting in %v...", sessID, backoff)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
		}
	})

	return g.Wait()
}

func (m *Manager) runPool(ctx context.Context, sessionID string, sessErrCh <-chan error) {
	refillCh := make(chan struct{}, m.poolSize*2)
	activeIdleMu := sync.Mutex{}
	activeIdle := 0

	triggerRefill := func() {
		select {
		case refillCh <- struct{}{}:
		default:
		}
	}

	dialOne := func() {
		activeIdleMu.Lock()
		if activeIdle >= m.poolSize {
			activeIdleMu.Unlock()
			return
		}
		activeIdle++
		activeIdleMu.Unlock()

		go func() {
			dialCtx, cancel := context.WithTimeout(ctx, m.dialTimeout)
			conn, err := m.client.DialTunnel(dialCtx, sessionID)
			cancel()

			if err != nil {
				activeIdleMu.Lock()
				activeIdle--
				activeIdleMu.Unlock()

				if ctx.Err() == nil {
					klog.Errorf("Failed to dial pooled tunnel connection for session %s: %v", sessionID, err)
					time.Sleep(200 * time.Millisecond)
					triggerRefill()
				}
				return
			}

			// Watch connection for activation via PROXY protocol header or drop
			bufReader := bufio.NewReader(conn)
			hdr, err := proxyproto.Decode(bufReader)

			// Decrement idle count immediately upon read (either activated or closed)
			activeIdleMu.Lock()
			activeIdle--
			activeIdleMu.Unlock()

			if err != nil {
				_ = conn.Close()
				if ctx.Err() == nil {
					triggerRefill()
				}
				return
			}

			// Connection is activated! Trigger pool refill immediately
			triggerRefill()

			var remaining []byte
			if bufReader.Buffered() > 0 {
				remaining = make([]byte, bufReader.Buffered())
				_, _ = io.ReadFull(bufReader, remaining)
			}

			proxyConn := proxyproto.NewConn(conn, hdr.SrcAddr, hdr.DstAddr, remaining)
			if err := m.listener.Enqueue(proxyConn); err != nil {
				_ = conn.Close()
			}
		}()
	}

	// Initial fill of the pool
	for i := 0; i < m.poolSize; i++ {
		dialOne()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case err := <-sessErrCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				klog.Warningf("Session error from frontend: %v", err)
			}
			return
		case <-refillCh:
			dialOne()
		}
	}
}
