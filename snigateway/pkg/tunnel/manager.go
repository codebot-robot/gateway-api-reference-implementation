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
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/api"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/client"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// ManagerOption configures a Manager.
type ManagerOption func(*Manager)

// WithListener sets a custom Listener for the Manager.
func WithListener(lis *Listener) ManagerOption {
	return func(m *Manager) {
		m.listener = lis
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

// Manager manages communication with the snigateway frontend, announcing SNI hostnames
// and feeding reverse-tunnelled connections into a tunnel Listener.
type Manager struct {
	client          *client.Client
	listener        *Listener
	dialTimeout     time.Duration
	registerTimeout time.Duration
	backoffMin      time.Duration
	backoffMax      time.Duration

	mu         sync.Mutex
	hostnames  []string
	connected  bool
	generation uint64
	updateCh   chan struct{}
}

// NewManager creates a new Manager using the given client.
func NewManager(c *client.Client, opts ...ManagerOption) *Manager {
	m := &Manager{
		client:          c,
		listener:        NewListener(),
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

// Run starts the connection and registration loops, maintaining connectivity to the frontend
// and feeding connections into the tunnel Listener until ctx is canceled.
func (m *Manager) Run(ctx context.Context) error {
	defer m.listener.Close()

	g, ctx := errgroup.WithContext(ctx)

	// Background registration worker
	g.Go(func() error {
		var lastRegistered []string
		var lastGen uint64
		var hasRegistered bool

		for {
			select {
			case <-ctx.Done():
				return nil
			case <-m.updateCh:
				m.mu.Lock()
				connected := m.connected
				gen := m.generation
				curr := slices.Clone(m.hostnames)
				m.mu.Unlock()

				if !connected {
					continue
				}

				if hasRegistered && lastGen == gen && slices.Equal(lastRegistered, curr) {
					continue
				}

				regCtx, cancel := context.WithTimeout(ctx, m.registerTimeout)
				resp, err := m.client.Register(regCtx, curr)
				cancel()

				if err != nil {
					if !errors.Is(err, context.Canceled) && ctx.Err() == nil {
						klog.Errorf("Failed to register hostnames %v with frontend: %v", curr, err)
						go func() {
							select {
							case <-ctx.Done():
							case <-time.After(500 * time.Millisecond):
								m.triggerRegistration()
							}
						}()
					}
				} else {
					klog.Infof("Successfully registered hostnames with frontend: %v", resp.Hostnames)
					lastRegistered = curr
					lastGen = gen
					hasRegistered = true
				}
			}
		}
	})

	// Connection streaming loop with reconnect and exponential backoff
	g.Go(func() error {
		backoff := m.backoffMin

		for {
			if ctx.Err() != nil {
				return nil
			}

			readyCh := make(chan struct{})

			// Handle ready state as soon as connection stream is established
			go func() {
				select {
				case <-readyCh:
					m.mu.Lock()
					m.connected = true
					m.generation++
					m.mu.Unlock()
					m.triggerRegistration()
				case <-ctx.Done():
				}
			}()

			// Open connection event stream (blocks until error/disconnect)
			streamErr := m.client.StreamConnectionsWithReady(ctx, readyCh, func(streamCtx context.Context, event *api.ConnectionEvent) error {
				go func(ev *api.ConnectionEvent) {
					dialCtx, cancel := context.WithTimeout(ctx, m.dialTimeout)
					defer cancel()

					tunnelConn, err := m.client.DialTunnel(dialCtx, ev.ID)
					if err != nil {
						klog.Errorf("Failed to dial reverse tunnel for connection %s: %v", ev.ID, err)
						return
					}

					if err := m.listener.Enqueue(tunnelConn); err != nil {
						klog.Errorf("Failed to enqueue reverse tunnel connection %s: %v", ev.ID, err)
					}
				}(event)
				return nil
			})

			m.mu.Lock()
			m.connected = false
			m.mu.Unlock()

			if ctx.Err() != nil {
				return nil
			}

			if streamErr != nil && !errors.Is(streamErr, context.Canceled) {
				klog.Warningf("Connection stream to frontend failed: %v. Reconnecting in %v...", streamErr, backoff)
			}

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}

			backoff *= 2
			if backoff > m.backoffMax {
				backoff = m.backoffMax
			}
		}
	})

	return g.Wait()
}
