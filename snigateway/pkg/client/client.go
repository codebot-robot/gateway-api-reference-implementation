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

package client

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/api"
)

// Client is a client for communicating with the snigateway frontend mTLS API and reverse tunnels.
type Client struct {
	serverAddr       string
	internalHostname string
	tlsConfig        *tls.Config
	httpClient       *http.Client
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithInternalHostname overrides the default internal hostname ("snigateway.internal").
func WithInternalHostname(hostname string) ClientOption {
	return func(c *Client) {
		c.internalHostname = hostname
	}
}

// NewClient creates a new Client connecting to serverAddr using mTLS.
func NewClient(serverAddr string, tlsConfig *tls.Config, opts ...ClientOption) *Client {
	c := &Client{
		serverAddr:       serverAddr,
		internalHostname: api.DefaultInternalHostname,
		tlsConfig:        tlsConfig.Clone(),
	}
	for _, opt := range opts {
		opt(c)
	}

	if c.tlsConfig.ServerName == "" {
		c.tlsConfig.ServerName = c.internalHostname
	}

	transport := &http.Transport{
		TLSClientConfig: c.tlsConfig,
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialer := &tls.Dialer{
				Config: c.tlsConfig,
			}
			return dialer.DialContext(ctx, "tcp", c.serverAddr)
		},
	}

	c.httpClient = &http.Client{
		Transport: transport,
	}

	return c
}

// Register registers the given hostnames with the frontend server.
func (c *Client) Register(ctx context.Context, hostnames []string) (*api.RegistrationResponse, error) {
	reqBody, err := json.Marshal(api.RegistrationRequest{
		Hostnames: hostnames,
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling registration request: %w", err)
	}

	url := fmt.Sprintf("https://%s/v1/registration", c.internalHostname)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("creating registration request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("performing registration request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("registration failed with status %d: %s", resp.StatusCode, string(body))
	}

	var regResp api.RegistrationResponse
	if err := json.NewDecoder(resp.Body).Decode(&regResp); err != nil {
		return nil, fmt.Errorf("decoding registration response: %w", err)
	}

	return &regResp, nil
}

// StreamConnections opens a long-lived connections stream from the frontend and invokes onEvent for each incoming connection.
func (c *Client) StreamConnections(ctx context.Context, onEvent func(ctx context.Context, event *api.ConnectionEvent) error) error {
	return c.StreamConnectionsWithReady(ctx, nil, onEvent)
}

// StreamConnectionsWithReady opens a connections stream, signals ready when connected, and invokes onEvent for each incoming connection.
func (c *Client) StreamConnectionsWithReady(ctx context.Context, ready chan<- struct{}, onEvent func(ctx context.Context, event *api.ConnectionEvent) error) error {
	url := fmt.Sprintf("https://%s/v1/connections", c.internalHostname)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("creating connections stream request: %w", err)
	}

	// Use custom client without timeout for long-lived stream
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("connecting to connections stream: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("connections stream failed with status %d: %s", resp.StatusCode, string(body))
	}

	if ready != nil {
		close(ready)
	}

	reader := bufio.NewReader(resp.Body)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("reading connections stream: %w", err)
		}

		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			continue
		}

		var event api.ConnectionEvent
		if err := json.Unmarshal(trimmed, &event); err != nil {
			continue
		}

		if err := onEvent(ctx, &event); err != nil {
			return err
		}
	}
}

// DialTunnel dials back to the frontend with an mTLS connection and performs an HTTP/1.1 Upgrade for the given connID.
func (c *Client) DialTunnel(ctx context.Context, connID string) (net.Conn, error) {
	dialer := &tls.Dialer{
		Config: c.tlsConfig,
	}

	tlsConn, err := dialer.DialContext(ctx, "tcp", c.serverAddr)
	if err != nil {
		return nil, fmt.Errorf("dialing frontend mTLS: %w", err)
	}

	urlPath := "/v1/connections/" + connID
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, urlPath, nil)
	if err != nil {
		_ = tlsConn.Close()
		return nil, fmt.Errorf("creating tunnel upgrade request: %w", err)
	}
	req.Host = c.internalHostname
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", api.UpgradeProtocol)

	if err := req.Write(tlsConn); err != nil {
		_ = tlsConn.Close()
		return nil, fmt.Errorf("writing upgrade request: %w", err)
	}

	bufReader := bufio.NewReader(tlsConn)
	resp, err := http.ReadResponse(bufReader, req)
	if err != nil {
		_ = tlsConn.Close()
		return nil, fmt.Errorf("reading upgrade response: %w", err)
	}

	if resp.StatusCode != http.StatusSwitchingProtocols {
		body, _ := io.ReadAll(resp.Body)
		_ = tlsConn.Close()
		return nil, fmt.Errorf("unexpected upgrade status %d: %s", resp.StatusCode, string(body))
	}

	if !strings.EqualFold(resp.Header.Get("Upgrade"), api.UpgradeProtocol) && !strings.Contains(strings.ToLower(resp.Header.Get("Connection")), "upgrade") {
		_ = tlsConn.Close()
		return nil, fmt.Errorf("invalid upgrade response headers: Upgrade=%s", resp.Header.Get("Upgrade"))
	}

	if bufReader.Buffered() > 0 {
		return &bufferedClientConn{
			Conn:   tlsConn,
			reader: bufReader,
		}, nil
	}

	return tlsConn, nil
}

// Serve registers hostnames and handles incoming connections using handler.
func (c *Client) Serve(ctx context.Context, hostnames []string, handler func(ctx context.Context, connID string, hostname string, tunnel net.Conn)) error {
	return c.StreamConnections(ctx, func(ctx context.Context, event *api.ConnectionEvent) error {
		go func(ev *api.ConnectionEvent) {
			dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			tunnel, err := c.DialTunnel(dialCtx, ev.ID)
			if err != nil {
				return
			}
			handler(ctx, ev.ID, ev.Hostname, tunnel)
		}(event)
		return nil
	})
}

// Run opens connections stream, registers hostnames, and for each event dials back and passes tunnel to handler.
func (c *Client) Run(ctx context.Context, hostnames []string, handler func(ctx context.Context, connID string, hostname string, tunnel net.Conn)) error {
	errCh := make(chan error, 1)
	readyCh := make(chan struct{})

	go func() {
		err := c.StreamConnectionsWithReady(ctx, readyCh, func(ctx context.Context, event *api.ConnectionEvent) error {
			go func(ev *api.ConnectionEvent) {
				dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				defer cancel()

				tunnel, err := c.DialTunnel(dialCtx, ev.ID)
				if err != nil {
					return
				}
				handler(ctx, ev.ID, ev.Hostname, tunnel)
			}(event)
			return nil
		})
		errCh <- err
	}()

	select {
	case <-readyCh:
	case err := <-errCh:
		return fmt.Errorf("establishing connections stream: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	}

	if _, err := c.Register(ctx, hostnames); err != nil {
		return fmt.Errorf("registering hostnames: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

type bufferedClientConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedClientConn) Read(b []byte) (int, error) {
	return c.reader.Read(b)
}
