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
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/api"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/sni"
)

// ServerConfig holds configuration options for the frontend server.
type ServerConfig struct {
	ListenAddrs      []string
	InternalHostname string
	ServerTLSConfig  *tls.Config
	ConnectTimeout   time.Duration
	ReadSNITimeout   time.Duration
}

// Server implements the SNI proxy frontend with mTLS API and reverse tunnels.
type Server struct {
	config ServerConfig
	table  *RegistrationTable

	mu      sync.RWMutex
	clients map[string]chan *api.ConnectionEvent // clientID -> active event stream channel
	pending map[string]chan net.Conn             // connectionID -> dialback tunnel channel

	internalListener *chanListener
	httpServer       *http.Server

	listenersMu sync.Mutex
	listeners   []net.Listener
	closed      bool
	shutdownCh  chan struct{}
}

// NewServer creates a new frontend Server with the given configuration.
func NewServer(cfg ServerConfig) (*Server, error) {
	if len(cfg.ListenAddrs) == 0 {
		cfg.ListenAddrs = []string{":443"}
	}
	if cfg.InternalHostname == "" {
		cfg.InternalHostname = api.DefaultInternalHostname
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	if cfg.ReadSNITimeout <= 0 {
		cfg.ReadSNITimeout = 5 * time.Second
	}
	if cfg.ServerTLSConfig == nil {
		return nil, errors.New("ServerTLSConfig is required")
	}

	s := &Server{
		config:           cfg,
		table:            NewRegistrationTable(),
		clients:          make(map[string]chan *api.ConnectionEvent),
		pending:          make(map[string]chan net.Conn),
		internalListener: newChanListener(&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0}),
		shutdownCh:       make(chan struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/registration", s.handleRegistration)
	mux.HandleFunc("/v1/connections", s.handleConnectionsStream)
	mux.HandleFunc("/v1/connections/", s.handleConnectionUpgrade)

	s.httpServer = &http.Server{
		Handler: mux,
	}

	return s, nil
}

// RegistrationTable returns the server's registration table.
func (s *Server) RegistrationTable() *RegistrationTable {
	return s.table
}

// ListenAndServe starts listening on all configured addresses and serves traffic.
func (s *Server) ListenAndServe() error {
	var listeners []net.Listener
	for _, addr := range s.config.ListenAddrs {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			for _, l := range listeners {
				_ = l.Close()
			}
			return fmt.Errorf("listening on %s: %w", addr, err)
		}
		listeners = append(listeners, ln)
	}

	return s.Serve(listeners...)
}

// Serve serves traffic on the given pre-created listeners.
func (s *Server) Serve(listeners ...net.Listener) error {
	s.listenersMu.Lock()
	s.listeners = listeners
	s.listenersMu.Unlock()

	// Start internal mTLS HTTP server
	errCh := make(chan error, len(listeners)+1)
	go func() {
		if err := s.httpServer.Serve(s.internalListener); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			errCh <- fmt.Errorf("internal HTTP server: %w", err)
		}
	}()

	// Accept connections on each listener
	for _, ln := range listeners {
		go func(l net.Listener) {
			for {
				conn, err := l.Accept()
				if err != nil {
					select {
					case <-s.shutdownCh:
						return
					default:
						errCh <- err
						return
					}
				}
				go s.handleConnection(conn)
			}
		}(ln)
	}

	select {
	case <-s.shutdownCh:
		return nil
	case err := <-errCh:
		return err
	}
}

// Close gracefully closes all listeners and stops the server.
func (s *Server) Close() error {
	s.listenersMu.Lock()
	if s.closed {
		s.listenersMu.Unlock()
		return nil
	}
	s.closed = true
	close(s.shutdownCh)
	for _, ln := range s.listeners {
		_ = ln.Close()
	}
	s.listenersMu.Unlock()

	_ = s.internalListener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleConnection(conn net.Conn) {
	sniHostname, peekedConn, err := sni.SniffSNI(conn, s.config.ReadSNITimeout)
	if err != nil {
		_ = peekedConn.Close()
		return
	}

	cleanSNI := CleanHostname(sniHostname)
	cleanInternal := CleanHostname(s.config.InternalHostname)

	// 1. If internal API hostname: terminate TLS and pass to mTLS HTTP server
	if cleanSNI == cleanInternal {
		tlsConn := tls.Server(peekedConn, s.config.ServerTLSConfig)
		if err := s.internalListener.SendConn(tlsConn); err != nil {
			_ = tlsConn.Close()
		}
		return
	}

	// 2. Otherwise look up in registration table
	clientID, found := s.table.Match(cleanSNI)
	if !found {
		log.Printf("Frontend: rejecting connection with SNI %q (clean: %q): not registered", sniHostname, cleanSNI)
		// No client registered for this hostname; close connection
		_ = peekedConn.Close()
		return
	}

	// Find active event channel for client
	s.mu.RLock()
	eventCh, active := s.clients[clientID]
	s.mu.RUnlock()

	if !active || eventCh == nil {
		log.Printf("Frontend: rejecting connection with SNI %q: client %q has no active event stream", sniHostname, clientID)
		_ = peekedConn.Close()
		return
	}

	// Generate connection ID and pending tunnel channel
	connID := generateConnID()
	tunnelCh := make(chan net.Conn, 1)

	s.mu.Lock()
	s.pending[connID] = tunnelCh
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.pending, connID)
		s.mu.Unlock()
	}()

	// Send event to client
	event := &api.ConnectionEvent{
		ID:         connID,
		Hostname:   sniHostname,
		RemoteAddr: peekedConn.RemoteAddr().String(),
	}

	select {
	case eventCh <- event:
	case <-time.After(s.config.ConnectTimeout):
		log.Printf("Frontend: timed out sending event for conn %s (SNI: %q) to client %s", connID, sniHostname, clientID)
		_ = peekedConn.Close()
		return
	}

	// Wait for client to dial back and upgrade
	select {
	case dialedConn := <-tunnelCh:
		if dialedConn == nil {
			_ = peekedConn.Close()
			return
		}
		spliceConnections(peekedConn, dialedConn)
	case <-time.After(s.config.ConnectTimeout):
		log.Printf("Frontend: timed out waiting for dialback for conn %s (SNI: %q) from client %s", connID, sniHostname, clientID)
		_ = peekedConn.Close()
	}
}

func spliceConnections(c1, c2 net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, _ = io.Copy(c2, c1)
		_ = c2.Close()
		_ = c1.Close()
	}()

	go func() {
		defer wg.Done()
		_, _ = io.Copy(c1, c2)
		_ = c1.Close()
		_ = c2.Close()
	}()

	wg.Wait()
}

func extractClientID(r *http.Request) (string, error) {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return "", errors.New("client certificate required")
	}
	cn := r.TLS.PeerCertificates[0].Subject.CommonName
	if cn == "" {
		cn = "unknown-client"
	}
	return cn, nil
}

func (s *Server) handleRegistration(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	clientID, err := extractClientID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	var req api.RegistrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.table.Register(clientID, req.Hostnames)
	registered := s.table.GetRegisteredHostnames(clientID)
	log.Printf("Frontend: client %q registered hostnames: %v", clientID, registered)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(api.RegistrationResponse{
		Status:    "registered",
		Hostnames: registered,
	})
}

func (s *Server) handleConnectionsStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	clientID, err := extractClientID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	eventCh := make(chan *api.ConnectionEvent, 64)

	s.mu.Lock()
	s.clients[clientID] = eventCh
	s.mu.Unlock()
	log.Printf("Frontend: client %q connected to event stream", clientID)

	defer func() {
		s.mu.Lock()
		if s.clients[clientID] == eventCh {
			delete(s.clients, clientID)
			s.table.Unregister(clientID)
			log.Printf("Frontend: client %q event stream closed and unregistered", clientID)
		} else {
			log.Printf("Frontend: old event stream for client %q closed (newer stream is active)", clientID)
		}
		s.mu.Unlock()
	}()

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	enc := json.NewEncoder(w)

	for {
		select {
		case <-r.Context().Done():
			return
		case event, ok := <-eventCh:
			if !ok {
				return
			}
			if err := enc.Encode(event); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Server) handleConnectionUpgrade(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	_, err := extractClientID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	connID := strings.TrimPrefix(r.URL.Path, "/v1/connections/")
	if connID == "" {
		http.Error(w, "missing connection id", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	tunnelCh, ok := s.pending[connID]
	s.mu.Unlock()

	if !ok || tunnelCh == nil {
		http.Error(w, "connection not found or expired", http.StatusNotFound)
		return
	}

	// Verify HTTP Upgrade header
	if !strings.EqualFold(r.Header.Get("Connection"), "Upgrade") && !strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") {
		http.Error(w, "upgrade header required", http.StatusBadRequest)
		return
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijack not supported", http.StatusInternalServerError)
		return
	}

	rawConn, rw, err := hijacker.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Send HTTP 101 Switching Protocols
	resp := "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: " + api.UpgradeProtocol + "\r\n\r\n"
	if _, err := rawConn.Write([]byte(resp)); err != nil {
		_ = rawConn.Close()
		return
	}

	var dialedConn net.Conn = rawConn
	if rw != nil && rw.Reader.Buffered() > 0 {
		dialedConn = &bufferedConn{
			Conn:   rawConn,
			reader: rw.Reader,
		}
	}

	select {
	case tunnelCh <- dialedConn:
	default:
		_ = dialedConn.Close()
	}
}

func generateConnID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(b []byte) (int, error) {
	return c.reader.Read(b)
}

// chanListener implements net.Listener over an in-memory channel.
type chanListener struct {
	addr    net.Addr
	connCh  chan net.Conn
	closeCh chan struct{}
	once    sync.Once
}

func newChanListener(addr net.Addr) *chanListener {
	return &chanListener{
		addr:    addr,
		connCh:  make(chan net.Conn, 128),
		closeCh: make(chan struct{}),
	}
}

func (l *chanListener) SendConn(c net.Conn) error {
	select {
	case <-l.closeCh:
		return net.ErrClosed
	case l.connCh <- c:
		return nil
	}
}

func (l *chanListener) Accept() (net.Conn, error) {
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

func (l *chanListener) Close() error {
	l.once.Do(func() {
		close(l.closeCh)
	})
	return nil
}

func (l *chanListener) Addr() net.Addr {
	return l.addr
}
