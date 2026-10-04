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
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
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
	Authorizer       Authorizer
	Transport        Transport
}

// Server implements the SNI proxy frontend with mTLS API and reverse tunnels.
type Server struct {
	config     ServerConfig
	authorizer Authorizer
	table      *RegistrationTable
	transport  Transport

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

	auth := cfg.Authorizer
	if auth == nil {
		auth = &AllowAllAuthorizer{}
	}

	tr := cfg.Transport
	if tr == nil {
		tr = NewDialbackTransport(cfg.ConnectTimeout)
	}

	s := &Server{
		config:           cfg,
		authorizer:       auth,
		table:            NewRegistrationTable(),
		transport:        tr,
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

// Transport returns the server's data-plane transport.
func (s *Server) Transport() Transport {
	return s.transport
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
	_ = s.transport.Close()
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
		_ = peekedConn.Close()
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.config.ConnectTimeout)
	defer cancel()

	dialedConn, err := s.transport.GetConn(ctx, clientID, sniHostname, peekedConn.RemoteAddr())
	if err != nil {
		log.Printf("Frontend: failed to get tunnel connection for SNI %q (client %s): %v", sniHostname, clientID, err)
		_ = peekedConn.Close()
		return
	}

	spliceConnections(peekedConn, dialedConn)
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

func extractClientIdentity(r *http.Request) (ClientIdentity, error) {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return ClientIdentity{}, errors.New("client certificate required")
	}
	peerCert := r.TLS.PeerCertificates[0]
	cn := peerCert.Subject.CommonName
	if cn == "" {
		cn = "unknown-client"
	}

	var rootCert *x509.Certificate
	if len(r.TLS.VerifiedChains) > 0 && len(r.TLS.VerifiedChains[0]) > 0 {
		chain := r.TLS.VerifiedChains[0]
		rootCert = chain[len(chain)-1]
	} else {
		rootCert = peerCert
	}

	fingerprint := fmt.Sprintf("%x", sha256.Sum256(rootCert.Raw))
	id := fmt.Sprintf("%s/%s", fingerprint, cn)

	return ClientIdentity{
		ID:            id,
		CAFingerprint: fingerprint,
		CommonName:    cn,
	}, nil
}

func (s *Server) handleRegistration(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	clientIdentity, err := extractClientIdentity(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	var req api.RegistrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.authorizer.Authorize(r.Context(), clientIdentity, req.Hostnames); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	s.table.Register(clientIdentity.ID, req.Hostnames)
	registered := s.table.GetRegisteredHostnames(clientIdentity.ID)
	log.Printf("Frontend: client %q registered hostnames: %v", clientIdentity.ID, registered)

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

	clientIdentity, err := extractClientIdentity(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	dialback, ok := s.transport.(*DialbackTransport)
	if !ok {
		http.Error(w, "event streaming not supported by current transport", http.StatusNotImplemented)
		return
	}

	eventCh := make(chan *api.ConnectionEvent, 64)
	activeStreamCount := dialback.AddClientStream(clientIdentity.ID, eventCh)
	log.Printf("Frontend: client %q connected to event stream (active streams: %d)", clientIdentity.ID, activeStreamCount)

	defer func() {
		remaining := dialback.RemoveClientStream(clientIdentity.ID, eventCh)
		if remaining == 0 {
			s.table.Unregister(clientIdentity.ID)
			log.Printf("Frontend: client %q all event streams closed and unregistered", clientIdentity.ID)
		} else {
			log.Printf("Frontend: client %q event stream closed (remaining active streams: %d)", clientIdentity.ID, remaining)
		}
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

	_, err := extractClientIdentity(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	connID := strings.TrimPrefix(r.URL.Path, "/v1/connections/")
	if connID == "" {
		http.Error(w, "missing connection id", http.StatusBadRequest)
		return
	}

	dialback, ok := s.transport.(*DialbackTransport)
	if !ok {
		http.Error(w, "connection dialback not supported by current transport", http.StatusNotImplemented)
		return
	}

	tunnelCh, ok := dialback.ClaimPending(connID)
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
