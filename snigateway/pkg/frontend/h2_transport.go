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
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/api"
	"golang.org/x/net/http2"
	"k8s.io/klog/v2"
)

// H2TransportOption configures an H2Transport.
type H2TransportOption func(*H2Transport)

// WithH2PingInterval sets the interval for sending HTTP/2 PINGs on idle connections.
func WithH2PingInterval(d time.Duration) H2TransportOption {
	return func(h *H2Transport) {
		if d > 0 {
			h.pingInterval = d
		}
	}
}

// WithH2DrainTimeout sets the timeout for draining in-flight streams on GOAWAY.
func WithH2DrainTimeout(d time.Duration) H2TransportOption {
	return func(h *H2Transport) {
		if d > 0 {
			h.drainTimeout = d
		}
	}
}

// WithH2TargetHost sets the default authority used in CONNECT requests.
func WithH2TargetHost(host string) H2TransportOption {
	return func(h *H2Transport) {
		if host != "" {
			h.targetHost = host
		}
	}
}

// h2Conn wraps an established HTTP/2 client connection pooled for a backend session.
type h2Conn struct {
	cc        *http2.ClientConn
	rawConn   net.Conn
	sessionID string
	draining  atomic.Bool
	closed    atomic.Bool
}

func (c *h2Conn) CanTakeNewRequest() bool {
	if c.closed.Load() || c.draining.Load() {
		return false
	}
	return c.cc.CanTakeNewRequest()
}

func (c *h2Conn) Load() int {
	st := c.cc.State()
	return st.StreamsActive + st.StreamsReserved
}

// H2Transport implements Transport multiplexing inbound client connections
// as HTTP/2 CONNECT streams over a pool of long-lived mTLS TCP connections.
type H2Transport struct {
	mu           sync.Mutex
	sessions     map[string][]*h2Conn
	notifyCh     chan struct{}
	closed       bool
	h2Tr         *http2.Transport
	pingInterval time.Duration
	drainTimeout time.Duration
	targetHost   string
}

// NewH2Transport creates a new H2Transport with tuned flow control windows and frame sizes.
func NewH2Transport(opts ...H2TransportOption) *H2Transport {
	h := &H2Transport{
		sessions:     make(map[string][]*h2Conn),
		notifyCh:     make(chan struct{}),
		h2Tr:         api.DefaultH2Transport(),
		pingInterval: api.DefaultH2PingInterval,
		drainTimeout: api.DefaultH2DrainTimeout,
		targetHost:   api.DefaultInternalHostname,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

func (h *H2Transport) broadcastLocked() {
	close(h.notifyCh)
	h.notifyCh = make(chan struct{})
}

func (h *H2Transport) broadcast() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.broadcastLocked()
}

// RegisterSession registers a backend session with the H2 transport.
func (h *H2Transport) RegisterSession(sessionID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.sessions[sessionID]; !exists {
		h.sessions[sessionID] = make([]*h2Conn, 0, 4)
		h.broadcastLocked()
	}
}

// UnregisterSession removes a session and drains its pooled H2 connections.
func (h *H2Transport) UnregisterSession(sessionID string) {
	h.mu.Lock()
	conns := h.sessions[sessionID]
	delete(h.sessions, sessionID)
	h.broadcastLocked()
	h.mu.Unlock()

	for _, c := range conns {
		h.drainConn(c)
	}
}

// HasSession reports whether sessionID is currently registered.
func (h *H2Transport) HasSession(sessionID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, exists := h.sessions[sessionID]
	return exists
}

// AddConn adds a newly hijacked and upgraded mTLS TCP connection to the H2 pool for sessionID.
func (h *H2Transport) AddConn(sessionID string, conn net.Conn) error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		closeCloser(conn)
		return net.ErrClosed
	}
	if _, exists := h.sessions[sessionID]; !exists {
		h.sessions[sessionID] = make([]*h2Conn, 0, 4)
	}
	h.mu.Unlock()

	cc, err := h.h2Tr.NewClientConn(conn)
	if err != nil {
		closeCloser(conn)
		return fmt.Errorf("creating http2 client connection: %w", err)
	}

	hConn := &h2Conn{
		cc:        cc,
		rawConn:   conn,
		sessionID: sessionID,
	}

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		h.drainConn(hConn)
		return net.ErrClosed
	}
	h.sessions[sessionID] = append(h.sessions[sessionID], hConn)
	h.broadcastLocked()
	h.mu.Unlock()

	return nil
}

func (h *H2Transport) drainConn(c *h2Conn) {
	if c.draining.Swap(true) {
		return
	}

	// Remove from session pool
	h.mu.Lock()
	if conns, ok := h.sessions[c.sessionID]; ok {
		var updated []*h2Conn
		for _, conn := range conns {
			if conn != c {
				updated = append(updated, conn)
			}
		}
		h.sessions[c.sessionID] = updated
		h.broadcastLocked()
	}
	h.mu.Unlock()

	go func() {
		drainCtx, cancel := context.WithTimeout(context.Background(), h.drainTimeout)
		if err := c.cc.Shutdown(drainCtx); err != nil && !errors.Is(err, net.ErrClosed) {
			klog.V(2).Infof("H2 tunnel shutdown error: %v", err)
		}
		cancel()

		c.closed.Store(true)
		if err := c.rawConn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			klog.V(2).Infof("H2 tunnel raw conn close error: %v", err)
		}
		h.broadcast()
	}()
}

// GetConn opens an HTTP/2 CONNECT stream for the specified sessionID.
func (h *H2Transport) GetConn(ctx context.Context, sessionID string) (net.Conn, error) {
	conn, _, err := h.GetConnForSessions(ctx, []string{sessionID})
	return conn, err
}

// TryGetConn attempts to open an HTTP/2 CONNECT stream on sessionID without blocking.
// Returns (nil, nil) if no capacity is currently available for this session.
func (h *H2Transport) TryGetConn(ctx context.Context, sessionID string) (net.Conn, error) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, net.ErrClosed
	}

	var candidates []*h2Conn
	for _, c := range h.sessions[sessionID] {
		if c.CanTakeNewRequest() {
			candidates = append(candidates, c)
		}
	}

	slices.SortFunc(candidates, func(a, b *h2Conn) int {
		return cmp.Compare(a.Load(), b.Load())
	})

	var bestConn *h2Conn
	for _, c := range candidates {
		if c.cc.ReserveNewRequest() {
			bestConn = c
			break
		}
	}

	if bestConn == nil {
		h.mu.Unlock()
		return nil, nil
	}
	h.mu.Unlock()

	streamConn, err := h.openConnectStream(ctx, bestConn)
	if err != nil {
		if ctx.Err() == nil {
			h.drainConn(bestConn)
		}
		h.broadcast()
		return nil, err
	}
	return streamConn, nil
}

// GetConnForSessions searches candidate sessions in order and opens an HTTP/2 CONNECT stream
// on the least-loaded connection that has capacity. If all pooled connections are at their
// MaxConcurrentStreams limit, it queues/waits until capacity becomes available or ctx is done.
func (h *H2Transport) GetConnForSessions(ctx context.Context, sessionIDs []string) (net.Conn, string, error) {
	for {
		h.mu.Lock()
		if h.closed {
			h.mu.Unlock()
			return nil, "", net.ErrClosed
		}

		ch := h.notifyCh

		var bestConn *h2Conn
		var bestSID string

		for _, sid := range sessionIDs {
			var candidates []*h2Conn
			for _, c := range h.sessions[sid] {
				if c.CanTakeNewRequest() {
					candidates = append(candidates, c)
				}
			}

			slices.SortFunc(candidates, func(a, b *h2Conn) int {
				return cmp.Compare(a.Load(), b.Load())
			})

			for _, c := range candidates {
				if c.cc.ReserveNewRequest() {
					bestConn = c
					bestSID = sid
					break
				}
			}
			if bestConn != nil {
				break
			}
		}

		if bestConn != nil {
			h.mu.Unlock()

			streamConn, err := h.openConnectStream(ctx, bestConn)
			if err != nil {
				if ctx.Err() != nil {
					return nil, "", ctx.Err()
				}

				// Connection-level error opening stream; drain connection and failover to next candidate
				h.drainConn(bestConn)
				h.broadcast()
				continue
			}

			return streamConn, bestSID, nil
		}

		h.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-ch:
			// Connection added, stream finished, or capacity freed
		case <-time.After(50 * time.Millisecond):
			// Periodic re-check to catch asynchronous http2 stream completions
		}
	}
}

type rtResult struct {
	res *http.Response
	err error
}

func (h *H2Transport) openConnectStream(ctx context.Context, hc *h2Conn) (net.Conn, error) {
	queue := api.NewH2ChunkQueue(api.DefaultH2StreamQueueBound)

	streamCtx, streamCancel := context.WithCancel(context.Background())
	headersSent := make(chan struct{})
	trace := &httptrace.ClientTrace{
		WroteHeaders: func() {
			close(headersSent)
		},
	}

	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(streamCtx, trace), http.MethodConnect, "", queue)
	if err != nil {
		streamCancel()
		queue.Abort(err)
		return nil, err
	}
	req.Host = h.targetHost
	req.URL = &url.URL{Host: req.Host}

	respCh := make(chan rtResult, 1)
	go func() {
		res, err := hc.cc.RoundTrip(req)
		respCh <- rtResult{res: res, err: err}
	}()

	// 0-RTT pipelining: hand stream back immediately as soon as request HEADERS are out!
	select {
	case <-ctx.Done():
		streamCancel()
		queue.Abort(ctx.Err())
		return nil, ctx.Err()
	case r := <-respCh:
		// RoundTrip failed or responded before headersSent was selected
		if r.err != nil {
			streamCancel()
			queue.Abort(r.err)
			return nil, r.err
		}
		if r.res.StatusCode != http.StatusOK {
			streamCancel()
			queue.Abort(net.ErrClosed)
			closeCloser(r.res.Body)
			return nil, fmt.Errorf("unexpected CONNECT response status: %s", r.res.Status)
		}
		streamConn := newH2StreamConn(queue, respCh, streamCancel, hc.rawConn.LocalAddr(), hc.rawConn.RemoteAddr(), h.broadcast)
		streamConn.resBody = r.res.Body
		return streamConn, nil
	case <-headersSent:
		// Request HEADERS frame is out on the wire! Return stream immediately for 0-RTT body writes.
		streamConn := newH2StreamConn(queue, respCh, streamCancel, hc.rawConn.LocalAddr(), hc.rawConn.RemoteAddr(), h.broadcast)
		return streamConn, nil
	}
}

// Close gracefully drains and closes all pooled connections across all sessions.
func (h *H2Transport) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	h.broadcastLocked()

	var allConns []*h2Conn
	for sid, conns := range h.sessions {
		allConns = append(allConns, conns...)
		delete(h.sessions, sid)
	}
	h.mu.Unlock()

	for _, c := range allConns {
		h.drainConn(c)
	}
	return nil
}

// h2StreamConn implements net.Conn over an HTTP/2 CONNECT stream.
// Writes are queued into req.Body and reads are received from res.Body.
type h2StreamConn struct {
	queue        *api.H2ChunkQueue
	respCh       chan rtResult
	streamCancel context.CancelFunc
	localAddr    net.Addr
	remoteAddr   net.Addr
	onClose      func()

	mu            sync.Mutex
	resBody       io.ReadCloser
	closed        bool
	readDeadline  time.Time
	writeDeadline time.Time
	readTimer     *time.Timer
	writeTimer    *time.Timer
	closeOnce     sync.Once
}

func newH2StreamConn(queue *api.H2ChunkQueue, respCh chan rtResult, streamCancel context.CancelFunc, localAddr, remoteAddr net.Addr, onClose func()) *h2StreamConn {
	return &h2StreamConn{
		queue:        queue,
		respCh:       respCh,
		streamCancel: streamCancel,
		localAddr:    localAddr,
		remoteAddr:   remoteAddr,
		onClose:      onClose,
	}
}

func (c *h2StreamConn) awaitResponse() (io.ReadCloser, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, net.ErrClosed
	}
	if c.resBody != nil {
		rb := c.resBody
		c.mu.Unlock()
		return rb, nil
	}
	c.mu.Unlock()

	r, ok := <-c.respCh
	if !ok {
		return nil, net.ErrClosed
	}
	if r.err != nil {
		closeCloser(c)
		return nil, r.err
	}
	if r.res.StatusCode != http.StatusOK {
		closeCloser(c)
		closeCloser(r.res.Body)
		return nil, fmt.Errorf("unexpected CONNECT response status: %s", r.res.Status)
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		closeCloser(r.res.Body)
		return nil, net.ErrClosed
	}
	c.resBody = r.res.Body
	rb := c.resBody
	c.mu.Unlock()

	return rb, nil
}

var h2SpliceBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 64*1024)
		return &b
	},
}

func (c *h2StreamConn) WriteTo(w io.Writer) (int64, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, net.ErrClosed
	}
	c.mu.Unlock()

	rb, err := c.awaitResponse()
	if err != nil {
		return 0, err
	}

	var first [4096]byte
	nr, rerr := rb.Read(first[:])
	var written int64
	if nr > 0 {
		nw, werr := w.Write(first[:nr])
		if nw > 0 {
			written += int64(nw)
		}
		if werr != nil {
			return written, werr
		}
		if nr != nw {
			return written, io.ErrShortWrite
		}
	}
	if rerr != nil {
		if rerr == io.EOF {
			rerr = nil
		}
		return written, rerr
	}

	bufPtr := h2SpliceBufPool.Get().(*[]byte)
	defer h2SpliceBufPool.Put(bufPtr)
	buf := *bufPtr

	for {
		nr, rerr := rb.Read(buf)
		if nr > 0 {
			nw, werr := w.Write(buf[:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if werr != nil {
				return written, werr
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				rerr = nil
			}
			return written, rerr
		}
	}
}

func (c *h2StreamConn) ReadFrom(r io.Reader) (int64, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, net.ErrClosed
	}
	c.mu.Unlock()

	var first [4096]byte
	nr, rerr := r.Read(first[:])
	var written int64
	if nr > 0 {
		nw, werr := c.Write(first[:nr])
		if nw > 0 {
			written += int64(nw)
		}
		if werr != nil {
			return written, werr
		}
		if nr != nw {
			return written, io.ErrShortWrite
		}
	}
	if rerr != nil {
		if rerr == io.EOF {
			rerr = nil
		}
		return written, rerr
	}

	bufPtr := h2SpliceBufPool.Get().(*[]byte)
	defer h2SpliceBufPool.Put(bufPtr)
	buf := *bufPtr

	for {
		nr, rerr := r.Read(buf)
		if nr > 0 {
			nw, werr := c.Write(buf[:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if werr != nil {
				return written, werr
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				rerr = nil
			}
			return written, rerr
		}
	}
}

func (c *h2StreamConn) Read(b []byte) (int, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, net.ErrClosed
	}
	if !c.readDeadline.IsZero() && time.Now().After(c.readDeadline) {
		c.mu.Unlock()
		return 0, os.ErrDeadlineExceeded
	}
	c.mu.Unlock()

	rb, err := c.awaitResponse()
	if err != nil {
		return 0, err
	}

	return rb.Read(b)
}

func (c *h2StreamConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, net.ErrClosed
	}
	if !c.writeDeadline.IsZero() && time.Now().After(c.writeDeadline) {
		c.mu.Unlock()
		return 0, os.ErrDeadlineExceeded
	}
	c.mu.Unlock()

	return c.queue.Write(b)
}

func (c *h2StreamConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readDeadline = t
	if c.readTimer != nil {
		c.readTimer.Stop()
		c.readTimer = nil
	}
	if !t.IsZero() && time.Until(t) > 0 {
		c.readTimer = time.AfterFunc(time.Until(t), func() {
			c.streamCancel()
			c.mu.Lock()
			rb := c.resBody
			c.mu.Unlock()
			if rb != nil {
				closeCloser(rb)
			}
		})
	}
	return nil
}

func (c *h2StreamConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeDeadline = t
	c.queue.SetWriteDeadline(t)
	if c.writeTimer != nil {
		c.writeTimer.Stop()
		c.writeTimer = nil
	}
	if !t.IsZero() && time.Until(t) > 0 {
		c.writeTimer = time.AfterFunc(time.Until(t), func() {
			c.queue.Wake()
		})
	}
	return nil
}

func (c *h2StreamConn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

func (c *h2StreamConn) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		if c.readTimer != nil {
			c.readTimer.Stop()
		}
		if c.writeTimer != nil {
			c.writeTimer.Stop()
		}
		rb := c.resBody
		c.mu.Unlock()

		c.streamCancel()
		closeCloser(c.queue)
		if rb != nil {
			closeCloser(rb)
		}
		if c.onClose != nil {
			c.onClose()
		}
	})
	return nil
}

func (c *h2StreamConn) LocalAddr() net.Addr {
	return c.localAddr
}

func (c *h2StreamConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}
