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

// H2Conn wraps an established HTTP/2 client connection pooled for a backend session.
type H2Conn = h2Conn

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
	st := c.cc.State()
	if st.Closed || st.Closing {
		return false
	}
	return c.cc.CanTakeNewRequest()
}

func (c *h2Conn) ClientConn() *http2.ClientConn {
	return c.cc
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
	for sid := range h.sessions {
		h.pruneClosedLocked(sid)
	}
	close(h.notifyCh)
	h.notifyCh = make(chan struct{})
}

func (h *H2Transport) broadcast() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.broadcastLocked()
}

// pruneClosedLocked removes closed connections from h.sessions[sessionID] and returns the remaining active connections.
func (h *H2Transport) pruneClosedLocked(sessionID string) []*h2Conn {
	conns, ok := h.sessions[sessionID]
	if !ok || len(conns) == 0 {
		return nil
	}
	var active []*h2Conn
	for _, c := range conns {
		if !c.cc.State().Closed && !c.closed.Load() {
			active = append(active, c)
		}
	}
	if len(active) != len(conns) {
		h.sessions[sessionID] = active
	}
	return active
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
	conns, exists := h.sessions[sessionID]
	delete(h.sessions, sessionID)
	if exists {
		h.broadcastLocked()
	}
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

// SessionConns returns a snapshot of pooled H2 connections for the given session.
func (h *H2Transport) SessionConns(sessionID string) []*H2Conn {
	h.mu.Lock()
	defer h.mu.Unlock()
	conns := h.pruneClosedLocked(sessionID)
	res := make([]*H2Conn, len(conns))
	copy(res, conns)
	return res
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
//
// HTTP/2 CONNECT streams return as soon as the request HEADERS frame is
// written to the wire, providing 0-RTT pipelining. If the chosen connection
// suffers a transport-level failure (such as a closed socket or network drop)
// before any body bytes have been queued for writing and before any response
// bytes have been delivered to the caller (even if the CONNECT 200 OK has
// arrived), the stream transparently reopens on another pooled connection
// during the first Write or Read.
//
// A non-200 CONNECT response indicates a backend decision rather than a
// connection failure; it returns to the caller without reopening or draining
// the connection. Once body bytes have been queued or response bytes have
// been delivered, failover is not attempted and errors surface to the caller
// as usual.
func (h *H2Transport) GetConn(ctx context.Context, sessionID string) (net.Conn, error) {
	conn, _, err := h.GetConnForSessions(ctx, []string{sessionID})
	return conn, err
}

func (h *H2Transport) makeReopenFunc(sessionIDs []string, connToDrain *h2Conn) func(ctx context.Context) (*h2StreamConn, error) {
	sids := slices.Clone(sessionIDs)
	return func(reopenCtx context.Context) (*h2StreamConn, error) {
		h.drainConn(connToDrain)
		h.broadcast()

		newConn, _, err := h.GetConnForSessions(reopenCtx, sids)
		if err != nil {
			return nil, err
		}
		h2sc, ok := newConn.(*h2StreamConn)
		if !ok {
			return nil, fmt.Errorf("unexpected connection type: %T", newConn)
		}
		return h2sc, nil
	}
}

// TryGetConn attempts to open an HTTP/2 CONNECT stream on sessionID without blocking.
// Returns (nil, nil) if no capacity is currently available for this session.
func (h *H2Transport) TryGetConn(ctx context.Context, sessionID string) (net.Conn, error) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, net.ErrClosed
	}

	conns := h.pruneClosedLocked(sessionID)
	var candidates []*h2Conn
	for _, c := range conns {
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
		var statusErr *connectStatusError
		if !errors.As(err, &statusErr) && ctx.Err() == nil {
			h.drainConn(bestConn)
		}
		h.broadcast()
		return nil, err
	}

	streamConn.reopen = h.makeReopenFunc([]string{sessionID}, bestConn)
	return streamConn, nil
}

// GetConnForSessions searches candidate sessions in order and opens an HTTP/2 CONNECT stream
// on the least-loaded connection that has capacity. If all pooled connections are at their
// MaxConcurrentStreams limit, it waits until capacity becomes available or ctx is done.
//
// Streams return as soon as request HEADERS are written. If the chosen connection suffers
// a transport failure before any body bytes have been queued for writing and before any
// response bytes have been delivered to the caller (even if the 200 has arrived), the stream
// reopens on another candidate connection during its first Write or Read.
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
			conns := h.pruneClosedLocked(sid)
			var candidates []*h2Conn
			for _, c := range conns {
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
				var statusErr *connectStatusError
				if errors.As(err, &statusErr) {
					return nil, "", err
				}

				// Connection-level error opening stream; drain connection and failover to next candidate
				h.drainConn(bestConn)
				h.broadcast()
				continue
			}

			streamConn.reopen = h.makeReopenFunc(sessionIDs, bestConn)
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

type connectStatusError struct {
	status string
}

func (e *connectStatusError) Error() string {
	return fmt.Sprintf("unexpected CONNECT response status: %s", e.status)
}

// openConnectStream initiates an HTTP/2 CONNECT tunnel on hc.
//
// To minimize connection setup latency on healthy connections, openConnectStream
// returns the stream as soon as the request HEADERS frame is written (the
// httptrace.ClientTrace WroteHeaders event), before waiting 1 RTT for the HTTP
// 200 OK response.
//
// If a pooled connection closes concurrently around the time GetConn selects it,
// WroteHeaders can fire before the read error is detected. To avoid a 1-RTT
// penalty on healthy connections, the stream preserves 0-RTT and provides
// transparent failover before the first byte: if the connection suffers a
// transport failure before any body bytes have been queued for writing and
// before any response bytes have been delivered to the caller (even if the 200 OK
// response has already arrived), the stream reopens on another pooled connection
// during the first Write or Read.
//
// Non-200 responses return directly to the caller without draining the
// connection. Once body bytes have been queued or response bytes have been
// delivered, failover is not attempted and errors surface to the caller as usual.
func (h *H2Transport) openConnectStream(ctx context.Context, hc *h2Conn) (*h2StreamConn, error) {
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

	streamConn := newH2StreamConn(queue, streamCancel, hc.rawConn.LocalAddr(), hc.rawConn.RemoteAddr(), h.broadcast, hc)
	att := streamConn.attempt.Load()

	go func() {
		res, err := hc.cc.RoundTrip(req)
		att.mu.Lock()
		if err != nil {
			att.result = &rtResult{err: err}
		} else if res.StatusCode != http.StatusOK {
			closeCloser(res.Body)
			att.result = &rtResult{res: res, err: &connectStatusError{status: res.Status}}
		} else {
			att.result = &rtResult{res: res}
			att.resBody = res.Body
		}
		att.mu.Unlock()
		close(att.done)
	}()

	select {
	case <-ctx.Done():
		streamCancel()
		queue.Abort(ctx.Err())
		return nil, ctx.Err()
	case <-att.done:
		if att.result.err != nil {
			streamCancel()
			queue.Abort(att.result.err)
			return nil, att.result.err
		}
		return streamConn, nil
	case <-headersSent:
		select {
		case <-att.done:
			if att.result.err != nil {
				streamCancel()
				queue.Abort(att.result.err)
				return nil, att.result.err
			}
		default:
		}
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

type streamAttempt struct {
	queue        *api.H2ChunkQueue
	streamCancel context.CancelFunc
	localAddr    net.Addr
	remoteAddr   net.Addr
	hc           *h2Conn

	done chan struct{}

	mu      sync.Mutex
	result  *rtResult
	resBody io.ReadCloser
}

func (a *streamAttempt) pollResult() (io.ReadCloser, error, bool) {
	select {
	case <-a.done:
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.resBody, a.result.err, true
	default:
		if a.hc != nil && (a.hc.closed.Load() || a.hc.cc.State().Closed) {
			return nil, net.ErrClosed, true
		}
		return nil, nil, false
	}
}

func (a *streamAttempt) awaitResult(closeCh chan struct{}, deadline time.Time) (io.ReadCloser, error) {
	if rb, err, done := a.pollResult(); done {
		return rb, err
	}

	var deadlineCh <-chan time.Time
	if !deadline.IsZero() {
		dur := time.Until(deadline)
		if dur <= 0 {
			return nil, os.ErrDeadlineExceeded
		}
		t := time.NewTimer(dur)
		defer t.Stop()
		deadlineCh = t.C
	}

	select {
	case <-a.done:
		rb, err, _ := a.pollResult()
		return rb, err
	case <-closeCh:
		return nil, net.ErrClosed
	case <-deadlineCh:
		return nil, os.ErrDeadlineExceeded
	}
}

// h2StreamConn implements net.Conn over an HTTP/2 CONNECT stream.
// Writes are queued into req.Body and reads are received from res.Body.
type h2StreamConn struct {
	attempt atomic.Pointer[streamAttempt]

	onClose func()
	closeCh chan struct{}

	reopen    func(ctx context.Context) (*h2StreamConn, error)
	reopening chan struct{}

	mu            sync.Mutex
	closed        bool
	bodyQueued    bool
	reopenErr     error
	readDeadline  time.Time
	writeDeadline time.Time
	readTimer     *time.Timer
	writeTimer    *time.Timer
	closeOnce     sync.Once

	// Write-side first-byte state guarded by mu.
	firstWriting chan struct{}

	// Response-side state guarded by mu.
	responseDelivered bool
	firstReading      chan struct{}
}

func newH2StreamConn(queue *api.H2ChunkQueue, streamCancel context.CancelFunc, localAddr, remoteAddr net.Addr, onClose func(), hc *h2Conn) *h2StreamConn {
	att := &streamAttempt{
		queue:        queue,
		streamCancel: streamCancel,
		localAddr:    localAddr,
		remoteAddr:   remoteAddr,
		hc:           hc,
		done:         make(chan struct{}),
	}
	sc := &h2StreamConn{
		onClose: onClose,
		closeCh: make(chan struct{}),
	}
	sc.attempt.Store(att)
	return sc
}

func (c *h2StreamConn) waitChannel(waitCh chan struct{}, deadline time.Time) error {
	var deadlineCh <-chan time.Time
	if !deadline.IsZero() {
		dur := time.Until(deadline)
		if dur <= 0 {
			return os.ErrDeadlineExceeded
		}
		t := time.NewTimer(dur)
		defer t.Stop()
		deadlineCh = t.C
	}

	select {
	case <-waitCh:
		return nil
	case <-c.closeCh:
		return net.ErrClosed
	case <-deadlineCh:
		return os.ErrDeadlineExceeded
	}
}

func (c *h2StreamConn) finishReopen(reopeningCh chan struct{}, origErr, reopenErr error, nextConn *h2StreamConn) {
	c.mu.Lock()
	c.finishReopenLocked(reopeningCh, origErr, reopenErr, nextConn)
	c.mu.Unlock()
}

func (c *h2StreamConn) finishReopenLocked(reopeningCh chan struct{}, origErr, reopenErr error, nextConn *h2StreamConn) {
	c.reopenErr = errors.Join(origErr, reopenErr)
	c.reopening = nil
	close(reopeningCh)
	if nextConn != nil {
		closeCloser(nextConn)
	}
}

// waitReopeningLocked waits for any active transparent reopen to complete.
// It must be called with c.mu locked. On return, c.mu is still locked.
func (c *h2StreamConn) waitReopeningLocked(deadline time.Time) error {
	for c.reopening != nil {
		waitCh := c.reopening
		c.mu.Unlock()
		if err := c.waitChannel(waitCh, deadline); err != nil {
			c.mu.Lock()
			return err
		}
		c.mu.Lock()
		if c.closed {
			return net.ErrClosed
		}
		if c.reopenErr != nil {
			return c.reopenErr
		}
	}
	return nil
}

// waitFirstWritingLocked waits for any active first write to complete.
// It must be called with c.mu locked. On return, c.mu is still locked.
func (c *h2StreamConn) waitFirstWritingLocked(deadline time.Time) error {
	for c.firstWriting != nil {
		waitCh := c.firstWriting
		c.mu.Unlock()
		if err := c.waitChannel(waitCh, deadline); err != nil {
			c.mu.Lock()
			return err
		}
		c.mu.Lock()
		if c.closed {
			return net.ErrClosed
		}
		if c.reopenErr != nil {
			return c.reopenErr
		}
	}
	return nil
}

// waitFirstReadingLocked waits for any active first read to complete.
// It must be called with c.mu locked. On return, c.mu is still locked.
func (c *h2StreamConn) waitFirstReadingLocked(deadline time.Time) error {
	for c.firstReading != nil {
		waitCh := c.firstReading
		c.mu.Unlock()
		if err := c.waitChannel(waitCh, deadline); err != nil {
			c.mu.Lock()
			return err
		}
		c.mu.Lock()
		if c.closed {
			return net.ErrClosed
		}
		if c.reopenErr != nil {
			return c.reopenErr
		}
	}
	return nil
}

// isConnFailure reports whether the underlying HTTP/2 connection has failed
// (or err is a GoAwayError) rather than a stream-level condition such as clean
// EOF, stream reset (RST_STREAM), or a closed chunk queue.
func isConnFailure(hc *h2Conn, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return false
	}
	var se http2.StreamError
	if errors.As(err, &se) {
		return false
	}
	if hc != nil && (hc.closed.Load() || hc.cc.State().Closed || hc.cc.State().Closing) {
		return true
	}
	var ge http2.GoAwayError
	if errors.As(err, &ge) {
		return true
	}
	return false
}

func (c *h2StreamConn) transparentReopen(deadline time.Time, origErr error, failedAtt *streamAttempt) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return net.ErrClosed
	}
	if c.reopenErr != nil {
		err := c.reopenErr
		c.mu.Unlock()
		return err
	}

	// If another goroutine already completed a reopen from failedAtt, use the new attempt.
	if failedAtt != nil && c.attempt.Load() != failedAtt {
		c.mu.Unlock()
		return nil
	}

	if err := c.waitReopeningLocked(deadline); err != nil {
		c.mu.Unlock()
		return err
	}

	if failedAtt != nil && c.attempt.Load() != failedAtt {
		c.mu.Unlock()
		return nil
	}

	if c.bodyQueued || c.responseDelivered {
		c.mu.Unlock()
		return origErr
	}

	if c.reopen == nil {
		c.mu.Unlock()
		return net.ErrClosed
	}

	reopenFn := c.reopen
	c.reopen = nil
	reopeningCh := make(chan struct{})
	c.reopening = reopeningCh
	c.mu.Unlock()

	var reopenCtx context.Context
	var cancel context.CancelFunc
	if !deadline.IsZero() {
		if time.Now().After(deadline) {
			c.finishReopen(reopeningCh, origErr, os.ErrDeadlineExceeded, nil)
			return os.ErrDeadlineExceeded
		}
		reopenCtx, cancel = context.WithDeadline(context.Background(), deadline)
	} else {
		reopenCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	}
	defer cancel()

	nextConn, err := reopenFn(reopenCtx)
	if err != nil {
		c.finishReopen(reopeningCh, origErr, err, nil)
		return err
	}

	c.mu.Lock()
	if c.closed || c.bodyQueued || c.responseDelivered {
		c.finishReopenLocked(reopeningCh, origErr, net.ErrClosed, nextConn)
		c.mu.Unlock()
		return net.ErrClosed
	}

	oldAtt := c.attempt.Load()
	nextAtt := nextConn.attempt.Load()

	c.attempt.Store(nextAtt)
	c.reopen = nextConn.reopen

	if !c.writeDeadline.IsZero() && nextAtt.queue != nil {
		nextAtt.queue.SetWriteDeadline(c.writeDeadline)
	}
	if !c.readDeadline.IsZero() && nextAtt.queue != nil {
		nextAtt.queue.SetReadDeadline(c.readDeadline)
	}
	c.reopening = nil
	close(reopeningCh)
	c.mu.Unlock()

	if oldAtt != nil {
		if oldAtt.streamCancel != nil {
			oldAtt.streamCancel()
		}
		if oldAtt.queue != nil {
			oldAtt.queue.Abort(net.ErrClosed)
		}
		oldAtt.mu.Lock()
		if oldAtt.resBody != nil {
			closeCloser(oldAtt.resBody)
		}
		oldAtt.mu.Unlock()
	}

	return nil
}

func (c *h2StreamConn) awaitResponse() (*streamAttempt, io.ReadCloser, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, net.ErrClosed
	}
	if c.reopenErr != nil {
		err := c.reopenErr
		c.mu.Unlock()
		return nil, nil, err
	}

	deadline := c.readDeadline
	if err := c.waitReopeningLocked(deadline); err != nil {
		c.mu.Unlock()
		return nil, nil, err
	}

	att := c.attempt.Load()
	closeCh := c.closeCh
	c.mu.Unlock()

	rb, err := att.awaitResult(closeCh, deadline)
	if err == nil {
		return att, rb, nil
	}

	var statusErr *connectStatusError
	if errors.As(err, &statusErr) {
		closeCloser(c)
		return nil, nil, err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, net.ErrClosed
	}
	if c.reopenErr != nil {
		err := c.reopenErr
		c.mu.Unlock()
		return nil, nil, err
	}

	// If a first write is in progress, awaitResponse must not reopen until that write resolves.
	if err := c.waitFirstWritingLocked(deadline); err != nil {
		c.mu.Unlock()
		return nil, nil, err
	}

	canReopen := !c.bodyQueued && !c.responseDelivered && !c.closed
	c.mu.Unlock()

	if canReopen {
		if rerr := c.transparentReopen(deadline, err, att); rerr != nil {
			closeCloser(c)
			return nil, nil, errors.Join(err, rerr)
		}
		return c.awaitResponse()
	}

	closeCloser(c)
	return nil, nil, err
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

	var first [4096]byte
	nr, rerr := c.Read(first[:])
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

	_, rb, err := c.awaitResponse()
	if err != nil {
		return written, err
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
	if c.reopenErr != nil {
		err := c.reopenErr
		c.mu.Unlock()
		return 0, err
	}
	if !c.readDeadline.IsZero() && time.Now().After(c.readDeadline) {
		c.mu.Unlock()
		return 0, os.ErrDeadlineExceeded
	}

	deadline := c.readDeadline
	if err := c.waitReopeningLocked(deadline); err != nil {
		c.mu.Unlock()
		return 0, err
	}
	c.mu.Unlock()

	att, rb, err := c.awaitResponse()
	if err != nil {
		return 0, err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, net.ErrClosed
	}
	if err := c.waitFirstReadingLocked(deadline); err != nil {
		c.mu.Unlock()
		return 0, err
	}

	if c.responseDelivered {
		c.mu.Unlock()
		return rb.Read(b)
	}

	firstReadCh := make(chan struct{})
	c.firstReading = firstReadCh
	c.mu.Unlock()

	var n int
	var rerr error
	func() {
		defer func() {
			c.mu.Lock()
			c.firstReading = nil
			if n > 0 {
				c.responseDelivered = true
			}
			close(firstReadCh)
			c.mu.Unlock()
		}()
		n, rerr = rb.Read(b)
	}()

	if n > 0 {
		return n, rerr
	}

	if rerr == nil {
		return 0, nil
	}

	// If this attempt was superseded by a concurrent reopen, retry Read on the active attempt.
	if c.attempt.Load() != att {
		return c.Read(b)
	}

	// A backend's clean EOF or stream reset (e.g. RST_STREAM) is a stream-level condition,
	// not a connection failure. It must surface directly to the caller without reopening
	// or draining the connection pool.
	if !isConnFailure(att.hc, rerr) {
		return 0, rerr
	}

	// n == 0 and rerr != nil: no response bytes have been delivered to caller.
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, net.ErrClosed
	}
	if c.reopenErr != nil {
		err := c.reopenErr
		c.mu.Unlock()
		return 0, err
	}
	if !c.readDeadline.IsZero() && time.Now().After(c.readDeadline) {
		c.mu.Unlock()
		return 0, os.ErrDeadlineExceeded
	}

	// If another goroutine completed a reopen while acquiring lock, retry on the new attempt.
	if c.attempt.Load() != att {
		c.mu.Unlock()
		return c.Read(b)
	}

	// If another first write is in progress, wait for it to resolve before deciding.
	if err := c.waitFirstWritingLocked(deadline); err != nil {
		c.mu.Unlock()
		return 0, err
	}
	if c.attempt.Load() != att {
		c.mu.Unlock()
		return c.Read(b)
	}

	canReopen := !c.bodyQueued && !c.responseDelivered && !c.closed
	c.mu.Unlock()

	if canReopen {
		if err := c.transparentReopen(deadline, rerr, att); err != nil {
			closeCloser(c)
			return 0, errors.Join(rerr, err)
		}
		return c.Read(b)
	}

	closeCloser(c)
	return 0, rerr
}

func (c *h2StreamConn) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, net.ErrClosed
	}
	if c.reopenErr != nil {
		err := c.reopenErr
		c.mu.Unlock()
		return 0, err
	}
	if !c.writeDeadline.IsZero() && time.Now().After(c.writeDeadline) {
		c.mu.Unlock()
		return 0, os.ErrDeadlineExceeded
	}

	deadline := c.writeDeadline

	if err := c.waitReopeningLocked(deadline); err != nil {
		c.mu.Unlock()
		return 0, err
	}

	// If another first write is in progress, wait for it to resolve.
	if err := c.waitFirstWritingLocked(deadline); err != nil {
		c.mu.Unlock()
		return 0, err
	}

	att := c.attempt.Load()
	alreadyQueued := c.bodyQueued
	canReopen := !alreadyQueued && !c.responseDelivered

	if canReopen {
		_, err, done := att.pollResult()
		if !done || err == nil {
			if att.hc != nil && (att.hc.closed.Load() || att.hc.cc.State().Closed) {
				err = net.ErrClosed
			}
		}

		if err != nil {
			var statusErr *connectStatusError
			if errors.As(err, &statusErr) {
				c.mu.Unlock()
				closeCloser(c)
				return 0, err
			}

			// A transport error was detected while reopen is still possible.
			// Before deciding to reopen, wait for any in-progress first read to resolve.
			// This wait is bounded because the connection is already dead.
			if werr := c.waitFirstReadingLocked(deadline); werr != nil {
				c.mu.Unlock()
				return 0, werr
			}

			// If the first read delivered bytes (n > 0), the stream is committed; do not reopen.
			if c.responseDelivered || c.bodyQueued {
				c.mu.Unlock()
				return 0, err
			}

			c.mu.Unlock()
			if rerr := c.transparentReopen(deadline, err, att); rerr != nil {
				return 0, errors.Join(err, rerr)
			}
			return c.Write(b)
		}

		// Mark first write as in progress before calling queue.Write
		firstWriteCh := make(chan struct{})
		c.firstWriting = firstWriteCh
		c.mu.Unlock()

		var n int
		var werr error
		func() {
			defer func() {
				c.mu.Lock()
				c.firstWriting = nil
				if n > 0 {
					c.bodyQueued = true
				}
				close(firstWriteCh)
				c.mu.Unlock()
			}()
			n, werr = att.queue.Write(b)
		}()

		if n > 0 {
			return n, werr
		}

		// Zero bytes accepted into the queue (n == 0 && werr != nil).
		// Treat as not yet queued and attempt transparent reopen if connection failed.
		if !isConnFailure(att.hc, werr) {
			return 0, werr
		}

		c.mu.Lock()
		if err := c.waitFirstReadingLocked(deadline); err != nil {
			c.mu.Unlock()
			return 0, err
		}
		if c.responseDelivered || c.bodyQueued {
			c.mu.Unlock()
			return 0, werr
		}
		c.mu.Unlock()

		if rerr := c.transparentReopen(deadline, werr, att); rerr == nil {
			return c.Write(b)
		} else {
			return 0, errors.Join(werr, rerr)
		}
	}

	c.mu.Unlock()
	return att.queue.Write(b)
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
			att := c.attempt.Load()
			if att != nil {
				if att.streamCancel != nil {
					att.streamCancel()
				}
				att.mu.Lock()
				rb := att.resBody
				att.mu.Unlock()
				if rb != nil {
					closeCloser(rb)
				}
			}
		})
	}
	return nil
}

func (c *h2StreamConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeDeadline = t
	att := c.attempt.Load()
	if att != nil && att.queue != nil {
		att.queue.SetWriteDeadline(t)
	}
	if c.writeTimer != nil {
		c.writeTimer.Stop()
		c.writeTimer = nil
	}
	if !t.IsZero() && time.Until(t) > 0 {
		c.writeTimer = time.AfterFunc(time.Until(t), func() {
			a := c.attempt.Load()
			if a != nil && a.queue != nil {
				a.queue.Wake()
			}
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
		close(c.closeCh)
		if c.readTimer != nil {
			c.readTimer.Stop()
		}
		if c.writeTimer != nil {
			c.writeTimer.Stop()
		}
		att := c.attempt.Load()
		c.mu.Unlock()

		if att != nil {
			if att.streamCancel != nil {
				att.streamCancel()
			}
			closeCloser(att.queue)
			att.mu.Lock()
			if att.resBody != nil {
				closeCloser(att.resBody)
			}
			att.mu.Unlock()
		}
		if c.onClose != nil {
			c.onClose()
		}
	})
	return nil
}

func (c *h2StreamConn) LocalAddr() net.Addr {
	att := c.attempt.Load()
	if att != nil {
		return att.localAddr
	}
	return nil
}

func (c *h2StreamConn) RemoteAddr() net.Addr {
	att := c.attempt.Load()
	if att != nil {
		return att.remoteAddr
	}
	return nil
}
