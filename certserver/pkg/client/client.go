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
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	certserverv1 "github.com/gke-labs/gateway-api-reference-implementation/certserver/api/v1"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/manager"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/provisioning/singlepod"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
)

// Config configures the certserver CertificateSource client.
type Config struct {
	// ServerAddress is the host:port of the certserver gRPC service.
	ServerAddress string

	// CABundlePath is the optional path to a PEM-encoded CA bundle file to verify certserver TLS.
	CABundlePath string

	// CABundle is optional PEM-encoded CA certificate bytes (takes precedence over CABundlePath).
	CABundle []byte

	// TokenPath is the path to the projected service-account token file on disk.
	TokenPath string

	// TLSConfig is an optional custom *tls.Config for testing or custom verification.
	// If set, CABundle and CABundlePath are ignored.
	TLSConfig *tls.Config

	// GRPCClient is an optional preconfigured CertificateServiceClient (primarily used for unit testing).
	GRPCClient certserverv1.CertificateServiceClient

	// MinBackoff is the initial retry delay on retryable errors. Defaults to 1s.
	MinBackoff time.Duration

	// MaxBackoff is the maximum retry delay on errors. Defaults to 30s.
	MaxBackoff time.Duration

	// RPCTimeout is the timeout for individual GetCertificate RPC calls. Defaults to 15s.
	RPCTimeout time.Duration

	// CheckInterval is the interval at which the background renewal ticker checks cached certificates. Defaults to 1m.
	CheckInterval time.Duration

	// RenewalWindow is the window before certificate expiry when background refresh begins.
	// Defaults to 1/3 of the certificate's validity lifetime (capped at 30 days).
	RenewalWindow time.Duration

	// EvictionPeriod is the duration after which an entry not requested via ResolveCertificates is evicted.
	// Defaults to 5 minutes (or 5 * CheckInterval).
	EvictionPeriod time.Duration

	// LogInterval is the minimum interval between logged failures for a cache entry. Defaults to 30s.
	LogInterval time.Duration

	// TimeNow returns the current time. If nil, time.Now is used.
	TimeNow func() time.Time
}

type cacheKey struct {
	gateway   types.NamespacedName
	hostnames string
}

type entryState int

const (
	statePending entryState = iota
	stateReady
	stateFailed
)

type cacheEntry struct {
	key         cacheKey
	hostnames   []string
	gateway     string
	dataPlaneSA string

	state        entryState
	certificate  *tls.Certificate
	leaf         *x509.Certificate
	expiresAt    time.Time
	refreshAt    time.Time
	failedReason string

	lastRequested time.Time
	lastLogged    time.Time
	inFlight      bool
}

// Client implements state.TriggerableCertificateSource by communicating with certserver.
type Client struct {
	cfg        Config
	conn       *grpc.ClientConn
	grpcClient certserverv1.CertificateServiceClient

	// ctx is cancelled when Close is called, terminating background routines and in-flight RPCs.
	ctx    context.Context
	cancel context.CancelFunc

	// mu guards cache and trigger.
	mu      sync.RWMutex
	cache   map[cacheKey]*cacheEntry
	trigger func()

	stopCh    chan struct{}
	closeOnce sync.Once
}

// New creates a new certserver client implementing state.TriggerableCertificateSource.
func New(cfg Config) (*Client, error) {
	if cfg.TokenPath == "" {
		return nil, errors.New("token path is required")
	}

	var conn *grpc.ClientConn
	grpcClient := cfg.GRPCClient

	if grpcClient == nil {
		if cfg.ServerAddress == "" {
			return nil, errors.New("server address is required")
		}

		tlsConfig := cfg.TLSConfig
		if tlsConfig == nil {
			tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
			if len(cfg.CABundle) > 0 {
				pool := x509.NewCertPool()
				if !pool.AppendCertsFromPEM(cfg.CABundle) {
					return nil, errors.New("failed to parse CA bundle PEM bytes")
				}
				tlsConfig.RootCAs = pool
			} else if cfg.CABundlePath != "" {
				data, err := os.ReadFile(cfg.CABundlePath)
				if err != nil {
					return nil, fmt.Errorf("reading CA bundle file %q: %w", cfg.CABundlePath, err)
				}
				pool := x509.NewCertPool()
				if !pool.AppendCertsFromPEM(data) {
					return nil, fmt.Errorf("failed to parse CA bundle from file %q", cfg.CABundlePath)
				}
				tlsConfig.RootCAs = pool
			}
		}

		creds := credentials.NewTLS(tlsConfig)
		var err error
		conn, err = grpc.NewClient(cfg.ServerAddress, grpc.WithTransportCredentials(creds))
		if err != nil {
			return nil, fmt.Errorf("dialing certserver %q: %w", cfg.ServerAddress, err)
		}
		grpcClient = certserverv1.NewCertificateServiceClient(conn)
	}

	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = 1 * time.Second
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 30 * time.Second
	}
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = 15 * time.Second
	}
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = 1 * time.Minute
	}
	if cfg.EvictionPeriod <= 0 {
		cfg.EvictionPeriod = 5 * cfg.CheckInterval
	}
	if cfg.LogInterval <= 0 {
		cfg.LogInterval = 30 * time.Second
	}
	if cfg.TimeNow == nil {
		cfg.TimeNow = time.Now
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Client{
		cfg:        cfg,
		conn:       conn,
		grpcClient: grpcClient,
		ctx:        ctx,
		cancel:     cancel,
		cache:      make(map[cacheKey]*cacheEntry),
		stopCh:     make(chan struct{}),
	}

	go c.renewalTickerLoop()

	return c, nil
}

// SetRecomputeTrigger sets the callback to request a State recompute when the cache changes.
func (c *Client) SetRecomputeTrigger(trigger func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trigger = trigger
}

func (c *Client) notifyTrigger() {
	c.mu.RLock()
	trig := c.trigger
	c.mu.RUnlock()
	if trig != nil {
		trig()
	}
}

func (c *Client) now() time.Time {
	if c.cfg.TimeNow != nil {
		return c.cfg.TimeNow()
	}
	return time.Now()
}

// Start satisfies controller-runtime manager.Runnable, running until ctx is cancelled and then closing the client.
func (c *Client) Start(ctx context.Context) error {
	<-ctx.Done()
	return c.Close()
}

// Close stops background routines and closes any open gRPC connections.
func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.cancel()
		close(c.stopCh)
		if c.conn != nil {
			err = c.conn.Close()
		}
	})
	return err
}

func (c *Client) renewalTickerLoop() {
	ticker := time.NewTicker(c.cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.checkAndRefresh()
		}
	}
}

func (c *Client) checkAndRefresh() {
	c.mu.Lock()
	now := c.now()
	for k, entry := range c.cache {
		// Evict inactive entries that ResolveCertificates has not requested within EvictionPeriod.
		if now.Sub(entry.lastRequested) > c.cfg.EvictionPeriod {
			delete(c.cache, k)
			continue
		}
		if entry.certificate != nil && !entry.inFlight && now.After(entry.refreshAt) {
			entry.inFlight = true
			go c.fetchLoop(entry.key, entry.hostnames, entry.gateway, entry.dataPlaneSA)
		}
	}
	c.mu.Unlock()
}

// ResolveCertificates returns certificates from the local cache and triggers background fetching on cache misses.
// Contract: ResolveCertificates is strictly non-blocking and makes no network calls.
func (c *Client) ResolveCertificates(gw *gatewayv1.Gateway, listener *gatewayv1.Listener) (state.CertificateResult, error) {
	if gw == nil || listener == nil {
		return state.CertificateResult{
			FailedReason:      gatewayv1.ListenerReasonInvalidCertificateRef,
			FailedMessage:     "Gateway or listener is nil",
			ProgrammedMessage: "Gateway or listener is nil",
		}, nil
	}
	if listener.Hostname == nil || *listener.Hostname == "" {
		return state.CertificateResult{
			FailedReason:      gatewayv1.ListenerReasonInvalidCertificateRef,
			FailedMessage:     "Certificate source requires an exact, non-wildcard hostname",
			ProgrammedMessage: "Certificate source requires an exact, non-wildcard hostname",
		}, nil
	}

	rawHostnames := []string{string(*listener.Hostname)}
	normalized, hostnamesKey, err := manager.NormalizeHostnames(rawHostnames, 100, 253)
	if err != nil {
		return state.CertificateResult{
			FailedReason:      gatewayv1.ListenerReasonInvalidCertificateRef,
			FailedMessage:     fmt.Sprintf("Invalid listener hostname: %v", err),
			ProgrammedMessage: fmt.Sprintf("Invalid listener hostname: %v", err),
		}, nil
	}

	gwKey := types.NamespacedName{Namespace: gw.Namespace, Name: gw.Name}
	gwFormatted := gwKey.String()
	dpSA := singlepod.ResourceNameForGateway(gw.Name)
	if gw.Namespace != "" {
		dpSA = fmt.Sprintf("%s/%s", gw.Namespace, dpSA)
	}
	key := cacheKey{
		gateway:   gwKey,
		hostnames: hostnamesKey,
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	entry, ok := c.cache[key]
	if !ok {
		entry = &cacheEntry{
			key:           key,
			hostnames:     normalized,
			gateway:       gwFormatted,
			dataPlaneSA:   dpSA,
			state:         statePending,
			lastRequested: now,
		}
		c.cache[key] = entry
	} else {
		entry.lastRequested = now
	}

	// If we have a valid unexpired certificate, keep serving it even if a recent background renewal failed.
	if entry.certificate != nil && !now.After(entry.expiresAt) {
		if now.After(entry.refreshAt) && !entry.inFlight {
			entry.inFlight = true
			go c.fetchLoop(entry.key, entry.hostnames, entry.gateway, entry.dataPlaneSA)
		}
		return state.CertificateResult{
			Certificates: []*tls.Certificate{state.CopyTLSCertificate(entry.certificate)},
		}, nil
	}

	// No valid unexpired certificate:
	switch entry.state {
	case stateFailed:
		if !entry.inFlight {
			entry.inFlight = true
			go c.fetchLoop(entry.key, entry.hostnames, entry.gateway, entry.dataPlaneSA)
		}
		msg := entry.failedReason
		if msg == "" {
			msg = "Certificate source error"
		}
		return state.CertificateResult{
			FailedReason:      gatewayv1.ListenerReasonInvalidCertificateRef,
			FailedMessage:     msg,
			ProgrammedMessage: msg,
		}, nil

	default: // statePending
		if !entry.inFlight {
			entry.inFlight = true
			go c.fetchLoop(entry.key, entry.hostnames, entry.gateway, entry.dataPlaneSA)
		}
		return state.CertificateResult{
			Pending: true,
		}, nil
	}
}

func genericStatusMessage(code codes.Code) string {
	switch code {
	case codes.Unavailable:
		return "Certificate source unavailable"
	case codes.PermissionDenied:
		return "Certificate source permission denied"
	case codes.Unauthenticated:
		return "Certificate source authentication failed"
	case codes.InvalidArgument:
		return "Certificate source invalid argument"
	case codes.NotFound:
		return "Certificate source resource not found"
	case codes.ResourceExhausted:
		return "Certificate source rate limit exceeded"
	case codes.DeadlineExceeded:
		return "Certificate source request timed out"
	case codes.Internal:
		return "Certificate source internal error"
	default:
		return "Certificate source error"
	}
}

func isNonRetryableCode(code codes.Code) bool {
	switch code {
	case codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument, codes.Internal, codes.NotFound:
		return true
	default:
		return false
	}
}

func (c *Client) fetchLoop(key cacheKey, hostnames []string, gateway string, dataPlaneSA string) {
	backoff := c.cfg.MinBackoff
	var retryTimer *time.Timer
	defer func() {
		if retryTimer != nil {
			retryTimer.Stop()
		}
	}()

	for {
		select {
		case <-c.ctx.Done():
			c.mu.Lock()
			if entry, ok := c.cache[key]; ok {
				entry.inFlight = false
			}
			c.mu.Unlock()
			return
		default:
		}

		// Evict entry if no longer requested via ResolveCertificates.
		c.mu.Lock()
		entry, ok := c.cache[key]
		if !ok || c.now().Sub(entry.lastRequested) > c.cfg.EvictionPeriod {
			if ok {
				entry.inFlight = false
				delete(c.cache, key)
			}
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()

		cert, leaf, stCode, err := c.doGetCertificate(hostnames, gateway, dataPlaneSA)
		if err == nil {
			c.mu.Lock()
			entry, ok := c.cache[key]
			if !ok {
				c.mu.Unlock()
				return
			}
			entry.inFlight = false
			entry.state = stateReady
			entry.certificate = cert
			entry.leaf = leaf
			entry.expiresAt = leaf.NotAfter

			lifetime := leaf.NotAfter.Sub(leaf.NotBefore)
			window := c.cfg.RenewalWindow
			if window <= 0 {
				window = lifetime / 3
			}
			if window > 30*24*time.Hour {
				window = 30 * 24 * time.Hour
			}
			entry.refreshAt = leaf.NotAfter.Add(-window)
			if entry.refreshAt.Before(c.now()) {
				entry.refreshAt = c.now().Add(1 * time.Minute)
			}
			entry.failedReason = ""
			c.mu.Unlock()

			c.notifyTrigger()
			return
		}

		// Failed fetch: log with rate limit and record generic failure reason.
		now := c.now()
		genericReason := genericStatusMessage(stCode)

		c.mu.Lock()
		entry, ok = c.cache[key]
		if !ok {
			c.mu.Unlock()
			return
		}
		shouldLog := false
		if now.Sub(entry.lastLogged) >= c.cfg.LogInterval {
			entry.lastLogged = now
			shouldLog = true
		}

		entry.failedReason = genericReason
		hasValidCert := entry.certificate != nil && !now.After(entry.expiresAt)
		notifyNeeded := false
		if !hasValidCert {
			if isNonRetryableCode(stCode) {
				if entry.state != stateFailed {
					entry.state = stateFailed
					notifyNeeded = true
				}
			}
		}
		c.mu.Unlock()

		if shouldLog {
			klog.Warningf("certserver: failed to fetch certificate for Gateway %s hostnames %v: %v", gateway, hostnames, err)
		}

		// If no valid certificate exists and the failure is non-retryable, trigger recompute to update condition.
		if notifyNeeded {
			c.notifyTrigger()
		}

		// Retry with capped backoff: use maximum backoff for non-retryable codes.
		if isNonRetryableCode(stCode) {
			backoff = c.cfg.MaxBackoff
		} else {
			backoff *= 2
			if backoff > c.cfg.MaxBackoff {
				backoff = c.cfg.MaxBackoff
			}
		}

		if retryTimer == nil {
			retryTimer = time.NewTimer(backoff)
		} else {
			retryTimer.Reset(backoff)
		}

		select {
		case <-c.ctx.Done():
			c.mu.Lock()
			if entry, ok := c.cache[key]; ok {
				entry.inFlight = false
			}
			c.mu.Unlock()
			return
		case <-retryTimer.C:
		}
	}
}

func (c *Client) doGetCertificate(hostnames []string, gateway string, dataPlaneSA string) (*tls.Certificate, *x509.Certificate, codes.Code, error) {
	tokenBytes, err := os.ReadFile(c.cfg.TokenPath)
	if err != nil {
		return nil, nil, codes.Unknown, fmt.Errorf("reading token file: %w", err)
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" {
		return nil, nil, codes.Unauthenticated, errors.New("token file is empty")
	}

	ctx, cancel := context.WithTimeout(c.ctx, c.cfg.RPCTimeout)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

	req := &certserverv1.GetCertificateRequest{
		Hostnames:               hostnames,
		Gateway:                 gateway,
		DataPlaneServiceAccount: dataPlaneSA,
	}

	resp, err := c.grpcClient.GetCertificate(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if ok {
			return nil, nil, st.Code(), fmt.Errorf("certserver error (%s): %s", st.Code(), st.Message())
		}
		return nil, nil, codes.Unknown, fmt.Errorf("certserver network error: %w", err)
	}

	tlsCert, err := tls.X509KeyPair(resp.CertificateChain, resp.PrivateKey)
	if err != nil {
		return nil, nil, codes.Internal, fmt.Errorf("parsing TLS key pair: %w", err)
	}

	leaf, err := x509.ParseCertificate(tlsCert.Certificate[0])
	if err != nil {
		return nil, nil, codes.Internal, fmt.Errorf("parsing leaf certificate: %w", err)
	}
	tlsCert.Leaf = leaf

	return &tlsCert, leaf, codes.OK, nil
}
