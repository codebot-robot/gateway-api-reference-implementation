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

package proxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"k8s.io/klog/v2"
)

const (
	// ACMEChallengePrefix is the standard URI path prefix for ACME HTTP-01 challenges.
	ACMEChallengePrefix = "/.well-known/acme-challenge/"

	// maxACMETokenLength bounds the maximum length accepted for ACME challenge tokens.
	maxACMETokenLength = 128

	// maxACMEResponseBodyBytes limits the relayed response body size to 1 KiB.
	maxACMEResponseBodyBytes = 1024

	// defaultACMETimeout is the default short timeout for upstream challenge requests.
	defaultACMETimeout = 2 * time.Second
)

// TokenSource provides bearer tokens for authenticating forwarded requests.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

// TokenSourceFunc allows a function to be used as a TokenSource.
type TokenSourceFunc func(ctx context.Context) (string, error)

func (f TokenSourceFunc) Token(ctx context.Context) (string, error) {
	return f(ctx)
}

// StaticTokenSource returns a fixed bearer token string.
type StaticTokenSource string

func (s StaticTokenSource) Token(ctx context.Context) (string, error) {
	return string(s), nil
}

// FileTokenSource reads and caches a token from a file path, re-reading when the file changes.
type FileTokenSource struct {
	path string

	// mu guards cachedToken, lastModTime, and lastSize.
	mu          sync.Mutex
	lastModTime time.Time
	lastSize    int64
	cachedToken string
}

// NewFileTokenSource creates a new FileTokenSource for the given file path.
func NewFileTokenSource(path string) *FileTokenSource {
	return &FileTokenSource{path: path}
}

// Token returns the bearer token from the file, re-reading from disk whenever the file changes.
func (s *FileTokenSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fi, err := os.Stat(s.path)
	if err != nil {
		return "", err
	}

	if fi.ModTime().Equal(s.lastModTime) && fi.Size() == s.lastSize && s.cachedToken != "" {
		return s.cachedToken, nil
	}

	data, err := os.ReadFile(s.path)
	if err != nil {
		return "", err
	}

	s.cachedToken = strings.TrimSpace(string(data))
	s.lastModTime = fi.ModTime()
	s.lastSize = fi.Size()
	return s.cachedToken, nil
}

// isValidACMEToken reports whether token is non-empty, within length bounds (<=128),
// a single path segment (no '/'), and composed exclusively of base64url characters (RFC 8555).
func isValidACMEToken(token string) bool {
	if len(token) == 0 || len(token) > maxACMETokenLength {
		return false
	}
	for i := 0; i < len(token); i++ {
		c := token[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

// ACMEChallengeConfig configures forwarding of ACME HTTP-01 challenge requests to an upstream certserver.
type ACMEChallengeConfig struct {
	UpstreamURL  string
	CABundlePEM  string
	CABundlePath string
	TokenPath    string
	Timeout      time.Duration
}

// NewACMEChallengeForwarder creates an http.Handler that forwards ACME HTTP-01 challenge
// requests to an upstream certserver according to cfg and tokens.
// If the token is invalid, the request is not GET/HEAD, or the upstream returns 404 or an error,
// it falls through by responding with 404 Not Found.
func NewACMEChallengeForwarder(cfg ACMEChallengeConfig, tokens TokenSource) (http.Handler, error) {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultACMETimeout
	}
	f, err := newACMEForwarder(&cfg, tokens, timeout)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// acmeForwarder manages forwarding ACME HTTP-01 challenge requests to an upstream certserver.
type acmeForwarder struct {
	upstreamURL *url.URL
	httpClient  *http.Client
	tokenSource TokenSource
	timeout     time.Duration
	logLimiter  *rate.Limiter
}

// tryForward checks if the request matches the ACME HTTP-01 challenge prefix,
// validates the token, and attempts to forward the challenge to the upstream certserver.
// Returns true if the challenge was successfully relayed (200 OK from upstream),
// or false if the path or token does not match, or the upstream returns 404, an error, or timeout.
func (f *acmeForwarder) tryForward(w http.ResponseWriter, r *http.Request) bool {
	if f == nil {
		return false
	}
	if r.Context().Value(originalHostContextKey) == nil {
		r = r.WithContext(context.WithValue(r.Context(), originalHostContextKey, r.Host))
	}
	if !strings.HasPrefix(r.URL.Path, ACMEChallengePrefix) {
		return false
	}
	token := strings.TrimPrefix(r.URL.Path, ACMEChallengePrefix)
	if !isValidACMEToken(token) {
		return false
	}
	return f.forward(w, r, token)
}

// ServeHTTP implements http.Handler by invoking tryForward and responding with
// 404 Not Found if forwarding returns false.
func (f *acmeForwarder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !f.tryForward(w, r) {
		http.NotFound(w, r)
	}
}

func newACMEForwarder(cfg *ACMEChallengeConfig, tokenSource TokenSource, timeout time.Duration) (*acmeForwarder, error) {
	if cfg == nil || cfg.UpstreamURL == "" {
		return nil, fmt.Errorf("upstream URL is required")
	}

	u, err := url.Parse(cfg.UpstreamURL)
	if err != nil {
		return nil, fmt.Errorf("invalid upstream URL %q: %w", cfg.UpstreamURL, err)
	}

	if !strings.EqualFold(u.Scheme, "https") {
		return nil, fmt.Errorf("upstream URL scheme must be https, got %q", u.Scheme)
	}

	if timeout <= 0 {
		timeout = defaultACMETimeout
	}

	tlsConfig := &tls.Config{}
	var caBytes []byte
	if cfg.CABundlePEM != "" {
		caBytes = []byte(cfg.CABundlePEM)
	} else if cfg.CABundlePath != "" {
		data, err := os.ReadFile(cfg.CABundlePath)
		if err != nil {
			return nil, fmt.Errorf("reading CA bundle file %q: %w", cfg.CABundlePath, err)
		}
		caBytes = data
	}

	if len(caBytes) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBytes) {
			return nil, fmt.Errorf("failed to parse CA bundle PEM")
		}
		tlsConfig.RootCAs = pool
	}

	transport := &http.Transport{
		TLSClientConfig:       tlsConfig,
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: timeout}).DialContext,
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: timeout,
		ExpectContinueTimeout: 1 * time.Second,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	ts := tokenSource
	if ts == nil && cfg.TokenPath != "" {
		ts = NewFileTokenSource(cfg.TokenPath)
	}
	if ts == nil {
		return nil, fmt.Errorf("token path is required when upstream URL is set")
	}

	return &acmeForwarder{
		upstreamURL: u,
		httpClient:  client,
		tokenSource: ts,
		timeout:     timeout,
		logLimiter:  rate.NewLimiter(rate.Every(5*time.Second), 1),
	}, nil
}

// forward attempts to forward an ACME challenge request to the configured upstream.
// Returns true if the challenge was successfully relayed (200 OK from upstream),
// or false if it should fall through to normal routing (404, error, or timeout).
func (f *acmeForwarder) forward(w http.ResponseWriter, r *http.Request, token string) bool {
	if f == nil || f.httpClient == nil {
		return false
	}

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}

	u := *f.upstreamURL
	u.Path = strings.TrimSuffix(u.Path, "/") + ACMEChallengePrefix + token
	targetURL := u.String()

	ctx, cancel := context.WithTimeout(r.Context(), f.timeout)
	defer cancel()

	outReq, err := http.NewRequestWithContext(ctx, r.Method, targetURL, nil)
	if err != nil {
		f.logError("failed to create ACME challenge upstream request: %v", err)
		return false
	}

	origHost := r.Host
	if v := r.Context().Value(originalHostContextKey); v != nil {
		if s, ok := v.(string); ok && s != "" {
			origHost = s
		}
	}
	outReq.Host = origHost

	// Attach bearer token if configured. Upstream is verified to be HTTPS.
	if f.tokenSource != nil {
		tok, err := f.tokenSource.Token(ctx)
		if err != nil {
			f.logError("failed to obtain bearer token for ACME challenge: %v", err)
			return false
		}
		if tok != "" {
			outReq.Header.Set("Authorization", "Bearer "+tok)
		}
	}

	resp, err := f.httpClient.Do(outReq)
	if err != nil {
		f.logError("ACME challenge upstream request error: %v", err)
		return false
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			f.logError("failed closing ACME challenge response body: %v", closeErr)
		}
	}()

	if resp.StatusCode == http.StatusOK {
		// Read body size-limited to 1 KiB
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxACMEResponseBodyBytes))
		if err != nil {
			f.logError("failed to read ACME challenge upstream response: %v", err)
			return false
		}

		// Relay only the body and Content-Type/Content-Length: no upstream headers, cookies, or redirects
		ct := resp.Header.Get("Content-Type")
		if !strings.HasPrefix(ct, "application/octet-stream") && !strings.HasPrefix(ct, "text/plain") {
			ct = "text/plain; charset=utf-8"
		}

		w.Header().Set("Content-Type", ct)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			if _, writeErr := w.Write(body); writeErr != nil {
				f.logError("failed writing ACME challenge relayed response: %v", writeErr)
			}
		}
		return true
	}

	if resp.StatusCode == http.StatusNotFound {
		// 404 is an expected response when certserver does not recognize the token;
		// fall through to normal routing without error logging.
		return false
	}

	f.logError("ACME challenge upstream returned unexpected status %d", resp.StatusCode)
	return false
}

func (f *acmeForwarder) logError(format string, args ...any) {
	if f.logLimiter == nil || f.logLimiter.Allow() {
		msg := fmt.Sprintf(format, args...)
		msg = redactACMETokens(msg)
		klog.Info(msg)
	}
}

// redactACMETokens ensures tokens are not logged.
func redactACMETokens(s string) string {
	if idx := strings.Index(s, ACMEChallengePrefix); idx != -1 {
		rest := s[idx+len(ACMEChallengePrefix):]
		end := strings.IndexAny(rest, " \t\r\n\"':?/")
		if end == -1 {
			s = s[:idx+len(ACMEChallengePrefix)] + "<redacted>"
		} else {
			s = s[:idx+len(ACMEChallengePrefix)] + "<redacted>" + rest[end:]
		}
	}
	return s
}
