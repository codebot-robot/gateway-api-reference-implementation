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
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	"golang.org/x/net/http2"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// Proxy is a minimal implementation of a Gateway API proxy.
type Proxy struct {
	mu           sync.RWMutex
	routes       []state.InternalRoute
	listeners    []state.InternalListener
	certificates map[string]*tls.Certificate
	defaultCert  *tls.Certificate

	hasPassthrough atomic.Bool
	sniCandidates  atomic.Pointer[[]state.InternalListener]
	sniListeners   []*sniListener
}

func NewProxy() *Proxy {
	p := &Proxy{
		routes:       []state.InternalRoute{},
		listeners:    []state.InternalListener{},
		certificates: make(map[string]*tls.Certificate),
	}
	emptyCandidates := []state.InternalListener{}
	p.sniCandidates.Store(&emptyCandidates)
	return p
}

func (p *Proxy) SetDefaultCertificate(cert *tls.Certificate) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.defaultCert = cert
}

func (p *Proxy) UpdateRoutes(routes []state.InternalRoute) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.routes = routes
}

func (p *Proxy) UpdateListeners(listeners []state.InternalListener) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.listeners = listeners
	p.updateSNIConfigLocked(listeners)
}

func (p *Proxy) UpdateConfig(listeners []state.InternalListener, routes []state.InternalRoute) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.listeners = listeners
	p.routes = routes
	p.updateSNIConfigLocked(listeners)
}

func (p *Proxy) updateSNIConfigLocked(listeners []state.InternalListener) {
	hasPassthrough := false
	var candidates []state.InternalListener
	for _, lis := range listeners {
		if lis.Protocol == gatewayv1.TLSProtocolType && lis.TLSMode != nil && *lis.TLSMode == gatewayv1.TLSModePassthrough {
			hasPassthrough = true
		}
		if lis.Protocol == gatewayv1.HTTPSProtocolType || lis.Protocol == gatewayv1.TLSProtocolType {
			candidates = append(candidates, lis)
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Port != candidates[j].Port {
			return candidates[i].Port < candidates[j].Port
		}
		return candidates[i].Name < candidates[j].Name
	})

	p.hasPassthrough.Store(hasPassthrough)
	p.sniCandidates.Store(&candidates)

	for _, l := range p.sniListeners {
		l.updatePassthrough(hasPassthrough)
	}
}

func (p *Proxy) UpdateCertificates(certs map[string]*tls.Certificate, defaultCert *tls.Certificate) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.certificates = certs
	if defaultCert != nil {
		p.defaultCert = defaultCert
	}
}

func (p *Proxy) GetCertificate(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if hello != nil && hello.ServerName != "" {
		sni := strings.ToLower(hello.ServerName)
		if cert, ok := p.certificates[sni]; ok {
			return cert, nil
		}
		parts := strings.Split(sni, ".")
		if len(parts) > 1 {
			wildcard := "*." + strings.Join(parts[1:], ".")
			if cert, ok := p.certificates[wildcard]; ok {
				return cert, nil
			}
		}
	}

	if p.defaultCert != nil {
		return p.defaultCert, nil
	}

	if hello != nil && hello.ServerName != "" {
		return nil, fmt.Errorf("no certificate found for server name %s", hello.ServerName)
	}
	return nil, fmt.Errorf("no default certificate available")
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.RLock()
	routes := p.routes
	listeners := p.listeners
	p.mu.RUnlock()

	if len(listeners) > 0 {
		if r.TLS != nil {
			reqHost := r.Host
			reqPort := int32(443)
			if h, pStr, err := net.SplitHostPort(reqHost); err == nil {
				reqHost = h
				if p, err := strconv.Atoi(pStr); err == nil {
					reqPort = int32(p)
				}
			}
			reqHost = strings.ToLower(reqHost)

			var httpsListeners []state.InternalListener
			for _, l := range listeners {
				if l.Protocol == gatewayv1.HTTPSProtocolType {
					if l.Port == 0 || int32(l.Port) == reqPort {
						httpsListeners = append(httpsListeners, l)
					}
				}
			}

			if len(httpsListeners) > 0 {
				sni := strings.ToLower(r.TLS.ServerName)
				connListeners, connMatchType := state.MatchListeners(httpsListeners, sni)
				if len(connListeners) == 0 {
					http.Error(w, fmt.Sprintf("No listener for server name %s", sni), http.StatusNotFound)
					return
				}

				reqHostListeners, reqMatchType := state.MatchListeners(httpsListeners, reqHost)

				if connMatchType == state.CatchAllMatch {
					if reqMatchType == state.ExactMatch || reqMatchType == state.WildcardMatch {
						http.Error(w, "Misdirected Request", http.StatusMisdirectedRequest)
						return
					}
				} else if connMatchType == state.WildcardMatch {
					hasMatchingConnListener := false
					for _, l := range connListeners {
						if state.MatchesWildcard(l.Hostname, reqHost) {
							hasMatchingConnListener = true
							break
						}
					}
					if !hasMatchingConnListener {
						if len(reqHostListeners) > 0 {
							http.Error(w, "Misdirected Request", http.StatusMisdirectedRequest)
						} else {
							http.Error(w, fmt.Sprintf("No route for host %s and path %s", r.Host, r.URL.Path), http.StatusNotFound)
						}
						return
					}
					if reqMatchType == state.ExactMatch {
						http.Error(w, "Misdirected Request", http.StatusMisdirectedRequest)
						return
					}
				} else if connMatchType == state.ExactMatch {
					hasMatchingConnListener := false
					for _, l := range connListeners {
						if strings.EqualFold(l.Hostname, reqHost) {
							hasMatchingConnListener = true
							break
						}
					}
					if !hasMatchingConnListener {
						if len(reqHostListeners) > 0 {
							http.Error(w, "Misdirected Request", http.StatusMisdirectedRequest)
						} else {
							http.Error(w, fmt.Sprintf("No route for host %s and path %s", r.Host, r.URL.Path), http.StatusNotFound)
						}
						return
					}
				}

				var connRoutes []state.InternalRoute
				for _, l := range connListeners {
					connRoutes = append(connRoutes, l.Routes...)
				}
				routes = connRoutes
			}
		} else {
			reqHost := r.Host
			reqPort := int32(80)
			if h, pStr, err := net.SplitHostPort(reqHost); err == nil {
				reqHost = h
				if p, err := strconv.Atoi(pStr); err == nil {
					reqPort = int32(p)
				}
			}
			reqHost = strings.ToLower(reqHost)

			var httpListeners []state.InternalListener
			for _, l := range listeners {
				if l.Protocol == gatewayv1.HTTPProtocolType {
					if l.Port == 0 || int32(l.Port) == reqPort {
						httpListeners = append(httpListeners, l)
					}
				}
			}

			if len(httpListeners) > 0 {
				reqHostListeners, _ := state.MatchListeners(httpListeners, reqHost)
				if len(reqHostListeners) == 0 {
					http.Error(w, fmt.Sprintf("No route for host %s and path %s", r.Host, r.URL.Path), http.StatusNotFound)
					return
				}
				var matchedRoutes []state.InternalRoute
				for _, l := range reqHostListeners {
					matchedRoutes = append(matchedRoutes, l.Routes...)
				}
				routes = matchedRoutes
			}
		}
	}

	bestRule, bestMatch := state.MatchRoute(routes, r)

	if bestRule != nil {
		cors := bestRule.CORS
		if cors == nil && len(bestRule.Backends) > 0 {
			cors = bestRule.Backends[0].CORS
		}

		// Handle CORS preflight request
		if r.Method == http.MethodOptions && r.Header.Get("Origin") != "" && cors != nil {
			applyCORSHeaders(w.Header(), r, cors, true)
			if bestRule.ResponseHeaderModifier != nil {
				modifyHeaders(w.Header(), *bestRule.ResponseHeaderModifier)
			}
			w.WriteHeader(http.StatusOK)
			return
		}

		if bestRule.Redirect != nil {
			if bestRule.CORS != nil {
				applyCORSHeaders(w.Header(), r, bestRule.CORS, false)
			}
			if bestRule.ResponseHeaderModifier != nil {
				modifyHeaders(w.Header(), *bestRule.ResponseHeaderModifier)
			}
			p.redirect(w, r, *bestRule.Redirect, bestMatch)
			return
		}
		if bestRule.Error != nil {
			if bestRule.CORS != nil {
				applyCORSHeaders(w.Header(), r, bestRule.CORS, false)
			}
			if bestRule.ResponseHeaderModifier != nil {
				modifyHeaders(w.Header(), *bestRule.ResponseHeaderModifier)
			}
			// Per Gateway API specification, if a rule matches but its backend is invalid
			// or unresolved, the implementation SHOULD return an HTTP 500 Internal Server Error.
			// This is also verified by conformance tests like HTTPRouteInvalidBackendRefUnknownKind.
			http.Error(w, bestRule.Error.HTTPMessage, bestRule.Error.HTTPStatusCode)
			return
		}
		if bestRule.Rewrite != nil {
			p.rewrite(r, *bestRule.Rewrite, bestMatch)
		}
		if bestRule.RequestHeaderModifier != nil {
			p.modifyHeaders(r, *bestRule.RequestHeaderModifier)
		}
		if len(bestRule.Backends) > 0 {
			backend, err := pickBackend(bestRule.Backends)
			if err != nil {
				if bestRule.CORS != nil {
					applyCORSHeaders(w.Header(), r, bestRule.CORS, false)
				}
				if bestRule.ResponseHeaderModifier != nil {
					modifyHeaders(w.Header(), *bestRule.ResponseHeaderModifier)
				}
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if backend.Error != nil {
				effectiveCORS := bestRule.CORS
				if backend.CORS != nil {
					effectiveCORS = backend.CORS
				}
				if effectiveCORS != nil {
					applyCORSHeaders(w.Header(), r, effectiveCORS, false)
				}
				if bestRule.ResponseHeaderModifier != nil {
					modifyHeaders(w.Header(), *bestRule.ResponseHeaderModifier)
				}
				if backend.ResponseHeaderModifier != nil {
					modifyHeaders(w.Header(), *backend.ResponseHeaderModifier)
				}
				http.Error(w, backend.Error.HTTPMessage, backend.Error.HTTPStatusCode)
				return
			}

			// Read request body if there are mirrors so it can be sent to both main backend and mirrors.
			// TODO: Support streaming/spooling for request bodies exceeding the memory limit.
			var bodyBytes []byte
			if len(bestRule.Mirrors) > 0 && r.Body != nil && r.Body != http.NoBody {
				const maxMirrorBodySize = 8 * 1024 * 1024 // 8MB limit to avoid OOM
				limitedReader := io.LimitReader(r.Body, maxMirrorBodySize+1)
				var readErr error
				bodyBytes, readErr = io.ReadAll(limitedReader)
				r.Body.Close()
				if readErr != nil {
					log.Log.Error(readErr, "Failed to read request body for mirroring", "path", r.URL.Path)
					if backend.ResponseHeaderModifier != nil {
						modifyHeaders(w.Header(), *backend.ResponseHeaderModifier)
					}
					if bestRule.ResponseHeaderModifier != nil {
						modifyHeaders(w.Header(), *bestRule.ResponseHeaderModifier)
					}
					http.Error(w, "Internal Server Error", http.StatusInternalServerError)
					return
				}
				if int64(len(bodyBytes)) > maxMirrorBodySize {
					log.Log.Info("Request body exceeds maximum size for mirroring", "path", r.URL.Path, "maxBytes", maxMirrorBodySize)
					if backend.ResponseHeaderModifier != nil {
						modifyHeaders(w.Header(), *backend.ResponseHeaderModifier)
					}
					if bestRule.ResponseHeaderModifier != nil {
						modifyHeaders(w.Header(), *bestRule.ResponseHeaderModifier)
					}
					http.Error(w, "Request Entity Too Large", http.StatusRequestEntityTooLarge)
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			}

			// Process mirrors
			for _, m := range bestRule.Mirrors {
				shouldMirror := false
				if m.Numerator >= m.Denominator && m.Denominator > 0 {
					shouldMirror = true
				} else if m.Numerator > 0 && m.Denominator > 0 {
					if rand.Int32N(m.Denominator) < m.Numerator {
						shouldMirror = true
					}
				}
				if shouldMirror {
					p.mirror(r, bodyBytes, m.Backend, bestRule.Timeouts)
				}
			}

			if backend.RequestHeaderModifier != nil {
				p.modifyHeaders(r, *backend.RequestHeaderModifier)
			}
			p.forward(w, r, backend, bestRule.ResponseHeaderModifier, bestRule.CORS, bestRule.Timeouts, bestRule.Retry)
			return
		}

		if bestRule.ResponseHeaderModifier != nil {
			modifyHeaders(w.Header(), *bestRule.ResponseHeaderModifier)
		}
		http.Error(w, "No backend refs specified", http.StatusInternalServerError)
		return
	}

	http.Error(w, fmt.Sprintf("No route for host %s and path %s", r.Host, r.URL.Path), http.StatusNotFound)
}

// modifyHeaders modifies the request headers in place before forwarding.
func (p *Proxy) modifyHeaders(r *http.Request, modifier gatewayv1.HTTPHeaderFilter) {
	modifyHeaders(r.Header, modifier)
}

// modifyHeaders modifies the HTTP headers in place according to the modifier.
func modifyHeaders(header http.Header, modifier gatewayv1.HTTPHeaderFilter) {
	for _, h := range modifier.Remove {
		header.Del(h)
		for k := range header {
			if strings.EqualFold(k, h) {
				delete(header, k)
			}
		}
	}
	for _, h := range modifier.Set {
		for k := range header {
			if strings.EqualFold(k, string(h.Name)) {
				delete(header, k)
			}
		}
		header.Set(string(h.Name), h.Value)
	}
	for _, h := range modifier.Add {
		var existing []string
		for k, v := range header {
			if strings.EqualFold(k, string(h.Name)) {
				existing = append(existing, v...)
				delete(header, k)
			}
		}
		for _, v := range existing {
			header.Add(string(h.Name), v)
		}
		header.Add(string(h.Name), h.Value)
	}
}

// rewrite modifies the incoming *http.Request in place before it is forwarded to the backend.
func (p *Proxy) rewrite(r *http.Request, rewrite state.InternalRewrite, match *state.InternalMatch) {
	if hostname := state.ValueOf(rewrite.Hostname); hostname != "" {
		r.Host = string(hostname)
	}

	if rewrite.Path != nil {
		switch rewrite.Path.Type {
		case gatewayv1.FullPathHTTPPathModifier:
			r.URL.Path = rewrite.Path.Value
			r.URL.RawPath = ""
		case gatewayv1.PrefixMatchHTTPPathModifier:
			prefix := "/"
			isValidPrefixMatch := true
			if match != nil && match.Path != nil {
				if match.Path.Type == gatewayv1.PathMatchPathPrefix {
					prefix = match.Path.Value
				} else {
					isValidPrefixMatch = false
				}
			}
			if isValidPrefixMatch && strings.HasPrefix(r.URL.Path, prefix) {
				suffix := r.URL.Path[len(prefix):]
				if len(suffix) > 0 && !strings.HasPrefix(suffix, "/") {
					suffix = "/" + suffix
				}
				newPath := rewrite.Path.Value + suffix
				for strings.Contains(newPath, "//") {
					newPath = strings.ReplaceAll(newPath, "//", "/")
				}
				r.URL.Path = newPath
				r.URL.RawPath = ""
			}
		}
	}
}

func (p *Proxy) redirect(w http.ResponseWriter, r *http.Request, redirect state.InternalRedirect, match *state.InternalMatch) {
	newURL := &url.URL{
		Path:     r.URL.Path,
		RawQuery: r.URL.RawQuery,
	}

	// Determine scheme
	targetScheme := "http"
	if r.TLS != nil {
		targetScheme = "https"
	}
	if scheme := state.ValueOf(redirect.Scheme); scheme != "" {
		targetScheme = scheme
	}
	newURL.Scheme = targetScheme

	// Determine host and port
	inHost, inPort, err := net.SplitHostPort(r.Host)
	if err != nil {
		inHost = r.Host
		inPort = ""
	}

	targetHost := inHost
	if hostname := state.ValueOf(redirect.Hostname); hostname != "" {
		targetHost = string(hostname)
	}

	var targetPort string
	if redirect.Port != nil {
		targetPort = fmt.Sprintf("%d", *redirect.Port)
	} else if redirect.Scheme != nil && *redirect.Scheme != "" {
		// If redirect scheme is not-empty, the redirect port MUST be the well-known port associated with the redirect scheme.
		// Specifically "http" to port 80 and "https" to port 443.
		if targetScheme == "http" {
			targetPort = "80"
		} else if targetScheme == "https" {
			targetPort = "443"
		}
	} else {
		// If redirect scheme is empty, the redirect port MUST be the Gateway Listener port.
		targetPort = inPort
	}

	// Implementations SHOULD NOT add the port number in the 'Location' header if:
	// - HTTP and port 80
	// - HTTPS and port 443
	if (targetScheme == "http" && targetPort == "80") || (targetScheme == "https" && targetPort == "443") {
		targetPort = ""
	}

	if targetPort != "" {
		newURL.Host = net.JoinHostPort(targetHost, targetPort)
	} else {
		newURL.Host = targetHost
	}

	if redirect.Path != nil {
		switch redirect.Path.Type {
		case gatewayv1.FullPathHTTPPathModifier:
			newURL.Path = redirect.Path.Value
		case gatewayv1.PrefixMatchHTTPPathModifier:
			prefix := "/"
			isValidPrefixMatch := true
			if match != nil && match.Path != nil {
				if match.Path.Type == gatewayv1.PathMatchPathPrefix {
					prefix = match.Path.Value
				} else {
					isValidPrefixMatch = false
				}
			}
			if isValidPrefixMatch && strings.HasPrefix(r.URL.Path, prefix) {
				suffix := r.URL.Path[len(prefix):]
				if len(suffix) > 0 && !strings.HasPrefix(suffix, "/") {
					suffix = "/" + suffix
				}
				newPath := redirect.Path.Value + suffix
				for strings.Contains(newPath, "//") {
					newPath = strings.ReplaceAll(newPath, "//", "/")
				}
				newURL.Path = newPath
			}
		}
	}

	statusCode := state.ValueOf(redirect.StatusCode)
	if statusCode == 0 {
		statusCode = http.StatusFound
	}

	log.Log.Info("Redirecting request", "host", r.Host, "path", r.URL.Path, "target", newURL.String(), "status", statusCode)
	http.Redirect(w, r, newURL.String(), statusCode)
}

var hopByHopHeaders = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailers",
	"Transfer-Encoding",
	"Upgrade",
}

func removeHopByHopHeaders(h http.Header) {
	for _, k := range hopByHopHeaders {
		h.Del(k)
	}
	if c := h.Get("Connection"); c != "" {
		for _, f := range strings.Split(c, ",") {
			if f = strings.TrimSpace(f); f != "" {
				h.Del(f)
			}
		}
	}
}

func isWebSocketRequest(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") &&
		containsToken(r.Header.Get("Connection"), "upgrade")
}

func containsToken(header, token string) bool {
	for _, v := range strings.Split(header, ",") {
		if strings.EqualFold(strings.TrimSpace(v), token) {
			return true
		}
	}
	return false
}

func (p *Proxy) buildBackendRequest(ctx context.Context, r *http.Request, backend state.InternalBackend, body io.Reader) (*http.Request, *url.URL, error) {
	scheme := "http"
	if strings.EqualFold(state.ValueOf(backend.AppProtocol), "https") {
		scheme = "https"
	}

	targetURL := &url.URL{
		Scheme:   scheme,
		Host:     fmt.Sprintf("%s:%d", backend.Host, backend.Port),
		Path:     r.URL.Path,
		RawQuery: r.URL.RawQuery,
	}

	req, err := http.NewRequestWithContext(ctx, r.Method, targetURL.String(), body)
	if err != nil {
		return nil, nil, err
	}

	req.Header = r.Header.Clone()
	removeHopByHopHeaders(req.Header)

	if clientIP, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		if prior := req.Header.Get("X-Forwarded-For"); prior != "" {
			clientIP = prior + ", " + clientIP
		}
		req.Header.Set("X-Forwarded-For", clientIP)
	} else if r.RemoteAddr != "" {
		if prior := req.Header.Get("X-Forwarded-For"); prior != "" {
			req.Header.Set("X-Forwarded-For", prior+", "+r.RemoteAddr)
		} else {
			req.Header.Set("X-Forwarded-For", r.RemoteAddr)
		}
	}

	req.Host = r.Host
	return req, targetURL, nil
}

func (p *Proxy) buildBackendTLSConfig(backend state.InternalBackend) *tls.Config {
	if backend.TLSConfig == nil {
		return &tls.Config{InsecureSkipVerify: true}
	}

	var rootCAs *x509.CertPool
	if backend.TLSConfig.WellKnownCACertificates != nil && *backend.TLSConfig.WellKnownCACertificates == gatewayv1.WellKnownCACertificatesSystem {
		// Leave RootCAs nil so the system pool is used
		rootCAs = nil
	} else if len(backend.TLSConfig.CACerts) > 0 {
		rootCAs = x509.NewCertPool()
		for _, cert := range backend.TLSConfig.CACerts {
			rootCAs.AppendCertsFromPEM(cert)
		}
	} else {
		rootCAs = x509.NewCertPool()
	}

	serverName := backend.TLSConfig.Hostname

	if len(backend.TLSConfig.SubjectAltNames) > 0 {
		sans := backend.TLSConfig.SubjectAltNames
		return &tls.Config{
			ServerName:         serverName,
			InsecureSkipVerify: true,
			VerifyConnection: func(cs tls.ConnectionState) error {
				if len(cs.PeerCertificates) == 0 {
					return fmt.Errorf("no peer certificates presented")
				}
				leaf := cs.PeerCertificates[0]

				verifyOpts := x509.VerifyOptions{
					Roots:         rootCAs,
					Intermediates: x509.NewCertPool(),
				}
				for _, cert := range cs.PeerCertificates[1:] {
					verifyOpts.Intermediates.AddCert(cert)
				}
				if _, err := leaf.Verify(verifyOpts); err != nil {
					return fmt.Errorf("certificate verification failed: %w", err)
				}

				if !verifySubjectAltNames(leaf, sans) {
					return fmt.Errorf("certificate does not match any configured SubjectAltNames")
				}
				return nil
			},
		}
	}

	return &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: false,
		RootCAs:            rootCAs,
	}
}

func verifySubjectAltNames(cert *x509.Certificate, sans []gatewayv1.SubjectAltName) bool {
	for _, san := range sans {
		switch san.Type {
		case gatewayv1.HostnameSubjectAltNameType:
			expectedHost := string(san.Hostname)
			if err := cert.VerifyHostname(expectedHost); err == nil {
				return true
			}
			for _, dns := range cert.DNSNames {
				if strings.EqualFold(dns, expectedHost) {
					return true
				}
			}
		case gatewayv1.URISubjectAltNameType:
			expectedURI := string(san.URI)
			parsedExpected, err := url.Parse(expectedURI)
			for _, u := range cert.URIs {
				if u == nil {
					continue
				}
				if u.String() == expectedURI {
					return true
				}
				if err == nil &&
					strings.EqualFold(u.Scheme, parsedExpected.Scheme) &&
					strings.EqualFold(u.Host, parsedExpected.Host) &&
					u.Path == parsedExpected.Path &&
					u.RawQuery == parsedExpected.RawQuery {
					return true
				}
			}
		}
	}
	return false
}

func (p *Proxy) buildTransport(backend state.InternalBackend) http.RoundTripper {
	if strings.EqualFold(state.ValueOf(backend.AppProtocol), "https") {
		return &http.Transport{
			TLSClientConfig: p.buildBackendTLSConfig(backend),
		}
	} else if state.ValueOf(backend.AppProtocol) == "kubernetes.io/h2c" {
		return &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, addr)
			},
		}
	}
	return http.DefaultTransport
}

func (p *Proxy) forwardWebSocket(w http.ResponseWriter, r *http.Request, backend state.InternalBackend, respHeaderModifier *gatewayv1.HTTPHeaderFilter, timeouts *state.InternalTimeouts) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		if backend.ResponseHeaderModifier != nil {
			modifyHeaders(w.Header(), *backend.ResponseHeaderModifier)
		}
		if respHeaderModifier != nil {
			modifyHeaders(w.Header(), *respHeaderModifier)
		}
		http.Error(w, "Websocket hijacking not supported", http.StatusInternalServerError)
		return
	}

	reqCtx := r.Context()
	if timeouts != nil && timeouts.Request != nil && *timeouts.Request > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(reqCtx, *timeouts.Request)
		defer cancel()
	}

	backendCtx := reqCtx
	if timeouts != nil && timeouts.BackendRequest != nil && *timeouts.BackendRequest > 0 {
		var cancel context.CancelFunc
		backendCtx, cancel = context.WithTimeout(backendCtx, *timeouts.BackendRequest)
		defer cancel()
	}

	scheme := "http"
	if strings.EqualFold(state.ValueOf(backend.AppProtocol), "https") || strings.EqualFold(state.ValueOf(backend.AppProtocol), "wss") {
		scheme = "https"
	}

	targetURL := &url.URL{
		Scheme:   scheme,
		Host:     fmt.Sprintf("%s:%d", backend.Host, backend.Port),
		Path:     r.URL.Path,
		RawQuery: r.URL.RawQuery,
	}

	targetAddr := fmt.Sprintf("%s:%d", backend.Host, backend.Port)
	var backendConn net.Conn
	var err error

	if scheme == "https" {
		tlsConfig := p.buildBackendTLSConfig(backend)
		var d net.Dialer
		backendConn, err = tls.DialWithDialer(&d, "tcp", targetAddr, tlsConfig)
	} else {
		var d net.Dialer
		backendConn, err = d.DialContext(backendCtx, "tcp", targetAddr)
	}

	if err != nil {
		if backend.ResponseHeaderModifier != nil {
			modifyHeaders(w.Header(), *backend.ResponseHeaderModifier)
		}
		if respHeaderModifier != nil {
			modifyHeaders(w.Header(), *respHeaderModifier)
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(backendCtx.Err(), context.DeadlineExceeded) || errors.Is(reqCtx.Err(), context.DeadlineExceeded) {
			http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
			return
		}
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
		return
	}
	defer backendConn.Close()

	outReq, err := http.NewRequestWithContext(backendCtx, r.Method, targetURL.String(), nil)
	if err != nil {
		if backend.ResponseHeaderModifier != nil {
			modifyHeaders(w.Header(), *backend.ResponseHeaderModifier)
		}
		if respHeaderModifier != nil {
			modifyHeaders(w.Header(), *respHeaderModifier)
		}
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
		return
	}

	outReq.Header = r.Header.Clone()
	for _, k := range hopByHopHeaders {
		if strings.EqualFold(k, "Upgrade") || strings.EqualFold(k, "Connection") {
			continue
		}
		outReq.Header.Del(k)
	}

	if clientIP, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		if prior := outReq.Header.Get("X-Forwarded-For"); prior != "" {
			clientIP = prior + ", " + clientIP
		}
		outReq.Header.Set("X-Forwarded-For", clientIP)
	} else if r.RemoteAddr != "" {
		if prior := outReq.Header.Get("X-Forwarded-For"); prior != "" {
			outReq.Header.Set("X-Forwarded-For", prior+", "+r.RemoteAddr)
		} else {
			outReq.Header.Set("X-Forwarded-For", r.RemoteAddr)
		}
	}

	outReq.Host = r.Host

	log.Log.Info("Forwarding WebSocket handshake", "host", r.Host, "path", r.URL.Path, "target", targetURL.String())

	if err := outReq.Write(backendConn); err != nil {
		if backend.ResponseHeaderModifier != nil {
			modifyHeaders(w.Header(), *backend.ResponseHeaderModifier)
		}
		if respHeaderModifier != nil {
			modifyHeaders(w.Header(), *respHeaderModifier)
		}
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
		return
	}

	br := bufio.NewReader(backendConn)
	resp, err := http.ReadResponse(br, outReq)
	if err != nil {
		if backend.ResponseHeaderModifier != nil {
			modifyHeaders(w.Header(), *backend.ResponseHeaderModifier)
		}
		if respHeaderModifier != nil {
			modifyHeaders(w.Header(), *respHeaderModifier)
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(backendCtx.Err(), context.DeadlineExceeded) || errors.Is(reqCtx.Err(), context.DeadlineExceeded) {
			http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
			return
		}
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	resp.Header.Del("Alt-Svc")

	if backend.ResponseHeaderModifier != nil {
		modifyHeaders(resp.Header, *backend.ResponseHeaderModifier)
	}
	if respHeaderModifier != nil {
		modifyHeaders(resp.Header, *respHeaderModifier)
	}

	if resp.StatusCode != http.StatusSwitchingProtocols {
		removeHopByHopHeaders(resp.Header)
		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	clientConn, clientBuf, err := hijacker.Hijack()
	if err != nil {
		http.Error(w, "Hijack failed", http.StatusInternalServerError)
		return
	}
	defer clientConn.Close()

	if err := resp.Write(clientConn); err != nil {
		return
	}

	if clientBuf != nil && clientBuf.Reader.Buffered() > 0 {
		bufferedBytes, _ := io.ReadAll(io.LimitReader(clientBuf, int64(clientBuf.Reader.Buffered())))
		backendConn.Write(bufferedBytes)
	}
	if br.Buffered() > 0 {
		bufferedBytes, _ := io.ReadAll(io.LimitReader(br, int64(br.Buffered())))
		clientConn.Write(bufferedBytes)
	}

	errc := make(chan error, 2)
	go func() {
		_, err := io.Copy(backendConn, clientConn)
		errc <- err
	}()
	go func() {
		_, err := io.Copy(clientConn, backendConn)
		errc <- err
	}()
	<-errc
}

func (p *Proxy) forward(w http.ResponseWriter, r *http.Request, backend state.InternalBackend, respHeaderModifier *gatewayv1.HTTPHeaderFilter, respCORS *gatewayv1.HTTPCORSFilter, timeouts *state.InternalTimeouts, retry *state.CompiledHTTPRouteRetry) {
	if isWebSocketRequest(r) {
		p.forwardWebSocket(w, r, backend, respHeaderModifier, timeouts)
		return
	}

	reqCtx := r.Context()
	if timeouts != nil && timeouts.Request != nil && *timeouts.Request > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(reqCtx, *timeouts.Request)
		defer cancel()
	}

	scheme := "http"
	if strings.EqualFold(state.ValueOf(backend.AppProtocol), "https") {
		scheme = "https"
	}

	targetURL := &url.URL{
		Scheme:   scheme,
		Host:     fmt.Sprintf("%s:%d", backend.Host, backend.Port),
		Path:     r.URL.Path,
		RawQuery: r.URL.RawQuery,
	}

	var bodyBytes []byte
	if r.Body != nil && r.Body != http.NoBody {
		var err error
		bodyBytes, err = io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
	}

	transport := p.buildTransport(backend)

	maxRetries := 0
	if retry != nil && retry.Attempts != nil && *retry.Attempts > 0 {
		maxRetries = *retry.Attempts
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if reqCtx.Err() != nil {
			effectiveCORS := respCORS
			if backend.CORS != nil {
				effectiveCORS = backend.CORS
			}
			if effectiveCORS != nil {
				applyCORSHeaders(w.Header(), r, effectiveCORS, false)
			}
			if backend.ResponseHeaderModifier != nil {
				modifyHeaders(w.Header(), *backend.ResponseHeaderModifier)
			}
			if respHeaderModifier != nil {
				modifyHeaders(w.Header(), *respHeaderModifier)
			}
			if errors.Is(reqCtx.Err(), context.DeadlineExceeded) {
				http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
			} else {
				http.Error(w, "Bad Gateway", http.StatusBadGateway)
			}
			return
		}

		if attempt > 0 && retry != nil && retry.Backoff != nil && *retry.Backoff > 0 {
			select {
			case <-time.After(*retry.Backoff):
			case <-reqCtx.Done():
				effectiveCORS := respCORS
				if backend.CORS != nil {
					effectiveCORS = backend.CORS
				}
				if effectiveCORS != nil {
					applyCORSHeaders(w.Header(), r, effectiveCORS, false)
				}
				if backend.ResponseHeaderModifier != nil {
					modifyHeaders(w.Header(), *backend.ResponseHeaderModifier)
				}
				if respHeaderModifier != nil {
					modifyHeaders(w.Header(), *respHeaderModifier)
				}
				http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
				return
			}
		}

		backendCtx := reqCtx
		var cancel context.CancelFunc
		if timeouts != nil && timeouts.BackendRequest != nil && *timeouts.BackendRequest > 0 {
			backendCtx, cancel = context.WithTimeout(reqCtx, *timeouts.BackendRequest)
		}

		var bodyReader io.Reader
		if bodyBytes != nil {
			bodyReader = bytes.NewReader(bodyBytes)
		}
		outReq, err := http.NewRequestWithContext(backendCtx, r.Method, targetURL.String(), bodyReader)
		if err != nil {
			if cancel != nil {
				cancel()
			}
			effectiveCORS := respCORS
			if backend.CORS != nil {
				effectiveCORS = backend.CORS
			}
			if effectiveCORS != nil {
				applyCORSHeaders(w.Header(), r, effectiveCORS, false)
			}
			if backend.ResponseHeaderModifier != nil {
				modifyHeaders(w.Header(), *backend.ResponseHeaderModifier)
			}
			if respHeaderModifier != nil {
				modifyHeaders(w.Header(), *respHeaderModifier)
			}
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
			return
		}

		outReq.Header = r.Header.Clone()
		removeHopByHopHeaders(outReq.Header)

		if clientIP, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			if prior := outReq.Header.Get("X-Forwarded-For"); prior != "" {
				clientIP = prior + ", " + clientIP
			}
			outReq.Header.Set("X-Forwarded-For", clientIP)
		} else if r.RemoteAddr != "" {
			if prior := outReq.Header.Get("X-Forwarded-For"); prior != "" {
				outReq.Header.Set("X-Forwarded-For", prior+", "+r.RemoteAddr)
			} else {
				outReq.Header.Set("X-Forwarded-For", r.RemoteAddr)
			}
		}

		outReq.Host = r.Host

		log.Log.Info("Forwarding request", "host", r.Host, "path", r.URL.Path, "target", targetURL.String(), "appProtocol", state.ValueOf(backend.AppProtocol), "attempt", attempt)

		resp, err := transport.RoundTrip(outReq)
		if cancel != nil {
			cancel()
		}

		if err != nil {
			if attempt < maxRetries && reqCtx.Err() == nil {
				continue
			}
			effectiveCORS := respCORS
			if backend.CORS != nil {
				effectiveCORS = backend.CORS
			}
			if effectiveCORS != nil {
				applyCORSHeaders(w.Header(), r, effectiveCORS, false)
			}
			if backend.ResponseHeaderModifier != nil {
				modifyHeaders(w.Header(), *backend.ResponseHeaderModifier)
			}
			if respHeaderModifier != nil {
				modifyHeaders(w.Header(), *respHeaderModifier)
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(backendCtx.Err(), context.DeadlineExceeded) || errors.Is(reqCtx.Err(), context.DeadlineExceeded) {
				http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
				return
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
				return
			}
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
			return
		}

		shouldRetry := false
		if retry != nil && attempt < maxRetries {
			for _, code := range retry.Codes {
				if resp.StatusCode == code {
					shouldRetry = true
					break
				}
			}
		}

		if shouldRetry {
			io.Copy(io.Discard, io.LimitReader(resp.Body, 1024*1024))
			resp.Body.Close()
			continue
		}

		defer resp.Body.Close()

		resp.Header.Del("Alt-Svc")

		effectiveCORS := respCORS
		if backend.CORS != nil {
			effectiveCORS = backend.CORS
		}
		if effectiveCORS != nil {
			applyCORSHeaders(resp.Header, r, effectiveCORS, false)
		}

		if backend.ResponseHeaderModifier != nil {
			modifyHeaders(resp.Header, *backend.ResponseHeaderModifier)
		}
		if respHeaderModifier != nil {
			modifyHeaders(resp.Header, *respHeaderModifier)
		}

		removeHopByHopHeaders(resp.Header)

		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}
}

func matchOrigin(allowedOrigins []gatewayv1.CORSOrigin, reqOrigin string) bool {
	if len(allowedOrigins) == 0 || reqOrigin == "" {
		return false
	}
	u, err := url.Parse(reqOrigin)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return false
	}
	reqScheme := strings.ToLower(u.Scheme)
	reqHost := u.Hostname()
	reqPort := u.Port()
	if reqPort == "" {
		if reqScheme == "http" {
			reqPort = "80"
		} else if reqScheme == "https" {
			reqPort = "443"
		}
	}

	for _, allowed := range allowedOrigins {
		allowedStr := string(allowed)
		if allowedStr == "*" {
			return true
		}
		au, err := url.Parse(allowedStr)
		if err != nil || au.Scheme == "" || au.Host == "" {
			continue
		}
		allowedScheme := strings.ToLower(au.Scheme)
		allowedHost := au.Hostname()
		allowedPort := au.Port()
		if allowedPort == "" {
			if allowedScheme == "http" {
				allowedPort = "80"
			} else if allowedScheme == "https" {
				allowedPort = "443"
			}
		}

		if allowedScheme != reqScheme {
			continue
		}
		if allowedPort != reqPort {
			continue
		}

		if allowedHost == "*" {
			return true
		}
		if strings.HasPrefix(allowedHost, "*.") {
			suffix := strings.ToLower(allowedHost[1:])
			reqHostLower := strings.ToLower(reqHost)
			if strings.HasSuffix(reqHostLower, suffix) && len(reqHostLower) > len(suffix) {
				return true
			}
		}
		if strings.EqualFold(allowedHost, reqHost) {
			return true
		}
	}
	return false
}

func containsWildcardOrigin(origins []gatewayv1.CORSOrigin) bool {
	for _, o := range origins {
		if string(o) == "*" {
			return true
		}
	}
	return false
}

func containsWildcardMethod(methods []gatewayv1.HTTPMethodWithWildcard) bool {
	for _, m := range methods {
		if string(m) == "*" {
			return true
		}
	}
	return false
}

func containsWildcardHeader(headers []gatewayv1.HTTPHeaderName) bool {
	for _, h := range headers {
		if string(h) == "*" {
			return true
		}
	}
	return false
}

func applyCORSHeaders(header http.Header, r *http.Request, cors *gatewayv1.HTTPCORSFilter, isPreflight bool) bool {
	if cors == nil {
		return false
	}
	reqOrigin := r.Header.Get("Origin")
	if reqOrigin == "" {
		return false
	}
	if !matchOrigin(cors.AllowOrigins, reqOrigin) {
		return false
	}

	allowCredentials := cors.AllowCredentials != nil && *cors.AllowCredentials

	// Access-Control-Allow-Origin
	if allowCredentials {
		header.Set("Access-Control-Allow-Origin", reqOrigin)
	} else if containsWildcardOrigin(cors.AllowOrigins) {
		header.Set("Access-Control-Allow-Origin", "*")
	} else {
		header.Set("Access-Control-Allow-Origin", reqOrigin)
	}

	// Access-Control-Allow-Credentials
	if allowCredentials {
		header.Set("Access-Control-Allow-Credentials", "true")
	} else {
		header.Del("Access-Control-Allow-Credentials")
		for k := range header {
			if strings.EqualFold(k, "Access-Control-Allow-Credentials") {
				delete(header, k)
			}
		}
	}

	// Access-Control-Expose-Headers
	if len(cors.ExposeHeaders) > 0 {
		if containsWildcardHeader(cors.ExposeHeaders) {
			if !allowCredentials {
				header.Set("Access-Control-Expose-Headers", "*")
			}
		} else {
			headers := make([]string, len(cors.ExposeHeaders))
			for i, h := range cors.ExposeHeaders {
				headers[i] = string(h)
			}
			header.Set("Access-Control-Expose-Headers", strings.Join(headers, ", "))
		}
	}

	if isPreflight {
		// Access-Control-Allow-Methods
		if len(cors.AllowMethods) > 0 {
			if containsWildcardMethod(cors.AllowMethods) {
				reqMethod := r.Header.Get("Access-Control-Request-Method")
				if reqMethod != "" {
					header.Set("Access-Control-Allow-Methods", reqMethod)
				} else if !allowCredentials {
					header.Set("Access-Control-Allow-Methods", "*")
				}
			} else {
				methods := make([]string, len(cors.AllowMethods))
				for i, m := range cors.AllowMethods {
					methods[i] = string(m)
				}
				header.Set("Access-Control-Allow-Methods", strings.Join(methods, ", "))
			}
		}

		// Access-Control-Allow-Headers
		if len(cors.AllowHeaders) > 0 {
			if containsWildcardHeader(cors.AllowHeaders) {
				reqHeaders := r.Header.Get("Access-Control-Request-Headers")
				if reqHeaders != "" {
					header.Set("Access-Control-Allow-Headers", reqHeaders)
				} else if !allowCredentials {
					header.Set("Access-Control-Allow-Headers", "*")
				}
			} else {
				headers := make([]string, len(cors.AllowHeaders))
				for i, h := range cors.AllowHeaders {
					headers[i] = string(h)
				}
				header.Set("Access-Control-Allow-Headers", strings.Join(headers, ", "))
			}
		}

		// Access-Control-Max-Age
		maxAge := cors.MaxAge
		if maxAge <= 0 {
			maxAge = 5
		}
		header.Set("Access-Control-Max-Age", strconv.Itoa(int(maxAge)))
	}

	return true
}

// pickBackend selects a backend from the list based on their weights.
// If all backends have weight 0 or the list is empty, an error is returned.
func pickBackend(backends []state.InternalBackend) (state.InternalBackend, error) {
	if len(backends) == 0 {
		return state.InternalBackend{}, fmt.Errorf("no backends configured")
	}

	if len(backends) == 1 {
		if backends[0].Weight <= 0 {
			return state.InternalBackend{}, fmt.Errorf("all backends have zero weight")
		}
		return backends[0], nil
	}

	var totalWeight int64
	for _, b := range backends {
		if b.Weight > 0 {
			totalWeight += int64(b.Weight)
		}
	}

	if totalWeight <= 0 {
		return state.InternalBackend{}, fmt.Errorf("all backends have zero weight")
	}

	n := rand.Int64N(totalWeight)
	for _, b := range backends {
		if b.Weight <= 0 {
			continue
		}
		if n < int64(b.Weight) {
			return b, nil
		}
		n -= int64(b.Weight)
	}

	return backends[len(backends)-1], nil
}

func (p *Proxy) mirror(r *http.Request, bodyBytes []byte, backend state.InternalBackend, timeouts *state.InternalTimeouts) {
	go func() {
		timeout := 30 * time.Second
		if timeouts != nil && timeouts.BackendRequest != nil && *timeouts.BackendRequest > 0 {
			timeout = *timeouts.BackendRequest
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		var body io.Reader
		if len(bodyBytes) > 0 {
			body = bytes.NewReader(bodyBytes)
		}

		mirrorReq, targetURL, err := p.buildBackendRequest(ctx, r, backend, body)
		if err != nil {
			log.Log.Error(err, "Failed to create mirror request")
			return
		}

		transport := p.buildTransport(backend)
		log.Log.Info("Mirroring request", "host", mirrorReq.Host, "path", mirrorReq.URL.Path, "target", targetURL.String())

		resp, err := transport.RoundTrip(mirrorReq)
		if err != nil {
			log.Log.Error(err, "Failed to send mirrored request", "target", targetURL.String())
			return
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
	}()
}
