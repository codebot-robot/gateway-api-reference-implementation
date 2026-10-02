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
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	"golang.org/x/net/http2"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// Proxy is a minimal implementation of a Gateway API proxy.
type Proxy struct {
	mu           sync.RWMutex
	routes       []state.InternalRoute
	certificates map[string]*tls.Certificate
	defaultCert  *tls.Certificate
}

func NewProxy() *Proxy {
	return &Proxy{
		routes:       []state.InternalRoute{},
		certificates: make(map[string]*tls.Certificate),
	}
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
	p.mu.RUnlock()

	bestRule, bestMatch := state.MatchRoute(routes, r)

	if bestRule != nil {
		if bestRule.Redirect != nil {
			if bestRule.ResponseHeaderModifier != nil {
				modifyHeaders(w.Header(), *bestRule.ResponseHeaderModifier)
			}
			p.redirect(w, r, *bestRule.Redirect, bestMatch)
			return
		}
		if bestRule.Error != nil {
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
				if bestRule.ResponseHeaderModifier != nil {
					modifyHeaders(w.Header(), *bestRule.ResponseHeaderModifier)
				}
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if backend.Error != nil {
				if bestRule.ResponseHeaderModifier != nil {
					modifyHeaders(w.Header(), *bestRule.ResponseHeaderModifier)
				}
				if backend.ResponseHeaderModifier != nil {
					modifyHeaders(w.Header(), *backend.ResponseHeaderModifier)
				}
				http.Error(w, backend.Error.HTTPMessage, backend.Error.HTTPStatusCode)
				return
			}
			if backend.RequestHeaderModifier != nil {
				p.modifyHeaders(r, *backend.RequestHeaderModifier)
			}
			p.forward(w, r, backend, bestRule.ResponseHeaderModifier)
			return
		}
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

func (p *Proxy) forward(w http.ResponseWriter, r *http.Request, backend state.InternalBackend, respHeaderModifier *gatewayv1.HTTPHeaderFilter) {
	scheme := "http"
	if state.ValueOf(backend.AppProtocol) == "https" {
		scheme = "https"
	}

	target := &url.URL{
		Scheme: scheme,
		Host:   fmt.Sprintf("%s:%d", backend.Host, backend.Port),
	}

	proxy := httputil.NewSingleHostReverseProxy(target)

	if respHeaderModifier != nil || backend.ResponseHeaderModifier != nil {
		proxy.ModifyResponse = func(res *http.Response) error {
			if backend.ResponseHeaderModifier != nil {
				modifyHeaders(res.Header, *backend.ResponseHeaderModifier)
			}
			if respHeaderModifier != nil {
				modifyHeaders(res.Header, *respHeaderModifier)
			}
			return nil
		}
	}

	if scheme == "https" {
		tlsConfig := &tls.Config{InsecureSkipVerify: false}
		if backend.TLSConfig != nil {
			if backend.TLSConfig.Hostname != "" {
				tlsConfig.ServerName = backend.TLSConfig.Hostname
			}
			if len(backend.TLSConfig.CACerts) > 0 {
				tlsConfig.RootCAs = x509.NewCertPool()
				for _, cert := range backend.TLSConfig.CACerts {
					tlsConfig.RootCAs.AppendCertsFromPEM(cert)
				}
			} else {
				tlsConfig.InsecureSkipVerify = true
			}
		} else {
			tlsConfig.InsecureSkipVerify = true
		}
		proxy.Transport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}
	} else if state.ValueOf(backend.AppProtocol) == "kubernetes.io/h2c" {
		proxy.Transport = &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, addr)
			},
		}
	}

	log.Log.Info("Forwarding request", "host", r.Host, "path", r.URL.Path, "target", target.String(), "appProtocol", state.ValueOf(backend.AppProtocol))
	proxy.ServeHTTP(w, r)
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
