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

package challenge

import (
	"net/http"
	"strings"
)

const (
	// ACMEChallengePrefix is the standard URI prefix for ACME HTTP-01 challenges.
	ACMEChallengePrefix = "/.well-known/acme-challenge/"
)

// HandlerOption configures the challenge HTTP handler.
type HandlerOption func(*Handler)

// WithCallerFunc configures a caller identification function.
func WithCallerFunc(fn func(*http.Request) string) HandlerOption {
	return func(h *Handler) {
		h.callerFunc = fn
	}
}

// Handler implements http.Handler to serve HTTP-01 ACME challenge validations.
type Handler struct {
	resolver   ChallengeResolver
	callerFunc func(*http.Request) string
}

// NewHandler creates a new ACME HTTP-01 challenge handler using the provided resolver.
func NewHandler(resolver ChallengeResolver, opts ...HandlerOption) *Handler {
	h := &Handler{
		resolver: resolver,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// ServeHTTP handles requests for /.well-known/acme-challenge/<token>.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	if !strings.HasPrefix(r.URL.Path, ACMEChallengePrefix) {
		http.NotFound(w, r)
		return
	}

	token := strings.TrimPrefix(r.URL.Path, ACMEChallengePrefix)
	// Reject empty tokens or path traversal attempts
	if token == "" || strings.Contains(token, "/") || strings.Contains(token, "..") {
		http.NotFound(w, r)
		return
	}

	caller := ""
	if h.callerFunc != nil {
		caller = h.callerFunc(r)
	}

	keyAuth, err := h.resolver.ResolveChallenge(r.Context(), caller, r.Host, token)
	if err != nil {
		// Return 404 for unknown, expired, or wrong Host.
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		if _, err := w.Write([]byte(keyAuth)); err != nil {
			return
		}
	}
}
