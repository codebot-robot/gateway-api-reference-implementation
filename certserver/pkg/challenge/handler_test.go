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
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestChallengeHandler_AcceptanceCriteria(t *testing.T) {
	ctx := t.Context()
	store := NewMemoryStore()
	handler := NewHandler(store)

	now := time.Now()
	store.timeNow = func() time.Time { return now }

	validToken := "valid-token-12345"
	validKeyAuth := "valid-token-12345.thumbprint-abcde"
	validHost := "example.com"

	expiredToken := "expired-token-67890"
	expiredKeyAuth := "expired-token-67890.thumbprint-fghij"

	// Register valid challenge
	if err := store.Put(ctx, validToken, validHost, validKeyAuth, now.Add(10*time.Minute)); err != nil {
		t.Fatalf("store.Put valid challenge failed: %v", err)
	}

	// Register expired challenge
	if err := store.Put(ctx, expiredToken, validHost, expiredKeyAuth, now.Add(-1*time.Minute)); err != nil {
		t.Fatalf("store.Put expired challenge failed: %v", err)
	}

	tests := []struct {
		name           string
		token          string
		host           string
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "unknown token",
			token:          "unknown-token-xyz",
			host:           validHost,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "expired token",
			token:          expiredToken,
			host:           validHost,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "wrong Host",
			token:          validToken,
			host:           "wrong.example.com",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "correct response",
			token:          validToken,
			host:           validHost,
			expectedStatus: http.StatusOK,
			expectedBody:   validKeyAuth,
		},
		{
			name:           "correct response with host port and uppercase",
			token:          validToken,
			host:           "EXAMPLE.COM:8080",
			expectedStatus: http.StatusOK,
			expectedBody:   validKeyAuth,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, ACMEChallengePrefix+tc.token, nil)
			req.Host = tc.host

			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			resp := rr.Result()
			defer resp.Body.Close()

			if resp.StatusCode != tc.expectedStatus {
				t.Errorf("expected status %d, got %d", tc.expectedStatus, resp.StatusCode)
			}

			if tc.expectedBody != "" {
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatalf("failed to read response body: %v", err)
				}
				if string(body) != tc.expectedBody {
					t.Errorf("expected body %q, got %q", tc.expectedBody, string(body))
				}
			}
		})
	}
}

func TestChallengeHandler_MethodNotAllowedAndMalformedPaths(t *testing.T) {
	store := NewMemoryStore()
	handler := NewHandler(store)

	// POST should return 405 Method Not Allowed
	req := httptest.NewRequest(http.MethodPost, ACMEChallengePrefix+"token", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 for POST, got %d", rr.Code)
	}

	// Empty token or path traversal
	for _, path := range []string{
		ACMEChallengePrefix,
		ACMEChallengePrefix + "../other",
		ACMEChallengePrefix + "sub/path",
		"/wrong-prefix/token",
	} {
		req = httptest.NewRequest(http.MethodGet, path, nil)
		rr = httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Errorf("expected 404 for path %q, got %d", path, rr.Code)
		}
	}
}

func TestChallengeHandler_CallerIdentity(t *testing.T) {
	ctx := t.Context()
	store := NewMemoryStore()

	var recordedCaller string
	customResolver := &testResolverWithCaller{
		store: store,
		onResolve: func(caller string) {
			recordedCaller = caller
		},
	}

	handler := NewHandler(customResolver, WithCallerFunc(func(r *http.Request) string {
		return r.Header.Get("X-Caller-ID")
	}))

	now := time.Now()
	store.timeNow = func() time.Time { return now }
	if err := store.Put(ctx, "caller-tok", "example.com", "caller-auth", now.Add(time.Minute)); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, ACMEChallengePrefix+"caller-tok", nil)
	req.Host = "example.com"
	req.Header.Set("X-Caller-ID", "gateway-cluster-1")

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
	if recordedCaller != "gateway-cluster-1" {
		t.Errorf("expected recorded caller 'gateway-cluster-1', got %q", recordedCaller)
	}
}

type testResolverWithCaller struct {
	store     *MemoryStore
	onResolve func(caller string)
}

func (r *testResolverWithCaller) ResolveChallenge(ctx context.Context, caller string, host string, token string) (string, error) {
	if r.onResolve != nil {
		r.onResolve(caller)
	}
	return r.store.ResolveChallenge(ctx, caller, host, token)
}
