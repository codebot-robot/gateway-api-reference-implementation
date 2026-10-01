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
	"net/http/httptest"
	"testing"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestProxyRewrite(t *testing.T) {
	tests := []struct {
		name            string
		rewrite         state.InternalRewrite
		match           *state.InternalMatch
		initialPath     string
		initialRawPath  string
		initialHost     string
		expectedPath    string
		expectedRawPath string
		expectedHost    string
	}{
		{
			name: "host rewrite",
			rewrite: state.InternalRewrite{
				Hostname: state.Ptr(gatewayv1.PreciseHostname("new.example.com")),
			},
			initialHost:  "old.example.com",
			expectedHost: "new.example.com",
			initialPath:  "/foo",
			expectedPath: "/foo",
		},
		{
			name: "full path rewrite",
			rewrite: state.InternalRewrite{
				Path: &state.InternalPathRewrite{
					Type:  gatewayv1.FullPathHTTPPathModifier,
					Value: "/new-path",
				},
			},
			initialHost:  "example.com",
			expectedHost: "example.com",
			initialPath:  "/old-path",
			expectedPath: "/new-path",
		},
		{
			name: "full path rewrite with encoded path",
			rewrite: state.InternalRewrite{
				Path: &state.InternalPathRewrite{
					Type:  gatewayv1.FullPathHTTPPathModifier,
					Value: "/new-path",
				},
			},
			initialHost:     "example.com",
			expectedHost:    "example.com",
			initialPath:     "/old path",
			initialRawPath:  "/old%20path",
			expectedPath:    "/new-path",
			expectedRawPath: "",
		},
		{
			name: "prefix path rewrite",
			rewrite: state.InternalRewrite{
				Path: &state.InternalPathRewrite{
					Type:  gatewayv1.PrefixMatchHTTPPathModifier,
					Value: "/new-prefix",
				},
			},
			match: &state.InternalMatch{
				Path: &state.InternalPathMatch{
					Type:  gatewayv1.PathMatchPathPrefix,
					Value: "/old-prefix",
				},
			},
			initialHost:  "example.com",
			expectedHost: "example.com",
			initialPath:  "/old-prefix/suffix",
			expectedPath: "/new-prefix/suffix",
		},
		{
			name: "prefix path rewrite with missing match (default /)",
			rewrite: state.InternalRewrite{
				Path: &state.InternalPathRewrite{
					Type:  gatewayv1.PrefixMatchHTTPPathModifier,
					Value: "/new-root",
				},
			},
			match:        nil, // simulates omitted match
			initialHost:  "example.com",
			expectedHost: "example.com",
			initialPath:  "/some/path",
			expectedPath: "/new-root/some/path",
		},
		{
			name: "prefix path rewrite with encoded path",
			rewrite: state.InternalRewrite{
				Path: &state.InternalPathRewrite{
					Type:  gatewayv1.PrefixMatchHTTPPathModifier,
					Value: "/new-prefix",
				},
			},
			match: &state.InternalMatch{
				Path: &state.InternalPathMatch{
					Type:  gatewayv1.PathMatchPathPrefix,
					Value: "/old-prefix",
				},
			},
			initialHost:     "example.com",
			expectedHost:    "example.com",
			initialPath:     "/old-prefix/some path",
			initialRawPath:  "/old-prefix/some%20path",
			expectedPath:    "/new-prefix/some path",
			expectedRawPath: "",
		},
	}

	p := NewProxy()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			targetPath := tt.initialPath
			if tt.initialRawPath != "" {
				targetPath = tt.initialRawPath
			}
			req := httptest.NewRequest("GET", "http://"+tt.initialHost+targetPath, nil)
			req.Host = tt.initialHost
			if tt.initialRawPath != "" {
				// double check RawPath is set correctly by httptest
				req.URL.RawPath = tt.initialRawPath
			}

			p.rewrite(req, tt.rewrite, tt.match)

			if req.Host != tt.expectedHost {
				t.Errorf("expected host %s, got %s", tt.expectedHost, req.Host)
			}
			if req.URL.Path != tt.expectedPath {
				t.Errorf("expected path %s, got %s", tt.expectedPath, req.URL.Path)
			}
			if req.URL.RawPath != tt.expectedRawPath {
				t.Errorf("expected RawPath %q, got %q", tt.expectedRawPath, req.URL.RawPath)
			}
		})
	}
}

func TestProxyModifyHeaders(t *testing.T) {
	tests := []struct {
		name           string
		modifier       gatewayv1.HTTPHeaderFilter
		initialHeaders map[string][]string
		expectedHeader map[string][]string
	}{
		{
			name: "add set and remove headers",
			modifier: gatewayv1.HTTPHeaderFilter{
				Set: []gatewayv1.HTTPHeader{
					{Name: "X-Header-Set", Value: "newValue"},
				},
				Add: []gatewayv1.HTTPHeader{
					{Name: "X-Header-Add", Value: "addedValue"},
				},
				Remove: []string{"X-Header-Remove"},
			},
			initialHeaders: map[string][]string{
				"X-Header-Set":    {"oldValue"},
				"X-Header-Add":    {"existingValue"},
				"X-Header-Remove": {"toRemove"},
			},
			expectedHeader: map[string][]string{
				"X-Header-Set": {"newValue"},
				"X-Header-Add": {"existingValue", "addedValue"},
			},
		},
	}

	p := NewProxy()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "http://example.com/foo", nil)
			for k, values := range tt.initialHeaders {
				for _, v := range values {
					req.Header.Add(k, v)
				}
			}

			p.modifyHeaders(req, tt.modifier)

			for k, expectedValues := range tt.expectedHeader {
				actualValues := req.Header[k]
				if len(actualValues) != len(expectedValues) {
					t.Fatalf("expected header %s to have values %v, got %v", k, expectedValues, actualValues)
				}
				for i, ev := range expectedValues {
					if actualValues[i] != ev {
						t.Errorf("expected header %s[%d] to be %q, got %q", k, i, ev, actualValues[i])
					}
				}
			}

			for _, removed := range tt.modifier.Remove {
				if len(req.Header[removed]) > 0 {
					t.Errorf("expected header %s to be removed, but still has values %v", removed, req.Header[removed])
				}
			}
		})
	}
}

func TestProxyRedirect(t *testing.T) {
	tests := []struct {
		name             string
		redirect         state.InternalRedirect
		match            *state.InternalMatch
		initialURL       string
		initialHost      string
		expectedStatus   int
		expectedLocation string
	}{
		{
			name: "307 redirect status code default path and host",
			redirect: state.InternalRedirect{
				StatusCode: state.Ptr(307),
			},
			initialURL:       "http://example.com/temporary",
			initialHost:      "example.com",
			expectedStatus:   307,
			expectedLocation: "http://example.com/temporary",
		},
		{
			name: "307 redirect with full path",
			redirect: state.InternalRedirect{
				StatusCode: state.Ptr(307),
				Path: &state.InternalPathRedirect{
					Type:  gatewayv1.FullPathHTTPPathModifier,
					Value: "/new-path",
				},
			},
			initialURL:       "http://example.com/old-path",
			initialHost:      "example.com",
			expectedStatus:   307,
			expectedLocation: "http://example.com/new-path",
		},
		{
			name: "307 redirect with scheme, hostname and port",
			redirect: state.InternalRedirect{
				Scheme:     state.Ptr("https"),
				Hostname:   state.Ptr(gatewayv1.PreciseHostname("foo.example.com")),
				Port:       state.Ptr(gatewayv1.PortNumber(8443)),
				StatusCode: state.Ptr(307),
			},
			initialURL:       "http://example.com/temporary",
			initialHost:      "example.com",
			expectedStatus:   307,
			expectedLocation: "https://foo.example.com:8443/temporary",
		},
	}

	p := NewProxy()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.initialURL, nil)
			req.Host = tt.initialHost
			w := httptest.NewRecorder()

			p.redirect(w, req, tt.redirect, tt.match)

			resp := w.Result()
			if resp.StatusCode != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, resp.StatusCode)
			}
			location := resp.Header.Get("Location")
			if location != tt.expectedLocation {
				t.Errorf("expected Location %q, got %q", tt.expectedLocation, location)
			}
		})
	}
}
