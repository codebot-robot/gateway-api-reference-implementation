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
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
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
			name: "prefix path rewrite strip prefix with subpath",
			rewrite: state.InternalRewrite{
				Path: &state.InternalPathRewrite{
					Type:  gatewayv1.PrefixMatchHTTPPathModifier,
					Value: "/",
				},
			},
			match: &state.InternalMatch{
				Path: &state.InternalPathMatch{
					Type:  gatewayv1.PathMatchPathPrefix,
					Value: "/strip-prefix",
				},
			},
			initialHost:  "example.com",
			expectedHost: "example.com",
			initialPath:  "/strip-prefix/three",
			expectedPath: "/three",
		},
		{
			name: "prefix path rewrite strip prefix exact match",
			rewrite: state.InternalRewrite{
				Path: &state.InternalPathRewrite{
					Type:  gatewayv1.PrefixMatchHTTPPathModifier,
					Value: "/",
				},
			},
			match: &state.InternalMatch{
				Path: &state.InternalPathMatch{
					Type:  gatewayv1.PathMatchPathPrefix,
					Value: "/strip-prefix",
				},
			},
			initialHost:  "example.com",
			expectedHost: "example.com",
			initialPath:  "/strip-prefix",
			expectedPath: "/",
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
			name: "303 redirect status code default path and host",
			redirect: state.InternalRedirect{
				StatusCode: state.Ptr(303),
			},
			initialURL:       "http://example.com/see-other",
			initialHost:      "example.com",
			expectedStatus:   303,
			expectedLocation: "http://example.com/see-other",
		},
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
			name: "308 redirect status code default path and host",
			redirect: state.InternalRedirect{
				StatusCode: state.Ptr(308),
			},
			initialURL:       "http://example.com/permanent",
			initialHost:      "example.com",
			expectedStatus:   308,
			expectedLocation: "http://example.com/permanent",
		},
		{
			name: "302 default status code with hostname redirect",
			redirect: state.InternalRedirect{
				Hostname: state.Ptr(gatewayv1.PreciseHostname("example.org")),
			},
			initialURL:       "http://example.com/hostname-redirect",
			initialHost:      "example.com",
			expectedStatus:   302,
			expectedLocation: "http://example.org/hostname-redirect",
		},
		{
			name: "301 redirect with host and status code",
			redirect: state.InternalRedirect{
				Hostname:   state.Ptr(gatewayv1.PreciseHostname("example.org")),
				StatusCode: state.Ptr(301),
			},
			initialURL:       "http://example.com/host-and-status",
			initialHost:      "example.com",
			expectedStatus:   301,
			expectedLocation: "http://example.org/host-and-status",
		},
		{
			name: "redirect with full path",
			redirect: state.InternalRedirect{
				StatusCode: state.Ptr(302),
				Path: &state.InternalPathRedirect{
					Type:  gatewayv1.FullPathHTTPPathModifier,
					Value: "/full-path-replacement",
				},
			},
			initialURL:       "http://example.com/full/path/original",
			initialHost:      "example.com",
			expectedStatus:   302,
			expectedLocation: "http://example.com/full-path-replacement",
		},
		{
			name: "redirect with prefix path replacement",
			redirect: state.InternalRedirect{
				StatusCode: state.Ptr(302),
				Path: &state.InternalPathRedirect{
					Type:  gatewayv1.PrefixMatchHTTPPathModifier,
					Value: "/replacement-prefix",
				},
			},
			match: &state.InternalMatch{
				Path: &state.InternalPathMatch{
					Type:  gatewayv1.PathMatchPathPrefix,
					Value: "/original-prefix",
				},
			},
			initialURL:       "http://example.com/original-prefix/lemon",
			initialHost:      "example.com",
			expectedStatus:   302,
			expectedLocation: "http://example.com/replacement-prefix/lemon",
		},
		{
			name: "redirect with port override",
			redirect: state.InternalRedirect{
				Port: state.Ptr(gatewayv1.PortNumber(8083)),
			},
			initialURL:       "http://example.com/port",
			initialHost:      "example.com",
			expectedStatus:   302,
			expectedLocation: "http://example.com:8083/port",
		},
		{
			name: "redirect with scheme https and port 443 omitted",
			redirect: state.InternalRedirect{
				Scheme:   state.Ptr("https"),
				Hostname: state.Ptr(gatewayv1.PreciseHostname("example.org")),
				Port:     state.Ptr(gatewayv1.PortNumber(443)),
			},
			initialURL:       "http://example.com/scheme",
			initialHost:      "example.com",
			expectedStatus:   302,
			expectedLocation: "https://example.org/scheme",
		},
		{
			name: "redirect with scheme https and port nil omits port even if request had port 8080",
			redirect: state.InternalRedirect{
				Scheme:   state.Ptr("https"),
				Hostname: state.Ptr(gatewayv1.PreciseHostname("example.org")),
			},
			initialURL:       "http://example.com:8080/scheme",
			initialHost:      "example.com:8080",
			expectedStatus:   302,
			expectedLocation: "https://example.org/scheme",
		},
		{
			name: "redirect with scheme nil inherits request port 8080",
			redirect: state.InternalRedirect{
				Hostname: state.Ptr(gatewayv1.PreciseHostname("example.org")),
			},
			initialURL:       "http://example.com:8080/scheme-nil-and-port-nil",
			initialHost:      "example.com:8080",
			expectedStatus:   302,
			expectedLocation: "http://example.org:8080/scheme-nil-and-port-nil",
		},
		{
			name: "redirect with scheme nil and explicit port 80 omits port 80",
			redirect: state.InternalRedirect{
				Hostname: state.Ptr(gatewayv1.PreciseHostname("example.org")),
				Port:     state.Ptr(gatewayv1.PortNumber(80)),
			},
			initialURL:       "http://example.com:8080/scheme-nil-and-port-80",
			initialHost:      "example.com:8080",
			expectedStatus:   302,
			expectedLocation: "http://example.org/scheme-nil-and-port-80",
		},
		{
			name: "redirect with scheme https and custom port 8443",
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

func TestProxyModifyHeadersCaseInsensitive(t *testing.T) {
	modifier := gatewayv1.HTTPHeaderFilter{
		Set: []gatewayv1.HTTPHeader{
			{Name: "X-Header-Set", Value: "header-set"},
		},
		Add: []gatewayv1.HTTPHeader{
			{Name: "X-Header-Add", Value: "header-add"},
			{Name: "X-New-Add", Value: "new-add"},
		},
		Remove: []string{"x-header-remove"},
	}

	header := http.Header{
		"x-header-set":    []string{"original-val-set"},
		"x-header-add":    []string{"original-val-add"},
		"x-header-remove": []string{"original-val-remove"},
		"Another-Header":  []string{"another-header-val"},
	}

	modifyHeaders(header, modifier)

	if got := header.Get("X-Header-Set"); got != "header-set" {
		t.Errorf("expected X-Header-Set to be 'header-set', got %q", got)
	}
	if got := strings.Join(header.Values("X-Header-Add"), ","); got != "original-val-add,header-add" {
		t.Errorf("expected X-Header-Add to be 'original-val-add,header-add', got %q", got)
	}
	if got := header.Get("X-New-Add"); got != "new-add" {
		t.Errorf("expected X-New-Add to be 'new-add', got %q", got)
	}
	if got := header.Get("Another-Header"); got != "another-header-val" {
		t.Errorf("expected Another-Header to be 'another-header-val', got %q", got)
	}
	if got := header.Values("X-Header-Remove"); len(got) > 0 {
		t.Errorf("expected X-Header-Remove to be absent, got %v", got)
	}
	if got := header.Values("x-header-remove"); len(got) > 0 {
		t.Errorf("expected x-header-remove to be absent, got %v", got)
	}
}

func TestProxyResponseHeaderModifier(t *testing.T) {
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Backend-Original", "orig")
		w.Header().Set("X-Header-Remove", "remove-me")
		w.Header().Set("X-Header-Set", "old-set")
		w.Header().Add("X-Header-Add", "existing-add")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer backendServer.Close()

	u, err := url.Parse(backendServer.URL)
	if err != nil {
		t.Fatalf("failed to parse backend server url: %v", err)
	}
	host := u.Hostname()
	port, _ := strconv.Atoi(u.Port())

	p := NewProxy()
	p.UpdateRoutes([]state.InternalRoute{
		{
			Rules: []state.InternalRule{
				{
					Backends: []state.InternalBackend{
						{
							Host:   host,
							Port:   int32(port),
							Weight: 1,
						},
					},
					ResponseHeaderModifier: &gatewayv1.HTTPHeaderFilter{
						Set: []gatewayv1.HTTPHeader{
							{Name: "X-Header-Set", Value: "new-set"},
						},
						Add: []gatewayv1.HTTPHeader{
							{Name: "X-Header-Add", Value: "new-add"},
						},
						Remove: []string{"X-Header-Remove"},
					},
				},
			},
		},
	})

	req := httptest.NewRequest("GET", "http://example.com/test", nil)
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Header-Set"); got != "new-set" {
		t.Errorf("expected X-Header-Set to be 'new-set', got %q", got)
	}
	if got := strings.Join(resp.Header.Values("X-Header-Add"), ","); got != "existing-add,new-add" {
		t.Errorf("expected X-Header-Add to be 'existing-add,new-add', got %q", got)
	}
	if got := resp.Header.Get("X-Backend-Original"); got != "orig" {
		t.Errorf("expected X-Backend-Original to be 'orig', got %q", got)
	}
	if got := resp.Header.Values("X-Header-Remove"); len(got) > 0 {
		t.Errorf("expected X-Header-Remove to be absent, got %v", got)
	}
}

func TestProxyBackendHeaderModifiers(t *testing.T) {
	var receivedHeaders http.Header
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = r.Header.Clone()
		w.Header().Set("X-Resp-Original", "resp-orig")
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	u, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatalf("failed to parse backend URL: %v", err)
	}
	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatalf("failed to split host and port: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("failed to parse port: %v", err)
	}

	p := NewProxy()
	p.UpdateRoutes([]state.InternalRoute{
		{
			Rules: []state.InternalRule{
				{
					RequestHeaderModifier: &gatewayv1.HTTPHeaderFilter{
						Set: []gatewayv1.HTTPHeader{
							{Name: "X-Rule-Req", Value: "rule-val"},
						},
					},
					ResponseHeaderModifier: &gatewayv1.HTTPHeaderFilter{
						Set: []gatewayv1.HTTPHeader{
							{Name: "X-Rule-Resp", Value: "rule-resp-val"},
						},
					},
					Backends: []state.InternalBackend{
						{
							Host:   host,
							Port:   int32(port),
							Weight: 1,
							RequestHeaderModifier: &gatewayv1.HTTPHeaderFilter{
								Set: []gatewayv1.HTTPHeader{
									{Name: "X-Backend-Req", Value: "backend-val"},
								},
							},
							ResponseHeaderModifier: &gatewayv1.HTTPHeaderFilter{
								Set: []gatewayv1.HTTPHeader{
									{Name: "X-Backend-Resp", Value: "backend-resp-val"},
								},
							},
						},
					},
				},
			},
		},
	})

	req := httptest.NewRequest("GET", "http://example.com/test", nil)
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	// Verify request headers received by backend
	if got := receivedHeaders.Get("X-Rule-Req"); got != "rule-val" {
		t.Errorf("expected X-Rule-Req to be 'rule-val', got %q", got)
	}
	if got := receivedHeaders.Get("X-Backend-Req"); got != "backend-val" {
		t.Errorf("expected X-Backend-Req to be 'backend-val', got %q", got)
	}

	// Verify response headers returned to client
	if got := resp.Header.Get("X-Rule-Resp"); got != "rule-resp-val" {
		t.Errorf("expected X-Rule-Resp to be 'rule-resp-val', got %q", got)
	}
	if got := resp.Header.Get("X-Backend-Resp"); got != "backend-resp-val" {
		t.Errorf("expected X-Backend-Resp to be 'backend-resp-val', got %q", got)
	}
	if got := resp.Header.Get("X-Resp-Original"); got != "resp-orig" {
		t.Errorf("expected X-Resp-Original to be 'resp-orig', got %q", got)
	}
}

func TestPickBackend(t *testing.T) {
	t.Run("empty backends", func(t *testing.T) {
		_, err := pickBackend(nil)
		if err == nil {
			t.Errorf("expected error for empty backends, got nil")
		}
	})

	t.Run("all zero weight", func(t *testing.T) {
		backends := []state.InternalBackend{
			{Host: "b1", Weight: 0},
			{Host: "b2", Weight: 0},
		}
		_, err := pickBackend(backends)
		if err == nil {
			t.Errorf("expected error for all zero weight, got nil")
		}
	})

	t.Run("single backend zero weight", func(t *testing.T) {
		backends := []state.InternalBackend{
			{Host: "b1", Weight: 0},
		}
		_, err := pickBackend(backends)
		if err == nil {
			t.Errorf("expected error for single backend with zero weight, got nil")
		}
	})

	t.Run("single backend", func(t *testing.T) {
		backends := []state.InternalBackend{
			{Host: "b1", Weight: 1},
		}
		b, err := pickBackend(backends)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if b.Host != "b1" {
			t.Errorf("expected host b1, got %s", b.Host)
		}
	})
}

func TestProxy_WeightedBackends(t *testing.T) {
	var counts sync.Map
	counts.Store("v1", 0)
	counts.Store("v2", 0)
	counts.Store("v3", 0)

	s1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, _ := counts.Load("v1")
		counts.Store("v1", v.(int)+1)
		w.WriteHeader(http.StatusOK)
	}))
	defer s1.Close()

	s2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, _ := counts.Load("v2")
		counts.Store("v2", v.(int)+1)
		w.WriteHeader(http.StatusOK)
	}))
	defer s2.Close()

	s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, _ := counts.Load("v3")
		counts.Store("v3", v.(int)+1)
		w.WriteHeader(http.StatusOK)
	}))
	defer s3.Close()

	u1, _ := url.Parse(s1.URL)
	port1, _ := strconv.Atoi(u1.Port())
	u2, _ := url.Parse(s2.URL)
	port2, _ := strconv.Atoi(u2.Port())
	u3, _ := url.Parse(s3.URL)
	port3, _ := strconv.Atoi(u3.Port())

	p := NewProxy()
	p.UpdateRoutes([]state.InternalRoute{
		{
			Rules: []state.InternalRule{
				{
					Backends: []state.InternalBackend{
						{Host: u1.Hostname(), Port: int32(port1), Weight: 70},
						{Host: u2.Hostname(), Port: int32(port2), Weight: 30},
						{Host: u3.Hostname(), Port: int32(port3), Weight: 0},
					},
				},
			},
		},
	})

	totalRequests := 1000
	for range totalRequests {
		req := httptest.NewRequest("GET", "http://example.com/", nil)
		w := httptest.NewRecorder()
		p.ServeHTTP(w, req)
		if w.Result().StatusCode != http.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Result().StatusCode)
		}
	}

	v1Count, _ := counts.Load("v1")
	v2Count, _ := counts.Load("v2")
	v3Count, _ := counts.Load("v3")

	if v3Count.(int) != 0 {
		t.Errorf("expected 0 requests to backend with weight 0, got %d", v3Count.(int))
	}

	v1Pct := float64(v1Count.(int)) / float64(totalRequests)
	v2Pct := float64(v2Count.(int)) / float64(totalRequests)

	// Tolerance of 5% around 0.70 and 0.30
	if v1Pct < 0.60 || v1Pct > 0.80 {
		t.Errorf("expected v1 percentage ~0.70 (+/- 0.10), got %f", v1Pct)
	}
	if v2Pct < 0.20 || v2Pct > 0.40 {
		t.Errorf("expected v2 percentage ~0.30 (+/- 0.10), got %f", v2Pct)
	}
}

func TestProxy_BackendRequestHeaderModifier(t *testing.T) {
	s1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Backend"); got != "infra-backend-v1" {
			t.Errorf("expected Backend header 'infra-backend-v1', got %q", got)
		}
		if got := r.Header.Get("X-Rule-Header"); got != "rule-val" {
			t.Errorf("expected X-Rule-Header 'rule-val', got %q", got)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer s1.Close()

	u1, _ := url.Parse(s1.URL)
	port1, _ := strconv.Atoi(u1.Port())

	p := NewProxy()
	p.UpdateRoutes([]state.InternalRoute{
		{
			Rules: []state.InternalRule{
				{
					RequestHeaderModifier: &gatewayv1.HTTPHeaderFilter{
						Set: []gatewayv1.HTTPHeader{
							{Name: "X-Rule-Header", Value: "rule-val"},
						},
					},
					Backends: []state.InternalBackend{
						{
							Host:   u1.Hostname(),
							Port:   int32(port1),
							Weight: 10,
							RequestHeaderModifier: &gatewayv1.HTTPHeaderFilter{
								Set: []gatewayv1.HTTPHeader{
									{Name: "Backend", Value: "infra-backend-v1"},
								},
							},
						},
					},
				},
			},
		},
	})

	req := httptest.NewRequest("GET", "http://example.com/", nil)
	w := httptest.NewRecorder()
	p.ServeHTTP(w, req)
	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Result().StatusCode)
	}
}

func TestProxy_BackendResponseHeaderModifier(t *testing.T) {
	s1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer s1.Close()

	u1, _ := url.Parse(s1.URL)
	port1, _ := strconv.Atoi(u1.Port())

	p := NewProxy()
	p.UpdateRoutes([]state.InternalRoute{
		{
			Rules: []state.InternalRule{
				{
					ResponseHeaderModifier: &gatewayv1.HTTPHeaderFilter{
						Set: []gatewayv1.HTTPHeader{
							{Name: "X-Rule-Resp", Value: "rule-resp"},
						},
					},
					Backends: []state.InternalBackend{
						{
							Host:   u1.Hostname(),
							Port:   int32(port1),
							Weight: 1,
							ResponseHeaderModifier: &gatewayv1.HTTPHeaderFilter{
								Set: []gatewayv1.HTTPHeader{
									{Name: "X-Backend-Resp", Value: "backend-resp"},
								},
							},
						},
					},
				},
			},
		},
	})

	req := httptest.NewRequest("GET", "http://example.com/", nil)
	w := httptest.NewRecorder()
	p.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Rule-Resp"); got != "rule-resp" {
		t.Errorf("expected X-Rule-Resp 'rule-resp', got %q", got)
	}
	if got := resp.Header.Get("X-Backend-Resp"); got != "backend-resp" {
		t.Errorf("expected X-Backend-Resp 'backend-resp', got %q", got)
	}
}
