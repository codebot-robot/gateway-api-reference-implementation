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

package state

import (
	"net/http"
	"testing"

	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestMatchRouteOrder(t *testing.T) {
	tests := []struct {
		name         string
		routes       []InternalRoute
		path         string
		expectedRule *InternalRule
	}{
		{
			name: "Exact vs Prefix",
			routes: []InternalRoute{
				{
					Rules: []InternalRule{
						{
							Matches: []InternalMatch{
								{
									Path: &InternalPathMatch{
										Type:  gatewayv1.PathMatchPathPrefix,
										Value: "/match",
									},
								},
							},
							Backends: []InternalBackend{{Host: "backend-prefix"}},
						},
						{
							Matches: []InternalMatch{
								{
									Path: &InternalPathMatch{
										Type:  gatewayv1.PathMatchExact,
										Value: "/match/exact",
									},
								},
							},
							Backends: []InternalBackend{{Host: "backend-exact"}},
						},
					},
				},
			},
			path: "/match/exact",
			expectedRule: &InternalRule{
				Backends: []InternalBackend{{Host: "backend-exact"}},
			},
		},
		{
			name: "Longest Prefix wins",
			routes: []InternalRoute{
				{
					Rules: []InternalRule{
						{
							Matches: []InternalMatch{
								{
									Path: &InternalPathMatch{
										Type:  gatewayv1.PathMatchPathPrefix,
										Value: "/match/prefix",
									},
								},
							},
							Backends: []InternalBackend{{Host: "backend-short"}},
						},
						{
							Matches: []InternalMatch{
								{
									Path: &InternalPathMatch{
										Type:  gatewayv1.PathMatchPathPrefix,
										Value: "/match/prefix/one",
									},
								},
							},
							Backends: []InternalBackend{{Host: "backend-long"}},
						},
					},
				},
			},
			path: "/match/prefix/one/any",
			expectedRule: &InternalRule{
				Backends: []InternalBackend{{Host: "backend-long"}},
			},
		},
		{
			name: "First rule wins on tie",
			routes: []InternalRoute{
				{
					Rules: []InternalRule{
						{
							Matches: []InternalMatch{
								{
									Path: &InternalPathMatch{
										Type:  gatewayv1.PathMatchPathPrefix,
										Value: "/match",
									},
								},
							},
							Backends: []InternalBackend{{Host: "backend-1"}},
						},
						{
							Matches: []InternalMatch{
								{
									Path: &InternalPathMatch{
										Type:  gatewayv1.PathMatchPathPrefix,
										Value: "/match",
									},
								},
							},
							Backends: []InternalBackend{{Host: "backend-2"}},
						},
					},
				},
			},
			path: "/match",
			expectedRule: &InternalRule{
				Backends: []InternalBackend{{Host: "backend-1"}},
			},
		},
		{
			name: "First route wins on tie across routes",
			routes: []InternalRoute{
				{
					Rules: []InternalRule{
						{
							Matches: []InternalMatch{
								{
									Path: &InternalPathMatch{
										Type:  gatewayv1.PathMatchPathPrefix,
										Value: "/match",
									},
								},
							},
							Backends: []InternalBackend{{Host: "backend-A"}},
						},
					},
				},
				{
					Rules: []InternalRule{
						{
							Matches: []InternalMatch{
								{
									Path: &InternalPathMatch{
										Type:  gatewayv1.PathMatchPathPrefix,
										Value: "/match",
									},
								},
							},
							Backends: []InternalBackend{{Host: "backend-B"}},
						},
					},
				},
			},
			path: "/match",
			expectedRule: &InternalRule{
				Backends: []InternalBackend{{Host: "backend-A"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _ := http.NewRequest("GET", tt.path, nil)
			bestRule, _ := MatchRoute(tt.routes, r)

			if bestRule == nil {
				t.Fatalf("Expected a match, but got nil")
			}

			if len(bestRule.Backends) == 0 || len(tt.expectedRule.Backends) == 0 || bestRule.Backends[0].Host != tt.expectedRule.Backends[0].Host {
				t.Errorf("Expected backend %v, but got %v", tt.expectedRule.Backends, bestRule.Backends)
			}
		})
	}
}

func TestMatchRoute_MethodVsHeaderPrecedence(t *testing.T) {
	routes := []InternalRoute{
		{
			Hostnames: []string{"example.com"},
			Rules: []InternalRule{
				{
					Matches: []InternalMatch{
						{
							Method: Ptr(gatewayv1.HTTPMethod("PATCH")),
						},
					},
					Backends: []InternalBackend{{Host: "method-backend"}},
				},
				{
					Matches: []InternalMatch{
						{
							Headers: []InternalHeaderMatch{
								{
									Name:            "version",
									MatchExactValue: "four",
								},
							},
						},
					},
					Backends: []InternalBackend{{Host: "header-backend"}},
				},
			},
		},
	}

	req, _ := http.NewRequest("PATCH", "http://example.com/", nil)
	req.Header.Set("version", "four")

	rule, _ := MatchRoute(routes, req)
	if rule == nil {
		t.Fatalf("Expected match, got nil")
	}
	if len(rule.Backends) == 0 || rule.Backends[0].Host != "method-backend" {
		t.Errorf("Expected method match (method-backend) to take precedence over header match (header-backend), got %v", rule.Backends)
	}
}

func TestMatchRoute_HeaderMatching(t *testing.T) {
	routes := []InternalRoute{
		{
			Hostnames: []string{"example.com"},
			Rules: []InternalRule{
				{
					Matches: []InternalMatch{
						{
							Headers: []InternalHeaderMatch{
								{
									Name:            "X-Header-One",
									MatchExactValue: "val1",
								},
								{
									Name:            "X-Header-Two",
									MatchExactValue: "val2",
								},
							},
						},
					},
					Backends: []InternalBackend{{Host: "two-headers-backend"}},
				},
				{
					Matches: []InternalMatch{
						{
							Headers: []InternalHeaderMatch{
								{
									Name:            "X-Header-One",
									MatchExactValue: "val1",
								},
							},
						},
					},
					Backends: []InternalBackend{{Host: "one-header-backend"}},
				},
			},
		},
	}

	// 1. Single header match
	req1, _ := http.NewRequest("GET", "http://example.com/", nil)
	req1.Header.Set("X-Header-One", "val1")
	rule1, _ := MatchRoute(routes, req1)
	if rule1 == nil || len(rule1.Backends) == 0 || rule1.Backends[0].Host != "one-header-backend" {
		t.Errorf("Expected single header match, got %v", rule1)
	}

	// 2. Multiple headers match - more headers win
	req2, _ := http.NewRequest("GET", "http://example.com/", nil)
	req2.Header.Set("x-header-one", "val1") // test case insensitivity
	req2.Header.Set("X-Header-Two", "val2")
	rule2, _ := MatchRoute(routes, req2)
	if rule2 == nil || len(rule2.Backends) == 0 || rule2.Backends[0].Host != "two-headers-backend" {
		t.Errorf("Expected two headers match (more headers win), got %v", rule2)
	}
}

func TestMatchRoute_HostnamePrecedence(t *testing.T) {
	routes := []InternalRoute{
		{
			Hostnames: []string{"*.bar.com"},
			Rules: []InternalRule{
				{
					Backends: []InternalBackend{{Host: "wildcard-bar-backend"}},
				},
			},
		},
		{
			Hostnames: []string{"foo.bar.com"},
			Rules: []InternalRule{
				{
					Backends: []InternalBackend{{Host: "exact-foo-bar-backend"}},
				},
			},
		},
		{
			Hostnames: []string{"*.foo.bar.com"},
			Rules: []InternalRule{
				{
					Backends: []InternalBackend{{Host: "longer-wildcard-backend"}},
				},
			},
		},
		{
			Hostnames: []string{"*"},
			Rules: []InternalRule{
				{
					Backends: []InternalBackend{{Host: "catch-all-backend"}},
				},
			},
		},
	}

	tests := []struct {
		name            string
		host            string
		expectedBackend string
	}{
		{
			name:            "Exact hostname wins over wildcard",
			host:            "foo.bar.com",
			expectedBackend: "exact-foo-bar-backend",
		},
		{
			name:            "Exact hostname wins with port and case insensitivity",
			host:            "FOO.BAR.COM:8080",
			expectedBackend: "exact-foo-bar-backend",
		},
		{
			name:            "Longer wildcard hostname wins over shorter wildcard",
			host:            "test.foo.bar.com",
			expectedBackend: "longer-wildcard-backend",
		},
		{
			name:            "Wildcard hostname matches single prefix",
			host:            "baz.bar.com",
			expectedBackend: "wildcard-bar-backend",
		},
		{
			name:            "Wildcard hostname matches multiple prefix levels",
			host:            "multiple.prefixes.bar.com",
			expectedBackend: "wildcard-bar-backend",
		},
		{
			name:            "Catch-all matches host not matching any wildcard suffix",
			host:            "other.domain.com",
			expectedBackend: "catch-all-backend",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest("GET", "http://"+tt.host+"/", nil)
			rule, _ := MatchRoute(routes, req)
			if rule == nil {
				t.Fatalf("Expected match, got nil")
			}
			if len(rule.Backends) == 0 || rule.Backends[0].Host != tt.expectedBackend {
				t.Errorf("Expected backend %s, got %v", tt.expectedBackend, rule.Backends)
			}
		})
	}
}

func TestMatchRoute_HostnamePrecedenceOverPath(t *testing.T) {
	routes := []InternalRoute{
		{
			Hostnames: []string{"*.bar.com"},
			Rules: []InternalRule{
				{
					Matches: []InternalMatch{
						{
							Path: &InternalPathMatch{
								Type:  gatewayv1.PathMatchExact,
								Value: "/exact-path",
							},
						},
					},
					Backends: []InternalBackend{{Host: "wildcard-host-exact-path-backend"}},
				},
			},
		},
		{
			Hostnames: []string{"foo.bar.com"},
			Rules: []InternalRule{
				{
					Matches: []InternalMatch{
						{
							Path: &InternalPathMatch{
								Type:  gatewayv1.PathMatchPathPrefix,
								Value: "/",
							},
						},
					},
					Backends: []InternalBackend{{Host: "exact-host-prefix-path-backend"}},
				},
			},
		},
	}

	req, _ := http.NewRequest("GET", "http://foo.bar.com/exact-path", nil)
	rule, _ := MatchRoute(routes, req)
	if rule == nil {
		t.Fatalf("Expected match, got nil")
	}
	if len(rule.Backends) == 0 || rule.Backends[0].Host != "exact-host-prefix-path-backend" {
		t.Errorf("Expected exact hostname match (exact-host-prefix-path-backend) to take precedence over wildcard host match with exact path, got %v", rule.Backends)
	}
}
