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
	"strings"

	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// IsHTTPRoute returns true if the group and kind represent an HTTPRoute in Gateway API.
func IsHTTPRoute(group *gatewayv1.Group, kind gatewayv1.Kind) bool {
	g := ValueOf(group)
	return (g == "" || g == gatewayv1.GroupName) && kind == "HTTPRoute"
}

// IntersectHostnames calculates the intersection of route hostnames and a listener hostname.
func IntersectHostnames(routeHostnames []string, listenerHostname string) []string {
	if listenerHostname == "" || listenerHostname == "*" {
		if len(routeHostnames) == 0 {
			return []string{"*"}
		}
		return routeHostnames
	}

	if len(routeHostnames) == 0 {
		return []string{listenerHostname}
	}

	var result []string
	for _, rh := range routeHostnames {
		if intersection, ok := Intersect(rh, listenerHostname); ok {
			result = append(result, intersection)
		}
	}
	return result
}

// Intersect calculates the intersection of two hostnames, including wildcard support.
func Intersect(h1, h2 string) (string, bool) {
	if h1 == "*" || h1 == "" {
		return h2, true
	}
	if h2 == "*" || h2 == "" {
		return h1, true
	}

	if h1 == h2 {
		return h1, true
	}

	// Wildcard matching
	h1Wild := strings.HasPrefix(h1, "*.")
	h2Wild := strings.HasPrefix(h2, "*.")

	if h1Wild && !h2Wild {
		if strings.HasSuffix(h2, h1[1:]) {
			return h2, true
		}
		return "", false
	}

	if !h1Wild && h2Wild {
		if strings.HasSuffix(h1, h2[1:]) {
			return h1, true
		}
		return "", false
	}

	if h1Wild && h2Wild {
		if strings.HasSuffix(h1, h2[1:]) {
			return h1, true
		}
		if strings.HasSuffix(h2, h1[1:]) {
			return h2, true
		}
		return "", false
	}

	return "", false
}

// ValueOf returns the value of the pointer if it is not nil, otherwise the zero value of the type.
func ValueOf[T any](t *T) T {
	if t == nil {
		var zero T
		return zero
	}
	return *t
}

// Ptr returns a pointer to the given value.
func Ptr[T any](t T) *T {
	return &t
}
