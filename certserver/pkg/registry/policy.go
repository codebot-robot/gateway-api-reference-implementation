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

package registry

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrInvalidPattern is returned when a policy pattern is malformed.
	ErrInvalidPattern = errors.New("invalid hostname pattern: only exact names or one leading '*.' label are allowed")
)

func cleanHostOrPattern(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(s, ".")
	return strings.ToLower(s)
}

// ValidateHostnamePattern verifies that pattern is either an exact name or has exactly one leading "*." label.
func ValidateHostnamePattern(pattern string) error {
	p := cleanHostOrPattern(pattern)
	if p == "" {
		return fmt.Errorf("%w: pattern is empty", ErrInvalidPattern)
	}

	if strings.HasPrefix(p, "*.") {
		rest := p[2:]
		if rest == "" || strings.Contains(rest, "*") || strings.HasPrefix(rest, ".") || strings.Contains(rest, "..") {
			return fmt.Errorf("%w: %q", ErrInvalidPattern, pattern)
		}
		return nil
	}

	if strings.Contains(p, "*") {
		return fmt.Errorf("%w: %q", ErrInvalidPattern, pattern)
	}
	if strings.HasPrefix(p, ".") || strings.Contains(p, "..") {
		return fmt.Errorf("%w: %q contains invalid dots", ErrInvalidPattern, pattern)
	}
	return nil
}

// MatchHostnamePattern checks whether a requested hostname matches an allow or deny pattern.
// - An exact pattern "example.com" matches only "example.com".
// - A wildcard pattern "*.example.com" matches "sub.example.com", but not "example.com" or "a.b.example.com" (single-label wildcard).
func MatchHostnamePattern(pattern, hostname string) bool {
	p := cleanHostOrPattern(pattern)
	h := cleanHostOrPattern(hostname)
	if p == "" || h == "" {
		return false
	}

	if p == h {
		return true
	}

	if strings.HasPrefix(p, "*.") {
		suffix := p[1:] // e.g. ".example.com"
		if strings.HasSuffix(h, suffix) {
			prefix := h[:len(h)-len(suffix)]
			if len(prefix) > 0 && !strings.Contains(prefix, ".") && prefix != "*" {
				return true
			}
		}
	}

	return false
}

// CheckHostnamePolicy checks whether a single hostname is permitted by the cluster's allow and deny lists.
func CheckHostnamePolicy(cluster *ClusterConfig, hostname string) error {
	clean := cleanHostOrPattern(hostname)

	// 1. Check allow list if specified: must match at least one pattern
	if len(cluster.AllowedHostnames) > 0 {
		allowed := false
		for _, pattern := range cluster.AllowedHostnames {
			if MatchHostnamePattern(pattern, clean) {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("hostname %q is outside cluster %q allow list", clean, cluster.ClusterID)
		}
	}

	// 2. Check deny list if specified: must not match any pattern
	if len(cluster.DeniedHostnames) > 0 {
		for _, pattern := range cluster.DeniedHostnames {
			if MatchHostnamePattern(pattern, clean) {
				return fmt.Errorf("hostname %q matches cluster %q deny list pattern %q", clean, cluster.ClusterID, pattern)
			}
		}
	}

	return nil
}

// CheckHostnamesPolicy checks whether all hostnames in the list are permitted by the cluster policy.
func CheckHostnamesPolicy(cluster *ClusterConfig, hostnames []string) error {
	for _, h := range hostnames {
		if err := CheckHostnamePolicy(cluster, h); err != nil {
			return err
		}
	}
	return nil
}
