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

package manager

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

var (
	// ErrNoHostnames is returned when an empty hostname slice is provided.
	ErrNoHostnames = errors.New("no hostnames provided")

	// ErrWildcardNotSupported indicates wildcard hostnames are disallowed for HTTP-01.
	ErrWildcardNotSupported = errors.New("wildcard hostnames are not supported for HTTP-01 issuance")

	// ErrTooManyHostnames is returned when hostnames count exceeds the configured limit.
	ErrTooManyHostnames = errors.New("too many hostnames in set")

	// ErrHostnameTooLong is returned when an individual hostname exceeds the maximum length.
	ErrHostnameTooLong = errors.New("hostname exceeds maximum allowed length")

	// ErrInvalidHostname is returned when a hostname is empty or malformed.
	ErrInvalidHostname = errors.New("invalid hostname format")
)

// NormalizeHostnames lower-cases, validates, deduplicates, and sorts a list of hostnames.
// Returns the normalized slice and a deterministic comma-separated key.
func NormalizeHostnames(hostnames []string, maxHostnames, maxLen int) ([]string, string, error) {
	if len(hostnames) == 0 {
		return nil, "", ErrNoHostnames
	}

	if maxHostnames > 0 && len(hostnames) > maxHostnames {
		return nil, "", fmt.Errorf("%w: %d exceeds maximum of %d", ErrTooManyHostnames, len(hostnames), maxHostnames)
	}

	seen := make(map[string]struct{}, len(hostnames))
	normalized := make([]string, 0, len(hostnames))

	for _, raw := range hostnames {
		h := strings.TrimSpace(raw)
		if h == "" {
			return nil, "", fmt.Errorf("%w: empty hostname in list", ErrInvalidHostname)
		}

		if strings.Contains(h, "*") {
			return nil, "", fmt.Errorf("%w: %q", ErrWildcardNotSupported, h)
		}

		if maxLen > 0 && len(h) > maxLen {
			return nil, "", fmt.Errorf("%w: %q has length %d (max %d)", ErrHostnameTooLong, h, len(h), maxLen)
		}

		// Disallow leading or trailing dots, and consecutive dots
		if strings.HasPrefix(h, ".") || strings.HasSuffix(h, ".") || strings.Contains(h, "..") {
			return nil, "", fmt.Errorf("%w: %q contains invalid dots", ErrInvalidHostname, h)
		}

		lower := strings.ToLower(h)
		if _, exists := seen[lower]; !exists {
			seen[lower] = struct{}{}
			normalized = append(normalized, lower)
		}
	}

	if len(normalized) == 0 {
		return nil, "", ErrNoHostnames
	}

	sort.Strings(normalized)
	key := strings.Join(normalized, ",")
	return normalized, key, nil
}
