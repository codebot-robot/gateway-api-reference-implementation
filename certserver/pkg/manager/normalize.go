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
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/api/client"
)

var (
	// ErrNoHostnames is returned when an empty hostname slice is provided.
	ErrNoHostnames = client.ErrNoHostnames

	// ErrWildcardNotSupported indicates wildcard hostnames are disallowed for HTTP-01.
	ErrWildcardNotSupported = client.ErrWildcardNotSupported

	// ErrTooManyHostnames is returned when hostnames count exceeds the configured limit.
	ErrTooManyHostnames = client.ErrTooManyHostnames

	// ErrHostnameTooLong is returned when an individual hostname exceeds the maximum length.
	ErrHostnameTooLong = client.ErrHostnameTooLong

	// ErrInvalidHostname is returned when a hostname is empty or malformed.
	ErrInvalidHostname = client.ErrInvalidHostname
)

// NormalizeHostnames lower-cases, validates, deduplicates, and sorts a list of hostnames.
// Returns the normalized slice and a deterministic comma-separated key.
func NormalizeHostnames(hostnames []string, maxHostnames, maxLen int) ([]string, string, error) {
	return client.NormalizeHostnames(hostnames, maxHostnames, maxLen)
}
