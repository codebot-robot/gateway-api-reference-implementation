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

package client

import (
	"errors"
	"strings"
	"testing"
)

func TestNormalizeHostnames(t *testing.T) {
	tests := []struct {
		name         string
		hostnames    []string
		maxHostnames int
		maxLen       int
		wantList     []string
		wantKey      string
		wantErr      error
	}{
		{
			name:      "empty list",
			hostnames: nil,
			wantErr:   ErrNoHostnames,
		},
		{
			name:         "too many hostnames",
			hostnames:    []string{"a.com", "b.com"},
			maxHostnames: 1,
			wantErr:      ErrTooManyHostnames,
		},
		{
			name:      "wildcard not supported",
			hostnames: []string{"*.example.com"},
			wantErr:   ErrWildcardNotSupported,
		},
		{
			name:      "hostname too long",
			hostnames: []string{"verylonghostname.example.com"},
			maxLen:    10,
			wantErr:   ErrHostnameTooLong,
		},
		{
			name:      "invalid dot prefix",
			hostnames: []string{".example.com"},
			wantErr:   ErrInvalidHostname,
		},
		{
			name:      "consecutive dots",
			hostnames: []string{"example..com"},
			wantErr:   ErrInvalidHostname,
		},
		{
			name:      "empty string hostname",
			hostnames: []string{"   "},
			wantErr:   ErrInvalidHostname,
		},
		{
			name:         "valid lowercase and deduplicate and sort",
			hostnames:    []string{"B.example.com.", "a.example.COM", "b.example.com"},
			maxHostnames: 10,
			maxLen:       253,
			wantList:     []string{"a.example.com", "b.example.com"},
			wantKey:      "a.example.com,b.example.com",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotList, gotKey, err := NormalizeHostnames(tc.hostnames, tc.maxHostnames, tc.maxLen)
			if tc.wantErr != nil {
				if err == nil {
					t.Fatalf("expected error %v, got nil", tc.wantErr)
				}
				if !errors.Is(err, tc.wantErr) && !strings.Contains(err.Error(), tc.wantErr.Error()) {
					t.Fatalf("expected error %v, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(gotList) != len(tc.wantList) {
				t.Fatalf("gotList len %d, want %d", len(gotList), len(tc.wantList))
			}
			for i := range gotList {
				if gotList[i] != tc.wantList[i] {
					t.Errorf("gotList[%d] = %q, want %q", i, gotList[i], tc.wantList[i])
				}
			}
			if gotKey != tc.wantKey {
				t.Errorf("gotKey = %q, want %q", gotKey, tc.wantKey)
			}
		})
	}
}
