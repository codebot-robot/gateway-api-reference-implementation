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

package validation

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
)

func TestValidateSingleLabelLocalHostname(t *testing.T) {
	tests := []struct {
		name      string
		hostname  string
		expectErr error
	}{
		{
			name:      "grafana.local accepted",
			hostname:  "grafana.local",
			expectErr: nil,
		},
		{
			name:      "uppercase GRAFANA.LOCAL accepted",
			hostname:  "GRAFANA.LOCAL",
			expectErr: nil,
		},
		{
			name:      "app-123.local accepted",
			hostname:  "app-123.local",
			expectErr: nil,
		},
		{
			name:      "single char label a.local accepted",
			hostname:  "a.local",
			expectErr: nil,
		},
		{
			name:      "max 63 char label accepted",
			hostname:  strings.Repeat("a", 63) + ".local",
			expectErr: nil,
		},
		{
			name:      "a.b.local rejected",
			hostname:  "a.b.local",
			expectErr: ErrMultiLabelLocal,
		},
		{
			name:      "example.com rejected",
			hostname:  "example.com",
			expectErr: ErrNotLocalDomain,
		},
		{
			name:      "*.local rejected",
			hostname:  "*.local",
			expectErr: ErrWildcardHostname,
		},
		{
			name:      "empty hostname rejected",
			hostname:  "",
			expectErr: ErrEmptyHostname,
		},
		{
			name:      "whitespace only rejected",
			hostname:  "   ",
			expectErr: ErrEmptyHostname,
		},
		{
			name:      ".local without label rejected",
			hostname:  ".local",
			expectErr: ErrEmptyHostname,
		},
		{
			name:      "invalid label starting with hyphen",
			hostname:  "-bad.local",
			expectErr: ErrInvalidDNSLabel,
		},
		{
			name:      "invalid label ending with hyphen",
			hostname:  "bad-.local",
			expectErr: ErrInvalidDNSLabel,
		},
		{
			name:      "invalid label with underscore",
			hostname:  "bad_label.local",
			expectErr: ErrInvalidDNSLabel,
		},
		{
			name:      "label longer than 63 characters",
			hostname:  strings.Repeat("a", 64) + ".local",
			expectErr: ErrInvalidDNSLabel,
		},
		{
			name:      "double dot in label",
			hostname:  "foo..local",
			expectErr: ErrMultiLabelLocal,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateSingleLabelLocalHostname(tc.hostname)
			if tc.expectErr == nil {
				if err != nil {
					t.Fatalf("expected valid hostname for %q, got error: %v", tc.hostname, err)
				}
			} else {
				if err == nil {
					t.Fatalf("expected error for hostname %q, got nil", tc.hostname)
				}
				if tc.expectErr != err {
					t.Errorf("expected error %v for hostname %q, got %v", tc.expectErr, tc.hostname, err)
				}
			}
		})
	}
}

func TestValidateListener(t *testing.T) {
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-gw",
			Generation: 3,
		},
	}

	owner := state.ListenerOwner{
		Kind: "Gateway",
		Name: "test-gw",
	}

	t.Run("grafana.local accepted", func(t *testing.T) {
		h := gatewayv1.Hostname("grafana.local")
		spec := state.ListenerSpec{
			Name:     "http",
			Port:     80,
			Protocol: gatewayv1.HTTPProtocolType,
			Hostname: &h,
		}

		conds := ValidateListener(spec, owner, gw)
		if len(conds) != 0 {
			t.Fatalf("expected 0 rejection conditions for grafana.local, got %+v", conds)
		}
	})

	t.Run("HTTPS listener rejected as not supported", func(t *testing.T) {
		h := gatewayv1.Hostname("grafana.local")
		spec := state.ListenerSpec{
			Name:     "https",
			Port:     443,
			Protocol: gatewayv1.HTTPSProtocolType,
			Hostname: &h,
		}

		conds := ValidateListener(spec, owner, gw)
		acc := state.FindCondition(conds, string(gatewayv1.ListenerConditionAccepted))
		if acc == nil {
			t.Fatalf("expected Accepted condition")
		}
		if acc.Status != metav1.ConditionFalse {
			t.Errorf("expected Accepted=False, got %s", acc.Status)
		}
		if acc.Reason != string(gatewayv1.ListenerReasonUnsupportedProtocol) {
			t.Errorf("expected Reason=UnsupportedProtocol, got %s", acc.Reason)
		}
		if acc.ObservedGeneration != 3 {
			t.Errorf("expected ObservedGeneration=3, got %d", acc.ObservedGeneration)
		}

		prog := state.FindCondition(conds, string(gatewayv1.ListenerConditionProgrammed))
		if prog == nil {
			t.Fatalf("expected Programmed condition")
		}
		if prog.Status != metav1.ConditionFalse {
			t.Errorf("expected Programmed=False, got %s", prog.Status)
		}
		if prog.Reason != string(gatewayv1.ListenerReasonInvalid) {
			t.Errorf("expected Reason=Invalid, got %s", prog.Reason)
		}
	})

	t.Run("TLS listener rejected as not supported", func(t *testing.T) {
		h := gatewayv1.Hostname("grafana.local")
		spec := state.ListenerSpec{
			Name:     "tls",
			Port:     443,
			Protocol: gatewayv1.TLSProtocolType,
			Hostname: &h,
		}

		conds := ValidateListener(spec, owner, gw)
		acc := state.FindCondition(conds, string(gatewayv1.ListenerConditionAccepted))
		if acc == nil || acc.Status != metav1.ConditionFalse || acc.Reason != string(gatewayv1.ListenerReasonUnsupportedProtocol) {
			t.Errorf("expected Accepted=False/UnsupportedProtocol for TLS listener, got %+v", acc)
		}
	})

	rejectionTestCases := []struct {
		name     string
		hostname *gatewayv1.Hostname
	}{
		{"a.b.local rejected", ptrHostname("a.b.local")},
		{"example.com rejected", ptrHostname("example.com")},
		{"*.local rejected", ptrHostname("*.local")},
		{"empty hostname rejected", ptrHostname("")},
		{"nil hostname rejected", nil},
		{"invalid label starting with hyphen rejected", ptrHostname("-bad.local")},
		{"invalid label with underscore rejected", ptrHostname("bad_label.local")},
	}

	for _, tc := range rejectionTestCases {
		t.Run(tc.name, func(t *testing.T) {
			spec := state.ListenerSpec{
				Name:     "http",
				Port:     80,
				Protocol: gatewayv1.HTTPProtocolType,
				Hostname: tc.hostname,
			}

			conds := ValidateListener(spec, owner, gw)
			acc := state.FindCondition(conds, string(gatewayv1.ListenerConditionAccepted))
			if acc == nil {
				t.Fatalf("expected Accepted condition for %s", tc.name)
			}
			if acc.Status != metav1.ConditionFalse {
				t.Errorf("expected Accepted=False for %s, got %s", tc.name, acc.Status)
			}
			if acc.Reason != string(gatewayv1.ListenerReasonUnsupportedValue) {
				t.Errorf("expected Reason=UnsupportedValue for %s, got %s", tc.name, acc.Reason)
			}

			prog := state.FindCondition(conds, string(gatewayv1.ListenerConditionProgrammed))
			if prog == nil {
				t.Fatalf("expected Programmed condition for %s", tc.name)
			}
			if prog.Status != metav1.ConditionFalse {
				t.Errorf("expected Programmed=False for %s, got %s", tc.name, prog.Status)
			}
			if prog.Reason != string(gatewayv1.ListenerReasonInvalid) {
				t.Errorf("expected Reason=Invalid for %s, got %s", tc.name, prog.Reason)
			}
		})
	}
}

func ptrHostname(s string) *gatewayv1.Hostname {
	h := gatewayv1.Hostname(s)
	return &h
}
