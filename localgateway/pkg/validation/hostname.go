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
	"errors"
	"fmt"
	"regexp"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
)

// Condition reasons and messages documented for localgateway hostname and protocol rules.
//
// Documented listener conditions:
// 1. Unsupported protocol (localgateway supports HTTP only):
//    - Accepted: False, Reason: UnsupportedProtocol (gatewayv1.ListenerReasonUnsupportedProtocol)
//    - Programmed: False, Reason: Invalid (gatewayv1.ListenerReasonInvalid)
// 2. Invalid hostname (must be exactly a single-label .local domain, e.g. <label>.local):
//    - Accepted: False, Reason: UnsupportedValue (gatewayv1.ListenerReasonUnsupportedValue)
//    - Programmed: False, Reason: Invalid (gatewayv1.ListenerReasonInvalid)

var (
	// rfc1123LabelRegex defines a valid DNS label per RFC 1123:
	// lowercase alphanumeric characters or '-', starting and ending with an alphanumeric character.
	rfc1123LabelRegex = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

	// ErrEmptyHostname indicates that the hostname is empty.
	ErrEmptyHostname = errors.New("hostname is required: must be a single-label .local domain (e.g. <label>.local)")

	// ErrWildcardHostname indicates that wildcard hostnames are not permitted.
	ErrWildcardHostname = errors.New("wildcard hostnames are not supported: must be a single-label .local domain (e.g. <label>.local)")

	// ErrNotLocalDomain indicates that the hostname does not end in .local.
	ErrNotLocalDomain = errors.New("domain must end in .local; other domains are out of scope for localgateway")

	// ErrMultiLabelLocal indicates that multiple subdomains before .local were specified.
	ErrMultiLabelLocal = errors.New("multi-label hostnames are not supported: must be exactly a single-label .local domain (e.g. <label>.local)")

	// ErrInvalidDNSLabel indicates that the label does not conform to RFC 1123.
	ErrInvalidDNSLabel = errors.New("invalid label: must consist of alphanumeric characters or '-', start and end with an alphanumeric character, and be at most 63 characters")
)

// ValidateSingleLabelLocalHostname validates that the hostname is a single-label .local name
// (exactly `<label>.local`, with `<label>` conforming to RFC 1123 DNS label rules).
func ValidateSingleLabelLocalHostname(hostname string) error {
	h := strings.ToLower(strings.TrimSpace(hostname))
	if h == "" {
		return ErrEmptyHostname
	}

	if strings.Contains(h, "*") {
		return ErrWildcardHostname
	}

	if !strings.HasSuffix(h, ".local") {
		return ErrNotLocalDomain
	}

	label := strings.TrimSuffix(h, ".local")
	if label == "" {
		return ErrEmptyHostname
	}

	// Must be single-label: no interior dots.
	if strings.Contains(label, ".") {
		return ErrMultiLabelLocal
	}

	// Length must be between 1 and 63.
	if len(label) > 63 {
		return ErrInvalidDNSLabel
	}

	if !rfc1123LabelRegex.MatchString(label) {
		return ErrInvalidDNSLabel
	}

	return nil
}

// ValidateListener implements state.ListenerValidator for localgateway.
// It verifies that:
// 1. The protocol is HTTP. HTTPS/TLS listeners are rejected with UnsupportedProtocol.
// 2. The hostname is specified and is a single-label .local name (RFC 1123).
func ValidateListener(spec state.ListenerSpec, owner state.ListenerOwner, gw *gatewayv1.Gateway) []metav1.Condition {
	var gen int64
	if gw != nil {
		gen = gw.Generation
	}

	// Protocol check: localgateway is HTTP-only for now.
	if spec.Protocol != gatewayv1.HTTPProtocolType {
		msg := fmt.Sprintf("Protocol %q is not supported yet; localgateway supports HTTP only", spec.Protocol)
		return []metav1.Condition{
			state.NewCondition(
				string(gatewayv1.ListenerConditionProgrammed),
				metav1.ConditionFalse,
				string(gatewayv1.ListenerReasonInvalid),
				msg,
				gen,
			),
			state.NewCondition(
				string(gatewayv1.ListenerConditionAccepted),
				metav1.ConditionFalse,
				string(gatewayv1.ListenerReasonUnsupportedProtocol),
				msg,
				gen,
			),
		}
	}

	// Hostname check: must be single-label .local name.
	if spec.Hostname == nil || strings.TrimSpace(string(*spec.Hostname)) == "" {
		return []metav1.Condition{
			state.NewCondition(
				string(gatewayv1.ListenerConditionProgrammed),
				metav1.ConditionFalse,
				string(gatewayv1.ListenerReasonInvalid),
				ErrEmptyHostname.Error(),
				gen,
			),
			state.NewCondition(
				string(gatewayv1.ListenerConditionAccepted),
				metav1.ConditionFalse,
				string(gatewayv1.ListenerReasonUnsupportedValue),
				ErrEmptyHostname.Error(),
				gen,
			),
		}
	}

	rawHost := string(*spec.Hostname)
	if err := ValidateSingleLabelLocalHostname(rawHost); err != nil {
		msg := fmt.Sprintf("hostname %q is rejected: %v", rawHost, err)
		return []metav1.Condition{
			state.NewCondition(
				string(gatewayv1.ListenerConditionProgrammed),
				metav1.ConditionFalse,
				string(gatewayv1.ListenerReasonInvalid),
				msg,
				gen,
			),
			state.NewCondition(
				string(gatewayv1.ListenerConditionAccepted),
				metav1.ConditionFalse,
				string(gatewayv1.ListenerReasonUnsupportedValue),
				msg,
				gen,
			),
		}
	}

	return nil
}
