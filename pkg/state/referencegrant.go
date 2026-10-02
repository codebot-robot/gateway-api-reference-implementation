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
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

// IsReferencePermitted checks whether a reference from a resource in fromNamespace
// (with fromGroup and fromKind) to a resource in toNamespace (with toGroup, toKind, and toName)
// is permitted by the provided ReferenceGrants.
func IsReferencePermitted(
	fromGroup, fromKind, fromNamespace string,
	toGroup, toKind, toNamespace, toName string,
	referenceGrants []*gatewayv1beta1.ReferenceGrant,
) bool {
	// References within the same namespace are always permitted.
	if fromNamespace == toNamespace {
		return true
	}

	for _, rg := range referenceGrants {
		if rg == nil || rg.Namespace != toNamespace {
			continue
		}

		// Check if any "From" matches the referencing resource
		fromMatches := false
		for _, from := range rg.Spec.From {
			fGroup := string(from.Group)
			// When empty, the Kubernetes core API group is inferred in ReferenceGrantFrom
			if fGroup == "" {
				fGroup = "core"
			}
			fKind := string(from.Kind)
			fNs := string(from.Namespace)

			expectedGroup := fromGroup
			if expectedGroup == "" {
				expectedGroup = "core"
			}

			if fGroup == expectedGroup && fKind == fromKind && fNs == fromNamespace {
				fromMatches = true
				break
			}
		}
		if !fromMatches {
			continue
		}

		// Check if any "To" matches the referenced resource
		toMatches := false
		for _, to := range rg.Spec.To {
			tGroup := string(to.Group)
			if tGroup == "" {
				tGroup = "core"
			}
			tKind := string(to.Kind)

			expectedToGroup := toGroup
			if expectedToGroup == "" {
				expectedToGroup = "core"
			}

			if tGroup == expectedToGroup && tKind == toKind {
				if to.Name == nil || string(*to.Name) == "" || string(*to.Name) == toName {
					toMatches = true
					break
				}
			}
		}

		if toMatches {
			return true
		}
	}

	return false
}
