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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

// Reference represents a reference to a Kubernetes resource for ReferenceGrant validation.
type Reference struct {
	GroupKind schema.GroupKind
	Namespace string
	Name      string
}

// ReferenceGrantValidator evaluates whether cross-namespace references are permitted.
type ReferenceGrantValidator interface {
	IsReferencePermitted(from, to Reference) bool
}

// IsReferencePermitted checks whether a reference from one resource to another is permitted by the state's ReferenceGrants.
func (s *State) IsReferencePermitted(from, to Reference) bool {
	// References within the same namespace are always permitted.
	if from.Namespace == to.Namespace {
		return true
	}

	if s == nil {
		return false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return isReferencePermitted(from, to, s.referenceGrants)
}

func isReferencePermitted(from, to Reference, referenceGrants map[types.NamespacedName]*gatewayv1beta1.ReferenceGrant) bool {
	if from.Namespace == to.Namespace {
		return true
	}

	fromGroup := from.GroupKind.Group
	if fromGroup == "" {
		fromGroup = "core"
	}
	toGroup := to.GroupKind.Group
	if toGroup == "" {
		toGroup = "core"
	}

	for _, rg := range referenceGrants {
		if rg == nil || rg.Namespace != to.Namespace {
			continue
		}

		// Check if any "From" matches the referencing resource
		fromMatches := false
		for _, f := range rg.Spec.From {
			fGroup := string(f.Group)
			if fGroup == "" {
				fGroup = "core"
			}
			fKind := string(f.Kind)
			fNs := string(f.Namespace)

			if fGroup == fromGroup && fKind == from.GroupKind.Kind && fNs == from.Namespace {
				fromMatches = true
				break
			}
		}
		if !fromMatches {
			continue
		}

		// Check if any "To" matches the referenced resource
		toMatches := false
		for _, t := range rg.Spec.To {
			tGroup := string(t.Group)
			if tGroup == "" {
				tGroup = "core"
			}
			tKind := string(t.Kind)

			if tGroup == toGroup && tKind == to.GroupKind.Kind {
				if t.Name == nil || string(*t.Name) == "" || string(*t.Name) == to.Name {
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
