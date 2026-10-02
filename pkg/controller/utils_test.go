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

package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestResolveNamespacedName(t *testing.T) {
	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "route-ns",
			Name:      "test-route",
		},
	}

	customNs := gatewayv1.Namespace("custom-ns")
	emptyNs := gatewayv1.Namespace("")

	tests := []struct {
		name      string
		namespace *gatewayv1.Namespace
		objName   gatewayv1.ObjectName
		src       client.Object
		expected  types.NamespacedName
	}{
		{
			name:      "nil namespace defaults to source object namespace",
			namespace: nil,
			objName:   "backend-svc",
			src:       route,
			expected: types.NamespacedName{
				Namespace: "route-ns",
				Name:      "backend-svc",
			},
		},
		{
			name:      "empty namespace defaults to source object namespace",
			namespace: &emptyNs,
			objName:   "backend-svc",
			src:       route,
			expected: types.NamespacedName{
				Namespace: "route-ns",
				Name:      "backend-svc",
			},
		},
		{
			name:      "explicit namespace overrides source object namespace",
			namespace: &customNs,
			objName:   "backend-svc",
			src:       route,
			expected: types.NamespacedName{
				Namespace: "custom-ns",
				Name:      "backend-svc",
			},
		},
		{
			name:      "nil src with explicit namespace",
			namespace: &customNs,
			objName:   "backend-svc",
			src:       nil,
			expected: types.NamespacedName{
				Namespace: "custom-ns",
				Name:      "backend-svc",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := ResolveNamespacedName(tt.namespace, tt.objName, tt.src)
			if actual != tt.expected {
				t.Errorf("ResolveNamespacedName() = %v, want %v", actual, tt.expected)
			}
		})
	}
}
