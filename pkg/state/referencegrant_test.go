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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

func TestIsReferencePermitted(t *testing.T) {
	tests := []struct {
		name            string
		fromGroup       string
		fromKind        string
		fromNamespace   string
		toGroup         string
		toKind          string
		toNamespace     string
		toName          string
		referenceGrants []*gatewayv1beta1.ReferenceGrant
		expected        bool
	}{
		{
			name:          "same namespace reference is always permitted",
			fromGroup:     gatewayv1.GroupName,
			fromKind:      "HTTPRoute",
			fromNamespace: "default",
			toGroup:       "",
			toKind:        "Service",
			toNamespace:   "default",
			toName:        "my-service",
			expected:      true,
		},
		{
			name:          "cross namespace reference without any grants is rejected",
			fromGroup:     gatewayv1.GroupName,
			fromKind:      "HTTPRoute",
			fromNamespace: "default",
			toGroup:       "",
			toKind:        "Service",
			toNamespace:   "target-ns",
			toName:        "my-service",
			expected:      false,
		},
		{
			name:          "cross namespace reference with wildcard grant is permitted",
			fromGroup:     gatewayv1.GroupName,
			fromKind:      "HTTPRoute",
			fromNamespace: "default",
			toGroup:       "",
			toKind:        "Service",
			toNamespace:   "target-ns",
			toName:        "my-service",
			referenceGrants: []*gatewayv1beta1.ReferenceGrant{
				{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "target-ns",
						Name:      "allow-httproute-services",
					},
					Spec: gatewayv1beta1.ReferenceGrantSpec{
						From: []gatewayv1beta1.ReferenceGrantFrom{
							{
								Group:     gatewayv1.GroupName,
								Kind:      "HTTPRoute",
								Namespace: "default",
							},
						},
						To: []gatewayv1beta1.ReferenceGrantTo{
							{
								Group: "",
								Kind:  "Service",
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name:          "cross namespace reference with named grant matching specific service is permitted",
			fromGroup:     gatewayv1.GroupName,
			fromKind:      "HTTPRoute",
			fromNamespace: "default",
			toGroup:       "",
			toKind:        "Service",
			toNamespace:   "target-ns",
			toName:        "my-service",
			referenceGrants: []*gatewayv1beta1.ReferenceGrant{
				{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "target-ns",
						Name:      "allow-specific-service",
					},
					Spec: gatewayv1beta1.ReferenceGrantSpec{
						From: []gatewayv1beta1.ReferenceGrantFrom{
							{
								Group:     gatewayv1.GroupName,
								Kind:      "HTTPRoute",
								Namespace: "default",
							},
						},
						To: []gatewayv1beta1.ReferenceGrantTo{
							{
								Group: "",
								Kind:  "Service",
								Name:  Ptr(gatewayv1.ObjectName("my-service")),
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name:          "cross namespace reference with named grant for different service is rejected",
			fromGroup:     gatewayv1.GroupName,
			fromKind:      "HTTPRoute",
			fromNamespace: "default",
			toGroup:       "",
			toKind:        "Service",
			toNamespace:   "target-ns",
			toName:        "other-service",
			referenceGrants: []*gatewayv1beta1.ReferenceGrant{
				{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "target-ns",
						Name:      "allow-specific-service",
					},
					Spec: gatewayv1beta1.ReferenceGrantSpec{
						From: []gatewayv1beta1.ReferenceGrantFrom{
							{
								Group:     gatewayv1.GroupName,
								Kind:      "HTTPRoute",
								Namespace: "default",
							},
						},
						To: []gatewayv1beta1.ReferenceGrantTo{
							{
								Group: "",
								Kind:  "Service",
								Name:  Ptr(gatewayv1.ObjectName("my-service")),
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name:          "cross namespace reference with grant in wrong namespace is rejected",
			fromGroup:     gatewayv1.GroupName,
			fromKind:      "HTTPRoute",
			fromNamespace: "default",
			toGroup:       "",
			toKind:        "Service",
			toNamespace:   "target-ns",
			toName:        "my-service",
			referenceGrants: []*gatewayv1beta1.ReferenceGrant{
				{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "wrong-ns",
						Name:      "allow-httproute-services",
					},
					Spec: gatewayv1beta1.ReferenceGrantSpec{
						From: []gatewayv1beta1.ReferenceGrantFrom{
							{
								Group:     gatewayv1.GroupName,
								Kind:      "HTTPRoute",
								Namespace: "default",
							},
						},
						To: []gatewayv1beta1.ReferenceGrantTo{
							{
								Group: "",
								Kind:  "Service",
							},
						},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := IsReferencePermitted(
				tt.fromGroup, tt.fromKind, tt.fromNamespace,
				tt.toGroup, tt.toKind, tt.toNamespace, tt.toName,
				tt.referenceGrants,
			)
			if actual != tt.expected {
				t.Errorf("IsReferencePermitted() = %v, want %v", actual, tt.expected)
			}
		})
	}
}
