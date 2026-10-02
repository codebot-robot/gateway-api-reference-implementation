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
	"k8s.io/apimachinery/pkg/runtime/schema"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

func TestIsReferencePermitted(t *testing.T) {
	tests := []struct {
		name            string
		from            Reference
		to              Reference
		referenceGrants []*gatewayv1beta1.ReferenceGrant
		expected        bool
	}{
		{
			name: "same namespace reference is always permitted",
			from: Reference{
				GroupKind: schema.GroupKind{Group: gatewayv1.GroupName, Kind: "HTTPRoute"},
				Namespace: "default",
			},
			to: Reference{
				GroupKind: schema.GroupKind{Group: "", Kind: "Service"},
				Namespace: "default",
				Name:      "my-service",
			},
			expected: true,
		},
		{
			name: "cross namespace reference without any grants is rejected",
			from: Reference{
				GroupKind: schema.GroupKind{Group: gatewayv1.GroupName, Kind: "HTTPRoute"},
				Namespace: "default",
			},
			to: Reference{
				GroupKind: schema.GroupKind{Group: "", Kind: "Service"},
				Namespace: "target-ns",
				Name:      "my-service",
			},
			expected: false,
		},
		{
			name: "cross namespace reference with wildcard grant is permitted",
			from: Reference{
				GroupKind: schema.GroupKind{Group: gatewayv1.GroupName, Kind: "HTTPRoute"},
				Namespace: "default",
			},
			to: Reference{
				GroupKind: schema.GroupKind{Group: "", Kind: "Service"},
				Namespace: "target-ns",
				Name:      "my-service",
			},
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
			name: "cross namespace reference with named grant matching specific service is permitted",
			from: Reference{
				GroupKind: schema.GroupKind{Group: gatewayv1.GroupName, Kind: "HTTPRoute"},
				Namespace: "default",
			},
			to: Reference{
				GroupKind: schema.GroupKind{Group: "", Kind: "Service"},
				Namespace: "target-ns",
				Name:      "my-service",
			},
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
			name: "cross namespace reference with named grant for different service is rejected",
			from: Reference{
				GroupKind: schema.GroupKind{Group: gatewayv1.GroupName, Kind: "HTTPRoute"},
				Namespace: "default",
			},
			to: Reference{
				GroupKind: schema.GroupKind{Group: "", Kind: "Service"},
				Namespace: "target-ns",
				Name:      "other-service",
			},
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
			name: "cross namespace reference with grant in wrong namespace is rejected",
			from: Reference{
				GroupKind: schema.GroupKind{Group: gatewayv1.GroupName, Kind: "HTTPRoute"},
				Namespace: "default",
			},
			to: Reference{
				GroupKind: schema.GroupKind{Group: "", Kind: "Service"},
				Namespace: "target-ns",
				Name:      "my-service",
			},
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
			st := NewState()
			for _, rg := range tt.referenceGrants {
				st.UpsertReferenceGrant(rg)
			}
			actual := st.IsReferencePermitted(tt.from, tt.to)
			if actual != tt.expected {
				t.Errorf("st.IsReferencePermitted() = %v, want %v", actual, tt.expected)
			}
		})
	}
}
