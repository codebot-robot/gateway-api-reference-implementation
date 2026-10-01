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
)

func TestComputeResolvedRefsCondition(t *testing.T) {
	tests := []struct {
		name           string
		route          *HTTPRouteState
		expectedStatus metav1.ConditionStatus
		expectedReason string
	}{
		{
			name: "valid backend ref with default kind and group",
			route: &HTTPRouteState{
				HTTPRoute: &gatewayv1.HTTPRoute{
					Spec: gatewayv1.HTTPRouteSpec{
						Rules: []gatewayv1.HTTPRouteRule{
							{
								BackendRefs: []gatewayv1.HTTPBackendRef{
									{
										BackendRef: gatewayv1.BackendRef{
											BackendObjectReference: gatewayv1.BackendObjectReference{
												Name: "valid-service",
												Port: Ptr(gatewayv1.PortNumber(80)),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedStatus: metav1.ConditionTrue,
			expectedReason: string(gatewayv1.RouteReasonResolvedRefs),
		},
		{
			name: "valid backend ref with explicit Service kind and empty group",
			route: &HTTPRouteState{
				HTTPRoute: &gatewayv1.HTTPRoute{
					Spec: gatewayv1.HTTPRouteSpec{
						Rules: []gatewayv1.HTTPRouteRule{
							{
								BackendRefs: []gatewayv1.HTTPBackendRef{
									{
										BackendRef: gatewayv1.BackendRef{
											BackendObjectReference: gatewayv1.BackendObjectReference{
												Group: Ptr(gatewayv1.Group("")),
												Kind:  Ptr(gatewayv1.Kind("Service")),
												Name:  "valid-service",
												Port:  Ptr(gatewayv1.PortNumber(80)),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedStatus: metav1.ConditionTrue,
			expectedReason: string(gatewayv1.RouteReasonResolvedRefs),
		},
		{
			name: "invalid backend ref with unknown kind",
			route: &HTTPRouteState{
				HTTPRoute: &gatewayv1.HTTPRoute{
					Spec: gatewayv1.HTTPRouteSpec{
						Rules: []gatewayv1.HTTPRouteRule{
							{
								BackendRefs: []gatewayv1.HTTPBackendRef{
									{
										BackendRef: gatewayv1.BackendRef{
											BackendObjectReference: gatewayv1.BackendObjectReference{
												Kind: Ptr(gatewayv1.Kind("NonExistent")),
												Name: "invalid-backend",
												Port: Ptr(gatewayv1.PortNumber(80)),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedStatus: metav1.ConditionFalse,
			expectedReason: string(gatewayv1.RouteReasonInvalidKind),
		},
		{
			name: "invalid backend ref with custom group and kind",
			route: &HTTPRouteState{
				HTTPRoute: &gatewayv1.HTTPRoute{
					Spec: gatewayv1.HTTPRouteSpec{
						Rules: []gatewayv1.HTTPRouteRule{
							{
								BackendRefs: []gatewayv1.HTTPBackendRef{
									{
										BackendRef: gatewayv1.BackendRef{
											BackendObjectReference: gatewayv1.BackendObjectReference{
												Group: Ptr(gatewayv1.Group("unknownkind.example.com")),
												Kind:  Ptr(gatewayv1.Kind("NonExistent")),
												Name:  "invalid-backend",
												Port:  Ptr(gatewayv1.PortNumber(80)),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedStatus: metav1.ConditionFalse,
			expectedReason: string(gatewayv1.RouteReasonInvalidKind),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond := tt.route.ComputeResolvedRefsCondition()
			if cond.Status != tt.expectedStatus {
				t.Errorf("ComputeResolvedRefsCondition() Status = %v, want %v", cond.Status, tt.expectedStatus)
			}
			if cond.Reason != tt.expectedReason {
				t.Errorf("ComputeResolvedRefsCondition() Reason = %v, want %v", cond.Reason, tt.expectedReason)
			}
		})
	}
}
