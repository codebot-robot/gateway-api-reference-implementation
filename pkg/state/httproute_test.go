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

func TestComputeAcceptedCondition(t *testing.T) {
	gw := &GatewayState{
		Gateway: &gatewayv1.Gateway{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "same-namespace",
				Namespace: "gateway-conformance-infra",
			},
			Spec: gatewayv1.GatewaySpec{
				Listeners: []gatewayv1.Listener{
					{
						Name:     "http",
						Port:     80,
						Protocol: gatewayv1.HTTPProtocolType,
						AllowedRoutes: &gatewayv1.AllowedRoutes{
							Namespaces: &gatewayv1.RouteNamespaces{
								From: Ptr(gatewayv1.NamespacesFromSame),
							},
						},
					},
					{
						Name:     "https",
						Port:     443,
						Hostname: Ptr(gatewayv1.Hostname("example.com")),
						Protocol: gatewayv1.HTTPSProtocolType,
						AllowedRoutes: &gatewayv1.AllowedRoutes{
							Namespaces: &gatewayv1.RouteNamespaces{
								From: Ptr(gatewayv1.NamespacesFromAll),
							},
						},
					},
					{
						Name:     "tcp",
						Port:     9000,
						Protocol: gatewayv1.TLSProtocolType,
					},
				},
			},
		},
	}

	gateways := []*GatewayState{gw}

	tests := []struct {
		name           string
		route          *HTTPRouteState
		parentRef      gatewayv1.ParentReference
		expectedStatus metav1.ConditionStatus
		expectedReason string
	}{
		{
			name: "valid route matching http listener in same namespace",
			route: &HTTPRouteState{
				HTTPRoute: &gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-route",
						Namespace: "gateway-conformance-infra",
					},
				},
			},
			parentRef: gatewayv1.ParentReference{
				Name: "same-namespace",
			},
			expectedStatus: metav1.ConditionTrue,
			expectedReason: string(gatewayv1.RouteReasonAccepted),
		},
		{
			name: "unsupported parent group",
			route: &HTTPRouteState{
				HTTPRoute: &gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-route",
						Namespace: "gateway-conformance-infra",
					},
				},
			},
			parentRef: gatewayv1.ParentReference{
				Group: Ptr(gatewayv1.Group("custom.io")),
				Name:  "same-namespace",
			},
			expectedStatus: metav1.ConditionFalse,
			expectedReason: string(gatewayv1.RouteReasonNoMatchingParent),
		},
		{
			name: "unsupported parent kind",
			route: &HTTPRouteState{
				HTTPRoute: &gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-route",
						Namespace: "gateway-conformance-infra",
					},
				},
			},
			parentRef: gatewayv1.ParentReference{
				Kind: Ptr(gatewayv1.Kind("Service")),
				Name: "same-namespace",
			},
			expectedStatus: metav1.ConditionFalse,
			expectedReason: string(gatewayv1.RouteReasonNoMatchingParent),
		},
		{
			name: "gateway not found",
			route: &HTTPRouteState{
				HTTPRoute: &gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-route",
						Namespace: "gateway-conformance-infra",
					},
				},
			},
			parentRef: gatewayv1.ParentReference{
				Name: "non-existent-gw",
			},
			expectedStatus: metav1.ConditionFalse,
			expectedReason: string(gatewayv1.RouteReasonNoMatchingParent),
		},
		{
			name: "invalid parentRef not matching listener port",
			route: &HTTPRouteState{
				HTTPRoute: &gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-route",
						Namespace: "gateway-conformance-infra",
					},
				},
			},
			parentRef: gatewayv1.ParentReference{
				Name:      "same-namespace",
				Namespace: Ptr(gatewayv1.Namespace("gateway-conformance-infra")),
				Port:      Ptr(gatewayv1.PortNumber(81)),
			},
			expectedStatus: metav1.ConditionFalse,
			expectedReason: string(gatewayv1.RouteReasonNoMatchingParent),
		},
		{
			name: "invalid parentRef not matching section name",
			route: &HTTPRouteState{
				HTTPRoute: &gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-route",
						Namespace: "gateway-conformance-infra",
					},
				},
			},
			parentRef: gatewayv1.ParentReference{
				Name:        "same-namespace",
				Namespace:   Ptr(gatewayv1.Namespace("gateway-conformance-infra")),
				Port:        Ptr(gatewayv1.PortNumber(80)),
				SectionName: Ptr(gatewayv1.SectionName("http1")),
			},
			expectedStatus: metav1.ConditionFalse,
			expectedReason: string(gatewayv1.RouteReasonNoMatchingParent),
		},
		{
			name: "invalid parentRef section name not matching port",
			route: &HTTPRouteState{
				HTTPRoute: &gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-route",
						Namespace: "gateway-conformance-infra",
					},
				},
			},
			parentRef: gatewayv1.ParentReference{
				Name:        "same-namespace",
				Namespace:   Ptr(gatewayv1.Namespace("gateway-conformance-infra")),
				SectionName: Ptr(gatewayv1.SectionName("http")),
				Port:        Ptr(gatewayv1.PortNumber(81)),
			},
			expectedStatus: metav1.ConditionFalse,
			expectedReason: string(gatewayv1.RouteReasonNoMatchingParent),
		},
		{
			name: "invalid cross namespace parent ref when listener allows only Same",
			route: &HTTPRouteState{
				HTTPRoute: &gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-route",
						Namespace: "gateway-conformance-web-backend",
					},
				},
			},
			parentRef: gatewayv1.ParentReference{
				Name:      "same-namespace",
				Namespace: Ptr(gatewayv1.Namespace("gateway-conformance-infra")),
				Port:      Ptr(gatewayv1.PortNumber(80)),
			},
			expectedStatus: metav1.ConditionFalse,
			expectedReason: string(gatewayv1.RouteReasonNotAllowedByListeners),
		},
		{
			name: "valid cross namespace parent ref when listener allows All",
			route: &HTTPRouteState{
				HTTPRoute: &gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-route",
						Namespace: "gateway-conformance-web-backend",
					},
					Spec: gatewayv1.HTTPRouteSpec{
						Hostnames: []gatewayv1.Hostname{"example.com"},
					},
				},
			},
			parentRef: gatewayv1.ParentReference{
				Name:        "same-namespace",
				Namespace:   Ptr(gatewayv1.Namespace("gateway-conformance-infra")),
				SectionName: Ptr(gatewayv1.SectionName("https")),
			},
			expectedStatus: metav1.ConditionTrue,
			expectedReason: string(gatewayv1.RouteReasonAccepted),
		},
		{
			name: "no matching listener hostname",
			route: &HTTPRouteState{
				HTTPRoute: &gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-route",
						Namespace: "gateway-conformance-web-backend",
					},
					Spec: gatewayv1.HTTPRouteSpec{
						Hostnames: []gatewayv1.Hostname{"other.com"},
					},
				},
			},
			parentRef: gatewayv1.ParentReference{
				Name:        "same-namespace",
				Namespace:   Ptr(gatewayv1.Namespace("gateway-conformance-infra")),
				SectionName: Ptr(gatewayv1.SectionName("https")),
			},
			expectedStatus: metav1.ConditionFalse,
			expectedReason: string(gatewayv1.RouteReasonNoMatchingListenerHostname),
		},
		{
			name: "listener protocol not compatible (TCP listener only)",
			route: &HTTPRouteState{
				HTTPRoute: &gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-route",
						Namespace: "gateway-conformance-infra",
					},
				},
			},
			parentRef: gatewayv1.ParentReference{
				Name:        "same-namespace",
				Namespace:   Ptr(gatewayv1.Namespace("gateway-conformance-infra")),
				SectionName: Ptr(gatewayv1.SectionName("tcp")),
			},
			expectedStatus: metav1.ConditionFalse,
			expectedReason: string(gatewayv1.RouteReasonNotAllowedByListeners),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond := tt.route.ComputeAcceptedCondition(tt.parentRef, gateways)
			if cond.Status != tt.expectedStatus {
				t.Errorf("ComputeAcceptedCondition() Status = %v, want %v", cond.Status, tt.expectedStatus)
			}
			if cond.Reason != tt.expectedReason {
				t.Errorf("ComputeAcceptedCondition() Reason = %v, want %v", cond.Reason, tt.expectedReason)
			}
		})
	}
}

func TestIsAcceptedForParentRef(t *testing.T) {
	route := &HTTPRouteState{
		HTTPRoute: &gatewayv1.HTTPRoute{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-route",
				Namespace: "default",
			},
			Status: gatewayv1.HTTPRouteStatus{
				RouteStatus: gatewayv1.RouteStatus{
					Parents: []gatewayv1.RouteParentStatus{
						{
							ParentRef: gatewayv1.ParentReference{
								Name: "gw-1",
							},
							ControllerName: "example.com/controller",
							Conditions: []metav1.Condition{
								{
									Type:   string(gatewayv1.RouteConditionAccepted),
									Status: metav1.ConditionTrue,
								},
							},
						},
						{
							ParentRef: gatewayv1.ParentReference{
								Name: "gw-2",
								Port: Ptr(gatewayv1.PortNumber(81)),
							},
							ControllerName: "example.com/controller",
							Conditions: []metav1.Condition{
								{
									Type:   string(gatewayv1.RouteConditionAccepted),
									Status: metav1.ConditionFalse,
									Reason: string(gatewayv1.RouteReasonNoMatchingParent),
								},
							},
						},
					},
				},
			},
		},
	}

	if !route.IsAcceptedForParentRef(gatewayv1.ParentReference{Name: "gw-1"}, "example.com/controller") {
		t.Errorf("expected route to be accepted for gw-1")
	}

	if route.IsAcceptedForParentRef(gatewayv1.ParentReference{Name: "gw-2", Port: Ptr(gatewayv1.PortNumber(81))}, "example.com/controller") {
		t.Errorf("expected route NOT to be accepted for gw-2 with port 81")
	}

	if route.IsAcceptedForParentRef(gatewayv1.ParentReference{Name: "gw-nonexistent"}, "example.com/controller") {
		t.Errorf("expected route NOT to be accepted for gw-nonexistent")
	}
}
