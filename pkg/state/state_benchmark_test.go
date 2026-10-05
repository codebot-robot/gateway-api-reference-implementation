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
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func BenchmarkState_Recompute(b *testing.B) {
	st := NewState()
	st.SetControllerName("example.net/gateway-controller")

	// Setup 100 Gateways and 1000 HTTPRoutes (10 routes per gateway)
	const numGateways = 100
	const routesPerGateway = 10

	for g := 0; g < numGateways; g++ {
		gwName := fmt.Sprintf("gw-%d", g)
		ns := fmt.Sprintf("ns-%d", g%10)

		st.UpsertNamespace(&corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		})

		gw := &gatewayv1.Gateway{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: gwName},
			Spec: gatewayv1.GatewaySpec{
				Listeners: []gatewayv1.Listener{
					{
						Name:     "http",
						Port:     80,
						Protocol: gatewayv1.HTTPProtocolType,
						AllowedRoutes: &gatewayv1.AllowedRoutes{
							Namespaces: &gatewayv1.RouteNamespaces{
								From: Ptr(gatewayv1.NamespacesFromAll),
							},
						},
					},
				},
			},
		}
		st.UpsertGateway(gw)
		st.SetGatewayAddresses(types.NamespacedName{Namespace: ns, Name: gwName}, []gatewayv1.GatewayStatusAddress{
			{Value: fmt.Sprintf("192.0.2.%d", g%250)},
		})

		svcName := fmt.Sprintf("svc-%d", g)
		st.UpsertService(&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: svcName},
		})

		for r := 0; r < routesPerGateway; r++ {
			rName := fmt.Sprintf("route-%d-%d", g, r)
			route := &gatewayv1.HTTPRoute{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: rName},
				Spec: gatewayv1.HTTPRouteSpec{
					CommonRouteSpec: gatewayv1.CommonRouteSpec{
						ParentRefs: []gatewayv1.ParentReference{
							{
								Namespace: Ptr(gatewayv1.Namespace(ns)),
								Name:      gatewayv1.ObjectName(gwName),
							},
						},
					},
					Rules: []gatewayv1.HTTPRouteRule{
						{
							Matches: []gatewayv1.HTTPRouteMatch{
								{
									Path: &gatewayv1.HTTPPathMatch{
										Type:  Ptr(gatewayv1.PathMatchPathPrefix),
										Value: Ptr(fmt.Sprintf("/v%d", r)),
									},
								},
							},
							BackendRefs: []gatewayv1.HTTPBackendRef{
								{
									BackendRef: gatewayv1.BackendRef{
										BackendObjectReference: gatewayv1.BackendObjectReference{
											Name: gatewayv1.ObjectName(svcName),
										},
									},
								},
							},
						},
					},
				},
			}
			st.UpsertHTTPRoute(route)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st.Recompute()
	}
}
