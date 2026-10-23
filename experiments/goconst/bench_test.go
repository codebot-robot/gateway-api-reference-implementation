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

package goconst

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
)

// BaselineState simulates the current pkg/state.State behavior of DeepCopy on every upsert.
type BaselineState struct {
	tcpRoutes map[types.NamespacedName]*state.TCPRouteState
}

func (s *BaselineState) UpsertTCPRoute(route *gatewayv1.TCPRoute) {
	if route == nil {
		return
	}
	key := types.NamespacedName{Namespace: route.Namespace, Name: route.Name}
	s.tcpRoutes[key] = &state.TCPRouteState{
		TCPRoute: route.DeepCopy(),
	}
}

func BenchmarkReadPath_Upsert(b *testing.B) {
	routes := make([]*gatewayv1.TCPRoute, 1000)
	for i := 0; i < len(routes); i++ {
		routes[i] = sampleRoute(fmt.Sprintf("route-%d", i), "default", int32(8000+i))
	}

	b.Run("StatusQuo_WithDeepCopy", func(b *testing.B) {
		st := &BaselineState{tcpRoutes: make(map[types.NamespacedName]*state.TCPRouteState)}
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			r := routes[i%len(routes)]
			st.UpsertTCPRoute(r)
		}
	})

	b.Run("Goconst_ZeroCopy", func(b *testing.B) {
		st := NewConstState()
		constRoutes := make([]Const[gatewayv1.TCPRoute], len(routes))
		for i := 0; i < len(routes); i++ {
			constRoutes[i] = WrapConst(routes[i])
		}
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			r := constRoutes[i%len(constRoutes)]
			st.UpsertTCPRoute(r)
		}
	})
}

func BenchmarkReadPath_EndToEnd(b *testing.B) {
	r := sampleRoute("route-bench", "gateway-system", 8080)
	services := map[types.NamespacedName]*corev1.Service{
		{Namespace: "gateway-system", Name: "backend-svc"}: sampleService("backend-svc", "gateway-system", "10.96.0.1"),
	}

	b.Run("StatusQuo_DeepCopy_Compile_Status", func(b *testing.B) {
		st := &BaselineState{tcpRoutes: make(map[types.NamespacedName]*state.TCPRouteState)}
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			st.UpsertTCPRoute(r)
			key := types.NamespacedName{Namespace: r.Namespace, Name: r.Name}
			rs := st.tcpRoutes[key]
			internal := state.CompileTCPRoute(rs.TCPRoute, services, nil)
			rs.Internal = internal
			compiled := &state.CompiledTCPRoute{
				TCPRoute:   rs.TCPRoute,
				RouteState: rs,
				ParentConditions: []metav1.Condition{
					state.NewCondition(string(gatewayv1.RouteConditionAccepted), metav1.ConditionTrue, string(gatewayv1.RouteReasonAccepted), "Accepted", 1),
				},
			}
			_ = state.ComputeDesiredTCPRouteStatus(rs.TCPRoute, compiled, "example.com/gateway-controller")
		}
	})

	b.Run("Goconst_ZeroCopy_Compile_Status", func(b *testing.B) {
		st := NewConstState()
		cr := WrapConst(r)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			st.UpsertTCPRoute(cr)
			key := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
			rs := st.GetTCPRoute(key)
			internal := CompileTCPRoute(rs.Route, services)
			rs.Internal = internal
			compiled := &ConstCompiledTCPRoute{
				Route:      rs.Route,
				RouteState: rs,
				ParentConditions: []metav1.Condition{
					state.NewCondition(string(gatewayv1.RouteConditionAccepted), metav1.ConditionTrue, string(gatewayv1.RouteReasonAccepted), "Accepted", 1),
				},
			}
			_ = ComputeDesiredTCPRouteStatus(rs.Route, compiled, "example.com/gateway-controller")
		}
	})
}
