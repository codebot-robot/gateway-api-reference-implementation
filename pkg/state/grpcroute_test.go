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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestGRPCRoute_MethodCompilation(t *testing.T) {
	svc := "my.package.MyService"
	method := "MyMethod"

	route := &gatewayv1.GRPCRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-grpcroute-methods",
			Namespace: "default",
		},
		Spec: gatewayv1.GRPCRouteSpec{
			Rules: []gatewayv1.GRPCRouteRule{
				{
					Matches: []gatewayv1.GRPCRouteMatch{
						{
							Method: &gatewayv1.GRPCMethodMatch{
								Service: &svc,
								Method:  &method,
							},
						},
						{
							Method: &gatewayv1.GRPCMethodMatch{
								Service: &svc,
							},
						},
					},
				},
			},
		},
	}

	compiled := CompileGRPCRoute(route, nil, nil, nil, nil)
	if compiled == nil {
		t.Fatalf("expected compiled GRPCRoute")
	}
	if len(compiled.Rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(compiled.Rules))
	}
	if len(compiled.Rules[0].Matches) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(compiled.Rules[0].Matches))
	}

	// First match: method{service, method} -> Exact match on /<service>/<method>
	m1 := compiled.Rules[0].Matches[0]
	if m1.Path == nil {
		t.Fatalf("expected path match for m1")
	}
	if m1.Path.Type != gatewayv1.PathMatchExact {
		t.Errorf("expected PathMatchExact, got %s", m1.Path.Type)
	}
	if m1.Path.Value != "/my.package.MyService/MyMethod" {
		t.Errorf("expected path /my.package.MyService/MyMethod, got %s", m1.Path.Value)
	}

	// Second match: service alone -> PathPrefix match on /<service>/
	m2 := compiled.Rules[0].Matches[1]
	if m2.Path == nil {
		t.Fatalf("expected path match for m2")
	}
	if m2.Path.Type != gatewayv1.PathMatchPathPrefix {
		t.Errorf("expected PathMatchPathPrefix, got %s", m2.Path.Type)
	}
	if m2.Path.Value != "/my.package.MyService/" {
		t.Errorf("expected path /my.package.MyService/, got %s", m2.Path.Value)
	}
}

func TestGRPCRoute_HeaderCompilation(t *testing.T) {
	regexType := gatewayv1.GRPCHeaderMatchRegularExpression
	route := &gatewayv1.GRPCRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-grpcroute-headers",
			Namespace: "default",
		},
		Spec: gatewayv1.GRPCRouteSpec{
			Rules: []gatewayv1.GRPCRouteRule{
				{
					Matches: []gatewayv1.GRPCRouteMatch{
						{
							Headers: []gatewayv1.GRPCHeaderMatch{
								{
									Name:  "X-Exact-Header",
									Value: "exact-val",
								},
								{
									Type:  &regexType,
									Name:  "X-Regex-Header",
									Value: "val-[0-9]+",
								},
								// Duplicate header name (case-insensitive) should be ignored
								{
									Name:  "x-exact-header",
									Value: "duplicate-ignored",
								},
							},
						},
					},
				},
			},
		},
	}

	compiled := CompileGRPCRoute(route, nil, nil, nil, nil)
	if compiled == nil {
		t.Fatalf("expected compiled GRPCRoute")
	}
	if len(compiled.Rules) != 1 || len(compiled.Rules[0].Matches) != 1 {
		t.Fatalf("expected 1 rule with 1 match")
	}

	match := compiled.Rules[0].Matches[0]
	if len(match.Headers) != 2 {
		t.Fatalf("expected 2 headers after deduplication, got %d", len(match.Headers))
	}

	h1 := match.Headers[0]
	if h1.Name != "X-Exact-Header" || h1.Type != gatewayv1.HeaderMatchExact || h1.MatchExactValue != "exact-val" {
		t.Errorf("unexpected h1: %+v", h1)
	}

	h2 := match.Headers[1]
	if h2.Name != "X-Regex-Header" || h2.Type != gatewayv1.HeaderMatchRegularExpression || h2.MatchRegularExpressionValue == nil {
		t.Errorf("unexpected h2: %+v", h2)
	}
	if !h2.MatchRegularExpressionValue.MatchString("val-123") {
		t.Errorf("regex didn't match expected string")
	}
}

func TestGRPCRoute_ListenerBinding(t *testing.T) {
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "multi-listener-gateway",
			Namespace: "default",
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "ref-class",
			Listeners: []gatewayv1.Listener{
				{
					Name:     "http-listener",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
				},
				{
					Name:     "https-listener",
					Port:     443,
					Protocol: gatewayv1.HTTPSProtocolType,
				},
				{
					Name:     "tls-listener",
					Port:     8443,
					Protocol: gatewayv1.TLSProtocolType,
				},
			},
		},
	}

	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "ref-class"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "example.net/gateway-controller"},
	}

	// GRPCRoute attempting to attach to all listeners of the Gateway
	grpcRoute := &gatewayv1.GRPCRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-grpcroute",
			Namespace: "default",
		},
		Spec: gatewayv1.GRPCRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{
						Name: "multi-listener-gateway",
					},
				},
			},
		},
	}

	inputs := ModelInputs{
		Gateways:       []*gatewayv1.Gateway{gw},
		GatewayClasses: []*gatewayv1.GatewayClass{gc},
		GRPCRoutes:     []*gatewayv1.GRPCRoute{grpcRoute},
		ControllerName: "example.net/gateway-controller",
	}

	cm := CompileModel(inputs)
	cg := cm.Gateways[types.NamespacedName{Namespace: "default", Name: "multi-listener-gateway"}]
	if cg == nil {
		t.Fatalf("expected compiled gateway")
	}

	var httpEl, httpsEl, tlsEl *EffectiveListener
	for _, el := range cg.EffectiveListeners {
		switch el.Name {
		case "http-listener":
			httpEl = el
		case "https-listener":
			httpsEl = el
		case "tls-listener":
			tlsEl = el
		}
	}

	if httpEl == nil || httpsEl == nil || tlsEl == nil {
		t.Fatalf("missing expected listeners")
	}

	// GRPCRoute binds to HTTP and HTTPS listeners, not to TLS
	if httpEl.AttachedRoutes != 1 {
		t.Errorf("expected 1 attached route on HTTP listener, got %d", httpEl.AttachedRoutes)
	}
	if len(httpEl.Routes) != 1 {
		t.Errorf("expected 1 compiled route on HTTP listener, got %d", len(httpEl.Routes))
	}

	if httpsEl.AttachedRoutes != 1 {
		t.Errorf("expected 1 attached route on HTTPS listener, got %d", httpsEl.AttachedRoutes)
	}
	if len(httpsEl.Routes) != 1 {
		t.Errorf("expected 1 compiled route on HTTPS listener, got %d", len(httpsEl.Routes))
	}

	if tlsEl.AttachedRoutes != 0 {
		t.Errorf("expected 0 attached routes on TLS listener, got %d", tlsEl.AttachedRoutes)
	}
	if len(tlsEl.Routes) != 0 {
		t.Errorf("expected 0 compiled routes on TLS listener, got %d", len(tlsEl.Routes))
	}
}

func TestGRPCRoute_AttachedRoutes_Coexistence(t *testing.T) {
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "shared-gateway",
			Namespace: "default",
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "ref-class",
			Listeners: []gatewayv1.Listener{
				{
					Name:     "http",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
				},
			},
		},
	}

	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "ref-class"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "example.net/gateway-controller"},
	}

	httpRoute := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-httproute",
			Namespace: "default",
		},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{Name: "shared-gateway"},
				},
			},
		},
	}

	grpcRoute := &gatewayv1.GRPCRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-grpcroute",
			Namespace: "default",
		},
		Spec: gatewayv1.GRPCRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{Name: "shared-gateway"},
				},
			},
		},
	}

	inputs := ModelInputs{
		Gateways:       []*gatewayv1.Gateway{gw},
		GatewayClasses: []*gatewayv1.GatewayClass{gc},
		HTTPRoutes:     []*gatewayv1.HTTPRoute{httpRoute},
		GRPCRoutes:     []*gatewayv1.GRPCRoute{grpcRoute},
		ControllerName: "example.net/gateway-controller",
	}

	cm := CompileModel(inputs)
	cg := cm.Gateways[types.NamespacedName{Namespace: "default", Name: "shared-gateway"}]
	if cg == nil {
		t.Fatalf("expected compiled gateway")
	}

	if len(cg.EffectiveListeners) != 1 {
		t.Fatalf("expected 1 effective listener, got %d", len(cg.EffectiveListeners))
	}

	el := cg.EffectiveListeners[0]
	// Listener with one HTTPRoute and one GRPCRoute reports attachedRoutes: 2
	if el.AttachedRoutes != 2 {
		t.Errorf("expected attachedRoutes 2, got %d", el.AttachedRoutes)
	}

	// Both routes are present in the compiled listener
	if len(el.Routes) != 2 {
		t.Errorf("expected 2 compiled routes in listener, got %d", len(el.Routes))
	}

	outputs := ComputeOutputs(inputs)
	gwStatus := outputs.GatewayStatuses[types.NamespacedName{Namespace: "default", Name: "shared-gateway"}]
	if len(gwStatus.Listeners) != 1 {
		t.Fatalf("expected 1 listener status")
	}
	if gwStatus.Listeners[0].AttachedRoutes != 2 {
		t.Errorf("expected listener status attachedRoutes 2, got %d", gwStatus.Listeners[0].AttachedRoutes)
	}
}

func TestGRPCRoute_Status_Conditions(t *testing.T) {
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "status-gateway",
			Namespace: "default",
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "ref-class",
			Listeners: []gatewayv1.Listener{
				{
					Name:     "http",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
				},
			},
		},
	}

	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "ref-class"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "example.net/gateway-controller"},
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "grpc-svc",
			Namespace: "default",
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Port: 8080},
			},
		},
	}

	port8080 := gatewayv1.PortNumber(8080)
	grpcRoute := &gatewayv1.GRPCRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "status-grpcroute",
			Namespace: "default",
		},
		Spec: gatewayv1.GRPCRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{Name: "status-gateway"},
				},
			},
			Rules: []gatewayv1.GRPCRouteRule{
				{
					BackendRefs: []gatewayv1.GRPCBackendRef{
						{
							BackendRef: gatewayv1.BackendRef{
								BackendObjectReference: gatewayv1.BackendObjectReference{
									Name: "grpc-svc",
									Port: &port8080,
								},
							},
						},
					},
				},
			},
		},
	}

	inputs := ModelInputs{
		Gateways:       []*gatewayv1.Gateway{gw},
		GatewayClasses: []*gatewayv1.GatewayClass{gc},
		GRPCRoutes:     []*gatewayv1.GRPCRoute{grpcRoute},
		Services: map[types.NamespacedName]*corev1.Service{
			{Namespace: "default", Name: "grpc-svc"}: svc,
		},
		ControllerName: "example.net/gateway-controller",
	}

	outputs := ComputeOutputs(inputs)
	routeStatus, ok := outputs.GRPCRouteStatuses[types.NamespacedName{Namespace: "default", Name: "status-grpcroute"}]
	if !ok {
		t.Fatalf("expected status for status-grpcroute")
	}

	if len(routeStatus.Parents) != 1 {
		t.Fatalf("expected 1 parent status, got %d", len(routeStatus.Parents))
	}

	parent := routeStatus.Parents[0]
	if string(parent.ControllerName) != "example.net/gateway-controller" {
		t.Errorf("expected controllerName example.net/gateway-controller, got %s", parent.ControllerName)
	}

	var acceptedCond, resolvedRefsCond *metav1.Condition
	for _, c := range parent.Conditions {
		if c.Type == string(gatewayv1.RouteConditionAccepted) {
			acceptedCond = &c
		}
		if c.Type == string(gatewayv1.RouteConditionResolvedRefs) {
			resolvedRefsCond = &c
		}
	}

	if acceptedCond == nil {
		t.Fatalf("expected Accepted condition")
	}
	if acceptedCond.Status != metav1.ConditionTrue {
		t.Errorf("expected Accepted=True, got %s", acceptedCond.Status)
	}

	if resolvedRefsCond == nil {
		t.Fatalf("expected ResolvedRefs condition")
	}
	if resolvedRefsCond.Status != metav1.ConditionTrue {
		t.Errorf("expected ResolvedRefs=True, got %s", resolvedRefsCond.Status)
	}
}
