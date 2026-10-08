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

func TestTLSRoute_SupportedKinds_InvalidRouteKinds(t *testing.T) {
	passthroughMode := gatewayv1.TLSModePassthrough
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gateway-tlsroute-passthrough-supported-kind",
			Namespace: "default",
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "ref-class",
			Listeners: []gatewayv1.Listener{
				{
					Name:     "tls-passthrough",
					Port:     443,
					Protocol: gatewayv1.TLSProtocolType,
					AllowedRoutes: &gatewayv1.AllowedRoutes{
						Kinds: []gatewayv1.RouteGroupKind{
							{Kind: "TCPRoute"},
							{Kind: "TLSRoute"},
						},
					},
					TLS: &gatewayv1.ListenerTLSConfig{
						Mode: &passthroughMode,
					},
				},
			},
		},
	}

	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "ref-class"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "example.net/gateway-controller"},
	}

	inputs := ModelInputs{
		Gateways:       []*gatewayv1.Gateway{gw},
		GatewayClasses: []*gatewayv1.GatewayClass{gc},
		ControllerName: "example.net/gateway-controller",
	}

	outputs := ComputeOutputs(inputs)
	gwStatus, ok := outputs.GatewayStatuses[types.NamespacedName{Namespace: "default", Name: gw.Name}]
	if !ok {
		t.Fatalf("expected status for gateway %s", gw.Name)
	}

	if len(gwStatus.Listeners) != 1 {
		t.Fatalf("expected 1 listener status, got %d", len(gwStatus.Listeners))
	}
	lStatus := gwStatus.Listeners[0]
	if string(lStatus.Name) != "tls-passthrough" {
		t.Errorf("expected listener name tls-passthrough, got %s", lStatus.Name)
	}

	// SupportedKinds must report only TLSRoute
	if len(lStatus.SupportedKinds) != 1 {
		t.Fatalf("expected 1 supported kind, got %d", len(lStatus.SupportedKinds))
	}
	if lStatus.SupportedKinds[0].Kind != "TLSRoute" || ValueOf(lStatus.SupportedKinds[0].Group) != gatewayv1.GroupName {
		t.Errorf("expected supported kind TLSRoute with group %s, got %+v", gatewayv1.GroupName, lStatus.SupportedKinds[0])
	}

	if lStatus.AttachedRoutes != 0 {
		t.Errorf("expected 0 attached routes, got %d", lStatus.AttachedRoutes)
	}

	// ResolvedRefs must be ConditionFalse with reason InvalidRouteKinds
	var resolvedRefsCond *metav1.Condition
	for _, c := range lStatus.Conditions {
		if c.Type == string(gatewayv1.ListenerConditionResolvedRefs) {
			resolvedRefsCond = &c
			break
		}
	}
	if resolvedRefsCond == nil {
		t.Fatal("expected ResolvedRefs condition on listener status")
	}
	if resolvedRefsCond.Status != metav1.ConditionFalse {
		t.Errorf("expected ResolvedRefs status False, got %s", resolvedRefsCond.Status)
	}
	if resolvedRefsCond.Reason != string(gatewayv1.ListenerReasonInvalidRouteKinds) {
		t.Errorf("expected ResolvedRefs reason InvalidRouteKinds, got %s", resolvedRefsCond.Reason)
	}
}

func TestTLSRoute_AttachedRoutes(t *testing.T) {
	passthroughMode := gatewayv1.TLSModePassthrough
	hostname := gatewayv1.Hostname("*.example.com")
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gateway-tlsroute",
			Namespace: "default",
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "ref-class",
			Listeners: []gatewayv1.Listener{
				{
					Name:     "tls",
					Port:     443,
					Protocol: gatewayv1.TLSProtocolType,
					Hostname: &hostname,
					AllowedRoutes: &gatewayv1.AllowedRoutes{
						Kinds: []gatewayv1.RouteGroupKind{
							{Kind: "TLSRoute"},
						},
					},
					TLS: &gatewayv1.ListenerTLSConfig{
						Mode: &passthroughMode,
					},
				},
			},
		},
	}

	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "ref-class"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "example.net/gateway-controller"},
	}

	port8443 := gatewayv1.PortNumber(8443)
	route1 := &gatewayv1.TLSRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "route-1",
			Namespace: "default",
		},
		Spec: gatewayv1.TLSRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{Name: "gateway-tlsroute"},
				},
			},
			Hostnames: []gatewayv1.Hostname{"abc.example.com"},
			Rules: []gatewayv1.TLSRouteRule{
				{
					BackendRefs: []gatewayv1.BackendRef{
						{
							BackendObjectReference: gatewayv1.BackendObjectReference{
								Name: "backend-1",
								Port: &port8443,
							},
						},
					},
				},
			},
		},
	}

	route2 := &gatewayv1.TLSRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "route-2",
			Namespace: "default",
		},
		Spec: gatewayv1.TLSRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{Name: "gateway-tlsroute"},
				},
			},
			Hostnames: []gatewayv1.Hostname{"def.example.com"},
			Rules: []gatewayv1.TLSRouteRule{
				{
					BackendRefs: []gatewayv1.BackendRef{
						{
							BackendObjectReference: gatewayv1.BackendObjectReference{
								Name: "backend-2",
								Port: &port8443,
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
		TLSRoutes:      []*gatewayv1.TLSRoute{route1, route2},
		ControllerName: "example.net/gateway-controller",
	}

	outputs := ComputeOutputs(inputs)
	gwStatus := outputs.GatewayStatuses[types.NamespacedName{Namespace: "default", Name: gw.Name}]
	if len(gwStatus.Listeners) != 1 {
		t.Fatalf("expected 1 listener status, got %d", len(gwStatus.Listeners))
	}
	if gwStatus.Listeners[0].AttachedRoutes != 2 {
		t.Errorf("expected 2 attached routes, got %d", gwStatus.Listeners[0].AttachedRoutes)
	}

	// Verify both TLSRoutes have Accepted=True and ResolvedRefs=True
	for _, r := range []*gatewayv1.TLSRoute{route1, route2} {
		rStatus, ok := outputs.TLSRouteStatuses[types.NamespacedName{Namespace: "default", Name: r.Name}]
		if !ok {
			t.Fatalf("expected status for route %s", r.Name)
		}
		if len(rStatus.Parents) != 1 {
			t.Fatalf("expected 1 parent status for route %s, got %d", r.Name, len(rStatus.Parents))
		}
		pStatus := rStatus.Parents[0]
		var acceptedCond, resolvedRefsCond *metav1.Condition
		for _, c := range pStatus.Conditions {
			if c.Type == string(gatewayv1.RouteConditionAccepted) {
				acceptedCond = &c
			}
			if c.Type == string(gatewayv1.RouteConditionResolvedRefs) {
				resolvedRefsCond = &c
			}
		}
		if acceptedCond == nil || acceptedCond.Status != metav1.ConditionTrue {
			t.Errorf("route %s: expected Accepted=True, got %+v", r.Name, acceptedCond)
		}
		if resolvedRefsCond == nil || resolvedRefsCond.Status != metav1.ConditionTrue {
			t.Errorf("route %s: expected ResolvedRefs=True, got %+v", r.Name, resolvedRefsCond)
		}
	}
}

func TestTLSRoute_NoMatchingListenerHostname(t *testing.T) {
	passthroughMode := gatewayv1.TLSModePassthrough
	gwHostname := gatewayv1.Hostname("foo.com")
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gateway-tlsroute",
			Namespace: "default",
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "ref-class",
			Listeners: []gatewayv1.Listener{
				{
					Name:     "tls",
					Port:     443,
					Protocol: gatewayv1.TLSProtocolType,
					Hostname: &gwHostname,
					TLS: &gatewayv1.ListenerTLSConfig{
						Mode: &passthroughMode,
					},
				},
			},
		},
	}

	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "ref-class"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "example.net/gateway-controller"},
	}

	port8443 := gatewayv1.PortNumber(8443)
	route := &gatewayv1.TLSRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "route-bar",
			Namespace: "default",
		},
		Spec: gatewayv1.TLSRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{Name: "gateway-tlsroute"},
				},
			},
			Hostnames: []gatewayv1.Hostname{"bar.com"},
			Rules: []gatewayv1.TLSRouteRule{
				{
					BackendRefs: []gatewayv1.BackendRef{
						{
							BackendObjectReference: gatewayv1.BackendObjectReference{
								Name: "backend",
								Port: &port8443,
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
		TLSRoutes:      []*gatewayv1.TLSRoute{route},
		ControllerName: "example.net/gateway-controller",
	}

	outputs := ComputeOutputs(inputs)
	rStatus, ok := outputs.TLSRouteStatuses[types.NamespacedName{Namespace: "default", Name: route.Name}]
	if !ok {
		t.Fatalf("expected status for route %s", route.Name)
	}
	if len(rStatus.Parents) != 1 {
		t.Fatalf("expected 1 parent status, got %d", len(rStatus.Parents))
	}
	pStatus := rStatus.Parents[0]

	var acceptedCond *metav1.Condition
	for _, c := range pStatus.Conditions {
		if c.Type == string(gatewayv1.RouteConditionAccepted) {
			acceptedCond = &c
			break
		}
	}
	if acceptedCond == nil {
		t.Fatal("expected Accepted condition on route parent status")
	}
	if acceptedCond.Status != metav1.ConditionFalse {
		t.Errorf("expected Accepted status False, got %s", acceptedCond.Status)
	}
	if acceptedCond.Reason != string(gatewayv1.RouteReasonNoMatchingListenerHostname) {
		t.Errorf("expected Accepted reason NoMatchingListenerHostname, got %s", acceptedCond.Reason)
	}
}

func TestTLSRoute_HostnameIntersection_SelectedBackendMatrix(t *testing.T) {
	passthroughMode := gatewayv1.TLSModePassthrough
	port443 := gatewayv1.PortNumber(443)

	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "ref-class"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "example.net/gateway-controller"},
	}

	services := map[types.NamespacedName]*corev1.Service{
		{Namespace: "infra", Name: "tls-backend"}: {
			ObjectMeta: metav1.ObjectMeta{Name: "tls-backend", Namespace: "infra"},
			Spec: corev1.ServiceSpec{
				Ports: []corev1.ServicePort{{Port: 443}},
			},
		},
		{Namespace: "infra", Name: "tls-backend-2"}: {
			ObjectMeta: metav1.ObjectMeta{Name: "tls-backend-2", Namespace: "infra"},
			Spec: corev1.ServiceSpec{
				Ports: []corev1.ServicePort{{Port: 443}},
			},
		},
	}

	// Matrix of tests corresponding to the four listener hostname shapes
	tests := []struct {
		name             string
		listenerHostname *gatewayv1.Hostname
		routes           []*gatewayv1.TLSRoute
		queries          map[string]string // sni -> expected selected backend (empty string = no backend)
	}{
		{
			name:             "Shape 1: Exact listener hostname (abc.example.com)",
			listenerHostname: Ptr(gatewayv1.Hostname("abc.example.com")),
			routes: []*gatewayv1.TLSRoute{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "route-wc", Namespace: "infra"},
					Spec: gatewayv1.TLSRouteSpec{
						CommonRouteSpec: gatewayv1.CommonRouteSpec{
							ParentRefs: []gatewayv1.ParentReference{{Name: "gw"}},
						},
						Hostnames: []gatewayv1.Hostname{"*.example.com"},
						Rules: []gatewayv1.TLSRouteRule{
							{
								BackendRefs: []gatewayv1.BackendRef{
									{
										BackendObjectReference: gatewayv1.BackendObjectReference{
											Name: "tls-backend",
											Port: &port443,
										},
									},
								},
							},
						},
					},
				},
			},
			queries: map[string]string{
				"abc.example.com":   "tls-backend.infra.svc.cluster.local:443",
				"other.example.com": "", // listener doesn't match other.example.com
				"non.matching.com":  "",
			},
		},
		{
			name:             "Shape 2: Specific wildcard listener hostname (*.example.com)",
			listenerHostname: Ptr(gatewayv1.Hostname("*.example.com")),
			routes: []*gatewayv1.TLSRoute{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "route-exact", Namespace: "infra"},
					Spec: gatewayv1.TLSRouteSpec{
						CommonRouteSpec: gatewayv1.CommonRouteSpec{
							ParentRefs: []gatewayv1.ParentReference{{Name: "gw"}},
						},
						Hostnames: []gatewayv1.Hostname{"abc.example.com"},
						Rules: []gatewayv1.TLSRouteRule{
							{
								BackendRefs: []gatewayv1.BackendRef{
									{
										BackendObjectReference: gatewayv1.BackendObjectReference{
											Name: "tls-backend",
											Port: &port443,
										},
									},
								},
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "route-broad-wc", Namespace: "infra"},
					Spec: gatewayv1.TLSRouteSpec{
						CommonRouteSpec: gatewayv1.CommonRouteSpec{
							ParentRefs: []gatewayv1.ParentReference{{Name: "gw"}},
						},
						Hostnames: []gatewayv1.Hostname{"*.com"},
						Rules: []gatewayv1.TLSRouteRule{
							{
								BackendRefs: []gatewayv1.BackendRef{
									{
										BackendObjectReference: gatewayv1.BackendObjectReference{
											Name: "tls-backend-2",
											Port: &port443,
										},
									},
								},
							},
						},
					},
				},
			},
			queries: map[string]string{
				"abc.example.com":   "tls-backend.infra.svc.cluster.local:443",   // exact match wins over wildcard
				"other.example.com": "tls-backend-2.infra.svc.cluster.local:443", // matches *.example.com intersection
				"non.matching.com":  "",                                          // listener doesn't match
			},
		},
		{
			name:             "Shape 3: Less specific wildcard listener hostname (*.com)",
			listenerHostname: Ptr(gatewayv1.Hostname("*.com")),
			routes: []*gatewayv1.TLSRoute{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "route-exact", Namespace: "infra"},
					Spec: gatewayv1.TLSRouteSpec{
						CommonRouteSpec: gatewayv1.CommonRouteSpec{
							ParentRefs: []gatewayv1.ParentReference{{Name: "gw"}},
						},
						Hostnames: []gatewayv1.Hostname{"abc.example.com"},
						Rules: []gatewayv1.TLSRouteRule{
							{
								BackendRefs: []gatewayv1.BackendRef{
									{
										BackendObjectReference: gatewayv1.BackendObjectReference{
											Name: "tls-backend",
											Port: &port443,
										},
									},
								},
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "route-narrow-wc", Namespace: "infra"},
					Spec: gatewayv1.TLSRouteSpec{
						CommonRouteSpec: gatewayv1.CommonRouteSpec{
							ParentRefs: []gatewayv1.ParentReference{{Name: "gw"}},
						},
						Hostnames: []gatewayv1.Hostname{"*.example.com"},
						Rules: []gatewayv1.TLSRouteRule{
							{
								BackendRefs: []gatewayv1.BackendRef{
									{
										BackendObjectReference: gatewayv1.BackendObjectReference{
											Name: "tls-backend-2",
											Port: &port443,
										},
									},
								},
							},
						},
					},
				},
			},
			queries: map[string]string{
				"abc.example.com":   "tls-backend.infra.svc.cluster.local:443",   // exact match wins
				"other.example.com": "tls-backend-2.infra.svc.cluster.local:443", // matches *.example.com
				"non.matching.com":  "",                                          // no route covers non.matching.com
			},
		},
		{
			name:             "Shape 4: Empty listener hostname",
			listenerHostname: nil,
			routes: []*gatewayv1.TLSRoute{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "route-exact", Namespace: "infra"},
					Spec: gatewayv1.TLSRouteSpec{
						CommonRouteSpec: gatewayv1.CommonRouteSpec{
							ParentRefs: []gatewayv1.ParentReference{{Name: "gw"}},
						},
						Hostnames: []gatewayv1.Hostname{"abc.example.com"},
						Rules: []gatewayv1.TLSRouteRule{
							{
								BackendRefs: []gatewayv1.BackendRef{
									{
										BackendObjectReference: gatewayv1.BackendObjectReference{
											Name: "tls-backend",
											Port: &port443,
										},
									},
								},
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "route-wc-com", Namespace: "infra"},
					Spec: gatewayv1.TLSRouteSpec{
						CommonRouteSpec: gatewayv1.CommonRouteSpec{
							ParentRefs: []gatewayv1.ParentReference{{Name: "gw"}},
						},
						Hostnames: []gatewayv1.Hostname{"*.com"},
						Rules: []gatewayv1.TLSRouteRule{
							{
								BackendRefs: []gatewayv1.BackendRef{
									{
										BackendObjectReference: gatewayv1.BackendObjectReference{
											Name: "tls-backend-2",
											Port: &port443,
										},
									},
								},
							},
						},
					},
				},
			},
			queries: map[string]string{
				"abc.example.com":   "tls-backend.infra.svc.cluster.local:443",   // exact match
				"other.example.com": "tls-backend-2.infra.svc.cluster.local:443", // matches *.com
				"non.matching.org":  "",                                          // no route covers .org
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gw := &gatewayv1.Gateway{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "gw",
					Namespace: "infra",
				},
				Spec: gatewayv1.GatewaySpec{
					GatewayClassName: "ref-class",
					Listeners: []gatewayv1.Listener{
						{
							Name:     "tls",
							Port:     443,
							Protocol: gatewayv1.TLSProtocolType,
							Hostname: tc.listenerHostname,
							TLS: &gatewayv1.ListenerTLSConfig{
								Mode: &passthroughMode,
							},
						},
					},
				},
			}

			inputs := ModelInputs{
				Gateways:       []*gatewayv1.Gateway{gw},
				GatewayClasses: []*gatewayv1.GatewayClass{gc},
				TLSRoutes:      tc.routes,
				Services:       services,
				ControllerName: "example.net/gateway-controller",
			}

			outputs := ComputeOutputs(inputs)
			if len(outputs.ProxyListeners) != 1 {
				t.Fatalf("expected 1 proxy listener, got %d", len(outputs.ProxyListeners))
			}
			pListener := outputs.ProxyListeners[0]

			for sni, wantBackend := range tc.queries {
				gotBackend, ok := pListener.SelectTLSBackend(sni)
				if wantBackend == "" {
					if ok {
						t.Errorf("sni %q: expected no backend, got %q", sni, gotBackend)
					}
				} else {
					if !ok {
						t.Errorf("sni %q: expected backend %q, got none", sni, wantBackend)
					} else if gotBackend != wantBackend {
						t.Errorf("sni %q: expected backend %q, got %q", sni, wantBackend, gotBackend)
					}
				}
			}
		})
	}
}
