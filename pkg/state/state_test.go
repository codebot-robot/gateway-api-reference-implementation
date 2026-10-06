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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

type testQueue struct {
	workqueue.TypedRateLimitingInterface[reconcile.Request]
}

func newTestQueue() *testQueue {
	return &testQueue{
		TypedRateLimitingInterface: workqueue.NewTypedRateLimitingQueue[reconcile.Request](workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]()),
	}
}

func (q *testQueue) drain() []types.NamespacedName {
	var res []types.NamespacedName
	for q.Len() > 0 {
		item, _ := q.Get()
		res = append(res, item.NamespacedName)
		q.Done(item)
	}
	return res
}

func TestComputeOutputs_UnchangedInputsProduceNoEvents(t *testing.T) {
	st := NewState()
	st.SetControllerName("example.net/gateway-controller")

	qGC := newTestQueue()
	qGW := newTestQueue()
	qRoute := newTestQueue()

	ctx := t.Context()
	_ = st.GatewayClassSource().Start(ctx, qGC)
	_ = st.GatewaySource().Start(ctx, qGW)
	_ = st.HTTPRouteSource().Start(ctx, qRoute)

	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "my-class"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "example.net/gateway-controller"},
	}
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "my-gw"},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "my-class",
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}
	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "my-route"},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{Name: "my-gw"},
				},
			},
		},
	}

	st.UpsertGatewayClass(gc)
	st.UpsertGateway(gw)
	st.UpsertHTTPRoute(route)
	st.Recompute()

	// Initial drain
	qGC.drain()
	qGW.drain()
	qRoute.drain()

	// Recompute with unchanged inputs
	st.Recompute()

	gcEvents := qGC.drain()
	gwEvents := qGW.drain()
	routeEvents := qRoute.drain()

	if len(gcEvents) != 0 {
		t.Errorf("expected 0 GatewayClass events for unchanged inputs, got %d", len(gcEvents))
	}
	if len(gwEvents) != 0 {
		t.Errorf("expected 0 Gateway events for unchanged inputs, got %d", len(gwEvents))
	}
	if len(routeEvents) != 0 {
		t.Errorf("expected 0 HTTPRoute events for unchanged inputs, got %d", len(routeEvents))
	}

	// Calling Upsert with identical object should not bump revision
	revBefore := st.Revision()
	st.UpsertGateway(gw)
	st.UpsertHTTPRoute(route)
	if st.Revision() != revBefore {
		t.Errorf("expected revision unchanged on duplicate upsert: before=%d, after=%d", revBefore, st.Revision())
	}
}

func TestComputeOutputs_SingleChangeProducesEventsOnlyForAffectedObjects(t *testing.T) {
	st := NewState()
	st.SetControllerName("example.net/gateway-controller")

	qGC := newTestQueue()
	qGW := newTestQueue()

	ctx := t.Context()
	_ = st.GatewayClassSource().Start(ctx, qGC)
	_ = st.GatewaySource().Start(ctx, qGW)

	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "my-class"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "example.net/gateway-controller"},
	}
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "my-gw"},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "my-class",
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}
	gw2 := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "other-gw"},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "my-class",
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 8080, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}

	st.UpsertGatewayClass(gc)
	st.UpsertGateway(gw)
	st.UpsertGateway(gw2)
	st.Recompute()

	qGW.drain()
	qGC.drain()

	// Change only gw2 addresses
	st.SetGatewayAddresses(types.NamespacedName{Namespace: "default", Name: "other-gw"}, []gatewayv1.GatewayStatusAddress{
		{Value: "10.0.0.2"},
	})
	st.Recompute()

	gwEvents := qGW.drain()
	if len(gwEvents) != 1 {
		t.Fatalf("expected exactly 1 Gateway event for other-gw, got %d (%v)", len(gwEvents), gwEvents)
	}
	if gwEvents[0].Name != "other-gw" {
		t.Errorf("expected event for other-gw, got %s", gwEvents[0].Name)
	}

	gcEvents := qGC.drain()
	if len(gcEvents) != 0 {
		t.Errorf("expected 0 GatewayClass events when Gateway addresses change, got %d", len(gcEvents))
	}
}

func TestEventSource_BuffersEventsBeforeStart(t *testing.T) {
	source := NewEventSource()

	req1 := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "gw-1"}}
	req2 := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "gw-2"}}

	// Enqueue before Start
	source.Enqueue(req1)
	source.Enqueue(req2)
	source.Enqueue(req1) // duplicate should dedupe in set

	q := newTestQueue()
	ctx := t.Context()
	if err := source.Start(ctx, q); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	items := q.drain()
	if len(items) != 2 {
		t.Fatalf("expected 2 buffered items flushed into workqueue, got %d (%v)", len(items), items)
	}
}

func TestState_NamespaceLabelChangeDependencyWithoutMapping(t *testing.T) {
	st := NewState()
	st.SetControllerName("example.net/gateway-controller")

	qRoute := newTestQueue()
	ctx := t.Context()
	_ = st.HTTPRouteSource().Start(ctx, qRoute)

	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "my-class"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "example.net/gateway-controller"},
	}

	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "infra-ns", Name: "prod-gw"},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "my-class",
			Listeners: []gatewayv1.Listener{
				{
					Name:     "http",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
					AllowedRoutes: &gatewayv1.AllowedRoutes{
						Namespaces: &gatewayv1.RouteNamespaces{
							From: Ptr(gatewayv1.NamespacesFromSelector),
							Selector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"env": "prod"},
							},
						},
					},
				},
			},
		},
	}

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "app-ns",
			Labels: map[string]string{"env": "dev"},
		},
	}

	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app-ns", Name: "app-route"},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{
						Namespace: Ptr(gatewayv1.Namespace("infra-ns")),
						Name:      "prod-gw",
					},
				},
			},
		},
	}

	st.UpsertGatewayClass(gc)
	st.UpsertNamespace(ns)
	st.UpsertGateway(gw)
	st.UpsertHTTPRoute(route)
	st.Recompute()

	// Route should initially be not accepted because namespace label is env: dev
	routeStatus, ok := st.GetDesiredHTTPRouteStatus(types.NamespacedName{Namespace: "app-ns", Name: "app-route"})
	if !ok || len(routeStatus.Parents) != 1 {
		t.Fatalf("expected route status present, got %+v", routeStatus)
	}
	if routeStatus.Parents[0].Conditions[0].Status != metav1.ConditionFalse {
		t.Fatalf("expected route initially Accepted=False, got %v", routeStatus.Parents[0].Conditions[0])
	}

	qRoute.drain()

	// Mutate namespace label to env: prod
	nsUpdated := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "app-ns",
			Labels: map[string]string{"env": "prod"},
		},
	}
	st.UpsertNamespace(nsUpdated)
	st.Recompute()

	// Central recomputation should automatically detect route is now accepted and emit an event!
	routeEvents := qRoute.drain()
	if len(routeEvents) != 1 {
		t.Fatalf("expected 1 HTTPRoute event on namespace label change, got %d", len(routeEvents))
	}
	if routeEvents[0].Namespace != "app-ns" || routeEvents[0].Name != "app-route" {
		t.Errorf("expected event for app-ns/app-route, got %v", routeEvents[0])
	}

	routeStatusAfter, _ := st.GetDesiredHTTPRouteStatus(types.NamespacedName{Namespace: "app-ns", Name: "app-route"})
	if routeStatusAfter.Parents[0].Conditions[0].Status != metav1.ConditionTrue {
		t.Errorf("expected route Accepted=True after namespace label change, got %v", routeStatusAfter.Parents[0].Conditions[0])
	}
}

func TestState_ReferenceGrantDeletionDependencyWithoutMapping(t *testing.T) {
	st := NewState()
	st.SetControllerName("example.net/gateway-controller")

	qRoute := newTestQueue()
	ctx := t.Context()
	_ = st.HTTPRouteSource().Start(ctx, qRoute)

	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "my-class"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "example.net/gateway-controller"},
	}

	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "my-gw"},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "my-class",
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "backend-ns", Name: "my-svc"},
	}

	rg := &gatewayv1beta1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: "backend-ns", Name: "allow-route"},
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
					Name:  Ptr(gatewayv1.ObjectName("my-svc")),
				},
			},
		},
	}

	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "cross-ns-route"},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{Name: "my-gw"},
				},
			},
			Rules: []gatewayv1.HTTPRouteRule{
				{
					BackendRefs: []gatewayv1.HTTPBackendRef{
						{
							BackendRef: gatewayv1.BackendRef{
								BackendObjectReference: gatewayv1.BackendObjectReference{
									Namespace: Ptr(gatewayv1.Namespace("backend-ns")),
									Name:      "my-svc",
								},
							},
						},
					},
				},
			},
		},
	}

	st.UpsertGatewayClass(gc)
	st.UpsertGateway(gw)
	st.UpsertService(svc)
	st.UpsertReferenceGrant(rg)
	st.UpsertHTTPRoute(route)
	st.Recompute()

	// Initially ResolvedRefs should be True
	statusBefore, ok := st.GetDesiredHTTPRouteStatus(types.NamespacedName{Namespace: "default", Name: "cross-ns-route"})
	if !ok || len(statusBefore.Parents) != 1 {
		t.Fatalf("expected route status present")
	}
	if statusBefore.Parents[0].Conditions[1].Status != metav1.ConditionTrue {
		t.Fatalf("expected ResolvedRefs=True, got %v", statusBefore.Parents[0].Conditions[1])
	}

	qRoute.drain()

	// Delete ReferenceGrant
	st.DeleteReferenceGrant(types.NamespacedName{Namespace: "backend-ns", Name: "allow-route"})
	st.Recompute()

	// Central recomputation should detect ResolvedRefs became False and emit an event
	events := qRoute.drain()
	if len(events) != 1 {
		t.Fatalf("expected 1 HTTPRoute event on ReferenceGrant deletion, got %d", len(events))
	}
	if events[0].Name != "cross-ns-route" {
		t.Errorf("expected event for cross-ns-route, got %s", events[0].Name)
	}

	statusAfter, _ := st.GetDesiredHTTPRouteStatus(types.NamespacedName{Namespace: "default", Name: "cross-ns-route"})
	if statusAfter.Parents[0].Conditions[1].Status != metav1.ConditionFalse ||
		statusAfter.Parents[0].Conditions[1].Reason != string(gatewayv1.RouteReasonRefNotPermitted) {
		t.Errorf("expected ResolvedRefs=False/RefNotPermitted, got %v", statusAfter.Parents[0].Conditions[1])
	}
}

func TestState_ConfigMapChangeBackendTLSPolicyDependencyWithoutMapping(t *testing.T) {
	st := NewState()
	st.SetControllerName("example.net/gateway-controller")

	qPolicy := newTestQueue()
	ctx := t.Context()
	_ = st.BackendTLSPolicySource().Start(ctx, qPolicy)

	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "my-class"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "example.net/gateway-controller"},
	}

	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "my-gw"},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "my-class",
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "secure-svc"},
	}
	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "route"},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{{Name: "my-gw"}},
			},
			Rules: []gatewayv1.HTTPRouteRule{
				{
					BackendRefs: []gatewayv1.HTTPBackendRef{
						{
							BackendRef: gatewayv1.BackendRef{
								BackendObjectReference: gatewayv1.BackendObjectReference{Name: "secure-svc"},
							},
						},
					},
				},
			},
		},
	}

	validCertPEM := "-----BEGIN CERTIFICATE-----\nMIIBkDCB+aADAgECAgEBMA0GCSqGSIb3DQEBCwUAMBMxETAPBgNVBAMMCHNlcnZp\nY2UwHhcNMjYwMTAxMDAwMDAwWhcNMjcwMTAxMDAwMDAwWjATMREwDwYDVQQDDAhz\nZXJ2aWNlMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC3/1234567890ABCDEF\nMA0GCSqGSIb3DQEBCwUAA4GBACb1234567890ABCDEF\n-----END CERTIFICATE-----"
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "ca-cert-cm"},
		Data: map[string]string{
			"ca.crt": validCertPEM,
		},
	}

	policy := &gatewayv1.BackendTLSPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "tls-policy"},
		Spec: gatewayv1.BackendTLSPolicySpec{
			TargetRefs: []gatewayv1.LocalPolicyTargetReferenceWithSectionName{
				{
					LocalPolicyTargetReference: gatewayv1.LocalPolicyTargetReference{
						Group: "",
						Kind:  "Service",
						Name:  "secure-svc",
					},
				},
			},
			Validation: gatewayv1.BackendTLSPolicyValidation{
				CACertificateRefs: []gatewayv1.LocalObjectReference{
					{
						Group: "",
						Kind:  "ConfigMap",
						Name:  "ca-cert-cm",
					},
				},
				Hostname: "secure.example.com",
			},
		},
	}

	st.UpsertGatewayClass(gc)
	st.UpsertGateway(gw)
	st.UpsertService(svc)
	st.UpsertHTTPRoute(route)
	st.UpsertConfigMap(cm)
	st.UpsertBackendTLSPolicy(policy)
	st.Recompute()

	// Policy should be Accepted=True
	policyStatus, ok := st.GetDesiredBackendTLSPolicyStatus(types.NamespacedName{Namespace: "default", Name: "tls-policy"})
	if !ok || len(policyStatus.Ancestors) != 1 {
		t.Fatalf("expected policy status with 1 ancestor, got %+v", policyStatus)
	}
	if policyStatus.Ancestors[0].Conditions[0].Status != metav1.ConditionTrue {
		t.Fatalf("expected policy Accepted=True, got %v", policyStatus.Ancestors[0].Conditions[0])
	}

	qPolicy.drain()

	// Update ConfigMap with invalid data
	cmInvalid := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "ca-cert-cm"},
		Data: map[string]string{
			"ca.crt": "invalid cert data",
		},
	}
	st.UpsertConfigMap(cmInvalid)
	st.Recompute()

	// Central recomputation should detect policy Accepted=False and emit an event
	events := qPolicy.drain()
	if len(events) != 1 {
		t.Fatalf("expected 1 BackendTLSPolicy event on ConfigMap change, got %d", len(events))
	}
	if events[0].Name != "tls-policy" {
		t.Errorf("expected event for tls-policy, got %s", events[0].Name)
	}

	policyStatusAfter, _ := st.GetDesiredBackendTLSPolicyStatus(types.NamespacedName{Namespace: "default", Name: "tls-policy"})
	if policyStatusAfter.Ancestors[0].Conditions[0].Status != metav1.ConditionFalse {
		t.Errorf("expected policy Accepted=False after ConfigMap corruption, got %v", policyStatusAfter.Ancestors[0].Conditions[0])
	}
}

func TestMergeHTTPRouteStatus_DeterministicOrderAndOtherControllers(t *testing.T) {
	myCtrl := gatewayv1.GatewayController("example.net/gateway-controller")
	otherCtrl := gatewayv1.GatewayController("other.org/other-controller")

	t0 := metav1.NewTime(time.Now().Add(-5 * time.Minute))
	c := []metav1.Condition{
		{
			Type:               "Accepted",
			Status:             metav1.ConditionTrue,
			Reason:             "Accepted",
			Message:            "Accepted",
			ObservedGeneration: 1,
			LastTransitionTime: t0,
		},
	}

	current := &gatewayv1.HTTPRouteStatus{
		Parents: []gatewayv1.RouteParentStatus{
			{ParentRef: gatewayv1.ParentReference{Name: "gw-other"}, ControllerName: otherCtrl, Conditions: c},
			{ParentRef: gatewayv1.ParentReference{Name: "gw-b"}, ControllerName: myCtrl, Conditions: c},
			{ParentRef: gatewayv1.ParentReference{Name: "gw-a"}, ControllerName: myCtrl, Conditions: c},
		},
	}

	// Desired parents for our controller in unsorted order
	desired := gatewayv1.HTTPRouteStatus{
		Parents: []gatewayv1.RouteParentStatus{
			{ParentRef: gatewayv1.ParentReference{Name: "gw-b"}, ControllerName: myCtrl, Conditions: c},
			{ParentRef: gatewayv1.ParentReference{Name: "gw-a"}, ControllerName: myCtrl, Conditions: c},
		},
	}

	updated := MergeHTTPRouteStatus(current, desired, "default", myCtrl)
	if updated {
		t.Errorf("expected updated=false when only ordering of existing entries differs")
	}

	// Current parents should now be deterministically sorted: gw-a, gw-b, then gw-other
	if len(current.Parents) != 3 {
		t.Fatalf("expected 3 parents, got %d", len(current.Parents))
	}
	if current.Parents[0].ParentRef.Name != "gw-a" || current.Parents[0].ControllerName != myCtrl {
		t.Errorf("expected current.Parents[0] to be myCtrl/gw-a, got %+v", current.Parents[0])
	}
	if current.Parents[1].ParentRef.Name != "gw-b" || current.Parents[1].ControllerName != myCtrl {
		t.Errorf("expected current.Parents[1] to be myCtrl/gw-b, got %+v", current.Parents[1])
	}
	if current.Parents[2].ParentRef.Name != "gw-other" || current.Parents[2].ControllerName != otherCtrl {
		t.Errorf("expected current.Parents[2] to be otherCtrl/gw-other, got %+v", current.Parents[2])
	}
}
