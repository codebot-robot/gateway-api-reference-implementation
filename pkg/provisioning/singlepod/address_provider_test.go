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

package singlepod

import (
	"context"
	"testing"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestSinglePodAddressProvider_GatewayAddresses(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

	svcWithIngress := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gari-proxy",
			Namespace: "default",
		},
		Status: corev1.ServiceStatus{
			LoadBalancer: corev1.LoadBalancerStatus{
				Ingress: []corev1.LoadBalancerIngress{
					{IP: "192.0.2.1"},
					{Hostname: "proxy.example.com"},
				},
			},
		},
	}

	svcNoIngress := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "empty-proxy",
			Namespace: "default",
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(svcWithIngress, svcNoIngress).
		Build()

	ctx := t.Context()
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-gw",
			Namespace: "default",
		},
	}

	// 1. Service with IP and Hostname
	p1 := NewAddressProvider(client, "default", "gari-proxy")
	addrs, err := p1.GatewayAddresses(ctx, gw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(addrs) != 2 {
		t.Fatalf("expected 2 addresses, got %d", len(addrs))
	}
	if state.ValueOf(addrs[0].Type) != gatewayv1.IPAddressType || addrs[0].Value != "192.0.2.1" {
		t.Errorf("expected IP 192.0.2.1, got %+v", addrs[0])
	}
	if state.ValueOf(addrs[1].Type) != gatewayv1.HostnameAddressType || addrs[1].Value != "proxy.example.com" {
		t.Errorf("expected Hostname proxy.example.com, got %+v", addrs[1])
	}

	// 2. Service with no Ingress
	p2 := NewAddressProvider(client, "default", "empty-proxy")
	addrs, err = p2.GatewayAddresses(ctx, gw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(addrs) != 0 {
		t.Fatalf("expected 0 addresses, got %d", len(addrs))
	}

	// 3. Service not found
	p3 := NewAddressProvider(client, "default", "non-existent")
	addrs, err = p3.GatewayAddresses(ctx, gw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(addrs) != 0 {
		t.Fatalf("expected 0 addresses, got %d", len(addrs))
	}

	// 4. Nil client
	p4 := NewAddressProvider(nil, "default", "gari-proxy")
	addrs, err = p4.GatewayAddresses(ctx, gw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(addrs) != 0 {
		t.Fatalf("expected 0 addresses, got %d", len(addrs))
	}
}

func TestSinglePodAddressProvider_Watches(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gw-1",
			Namespace: "default",
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(gw).
		Build()

	p := NewAddressProvider(client, "default", "gari-proxy")

	ctx := t.Context()

	// Matching service
	matchingSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gari-proxy",
			Namespace: "default",
		},
	}
	// Non-matching service
	otherSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-service",
			Namespace: "default",
		},
	}

	// Helper to simulate the map func logic inside SetupWatches
	mapFunc := func(ctx context.Context, obj *corev1.Service) []ctrl.Request {
		if obj.Name == p.name && obj.Namespace == p.namespace {
			var gwList gatewayv1.GatewayList
			if err := p.client.List(ctx, &gwList); err == nil {
				var requests []ctrl.Request
				for _, g := range gwList.Items {
					requests = append(requests, ctrl.Request{
						NamespacedName: types.NamespacedName{
							Namespace: g.Namespace,
							Name:      g.Name,
						},
					})
				}
				return requests
			}
		}
		return nil
	}

	reqsMatching := mapFunc(ctx, matchingSvc)
	if len(reqsMatching) != 1 || reqsMatching[0].NamespacedName != (types.NamespacedName{Namespace: "default", Name: "gw-1"}) {
		t.Errorf("expected 1 request for default/gw-1, got %+v", reqsMatching)
	}

	reqsOther := mapFunc(ctx, otherSvc)
	if len(reqsOther) != 0 {
		t.Errorf("expected 0 requests for other service, got %+v", reqsOther)
	}
}
