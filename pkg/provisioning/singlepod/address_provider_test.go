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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestSinglePodAddressProvider_GatewayAddresses(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	ctx := t.Context()
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-gw",
			Namespace: "my-ns",
			UID:       types.UID("12345"),
		},
		Spec: gatewayv1.GatewaySpec{
			Infrastructure: &gatewayv1.GatewayInfrastructure{
				Labels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{
					"custom-label": "custom-val",
				},
				Annotations: map[gatewayv1.AnnotationKey]gatewayv1.AnnotationValue{
					"custom-anno": "custom-val",
				},
			},
			Listeners: []gatewayv1.Listener{
				{
					Name:     "http",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
				},
				{
					Name:     "https",
					Port:     443,
					Protocol: gatewayv1.HTTPSProtocolType,
				},
			},
		},
	}

	effectiveListeners := []*state.EffectiveListener{
		{
			Name:     "http",
			Port:     80,
			Protocol: gatewayv1.HTTPProtocolType,
			Owner:    state.ListenerOwner{Kind: "Gateway", Namespace: "my-ns", Name: "my-gw"},
		},
		{
			Name:     "https",
			Port:     443,
			Protocol: gatewayv1.HTTPSProtocolType,
			Owner:    state.ListenerOwner{Kind: "Gateway", Namespace: "my-ns", Name: "my-gw"},
		},
		// ListenerSet listener on port 8080
		{
			Name:     "ls-http",
			Port:     8080,
			Protocol: gatewayv1.HTTPProtocolType,
			Owner:    state.ListenerOwner{Kind: "ListenerSet", Namespace: "my-ns", Name: "my-ls"},
		},
	}

	p := NewAddressProvider(client, WithEnableH2C(true))

	// 1. Initial reconcile creates SA, Deployment and Service in my-ns, but no Ingress and replicas not ready yet
	addrs, ready, err := p.GatewayAddresses(ctx, gw, effectiveListeners)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(addrs) != 0 {
		t.Fatalf("expected 0 addresses before LB ingress is assigned, got %d", len(addrs))
	}
	if ready {
		t.Fatalf("expected ready=false before deployment replicas become available")
	}

	// Verify Service was created in my-ns with all effective listener ports (including ListenerSet port 8080)
	name := ResourceNameForGateway("my-gw")
	var svc corev1.Service
	if err := client.Get(ctx, types.NamespacedName{Namespace: "my-ns", Name: name}, &svc); err != nil {
		t.Fatalf("failed to get created Service: %v", err)
	}
	if svc.Spec.Type != corev1.ServiceTypeLoadBalancer {
		t.Errorf("expected Service type LoadBalancer, got %s", svc.Spec.Type)
	}
	if svc.Labels["custom-label"] != "custom-val" {
		t.Errorf("expected custom-label on Service, got %s", svc.Labels["custom-label"])
	}
	if svc.Annotations["custom-anno"] != "custom-val" {
		t.Errorf("expected custom-anno on Service, got %s", svc.Annotations["custom-anno"])
	}
	if len(svc.OwnerReferences) == 0 || svc.OwnerReferences[0].Name != "my-gw" {
		t.Errorf("expected ownerReference pointing to my-gw, got %+v", svc.OwnerReferences)
	}
	// Expected ports: 80 (HTTP), 443 (HTTPS TCP), 443 (HTTP3 UDP), 8080 (HTTP) -> 4 ports
	if len(svc.Spec.Ports) != 4 {
		t.Fatalf("expected 4 service ports, got %d: %+v", len(svc.Spec.Ports), svc.Spec.Ports)
	}

	// Verify ServiceAccount was created in my-ns
	var sa corev1.ServiceAccount
	if err := client.Get(ctx, types.NamespacedName{Namespace: "my-ns", Name: name}, &sa); err != nil {
		t.Fatalf("failed to get created ServiceAccount: %v", err)
	}
	if sa.Labels["custom-label"] != "custom-val" {
		t.Errorf("expected custom-label on ServiceAccount, got %s", sa.Labels["custom-label"])
	}

	// Verify Deployment was created in my-ns
	var deploy appsv1.Deployment
	if err := client.Get(ctx, types.NamespacedName{Namespace: "my-ns", Name: name}, &deploy); err != nil {
		t.Fatalf("failed to get created Deployment: %v", err)
	}
	if len(deploy.Spec.Template.Spec.Containers) == 0 {
		t.Fatalf("expected container in deployment")
	}
	if deploy.Spec.Template.Spec.Containers[0].ReadinessProbe == nil {
		t.Fatalf("expected readiness probe on container")
	}
	if deploy.Spec.Template.Spec.ServiceAccountName != name {
		t.Errorf("expected ServiceAccountName %s, got %s", name, deploy.Spec.Template.Spec.ServiceAccountName)
	}

	// 2. Simulate MetalLB assigning LB Ingress IP and Deployment replicas becoming available
	svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{
		{IP: "172.18.255.201"},
	}
	if err := client.Status().Update(ctx, &svc); err != nil {
		t.Fatalf("failed to update service status: %v", err)
	}

	deploy.Status.AvailableReplicas = 1
	if err := client.Status().Update(ctx, &deploy); err != nil {
		t.Fatalf("failed to update deploy status: %v", err)
	}

	// Reconcile again -> should return the assigned LB address and ready=true
	addrs, ready, err = p.GatewayAddresses(ctx, gw, effectiveListeners)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ready {
		t.Fatalf("expected ready=true after replicas available")
	}
	if len(addrs) != 1 || addrs[0].Value != "172.18.255.201" || state.ValueOf(addrs[0].Type) != gatewayv1.IPAddressType {
		t.Fatalf("expected IP 172.18.255.201, got %+v", addrs)
	}

	// 3. Delete Gateway -> cleans up Service, Deployment, and ServiceAccount
	gwKey := types.NamespacedName{Namespace: "my-ns", Name: "my-gw"}
	if err := p.OnGatewayDeleted(ctx, gwKey); err != nil {
		t.Fatalf("OnGatewayDeleted failed: %v", err)
	}
	var deletedSvc corev1.Service
	err = client.Get(ctx, types.NamespacedName{Namespace: "my-ns", Name: name}, &deletedSvc)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected Service to be deleted, got err: %v", err)
	}
	var deletedDeploy appsv1.Deployment
	err = client.Get(ctx, types.NamespacedName{Namespace: "my-ns", Name: name}, &deletedDeploy)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected Deployment to be deleted, got err: %v", err)
	}
	var deletedSA corev1.ServiceAccount
	err = client.Get(ctx, types.NamespacedName{Namespace: "my-ns", Name: name}, &deletedSA)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected ServiceAccount to be deleted, got err: %v", err)
	}
}

func TestSinglePodAddressProvider_SweepOrphans(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

	orphanName := ResourceNameForGateway("deleted-gw")
	orphanSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      orphanName,
			Namespace: "test-ns",
			Labels: map[string]string{
				LabelGatewayNamespace: "test-ns",
				LabelGatewayName:      "deleted-gw",
				LabelManagedBy:        ManagedByValue,
			},
		},
	}
	orphanDeploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      orphanName,
			Namespace: "test-ns",
			Labels: map[string]string{
				LabelGatewayNamespace: "test-ns",
				LabelGatewayName:      "deleted-gw",
				LabelManagedBy:        ManagedByValue,
			},
		},
	}
	orphanSA := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      orphanName,
			Namespace: "test-ns",
			Labels: map[string]string{
				LabelGatewayNamespace: "test-ns",
				LabelGatewayName:      "deleted-gw",
				LabelManagedBy:        ManagedByValue,
			},
		},
	}

	activeGw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "active-gw",
			Namespace: "test-ns",
		},
	}
	activeName := ResourceNameForGateway("active-gw")
	activeSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      activeName,
			Namespace: "test-ns",
			Labels: map[string]string{
				LabelGatewayNamespace: "test-ns",
				LabelGatewayName:      "active-gw",
				LabelManagedBy:        ManagedByValue,
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(orphanSvc, orphanDeploy, orphanSA, activeGw, activeSvc).
		Build()

	ctx := t.Context()
	p := NewAddressProvider(client)

	if err := p.SweepOrphans(ctx); err != nil {
		t.Fatalf("SweepOrphans failed: %v", err)
	}

	// Orphan resources should be deleted
	var checkSvc corev1.Service
	if err := client.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: orphanName}, &checkSvc); !apierrors.IsNotFound(err) {
		t.Errorf("expected orphan Service to be deleted, got err: %v", err)
	}
	var checkDeploy appsv1.Deployment
	if err := client.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: orphanName}, &checkDeploy); !apierrors.IsNotFound(err) {
		t.Errorf("expected orphan Deployment to be deleted, got err: %v", err)
	}
	var checkSA corev1.ServiceAccount
	if err := client.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: orphanName}, &checkSA); !apierrors.IsNotFound(err) {
		t.Errorf("expected orphan ServiceAccount to be deleted, got err: %v", err)
	}

	// Active gateway resources should remain
	if err := client.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: activeName}, &checkSvc); err != nil {
		t.Errorf("expected active Service to remain, got err: %v", err)
	}
}

func TestSinglePodAddressProvider_Watches(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

	p := NewAddressProvider(nil)
	_ = p

	// Managed service
	managedSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ResourceNameForGateway("gw-1"),
			Namespace: "custom-ns",
			Labels: map[string]string{
				LabelGatewayNamespace: "custom-ns",
				LabelGatewayName:      "gw-1",
				LabelManagedBy:        ManagedByValue,
			},
		},
	}
	// Non-managed service
	unmanagedSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-service",
			Namespace: "custom-ns",
		},
	}

	mapFunc := func(ctx context.Context, obj client.Object) []ctrl.Request {
		labels := obj.GetLabels()
		if labels == nil || labels[LabelManagedBy] != ManagedByValue {
			return nil
		}
		gwName := labels[LabelGatewayName]
		gwNs := labels[LabelGatewayNamespace]
		if gwName == "" || gwNs == "" {
			return nil
		}
		return []ctrl.Request{
			{
				NamespacedName: types.NamespacedName{
					Namespace: gwNs,
					Name:      gwName,
				},
			},
		}
	}

	ctx := t.Context()
	reqs := mapFunc(ctx, managedSvc)
	if len(reqs) != 1 || reqs[0].NamespacedName != (types.NamespacedName{Namespace: "custom-ns", Name: "gw-1"}) {
		t.Errorf("expected 1 request for custom-ns/gw-1, got %+v", reqs)
	}

	reqsOther := mapFunc(ctx, unmanagedSvc)
	if len(reqsOther) != 0 {
		t.Errorf("expected 0 requests for unmanaged service, got %+v", reqsOther)
	}
}
