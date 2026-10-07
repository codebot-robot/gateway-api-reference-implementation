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
	"strings"
	"sync"
	"testing"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
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

	// Create initial shared gari-dataplane ClusterRoleBinding
	sharedCRB := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: DataplaneClusterRoleBindingName,
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     DataplaneClusterRoleName,
		},
		Subjects: []rbacv1.Subject{},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sharedCRB).
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
					// User label with reserved prefix should be ignored
					"gateway.networking.k8s.io/gateway-name": "malicious-name",
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

	// 1. Initial reconcile creates SA, adds to CRB, creates Deployment and Service in my-ns, but no Ingress and replicas not ready yet
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
	if svc.Labels[LabelGatewayName] != "my-gw" {
		t.Errorf("expected LabelGatewayName to not be overridden by user label, got %s", svc.Labels[LabelGatewayName])
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

	// Verify subject was added to shared ClusterRoleBinding
	var crb rbacv1.ClusterRoleBinding
	if err := client.Get(ctx, types.NamespacedName{Name: DataplaneClusterRoleBindingName}, &crb); err != nil {
		t.Fatalf("failed to get shared ClusterRoleBinding: %v", err)
	}
	if len(crb.Subjects) != 1 || crb.Subjects[0].Name != name || crb.Subjects[0].Namespace != "my-ns" {
		t.Errorf("expected CRB subject to be %s in my-ns, got %+v", name, crb.Subjects)
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
	initialHash := deploy.Annotations[AnnotationTemplateHash]
	if initialHash == "" {
		t.Errorf("expected template hash annotation on Deployment")
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

	// 3. Update Gateway infrastructure labels -> should update template hash and pod template
	gw.Spec.Infrastructure.Labels["updated-label"] = "updated-val"
	_, _, err = p.GatewayAddresses(ctx, gw, effectiveListeners)
	if err != nil {
		t.Fatalf("failed to update gateway with new infrastructure label: %v", err)
	}
	if err := client.Get(ctx, types.NamespacedName{Namespace: "my-ns", Name: name}, &deploy); err != nil {
		t.Fatalf("failed to get updated deployment: %v", err)
	}
	if deploy.Annotations[AnnotationTemplateHash] == initialHash {
		t.Errorf("expected template hash to change when infrastructure labels change")
	}
	if deploy.Spec.Template.Labels["updated-label"] != "updated-val" {
		t.Errorf("expected updated pod template labels to include updated-label")
	}

	// 4. Delete Gateway -> cleans up Service, Deployment, ServiceAccount, and removes CRB subject
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
	_ = client.Get(ctx, types.NamespacedName{Name: DataplaneClusterRoleBindingName}, &crb)
	if len(crb.Subjects) != 0 {
		t.Errorf("expected CRB subjects to be empty after Gateway deletion, got %+v", crb.Subjects)
	}
}

func TestSinglePodAddressProvider_ClusterRoleBindingSubjects(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

	sharedCRB := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: DataplaneClusterRoleBindingName,
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     DataplaneClusterRoleName,
		},
		Subjects: []rbacv1.Subject{},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sharedCRB).
		Build()

	p := NewAddressProvider(client)
	ctx := t.Context()

	gw1 := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw-1", Namespace: "ns-1"},
		Spec:       gatewayv1.GatewaySpec{Listeners: []gatewayv1.Listener{{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType}}},
	}
	gw2 := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw-2", Namespace: "ns-2"},
		Spec:       gatewayv1.GatewaySpec{Listeners: []gatewayv1.Listener{{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType}}},
	}

	// Add two gateways concurrently
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _, _ = p.GatewayAddresses(ctx, gw1, nil)
	}()
	go func() {
		defer wg.Done()
		_, _, _ = p.GatewayAddresses(ctx, gw2, nil)
	}()
	wg.Wait()

	var crb rbacv1.ClusterRoleBinding
	if err := client.Get(ctx, types.NamespacedName{Name: DataplaneClusterRoleBindingName}, &crb); err != nil {
		t.Fatalf("failed to get CRB: %v", err)
	}
	if len(crb.Subjects) != 2 {
		t.Fatalf("expected 2 subjects in CRB, got %d: %+v", len(crb.Subjects), crb.Subjects)
	}

	// Delete gw1
	if err := p.OnGatewayDeleted(ctx, types.NamespacedName{Namespace: "ns-1", Name: "gw-1"}); err != nil {
		t.Fatalf("failed to delete gw1: %v", err)
	}

	_ = client.Get(ctx, types.NamespacedName{Name: DataplaneClusterRoleBindingName}, &crb)
	if len(crb.Subjects) != 1 || crb.Subjects[0].Namespace != "ns-2" || crb.Subjects[0].Name != "gw-2-gari" {
		t.Fatalf("expected only gw-2 in CRB, got %+v", crb.Subjects)
	}
}

func TestSinglePodAddressProvider_OwnershipConflict(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

	// Existing service not created by GARI
	foreignSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ResourceNameForGateway("conflict-gw"),
			Namespace: "default",
		},
	}
	sharedCRB := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: DataplaneClusterRoleBindingName,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(foreignSvc, sharedCRB).
		Build()

	ctx := t.Context()
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "conflict-gw",
			Namespace: "default",
			UID:       types.UID("99999"),
		},
		Spec: gatewayv1.GatewaySpec{
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}

	p := NewAddressProvider(client)

	// GatewayAddresses should fail with conflict error and not overwrite the foreign service
	_, _, err := p.GatewayAddresses(ctx, gw, nil)
	if err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("expected conflict error, got: %v", err)
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
	sharedCRB := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: DataplaneClusterRoleBindingName,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Namespace: "test-ns",
				Name:      orphanName,
			},
			{
				Kind:      "ServiceAccount",
				Namespace: "test-ns",
				Name:      ResourceNameForGateway("active-gw"),
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
	activeSA := &corev1.ServiceAccount{
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
		WithObjects(orphanSvc, orphanDeploy, orphanSA, sharedCRB, activeGw, activeSvc, activeSA).
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
	var checkCRB rbacv1.ClusterRoleBinding
	if err := client.Get(ctx, types.NamespacedName{Name: DataplaneClusterRoleBindingName}, &checkCRB); err != nil {
		t.Fatalf("failed to get CRB: %v", err)
	}
	if len(checkCRB.Subjects) != 1 || checkCRB.Subjects[0].Name != activeName {
		t.Errorf("expected only active-gw in CRB subjects, got %+v", checkCRB.Subjects)
	}

	// Active gateway resources should remain
	if err := client.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: activeName}, &checkSvc); err != nil {
		t.Errorf("expected active Service to remain, got err: %v", err)
	}
}

func TestSinglePodAddressProvider_SweepOrphans_APIReaderServiceAccount(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

	activeName := ResourceNameForGateway("active-gw")
	activeGw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "active-gw",
			Namespace: "test-ns",
		},
	}
	activeSA := &corev1.ServiceAccount{
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
	sharedCRB := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: DataplaneClusterRoleBindingName,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      activeName,
				Namespace: "test-ns",
			},
		},
	}

	// cachedClient does NOT have activeSA (simulating cache lag where SA was created recently)
	cachedClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sharedCRB.DeepCopy(), activeGw).
		Build()

	// apiReader DOES have activeSA (reading from API server directly)
	apiReader := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sharedCRB.DeepCopy(), activeGw, activeSA).
		Build()

	ctx := t.Context()
	p := NewAddressProvider(cachedClient, WithAPIReader(apiReader))

	if err := p.SweepOrphans(ctx); err != nil {
		t.Fatalf("SweepOrphans failed: %v", err)
	}

	// CRB should retain activeSA subject because it exists in the API reader
	var checkCRB rbacv1.ClusterRoleBinding
	if err := cachedClient.Get(ctx, types.NamespacedName{Name: DataplaneClusterRoleBindingName}, &checkCRB); err != nil {
		t.Fatalf("failed to get CRB: %v", err)
	}
	if len(checkCRB.Subjects) != 1 || checkCRB.Subjects[0].Name != activeName {
		t.Errorf("expected active-gw ServiceAccount to be retained in CRB subjects, got %+v", checkCRB.Subjects)
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

func setupTestClient(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

	sharedCRB := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: DataplaneClusterRoleBindingName,
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     DataplaneClusterRoleName,
		},
		Subjects: []rbacv1.Subject{},
	}

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sharedCRB).
		Build()
}

func TestSinglePodAddressProvider_InfrastructureLabelsAndAnnotationsLandOnAllFour(t *testing.T) {
	c := setupTestClient(t)
	ctx := t.Context()
	p := NewAddressProvider(c)

	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-gw",
			Namespace: "test-ns",
			UID:       types.UID("gw-uid-1"),
		},
		Spec: gatewayv1.GatewaySpec{
			Infrastructure: &gatewayv1.GatewayInfrastructure{
				Labels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{
					"example.com/tier": "frontend",
					"custom-label":     "value-1",
				},
				Annotations: map[gatewayv1.AnnotationKey]gatewayv1.AnnotationValue{
					"example.com/cost-center": "12345",
					"custom-anno":             "anno-val-1",
				},
			},
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}

	_, _, err := p.GatewayAddresses(ctx, gw, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resName := ResourceNameForGateway("test-gw")

	// 1. ServiceAccount
	var sa corev1.ServiceAccount
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &sa); err != nil {
		t.Fatalf("failed to get ServiceAccount: %v", err)
	}
	if sa.Labels["example.com/tier"] != "frontend" || sa.Labels["custom-label"] != "value-1" {
		t.Errorf("ServiceAccount missing infrastructure labels: got %+v", sa.Labels)
	}
	if sa.Annotations["example.com/cost-center"] != "12345" || sa.Annotations["custom-anno"] != "anno-val-1" {
		t.Errorf("ServiceAccount missing infrastructure annotations: got %+v", sa.Annotations)
	}

	// 2. Deployment
	var deploy appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &deploy); err != nil {
		t.Fatalf("failed to get Deployment: %v", err)
	}
	if deploy.Labels["example.com/tier"] != "frontend" || deploy.Labels["custom-label"] != "value-1" {
		t.Errorf("Deployment missing infrastructure labels: got %+v", deploy.Labels)
	}
	if deploy.Annotations["example.com/cost-center"] != "12345" || deploy.Annotations["custom-anno"] != "anno-val-1" {
		t.Errorf("Deployment missing infrastructure annotations: got %+v", deploy.Annotations)
	}
	if deploy.Annotations[AnnotationTemplateHash] == "" {
		t.Errorf("Deployment missing template hash annotation")
	}

	// 3. Pod Template
	if deploy.Spec.Template.Labels["example.com/tier"] != "frontend" || deploy.Spec.Template.Labels["custom-label"] != "value-1" {
		t.Errorf("Pod template missing infrastructure labels: got %+v", deploy.Spec.Template.Labels)
	}
	if deploy.Spec.Template.Annotations["example.com/cost-center"] != "12345" || deploy.Spec.Template.Annotations["custom-anno"] != "anno-val-1" {
		t.Errorf("Pod template missing infrastructure annotations: got %+v", deploy.Spec.Template.Annotations)
	}

	// 4. Service
	var svc corev1.Service
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &svc); err != nil {
		t.Fatalf("failed to get Service: %v", err)
	}
	if svc.Labels["example.com/tier"] != "frontend" || svc.Labels["custom-label"] != "value-1" {
		t.Errorf("Service missing infrastructure labels: got %+v", svc.Labels)
	}
	if svc.Annotations["example.com/cost-center"] != "12345" || svc.Annotations["custom-anno"] != "anno-val-1" {
		t.Errorf("Service missing infrastructure annotations: got %+v", svc.Annotations)
	}
}

func TestSinglePodAddressProvider_InfrastructureValueUpdate(t *testing.T) {
	c := setupTestClient(t)
	ctx := t.Context()
	p := NewAddressProvider(c)

	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-gw",
			Namespace: "test-ns",
			UID:       types.UID("gw-uid-1"),
		},
		Spec: gatewayv1.GatewaySpec{
			Infrastructure: &gatewayv1.GatewayInfrastructure{
				Labels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{
					"tier": "frontend-v1",
				},
				Annotations: map[gatewayv1.AnnotationKey]gatewayv1.AnnotationValue{
					"cost-center": "1000",
				},
			},
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}

	_, _, err := p.GatewayAddresses(ctx, gw, nil)
	if err != nil {
		t.Fatalf("unexpected error on initial reconcile: %v", err)
	}

	resName := ResourceNameForGateway("test-gw")
	var initialDeploy appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &initialDeploy); err != nil {
		t.Fatalf("failed to get initial Deployment: %v", err)
	}
	initialHash := initialDeploy.Annotations[AnnotationTemplateHash]

	// Update label and annotation values in Gateway infrastructure
	gw.Spec.Infrastructure.Labels["tier"] = "frontend-v2"
	gw.Spec.Infrastructure.Annotations["cost-center"] = "2000"

	_, _, err = p.GatewayAddresses(ctx, gw, nil)
	if err != nil {
		t.Fatalf("unexpected error on update reconcile: %v", err)
	}

	// 1. ServiceAccount
	var sa corev1.ServiceAccount
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &sa); err != nil {
		t.Fatalf("failed to get ServiceAccount: %v", err)
	}
	if sa.Labels["tier"] != "frontend-v2" {
		t.Errorf("ServiceAccount label not updated: got %s, want frontend-v2", sa.Labels["tier"])
	}
	if sa.Annotations["cost-center"] != "2000" {
		t.Errorf("ServiceAccount annotation not updated: got %s, want 2000", sa.Annotations["cost-center"])
	}

	// 2. Deployment
	var deploy appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &deploy); err != nil {
		t.Fatalf("failed to get Deployment: %v", err)
	}
	if deploy.Labels["tier"] != "frontend-v2" {
		t.Errorf("Deployment label not updated: got %s, want frontend-v2", deploy.Labels["tier"])
	}
	if deploy.Annotations["cost-center"] != "2000" {
		t.Errorf("Deployment annotation not updated: got %s, want 2000", deploy.Annotations["cost-center"])
	}
	if deploy.Annotations[AnnotationTemplateHash] == initialHash {
		t.Errorf("Deployment template hash did not change after updating infrastructure values")
	}

	// 3. Pod Template (ensures pods are rolled)
	if deploy.Spec.Template.Labels["tier"] != "frontend-v2" {
		t.Errorf("Pod template label not updated: got %s, want frontend-v2", deploy.Spec.Template.Labels["tier"])
	}
	if deploy.Spec.Template.Annotations["cost-center"] != "2000" {
		t.Errorf("Pod template annotation not updated: got %s, want 2000", deploy.Spec.Template.Annotations["cost-center"])
	}

	// 4. Service
	var svc corev1.Service
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &svc); err != nil {
		t.Fatalf("failed to get Service: %v", err)
	}
	if svc.Labels["tier"] != "frontend-v2" {
		t.Errorf("Service label not updated: got %s, want frontend-v2", svc.Labels["tier"])
	}
	if svc.Annotations["cost-center"] != "2000" {
		t.Errorf("Service annotation not updated: got %s, want 2000", svc.Annotations["cost-center"])
	}
}

func TestSinglePodAddressProvider_InfrastructureKeyRemoval(t *testing.T) {
	c := setupTestClient(t)
	ctx := t.Context()
	p := NewAddressProvider(c)

	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-gw",
			Namespace: "test-ns",
			UID:       types.UID("gw-uid-1"),
		},
		Spec: gatewayv1.GatewaySpec{
			Infrastructure: &gatewayv1.GatewayInfrastructure{
				Labels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{
					"tier":      "frontend",
					"remove-me": "label-val",
				},
				Annotations: map[gatewayv1.AnnotationKey]gatewayv1.AnnotationValue{
					"cost-center": "1000",
					"remove-me":   "anno-val",
				},
			},
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}

	_, _, err := p.GatewayAddresses(ctx, gw, nil)
	if err != nil {
		t.Fatalf("unexpected error on initial reconcile: %v", err)
	}

	resName := ResourceNameForGateway("test-gw")
	var initialDeploy appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &initialDeploy); err != nil {
		t.Fatalf("failed to get initial Deployment: %v", err)
	}
	initialHash := initialDeploy.Annotations[AnnotationTemplateHash]

	// Remove keys from Gateway infrastructure
	delete(gw.Spec.Infrastructure.Labels, "remove-me")
	delete(gw.Spec.Infrastructure.Annotations, "remove-me")

	_, _, err = p.GatewayAddresses(ctx, gw, nil)
	if err != nil {
		t.Fatalf("unexpected error on removal reconcile: %v", err)
	}

	// 1. ServiceAccount
	var sa corev1.ServiceAccount
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &sa); err != nil {
		t.Fatalf("failed to get ServiceAccount: %v", err)
	}
	if _, ok := sa.Labels["remove-me"]; ok {
		t.Errorf("ServiceAccount still has removed label: %+v", sa.Labels)
	}
	if sa.Labels["tier"] != "frontend" {
		t.Errorf("ServiceAccount lost retained label 'tier'")
	}
	if _, ok := sa.Annotations["remove-me"]; ok {
		t.Errorf("ServiceAccount still has removed annotation: %+v", sa.Annotations)
	}
	if sa.Annotations["cost-center"] != "1000" {
		t.Errorf("ServiceAccount lost retained annotation 'cost-center'")
	}

	// 2. Deployment
	var deploy appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &deploy); err != nil {
		t.Fatalf("failed to get Deployment: %v", err)
	}
	if _, ok := deploy.Labels["remove-me"]; ok {
		t.Errorf("Deployment still has removed label: %+v", deploy.Labels)
	}
	if deploy.Labels["tier"] != "frontend" {
		t.Errorf("Deployment lost retained label 'tier'")
	}
	if _, ok := deploy.Annotations["remove-me"]; ok {
		t.Errorf("Deployment still has removed annotation: %+v", deploy.Annotations)
	}
	if deploy.Annotations["cost-center"] != "1000" {
		t.Errorf("Deployment lost retained annotation 'cost-center'")
	}
	if deploy.Annotations[AnnotationTemplateHash] == initialHash {
		t.Errorf("Deployment template hash did not change after removing infrastructure keys")
	}

	// 3. Pod Template
	if _, ok := deploy.Spec.Template.Labels["remove-me"]; ok {
		t.Errorf("Pod template still has removed label: %+v", deploy.Spec.Template.Labels)
	}
	if deploy.Spec.Template.Labels["tier"] != "frontend" {
		t.Errorf("Pod template lost retained label 'tier'")
	}
	if _, ok := deploy.Spec.Template.Annotations["remove-me"]; ok {
		t.Errorf("Pod template still has removed annotation: %+v", deploy.Spec.Template.Annotations)
	}
	if deploy.Spec.Template.Annotations["cost-center"] != "1000" {
		t.Errorf("Pod template lost retained annotation 'cost-center'")
	}

	// 4. Service
	var svc corev1.Service
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &svc); err != nil {
		t.Fatalf("failed to get Service: %v", err)
	}
	if _, ok := svc.Labels["remove-me"]; ok {
		t.Errorf("Service still has removed label: %+v", svc.Labels)
	}
	if svc.Labels["tier"] != "frontend" {
		t.Errorf("Service lost retained label 'tier'")
	}
	if _, ok := svc.Annotations["remove-me"]; ok {
		t.Errorf("Service still has removed annotation: %+v", svc.Annotations)
	}
	if svc.Annotations["cost-center"] != "1000" {
		t.Errorf("Service lost retained annotation 'cost-center'")
	}
}

func TestSinglePodAddressProvider_InfrastructureUserKeysCannotOverrideReservedLabelsOrSelectors(t *testing.T) {
	c := setupTestClient(t)
	ctx := t.Context()
	p := NewAddressProvider(c)

	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-gw",
			Namespace: "test-ns",
			UID:       types.UID("gw-uid-1"),
		},
		Spec: gatewayv1.GatewaySpec{
			Infrastructure: &gatewayv1.GatewayInfrastructure{
				Labels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{
					"gateway.networking.k8s.io/gateway-name":      "override-name",
					"gateway.networking.k8s.io/gateway-namespace": "override-ns",
					"gateway.networking.k8s.io/custom-reserved":   "malicious-val",
					"app.kubernetes.io/managed-by":                "override-managed-by",
					"app.kubernetes.io/name":                      "override-name",
					"custom-valid-label":                          "valid-val",
				},
			},
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}

	_, _, err := p.GatewayAddresses(ctx, gw, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resName := ResourceNameForGateway("test-gw")

	var sa corev1.ServiceAccount
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &sa); err != nil {
		t.Fatalf("failed to get ServiceAccount: %v", err)
	}
	var deploy appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &deploy); err != nil {
		t.Fatalf("failed to get Deployment: %v", err)
	}
	var svc corev1.Service
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &svc); err != nil {
		t.Fatalf("failed to get Service: %v", err)
	}

	type labelHolder struct {
		name   string
		labels map[string]string
	}
	holders := []labelHolder{
		{name: "ServiceAccount", labels: sa.Labels},
		{name: "Deployment", labels: deploy.Labels},
		{name: "PodTemplate", labels: deploy.Spec.Template.Labels},
		{name: "Service", labels: svc.Labels},
	}

	for _, h := range holders {
		if h.labels[LabelGatewayName] != "test-gw" {
			t.Errorf("%s: LabelGatewayName was overridden: got %q, want %q", h.name, h.labels[LabelGatewayName], "test-gw")
		}
		if h.labels[LabelGatewayNamespace] != "test-ns" {
			t.Errorf("%s: LabelGatewayNamespace was overridden: got %q, want %q", h.name, h.labels[LabelGatewayNamespace], "test-ns")
		}
		if h.labels[LabelManagedBy] != ManagedByValue {
			t.Errorf("%s: LabelManagedBy was overridden: got %q, want %q", h.name, h.labels[LabelManagedBy], ManagedByValue)
		}
		if h.labels[LabelAppName] != AppNameValue {
			t.Errorf("%s: LabelAppName was overridden: got %q, want %q", h.name, h.labels[LabelAppName], AppNameValue)
		}
		if _, ok := h.labels["gateway.networking.k8s.io/custom-reserved"]; ok {
			t.Errorf("%s: contains forbidden gateway.networking.k8s.io/* label", h.name)
		}
		if h.labels["custom-valid-label"] != "valid-val" {
			t.Errorf("%s: missing valid custom label: got %q", h.name, h.labels["custom-valid-label"])
		}
	}

	// Verify Deployment selector is not overridden
	expectedDeploySelector := map[string]string{
		LabelGatewayName: "test-gw",
		LabelAppName:     AppNameValue,
	}
	if !reflectMapEqual(deploy.Spec.Selector.MatchLabels, expectedDeploySelector) {
		t.Errorf("Deployment selector was modified: got %+v, want %+v", deploy.Spec.Selector.MatchLabels, expectedDeploySelector)
	}

	// Verify Service selector is not overridden
	if !reflectMapEqual(svc.Spec.Selector, expectedDeploySelector) {
		t.Errorf("Service selector was modified: got %+v, want %+v", svc.Spec.Selector, expectedDeploySelector)
	}
}

func TestSinglePodAddressProvider_PreservesForeignLabelsAndAnnotations(t *testing.T) {
	c := setupTestClient(t)
	ctx := t.Context()
	p := NewAddressProvider(c)

	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-gw",
			Namespace: "test-ns",
			UID:       types.UID("gw-uid-1"),
		},
		Spec: gatewayv1.GatewaySpec{
			Infrastructure: &gatewayv1.GatewayInfrastructure{
				Labels: map[gatewayv1.LabelKey]gatewayv1.LabelValue{
					"tier":       "frontend",
					"remove-lbl": "old-val",
				},
				Annotations: map[gatewayv1.AnnotationKey]gatewayv1.AnnotationValue{
					"cost-center": "1000",
					"remove-anno": "old-val",
				},
			},
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}

	_, _, err := p.GatewayAddresses(ctx, gw, nil)
	if err != nil {
		t.Fatalf("unexpected error on initial reconcile: %v", err)
	}

	resName := ResourceNameForGateway("test-gw")

	// Inject foreign annotation and foreign label onto existing ServiceAccount, Deployment, and Service
	var sa corev1.ServiceAccount
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &sa); err != nil {
		t.Fatalf("failed to get ServiceAccount: %v", err)
	}
	sa.Labels["custom.io/zone"] = "us-central1-a"
	sa.Annotations["cloud.google.com/neg-status"] = `{"network_endpoint_groups":{"80":"k8s1-neg"}}`
	if err := c.Update(ctx, &sa); err != nil {
		t.Fatalf("failed to update ServiceAccount with foreign metadata: %v", err)
	}

	var deploy appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &deploy); err != nil {
		t.Fatalf("failed to get Deployment: %v", err)
	}
	deploy.Labels["custom.io/zone"] = "us-central1-a"
	deploy.Annotations["deployment.kubernetes.io/revision"] = "1"
	deploy.Annotations["cloud.google.com/neg-status"] = `{"network_endpoint_groups":{"80":"k8s1-neg"}}`
	if err := c.Update(ctx, &deploy); err != nil {
		t.Fatalf("failed to update Deployment with foreign metadata: %v", err)
	}

	var svc corev1.Service
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &svc); err != nil {
		t.Fatalf("failed to get Service: %v", err)
	}
	svc.Labels["custom.io/zone"] = "us-central1-a"
	svc.Annotations["cloud.google.com/neg-status"] = `{"network_endpoint_groups":{"80":"k8s1-neg"}}`
	svc.Annotations["metallb.universe.tf/ip-allocated-from-pool"] = "example"
	if err := c.Update(ctx, &svc); err != nil {
		t.Fatalf("failed to update Service with foreign metadata: %v", err)
	}

	// Update infrastructure metadata: change one value and remove one key
	gw.Spec.Infrastructure.Labels["tier"] = "frontend-v2"
	delete(gw.Spec.Infrastructure.Labels, "remove-lbl")
	gw.Spec.Infrastructure.Annotations["cost-center"] = "2000"
	delete(gw.Spec.Infrastructure.Annotations, "remove-anno")

	_, _, err = p.GatewayAddresses(ctx, gw, nil)
	if err != nil {
		t.Fatalf("unexpected error on second reconcile: %v", err)
	}

	// Verify ServiceAccount preserves foreign labels/annotations and updates/removes infra keys
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &sa); err != nil {
		t.Fatalf("failed to get ServiceAccount: %v", err)
	}
	if sa.Labels["custom.io/zone"] != "us-central1-a" {
		t.Errorf("ServiceAccount lost foreign label: got %+v", sa.Labels)
	}
	if sa.Annotations["cloud.google.com/neg-status"] != `{"network_endpoint_groups":{"80":"k8s1-neg"}}` {
		t.Errorf("ServiceAccount lost foreign annotation: got %+v", sa.Annotations)
	}
	if sa.Labels["tier"] != "frontend-v2" {
		t.Errorf("ServiceAccount did not update tier label: got %q", sa.Labels["tier"])
	}
	if _, ok := sa.Labels["remove-lbl"]; ok {
		t.Errorf("ServiceAccount still has removed label: got %+v", sa.Labels)
	}
	if sa.Annotations["cost-center"] != "2000" {
		t.Errorf("ServiceAccount did not update cost-center annotation: got %q", sa.Annotations["cost-center"])
	}
	if _, ok := sa.Annotations["remove-anno"]; ok {
		t.Errorf("ServiceAccount still has removed annotation: got %+v", sa.Annotations)
	}

	// Verify Deployment preserves foreign labels/annotations and updates/removes infra keys
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &deploy); err != nil {
		t.Fatalf("failed to get Deployment: %v", err)
	}
	if deploy.Labels["custom.io/zone"] != "us-central1-a" {
		t.Errorf("Deployment lost foreign label: got %+v", deploy.Labels)
	}
	if deploy.Annotations["cloud.google.com/neg-status"] != `{"network_endpoint_groups":{"80":"k8s1-neg"}}` {
		t.Errorf("Deployment lost foreign annotation cloud.google.com/neg-status: got %+v", deploy.Annotations)
	}
	if deploy.Annotations["deployment.kubernetes.io/revision"] != "1" {
		t.Errorf("Deployment lost foreign annotation deployment.kubernetes.io/revision: got %+v", deploy.Annotations)
	}
	if deploy.Labels["tier"] != "frontend-v2" {
		t.Errorf("Deployment did not update tier label: got %q", deploy.Labels["tier"])
	}
	if _, ok := deploy.Labels["remove-lbl"]; ok {
		t.Errorf("Deployment still has removed label: got %+v", deploy.Labels)
	}
	if deploy.Annotations["cost-center"] != "2000" {
		t.Errorf("Deployment did not update cost-center annotation: got %q", deploy.Annotations["cost-center"])
	}
	if _, ok := deploy.Annotations["remove-anno"]; ok {
		t.Errorf("Deployment still has removed annotation: got %+v", deploy.Annotations)
	}

	// Verify Service preserves foreign labels/annotations and updates/removes infra keys
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &svc); err != nil {
		t.Fatalf("failed to get Service: %v", err)
	}
	if svc.Labels["custom.io/zone"] != "us-central1-a" {
		t.Errorf("Service lost foreign label: got %+v", svc.Labels)
	}
	if svc.Annotations["cloud.google.com/neg-status"] != `{"network_endpoint_groups":{"80":"k8s1-neg"}}` {
		t.Errorf("Service lost foreign annotation cloud.google.com/neg-status: got %+v", svc.Annotations)
	}
	if svc.Annotations["metallb.universe.tf/ip-allocated-from-pool"] != "example" {
		t.Errorf("Service lost foreign annotation metallb.universe.tf/ip-allocated-from-pool: got %+v", svc.Annotations)
	}
	if svc.Labels["tier"] != "frontend-v2" {
		t.Errorf("Service did not update tier label: got %q", svc.Labels["tier"])
	}
	if _, ok := svc.Labels["remove-lbl"]; ok {
		t.Errorf("Service still has removed label: got %+v", svc.Labels)
	}
	if svc.Annotations["cost-center"] != "2000" {
		t.Errorf("Service did not update cost-center annotation: got %q", svc.Annotations["cost-center"])
	}
	if _, ok := svc.Annotations["remove-anno"]; ok {
		t.Errorf("Service still has removed annotation: got %+v", svc.Annotations)
	}
}

func TestSinglePodAddressProvider_StaleControllerOwnerRefUIDUpdated(t *testing.T) {
	c := setupTestClient(t)
	ctx := t.Context()
	p := NewAddressProvider(c)

	initialGW := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-gw",
			Namespace: "test-ns",
			UID:       types.UID("gw-uid-original"),
		},
		Spec: gatewayv1.GatewaySpec{
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}

	_, _, err := p.GatewayAddresses(ctx, initialGW, nil)
	if err != nil {
		t.Fatalf("unexpected error on initial reconcile: %v", err)
	}

	resName := ResourceNameForGateway("test-gw")

	// Simulate objects retaining stale UID (e.g. Gateway was deleted and recreated with a new UID)
	staleUID := types.UID("stale-uid-12345")
	var sa corev1.ServiceAccount
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &sa); err != nil {
		t.Fatalf("failed to get ServiceAccount: %v", err)
	}
	sa.OwnerReferences[0].UID = staleUID
	if err := c.Update(ctx, &sa); err != nil {
		t.Fatalf("failed to set stale UID on ServiceAccount: %v", err)
	}

	var deploy appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &deploy); err != nil {
		t.Fatalf("failed to get Deployment: %v", err)
	}
	deploy.OwnerReferences[0].UID = staleUID
	if err := c.Update(ctx, &deploy); err != nil {
		t.Fatalf("failed to set stale UID on Deployment: %v", err)
	}

	var svc corev1.Service
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &svc); err != nil {
		t.Fatalf("failed to get Service: %v", err)
	}
	svc.OwnerReferences[0].UID = staleUID
	if err := c.Update(ctx, &svc); err != nil {
		t.Fatalf("failed to set stale UID on Service: %v", err)
	}

	// Reconcile with new Gateway having new UID
	newGW := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-gw",
			Namespace: "test-ns",
			UID:       types.UID("gw-uid-new"),
		},
		Spec: gatewayv1.GatewaySpec{
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}

	_, _, err = p.GatewayAddresses(ctx, newGW, nil)
	if err != nil {
		t.Fatalf("unexpected error on reconcile with new Gateway UID: %v", err)
	}

	// Verify all three objects have their controller ownerRef updated to newGW.UID
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &sa); err != nil {
		t.Fatalf("failed to get ServiceAccount: %v", err)
	}
	if len(sa.OwnerReferences) == 0 || sa.OwnerReferences[0].UID != "gw-uid-new" {
		t.Errorf("ServiceAccount ownerRef UID not updated: got %+v, want gw-uid-new", sa.OwnerReferences)
	}

	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &deploy); err != nil {
		t.Fatalf("failed to get Deployment: %v", err)
	}
	if len(deploy.OwnerReferences) == 0 || deploy.OwnerReferences[0].UID != "gw-uid-new" {
		t.Errorf("Deployment ownerRef UID not updated: got %+v, want gw-uid-new", deploy.OwnerReferences)
	}

	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &svc); err != nil {
		t.Fatalf("failed to get Service: %v", err)
	}
	if len(svc.OwnerReferences) == 0 || svc.OwnerReferences[0].UID != "gw-uid-new" {
		t.Errorf("Service ownerRef UID not updated: got %+v, want gw-uid-new", svc.OwnerReferences)
	}
}
