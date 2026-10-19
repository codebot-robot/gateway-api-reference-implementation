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
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/controller"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// ssaNoOpInterceptor adjusts controller-runtime's fake client for Server-Side Apply:
// It models Kubernetes SSA no-op behavior: when an apply configuration produces no changes on an existing object, no write is issued and resourceVersion is not incremented.
func ssaNoOpInterceptor() interceptor.Funcs {
	return interceptor.Funcs{
		Apply: func(ctx context.Context, cl client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
			data, err := json.Marshal(obj)
			if err != nil {
				return err
			}
			var tm struct {
				APIVersion string `json:"apiVersion"`
				Kind       string `json:"kind"`
				Metadata   struct {
					Name      string `json:"name"`
					Namespace string `json:"namespace"`
				} `json:"metadata"`
			}
			if err := json.Unmarshal(data, &tm); err != nil {
				return err
			}

			u := &unstructured.Unstructured{}
			u.SetAPIVersion(tm.APIVersion)
			u.SetKind(tm.Kind)
			key := types.NamespacedName{Namespace: tm.Metadata.Namespace, Name: tm.Metadata.Name}
			if err := cl.Get(ctx, key, u); err != nil {
				return cl.Apply(ctx, obj, opts...)
			}

			probeClient := fake.NewClientBuilder().
				WithScheme(cl.Scheme()).
				WithReturnManagedFields().
				WithObjects(u.DeepCopy()).
				Build()

			probeObj := &unstructured.Unstructured{}
			if err := json.Unmarshal(data, probeObj); err != nil {
				return err
			}
			unstructured.RemoveNestedField(probeObj.Object, "metadata", "resourceVersion")
			probeAC := client.ApplyConfigurationFromUnstructured(probeObj)

			if err := probeClient.Apply(ctx, probeAC, opts...); err != nil {
				return err
			}

			probed := &unstructured.Unstructured{}
			probed.SetAPIVersion(tm.APIVersion)
			probed.SetKind(tm.Kind)
			if err := probeClient.Get(ctx, key, probed); err != nil {
				return cl.Apply(ctx, obj, opts...)
			}

			clean := func(item *unstructured.Unstructured) map[string]any {
				m := item.DeepCopy().Object
				unstructured.RemoveNestedField(m, "metadata", "resourceVersion")
				unstructured.RemoveNestedField(m, "metadata", "managedFields")
				return m
			}

			if reflect.DeepEqual(clean(u), clean(probed)) {
				// No fields changed; SSA is a no-op so avoid writing and bumping resourceVersion.
				return nil
			}

			return cl.Apply(ctx, obj, opts...)
		},
	}
}

func TestSinglePodAddressProvider_GatewayAddresses(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithReturnManagedFields().
		WithInterceptorFuncs(ssaNoOpInterceptor()).
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

	// Verify Role was created in my-ns
	var role rbacv1.Role
	if err := client.Get(ctx, types.NamespacedName{Namespace: "my-ns", Name: name}, &role); err != nil {
		t.Fatalf("failed to get created Role: %v", err)
	}
	if role.Labels["custom-label"] != "custom-val" {
		t.Errorf("expected custom-label on Role, got %s", role.Labels["custom-label"])
	}
	if len(role.Rules) != 1 || role.Rules[0].Resources[0] != "secrets" || role.Rules[0].ResourceNames[0] != name {
		t.Errorf("expected Role rule to permit secrets/%s, got %+v", name, role.Rules)
	}

	// Verify RoleBinding was created in my-ns
	var rb rbacv1.RoleBinding
	if err := client.Get(ctx, types.NamespacedName{Namespace: "my-ns", Name: name}, &rb); err != nil {
		t.Fatalf("failed to get created RoleBinding: %v", err)
	}
	if rb.RoleRef.Name != name {
		t.Errorf("expected RoleBinding roleRef %s, got %s", name, rb.RoleRef.Name)
	}
	if len(rb.Subjects) != 1 || rb.Subjects[0].Name != name || rb.Subjects[0].Namespace != "my-ns" {
		t.Errorf("expected RoleBinding subject to be SA %s in my-ns, got %+v", name, rb.Subjects)
	}

	// Verify Secret is NOT created by GatewayAddresses (placeholder secrets are not created;
	// config secret is created only when real config is published).
	var sec corev1.Secret
	if err := client.Get(ctx, types.NamespacedName{Namespace: "my-ns", Name: name}, &sec); !apierrors.IsNotFound(err) {
		t.Fatalf("expected Secret to not exist before publish, got: %v", err)
	}

	// Publish DataplaneConfig -> Secret is created
	dpCfg := &state.DataplaneConfig{
		GatewayName: types.NamespacedName{Namespace: "my-ns", Name: "my-gw"},
	}
	if err := p.PublishDataplaneConfig(ctx, gw, dpCfg); err != nil {
		t.Fatalf("failed to publish config: %v", err)
	}
	if err := client.Get(ctx, types.NamespacedName{Namespace: "my-ns", Name: name}, &sec); err != nil {
		t.Fatalf("failed to get created Secret: %v", err)
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

	// 3. Update Gateway infrastructure labels -> should update template hash and pod template
	gw.Spec.Infrastructure.Labels["updated-label"] = "updated-val"
	_, _, err = p.GatewayAddresses(ctx, gw, effectiveListeners)
	if err != nil {
		t.Fatalf("failed to update gateway with new infrastructure label: %v", err)
	}
	if err := client.Get(ctx, types.NamespacedName{Namespace: "my-ns", Name: name}, &deploy); err != nil {
		t.Fatalf("failed to get updated deployment: %v", err)
	}
	if deploy.Spec.Template.Labels["updated-label"] != "updated-val" {
		t.Errorf("expected updated pod template labels to include updated-label")
	}

	// 4. (Namespaced resources have controller ownerReferences verified in step 1 and are deleted by k8s garbage collector)
}

func TestSinglePodAddressProvider_NamespacedRolesAndSecrets(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	p := NewAddressProvider(client)
	ctx := t.Context()

	gw1 := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw-1", Namespace: "ns-1", UID: types.UID("gw-1-uid")},
		Spec:       gatewayv1.GatewaySpec{Listeners: []gatewayv1.Listener{{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType}}},
	}
	gw2 := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw-2", Namespace: "ns-2", UID: types.UID("gw-2-uid")},
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

	// Verify ns-1 has Role, RoleBinding
	var role1 rbacv1.Role
	if err := client.Get(ctx, types.NamespacedName{Namespace: "ns-1", Name: "gw-1-gari"}, &role1); err != nil {
		t.Fatalf("failed to get Role for gw-1: %v", err)
	}
	var rb1 rbacv1.RoleBinding
	if err := client.Get(ctx, types.NamespacedName{Namespace: "ns-1", Name: "gw-1-gari"}, &rb1); err != nil {
		t.Fatalf("failed to get RoleBinding for gw-1: %v", err)
	}
	var sec1 corev1.Secret
	if err := client.Get(ctx, types.NamespacedName{Namespace: "ns-1", Name: "gw-1-gari"}, &sec1); !apierrors.IsNotFound(err) {
		t.Fatalf("expected Secret for gw-1 to not exist before publish, got: %v", err)
	}

	// Verify ns-2 has Role, RoleBinding
	var role2 rbacv1.Role
	if err := client.Get(ctx, types.NamespacedName{Namespace: "ns-2", Name: "gw-2-gari"}, &role2); err != nil {
		t.Fatalf("failed to get Role for gw-2: %v", err)
	}
	var rb2 rbacv1.RoleBinding
	if err := client.Get(ctx, types.NamespacedName{Namespace: "ns-2", Name: "gw-2-gari"}, &rb2); err != nil {
		t.Fatalf("failed to get RoleBinding for gw-2: %v", err)
	}
	var sec2 corev1.Secret
	if err := client.Get(ctx, types.NamespacedName{Namespace: "ns-2", Name: "gw-2-gari"}, &sec2); !apierrors.IsNotFound(err) {
		t.Fatalf("expected Secret for gw-2 to not exist before publish, got: %v", err)
	}

	// Publish DataplaneConfig for gw1
	dpCfg := &state.DataplaneConfig{
		GatewayName: types.NamespacedName{Namespace: "ns-1", Name: "gw-1"},
		Routes: []state.InternalRoute{
			{Hostnames: []string{"test.com"}},
		},
	}
	if err := p.PublishDataplaneConfig(ctx, gw1, dpCfg); err != nil {
		t.Fatalf("PublishDataplaneConfig failed: %v", err)
	}

	if err := client.Get(ctx, types.NamespacedName{Namespace: "ns-1", Name: "gw-1-gari"}, &sec1); err != nil {
		t.Fatalf("failed to get updated Secret for gw-1: %v", err)
	}
	unmarshaled, err := state.UnmarshalDataplaneConfig(sec1.Data[DataplaneSecretDataKey])
	if err != nil {
		t.Fatalf("failed to unmarshal published config: %v", err)
	}
	if len(unmarshaled.Routes) != 1 || unmarshaled.Routes[0].Hostnames[0] != "test.com" {
		t.Errorf("unexpected published config: %+v", unmarshaled)
	}
}

func TestSinglePodAddressProvider_OwnershipConflict(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

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
	resName := ResourceNameForGateway("conflict-gw")

	trueVal := true
	diffOwnerRef := metav1.OwnerReference{
		APIVersion: gatewayv1.GroupVersion.String(),
		Kind:       "Gateway",
		Name:       "conflict-gw",
		UID:        types.UID("other-uid-11111"),
		Controller: &trueVal,
	}

	tests := []struct {
		name   string
		object client.Object
	}{
		{
			name: "foreign service with no labels and no ownerRef",
			object: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resName,
					Namespace: "default",
				},
			},
		},
		{
			name: "service with managed labels but no ownerRef",
			object: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resName,
					Namespace: "default",
					Labels: map[string]string{
						LabelGatewayName: "conflict-gw",
						LabelManagedBy:   ManagedByValue,
					},
				},
			},
		},
		{
			name: "service with managed labels and ownerRef with different UID",
			object: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resName,
					Namespace: "default",
					Labels: map[string]string{
						LabelGatewayName: "conflict-gw",
						LabelManagedBy:   ManagedByValue,
					},
					OwnerReferences: []metav1.OwnerReference{diffOwnerRef},
				},
			},
		},
		{
			name: "deployment with managed labels but no ownerRef",
			object: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resName,
					Namespace: "default",
					Labels: map[string]string{
						LabelGatewayName: "conflict-gw",
						LabelManagedBy:   ManagedByValue,
					},
				},
			},
		},
		{
			name: "deployment with managed labels and ownerRef with different UID",
			object: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resName,
					Namespace: "default",
					Labels: map[string]string{
						LabelGatewayName: "conflict-gw",
						LabelManagedBy:   ManagedByValue,
					},
					OwnerReferences: []metav1.OwnerReference{diffOwnerRef},
				},
			},
		},
		{
			name: "serviceaccount with managed labels but no ownerRef",
			object: &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resName,
					Namespace: "default",
					Labels: map[string]string{
						LabelGatewayName: "conflict-gw",
						LabelManagedBy:   ManagedByValue,
					},
				},
			},
		},
		{
			name: "serviceaccount with managed labels and ownerRef with different UID",
			object: &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resName,
					Namespace: "default",
					Labels: map[string]string{
						LabelGatewayName: "conflict-gw",
						LabelManagedBy:   ManagedByValue,
					},
					OwnerReferences: []metav1.OwnerReference{diffOwnerRef},
				},
			},
		},
		{
			name: "role with managed labels and ownerRef with different UID",
			object: &rbacv1.Role{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resName,
					Namespace: "default",
					Labels: map[string]string{
						LabelGatewayName: "conflict-gw",
						LabelManagedBy:   ManagedByValue,
					},
					OwnerReferences: []metav1.OwnerReference{diffOwnerRef},
				},
			},
		},
		{
			name: "rolebinding with managed labels and ownerRef with different UID",
			object: &rbacv1.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resName,
					Namespace: "default",
					Labels: map[string]string{
						LabelGatewayName: "conflict-gw",
						LabelManagedBy:   ManagedByValue,
					},
					OwnerReferences: []metav1.OwnerReference{diffOwnerRef},
				},
			},
		},
		{
			name: "secret with managed labels and ownerRef with different UID",
			object: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resName,
					Namespace: "default",
					Labels: map[string]string{
						LabelGatewayName: "conflict-gw",
						LabelManagedBy:   ManagedByValue,
					},
					OwnerReferences: []metav1.OwnerReference{diffOwnerRef},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.object).
				Build()

			p := NewAddressProvider(cl)
			ctx := t.Context()

			_, _, err := p.GatewayAddresses(ctx, gw, nil)
			if err == nil {
				t.Fatalf("expected ownership conflict error, got nil")
			}
			var conflictErr *controller.OwnershipConflictError
			if !errors.As(err, &conflictErr) {
				t.Fatalf("expected OwnershipConflictError, got: %v", err)
			}
		})
	}
}

func TestSinglePodAddressProvider_PublishDataplaneConfig_OwnershipConflict(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-gw",
			Namespace: "default",
			UID:       types.UID("gw-uid-123"),
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "my-class",
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}
	resName := ResourceNameForGateway("my-gw")

	trueVal := true
	diffOwnerRef := metav1.OwnerReference{
		APIVersion: gatewayv1.GroupVersion.String(),
		Kind:       "Gateway",
		Name:       "my-gw",
		UID:        types.UID("different-uid-456"),
		Controller: &trueVal,
	}

	existingSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            resName,
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{diffOwnerRef},
		},
		Data: map[string][]byte{
			"custom-key": []byte("original-payload"),
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existingSecret).
		Build()

	p := NewAddressProvider(cl)
	ctx := t.Context()

	dpCfg := &state.DataplaneConfig{
		GatewayName: types.NamespacedName{Namespace: "default", Name: "my-gw"},
	}

	// 1. Direct call to PublishDataplaneConfig returns OwnershipConflictError
	err := p.PublishDataplaneConfig(ctx, gw, dpCfg)
	if err == nil {
		t.Fatalf("expected OwnershipConflictError, got nil")
	}
	var conflictErr *controller.OwnershipConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected OwnershipConflictError, got: %v", err)
	}
	expectedMsg := fmt.Sprintf("conflict: existing Secret default/%s is not owned by Gateway my-gw", resName)
	if conflictErr.Error() != expectedMsg {
		t.Errorf("expected error message %q, got %q", expectedMsg, conflictErr.Error())
	}

	// 2. Secret data and ownerReferences are unchanged
	var fetchedSec corev1.Secret
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "default", Name: resName}, &fetchedSec); err != nil {
		t.Fatalf("failed to fetch secret: %v", err)
	}
	if string(fetchedSec.Data["custom-key"]) != "original-payload" {
		t.Errorf("expected secret data to remain unchanged, got %+v", fetchedSec.Data)
	}
	if _, ok := fetchedSec.Data[DataplaneSecretDataKey]; ok {
		t.Errorf("dataplane config key should NOT be present on conflicted secret")
	}
	if len(fetchedSec.OwnerReferences) != 1 || fetchedSec.OwnerReferences[0].UID != "different-uid-456" {
		t.Errorf("expected ownerReferences to remain unchanged, got %+v", fetchedSec.OwnerReferences)
	}

	// 3. Gateway reports Programmed=False with the conflict message when state recomputes and publishes
	st := state.NewState()
	st.SetControllerName("test-controller")
	st.SetDataplanePublisher(p)
	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "my-class"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "test-controller"},
	}
	gwKey := types.NamespacedName{Namespace: "default", Name: "my-gw"}
	st.UpsertGatewayClass(gc)
	st.UpsertGateway(gw)
	st.SetGatewayAddresses(gwKey, []gatewayv1.GatewayStatusAddress{{Value: "192.0.2.1"}})
	st.Recompute()
	st.Recompute()

	status, ok := st.GetDesiredGatewayStatus(gwKey)
	if !ok {
		t.Fatalf("expected desired gateway status")
	}
	var progCond *metav1.Condition
	for i := range status.Conditions {
		if status.Conditions[i].Type == string(gatewayv1.GatewayConditionProgrammed) {
			progCond = &status.Conditions[i]
			break
		}
	}
	if progCond == nil || progCond.Status != metav1.ConditionFalse || progCond.Reason != string(gatewayv1.GatewayReasonPending) {
		t.Fatalf("expected Programmed=False/Pending on conflict, got %+v", progCond)
	}
	if !strings.Contains(progCond.Message, conflictErr.Error()) {
		t.Errorf("expected conflict message in condition, got %s", progCond.Message)
	}

	// 4. Resolve conflict by deleting conflicting secret -> Recompute succeeds and restores Programmed=True
	if err := cl.Delete(ctx, &fetchedSec); err != nil {
		t.Fatalf("failed to delete conflicting secret: %v", err)
	}
	st.Recompute()
	st.Recompute()

	status, ok = st.GetDesiredGatewayStatus(gwKey)
	if !ok {
		t.Fatalf("expected desired gateway status")
	}
	progCond = nil
	for i := range status.Conditions {
		if status.Conditions[i].Type == string(gatewayv1.GatewayConditionProgrammed) {
			progCond = &status.Conditions[i]
			break
		}
	}
	if progCond == nil || progCond.Status != metav1.ConditionTrue || progCond.Reason != string(gatewayv1.GatewayReasonProgrammed) {
		t.Fatalf("expected Programmed=True when conflict resolved, got %+v", progCond)
	}
}

func TestSinglePodAddressProvider_PublishDataplaneConfig_OwnedAndNew(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-gw",
			Namespace: "default",
			UID:       types.UID("gw-uid-current"),
		},
		Spec: gatewayv1.GatewaySpec{
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}
	resName := ResourceNameForGateway("test-gw")
	dpCfg := &state.DataplaneConfig{
		GatewayName: types.NamespacedName{Namespace: "default", Name: "test-gw"},
	}

	t.Run("no existing secret -> created with controller ownerRef", func(t *testing.T) {
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithReturnManagedFields().
			WithInterceptorFuncs(ssaNoOpInterceptor()).
			Build()
		p := NewAddressProvider(cl)
		ctx := t.Context()

		if err := p.PublishDataplaneConfig(ctx, gw, dpCfg); err != nil {
			t.Fatalf("PublishDataplaneConfig failed: %v", err)
		}

		var sec corev1.Secret
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "default", Name: resName}, &sec); err != nil {
			t.Fatalf("failed to get created Secret: %v", err)
		}
		if _, ok := sec.Data[DataplaneSecretDataKey]; !ok {
			t.Fatalf("expected DataplaneSecretDataKey in secret data")
		}
		ref := metav1.GetControllerOf(&sec)
		if ref == nil {
			t.Fatalf("expected controller ownerRef on created secret")
		}
		if ref.Kind != "Gateway" || ref.Name != "test-gw" || ref.UID != "gw-uid-current" {
			t.Errorf("unexpected ownerRef: %+v", ref)
		}
	})

	t.Run("secret owned by this Gateway -> applied and updated", func(t *testing.T) {
		trueVal := true
		existingSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      resName,
				Namespace: "default",
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: gatewayv1.GroupVersion.String(),
						Kind:       "Gateway",
						Name:       "test-gw",
						UID:        types.UID("gw-uid-current"),
						Controller: &trueVal,
					},
				},
			},
			Data: map[string][]byte{
				DataplaneSecretDataKey: []byte(`{"old":"data"}`),
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(existingSecret).
			WithReturnManagedFields().
			WithInterceptorFuncs(ssaNoOpInterceptor()).
			Build()
		p := NewAddressProvider(cl)
		ctx := t.Context()

		if err := p.PublishDataplaneConfig(ctx, gw, dpCfg); err != nil {
			t.Fatalf("PublishDataplaneConfig failed: %v", err)
		}

		var sec corev1.Secret
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "default", Name: resName}, &sec); err != nil {
			t.Fatalf("failed to get updated Secret: %v", err)
		}
		if string(sec.Data[DataplaneSecretDataKey]) == `{"old":"data"}` {
			t.Errorf("expected Secret data to be updated from old value")
		}
		ref := metav1.GetControllerOf(&sec)
		if ref == nil || ref.UID != "gw-uid-current" {
			t.Errorf("expected controller ownerRef with matching UID, got: %+v", ref)
		}
	})

	t.Run("secret with no ownerRef -> conflict error", func(t *testing.T) {
		unownedSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      resName,
				Namespace: "default",
			},
			Data: map[string][]byte{
				"foo": []byte("bar"),
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(unownedSecret).
			Build()
		p := NewAddressProvider(cl)
		ctx := t.Context()

		err := p.PublishDataplaneConfig(ctx, gw, dpCfg)
		if err == nil {
			t.Fatalf("expected OwnershipConflictError, got nil")
		}
		var conflictErr *controller.OwnershipConflictError
		if !errors.As(err, &conflictErr) {
			t.Fatalf("expected OwnershipConflictError, got: %v", err)
		}
	})
}

func TestSinglePodAddressProvider_Watches(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

	gwGV := schema.GroupVersion{Group: gatewayv1.GroupName, Version: "v1"}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{gwGV})
	mapper.Add(gwGV.WithKind("Gateway"), meta.RESTScopeNamespace)
	pred := managedByPredicate()
	ownerHandler := handler.EnqueueRequestForOwner(scheme, mapper, &gatewayv1.Gateway{}, handler.OnlyControllerOwner())

	trueVal := true
	falseVal := false
	ownerRef := metav1.OwnerReference{
		APIVersion: gatewayv1.GroupVersion.String(),
		Kind:       "Gateway",
		Name:       "gw-1",
		UID:        types.UID("gw-uid-1"),
		Controller: &trueVal,
	}
	nonControllerOwnerRef := metav1.OwnerReference{
		APIVersion: gatewayv1.GroupVersion.String(),
		Kind:       "Gateway",
		Name:       "gw-1",
		UID:        types.UID("gw-uid-1"),
		Controller: &falseVal,
	}

	ctx := t.Context()
	processCreate := func(obj client.Object) []reconcile.Request {
		evt := event.CreateEvent{Object: obj}
		if !pred.Create(evt) {
			return nil
		}
		q := workqueue.NewTypedRateLimitingQueue[reconcile.Request](workqueue.DefaultTypedItemBasedRateLimiter[reconcile.Request]())
		defer q.ShutDown()
		ownerHandler.Create(ctx, evt, q)
		var reqs []reconcile.Request
		for q.Len() > 0 {
			item, _ := q.Get()
			reqs = append(reqs, item)
			q.Done(item)
		}
		return reqs
	}

	processUpdate := func(oldObj, newObj client.Object) []reconcile.Request {
		evt := event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj}
		if !pred.Update(evt) {
			return nil
		}
		q := workqueue.NewTypedRateLimitingQueue[reconcile.Request](workqueue.DefaultTypedItemBasedRateLimiter[reconcile.Request]())
		defer q.ShutDown()
		ownerHandler.Update(ctx, evt, q)
		var reqs []reconcile.Request
		for q.Len() > 0 {
			item, _ := q.Get()
			reqs = append(reqs, item)
			q.Done(item)
		}
		return reqs
	}

	// 1. Service with managed label and controller ownerReference -> enqueues owning Gateway
	ownedSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            ResourceNameForGateway("gw-1"),
			Namespace:       "custom-ns",
			Labels:          map[string]string{LabelManagedBy: ManagedByValue},
			OwnerReferences: []metav1.OwnerReference{ownerRef},
		},
	}
	reqs := processCreate(ownedSvc)
	if len(reqs) != 1 || reqs[0].NamespacedName != (types.NamespacedName{Namespace: "custom-ns", Name: "gw-1"}) {
		t.Errorf("expected 1 request for custom-ns/gw-1 from owned Service, got %+v", reqs)
	}

	// 2. Deployment with managed label and controller ownerReference -> enqueues owning Gateway
	ownedDeploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            ResourceNameForGateway("gw-1"),
			Namespace:       "custom-ns",
			Labels:          map[string]string{LabelManagedBy: ManagedByValue},
			OwnerReferences: []metav1.OwnerReference{ownerRef},
		},
	}
	reqs = processCreate(ownedDeploy)
	if len(reqs) != 1 || reqs[0].NamespacedName != (types.NamespacedName{Namespace: "custom-ns", Name: "gw-1"}) {
		t.Errorf("expected 1 request for custom-ns/gw-1 from owned Deployment, got %+v", reqs)
	}

	// 3. Non-owned Service with managed labels (no ownerRef) -> does not enqueue
	nonOwnedSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ResourceNameForGateway("gw-1"),
			Namespace: "custom-ns",
			Labels:    map[string]string{LabelManagedBy: ManagedByValue, LabelGatewayName: "gw-1"},
		},
	}
	reqs = processCreate(nonOwnedSvc)
	if len(reqs) != 0 {
		t.Errorf("expected 0 requests for non-owned Service with managed labels, got %+v", reqs)
	}

	// 4. Non-owned Deployment with managed labels (no ownerRef) -> does not enqueue
	nonOwnedDeploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ResourceNameForGateway("gw-1"),
			Namespace: "custom-ns",
			Labels:    map[string]string{LabelManagedBy: ManagedByValue, LabelGatewayName: "gw-1"},
		},
	}
	reqs = processCreate(nonOwnedDeploy)
	if len(reqs) != 0 {
		t.Errorf("expected 0 requests for non-owned Deployment with managed labels, got %+v", reqs)
	}

	// 5. Service with managed labels and non-controller ownerRef -> does not enqueue
	nonCtrlSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            ResourceNameForGateway("gw-1"),
			Namespace:       "custom-ns",
			Labels:          map[string]string{LabelManagedBy: ManagedByValue},
			OwnerReferences: []metav1.OwnerReference{nonControllerOwnerRef},
		},
	}
	reqs = processCreate(nonCtrlSvc)
	if len(reqs) != 0 {
		t.Errorf("expected 0 requests for Service with non-controller ownerRef, got %+v", reqs)
	}

	// 6. Unmanaged Service (without managed-by label) -> predicate filters out
	unmanagedSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "unmanaged",
			Namespace:       "custom-ns",
			OwnerReferences: []metav1.OwnerReference{ownerRef},
		},
	}
	reqs = processCreate(unmanagedSvc)
	if len(reqs) != 0 {
		t.Errorf("expected 0 requests for unmanaged Service, got %+v", reqs)
	}

	// 7. Update event on owned Service -> enqueues owning Gateway
	updatedSvc := ownedSvc.DeepCopy()
	updatedSvc.Annotations = map[string]string{"foo": "bar"}
	reqs = processUpdate(ownedSvc, updatedSvc)
	if len(reqs) != 1 || reqs[0].NamespacedName != (types.NamespacedName{Namespace: "custom-ns", Name: "gw-1"}) {
		t.Errorf("expected 1 request for custom-ns/gw-1 from updated owned Service, got %+v", reqs)
	}
}

func setupTestClient(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithReturnManagedFields().
		WithInterceptorFuncs(ssaNoOpInterceptor()).
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
		if _, ok := h.labels["gateway.networking.k8s.io/gateway-namespace"]; ok {
			t.Errorf("%s: contains dropped gateway.networking.k8s.io/gateway-namespace label: got %q", h.name, h.labels["gateway.networking.k8s.io/gateway-namespace"])
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
	if !maps.Equal(deploy.Spec.Selector.MatchLabels, expectedDeploySelector) {
		t.Errorf("Deployment selector was modified: got %+v, want %+v", deploy.Spec.Selector.MatchLabels, expectedDeploySelector)
	}

	// Verify Service selector is not overridden
	if !maps.Equal(svc.Spec.Selector, expectedDeploySelector) {
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

func TestSinglePodAddressProvider_NoChangesIssuesNoWrite(t *testing.T) {
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
					"tier": "frontend",
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

	resName := ResourceNameForGateway("test-gw")

	// 1. Initial reconcile creates resources
	_, _, err := p.GatewayAddresses(ctx, gw, nil)
	if err != nil {
		t.Fatalf("unexpected error on initial reconcile: %v", err)
	}

	var sa corev1.ServiceAccount
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &sa); err != nil {
		t.Fatalf("failed to get ServiceAccount: %v", err)
	}
	saRV := sa.ResourceVersion

	var deploy appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &deploy); err != nil {
		t.Fatalf("failed to get Deployment: %v", err)
	}
	deployRV := deploy.ResourceVersion

	var svc corev1.Service
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &svc); err != nil {
		t.Fatalf("failed to get Service: %v", err)
	}
	svcRV := svc.ResourceVersion

	// 2. Second reconcile with identical configuration: no writes issued, resourceVersion unchanged
	_, _, err = p.GatewayAddresses(ctx, gw, nil)
	if err != nil {
		t.Fatalf("unexpected error on no-op reconcile: %v", err)
	}

	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &sa); err != nil {
		t.Fatalf("failed to get ServiceAccount: %v", err)
	}
	if sa.ResourceVersion != saRV {
		t.Errorf("ServiceAccount resourceVersion changed on no-op reconcile: got %s, want %s", sa.ResourceVersion, saRV)
	}

	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &deploy); err != nil {
		t.Fatalf("failed to get Deployment: %v", err)
	}
	if deploy.ResourceVersion != deployRV {
		t.Errorf("Deployment resourceVersion changed on no-op reconcile: got %s, want %s", deploy.ResourceVersion, deployRV)
	}

	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &svc); err != nil {
		t.Fatalf("failed to get Service: %v", err)
	}
	if svc.ResourceVersion != svcRV {
		t.Errorf("Service resourceVersion changed on no-op reconcile: got %s, want %s", svc.ResourceVersion, svcRV)
	}

	// 3. Third reconcile with changed infrastructure metadata: resourceVersion must change
	gw.Spec.Infrastructure.Labels["tier"] = "frontend-v2"
	_, _, err = p.GatewayAddresses(ctx, gw, nil)
	if err != nil {
		t.Fatalf("unexpected error on update reconcile: %v", err)
	}

	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &sa); err != nil {
		t.Fatalf("failed to get ServiceAccount: %v", err)
	}
	if sa.ResourceVersion == saRV {
		t.Errorf("ServiceAccount resourceVersion expected to change on update, but stayed %s", sa.ResourceVersion)
	}

	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &deploy); err != nil {
		t.Fatalf("failed to get Deployment: %v", err)
	}
	if deploy.ResourceVersion == deployRV {
		t.Errorf("Deployment resourceVersion expected to change on update, but stayed %s", deploy.ResourceVersion)
	}

	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &svc); err != nil {
		t.Fatalf("failed to get Service: %v", err)
	}
	if svc.ResourceVersion == svcRV {
		t.Errorf("Service resourceVersion expected to change on update, but stayed %s", svc.ResourceVersion)
	}
}

func TestSinglePodAddressProvider_RequestedAddresses_AddRemove(t *testing.T) {
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
			Addresses: []gatewayv1.GatewaySpecAddress{
				{
					Type:  state.Ptr(gatewayv1.IPAddressType),
					Value: "192.0.2.1",
				},
			},
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}

	// 1. Initial reconcile with requested IP: Service pins loadBalancerIP, readiness is AddressNotUsable
	addrs, readiness, err := p.GatewayAddressesWithReadiness(ctx, gw, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if readiness.Ready {
		t.Errorf("expected ready=false when requested address not in ingress")
	}
	if readiness.Reason != string(gatewayv1.GatewayReasonAddressNotUsable) {
		t.Errorf("expected reason=AddressNotUsable, got %s", readiness.Reason)
	}
	if !strings.Contains(readiness.Message, "192.0.2.1") {
		t.Errorf("expected message mentioning 192.0.2.1, got %s", readiness.Message)
	}

	resName := ResourceNameForGateway("test-gw")
	var svc corev1.Service
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &svc); err != nil {
		t.Fatalf("failed to get Service: %v", err)
	}
	if svc.Spec.LoadBalancerIP != "192.0.2.1" {
		t.Errorf("expected Service LoadBalancerIP='192.0.2.1', got %q", svc.Spec.LoadBalancerIP)
	}

	// 2. Mark Deployment ready and Service ingress bound
	var deploy appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &deploy); err != nil {
		t.Fatalf("failed to get Deployment: %v", err)
	}
	deploy.Status.ReadyReplicas = 1
	if err := c.Status().Update(ctx, &deploy); err != nil {
		t.Fatalf("failed to update Deployment status: %v", err)
	}

	svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{
		{IP: "192.0.2.1"},
	}
	if err := c.Status().Update(ctx, &svc); err != nil {
		t.Fatalf("failed to update Service status: %v", err)
	}

	addrs, readiness, err = p.GatewayAddressesWithReadiness(ctx, gw, nil)
	if err != nil {
		t.Fatalf("unexpected error on second reconcile: %v", err)
	}
	if !readiness.Ready {
		t.Errorf("expected ready=true when requested address is bound in ingress")
	}
	if len(addrs) != 1 || addrs[0].Value != "192.0.2.1" {
		t.Errorf("expected 1 address 192.0.2.1, got %+v", addrs)
	}

	// 3. Removing requested address from spec.addresses releases it:
	// Service no longer pins it and status updates on next reconcile without restart.
	gw.Spec.Addresses = nil
	addrs, readiness, err = p.GatewayAddressesWithReadiness(ctx, gw, nil)
	if err != nil {
		t.Fatalf("unexpected error on remove reconcile: %v", err)
	}
	if !readiness.Ready {
		t.Errorf("expected ready=true after releasing static address")
	}
	if len(addrs) != 1 || addrs[0].Value != "192.0.2.1" {
		t.Errorf("expected reported address to still report ingress address 192.0.2.1, got %+v", addrs)
	}

	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &svc); err != nil {
		t.Fatalf("failed to get Service: %v", err)
	}
	if svc.Spec.LoadBalancerIP != "" {
		t.Errorf("expected Service LoadBalancerIP to be released/empty, got %q", svc.Spec.LoadBalancerIP)
	}
}

func TestSinglePodAddressProvider_TwoGatewaysSameIP(t *testing.T) {
	c := setupTestClient(t)
	ctx := t.Context()
	p := NewAddressProvider(c)

	gw1 := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gw-1",
			Namespace: "test-ns",
			UID:       types.UID("gw-1-uid"),
		},
		Spec: gatewayv1.GatewaySpec{
			Addresses: []gatewayv1.GatewaySpecAddress{
				{Value: "192.0.2.50"},
			},
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}
	gw2 := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gw-2",
			Namespace: "test-ns",
			UID:       types.UID("gw-2-uid"),
		},
		Spec: gatewayv1.GatewaySpec{
			Addresses: []gatewayv1.GatewaySpecAddress{
				{Value: "192.0.2.50"},
			},
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}

	// Reconcile gw1 and mark bound
	_, _, err := p.GatewayAddressesWithReadiness(ctx, gw1, nil)
	if err != nil {
		t.Fatalf("unexpected error on gw1: %v", err)
	}
	svc1Name := ResourceNameForGateway("gw-1")
	var svc1 corev1.Service
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: svc1Name}, &svc1); err != nil {
		t.Fatalf("failed to get svc1: %v", err)
	}
	svc1.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "192.0.2.50"}}
	_ = c.Status().Update(ctx, &svc1)
	var deploy1 appsv1.Deployment
	_ = c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: svc1Name}, &deploy1)
	deploy1.Status.ReadyReplicas = 1
	_ = c.Status().Update(ctx, &deploy1)

	_, readiness1, _ := p.GatewayAddressesWithReadiness(ctx, gw1, nil)
	if !readiness1.Ready {
		t.Fatalf("expected gw1 to be ready")
	}

	// Reconcile gw2 requesting the same IP: its service does NOT have the ingress IP bound
	_, readiness2, err := p.GatewayAddressesWithReadiness(ctx, gw2, nil)
	if err != nil {
		t.Fatalf("unexpected error on gw2: %v", err)
	}
	if readiness2.Ready {
		t.Errorf("expected gw2 ready=false when requesting already bound IP")
	}
	if readiness2.Reason != string(gatewayv1.GatewayReasonAddressNotUsable) {
		t.Errorf("expected gw2 reason=AddressNotUsable, got %s", readiness2.Reason)
	}
	if !strings.Contains(readiness2.Message, "192.0.2.50") {
		t.Errorf("expected message mentioning 192.0.2.50, got %s", readiness2.Message)
	}
}

func TestSinglePodAddressProvider_MultipleRequestedIPs(t *testing.T) {
	c := setupTestClient(t)
	ctx := t.Context()
	p := NewAddressProvider(c)

	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gw-multi",
			Namespace: "test-ns",
			UID:       types.UID("gw-multi-uid"),
		},
		Spec: gatewayv1.GatewaySpec{
			Addresses: []gatewayv1.GatewaySpecAddress{
				{Value: "192.0.2.10"},
				{Value: "192.0.2.20"},
			},
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}

	_, _, err := p.GatewayAddressesWithReadiness(ctx, gw, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Mock the first IP bound in service ingress
	resName := ResourceNameForGateway("gw-multi")
	var svc corev1.Service
	_ = c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &svc)
	svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "192.0.2.10"}}
	_ = c.Status().Update(ctx, &svc)

	_, readiness, err := p.GatewayAddressesWithReadiness(ctx, gw, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if readiness.Ready {
		t.Errorf("expected ready=false when second requested IP cannot be bound")
	}
	if readiness.Reason != string(gatewayv1.GatewayReasonAddressNotUsable) {
		t.Errorf("expected reason=AddressNotUsable, got %s", readiness.Reason)
	}
	expectedMsgSub := "singlepod binds at most one requested IP"
	if !strings.Contains(readiness.Message, expectedMsgSub) {
		t.Errorf("expected message to contain %q, got %q", expectedMsgSub, readiness.Message)
	}
}

func TestSinglePodAddressProvider_TCPListenerServiceAndDeploymentPorts(t *testing.T) {
	c := setupTestClient(t)
	p := NewAddressProvider(c)
	ctx := t.Context()

	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gw-tcp",
			Namespace: "test-ns",
			UID:       types.UID("gw-tcp-uid"),
		},
		Spec: gatewayv1.GatewaySpec{
			Listeners: []gatewayv1.Listener{
				{
					Name:     "http",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
				},
				{
					Name:     "tcp-1",
					Port:     9000,
					Protocol: gatewayv1.TCPProtocolType,
				},
				{
					Name:     "tcp-2",
					Port:     9001,
					Protocol: gatewayv1.TCPProtocolType,
				},
				{
					Name:     "tcp-conflicted",
					Port:     8000,
					Protocol: gatewayv1.TCPProtocolType,
				},
			},
		},
	}

	effectiveListeners := []*state.EffectiveListener{
		{
			Name:     "http",
			Port:     80,
			Protocol: gatewayv1.HTTPProtocolType,
		},
		{
			Name:     "tcp-1",
			Port:     9000,
			Protocol: gatewayv1.TCPProtocolType,
		},
		{
			Name:     "tcp-2",
			Port:     9001,
			Protocol: gatewayv1.TCPProtocolType,
		},
		{
			Name:     "tcp-conflicted",
			Port:     8000,
			Protocol: gatewayv1.TCPProtocolType,
			Conditions: []metav1.Condition{
				{
					Type:   string(gatewayv1.ListenerConditionConflicted),
					Status: metav1.ConditionTrue,
					Reason: string(gatewayv1.ListenerReasonProtocolConflict),
				},
			},
		},
	}

	_, _, err := p.GatewayAddressesWithReadiness(ctx, gw, effectiveListeners)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resName := ResourceNameForGateway("gw-tcp")

	// 1. Verify Service ports
	var svc corev1.Service
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &svc); err != nil {
		t.Fatalf("failed to get created Service: %v", err)
	}

	svcPortMap := make(map[string]corev1.ServicePort)
	for _, sp := range svc.Spec.Ports {
		svcPortMap[sp.Name] = sp
	}

	// Verify HTTP port remapped to 8000
	if sp, ok := svcPortMap["http-80"]; !ok {
		t.Errorf("expected Service port http-80")
	} else {
		if sp.Port != 80 || sp.TargetPort.IntValue() != 8000 || sp.Protocol != corev1.ProtocolTCP {
			t.Errorf("http-80: got port=%d targetPort=%v proto=%s", sp.Port, sp.TargetPort, sp.Protocol)
		}
	}

	// Verify TCP ports mapped to matching target ports
	if sp, ok := svcPortMap["tcp-9000"]; !ok {
		t.Errorf("expected Service port tcp-9000")
	} else {
		if sp.Port != 9000 || sp.TargetPort.IntValue() != 9000 || sp.Protocol != corev1.ProtocolTCP {
			t.Errorf("tcp-9000: got port=%d targetPort=%v proto=%s, want matching targetPort 9000", sp.Port, sp.TargetPort, sp.Protocol)
		}
	}

	if sp, ok := svcPortMap["tcp-9001"]; !ok {
		t.Errorf("expected Service port tcp-9001")
	} else {
		if sp.Port != 9001 || sp.TargetPort.IntValue() != 9001 || sp.Protocol != corev1.ProtocolTCP {
			t.Errorf("tcp-9001: got port=%d targetPort=%v proto=%s, want matching targetPort 9001", sp.Port, sp.TargetPort, sp.Protocol)
		}
	}

	// Verify conflicted TCP listener port 8000 is NOT in Service
	if _, ok := svcPortMap["tcp-8000"]; ok {
		t.Errorf("conflicted port 8000 should NOT be exposed on Service")
	}

	// 2. Verify Deployment container ports
	var deploy appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &deploy); err != nil {
		t.Fatalf("failed to get created Deployment: %v", err)
	}

	containerPorts := deploy.Spec.Template.Spec.Containers[0].Ports
	cpMap := make(map[string]corev1.ContainerPort)
	for _, cp := range containerPorts {
		cpMap[cp.Name] = cp
	}

	// Verify default container ports
	if cp, ok := cpMap["http"]; !ok || cp.ContainerPort != 8000 {
		t.Errorf("expected container port http:8000")
	}
	if cp, ok := cpMap["https"]; !ok || cp.ContainerPort != 8443 {
		t.Errorf("expected container port https:8443")
	}

	// Verify TCP listener container ports
	if cp, ok := cpMap["tcp-9000"]; !ok {
		t.Errorf("expected container port tcp-9000")
	} else if cp.ContainerPort != 9000 || cp.Protocol != corev1.ProtocolTCP {
		t.Errorf("tcp-9000: got containerPort=%d proto=%s, want 9000/TCP", cp.ContainerPort, cp.Protocol)
	}

	if cp, ok := cpMap["tcp-9001"]; !ok {
		t.Errorf("expected container port tcp-9001")
	} else if cp.ContainerPort != 9001 || cp.Protocol != corev1.ProtocolTCP {
		t.Errorf("tcp-9001: got containerPort=%d proto=%s, want 9001/TCP", cp.ContainerPort, cp.Protocol)
	}

	// Verify conflicted TCP listener port 8000 is NOT in Deployment
	if _, ok := cpMap["tcp-8000"]; ok {
		t.Errorf("conflicted port 8000 should NOT be declared on Deployment container ports")
	}
}

func TestSinglePodAddressProvider_UDPListenerServiceAndDeploymentPorts(t *testing.T) {
	c := setupTestClient(t)
	p := NewAddressProvider(c)
	ctx := t.Context()

	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gw-udp",
			Namespace: "test-ns",
			UID:       types.UID("gw-udp-uid"),
		},
		Spec: gatewayv1.GatewaySpec{
			Listeners: []gatewayv1.Listener{
				{
					Name:     "http",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
				},
				{
					Name:     "udp-dns",
					Port:     53,
					Protocol: gatewayv1.UDPProtocolType,
				},
				{
					Name:     "udp-custom",
					Port:     5300,
					Protocol: gatewayv1.UDPProtocolType,
				},
				{
					Name:     "udp-conflicted",
					Port:     8443,
					Protocol: gatewayv1.UDPProtocolType,
				},
			},
		},
	}

	effectiveListeners := []*state.EffectiveListener{
		{
			Name:     "http",
			Port:     80,
			Protocol: gatewayv1.HTTPProtocolType,
		},
		{
			Name:     "udp-dns",
			Port:     53,
			Protocol: gatewayv1.UDPProtocolType,
		},
		{
			Name:     "udp-custom",
			Port:     5300,
			Protocol: gatewayv1.UDPProtocolType,
		},
		{
			Name:     "udp-conflicted",
			Port:     8443,
			Protocol: gatewayv1.UDPProtocolType,
			Conditions: []metav1.Condition{
				{
					Type:   string(gatewayv1.ListenerConditionConflicted),
					Status: metav1.ConditionTrue,
					Reason: string(gatewayv1.ListenerReasonProtocolConflict),
				},
			},
		},
	}

	_, _, err := p.GatewayAddressesWithReadiness(ctx, gw, effectiveListeners)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resName := ResourceNameForGateway("gw-udp")

	// 1. Verify Service ports
	var svc corev1.Service
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &svc); err != nil {
		t.Fatalf("failed to get created Service: %v", err)
	}

	svcPortMap := make(map[string]corev1.ServicePort)
	for _, sp := range svc.Spec.Ports {
		svcPortMap[sp.Name] = sp
	}

	// Verify HTTP port remapped to 8000
	if sp, ok := svcPortMap["http-80"]; !ok {
		t.Errorf("expected Service port http-80")
	} else {
		if sp.Port != 80 || sp.TargetPort.IntValue() != 8000 || sp.Protocol != corev1.ProtocolTCP {
			t.Errorf("http-80: got port=%d targetPort=%v proto=%s", sp.Port, sp.TargetPort, sp.Protocol)
		}
	}

	// Verify UDP ports mapped to matching target ports
	if sp, ok := svcPortMap["udp-53"]; !ok {
		t.Errorf("expected Service port udp-53")
	} else {
		if sp.Port != 53 || sp.TargetPort.IntValue() != 53 || sp.Protocol != corev1.ProtocolUDP {
			t.Errorf("udp-53: got port=%d targetPort=%v proto=%s, want matching targetPort 53", sp.Port, sp.TargetPort, sp.Protocol)
		}
	}

	if sp, ok := svcPortMap["udp-5300"]; !ok {
		t.Errorf("expected Service port udp-5300")
	} else {
		if sp.Port != 5300 || sp.TargetPort.IntValue() != 5300 || sp.Protocol != corev1.ProtocolUDP {
			t.Errorf("udp-5300: got port=%d targetPort=%v proto=%s, want matching targetPort 5300", sp.Port, sp.TargetPort, sp.Protocol)
		}
	}

	// Verify conflicted UDP listener port 8443 is NOT in Service as udp-8443
	if _, ok := svcPortMap["udp-8443"]; ok {
		t.Errorf("conflicted port 8443 should NOT be exposed on Service as udp-8443")
	}

	// 2. Verify Deployment container ports
	var deploy appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &deploy); err != nil {
		t.Fatalf("failed to get created Deployment: %v", err)
	}

	containerPorts := deploy.Spec.Template.Spec.Containers[0].Ports
	cpMap := make(map[string]corev1.ContainerPort)
	for _, cp := range containerPorts {
		cpMap[cp.Name] = cp
	}

	// Verify default container ports
	if cp, ok := cpMap["http"]; !ok || cp.ContainerPort != 8000 {
		t.Errorf("expected container port http:8000")
	}
	if cp, ok := cpMap["https"]; !ok || cp.ContainerPort != 8443 {
		t.Errorf("expected container port https:8443")
	}
	if cp, ok := cpMap["http3"]; !ok || cp.ContainerPort != 8443 || cp.Protocol != corev1.ProtocolUDP {
		t.Errorf("expected container port http3:8443/UDP")
	}

	// Verify UDP listener container ports
	if cp, ok := cpMap["udp-53"]; !ok {
		t.Errorf("expected container port udp-53")
	} else if cp.ContainerPort != 53 || cp.Protocol != corev1.ProtocolUDP {
		t.Errorf("udp-53: got containerPort=%d proto=%s, want 53/UDP", cp.ContainerPort, cp.Protocol)
	}

	if cp, ok := cpMap["udp-5300"]; !ok {
		t.Errorf("expected container port udp-5300")
	} else if cp.ContainerPort != 5300 || cp.Protocol != corev1.ProtocolUDP {
		t.Errorf("udp-5300: got containerPort=%d proto=%s, want 5300/UDP", cp.ContainerPort, cp.Protocol)
	}

	// Verify conflicted UDP listener port 8443 is NOT in Deployment as udp-8443
	if _, ok := cpMap["udp-8443"]; ok {
		t.Errorf("conflicted port 8443 should NOT be declared on Deployment container ports as udp-8443")
	}
}

func TestSinglePodAddressProvider_ForwardedForModeArg(t *testing.T) {
	c := setupTestClient(t)
	ctx := t.Context()
	p := NewAddressProvider(c, WithForwardedForMode("replace"))

	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-gw",
			Namespace: "test-ns",
			UID:       types.UID("test-gw-uid"),
		},
		Spec: gatewayv1.GatewaySpec{
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}

	_, _, err := p.GatewayAddresses(ctx, gw, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	name := ResourceNameForGateway("test-gw")
	var deploy appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: name}, &deploy); err != nil {
		t.Fatalf("failed to get created Deployment: %v", err)
	}

	args := deploy.Spec.Template.Spec.Containers[0].Args
	hasArg := false
	for _, arg := range args {
		if arg == "--forwarded-for-mode=replace" {
			hasArg = true
			break
		}
	}
	if !hasArg {
		t.Errorf("expected container args to include '--forwarded-for-mode=replace', got %v", args)
	}
}

func TestSinglePodAddressProvider_WithholdAddressesUntilDataPlaneReady(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

	st := state.NewState()
	st.SetControllerName("test-controller")

	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "test-class"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "test-controller"},
	}

	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-gw",
			Namespace:  "test-ns",
			UID:        types.UID("gw-uid-123"),
			Generation: 1,
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "test-class",
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}

	st.UpsertGatewayClass(gc)
	st.UpsertGateway(gw)
	st.Recompute()

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(gc, gw).
		WithStatusSubresource(gw).
		WithReturnManagedFields().
		WithInterceptorFuncs(ssaNoOpInterceptor()).
		Build()

	ctx := t.Context()
	p := NewAddressProvider(c)
	st.SetSupportedAddressTypes(p.SupportedAddressTypes())

	r := &controller.GatewayReconciler{
		Client:          c,
		Scheme:          scheme,
		State:           st,
		ControllerName:  "test-controller",
		AddressProvider: p,
	}

	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "test-ns", Name: "test-gw"}}

	// 1. Initial reconcile creates resources: Service has an LB IP, but Deployment has no ready replicas.
	_, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error reconciling gateway: %v", err)
	}

	resName := ResourceNameForGateway("test-gw")
	var svc corev1.Service
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &svc); err != nil {
		t.Fatalf("failed to get Service: %v", err)
	}
	// Simulate MetalLB assigning an LB IP to the Service
	svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "192.0.2.100"}}
	if err := c.Status().Update(ctx, &svc); err != nil {
		t.Fatalf("failed to update Service status: %v", err)
	}

	// Verify at the provider level: Service has an LB IP but Deployment has 0 ready replicas
	addrs, readiness, err := p.GatewayAddressesWithReadiness(ctx, gw, nil)
	if err != nil {
		t.Fatalf("unexpected error from GatewayAddressesWithReadiness: %v", err)
	}
	if len(addrs) != 0 {
		t.Fatalf("expected no addresses when data plane is not ready, got %d", len(addrs))
	}
	if readiness.Ready {
		t.Fatalf("expected readiness.Ready=false when deployment has no ready replicas")
	}
	if readiness.Reason != string(gatewayv1.GatewayReasonAddressNotAssigned) {
		t.Fatalf("expected readiness.Reason=AddressNotAssigned, got %s", readiness.Reason)
	}

	// Reconcile through GatewayReconciler and verify status:
	// Programmed=False with reason AddressNotAssigned, status.addresses empty.
	_, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error reconciling gateway: %v", err)
	}
	st.Recompute()
	_, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error reconciling gateway second time: %v", err)
	}

	var reconciledGW gatewayv1.Gateway
	if err := c.Get(ctx, req.NamespacedName, &reconciledGW); err != nil {
		t.Fatalf("failed to get reconciled gateway: %v", err)
	}
	if len(reconciledGW.Status.Addresses) != 0 {
		t.Fatalf("expected 0 addresses in status, got %+v", reconciledGW.Status.Addresses)
	}
	progCond := state.FindCondition(reconciledGW.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
	if progCond == nil || progCond.Status != metav1.ConditionFalse || progCond.Reason != string(gatewayv1.GatewayReasonAddressNotAssigned) {
		t.Fatalf("expected Programmed=False/AddressNotAssigned, got %+v", progCond)
	}

	// 2. Mark Deployment ready -> address present, Programmed=True
	var deploy appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Namespace: "test-ns", Name: resName}, &deploy); err != nil {
		t.Fatalf("failed to get Deployment: %v", err)
	}
	deploy.Status.ReadyReplicas = 1
	if err := c.Status().Update(ctx, &deploy); err != nil {
		t.Fatalf("failed to update Deployment status: %v", err)
	}

	// Provider level check
	addrs, readiness, err = p.GatewayAddressesWithReadiness(ctx, gw, nil)
	if err != nil {
		t.Fatalf("unexpected error from GatewayAddressesWithReadiness: %v", err)
	}
	if len(addrs) != 1 || addrs[0].Value != "192.0.2.100" {
		t.Fatalf("expected 1 address 192.0.2.100, got %+v", addrs)
	}
	if !readiness.Ready {
		t.Fatalf("expected readiness.Ready=true when deployment is ready")
	}
	if readiness.Reason != string(gatewayv1.GatewayReasonProgrammed) {
		t.Fatalf("expected readiness.Reason=Programmed, got %s", readiness.Reason)
	}

	// Controller reconcile check
	_, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error reconciling gateway: %v", err)
	}
	st.Recompute()
	_, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error reconciling gateway: %v", err)
	}

	if err := c.Get(ctx, req.NamespacedName, &reconciledGW); err != nil {
		t.Fatalf("failed to get reconciled gateway: %v", err)
	}
	if len(reconciledGW.Status.Addresses) != 1 || reconciledGW.Status.Addresses[0].Value != "192.0.2.100" {
		t.Fatalf("expected address 192.0.2.100 in status, got %+v", reconciledGW.Status.Addresses)
	}
	progCond = state.FindCondition(reconciledGW.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
	if progCond == nil || progCond.Status != metav1.ConditionTrue || progCond.Reason != string(gatewayv1.GatewayReasonProgrammed) {
		t.Fatalf("expected Programmed=True/Programmed, got %+v", progCond)
	}

	// 3. Requested-IP-not-bound case unchanged: Programmed=False/AddressNotUsable
	reconciledGW.Spec.Addresses = []gatewayv1.GatewaySpecAddress{
		{
			Type:  state.Ptr(gatewayv1.IPAddressType),
			Value: "192.0.2.200", // different from Service's 192.0.2.100
		},
	}
	if err := c.Update(ctx, &reconciledGW); err != nil {
		t.Fatalf("failed to update gateway with requested address: %v", err)
	}
	st.UpsertGateway(&reconciledGW)
	st.Recompute()

	addrs, readiness, err = p.GatewayAddressesWithReadiness(ctx, &reconciledGW, nil)
	if err != nil {
		t.Fatalf("unexpected error from GatewayAddressesWithReadiness: %v", err)
	}
	if readiness.Ready {
		t.Fatalf("expected readiness.Ready=false when requested IP is not bound")
	}
	if readiness.Reason != string(gatewayv1.GatewayReasonAddressNotUsable) {
		t.Fatalf("expected readiness.Reason=AddressNotUsable, got %s", readiness.Reason)
	}

	_, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error reconciling gateway: %v", err)
	}
	st.Recompute()
	_, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error reconciling gateway: %v", err)
	}

	if err := c.Get(ctx, req.NamespacedName, &reconciledGW); err != nil {
		t.Fatalf("failed to get reconciled gateway: %v", err)
	}
	progCond = state.FindCondition(reconciledGW.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed))
	if progCond == nil || progCond.Status != metav1.ConditionFalse || progCond.Reason != string(gatewayv1.GatewayReasonAddressNotUsable) {
		t.Fatalf("expected Programmed=False/AddressNotUsable, got %+v", progCond)
	}
}
