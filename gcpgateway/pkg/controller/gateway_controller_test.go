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

package controller

import (
	"testing"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/gcp"
	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/metadata"
	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/registrar"
	compute "google.golang.org/api/compute/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func setupScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = gatewayv1.Install(s)
	return s
}

func getCondition(gw *gatewayv1.Gateway, condType string) *metav1.Condition {
	for _, c := range gw.Status.Conditions {
		if c.Type == condType {
			return &c
		}
	}
	return nil
}

// TestReadinessGate_SetOnlyAfterHealthy verifies that the pod readiness gate is set to True
// only after fake getHealth reports HEALTHY, and sets Gateway condition Programmed=True.
func TestReadinessGate_SetOnlyAfterHealthy(t *testing.T) {
	scheme := setupScheme()
	ipAddrType := gatewayv1.IPAddressType
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "my-gw",
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "gcpgateway",
			Addresses: []gatewayv1.GatewaySpecAddress{
				{
					Type:  &ipAddrType,
					Value: "34.120.254.10",
				},
			},
			Listeners: []gatewayv1.Listener{
				{
					Name:     "https",
					Port:     8443,
					Protocol: gatewayv1.HTTPSProtocolType,
				},
			},
		},
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
		Spec:       corev1.NodeSpec{ProviderID: "gce://my-project/us-central1-a/gke-node-1"},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "dp-pod-1",
			Labels: map[string]string{
				"app":                                    registrar.AppLabelDataplane,
				"gateway.networking.k8s.io/gateway-name": "my-gw",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			ReadinessGates: []corev1.PodReadinessGate{
				{ConditionType: corev1.PodConditionType(registrar.LoadBalancerReadyGate)},
			},
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.1.5",
			Conditions: []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
				{Type: corev1.PodConditionType(registrar.LoadBalancerReadyGate), Status: corev1.ConditionFalse},
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(gw, node, pod).
		WithStatusSubresource(gw, pod).
		Build()

	fakeGCP := gcp.NewFakeComputeClient()
	negDesc, _ := (&metadata.NegMetadata{
		ClusterID:   "cluster-1",
		Namespace:   "default",
		GatewayName: "my-gw",
		Port:        8443,
	}).Encode()

	negName := metadata.ResourceName("gcpgw", "cluster-1", "default", "my-gw", "8443")
	fakeGCP.AddNEG("my-project", "us-central1-a", &compute.NetworkEndpointGroup{
		Name:        negName,
		Zone:        "us-central1-a",
		Description: negDesc,
	})

	bsName := metadata.ResourceName("gcpgw", "cluster-1", "default", "my-gw", "bs-443")

	reg := registrar.NewRegistrar(c, fakeGCP, registrar.Config{
		ClusterID: "cluster-1",
		NegPrefix: "gcpgw",
		Project:   "my-project",
		Zones:     []string{"us-central1-a"},
	})

	reconciler := NewGatewayReconciler(c, scheme, fakeGCP, reg, "cluster-1", "gcpgw", "my-project")
	reconciler.Zones = []string{"us-central1-a"}

	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "my-gw"}}

	// 1. Initial reconcile: getHealth reports UNHEALTHY
	fakeGCP.SetBackendServiceHealth("my-project", bsName, negName, &compute.BackendServiceGroupHealth{
		HealthStatus: []*compute.HealthStatus{
			{Instance: "gke-node-1", IpAddress: "10.0.1.5", Port: 8443, HealthState: "UNHEALTHY"},
		},
	})

	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Verify pod readiness gate is still False
	var updatedPod corev1.Pod
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "dp-pod-1"}, &updatedPod); err != nil {
		t.Fatalf("failed to get pod: %v", err)
	}
	var gateCond *corev1.PodCondition
	for _, cond := range updatedPod.Status.Conditions {
		if cond.Type == corev1.PodConditionType(registrar.LoadBalancerReadyGate) {
			gateCond = &cond
			break
		}
	}
	if gateCond == nil || gateCond.Status != corev1.ConditionFalse {
		t.Errorf("expected readiness gate to be False when UNHEALTHY, got %+v", gateCond)
	}

	// Verify Gateway condition is WaitingForEndpoints
	var updatedGW gatewayv1.Gateway
	_ = c.Get(t.Context(), req.NamespacedName, &updatedGW)
	prog := getCondition(&updatedGW, string(gatewayv1.GatewayConditionProgrammed))
	if prog == nil || prog.Reason != ReasonWaitingForEndpoints {
		t.Errorf("expected Programmed=False with Reason WaitingForEndpoints, got %+v", prog)
	}

	// 2. Health check passes: getHealth reports HEALTHY
	fakeGCP.SetBackendServiceHealth("my-project", bsName, negName, &compute.BackendServiceGroupHealth{
		HealthStatus: []*compute.HealthStatus{
			{Instance: "gke-node-1", IpAddress: "10.0.1.5", Port: 8443, HealthState: "HEALTHY"},
		},
	})

	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Verify pod readiness gate was updated to True!
	_ = c.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "dp-pod-1"}, &updatedPod)
	for _, cond := range updatedPod.Status.Conditions {
		if cond.Type == corev1.PodConditionType(registrar.LoadBalancerReadyGate) {
			gateCond = &cond
			break
		}
	}
	if gateCond == nil || gateCond.Status != corev1.ConditionTrue {
		t.Errorf("expected readiness gate to be True when HEALTHY, got %+v", gateCond)
	}

	// Verify Gateway condition is Programmed=True!
	_ = c.Get(t.Context(), req.NamespacedName, &updatedGW)
	prog = getCondition(&updatedGW, string(gatewayv1.GatewayConditionProgrammed))
	if prog == nil || prog.Status != metav1.ConditionTrue || prog.Reason != ReasonProgrammed {
		t.Errorf("expected Programmed=True with Reason Programmed, got %+v", prog)
	}
	if len(updatedGW.Status.Addresses) != 1 || updatedGW.Status.Addresses[0].Value != "34.120.254.10" {
		t.Errorf("expected Gateway address 34.120.254.10, got %+v", updatedGW.Status.Addresses)
	}
}

// TestRolloutStalled_ReportsConditionWhenUnhealthy verifies that the Gateway reports RolloutStalled
// when a pod remains held by the readiness gate and never becomes healthy.
func TestRolloutStalled_ReportsConditionWhenUnhealthy(t *testing.T) {
	scheme := setupScheme()
	ipAddrType := gatewayv1.IPAddressType
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "my-gw",
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "gcpgateway",
			Addresses: []gatewayv1.GatewaySpecAddress{
				{
					Type:  &ipAddrType,
					Value: "34.120.254.10",
				},
			},
			Listeners: []gatewayv1.Listener{
				{
					Name:     "https",
					Port:     8443,
					Protocol: gatewayv1.HTTPSProtocolType,
				},
			},
		},
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
		Spec:       corev1.NodeSpec{ProviderID: "gce://my-project/us-central1-a/gke-node-1"},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "dp-pod-1",
			Labels: map[string]string{
				"app":                                    registrar.AppLabelDataplane,
				"gateway.networking.k8s.io/gateway-name": "my-gw",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			ReadinessGates: []corev1.PodReadinessGate{
				{ConditionType: corev1.PodConditionType(registrar.LoadBalancerReadyGate)},
			},
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.1.5",
			Conditions: []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
				{Type: corev1.PodConditionType(registrar.LoadBalancerReadyGate), Status: corev1.ConditionFalse},
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(gw, node, pod).
		WithStatusSubresource(gw, pod).
		Build()

	fakeGCP := gcp.NewFakeComputeClient()
	negDesc, _ := (&metadata.NegMetadata{
		ClusterID:   "cluster-1",
		Namespace:   "default",
		GatewayName: "my-gw",
		Port:        8443,
	}).Encode()

	negName := metadata.ResourceName("gcpgw", "cluster-1", "default", "my-gw", "8443")
	fakeGCP.AddNEG("my-project", "us-central1-a", &compute.NetworkEndpointGroup{
		Name:        negName,
		Zone:        "us-central1-a",
		Description: negDesc,
	})

	bsName := metadata.ResourceName("gcpgw", "cluster-1", "default", "my-gw", "bs-443")
	fakeGCP.SetBackendServiceHealth("my-project", bsName, negName, &compute.BackendServiceGroupHealth{
		HealthStatus: []*compute.HealthStatus{
			{Instance: "gke-node-1", IpAddress: "10.0.1.5", Port: 8443, HealthState: "UNHEALTHY"},
		},
	})

	reg := registrar.NewRegistrar(c, fakeGCP, registrar.Config{
		ClusterID: "cluster-1",
		NegPrefix: "gcpgw",
		Project:   "my-project",
		Zones:     []string{"us-central1-a"},
	})

	reconciler := NewGatewayReconciler(c, scheme, fakeGCP, reg, "cluster-1", "gcpgw", "my-project")
	// Health timeout set to 1 millisecond so it triggers RolloutStalled immediately on second reconcile
	reconciler.HealthTimeout = 1 * time.Millisecond
	reconciler.Zones = []string{"us-central1-a"}

	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "my-gw"}}

	// First reconcile starts the timer
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("first Reconcile failed: %v", err)
	}

	time.Sleep(5 * time.Millisecond)

	// Second reconcile: timeout elapsed, pod still not healthy -> RolloutStalled
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("second Reconcile failed: %v", err)
	}

	var updatedGW gatewayv1.Gateway
	_ = c.Get(t.Context(), req.NamespacedName, &updatedGW)
	prog := getCondition(&updatedGW, string(gatewayv1.GatewayConditionProgrammed))
	if prog == nil || prog.Status != metav1.ConditionFalse || prog.Reason != ReasonRolloutStalled {
		t.Fatalf("expected Programmed=False with Reason RolloutStalled, got %+v", prog)
	}
}

// TestGateway_ErrorConditions verifies that Compute API errors produce expected condition reasons.
func TestGateway_ErrorConditions(t *testing.T) {
	tests := []struct {
		name           string
		injectError    error
		expectedReason string
	}{
		{"PERMISSION_DENIED", gcp.ErrPermissionDenied, ReasonPermissionDenied},
		{"QuotaExceeded", gcp.ErrQuotaExceeded, ReasonQuotaExceeded},
		{"NEGNotFound", gcp.ErrNEGNotFound, ReasonNEGNotFound},
		{"InvalidIP", gcp.ErrInvalidIP, ReasonUnsupportedCluster},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scheme := setupScheme()
			ipAddrType := gatewayv1.IPAddressType
			gw := &gatewayv1.Gateway{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "my-gw",
				},
				Spec: gatewayv1.GatewaySpec{
					GatewayClassName: "gcpgateway",
					Addresses: []gatewayv1.GatewaySpecAddress{
						{Type: &ipAddrType, Value: "34.120.254.10"},
					},
					Listeners: []gatewayv1.Listener{
						{Name: "https", Port: 8443, Protocol: gatewayv1.HTTPSProtocolType},
					},
				},
			}

			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
				Spec:       corev1.NodeSpec{ProviderID: "gce://my-project/us-central1-a/gke-node-1"},
			}

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "dp-pod-1",
					Labels: map[string]string{
						"app":                                    registrar.AppLabelDataplane,
						"gateway.networking.k8s.io/gateway-name": "my-gw",
					},
				},
				Spec: corev1.PodSpec{NodeName: "node-1"},
				Status: corev1.PodStatus{
					PodIP: "10.0.1.5",
					Conditions: []corev1.PodCondition{
						{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
					},
				},
			}

			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(gw, node, pod).
				WithStatusSubresource(gw).
				Build()

			fakeGCP := gcp.NewFakeComputeClient()
			negDesc, _ := (&metadata.NegMetadata{
				ClusterID:   "cluster-1",
				Namespace:   "default",
				GatewayName: "my-gw",
				Port:        8443,
			}).Encode()

			negName := metadata.ResourceName("gcpgw", "cluster-1", "default", "my-gw", "8443")
			fakeGCP.AddNEG("my-project", "us-central1-a", &compute.NetworkEndpointGroup{
				Name:        negName,
				Zone:        "us-central1-a",
				Description: negDesc,
			})
			fakeGCP.InjectAttachError = tc.injectError

			reg := registrar.NewRegistrar(c, fakeGCP, registrar.Config{
				ClusterID: "cluster-1",
				NegPrefix: "gcpgw",
				Project:   "my-project",
				Zones:     []string{"us-central1-a"},
			})

			reconciler := NewGatewayReconciler(c, scheme, fakeGCP, reg, "cluster-1", "gcpgw", "my-project")
			reconciler.Zones = []string{"us-central1-a"}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "my-gw"}}
			_, err := reconciler.Reconcile(t.Context(), req)
			if err != nil {
				t.Fatalf("unexpected controller crash on GCP error: %v", err)
			}

			var updatedGW gatewayv1.Gateway
			_ = c.Get(t.Context(), req.NamespacedName, &updatedGW)
			prog := getCondition(&updatedGW, string(gatewayv1.GatewayConditionProgrammed))
			if prog == nil || prog.Status != metav1.ConditionFalse || prog.Reason != tc.expectedReason {
				t.Errorf("expected condition Reason %q, got %+v", tc.expectedReason, prog)
			}
		})
	}
}

// TestGateway_ConflictingGateways verifies that two Gateways targeting identical NEGs resolve deterministically.
func TestGateway_ConflictingGateways(t *testing.T) {
	scheme := setupScheme()
	t0 := time.Now()
	t1 := t0.Add(10 * time.Minute)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "shared-params",
		},
		Data: map[string]string{
			"negHTTPS": "gcpgw-shared-8443",
		},
	}

	paramsRef := &gatewayv1.LocalParametersReference{
		Group: "",
		Kind:  "ConfigMap",
		Name:  "shared-params",
	}

	gwOld := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "default",
			Name:              "gw-old",
			CreationTimestamp: metav1.NewTime(t0),
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "gcpgateway",
			Infrastructure: &gatewayv1.GatewayInfrastructure{
				ParametersRef: paramsRef,
			},
		},
	}

	gwNew := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "default",
			Name:              "gw-new",
			CreationTimestamp: metav1.NewTime(t1),
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "gcpgateway",
			Infrastructure: &gatewayv1.GatewayInfrastructure{
				ParametersRef: paramsRef,
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cm, gwOld, gwNew).
		WithStatusSubresource(gwOld, gwNew).
		Build()

	fakeGCP := gcp.NewFakeComputeClient()
	reg := registrar.NewRegistrar(c, fakeGCP, registrar.Config{
		ClusterID: "cluster-1",
		NegPrefix: "gcpgw",
		Project:   "my-project",
	})

	reconciler := NewGatewayReconciler(c, scheme, fakeGCP, reg, "cluster-1", "gcpgw", "my-project")

	// Reconcile gwNew
	reqNew := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "gw-new"}}
	_, err := reconciler.Reconcile(t.Context(), reqNew)
	if err != nil {
		t.Fatalf("Reconcile gw-new failed: %v", err)
	}

	var updatedNew gatewayv1.Gateway
	_ = c.Get(t.Context(), reqNew.NamespacedName, &updatedNew)
	acc := getCondition(&updatedNew, string(gatewayv1.GatewayConditionAccepted))
	if acc == nil || acc.Status != metav1.ConditionFalse || acc.Reason != ReasonConflictingParameters {
		t.Errorf("expected gw-new to have Accepted=False with ConflictingParameters, got %+v", acc)
	}
}
