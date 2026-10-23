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
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/gcp"
	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/metadata"
	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/registrar"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/provisioning/singlepod"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/testenv"
	compute "google.golang.org/api/compute/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrlcfg "sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func setupScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
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

func makeTestPod(t *testing.T, c client.Client, gw *gatewayv1.Gateway, name, nodeName, ip string, extraConditions []corev1.PodCondition) *corev1.Pod {
	drainVol := corev1ac.Volume().
		WithName("drain-volume").
		WithEmptyDir(corev1ac.EmptyDirVolumeSource().WithMedium(corev1.StorageMediumMemory))
	drainMount := corev1ac.VolumeMount().
		WithName("drain-volume").
		WithMountPath("/var/run/gcpgateway")
	preStopCmd := []string{"/gcpgateway", "hook", "pre-stop", "--drain-seconds=35"}

	p := singlepod.NewAddressProvider(
		c,
		singlepod.WithExtraReadinessGates(registrar.LoadBalancerReadyGate),
		singlepod.WithPreStopCommand(preStopCmd...),
		singlepod.WithExtraVolumes([]*corev1ac.VolumeApplyConfiguration{drainVol}, []*corev1ac.VolumeMountApplyConfiguration{drainMount}),
		singlepod.WithTerminationGracePeriodSeconds(60),
	)

	_, _, err := p.GatewayAddressesWithReadiness(t.Context(), gw, nil)
	if err != nil {
		t.Fatalf("failed to reconcile deployment for pod template: %v", err)
	}

	resName := singlepod.ResourceNameForGateway(gw.Name)
	var deploy appsv1.Deployment
	if err := c.Get(t.Context(), types.NamespacedName{Namespace: gw.Namespace, Name: resName}, &deploy); err != nil {
		t.Fatalf("failed to get created Deployment: %v", err)
	}

	pod := testenv.BuildPodFromDeployment(
		&deploy,
		testenv.WithPodName(name),
		testenv.WithNodeName(nodeName),
		testenv.WithPodIP(ip),
	)

	if err := c.Create(t.Context(), pod); err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}

	pod.Status = corev1.PodStatus{
		PodIP:      ip,
		Conditions: extraConditions,
	}
	if err := c.Status().Update(t.Context(), pod); err != nil {
		t.Fatalf("failed to update pod status: %v", err)
	}

	return pod
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
			UID:       types.UID("my-gw-uid"),
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

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1.Pod{}, &gatewayv1.Gateway{}, &appsv1.Deployment{}).
		WithObjects(gw, node).
		Build()

	makeTestPod(t, c, gw, "dp-pod-1", "node-1", "10.0.1.5", []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: corev1.PodConditionType(registrar.LoadBalancerReadyGate), Status: corev1.ConditionFalse},
	})

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

	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter != HealthPollInterval {
		t.Errorf("expected RequeueAfter=%v when UNHEALTHY, got %v", HealthPollInterval, res.RequeueAfter)
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

	res, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter != ResyncInterval {
		t.Errorf("expected RequeueAfter=%v when HEALTHY, got %v", ResyncInterval, res.RequeueAfter)
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
			UID:       types.UID("my-gw-uid"),
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

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1.Pod{}, &gatewayv1.Gateway{}, &appsv1.Deployment{}).
		WithObjects(gw, node).
		Build()

	makeTestPod(t, c, gw, "dp-pod-1", "node-1", "10.0.1.5", []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: corev1.PodConditionType(registrar.LoadBalancerReadyGate), Status: corev1.ConditionFalse},
	})

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
					UID:       types.UID("my-gw-uid"),
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

			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithStatusSubresource(&corev1.Pod{}, &gatewayv1.Gateway{}, &appsv1.Deployment{}).
				WithObjects(gw, node).
				Build()

			makeTestPod(t, c, gw, "dp-pod-1", "node-1", "10.0.1.5", []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			})

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
			UID:               types.UID("gw-old-uid"),
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
			UID:               types.UID("gw-new-uid"),
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

// TestController_ScopedPodStatusPatching verifies that the controller only ever patches
// its own gcpgateway.gke-labs.io/load-balancer-ready condition on this Gateway's data-plane pods,
// leaving other conditions and foreign pods untouched.
func TestController_ScopedPodStatusPatching(t *testing.T) {
	scheme := setupScheme()
	ipAddrType := gatewayv1.IPAddressType
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "my-gw",
			UID:       types.UID("my-gw-uid"),
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

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1.Pod{}, &gatewayv1.Gateway{}, &appsv1.Deployment{}).
		WithObjects(gw, node).
		Build()

	// 1. Data-plane pod for my-gw with existing conditions
	makeTestPod(t, c, gw, "dp-pod-1", "node-1", "10.0.1.5", []corev1.PodCondition{
		{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, Reason: "Scheduled"},
		{Type: corev1.PodInitialized, Status: corev1.ConditionTrue, Reason: "Initialized"},
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, Reason: "ContainersReady"},
		{Type: corev1.PodConditionType(registrar.LoadBalancerReadyGate), Status: corev1.ConditionFalse, Reason: "Waiting"},
	})

	// 2. Foreign pod in same namespace belonging to another service/app
	foreignPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "foreign-backend",
			Labels: map[string]string{
				"app": "unrelated-app",
			},
		},
		Spec: corev1.PodSpec{NodeName: "node-1"},
		Status: corev1.PodStatus{
			PodIP: "10.0.1.99",
			Conditions: []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
	if err := c.Create(t.Context(), foreignPod); err != nil {
		t.Fatalf("failed to create foreign pod: %v", err)
	}

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
			{Instance: "gke-node-1", IpAddress: "10.0.1.5", Port: 8443, HealthState: "HEALTHY"},
		},
	})

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
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Verify my-gw data-plane pod: LoadBalancerReadyGate patched to True, other conditions preserved
	var updatedDPPod corev1.Pod
	_ = c.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "dp-pod-1"}, &updatedDPPod)

	condMap := make(map[corev1.PodConditionType]corev1.ConditionStatus)
	for _, cond := range updatedDPPod.Status.Conditions {
		condMap[cond.Type] = cond.Status
	}

	if condMap[corev1.PodScheduled] != corev1.ConditionTrue {
		t.Errorf("expected PodScheduled preserved as True")
	}
	if condMap[corev1.PodInitialized] != corev1.ConditionTrue {
		t.Errorf("expected PodInitialized preserved as True")
	}
	if condMap[corev1.ContainersReady] != corev1.ConditionTrue {
		t.Errorf("expected ContainersReady preserved as True")
	}
	if condMap[corev1.PodConditionType(registrar.LoadBalancerReadyGate)] != corev1.ConditionTrue {
		t.Errorf("expected LoadBalancerReadyGate patched to True, got %v", condMap[corev1.PodConditionType(registrar.LoadBalancerReadyGate)])
	}

	// Verify foreign pod was NEVER touched
	var updatedForeignPod corev1.Pod
	_ = c.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "foreign-backend"}, &updatedForeignPod)
	if len(updatedForeignPod.Status.Conditions) != 2 {
		t.Errorf("expected foreign pod to retain exactly 2 conditions, got %d", len(updatedForeignPod.Status.Conditions))
	}
	for _, cond := range updatedForeignPod.Status.Conditions {
		if cond.Type == corev1.PodConditionType(registrar.LoadBalancerReadyGate) {
			t.Errorf("foreign pod must NEVER have load-balancer-ready readiness gate patched onto it")
		}
	}
}

type failOnceClient struct {
	client.Client
	failOnce atomic.Bool
}

func (f *failOnceClient) Status() client.SubResourceWriter {
	return &failOnceStatusWriter{
		SubResourceWriter: f.Client.Status(),
		failOnce:          &f.failOnce,
	}
}

type failOnceStatusWriter struct {
	client.SubResourceWriter
	failOnce *atomic.Bool
}

func (w *failOnceStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if _, isPod := obj.(*corev1.Pod); isPod {
		if w.failOnce.CompareAndSwap(false, true) {
			return errors.New("injected transient pod status update error")
		}
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

// TestReadinessGate_PatchRetryOnFailure verifies that if patching a pod's readiness gate fails,
// the error is returned (not dropped), and subsequent reconciliation retries successfully.
func TestReadinessGate_PatchRetryOnFailure(t *testing.T) {
	scheme := setupScheme()
	ipAddrType := gatewayv1.IPAddressType
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "my-gw",
			UID:       types.UID("my-gw-uid"),
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

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1.Pod{}, &gatewayv1.Gateway{}, &appsv1.Deployment{}).
		WithObjects(gw, node).
		Build()

	makeTestPod(t, baseClient, gw, "dp-pod-1", "node-1", "10.0.1.5", []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: corev1.PodConditionType(registrar.LoadBalancerReadyGate), Status: corev1.ConditionFalse},
	})

	c := &failOnceClient{Client: baseClient}

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
			{Instance: "gke-node-1", IpAddress: "10.0.1.5", Port: 8443, HealthState: "HEALTHY"},
		},
	})

	reg := registrar.NewRegistrar(c, fakeGCP, registrar.Config{
		ClusterID: "cluster-1",
		NegPrefix: "gcpgw",
		Project:   "my-project",
		Zones:     []string{"us-central1-a"},
	})

	reconciler := NewGatewayReconciler(c, scheme, fakeGCP, reg, "cluster-1", "gcpgw", "my-project")
	reconciler.Zones = []string{"us-central1-a"}

	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "my-gw"}}

	// First reconcile: injected client error on pod status update -> returns error (not dropped!)
	_, err := reconciler.Reconcile(t.Context(), req)
	if err == nil {
		t.Fatalf("expected Reconcile to return error on gate patch failure, got nil")
	}

	// Verify pod readiness gate was NOT updated to True
	var pod corev1.Pod
	if err := baseClient.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "dp-pod-1"}, &pod); err != nil {
		t.Fatalf("failed to get pod: %v", err)
	}
	var gateCond *corev1.PodCondition
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodConditionType(registrar.LoadBalancerReadyGate) {
			gateCond = &cond
			break
		}
	}
	if gateCond == nil || gateCond.Status != corev1.ConditionFalse {
		t.Errorf("expected readiness gate to remain False after failed patch, got %+v", gateCond)
	}

	// Second reconcile: retry succeeds
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("retried Reconcile failed: %v", err)
	}
	if res.RequeueAfter != ResyncInterval {
		t.Errorf("expected RequeueAfter=%v after success, got %v", ResyncInterval, res.RequeueAfter)
	}

	// Verify readiness gate is now True
	if err := baseClient.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "dp-pod-1"}, &pod); err != nil {
		t.Fatalf("failed to get pod: %v", err)
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodConditionType(registrar.LoadBalancerReadyGate) {
			gateCond = &cond
			break
		}
	}
	if gateCond == nil || gateCond.Status != corev1.ConditionTrue {
		t.Errorf("expected readiness gate to be True after successful retry, got %+v", gateCond)
	}

	// Verify Gateway condition Programmed=True
	var updatedGW gatewayv1.Gateway
	if err := baseClient.Get(t.Context(), req.NamespacedName, &updatedGW); err != nil {
		t.Fatalf("failed to get gateway: %v", err)
	}
	prog := getCondition(&updatedGW, string(gatewayv1.GatewayConditionProgrammed))
	if prog == nil || prog.Status != metav1.ConditionTrue || prog.Reason != ReasonProgrammed {
		t.Errorf("expected Programmed=True after successful retry, got %+v", prog)
	}
}

// TestGateway_PeriodicResyncPrunesOrphanedEndpoints verifies that an orphaned endpoint,
// attached in the fake NEG with no matching pod, is pruned by the periodic resync without any Kubernetes event.
func TestGateway_PeriodicResyncPrunesOrphanedEndpoints(t *testing.T) {
	env := testenv.Start(t)

	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "gcpgateway"},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "example.com/gcpgateway"},
	}
	if err := env.Client.Create(t.Context(), gc); err != nil {
		t.Fatalf("creating GatewayClass: %v", err)
	}

	ipAddrType := gatewayv1.IPAddressType
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "resync-gw",
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
	if err := env.Client.Create(t.Context(), gw); err != nil {
		t.Fatalf("creating Gateway: %v", err)
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
			Labels: map[string]string{
				registrar.ZoneLabelStandard: "us-central1-a",
			},
		},
		Spec: corev1.NodeSpec{ProviderID: "gce://my-project/us-central1-a/gke-node-1"},
	}
	if err := env.Client.Create(t.Context(), node); err != nil {
		t.Fatalf("creating Node: %v", err)
	}

	fakeGCP := gcp.NewFakeComputeClient()
	negDesc, _ := (&metadata.NegMetadata{
		ClusterID:   "cluster-1",
		Namespace:   "default",
		GatewayName: "resync-gw",
		Port:        8443,
	}).Encode()

	negName := metadata.ResourceName("gcpgw", "cluster-1", "default", "resync-gw", "8443")
	fakeGCP.AddNEG("my-project", "us-central1-a", &compute.NetworkEndpointGroup{
		Name:        negName,
		Zone:        "us-central1-a",
		Description: negDesc,
	})

	bsName := metadata.ResourceName("gcpgw", "cluster-1", "default", "resync-gw", "bs-443")
	fakeGCP.SetBackendServiceHealth("my-project", bsName, negName, &compute.BackendServiceGroupHealth{
		HealthStatus: []*compute.HealthStatus{
			{Instance: "gke-node-1", IpAddress: "10.0.1.5", Port: 8443, HealthState: "HEALTHY"},
		},
	})

	ap := singlepod.NewAddressProvider(
		env.Client,
		singlepod.WithExtraReadinessGates(registrar.LoadBalancerReadyGate),
		singlepod.WithTerminationGracePeriodSeconds(60),
	)
	_, _, err := ap.GatewayAddressesWithReadiness(t.Context(), gw, nil)
	if err != nil {
		t.Fatalf("GatewayAddressesWithReadiness failed: %v", err)
	}

	resName := singlepod.ResourceNameForGateway(gw.Name)
	var deploy appsv1.Deployment
	if err := env.Client.Get(t.Context(), types.NamespacedName{Namespace: gw.Namespace, Name: resName}, &deploy); err != nil {
		t.Fatalf("failed to get Deployment: %v", err)
	}

	pod := testenv.BuildPodFromDeployment(
		&deploy,
		testenv.WithPodName("dp-pod-1"),
		testenv.WithNodeName("node-1"),
		testenv.WithPodIP("10.0.1.5"),
		testenv.WithContainersReady(true),
	)
	if err := env.Client.Create(t.Context(), pod); err != nil {
		t.Fatalf("creating pod: %v", err)
	}
	pod.Status.PodIP = "10.0.1.5"
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: corev1.PodConditionType(registrar.LoadBalancerReadyGate), Status: corev1.ConditionTrue},
	}
	if err := env.Client.Status().Update(t.Context(), pod); err != nil {
		t.Fatalf("updating pod status: %v", err)
	}

	skipNameValidation := true
	mgr, err := ctrl.NewManager(env.Config, ctrl.Options{
		Scheme:                 env.Scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller: ctrlcfg.Controller{
			SkipNameValidation: &skipNameValidation,
		},
	})
	if err != nil {
		t.Fatalf("creating manager: %v", err)
	}

	reg := registrar.NewRegistrar(env.Client, fakeGCP, registrar.Config{
		ClusterID: "cluster-1",
		NegPrefix: "gcpgw",
		Project:   "my-project",
		Zones:     []string{"us-central1-a"},
	})

	rec := NewGatewayReconciler(mgr.GetClient(), mgr.GetScheme(), fakeGCP, reg, "cluster-1", "gcpgw", "my-project")
	rec.ControllerName = "example.com/gcpgateway"
	rec.Zones = []string{"us-central1-a"}
	rec.resyncInterval = 200 * time.Millisecond
	rec.healthPollInterval = 200 * time.Millisecond

	if err := rec.SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager failed: %v", err)
	}

	mgrErrCh := make(chan error, 1)
	mgrCtx, cancelMgr := context.WithCancel(t.Context())
	go func() {
		mgrErrCh <- mgr.Start(mgrCtx)
	}()
	t.Cleanup(func() {
		cancelMgr()
		select {
		case err := <-mgrErrCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("manager stopped with unexpected error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Errorf("timed out waiting for manager to exit")
		}
	})

	// 1. Wait for steady state: pod endpoint attached to NEG and Gateway Programmed=True
	for start := time.Now(); time.Since(start) < 60*time.Second; time.Sleep(50 * time.Millisecond) {
		eps, _ := fakeGCP.ListNetworkEndpoints(t.Context(), "my-project", "us-central1-a", negName)
		var updatedGW gatewayv1.Gateway
		_ = env.Client.Get(t.Context(), types.NamespacedName{Namespace: gw.Namespace, Name: gw.Name}, &updatedGW)
		prog := getCondition(&updatedGW, string(gatewayv1.GatewayConditionProgrammed))
		if len(eps) == 1 && prog != nil && prog.Status == metav1.ConditionTrue {
			break
		}
	}

	// 2. Attach an orphaned endpoint directly to the fake NEG (e.g. leftover from node crash)
	orphanEP := &compute.NetworkEndpoint{
		Instance:  "gke-node-crashed",
		IpAddress: "10.0.99.88",
		Port:      8443,
	}
	if err := fakeGCP.AttachNetworkEndpoints(t.Context(), "my-project", "us-central1-a", negName, []*compute.NetworkEndpoint{orphanEP}); err != nil {
		t.Fatalf("failed to attach orphan endpoint: %v", err)
	}

	currentEps, err := fakeGCP.ListNetworkEndpoints(t.Context(), "my-project", "us-central1-a", negName)
	if err != nil || len(currentEps) != 2 {
		t.Fatalf("expected 2 endpoints in NEG after injecting orphan, got %d (err: %v)", len(currentEps), err)
	}

	// 3. Assert that periodic resync prunes the orphaned endpoint WITHOUT ANY Kubernetes event
	var finalEps []*compute.NetworkEndpoint
	for start := time.Now(); time.Since(start) < 60*time.Second; time.Sleep(50 * time.Millisecond) {
		finalEps, _ = fakeGCP.ListNetworkEndpoints(t.Context(), "my-project", "us-central1-a", negName)
		if len(finalEps) == 1 {
			break
		}
	}

	if len(finalEps) != 1 || finalEps[0].IpAddress != "10.0.1.5" {
		t.Errorf("expected orphaned endpoint to be pruned by periodic resync, remaining: %+v", finalEps)
	}
}
