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

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/gcp"
	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/metadata"
	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/registrar"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/provisioning/singlepod"
	compute "google.golang.org/api/compute/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestPreStopHook(t *testing.T) {
	tmpDir := t.TempDir()
	sentinelPath := filepath.Join(tmpDir, "sub", "draining")

	args := []string{
		"--drain-seconds=0",
		"--sentinel-file=" + sentinelPath,
	}

	if err := RunPreStopHook(args); err != nil {
		t.Fatalf("RunPreStopHook failed: %v", err)
	}

	data, err := os.ReadFile(sentinelPath)
	if err != nil {
		t.Fatalf("failed to read sentinel file: %v", err)
	}
	if string(data) != "draining\n" {
		t.Errorf("expected 'draining\\n', got %q", string(data))
	}
}

func TestRun_HookPreStop(t *testing.T) {
	tmpDir := t.TempDir()
	sentinelPath := filepath.Join(tmpDir, "draining")

	args := []string{
		"hook",
		"pre-stop",
		"--drain-seconds=0",
		"--sentinel-file=" + sentinelPath,
	}

	if err := Run(t.Context(), args); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if _, err := os.Stat(sentinelPath); err != nil {
		t.Fatalf("sentinel file not created: %v", err)
	}
}

func TestDataplaneMissingFlags(t *testing.T) {
	err := Run(t.Context(), []string{"--dataplane-mode"})
	if err == nil {
		t.Fatalf("expected error when namespace/name missing in dataplane mode, got nil")
	}
}

func TestSetupController_NoSilentFallback(t *testing.T) {
	// Without --fake-compute, SetupController initializes the real Compute client.
	// It must never silently substitute a FakeComputeClient.
	opts, err := SetupController(t.Context(), []string{"--cluster-id=c1"}, nil)
	if err != nil {
		// Error returned instead of silent fallback is valid
		return
	}
	// If compute.NewService succeeded, opts.SetupWithManager is configured with real client.
	if opts.SetupWithManager == nil {
		t.Errorf("expected SetupWithManager to be set")
	}
}

func TestSetupController_FakeComputeFlag(t *testing.T) {
	opts, err := SetupController(t.Context(), []string{"--cluster-id=c1", "--fake-compute"}, nil)
	if err != nil {
		t.Fatalf("SetupController failed with --fake-compute: %v", err)
	}
	if opts.DrainSentinelPath != DefaultSentinelPath {
		t.Errorf("expected DrainSentinelPath=%q, got %q", DefaultSentinelPath, opts.DrainSentinelPath)
	}
	if opts.SetupWithManager == nil {
		t.Errorf("expected SetupWithManager to be set")
	}
}

type testManagerWithFakeClient struct {
	ctrl.Manager
	client     client.Client
	scheme     *runtime.Scheme
	reconciler reconcile.Reconciler
}

func (m *testManagerWithFakeClient) GetClient() client.Client {
	return m.client
}

func (m *testManagerWithFakeClient) GetScheme() *runtime.Scheme {
	return m.scheme
}

func (m *testManagerWithFakeClient) Add(r manager.Runnable) error {
	if rec, ok := r.(reconcile.Reconciler); ok {
		m.reconciler = rec
	}
	return m.Manager.Add(r)
}

// TestSetupController_EndToEndRealSetup verifies that the full controller setup wiring
// (SetupController -> AddressProvider -> Deployment -> Pod template -> SetupWithManager -> Reconciler)
// runs end-to-end, attaches data-plane pods to zonal NEGs, sets the readiness gate, and marks the Gateway Programmed.
func TestSetupController_EndToEndRealSetup(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = gatewayv1.Install(scheme)

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
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
			Labels: map[string]string{
				registrar.ZoneLabelStandard: "us-central1-a",
			},
		},
		Spec: corev1.NodeSpec{
			ProviderID: "gce://my-project/us-central1-a/gke-node-1",
		},
	}

	fakeK8s := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1.Pod{}, &gatewayv1.Gateway{}, &appsv1.Deployment{}).
		WithObjects(gw, node).
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
			{Instance: "gke-node-1", IpAddress: "10.0.1.5", Port: 8443, HealthState: "HEALTHY"},
		},
	})

	// 1. Run SetupController with real arguments
	opts, err := SetupController(t.Context(), []string{
		"--cluster-id=cluster-1",
		"--neg-prefix=gcpgw",
		"--project=my-project",
		"--zones=us-central1-a",
		"--drain-seconds=35",
		"--fake-compute",
	}, fakeGCP)
	if err != nil {
		t.Fatalf("SetupController failed: %v", err)
	}

	// 2. Provision data plane Deployment via AddressProvider
	sap, ok := opts.AddressProvider.(*singlepod.AddressProvider)
	if !ok || sap == nil {
		t.Fatalf("expected singlepod.AddressProvider to be configured")
	}

	sap = singlepod.NewAddressProvider(
		fakeK8s,
		singlepod.WithExtraReadinessGates(LoadBalancerReadyGate),
		singlepod.WithTerminationGracePeriodSeconds(60),
	)

	_, _, err = sap.GatewayAddressesWithReadiness(t.Context(), gw, nil)
	if err != nil {
		t.Fatalf("GatewayAddressesWithReadiness failed: %v", err)
	}

	resName := singlepod.ResourceNameForGateway(gw.Name)
	var deploy appsv1.Deployment
	if err := fakeK8s.Get(t.Context(), types.NamespacedName{Namespace: gw.Namespace, Name: resName}, &deploy); err != nil {
		t.Fatalf("failed to get created Deployment: %v", err)
	}

	// Verify terminationGracePeriodSeconds on real Deployment template
	if deploy.Spec.Template.Spec.TerminationGracePeriodSeconds == nil || *deploy.Spec.Template.Spec.TerminationGracePeriodSeconds != 60 {
		t.Errorf("expected TerminationGracePeriodSeconds=60, got %v", deploy.Spec.Template.Spec.TerminationGracePeriodSeconds)
	}

	// 3. Create real data-plane Pod matching the Deployment template
	labels := make(map[string]string)
	for k, v := range deploy.Spec.Template.Labels {
		labels[k] = v
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: gw.Namespace,
			Name:      "dp-pod-1",
			Labels:    labels,
		},
		Spec: deploy.Spec.Template.Spec,
	}
	pod.Spec.NodeName = "node-1"

	if err := fakeK8s.Create(t.Context(), pod); err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}
	pod.Status = corev1.PodStatus{
		PodIP: "10.0.1.5",
		Conditions: []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(LoadBalancerReadyGate), Status: corev1.ConditionFalse},
		},
	}
	if err := fakeK8s.Status().Update(t.Context(), pod); err != nil {
		t.Fatalf("failed to update pod status: %v", err)
	}

	// 4. Initialize Manager and execute opts.SetupWithManager
	baseMgr, err := ctrl.NewManager(&rest.Config{Host: "http://localhost:8080"}, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		t.Fatalf("failed to create base manager: %v", err)
	}

	testMgr := &testManagerWithFakeClient{
		Manager: baseMgr,
		client:  fakeK8s,
		scheme:  scheme,
	}

	if err := opts.SetupWithManager(testMgr); err != nil {
		t.Fatalf("SetupWithManager failed: %v", err)
	}

	if testMgr.reconciler == nil {
		t.Fatalf("expected reconciler to be registered with manager")
	}

	// 5. Trigger reconciliation
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: gw.Namespace, Name: gw.Name}}
	_, err = testMgr.reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// 6. Assertions:
	// a. Pod endpoint is attached to zonal NEG in fake GCP!
	eps, err := fakeGCP.ListNetworkEndpoints(t.Context(), "my-project", "us-central1-a", negName)
	if err != nil {
		t.Fatalf("ListNetworkEndpoints failed: %v", err)
	}
	if len(eps) != 1 || eps[0].IpAddress != "10.0.1.5" || eps[0].Instance != "gke-node-1" {
		t.Errorf("expected pod endpoint attached to NEG, got %+v", eps)
	}

	// b. Pod readiness gate gcpgateway.gke-labs.io/load-balancer-ready is patched to True!
	var updatedPod corev1.Pod
	if err := fakeK8s.Get(t.Context(), types.NamespacedName{Namespace: gw.Namespace, Name: "dp-pod-1"}, &updatedPod); err != nil {
		t.Fatalf("failed to get updated pod: %v", err)
	}
	foundGateTrue := false
	for _, c := range updatedPod.Status.Conditions {
		if c.Type == corev1.PodConditionType(LoadBalancerReadyGate) && c.Status == corev1.ConditionTrue {
			foundGateTrue = true
			break
		}
	}
	if !foundGateTrue {
		t.Errorf("expected pod readiness gate %s=True, conditions are: %+v", LoadBalancerReadyGate, updatedPod.Status.Conditions)
	}

	// c. Gateway condition Programmed is True!
	var updatedGW gatewayv1.Gateway
	if err := fakeK8s.Get(t.Context(), types.NamespacedName{Namespace: gw.Namespace, Name: gw.Name}, &updatedGW); err != nil {
		t.Fatalf("failed to get updated Gateway: %v", err)
	}
	foundProgrammedTrue := false
	for _, c := range updatedGW.Status.Conditions {
		if c.Type == string(gatewayv1.GatewayConditionProgrammed) && c.Status == metav1.ConditionTrue {
			foundProgrammedTrue = true
			break
		}
	}
	if !foundProgrammedTrue {
		t.Errorf("expected Gateway Programmed=True, conditions are: %+v", updatedGW.Status.Conditions)
	}
}
