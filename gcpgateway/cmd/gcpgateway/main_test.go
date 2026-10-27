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
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/gcp"
	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/metadata"
	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/registrar"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/gari"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/provisioning/singlepod"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/testenv"
	compute "google.golang.org/api/compute/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
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

func assertProductionDeployment(t *testing.T, deploy *appsv1.Deployment, expectedDrainSeconds int, expectedGracePeriod int64) {
	t.Helper()

	// 1. TerminationGracePeriodSeconds derived from --drain-seconds
	if deploy.Spec.Template.Spec.TerminationGracePeriodSeconds == nil {
		t.Errorf("expected TerminationGracePeriodSeconds to be set, got nil")
	} else if *deploy.Spec.Template.Spec.TerminationGracePeriodSeconds != expectedGracePeriod {
		t.Errorf("expected TerminationGracePeriodSeconds=%d, got %d", expectedGracePeriod, *deploy.Spec.Template.Spec.TerminationGracePeriodSeconds)
	}

	// 2. Pre-stop command
	if len(deploy.Spec.Template.Spec.Containers) == 0 {
		t.Fatalf("expected at least 1 container in Deployment template, got 0")
	}
	c := deploy.Spec.Template.Spec.Containers[0]
	if c.Lifecycle == nil || c.Lifecycle.PreStop == nil || c.Lifecycle.PreStop.Exec == nil {
		t.Fatalf("expected Lifecycle.PreStop.Exec to be configured on container")
	}
	expectedPreStop := []string{
		"/gcpgateway",
		"hook",
		"pre-stop",
		fmt.Sprintf("--drain-seconds=%d", expectedDrainSeconds),
	}
	if !slices.Equal(c.Lifecycle.PreStop.Exec.Command, expectedPreStop) {
		t.Errorf("expected pre-stop command %v, got %v", expectedPreStop, c.Lifecycle.PreStop.Exec.Command)
	}

	// 3. Drain volume (memory-backed EmptyDir)
	var foundVol bool
	for _, v := range deploy.Spec.Template.Spec.Volumes {
		if v.Name == "drain-volume" {
			foundVol = true
			if v.EmptyDir == nil || v.EmptyDir.Medium != corev1.StorageMediumMemory {
				t.Errorf("expected drain-volume to have memory-backed EmptyDir, got %+v", v.EmptyDir)
			}
			break
		}
	}
	if !foundVol {
		t.Errorf("expected 'drain-volume' in Deployment template volumes")
	}

	// 4. Drain volume mount (/var/run/gcpgateway)
	var foundMount bool
	for _, m := range c.VolumeMounts {
		if m.Name == "drain-volume" {
			foundMount = true
			if m.MountPath != "/var/run/gcpgateway" {
				t.Errorf("expected drain-volume mount path '/var/run/gcpgateway', got %q", m.MountPath)
			}
			break
		}
	}
	if !foundMount {
		t.Errorf("expected 'drain-volume' mount in container volumeMounts")
	}

	// 5. Readiness gate
	var foundGate bool
	for _, g := range deploy.Spec.Template.Spec.ReadinessGates {
		if g.ConditionType == corev1.PodConditionType(LoadBalancerReadyGate) {
			foundGate = true
			break
		}
	}
	if !foundGate {
		t.Errorf("expected readiness gate %s in Deployment template, got %+v", LoadBalancerReadyGate, deploy.Spec.Template.Spec.ReadinessGates)
	}
}

// TestSetupController_EndToEndRealSetup verifies that the full controller setup wiring
// (SetupController -> AddressProvider -> Deployment -> Pod template -> SetupWithManager -> Reconciler)
// runs against an envtest control plane, attaches data-plane pods to zonal NEGs,
// sets the readiness gate, and marks the Gateway Programmed.
func TestSetupController_EndToEndRealSetup(t *testing.T) {
	env := testenv.Start(t)

	// Create GatewayClass
	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gcpgateway",
		},
		Spec: gatewayv1.GatewayClassSpec{
			ControllerName: DefaultControllerName,
		},
	}
	if err := env.Client.Create(t.Context(), gc); err != nil {
		t.Fatalf("creating GatewayClass: %v", err)
	}

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
		Spec: corev1.NodeSpec{
			ProviderID: "gce://my-project/us-central1-a/gke-node-1",
		},
	}
	if err := env.Client.Create(t.Context(), node); err != nil {
		t.Fatalf("creating Node: %v", err)
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
	// Initially, load balancer health check reports UNHEALTHY.
	fakeGCP.SetBackendServiceHealth("my-project", bsName, negName, &compute.BackendServiceGroupHealth{
		HealthStatus: []*compute.HealthStatus{
			{Instance: "gke-node-1", IpAddress: "10.0.1.5", Port: 8443, HealthState: "UNHEALTHY"},
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

	trueVal := true
	mgr, err := ctrl.NewManager(env.Config, ctrl.Options{
		Scheme:                 env.Scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller: config.Controller{
			SkipNameValidation: &trueVal,
		},
	})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	opts.Manager = mgr

	server, err := gari.New(opts)
	if err != nil {
		t.Fatalf("gari.New failed: %v", err)
	}

	// 2. Start GARI manager in background; GARI supplies the client,
	// executes opts.SetupWithManager, and provisions the Deployment.
	mgrCtx, cancelMgr := context.WithCancel(t.Context())
	defer cancelMgr()
	mgrErrCh := make(chan error, 1)
	go func() {
		if err := server.Manager().Start(mgrCtx); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("manager exited with error: %v", err)
			mgrErrCh <- err
		}
	}()
	t.Cleanup(func() {
		cancelMgr()
		select {
		case err := <-mgrErrCh:
			t.Errorf("manager error reported during cleanup: %v", err)
		default:
		}
	})

	resName := singlepod.ResourceNameForGateway(gw.Name)
	var deploy appsv1.Deployment
	for start := time.Now(); time.Since(start) < 15*time.Second; time.Sleep(100 * time.Millisecond) {
		if err := env.Client.Get(t.Context(), types.NamespacedName{Namespace: gw.Namespace, Name: resName}, &deploy); err == nil {
			break
		}
	}
	if deploy.Name == "" {
		t.Fatalf("timed out waiting for Deployment %s to be created by GARI", resName)
	}

	// Assert production-derived properties on real Deployment template (drain-seconds=35 -> grace period 60)
	assertProductionDeployment(t, &deploy, 35, 60)

	// 3. Create real data-plane Pod matching the Deployment template via testenv.BuildPodFromDeployment
	pod := testenv.BuildPodFromDeployment(
		&deploy,
		testenv.WithPodName("dp-pod-1"),
		testenv.WithNodeName("node-1"),
		testenv.WithPodIP("10.0.1.5"),
		testenv.WithContainersReady(true),
	)
	// Readiness gate starts false
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:   corev1.PodConditionType(LoadBalancerReadyGate),
		Status: corev1.ConditionFalse,
	})

	if err := env.Client.Create(t.Context(), pod); err != nil {
		t.Fatalf("creating pod: %v", err)
	}
	pod.Status.PodIP = "10.0.1.5"
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: corev1.PodConditionType(LoadBalancerReadyGate), Status: corev1.ConditionFalse},
	}
	if err := env.Client.Status().Update(t.Context(), pod); err != nil {
		t.Fatalf("updating pod status: %v", err)
	}

	// 4. Assertions:
	// a. Verify endpoint is attached to zonal NEG while UNHEALTHY
	var (
		attachedEps []*compute.NetworkEndpoint
		updatedPod  corev1.Pod
		updatedGW   gatewayv1.Gateway
		foundGate   bool
		foundProg   bool
	)

	for start := time.Now(); time.Since(start) < 60*time.Second; time.Sleep(50 * time.Millisecond) {
		attachedEps, _ = fakeGCP.ListNetworkEndpoints(t.Context(), "my-project", "us-central1-a", negName)
		if len(attachedEps) == 1 {
			break
		}
	}
	if len(attachedEps) != 1 || attachedEps[0].IpAddress != "10.0.1.5" || attachedEps[0].Instance != "gke-node-1" {
		t.Fatalf("expected pod endpoint attached to NEG while UNHEALTHY, got %+v", attachedEps)
	}

	// Verify readiness gate remains False while UNHEALTHY
	if err := env.Client.Get(t.Context(), types.NamespacedName{Namespace: gw.Namespace, Name: "dp-pod-1"}, &updatedPod); err != nil {
		t.Fatalf("failed to get pod: %v", err)
	}
	for _, c := range updatedPod.Status.Conditions {
		if c.Type == corev1.PodConditionType(LoadBalancerReadyGate) && c.Status == corev1.ConditionTrue {
			t.Fatalf("readiness gate must NOT be True while fake reports UNHEALTHY: %+v", c)
		}
	}

	// b. Fake load balancer switches to HEALTHY with NO Kubernetes event
	fakeGCP.SetBackendServiceHealth("my-project", bsName, negName, &compute.BackendServiceGroupHealth{
		HealthStatus: []*compute.HealthStatus{
			{Instance: "gke-node-1", IpAddress: "10.0.1.5", Port: 8443, HealthState: "HEALTHY"},
		},
	})

	// Assert readiness gate becomes True within bounded time via health polling (no k8s events triggered)
	for start := time.Now(); time.Since(start) < 60*time.Second; time.Sleep(100 * time.Millisecond) {
		_ = env.Client.Get(t.Context(), types.NamespacedName{Namespace: gw.Namespace, Name: "dp-pod-1"}, &updatedPod)
		_ = env.Client.Get(t.Context(), types.NamespacedName{Namespace: gw.Namespace, Name: gw.Name}, &updatedGW)

		foundGate = false
		for _, c := range updatedPod.Status.Conditions {
			if c.Type == corev1.PodConditionType(LoadBalancerReadyGate) && c.Status == corev1.ConditionTrue {
				foundGate = true
				break
			}
		}

		foundProg = false
		for _, c := range updatedGW.Status.Conditions {
			if c.Type == string(gatewayv1.GatewayConditionProgrammed) && c.Status == metav1.ConditionTrue {
				foundProg = true
				break
			}
		}

		if foundGate && foundProg {
			break
		}
	}

	if !foundGate {
		t.Errorf("expected pod readiness gate %s=True after polling, conditions are: %+v", LoadBalancerReadyGate, updatedPod.Status.Conditions)
	}

	if !foundProg {
		t.Errorf("expected Gateway Programmed=True after polling, conditions are: %+v", updatedGW.Status.Conditions)
	}
}

// TestSetupController_EndToEndRealSetup_NonDefaultDrainSeconds verifies that GARI
// provisions a data-plane Deployment deriving the non-default grace period (45 + 25 = 70)
// and pre-stop command (--drain-seconds=45) from SetupController.
func TestSetupController_EndToEndRealSetup_NonDefaultDrainSeconds(t *testing.T) {
	env := testenv.Start(t)

	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gcpgateway",
		},
		Spec: gatewayv1.GatewayClassSpec{
			ControllerName: DefaultControllerName,
		},
	}
	if err := env.Client.Create(t.Context(), gc); err != nil {
		t.Fatalf("creating GatewayClass: %v", err)
	}

	ipAddrType := gatewayv1.IPAddressType
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "my-gw-drain45",
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "gcpgateway",
			Addresses: []gatewayv1.GatewaySpecAddress{
				{
					Type:  &ipAddrType,
					Value: "34.120.254.20",
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
	if err := env.Client.Create(t.Context(), gw); err != nil {
		t.Fatalf("creating Gateway: %v", err)
	}

	fakeGCP := gcp.NewFakeComputeClient()

	opts, err := SetupController(t.Context(), []string{
		"--cluster-id=cluster-1",
		"--neg-prefix=gcpgw",
		"--project=my-project",
		"--zones=us-central1-a",
		"--drain-seconds=45",
		"--fake-compute",
	}, fakeGCP)
	if err != nil {
		t.Fatalf("SetupController failed: %v", err)
	}

	trueVal := true
	mgr, err := ctrl.NewManager(env.Config, ctrl.Options{
		Scheme:                 env.Scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller: config.Controller{
			SkipNameValidation: &trueVal,
		},
	})
	if err != nil {
		t.Fatalf("creating manager: %v", err)
	}
	opts.Manager = mgr

	server, err := gari.New(opts)
	if err != nil {
		t.Fatalf("gari.New failed: %v", err)
	}

	mgrCtx, cancelMgr := context.WithCancel(t.Context())
	defer cancelMgr()
	mgrErrCh := make(chan error, 1)
	go func() {
		if err := server.Manager().Start(mgrCtx); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("manager exited with error: %v", err)
			mgrErrCh <- err
		}
	}()
	t.Cleanup(func() {
		cancelMgr()
		select {
		case err := <-mgrErrCh:
			t.Errorf("manager error reported during cleanup: %v", err)
		default:
		}
	})

	resName := singlepod.ResourceNameForGateway(gw.Name)
	var deploy appsv1.Deployment
	for start := time.Now(); time.Since(start) < 15*time.Second; time.Sleep(100 * time.Millisecond) {
		if err := env.Client.Get(t.Context(), types.NamespacedName{Namespace: gw.Namespace, Name: resName}, &deploy); err == nil {
			break
		}
	}
	if deploy.Name == "" {
		t.Fatalf("timed out waiting for Deployment %s to be created by GARI", resName)
	}

	// drain-seconds=45 -> 45 + 25 = 70 (strictly greater than the 60s minimum clamp)
	assertProductionDeployment(t, &deploy, 45, 70)
}
