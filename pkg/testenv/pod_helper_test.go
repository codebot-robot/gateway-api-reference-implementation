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

package testenv

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestBuildPodFromDeployment_Defaults(t *testing.T) {
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-deploy",
			Namespace: "test-ns",
			UID:       types.UID("deploy-uid-123"),
			Labels: map[string]string{
				"app.kubernetes.io/name":       "gari-dataplane",
				"app.kubernetes.io/managed-by": "gari-singlepod",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name":                 "gari-dataplane",
						"app.kubernetes.io/managed-by":           "gari-singlepod",
						"gateway.networking.k8s.io/gateway-name": "my-gateway",
					},
					Annotations: map[string]string{
						"custom.annotation": "val",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "dataplane",
							Image: "gari-controller:v1",
							Args:  []string{"--dataplane-mode", "--gateway-name=my-gateway"},
						},
					},
				},
			},
		},
	}

	pod := BuildPodFromDeployment(deploy)
	if pod == nil {
		t.Fatalf("expected non-nil pod")
	}

	if pod.Namespace != "test-ns" {
		t.Errorf("expected namespace test-ns, got %q", pod.Namespace)
	}
	if pod.Name != "test-deploy-pod" {
		t.Errorf("expected pod name test-deploy-pod, got %q", pod.Name)
	}

	// Verify labels match template
	expectedLabels := map[string]string{
		"app.kubernetes.io/name":                 "gari-dataplane",
		"app.kubernetes.io/managed-by":           "gari-singlepod",
		"gateway.networking.k8s.io/gateway-name": "my-gateway",
	}
	for k, v := range expectedLabels {
		if pod.Labels[k] != v {
			t.Errorf("expected label %s=%q, got %q", k, v, pod.Labels[k])
		}
	}

	// Verify owner reference points to Deployment
	if len(pod.OwnerReferences) != 1 {
		t.Fatalf("expected 1 ownerReference, got %d", len(pod.OwnerReferences))
	}
	ref := pod.OwnerReferences[0]
	if ref.Kind != "Deployment" || ref.Name != "test-deploy" || ref.UID != "deploy-uid-123" {
		t.Errorf("unexpected ownerReference: %+v", ref)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("expected controller ownerReference")
	}

	// Verify Spec
	if len(pod.Spec.Containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(pod.Spec.Containers))
	}
	c := pod.Spec.Containers[0]
	if c.Name != "dataplane" || c.Image != "gari-controller:v1" {
		t.Errorf("unexpected container: %+v", c)
	}
	if pod.Spec.NodeName != "test-node" {
		t.Errorf("expected default nodeName test-node, got %q", pod.Spec.NodeName)
	}

	// Verify Status
	if pod.Status.Phase != corev1.PodRunning {
		t.Errorf("expected phase Running, got %v", pod.Status.Phase)
	}
	if pod.Status.PodIP != "10.244.0.10" {
		t.Errorf("expected default podIP 10.244.0.10, got %q", pod.Status.PodIP)
	}

	// Verify conditions
	var readyCond, containersReadyCond *corev1.PodCondition
	for i := range pod.Status.Conditions {
		cond := &pod.Status.Conditions[i]
		if cond.Type == corev1.PodReady {
			readyCond = cond
		}
		if cond.Type == corev1.ContainersReady {
			containersReadyCond = cond
		}
	}
	if readyCond == nil || readyCond.Status != corev1.ConditionTrue {
		t.Errorf("expected PodReady=True, got %+v", readyCond)
	}
	if containersReadyCond == nil || containersReadyCond.Status != corev1.ConditionTrue {
		t.Errorf("expected ContainersReady=True, got %+v", containersReadyCond)
	}
}

func TestBuildPodFromDeployment_Options(t *testing.T) {
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-deploy",
			Namespace: "test-ns",
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name": "gari-dataplane",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "c1", Image: "img"},
					},
				},
			},
		},
	}

	now := metav1.Now()
	pod := BuildPodFromDeployment(
		deploy,
		WithPodName("custom-pod-name"),
		WithPodNamespace("custom-ns"),
		WithPodIP("10.100.1.5"),
		WithHostIP("192.168.50.1"),
		WithNodeName("node-prod-01"),
		WithPhase(corev1.PodPending),
		WithReady(false),
		WithDeletionTimestamp(&now),
	)

	if pod.Name != "custom-pod-name" {
		t.Errorf("expected pod name custom-pod-name, got %q", pod.Name)
	}
	if pod.Namespace != "custom-ns" {
		t.Errorf("expected pod namespace custom-ns, got %q", pod.Namespace)
	}
	if pod.Status.PodIP != "10.100.1.5" {
		t.Errorf("expected pod IP 10.100.1.5, got %q", pod.Status.PodIP)
	}
	if pod.Status.HostIP != "192.168.50.1" {
		t.Errorf("expected host IP 192.168.50.1, got %q", pod.Status.HostIP)
	}
	if pod.Spec.NodeName != "node-prod-01" {
		t.Errorf("expected nodeName node-prod-01, got %q", pod.Spec.NodeName)
	}
	if pod.Status.Phase != corev1.PodPending {
		t.Errorf("expected phase Pending, got %v", pod.Status.Phase)
	}
	if pod.DeletionTimestamp == nil || *pod.DeletionTimestamp != now {
		t.Errorf("expected DeletionTimestamp %v, got %v", now, pod.DeletionTimestamp)
	}

	var readyCond, containersReadyCond *corev1.PodCondition
	for i := range pod.Status.Conditions {
		cond := &pod.Status.Conditions[i]
		if cond.Type == corev1.PodReady {
			readyCond = cond
		}
		if cond.Type == corev1.ContainersReady {
			containersReadyCond = cond
		}
	}
	if readyCond == nil || readyCond.Status != corev1.ConditionFalse {
		t.Errorf("expected PodReady=False, got %+v", readyCond)
	}
	if containersReadyCond == nil || containersReadyCond.Status != corev1.ConditionFalse {
		t.Errorf("expected ContainersReady=False, got %+v", containersReadyCond)
	}
}

func TestBuildPodFromDeployment_Nil(t *testing.T) {
	if pod := BuildPodFromDeployment(nil); pod != nil {
		t.Errorf("expected nil for nil deployment, got %+v", pod)
	}
}
