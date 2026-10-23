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
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PodOption configures pod attributes in BuildPodFromDeployment.
type PodOption func(*corev1.Pod)

// WithPodName sets the Pod name.
func WithPodName(name string) PodOption {
	return func(p *corev1.Pod) {
		p.Name = name
	}
}

// WithPodNamespace sets the Pod namespace.
func WithPodNamespace(ns string) PodOption {
	return func(p *corev1.Pod) {
		p.Namespace = ns
	}
}

// WithPodIP sets the Pod IP address.
func WithPodIP(ip string) PodOption {
	return func(p *corev1.Pod) {
		p.Status.PodIP = ip
		p.Status.PodIPs = []corev1.PodIP{{IP: ip}}
	}
}

// WithHostIP sets the host/node IP address.
func WithHostIP(ip string) PodOption {
	return func(p *corev1.Pod) {
		p.Status.HostIP = ip
	}
}

// WithNodeName sets the node name the pod is scheduled on.
func WithNodeName(nodeName string) PodOption {
	return func(p *corev1.Pod) {
		p.Spec.NodeName = nodeName
	}
}

// WithPhase sets the Pod phase (Running, Pending, Succeeded, Failed).
func WithPhase(phase corev1.PodPhase) PodOption {
	return func(p *corev1.Pod) {
		p.Status.Phase = phase
	}
}

// WithReady sets the Ready and ContainersReady status conditions.
func WithReady(ready bool) PodOption {
	return func(p *corev1.Pod) {
		status := corev1.ConditionTrue
		if !ready {
			status = corev1.ConditionFalse
		}
		setPodCondition(p, corev1.PodReady, status)
		setPodCondition(p, corev1.ContainersReady, status)
		for i := range p.Status.ContainerStatuses {
			p.Status.ContainerStatuses[i].Ready = ready
		}
	}
}

// WithContainersReady sets the ContainersReady status condition specifically.
func WithContainersReady(ready bool) PodOption {
	return func(p *corev1.Pod) {
		status := corev1.ConditionTrue
		if !ready {
			status = corev1.ConditionFalse
		}
		setPodCondition(p, corev1.ContainersReady, status)
		for i := range p.Status.ContainerStatuses {
			p.Status.ContainerStatuses[i].Ready = ready
		}
	}
}

// WithDeletionTimestamp sets the DeletionTimestamp simulating a terminating pod.
func WithDeletionTimestamp(ts *metav1.Time) PodOption {
	return func(p *corev1.Pod) {
		p.DeletionTimestamp = ts
	}
}

// WithCondition adds or replaces a status condition on the Pod.
func WithCondition(cond corev1.PodCondition) PodOption {
	return func(p *corev1.Pod) {
		setPodCondition(p, cond.Type, cond.Status)
	}
}

func setPodCondition(p *corev1.Pod, condType corev1.PodConditionType, status corev1.ConditionStatus) {
	for i := range p.Status.Conditions {
		if p.Status.Conditions[i].Type == condType {
			p.Status.Conditions[i].Status = status
			p.Status.Conditions[i].LastTransitionTime = metav1.Now()
			return
		}
	}
	p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{
		Type:               condType,
		Status:             status,
		LastTransitionTime: metav1.Now(),
	})
}

// BuildPodFromDeployment constructs a Pod object derived directly from the given
// Deployment's pod template. This allows controller tests (such as registrars)
// to test against the exact labels, annotations, container images, and arguments
// produced by the provisioner without running a full Kubernetes cluster.
func BuildPodFromDeployment(deploy *appsv1.Deployment, opts ...PodOption) *corev1.Pod {
	if deploy == nil {
		return nil
	}

	podName := fmt.Sprintf("%s-pod", deploy.Name)
	if deploy.Name == "" {
		podName = "dataplane-pod"
	}

	labels := make(map[string]string)
	for k, v := range deploy.Spec.Template.Labels {
		labels[k] = v
	}
	if len(labels) == 0 {
		for k, v := range deploy.Labels {
			labels[k] = v
		}
	}

	annotations := make(map[string]string)
	for k, v := range deploy.Spec.Template.Annotations {
		annotations[k] = v
	}

	isController := true
	blockOwnerDeletion := true
	ownerRefs := []metav1.OwnerReference{
		{
			APIVersion:         "apps/v1",
			Kind:               "Deployment",
			Name:               deploy.Name,
			UID:                deploy.UID,
			Controller:         &isController,
			BlockOwnerDeletion: &blockOwnerDeletion,
		},
	}

	podSpec := *deploy.Spec.Template.Spec.DeepCopy()
	if podSpec.NodeName == "" {
		podSpec.NodeName = "test-node"
	}

	defaultIP := "10.244.0.10"
	defaultHostIP := "192.168.1.10"

	var containerStatuses []corev1.ContainerStatus
	for _, c := range podSpec.Containers {
		containerStatuses = append(containerStatuses, corev1.ContainerStatus{
			Name:    c.Name,
			Image:   c.Image,
			Ready:   true,
			Started: boolPtr(true),
			State: corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{
					StartedAt: metav1.Now(),
				},
			},
		})
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       deploy.Namespace,
			Name:            podName,
			GenerateName:    fmt.Sprintf("%s-", deploy.Name),
			Labels:          labels,
			Annotations:     annotations,
			OwnerReferences: ownerRefs,
		},
		Spec: podSpec,
		Status: corev1.PodStatus{
			Phase:     corev1.PodRunning,
			PodIP:     defaultIP,
			PodIPs:    []corev1.PodIP{{IP: defaultIP}},
			HostIP:    defaultHostIP,
			StartTime: timePtr(metav1.Now()),
			Conditions: []corev1.PodCondition{
				{
					Type:               corev1.PodScheduled,
					Status:             corev1.ConditionTrue,
					LastTransitionTime: metav1.Now(),
				},
				{
					Type:               corev1.PodInitialized,
					Status:             corev1.ConditionTrue,
					LastTransitionTime: metav1.Now(),
				},
				{
					Type:               corev1.ContainersReady,
					Status:             corev1.ConditionTrue,
					LastTransitionTime: metav1.Now(),
				},
				{
					Type:               corev1.PodReady,
					Status:             corev1.ConditionTrue,
					LastTransitionTime: metav1.Now(),
				},
			},
			ContainerStatuses: containerStatuses,
		},
	}

	for _, opt := range opts {
		opt(pod)
	}

	return pod
}

func boolPtr(b bool) *bool {
	return &b
}

func timePtr(t metav1.Time) *metav1.Time {
	return &t
}
