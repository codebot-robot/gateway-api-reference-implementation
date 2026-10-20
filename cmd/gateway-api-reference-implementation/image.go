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
	"strings"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/provisioning/singlepod"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
)

// parseDigestReference checks if imageID is a digest reference (optionally prefixed by
// docker-pullable:// or docker://) and returns the normalized digest reference.
func parseDigestReference(imageID string) (string, bool) {
	ref := strings.TrimPrefix(imageID, "docker-pullable://")
	ref = strings.TrimPrefix(ref, "docker://")
	parts := strings.SplitN(ref, "@", 2)
	if len(parts) == 2 && parts[0] != "" && strings.Contains(parts[1], ":") {
		return ref, true
	}
	return "", false
}

// extractRepository returns the repository portion of an image reference, stripping
// any scheme (docker://, docker-pullable://), digest (@sha256:...), or tag (:tag).
func extractRepository(imageRef string) string {
	ref := strings.TrimPrefix(imageRef, "docker-pullable://")
	ref = strings.TrimPrefix(ref, "docker://")

	if i := strings.Index(ref, "@"); i != -1 {
		ref = ref[:i]
	}

	lastColon := strings.LastIndex(ref, ":")
	lastSlash := strings.LastIndex(ref, "/")
	if lastColon > lastSlash {
		ref = ref[:lastColon]
	}

	return ref
}

// normalizeRepository strips default Docker registry prefixes for comparison.
func normalizeRepository(repo string) string {
	repo = strings.TrimPrefix(repo, "docker.io/library/")
	repo = strings.TrimPrefix(repo, "docker.io/")
	return repo
}

// repositoryMatches checks whether the repository of statusImage matches that of specImage.
func repositoryMatches(specImage, statusImage string) bool {
	specRepo := normalizeRepository(extractRepository(specImage))
	statusRepo := normalizeRepository(extractRepository(statusImage))
	return specRepo != "" && specRepo == statusRepo
}

// extractPodImage returns the container image from the given Pod.
// It looks for a container named "controller", falling back to the only container if there is exactly one.
// It prefers status.containerStatuses[].imageID when it is a digest reference whose repository matches
// the container's spec image; otherwise it returns spec.containers[].image.
func extractPodImage(pod *corev1.Pod) (string, error) {
	if pod == nil {
		return "", errors.New("pod is nil")
	}
	var targetContainer *corev1.Container
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == "controller" {
			targetContainer = &pod.Spec.Containers[i]
			break
		}
	}
	if targetContainer == nil && len(pod.Spec.Containers) == 1 {
		targetContainer = &pod.Spec.Containers[0]
	}
	if targetContainer == nil {
		return "", fmt.Errorf("could not find container 'controller' in pod with %d containers", len(pod.Spec.Containers))
	}

	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == targetContainer.Name {
			if ref, ok := parseDigestReference(cs.ImageID); ok {
				if repositoryMatches(targetContainer.Image, ref) {
					return ref, nil
				}
			}
			break
		}
	}

	if targetContainer.Image != "" {
		return targetContainer.Image, nil
	}
	return "", fmt.Errorf("container %q has empty image", targetContainer.Name)
}

// resolveDataplaneImage determines the container image to use for data-plane instances
// using the selection order: flag > env > own Pod (digest preferred) > existing default.
// It returns the chosen image, its source ("flag", "env", "self", or "default"), and an optional warning.
func resolveDataplaneImage(
	ctx context.Context,
	flagValue string,
	getenv func(string) string,
	getOwnPod func(ctx context.Context) (*corev1.Pod, error),
) (string, string, error) {
	if flagValue != "" {
		return flagValue, "flag", nil
	}

	if envImg := getenv("GARI_IMAGE"); envImg != "" {
		return envImg, "env", nil
	}

	if getOwnPod != nil {
		pod, err := getOwnPod(ctx)
		if err == nil {
			if img, err := extractPodImage(pod); err == nil && img != "" {
				return img, "self", nil
			} else if err != nil {
				return singlepod.DefaultDataplaneImage, "default", fmt.Errorf("failed to extract image from own pod: %w", err)
			}
		} else {
			return singlepod.DefaultDataplaneImage, "default", fmt.Errorf("failed to read own pod: %w", err)
		}
	}

	return singlepod.DefaultDataplaneImage, "default", nil
}

// fetchOwnPod retrieves the controller's own Pod using the downward API environment variables
// POD_NAME and POD_NAMESPACE, and the provided REST config (or default in-cluster config).
func fetchOwnPod(ctx context.Context, getenv func(string) string, cfg *rest.Config) (*corev1.Pod, error) {
	podName := getenv("POD_NAME")
	podNamespace := getenv("POD_NAMESPACE")
	if podName == "" || podNamespace == "" {
		return nil, fmt.Errorf("POD_NAME (%q) or POD_NAMESPACE (%q) environment variable is not set", podName, podNamespace)
	}

	if cfg == nil {
		var err error
		cfg, err = ctrl.GetConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to get kubeconfig: %w", err)
		}
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes clientset: %w", err)
	}

	fetchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	pod, err := clientset.CoreV1().Pods(podNamespace).Get(fetchCtx, podName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get pod %s/%s: %w", podNamespace, podName, err)
	}
	return pod, nil
}
