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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/provisioning/singlepod"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/yaml"
)

func TestParseDigestReference(t *testing.T) {
	tests := []struct {
		name       string
		imageID    string
		wantDigest string
		wantOK     bool
	}{
		{
			name:       "docker-pullable prefix with digest",
			imageID:    "docker-pullable://gcr.io/gke-labs/gari-controller@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			wantDigest: "gcr.io/gke-labs/gari-controller@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			wantOK:     true,
		},
		{
			name:       "docker prefix with digest",
			imageID:    "docker://gcr.io/gke-labs/gari-controller@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			wantDigest: "gcr.io/gke-labs/gari-controller@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			wantOK:     true,
		},
		{
			name:       "no scheme prefix with digest",
			imageID:    "registry.example.com/team/gari-controller@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			wantDigest: "registry.example.com/team/gari-controller@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			wantOK:     true,
		},
		{
			name:       "bare sha256 id without repository",
			imageID:    "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			wantDigest: "",
			wantOK:     false,
		},
		{
			name:       "docker-pullable bare sha256 without repository",
			imageID:    "docker-pullable://sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			wantDigest: "",
			wantOK:     false,
		},
		{
			name:       "tag reference without digest",
			imageID:    "gcr.io/gke-labs/gari-controller:latest",
			wantDigest: "",
			wantOK:     false,
		},
		{
			name:       "empty string",
			imageID:    "",
			wantDigest: "",
			wantOK:     false,
		},
		{
			name:       "missing repository before @",
			imageID:    "@sha256:0123456789abcdef",
			wantDigest: "",
			wantOK:     false,
		},
		{
			name:       "missing digest after @",
			imageID:    "gcr.io/gke-labs/gari-controller@",
			wantDigest: "",
			wantOK:     false,
		},
		{
			name:       "missing colon in digest after @",
			imageID:    "gcr.io/gke-labs/gari-controller@sha256",
			wantDigest: "",
			wantOK:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDigest, gotOK := parseDigestReference(tt.imageID)
			if gotOK != tt.wantOK {
				t.Errorf("parseDigestReference(%q) ok = %v, want %v", tt.imageID, gotOK, tt.wantOK)
			}
			if gotDigest != tt.wantDigest {
				t.Errorf("parseDigestReference(%q) digest = %q, want %q", tt.imageID, gotDigest, tt.wantDigest)
			}
		})
	}
}

func TestExtractPodImage(t *testing.T) {
	t.Run("nil pod", func(t *testing.T) {
		_, err := extractPodImage(nil)
		if err == nil {
			t.Fatal("expected error for nil pod, got nil")
		}
	})

	t.Run("empty containers", func(t *testing.T) {
		pod := &corev1.Pod{}
		_, err := extractPodImage(pod)
		if err == nil {
			t.Fatal("expected error for pod with no containers, got nil")
		}
	})

	t.Run("prefers container named controller and digest imageID", func(t *testing.T) {
		pod := &corev1.Pod{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "sidecar",
						Image: "sidecar:latest",
					},
					{
						Name:  "controller",
						Image: "gari-controller:v1",
					},
				},
			},
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name:    "sidecar",
						ImageID: "docker-pullable://sidecar@sha256:aaaa",
					},
					{
						Name:    "controller",
						ImageID: "docker-pullable://gcr.io/gke-labs/gari-controller@sha256:bbbb",
					},
				},
			},
		}

		img, err := extractPodImage(pod)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if want := "gcr.io/gke-labs/gari-controller@sha256:bbbb"; img != want {
			t.Errorf("extractPodImage() = %q, want %q", img, want)
		}
	})

	t.Run("controller container falls back to spec image when imageID is not a digest", func(t *testing.T) {
		pod := &corev1.Pod{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "controller",
						Image: "gcr.io/gke-labs/gari-controller:v2",
					},
				},
			},
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name:    "controller",
						ImageID: "sha256:localidwithoutrepo",
					},
				},
			},
		}

		img, err := extractPodImage(pod)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if want := "gcr.io/gke-labs/gari-controller:v2"; img != want {
			t.Errorf("extractPodImage() = %q, want %q", img, want)
		}
	})

	t.Run("falls back to only container if not named controller", func(t *testing.T) {
		pod := &corev1.Pod{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "manager",
						Image: "custom-manager:latest",
					},
				},
			},
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name:    "manager",
						ImageID: "docker-pullable://custom-manager@sha256:cccc",
					},
				},
			},
		}

		img, err := extractPodImage(pod)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if want := "custom-manager@sha256:cccc"; img != want {
			t.Errorf("extractPodImage() = %q, want %q", img, want)
		}
	})

	t.Run("fails if multiple containers and none named controller", func(t *testing.T) {
		pod := &corev1.Pod{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "c1",
						Image: "c1:latest",
					},
					{
						Name:  "c2",
						Image: "c2:latest",
					},
				},
			},
		}

		_, err := extractPodImage(pod)
		if err == nil {
			t.Fatal("expected error when no container is named controller, got nil")
		}
	})
}

func TestResolveDataplaneImage_SelectionOrder(t *testing.T) {
	ctx := context.Background()

	ownPodWithDigest := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "controller",
					Image: "controller-spec:tag",
				},
			},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:    "controller",
					ImageID: "docker-pullable://gcr.io/gke-labs/gari-controller@sha256:deadbeef",
				},
			},
		},
	}

	ownPodWithoutDigest := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "controller",
					Image: "controller-spec:tag",
				},
			},
		},
	}

	tests := []struct {
		name        string
		flagValue   string
		envMap      map[string]string
		getOwnPod   func(context.Context) (*corev1.Pod, error)
		wantImage   string
		wantSource  string
		wantWarning bool
	}{
		{
			name:      "flag overrides env, own pod, and default",
			flagValue: "flag-image:v1",
			envMap: map[string]string{
				"GARI_IMAGE": "env-image:v1",
			},
			getOwnPod: func(context.Context) (*corev1.Pod, error) {
				return ownPodWithDigest, nil
			},
			wantImage:   "flag-image:v1",
			wantSource:  "flag",
			wantWarning: false,
		},
		{
			name:      "env overrides own pod and default when flag is empty",
			flagValue: "",
			envMap: map[string]string{
				"GARI_IMAGE": "env-image:v1",
			},
			getOwnPod: func(context.Context) (*corev1.Pod, error) {
				return ownPodWithDigest, nil
			},
			wantImage:   "env-image:v1",
			wantSource:  "env",
			wantWarning: false,
		},
		{
			name:      "own pod with digest preferred when flag and env are empty",
			flagValue: "",
			envMap:    map[string]string{},
			getOwnPod: func(context.Context) (*corev1.Pod, error) {
				return ownPodWithDigest, nil
			},
			wantImage:   "gcr.io/gke-labs/gari-controller@sha256:deadbeef",
			wantSource:  "self",
			wantWarning: false,
		},
		{
			name:      "own pod without digest uses spec image when flag and env are empty",
			flagValue: "",
			envMap:    map[string]string{},
			getOwnPod: func(context.Context) (*corev1.Pod, error) {
				return ownPodWithoutDigest, nil
			},
			wantImage:   "controller-spec:tag",
			wantSource:  "self",
			wantWarning: false,
		},
		{
			name:      "falls back to existing default when pod read fails",
			flagValue: "",
			envMap:    map[string]string{},
			getOwnPod: func(context.Context) (*corev1.Pod, error) {
				return nil, errors.New("pods not found")
			},
			wantImage:   singlepod.DefaultDataplaneImage,
			wantSource:  "default",
			wantWarning: true,
		},
		{
			name:      "falls back to existing default when pod has invalid containers",
			flagValue: "",
			envMap:    map[string]string{},
			getOwnPod: func(context.Context) (*corev1.Pod, error) {
				return &corev1.Pod{}, nil
			},
			wantImage:   singlepod.DefaultDataplaneImage,
			wantSource:  "default",
			wantWarning: true,
		},
		{
			name:        "falls back to existing default when getOwnPod is nil",
			flagValue:   "",
			envMap:      map[string]string{},
			getOwnPod:   nil,
			wantImage:   singlepod.DefaultDataplaneImage,
			wantSource:  "default",
			wantWarning: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getenv := func(key string) string {
				return tt.envMap[key]
			}
			gotImage, gotSource, err := resolveDataplaneImage(ctx, tt.flagValue, getenv, tt.getOwnPod)
			if (err != nil) != tt.wantWarning {
				t.Errorf("resolveDataplaneImage() warning = %v, wantWarning %v", err, tt.wantWarning)
			}
			if gotImage != tt.wantImage {
				t.Errorf("resolveDataplaneImage() image = %q, want %q", gotImage, tt.wantImage)
			}
			if gotSource != tt.wantSource {
				t.Errorf("resolveDataplaneImage() source = %q, want %q", gotSource, tt.wantSource)
			}
		})
	}
}

func TestFetchOwnPod_MissingEnv(t *testing.T) {
	ctx := context.Background()

	t.Run("missing both", func(t *testing.T) {
		getenv := func(key string) string { return "" }
		_, err := fetchOwnPod(ctx, getenv, nil)
		if err == nil {
			t.Fatal("expected error when POD_NAME and POD_NAMESPACE are not set, got nil")
		}
	})

	t.Run("missing POD_NAMESPACE", func(t *testing.T) {
		getenv := func(key string) string {
			if key == "POD_NAME" {
				return "my-pod"
			}
			return ""
		}
		_, err := fetchOwnPod(ctx, getenv, nil)
		if err == nil {
			t.Fatal("expected error when POD_NAMESPACE is not set, got nil")
		}
	})

	t.Run("missing POD_NAME", func(t *testing.T) {
		getenv := func(key string) string {
			if key == "POD_NAMESPACE" {
				return "default"
			}
			return ""
		}
		_, err := fetchOwnPod(ctx, getenv, nil)
		if err == nil {
			t.Fatal("expected error when POD_NAME is not set, got nil")
		}
	})
}

func TestFetchOwnPod_Success(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "test-ns",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "controller",
					Image: "test-image:v1",
				},
			},
		},
	}

	clientset := fake.NewClientset(pod)

	getenv := func(key string) string {
		switch key {
		case "POD_NAME":
			return "test-pod"
		case "POD_NAMESPACE":
			return "test-ns"
		default:
			return ""
		}
	}

	fetchedPod, err := clientset.CoreV1().Pods(getenv("POD_NAMESPACE")).Get(ctx, getenv("POD_NAME"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get pod: %v", err)
	}
	if fetchedPod.Name != "test-pod" {
		t.Errorf("pod name = %q, want test-pod", fetchedPod.Name)
	}
}

func TestControllerManifest_RoleAndDownwardAPI(t *testing.T) {
	manifestPath := filepath.Join("..", "..", "k8s", "controller.yaml")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("failed to read controller manifest %s: %v", manifestPath, err)
	}

	documents := strings.Split(string(data), "\n---")

	var (
		foundRole         bool
		foundRoleBinding  bool
		foundDownwardAPI  bool
		clusterRoleHasPod bool
	)

	for _, doc := range documents {
		doc = strings.TrimSpace(doc)
		if doc == "" {
			continue
		}

		var meta struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
			Metadata   struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
			} `json:"metadata"`
		}
		if err := yaml.Unmarshal([]byte(doc), &meta); err != nil {
			t.Fatalf("failed to unmarshal doc: %v", err)
		}

		switch {
		case meta.Kind == "Role" && meta.Metadata.Name == "gari-controller":
			foundRole = true
			if meta.Metadata.Namespace != "default" {
				t.Errorf("Role namespace = %q, want default", meta.Metadata.Namespace)
			}
			var role struct {
				Rules []struct {
					APIGroups []string `json:"apiGroups"`
					Resources []string `json:"resources"`
					Verbs     []string `json:"verbs"`
				} `json:"rules"`
			}
			if err := yaml.Unmarshal([]byte(doc), &role); err != nil {
				t.Fatalf("failed to unmarshal Role: %v", err)
			}
			var hasPodGet bool
			for _, r := range role.Rules {
				for _, res := range r.Resources {
					if res == "pods" {
						for _, v := range r.Verbs {
							if v == "get" {
								hasPodGet = true
							}
						}
					}
				}
			}
			if !hasPodGet {
				t.Errorf("Role does not grant get on pods: %+v", role.Rules)
			}

		case meta.Kind == "RoleBinding" && meta.Metadata.Name == "gari-controller":
			foundRoleBinding = true
			if meta.Metadata.Namespace != "default" {
				t.Errorf("RoleBinding namespace = %q, want default", meta.Metadata.Namespace)
			}

		case meta.Kind == "ClusterRole" && meta.Metadata.Name == "gari-controller":
			var clusterRole struct {
				Rules []struct {
					Resources []string `json:"resources"`
				} `json:"rules"`
			}
			if err := yaml.Unmarshal([]byte(doc), &clusterRole); err != nil {
				t.Fatalf("failed to unmarshal ClusterRole: %v", err)
			}
			for _, r := range clusterRole.Rules {
				for _, res := range r.Resources {
					if res == "pods" {
						clusterRoleHasPod = true
					}
				}
			}

		case meta.Kind == "Deployment" && meta.Metadata.Name == "gari-controller":
			var deployment struct {
				Spec struct {
					Template struct {
						Spec struct {
							Containers []struct {
								Name string `json:"name"`
								Env  []struct {
									Name      string `json:"name"`
									ValueFrom *struct {
										FieldRef *struct {
											FieldPath string `json:"fieldPath"`
										} `json:"fieldRef"`
									} `json:"valueFrom"`
								} `json:"env"`
							} `json:"containers"`
						} `json:"spec"`
					} `json:"template"`
				} `json:"spec"`
			}
			if err := yaml.Unmarshal([]byte(doc), &deployment); err != nil {
				t.Fatalf("failed to unmarshal Deployment: %v", err)
			}
			for _, c := range deployment.Spec.Template.Spec.Containers {
				if c.Name == "controller" {
					var hasPodName, hasPodNamespace bool
					for _, e := range c.Env {
						if e.Name == "POD_NAME" && e.ValueFrom != nil && e.ValueFrom.FieldRef != nil && e.ValueFrom.FieldRef.FieldPath == "metadata.name" {
							hasPodName = true
						}
						if e.Name == "POD_NAMESPACE" && e.ValueFrom != nil && e.ValueFrom.FieldRef != nil && e.ValueFrom.FieldRef.FieldPath == "metadata.namespace" {
							hasPodNamespace = true
						}
					}
					if hasPodName && hasPodNamespace {
						foundDownwardAPI = true
					}
				}
			}
		}
	}

	if !foundRole {
		t.Error("Role gari-controller not found in k8s/controller.yaml")
	}
	if !foundRoleBinding {
		t.Error("RoleBinding gari-controller not found in k8s/controller.yaml")
	}
	if !foundDownwardAPI {
		t.Error("Deployment gari-controller missing POD_NAME or POD_NAMESPACE downward API env vars")
	}
	if clusterRoleHasPod {
		t.Error("ClusterRole gari-controller must NOT grant access to pods (no cluster-wide pod access)")
	}
}
