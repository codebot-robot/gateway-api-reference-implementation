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
	"slices"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

func TestControllerManifest_RBACCustomResourceDefinitions(t *testing.T) {
	manifestPath := filepath.Join("..", "..", "k8s", "controller.yaml")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("failed to read %s: %v", manifestPath, err)
	}

	docs := strings.Split(string(data), "\n---")
	var foundClusterRole bool
	var foundCRDRule bool

	for _, doc := range docs {
		var role rbacv1.ClusterRole
		if err := yaml.Unmarshal([]byte(doc), &role); err != nil {
			continue
		}
		if role.Kind != "ClusterRole" || role.Name != "gcpgateway-controller" {
			continue
		}
		foundClusterRole = true
		for _, rule := range role.Rules {
			if slices.Contains(rule.APIGroups, "apiextensions.k8s.io") && slices.Contains(rule.Resources, "customresourcedefinitions") {
				foundCRDRule = true
				if len(rule.ResourceNames) != 0 {
					t.Errorf("customresourcedefinitions rule must not restrict resourceNames (got %v), because controller-runtime informers cannot be scoped by resourceNames", rule.ResourceNames)
				}
				for _, verb := range []string{"get", "list", "watch"} {
					if !slices.Contains(rule.Verbs, verb) {
						t.Errorf("customresourcedefinitions rule missing required verb %q; got %v", verb, rule.Verbs)
					}
				}
				if len(rule.Verbs) != 3 {
					t.Errorf("expected exactly [get, list, watch] verbs; got %v", rule.Verbs)
				}
			}
		}
	}

	if !foundClusterRole {
		t.Fatalf("gcpgateway-controller ClusterRole not found in %s", manifestPath)
	}
	if !foundCRDRule {
		t.Fatalf("customresourcedefinitions rule not found in gcpgateway-controller ClusterRole")
	}
}
