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

package registrar

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

const (
	ZoneLabelStandard = "topology.kubernetes.io/zone"
	ZoneLabelLegacy   = "failure-domain.beta.kubernetes.io/zone"
)

// NodeInstanceInfo contains the resolved GCP zone and VM instance name for a Kubernetes node.
type NodeInstanceInfo struct {
	Zone     string
	Instance string
}

// ResolveNodeInstance extracts the GCP zone and VM instance name from a Node's spec.providerID or labels.
func ResolveNodeInstance(node *corev1.Node) (*NodeInstanceInfo, error) {
	if node == nil {
		return nil, fmt.Errorf("node is nil")
	}

	providerID := node.Spec.ProviderID
	if strings.HasPrefix(providerID, "gce://") {
		trimmed := strings.TrimPrefix(providerID, "gce://")
		parts := strings.Split(trimmed, "/")
		if len(parts) >= 3 {
			zone := parts[1]
			instance := parts[2]
			if zone != "" && instance != "" {
				return &NodeInstanceInfo{Zone: zone, Instance: instance}, nil
			}
		}
	}

	// Fallback to node labels and node name
	zone := node.Labels[ZoneLabelStandard]
	if zone == "" {
		zone = node.Labels[ZoneLabelLegacy]
	}

	instance := node.Name
	if zone != "" && instance != "" {
		return &NodeInstanceInfo{Zone: zone, Instance: instance}, nil
	}

	return nil, fmt.Errorf("unable to resolve zone and instance from node %q (providerID: %q)", node.Name, providerID)
}
