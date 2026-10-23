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
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/gcp"
	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/metadata"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/provisioning/singlepod"
	compute "google.golang.org/api/compute/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

const (
	// MaxBatchSize is the maximum number of endpoints attached or detached per Compute API call.
	MaxBatchSize = 500

	// PortHTTPS is the container target port for HTTPS traffic.
	PortHTTPS = 8443

	// PortHTTP is the container target port for HTTP traffic.
	PortHTTP = 8080

	// LoadBalancerReadyGate is the readiness gate condition type added to data plane pods.
	LoadBalancerReadyGate = "gcpgateway.gke-labs.io/load-balancer-ready"
)

// Config configures the NEG registrar.
type Config struct {
	ClusterID        string
	NegPrefix        string
	Project          string
	Zones            []string
	HysteresisWindow time.Duration
}

// Registrar reconciles Kubernetes data plane pods into Google Cloud zonal NEGs.
type Registrar struct {
	client    client.Client
	gcpClient gcp.ComputeClient
	cfg       Config
	tracker   *HysteresisTracker
	mu        sync.Mutex
}

// NewRegistrar creates a new NEG Registrar.
func NewRegistrar(c client.Client, gcpClient gcp.ComputeClient, cfg Config) *Registrar {
	if cfg.NegPrefix == "" {
		cfg.NegPrefix = "gcpgw"
	}
	return &Registrar{
		client:    c,
		gcpClient: gcpClient,
		cfg:       cfg,
		tracker:   NewHysteresisTracker(cfg.HysteresisWindow),
	}
}

// IsContainersReady returns true only if the pod has ContainersReady condition set to ConditionTrue.
// It explicitly ignores the pod's overall Ready condition to prevent deadlock with the readiness gate.
func IsContainersReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.ContainersReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func epKey(instance, ip string, port int64) string {
	return fmt.Sprintf("%s:%s:%d", instance, ip, port)
}

// ReconcileResult summarizes the registrar's synchronization action for a Gateway.
type ReconcileResult struct {
	AttachedCount int
	DetachedCount int
	MatchedNEGs   int
	SyncedNEGs    []*compute.NetworkEndpointGroup
}

// ReconcileEndpoints synchronizes data plane pods with the matched zonal NEGs for a Gateway.
func (r *Registrar) ReconcileEndpoints(ctx context.Context, gw *gatewayv1.Gateway) (*ReconcileResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := &ReconcileResult{}

	// 1. Determine target ports from Gateway listeners
	targetPorts := make(map[int]bool)
	for _, l := range gw.Spec.Listeners {
		switch l.Protocol {
		case gatewayv1.HTTPSProtocolType, gatewayv1.TLSProtocolType, gatewayv1.TCPProtocolType:
			targetPorts[PortHTTPS] = true
		case gatewayv1.HTTPProtocolType:
			targetPorts[PortHTTP] = true
		default:
			targetPorts[int(l.Port)] = true
		}
	}
	if len(targetPorts) == 0 {
		targetPorts[PortHTTPS] = true
	}

	// 2. Discover zones from configured zones and active cluster nodes
	zonesMap := make(map[string]bool)
	for _, z := range r.cfg.Zones {
		if z != "" {
			zonesMap[z] = true
		}
	}

	var nodeList corev1.NodeList
	if err := r.client.List(ctx, &nodeList); err != nil {
		return nil, fmt.Errorf("listing nodes: %w", err)
	}
	nodesByName := make(map[string]*corev1.Node)
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		nodesByName[node.Name] = node
		if info, err := ResolveNodeInstance(node); err == nil && info.Zone != "" {
			zonesMap[info.Zone] = true
		}
	}

	var zones []string
	for z := range zonesMap {
		zones = append(zones, z)
	}
	sort.Strings(zones)

	// 3. Find NEGs belonging to this Gateway by listing with prefix and matching metadata
	type matchedNeg struct {
		zone string
		port int
		neg  *compute.NetworkEndpointGroup
	}
	var matchedList []matchedNeg

	for _, zone := range zones {
		negs, err := r.gcpClient.ListNEGs(ctx, r.cfg.Project, zone, r.cfg.NegPrefix)
		if err != nil {
			return nil, fmt.Errorf("listing NEGs in zone %s: %w", zone, err)
		}

		for _, neg := range negs {
			meta, err := metadata.Parse(neg.Description)
			if err != nil {
				klog.V(2).Infof("Skipping NEG %q in zone %q: unparseable metadata: %v", neg.Name, zone, err)
				continue
			}

			if !meta.Matches(r.cfg.ClusterID, gw.Namespace, gw.Name, 0) {
				klog.V(4).Infof("Skipping NEG %q in zone %q: foreign metadata %+v", neg.Name, zone, meta)
				continue
			}

			if !targetPorts[meta.Port] && meta.Port > 0 {
				continue
			}

			matchedList = append(matchedList, matchedNeg{
				zone: zone,
				port: meta.Port,
				neg:  neg,
			})
			result.SyncedNEGs = append(result.SyncedNEGs, neg)
		}
	}
	result.MatchedNEGs = len(matchedList)

	if len(matchedList) == 0 {
		return result, nil
	}

	// 4. List data plane pods for this Gateway
	var podList corev1.PodList
	if err := r.client.List(ctx, &podList, client.InNamespace(gw.Namespace)); err != nil {
		return nil, fmt.Errorf("listing pods in namespace %s: %w", gw.Namespace, err)
	}

	// Filter pods belonging to this Gateway
	now := time.Now()
	type desiredPod struct {
		instance string
		zone     string
		ip       string
	}
	var activePods []desiredPod

	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Labels[singlepod.LabelAppName] != singlepod.AppNameValue {
			continue
		}
		if pod.Labels[singlepod.LabelGatewayName] != gw.Name {
			continue
		}
		if pod.Status.PodIP == "" || pod.Spec.NodeName == "" {
			continue
		}

		node, ok := nodesByName[pod.Spec.NodeName]
		if !ok {
			var n corev1.Node
			if err := r.client.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, &n); err != nil {
				klog.Warningf("Failed to get node %q for pod %s/%s: %v", pod.Spec.NodeName, pod.Namespace, pod.Name, err)
				continue
			}
			node = &n
			nodesByName[pod.Spec.NodeName] = node
		}

		nodeInfo, err := ResolveNodeInstance(node)
		if err != nil {
			klog.Warningf("Failed to resolve node instance for pod %s/%s: %v", pod.Namespace, pod.Name, err)
			continue
		}

		containersReady := IsContainersReady(pod)
		isTerminating := pod.DeletionTimestamp != nil
		podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)

		shouldDetach := r.tracker.ShouldDetach(podKey, containersReady, isTerminating, now)
		if !shouldDetach && containersReady && !isTerminating {
			activePods = append(activePods, desiredPod{
				instance: nodeInfo.Instance,
				zone:     nodeInfo.Zone,
				ip:       pod.Status.PodIP,
			})
		}
	}

	// 5. Reconcile endpoints for each matched NEG
	for _, mn := range matchedList {
		desiredMap := make(map[string]*compute.NetworkEndpoint)
		for _, ap := range activePods {
			if ap.zone == mn.zone {
				portVal := int64(mn.port)
				if portVal == 0 {
					portVal = PortHTTPS
				}
				ep := &compute.NetworkEndpoint{
					Instance:  ap.instance,
					IpAddress: ap.ip,
					Port:      portVal,
				}
				desiredMap[epKey(ep.Instance, ep.IpAddress, ep.Port)] = ep
			}
		}

		currentEndpoints, err := r.gcpClient.ListNetworkEndpoints(ctx, r.cfg.Project, mn.zone, mn.neg.Name)
		if err != nil {
			return nil, fmt.Errorf("listing endpoints in NEG %s: %w", mn.neg.Name, err)
		}

		currentMap := make(map[string]*compute.NetworkEndpoint)
		for _, ep := range currentEndpoints {
			currentMap[epKey(ep.Instance, ep.IpAddress, ep.Port)] = ep
		}

		var toAttach []*compute.NetworkEndpoint
		for k, ep := range desiredMap {
			if _, exists := currentMap[k]; !exists {
				toAttach = append(toAttach, ep)
			}
		}

		var toDetach []*compute.NetworkEndpoint
		for k, ep := range currentMap {
			if _, exists := desiredMap[k]; !exists {
				toDetach = append(toDetach, ep)
			}
		}

		// Batch attach calls
		for i := 0; i < len(toAttach); i += MaxBatchSize {
			end := i + MaxBatchSize
			if end > len(toAttach) {
				end = len(toAttach)
			}
			batch := toAttach[i:end]
			if err := r.gcpClient.AttachNetworkEndpoints(ctx, r.cfg.Project, mn.zone, mn.neg.Name, batch); err != nil {
				return nil, fmt.Errorf("attaching endpoints to NEG %s: %w", mn.neg.Name, err)
			}
			result.AttachedCount += len(batch)
		}

		// Batch detach calls (including orphan pruning)
		for i := 0; i < len(toDetach); i += MaxBatchSize {
			end := i + MaxBatchSize
			if end > len(toDetach) {
				end = len(toDetach)
			}
			batch := toDetach[i:end]
			if err := r.gcpClient.DetachNetworkEndpoints(ctx, r.cfg.Project, mn.zone, mn.neg.Name, batch); err != nil {
				return nil, fmt.Errorf("detaching endpoints from NEG %s: %w", mn.neg.Name, err)
			}
			result.DetachedCount += len(batch)
		}
	}

	return result, nil
}

// ResolveNEGConflict deterministically resolves conflicts between Gateways targeting identical NEGs.
// The oldest Gateway (by creation timestamp) wins, with ties broken lexicographically by namespace/name.
func ResolveNEGConflict(gateways []*gatewayv1.Gateway) *gatewayv1.Gateway {
	if len(gateways) == 0 {
		return nil
	}
	sorted := make([]*gatewayv1.Gateway, len(gateways))
	copy(sorted, gateways)

	sort.Slice(sorted, func(i, j int) bool {
		ti := sorted[i].CreationTimestamp.Time
		tj := sorted[j].CreationTimestamp.Time
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		keyI := fmt.Sprintf("%s/%s", sorted[i].Namespace, sorted[i].Name)
		keyJ := fmt.Sprintf("%s/%s", sorted[j].Namespace, sorted[j].Name)
		return keyI < keyJ
	})

	return sorted[0]
}
