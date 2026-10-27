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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/gcp"
	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/metadata"
	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/registrar"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/provisioning/singlepod"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

const (
	// DefaultHealthTimeout is the duration after which an unconfirmed endpoint triggers RolloutStalled.
	DefaultHealthTimeout = 60 * time.Second

	// HealthPollInterval is the interval at which the reconciler requeues to poll load balancer
	// health when data-plane pods are attached to NEGs but their readiness gates are not yet True.
	// A 5-second interval balances fast rollout progression (health check convergence typically
	// takes 10–30s on Google Cloud) against Compute API rate limits and quota usage.
	HealthPollInterval = 5 * time.Second

	// ResyncInterval is the interval at which every Gateway is periodically requeued in the steady state.
	// This ensures periodic orphan endpoint pruning (such as endpoints left behind after node crashes
	// or ungraceful pod deletions without Kubernetes events) and drift correction. A 60-second
	// interval bounds the lifetime of orphaned endpoints while minimizing background Compute API load.
	ResyncInterval = 60 * time.Second

	// Condition reasons
	ReasonAccepted                 = "Accepted"
	ReasonInvalidParameters        = "InvalidParameters"
	ReasonConflictingParameters    = "ConflictingParameters"
	ReasonAddressNotAssigned       = "AddressNotAssigned"
	ReasonWaitingForDataPlane      = "WaitingForDataPlane"
	ReasonWaitingForEndpoints      = "WaitingForEndpoints"
	ReasonRolloutStalled           = "RolloutStalled"
	ReasonProgrammed               = "Programmed"
	ReasonPermissionDenied         = "PermissionDenied"
	ReasonQuotaExceeded            = "QuotaExceeded"
	ReasonNEGNotFound              = "NEGNotFound"
	ReasonUnsupportedCluster       = "UnsupportedCluster"
	ReasonEndpointAttachmentFailed = "EndpointAttachmentFailed"
)

// GatewayReconciler reconciles Gateways using Google Cloud zonal NEGs and L4 load balancers.
type GatewayReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	GCPClient      gcp.ComputeClient
	ClusterID      string
	NegPrefix      string
	Project        string
	Zones          []string
	ControllerName string
	HealthTimeout  time.Duration
	Registrar      *registrar.Registrar

	healthPollInterval time.Duration
	resyncInterval     time.Duration

	mu           sync.Mutex
	waitingSince map[string]time.Time
}

// NewGatewayReconciler creates a new GatewayReconciler.
func NewGatewayReconciler(c client.Client, scheme *runtime.Scheme, gcpClient gcp.ComputeClient, reg *registrar.Registrar, clusterID, negPrefix, project string) *GatewayReconciler {
	if negPrefix == "" {
		negPrefix = "gcpgw"
	}
	return &GatewayReconciler{
		Client:        c,
		Scheme:        scheme,
		GCPClient:     gcpClient,
		ClusterID:     clusterID,
		NegPrefix:     negPrefix,
		Project:       project,
		HealthTimeout: DefaultHealthTimeout,
		Registrar:     reg,
		waitingSince:  make(map[string]time.Time),
	}
}

// SetupWithManager registers GatewayReconciler with the controller-runtime Manager,
// watching Gateways, data-plane Pods, and EndpointSlices.
func (r *GatewayReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("gcpgateway").
		For(&gatewayv1.Gateway{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.podToGateway)).
		Watches(&discoveryv1.EndpointSlice{}, handler.EnqueueRequestsFromMapFunc(r.endpointSliceToGateway)).
		Complete(r)
}

func (r *GatewayReconciler) podToGateway(ctx context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod == nil {
		return nil
	}
	if pod.Labels[singlepod.LabelAppName] != singlepod.AppNameValue {
		return nil
	}
	gwName := pod.Labels[singlepod.LabelGatewayName]
	if gwName == "" {
		return nil
	}
	return []reconcile.Request{
		{
			NamespacedName: types.NamespacedName{
				Namespace: pod.Namespace,
				Name:      gwName,
			},
		},
	}
}

func (r *GatewayReconciler) endpointSliceToGateway(ctx context.Context, obj client.Object) []reconcile.Request {
	slice, ok := obj.(*discoveryv1.EndpointSlice)
	if !ok || slice == nil {
		return nil
	}
	gwName := slice.Labels[singlepod.LabelGatewayName]
	if gwName != "" {
		return []reconcile.Request{
			{
				NamespacedName: types.NamespacedName{
					Namespace: slice.Namespace,
					Name:      gwName,
				},
			},
		}
	}
	svcName := slice.Labels["kubernetes.io/service-name"]
	if svcName != "" {
		var gwList gatewayv1.GatewayList
		if err := r.Client.List(ctx, &gwList, client.InNamespace(slice.Namespace)); err != nil {
			klog.V(4).Infof("endpointSliceToGateway: listing gateways: %v", err)
			return nil
		}
		for _, gw := range gwList.Items {
			if singlepod.ResourceNameForGateway(gw.Name) == svcName {
				return []reconcile.Request{
					{
						NamespacedName: types.NamespacedName{
							Namespace: slice.Namespace,
							Name:      gw.Name,
						},
					},
				}
			}
		}
	}
	return nil
}

// Reconcile performs the full reconciliation loop for a Gateway.
func (r *GatewayReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	pollInterval := r.healthPollInterval
	if pollInterval <= 0 {
		pollInterval = HealthPollInterval
	}
	resyncInterval := r.resyncInterval
	if resyncInterval <= 0 {
		resyncInterval = ResyncInterval
	}

	var gw gatewayv1.Gateway
	if err := r.Get(ctx, req.NamespacedName, &gw); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	// 1. Check GatewayClass match
	var gc gatewayv1.GatewayClass
	if err := r.Get(ctx, types.NamespacedName{Name: string(gw.Spec.GatewayClassName)}, &gc); err != nil {
		if !apierrors.IsNotFound(err) {
			return reconcile.Result{}, err
		}
	} else if r.ControllerName != "" && string(gc.Spec.ControllerName) != r.ControllerName {
		return reconcile.Result{}, nil
	}

	// 2. Resolve NEG conflicts deterministically across Gateways targeting identical NEGs
	targetNEG, err := r.targetNEGKey(ctx, &gw)
	if err != nil {
		return reconcile.Result{}, err
	}
	var allGateways gatewayv1.GatewayList
	if err := r.List(ctx, &allGateways); err != nil {
		return reconcile.Result{}, fmt.Errorf("listing gateways: %w", err)
	}

	var conflicting []*gatewayv1.Gateway
	for i := range allGateways.Items {
		candidate := &allGateways.Items[i]
		if candidate.Spec.GatewayClassName == gw.Spec.GatewayClassName {
			candKey, err := r.targetNEGKey(ctx, candidate)
			if err != nil {
				return reconcile.Result{}, err
			}
			if candKey == targetNEG {
				conflicting = append(conflicting, candidate)
			}
		}
	}

	if len(conflicting) > 1 {
		winner := registrar.ResolveNEGConflict(conflicting)
		if winner != nil && (winner.Namespace != gw.Namespace || winner.Name != gw.Name) {
			setCondition(&gw, string(gatewayv1.GatewayConditionAccepted), metav1.ConditionFalse, ReasonConflictingParameters, "Configured NEGs are already claimed by an older active Gateway in the cluster.")
			setCondition(&gw, string(gatewayv1.GatewayConditionProgrammed), metav1.ConditionFalse, ReasonConflictingParameters, "Configured NEGs are already claimed by an older active Gateway in the cluster.")
			if err := r.Status().Update(ctx, &gw); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{RequeueAfter: resyncInterval}, nil
		}
	}

	setCondition(&gw, string(gatewayv1.GatewayConditionAccepted), metav1.ConditionTrue, ReasonAccepted, "Gateway spec and parameters are valid.")

	// 3. Resolve external static IP
	externalIP, err := r.resolveExternalIP(ctx, &gw)
	if err != nil {
		return reconcile.Result{}, err
	}

	// 4. Reconcile endpoints with zonal NEGs via the registrar
	reconcileRes, err := r.Registrar.ReconcileEndpoints(ctx, &gw)
	if err != nil {
		r.handleGCPError(&gw, err)
		if updateErr := r.Status().Update(ctx, &gw); updateErr != nil {
			return reconcile.Result{}, updateErr
		}
		return reconcile.Result{RequeueAfter: pollInterval}, nil
	}

	if externalIP == "" {
		setCondition(&gw, string(gatewayv1.GatewayConditionProgrammed), metav1.ConditionFalse, ReasonAddressNotAssigned, "Neither spec.addresses nor parameters ConfigMap provides external static IP.")
		if err := r.Status().Update(ctx, &gw); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{RequeueAfter: resyncInterval}, nil
	}

	// 5. Poll health status from Google Cloud Load Balancer and update readiness gates
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.InNamespace(gw.Namespace)); err != nil {
		return reconcile.Result{}, fmt.Errorf("listing pods: %w", err)
	}

	hasHealthyReplica := false
	isRolloutStalled := false
	dataplanePodCount := 0
	waitingForHealth := false
	now := time.Now()
	activePodKeys := make(map[string]bool)

	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Labels[singlepod.LabelAppName] != singlepod.AppNameValue {
			continue
		}
		if pod.Labels[singlepod.LabelGatewayName] != gw.Name {
			continue
		}
		dataplanePodCount++

		if pod.Status.PodIP == "" || pod.Spec.NodeName == "" || !registrar.IsContainersReady(pod) || pod.DeletionTimestamp != nil {
			continue
		}

		podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
		activePodKeys[podKey] = true

		var node corev1.Node
		if err := r.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, &node); err != nil {
			if !apierrors.IsNotFound(err) {
				return reconcile.Result{}, fmt.Errorf("getting node %s: %w", pod.Spec.NodeName, err)
			}
			continue
		}
		nodeInfo, err := registrar.ResolveNodeInstance(&node)
		if err != nil {
			klog.Warningf("Failed to resolve node instance for pod %s/%s: %v", pod.Namespace, pod.Name, err)
			continue
		}

		// Check health on backend service
		bsName := metadata.ResourceName(r.NegPrefix, r.ClusterID, gw.Namespace, gw.Name, "bs-443")
		negName := metadata.ResourceName(r.NegPrefix, r.ClusterID, gw.Namespace, gw.Name, "8443")
		// backendServices.getHealth takes a resourceGroup reference, which must be the full NEG URL;
		// a bare name is rejected with "Invalid value for field 'resourceGroup.group' ... malformed".
		negURL := fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s/networkEndpointGroups/%s", r.Project, nodeInfo.Zone, negName)

		health, err := r.GCPClient.GetBackendServiceHealth(ctx, r.Project, bsName, negURL)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("getting backend service health for %s: %w", bsName, err)
		}
		isHealthy := false
		if health != nil {
			for _, hs := range health.HealthStatus {
				if (hs.Instance == nodeInfo.Instance || strings.HasSuffix(hs.Instance, "/"+nodeInfo.Instance)) && hs.IpAddress == pod.Status.PodIP && hs.HealthState == "HEALTHY" {
					isHealthy = true
					break
				}
			}
		}

		if isHealthy {
			delete(r.waitingSince, podKey)
			hasHealthyReplica = true
			if err := r.updatePodReadinessGate(ctx, pod, corev1.ConditionTrue, "LoadBalancerHealthy", "Endpoint is healthy on Google Cloud load balancer"); err != nil {
				return reconcile.Result{}, fmt.Errorf("updating readiness gate for pod %s: %w", podKey, err)
			}
		} else {
			since, exists := r.waitingSince[podKey]
			if !exists {
				r.waitingSince[podKey] = now
			} else if now.Sub(since) >= r.HealthTimeout {
				isRolloutStalled = true
			}
		}

		if !isPodReadinessGateTrue(pod) {
			waitingForHealth = true
		}
	}

	for k := range r.waitingSince {
		if !activePodKeys[k] {
			delete(r.waitingSince, k)
		}
	}

	// 6. Update Gateway status conditions and addresses
	if dataplanePodCount == 0 {
		setCondition(&gw, string(gatewayv1.GatewayConditionProgrammed), metav1.ConditionFalse, ReasonWaitingForDataPlane, "Data-plane Deployment has 0 available/ready replicas.")
	} else if isRolloutStalled {
		setCondition(&gw, string(gatewayv1.GatewayConditionProgrammed), metav1.ConditionFalse, ReasonRolloutStalled, "New data-plane pod failed to become healthy on load balancer; rollout halted to protect old pods.")
	} else if !hasHealthyReplica {
		setCondition(&gw, string(gatewayv1.GatewayConditionProgrammed), metav1.ConditionFalse, ReasonWaitingForEndpoints, "Ready pods exist, but attachment to zonal NEGs or health check convergence is in-flight.")
	} else {
		setCondition(&gw, string(gatewayv1.GatewayConditionProgrammed), metav1.ConditionTrue, ReasonProgrammed, "Ready data-plane pods attached to zonal NEGs and passing health checks behind Google Cloud Load Balancer")
		addrType := gatewayv1.IPAddressType
		gw.Status.Addresses = []gatewayv1.GatewayStatusAddress{
			{
				Type:  &addrType,
				Value: externalIP,
			},
		}
	}

	_ = reconcileRes
	if err := r.Status().Update(ctx, &gw); err != nil {
		return reconcile.Result{}, err
	}

	if waitingForHealth {
		return reconcile.Result{RequeueAfter: pollInterval}, nil
	}
	return reconcile.Result{RequeueAfter: resyncInterval}, nil
}

func isPodReadinessGateTrue(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	gateType := corev1.PodConditionType(registrar.LoadBalancerReadyGate)
	for _, cond := range pod.Status.Conditions {
		if cond.Type == gateType {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (r *GatewayReconciler) targetNEGKey(ctx context.Context, gw *gatewayv1.Gateway) (string, error) {
	if gw.Spec.Infrastructure != nil && gw.Spec.Infrastructure.ParametersRef != nil {
		ref := gw.Spec.Infrastructure.ParametersRef
		if ref.Kind == "ConfigMap" && ref.Name != "" {
			var cm corev1.ConfigMap
			err := r.Get(ctx, types.NamespacedName{Namespace: gw.Namespace, Name: string(ref.Name)}, &cm)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return "", fmt.Errorf("getting parameters ConfigMap %s: %w", ref.Name, err)
				}
			} else {
				if neg := cm.Data["negHTTPS"]; neg != "" {
					return neg, nil
				}
				if targetGW := cm.Data["gatewayName"]; targetGW != "" {
					ns := cm.Data["gatewayNamespace"]
					if ns == "" {
						ns = gw.Namespace
					}
					return metadata.ResourceName(r.NegPrefix, r.ClusterID, ns, targetGW, "8443"), nil
				}
			}
		}
	}
	return metadata.ResourceName(r.NegPrefix, r.ClusterID, gw.Namespace, gw.Name, "8443"), nil
}

func (r *GatewayReconciler) resolveExternalIP(ctx context.Context, gw *gatewayv1.Gateway) (string, error) {
	for _, addr := range gw.Spec.Addresses {
		if addr.Type == nil || *addr.Type == gatewayv1.IPAddressType {
			if strings.TrimSpace(addr.Value) != "" {
				return strings.TrimSpace(addr.Value), nil
			}
		}
	}

	if gw.Spec.Infrastructure != nil && gw.Spec.Infrastructure.ParametersRef != nil {
		ref := gw.Spec.Infrastructure.ParametersRef
		if ref.Kind == "ConfigMap" && ref.Name != "" {
			var cm corev1.ConfigMap
			err := r.Get(ctx, types.NamespacedName{Namespace: gw.Namespace, Name: string(ref.Name)}, &cm)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return "", fmt.Errorf("getting parameters ConfigMap %s: %w", ref.Name, err)
				}
			} else {
				if ip, ok := cm.Data["externalIP"]; ok && strings.TrimSpace(ip) != "" {
					return strings.TrimSpace(ip), nil
				}
			}
		}
	}

	return "", nil
}

func (r *GatewayReconciler) updatePodReadinessGate(ctx context.Context, pod *corev1.Pod, status corev1.ConditionStatus, reason, message string) error {
	gateType := corev1.PodConditionType(registrar.LoadBalancerReadyGate)
	for i, cond := range pod.Status.Conditions {
		if cond.Type == gateType {
			if cond.Status == status {
				return nil
			}
			pod.Status.Conditions[i].Status = status
			pod.Status.Conditions[i].Reason = reason
			pod.Status.Conditions[i].Message = message
			pod.Status.Conditions[i].LastTransitionTime = metav1.Now()
			return r.Status().Update(ctx, pod)
		}
	}

	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:               gateType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	})
	return r.Status().Update(ctx, pod)
}

func (r *GatewayReconciler) handleGCPError(gw *gatewayv1.Gateway, err error) {
	if gcp.IsPermissionDenied(err) {
		setCondition(gw, string(gatewayv1.GatewayConditionProgrammed), metav1.ConditionFalse, ReasonPermissionDenied, "GCP service account lacks compute.networkEndpointGroups.use or attach permission.")
	} else if gcp.IsQuotaExceeded(err) {
		setCondition(gw, string(gatewayv1.GatewayConditionProgrammed), metav1.ConditionFalse, ReasonQuotaExceeded, "Compute API quota exceeded when registering endpoints.")
	} else if gcp.IsNEGNotFound(err) {
		setCondition(gw, string(gatewayv1.GatewayConditionProgrammed), metav1.ConditionFalse, ReasonNEGNotFound, "One or more configured NEGs do not exist in the GCP project/zones.")
	} else if gcp.IsInvalidIP(err) {
		setCondition(gw, string(gatewayv1.GatewayConditionProgrammed), metav1.ConditionFalse, ReasonUnsupportedCluster, "Pod IP is not a valid VPC alias IP.")
	} else {
		setCondition(gw, string(gatewayv1.GatewayConditionProgrammed), metav1.ConditionFalse, ReasonEndpointAttachmentFailed, err.Error())
	}
}

func setCondition(gw *gatewayv1.Gateway, condType string, status metav1.ConditionStatus, reason, message string) {
	for i, cond := range gw.Status.Conditions {
		if cond.Type == condType {
			gw.Status.Conditions[i].Status = status
			gw.Status.Conditions[i].Reason = reason
			gw.Status.Conditions[i].Message = message
			gw.Status.Conditions[i].LastTransitionTime = metav1.Now()
			gw.Status.Conditions[i].ObservedGeneration = gw.Generation
			return
		}
	}
	gw.Status.Conditions = append(gw.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
		ObservedGeneration: gw.Generation,
	})
}
