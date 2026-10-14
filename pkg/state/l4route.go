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

package state

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// InternalL4Backend represents a resolved backend target and weight for an L4 route (TCPRoute or UDPRoute).
type InternalL4Backend struct {
	Target string
	Weight int32
}

// InternalTCPBackend is an alias for InternalL4Backend.
type InternalTCPBackend = InternalL4Backend

// InternalUDPBackend is an alias for InternalL4Backend.
type InternalUDPBackend = InternalL4Backend

// resolveL4BackendRefs compiles a slice of gatewayv1.BackendRef into InternalL4Backend targets,
// enforcing Kind support (Service only), ReferenceGrant for cross-namespace references,
// Service existence, and preserving weights including 0.
func resolveL4BackendRefs(
	backendRefs []gatewayv1.BackendRef,
	routeNamespace string,
	routeGeneration int64,
	routeKind string,
	services map[types.NamespacedName]*corev1.Service,
	refValidator ReferenceGrantValidator,
	resolvedRefsCondition *metav1.Condition,
) []InternalL4Backend {
	var backends []InternalL4Backend
	for _, backendRef := range backendRefs {
		group := ValueOf(backendRef.Group)
		kind := ValueOf(backendRef.Kind)
		if kind == "" {
			kind = "Service"
		}

		if (group != "" && group != "core") || kind != "Service" {
			var msg string
			if group != "" && group != "core" {
				msg = fmt.Sprintf("Unsupported backend: %s/%s", group, kind)
			} else {
				msg = fmt.Sprintf("Unsupported backend kind: %s", kind)
			}
			*resolvedRefsCondition = NewCondition(
				string(gatewayv1.RouteConditionResolvedRefs),
				metav1.ConditionFalse,
				string(gatewayv1.RouteReasonInvalidKind),
				msg,
				routeGeneration,
			)
			continue
		}

		svcNamespace := routeNamespace
		if backendRef.Namespace != nil && string(*backendRef.Namespace) != "" {
			svcNamespace = string(*backendRef.Namespace)
		}

		// Cross-namespace ReferenceGrant check
		if svcNamespace != routeNamespace {
			from := Reference{
				GroupKind: schema.GroupKind{Group: gatewayv1.GroupName, Kind: routeKind},
				Namespace: routeNamespace,
			}
			to := Reference{
				GroupKind: schema.GroupKind{Group: string(group), Kind: string(kind)},
				Namespace: svcNamespace,
				Name:      string(backendRef.Name),
			}
			if refValidator == nil || !refValidator.IsReferencePermitted(from, to) {
				msg := fmt.Sprintf("Cross-namespace reference to service %s/%s is not permitted by any ReferenceGrant", svcNamespace, string(backendRef.Name))
				*resolvedRefsCondition = NewCondition(
					string(gatewayv1.RouteConditionResolvedRefs),
					metav1.ConditionFalse,
					string(gatewayv1.RouteReasonRefNotPermitted),
					msg,
					routeGeneration,
				)
				continue
			}
		}

		port := int32(0)
		if backendRef.Port != nil {
			port = int32(*backendRef.Port)
		}

		svcKey := types.NamespacedName{
			Namespace: svcNamespace,
			Name:      string(backendRef.Name),
		}

		if services != nil {
			svc, ok := services[svcKey]
			if !ok || svc == nil {
				msg := fmt.Sprintf("Backend service %s/%s not found", svcNamespace, string(backendRef.Name))
				*resolvedRefsCondition = NewCondition(
					string(gatewayv1.RouteConditionResolvedRefs),
					metav1.ConditionFalse,
					string(gatewayv1.RouteReasonBackendNotFound),
					msg,
					routeGeneration,
				)
				continue
			}

			for _, p := range svc.Spec.Ports {
				if p.Port == port {
					if svc.Spec.ClusterIP == corev1.ClusterIPNone && p.TargetPort.IntValue() > 0 {
						port = int32(p.TargetPort.IntValue())
					}
					break
				}
			}
		}

		weight := int32(1)
		if backendRef.Weight != nil {
			weight = *backendRef.Weight
			if weight < 0 {
				weight = 0
			}
		}

		target := fmt.Sprintf("%s.%s.svc.cluster.local:%d", backendRef.Name, svcNamespace, port)
		backends = append(backends, InternalL4Backend{
			Target: target,
			Weight: weight,
		})
	}
	return backends
}

func isL4RouteAccepted(parents []gatewayv1.RouteParentStatus, controllerName string) bool {
	for _, ps := range parents {
		if string(ps.ControllerName) == controllerName {
			for _, c := range ps.Conditions {
				if c.Type == string(gatewayv1.RouteConditionAccepted) && c.Status == metav1.ConditionTrue {
					return true
				}
			}
		}
	}
	return false
}

func isL4RouteAcceptedForParentRef(parents []gatewayv1.RouteParentStatus, parentRef gatewayv1.ParentReference, routeNamespace string, controllerName string) bool {
	parentNamespace := routeNamespace
	if ns := ValueOf(parentRef.Namespace); ns != "" {
		parentNamespace = string(ns)
	}
	pKind := ValueOf(parentRef.Kind)
	if pKind == "" {
		pKind = "Gateway"
	}
	pGroup := ValueOf(parentRef.Group)
	if pGroup == "" {
		pGroup = gatewayv1.GroupName
	}
	for _, ps := range parents {
		if string(ps.ControllerName) != controllerName {
			continue
		}
		psNamespace := routeNamespace
		if ns := ValueOf(ps.ParentRef.Namespace); ns != "" {
			psNamespace = string(ns)
		}
		psKind := ValueOf(ps.ParentRef.Kind)
		if psKind == "" {
			psKind = "Gateway"
		}
		psGroup := ValueOf(ps.ParentRef.Group)
		if psGroup == "" {
			psGroup = gatewayv1.GroupName
		}
		if string(ps.ParentRef.Name) == string(parentRef.Name) &&
			psNamespace == parentNamespace &&
			psKind == pKind &&
			psGroup == pGroup &&
			ValueOf(ps.ParentRef.SectionName) == ValueOf(parentRef.SectionName) &&
			ValueOf(ps.ParentRef.Port) == ValueOf(parentRef.Port) {
			for _, c := range ps.Conditions {
				if c.Type == string(gatewayv1.RouteConditionAccepted) && c.Status == metav1.ConditionTrue {
					return true
				}
			}
		}
	}
	return false
}
