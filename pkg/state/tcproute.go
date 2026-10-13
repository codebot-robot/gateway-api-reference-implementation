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
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// InternalTCPBackend represents a resolved backend target and weight for a TCPRoute.
type InternalTCPBackend struct {
	Target string
	Weight int32
}

// InternalTCPRule represents a compiled TCPRoute rule with resolved backend targets.
type InternalTCPRule struct {
	Name     *gatewayv1.SectionName
	Backends []InternalTCPBackend
}

// InternalTCPRoute represents a compiled TCPRoute containing resolved references,
// rules, and conditions.
type InternalTCPRoute struct {
	Route                 *gatewayv1.TCPRoute
	Rules                 []InternalTCPRule
	ParentRefs            []gatewayv1.ParentReference
	ValidationCondition   metav1.Condition
	ResolvedRefsCondition metav1.Condition
}

// TCPRouteState wraps a TCPRoute with its compiled representation.
type TCPRouteState struct {
	*gatewayv1.TCPRoute
	Internal *InternalTCPRoute
}

// GetNamespace returns the namespace of the route.
func (s *TCPRouteState) GetNamespace() string {
	if s == nil || s.TCPRoute == nil {
		return ""
	}
	return s.Namespace
}

// Validate checks that the TCPRoute values are syntactically and structurally correct.
func (s *TCPRouteState) Validate() error {
	if s == nil || s.TCPRoute == nil {
		return nil
	}
	if s.Internal == nil {
		s.Compile(nil, nil)
	}
	if s.Internal.ValidationCondition.Status == metav1.ConditionFalse {
		return errors.New(s.Internal.ValidationCondition.Message)
	}
	return nil
}

// Compile compiles the TCPRoute into an InternalTCPRoute, resolving references and recording conditions.
func (s *TCPRouteState) Compile(
	services map[types.NamespacedName]*corev1.Service,
	refValidator ReferenceGrantValidator,
) *InternalTCPRoute {
	if s == nil || s.TCPRoute == nil {
		return nil
	}
	s.Internal = CompileTCPRoute(s.TCPRoute, services, refValidator)
	return s.Internal
}

// CompileTCPRoute parses and compiles a TCPRoute object into an InternalTCPRoute.
func CompileTCPRoute(
	route *gatewayv1.TCPRoute,
	services map[types.NamespacedName]*corev1.Service,
	refValidator ReferenceGrantValidator,
) *InternalTCPRoute {
	if route == nil {
		return nil
	}

	validationCondition := NewCondition(
		string(gatewayv1.RouteConditionAccepted),
		metav1.ConditionTrue,
		string(gatewayv1.RouteReasonAccepted),
		"Route validation succeeded",
		route.Generation,
	)

	resolvedRefsCondition := NewCondition(
		string(gatewayv1.RouteConditionResolvedRefs),
		metav1.ConditionTrue,
		string(gatewayv1.RouteReasonResolvedRefs),
		"All references resolved",
		route.Generation,
	)

	var compiledRules []InternalTCPRule
	for _, rule := range route.Spec.Rules {
		iRule := InternalTCPRule{
			Name: rule.Name,
		}

		for _, backendRef := range rule.BackendRefs {
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
				resolvedRefsCondition = NewCondition(
					string(gatewayv1.RouteConditionResolvedRefs),
					metav1.ConditionFalse,
					string(gatewayv1.RouteReasonInvalidKind),
					msg,
					route.Generation,
				)
				continue
			}

			svcNamespace := route.Namespace
			if backendRef.Namespace != nil && string(*backendRef.Namespace) != "" {
				svcNamespace = string(*backendRef.Namespace)
			}

			// Cross-namespace ReferenceGrant check
			if svcNamespace != route.Namespace {
				from := Reference{
					GroupKind: schema.GroupKind{Group: gatewayv1.GroupName, Kind: "TCPRoute"},
					Namespace: route.Namespace,
				}
				to := Reference{
					GroupKind: schema.GroupKind{Group: string(group), Kind: string(kind)},
					Namespace: svcNamespace,
					Name:      string(backendRef.Name),
				}
				if refValidator == nil || !refValidator.IsReferencePermitted(from, to) {
					msg := fmt.Sprintf("Cross-namespace reference to service %s/%s is not permitted by any ReferenceGrant", svcNamespace, string(backendRef.Name))
					resolvedRefsCondition = NewCondition(
						string(gatewayv1.RouteConditionResolvedRefs),
						metav1.ConditionFalse,
						string(gatewayv1.RouteReasonRefNotPermitted),
						msg,
						route.Generation,
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
					resolvedRefsCondition = NewCondition(
						string(gatewayv1.RouteConditionResolvedRefs),
						metav1.ConditionFalse,
						string(gatewayv1.RouteReasonBackendNotFound),
						msg,
						route.Generation,
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
			iRule.Backends = append(iRule.Backends, InternalTCPBackend{
				Target: target,
				Weight: weight,
			})
		}

		compiledRules = append(compiledRules, iRule)
	}

	return &InternalTCPRoute{
		Route:                 route,
		Rules:                 compiledRules,
		ParentRefs:            route.Spec.ParentRefs,
		ValidationCondition:   validationCondition,
		ResolvedRefsCondition: resolvedRefsCondition,
	}
}

// IsAccepted returns true if the route has an Accepted condition with Status True for the given controller.
func (s *TCPRouteState) IsAccepted(controllerName string) bool {
	if s == nil || s.TCPRoute == nil {
		return false
	}
	for _, ps := range s.Status.Parents {
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

// IsAcceptedForParentRef checks if the route is accepted for a specific parent reference.
func (s *TCPRouteState) IsAcceptedForParentRef(parentRef gatewayv1.ParentReference, controllerName string) bool {
	if s == nil || s.TCPRoute == nil {
		return false
	}
	parentNamespace := s.Namespace
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
	for _, ps := range s.Status.Parents {
		if string(ps.ControllerName) != controllerName {
			continue
		}
		psNamespace := s.Namespace
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
