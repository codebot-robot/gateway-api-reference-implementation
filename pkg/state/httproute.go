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
	"regexp"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

type HTTPRouteState struct {
	*gatewayv1.HTTPRoute
}

func (s *HTTPRouteState) Validate() error {
	if s.HTTPRoute == nil {
		return nil
	}
	for _, rule := range s.Spec.Rules {
		for _, filter := range rule.Filters {
			switch filter.Type {
			case gatewayv1.HTTPRouteFilterRequestRedirect,
				gatewayv1.HTTPRouteFilterURLRewrite,
				gatewayv1.HTTPRouteFilterRequestHeaderModifier,
				gatewayv1.HTTPRouteFilterResponseHeaderModifier:
			default:
				return fmt.Errorf("unsupported filter type: %s", filter.Type)
			}
		}
		for _, backendRef := range rule.BackendRefs {
			for _, filter := range backendRef.Filters {
				switch filter.Type {
				case gatewayv1.HTTPRouteFilterRequestHeaderModifier,
					gatewayv1.HTTPRouteFilterResponseHeaderModifier:
				default:
					return fmt.Errorf("unsupported backend filter type: %s", filter.Type)
				}
			}
		}
		for _, match := range rule.Matches {
			for _, header := range match.Headers {
				if ValueOf(header.Type) == gatewayv1.HeaderMatchRegularExpression {
					if _, err := regexp.Compile(header.Value); err != nil {
						return fmt.Errorf("invalid regular expression in header match: %w", err)
					}
				}
			}
		}
	}
	return nil
}

func (s *HTTPRouteState) ComputeAcceptedCondition(parentRef gatewayv1.ParentReference, gateways []*GatewayState) metav1.Condition {
	acceptedStatus := metav1.ConditionTrue
	acceptedReason := gatewayv1.RouteReasonAccepted
	acceptedMessage := "Route accepted by reference implementation"

	if err := s.Validate(); err != nil {
		acceptedStatus = metav1.ConditionFalse
		acceptedReason = gatewayv1.RouteReasonUnsupportedValue
		acceptedMessage = fmt.Sprintf("Invalid route: %v", err)
	} else if group := ValueOf(parentRef.Group); group != "" && group != "gateway.networking.k8s.io" {
		acceptedStatus = metav1.ConditionFalse
		acceptedReason = gatewayv1.RouteReasonNoMatchingParent
		acceptedMessage = fmt.Sprintf("Unsupported parent group: %s", group)
	} else if kind := ValueOf(parentRef.Kind); kind != "" && kind != "Gateway" {
		acceptedStatus = metav1.ConditionFalse
		acceptedReason = gatewayv1.RouteReasonNoMatchingParent
		acceptedMessage = fmt.Sprintf("Unsupported parent kind: %s", kind)
	} else {
		// Check if Gateway exists and has matching listeners
		var gw *GatewayState
		targetNamespace := s.Namespace
		if parentNamespace := ValueOf(parentRef.Namespace); parentNamespace != "" {
			targetNamespace = string(parentNamespace)
		}
		for _, g := range gateways {
			if g.Name == string(parentRef.Name) && (targetNamespace == "" || g.Namespace == "" || g.Namespace == targetNamespace) {
				gw = g
				break
			}
		}

		if gw == nil {
			acceptedStatus = metav1.ConditionFalse
			acceptedReason = gatewayv1.RouteReasonNoMatchingParent
			acceptedMessage = "Gateway not found"
		} else {
			hasMatchingListener := false
			hasAllowedListener := false
			hasMatchingHostname := false

			for _, listener := range gw.Spec.Listeners {
				if sectionName := ValueOf(parentRef.SectionName); sectionName != "" && sectionName != listener.Name {
					continue
				}
				if port := ValueOf(parentRef.Port); port != 0 && port != listener.Port {
					continue
				}
				hasMatchingListener = true

				// Check protocol compatibility
				if listener.Protocol != gatewayv1.HTTPProtocolType && listener.Protocol != gatewayv1.HTTPSProtocolType {
					continue
				}

				// Check AllowedRoutes kinds
				if listener.AllowedRoutes != nil && len(listener.AllowedRoutes.Kinds) > 0 {
					kindAllowed := false
					for _, k := range listener.AllowedRoutes.Kinds {
						if IsHTTPRoute(k.Group, k.Kind) {
							kindAllowed = true
							break
						}
					}
					if !kindAllowed {
						continue
					}
				}

				// Check AllowedRoutes namespaces
				if listener.AllowedRoutes != nil && listener.AllowedRoutes.Namespaces != nil && listener.AllowedRoutes.Namespaces.From != nil {
					switch *listener.AllowedRoutes.Namespaces.From {
					case gatewayv1.NamespacesFromSame:
						if s.Namespace != gw.Namespace {
							continue
						}
					case gatewayv1.NamespacesFromAll:
						// Allowed
					case gatewayv1.NamespacesFromSelector:
						if s.Namespace != gw.Namespace && listener.AllowedRoutes.Namespaces.Selector == nil {
							continue
						}
					}
				} else {
					// Default is Same namespace
					if s.Namespace != gw.Namespace {
						continue
					}
				}

				hasAllowedListener = true

				effectiveHostnames := IntersectHostnames(s.GetHostnames(), string(ValueOf(listener.Hostname)))
				if len(effectiveHostnames) > 0 || len(s.Spec.Hostnames) == 0 {
					hasMatchingHostname = true
					break
				}
			}

			if hasMatchingHostname {
				acceptedStatus = metav1.ConditionTrue
				acceptedReason = gatewayv1.RouteReasonAccepted
				acceptedMessage = "Route accepted by reference implementation"
			} else if hasAllowedListener {
				acceptedStatus = metav1.ConditionFalse
				acceptedReason = gatewayv1.RouteReasonNoMatchingListenerHostname
				acceptedMessage = "No matching listener hostname"
			} else if hasMatchingListener {
				acceptedStatus = metav1.ConditionFalse
				acceptedReason = gatewayv1.RouteReasonNotAllowedByListeners
				acceptedMessage = "Not allowed by listener permissions or protocol"
			} else {
				acceptedStatus = metav1.ConditionFalse
				acceptedReason = gatewayv1.RouteReasonNoMatchingParent
				acceptedMessage = "No matching listener for parentRef"
			}
		}
	}

	return metav1.Condition{
		Type:               string(gatewayv1.RouteConditionAccepted),
		Status:             acceptedStatus,
		ObservedGeneration: s.Generation,
		LastTransitionTime: metav1.Now(),
		Reason:             string(acceptedReason),
		Message:            acceptedMessage,
	}
}

func (s *HTTPRouteState) ComputeResolvedRefsCondition(services map[types.NamespacedName]*corev1.Service, refValidator ReferenceGrantValidator) metav1.Condition {
	resolvedRefsStatus := metav1.ConditionTrue
	resolvedRefsReason := gatewayv1.RouteReasonResolvedRefs
	resolvedRefsMessage := "All references resolved"

	for _, rule := range s.Spec.Rules {
		for _, backendRef := range rule.BackendRefs {
			group := ""
			if backendRef.Group != nil {
				group = string(*backendRef.Group)
			}
			kind := "Service"
			if backendRef.Kind != nil {
				kind = string(*backendRef.Kind)
			}
			if (group != "" && group != "core") || kind != "Service" {
				resolvedRefsStatus = metav1.ConditionFalse
				resolvedRefsReason = gatewayv1.RouteReasonInvalidKind
				if group != "" && group != "core" {
					resolvedRefsMessage = fmt.Sprintf("Unsupported backend: %s/%s", group, kind)
				} else {
					resolvedRefsMessage = fmt.Sprintf("Unsupported backend kind: %s", kind)
				}
				goto done
			}

			backendNs := s.Namespace
			if backendRef.Namespace != nil && string(*backendRef.Namespace) != "" {
				backendNs = string(*backendRef.Namespace)
			}

			if backendNs != s.Namespace {
				from := Reference{
					GroupKind: schema.GroupKind{Group: gatewayv1.GroupName, Kind: "HTTPRoute"},
					Namespace: s.Namespace,
				}
				to := Reference{
					GroupKind: schema.GroupKind{Group: group, Kind: kind},
					Namespace: backendNs,
					Name:      string(backendRef.Name),
				}
				if refValidator == nil || !refValidator.IsReferencePermitted(from, to) {
					resolvedRefsStatus = metav1.ConditionFalse
					resolvedRefsReason = gatewayv1.RouteReasonRefNotPermitted
					resolvedRefsMessage = fmt.Sprintf("Cross-namespace reference to service %s/%s is not permitted by any ReferenceGrant", backendNs, string(backendRef.Name))
					goto done
				}
			}

			if services != nil {
				svcName := types.NamespacedName{
					Namespace: backendNs,
					Name:      string(backendRef.Name),
				}
				if svc, ok := services[svcName]; !ok || svc == nil {
					resolvedRefsStatus = metav1.ConditionFalse
					resolvedRefsReason = gatewayv1.RouteReasonBackendNotFound
					resolvedRefsMessage = fmt.Sprintf("Backend service %s/%s not found", backendNs, string(backendRef.Name))
					goto done
				}
			}
		}
	}

done:
	return metav1.Condition{
		Type:               string(gatewayv1.RouteConditionResolvedRefs),
		Status:             resolvedRefsStatus,
		ObservedGeneration: s.Generation,
		LastTransitionTime: metav1.Now(),
		Reason:             string(resolvedRefsReason),
		Message:            resolvedRefsMessage,
	}
}

func (s *HTTPRouteState) IsAccepted(controllerName string) bool {
	if s.HTTPRoute == nil {
		return false
	}
	for _, ps := range s.HTTPRoute.Status.Parents {
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

func (s *HTTPRouteState) IsAcceptedForParentRef(parentRef gatewayv1.ParentReference, controllerName string) bool {
	if s.HTTPRoute == nil {
		return false
	}
	parentNamespace := s.Namespace
	if ns := ValueOf(parentRef.Namespace); ns != "" {
		parentNamespace = string(ns)
	}
	for _, ps := range s.HTTPRoute.Status.Parents {
		if string(ps.ControllerName) != controllerName {
			continue
		}
		psNamespace := s.Namespace
		if ns := ValueOf(ps.ParentRef.Namespace); ns != "" {
			psNamespace = string(ns)
		}
		if string(ps.ParentRef.Name) == string(parentRef.Name) &&
			psNamespace == parentNamespace &&
			ValueOf(ps.ParentRef.SectionName) == ValueOf(parentRef.SectionName) &&
			ValueOf(ps.ParentRef.Port) == ValueOf(parentRef.Port) &&
			ValueOf(ps.ParentRef.Group) == ValueOf(parentRef.Group) &&
			ValueOf(ps.ParentRef.Kind) == ValueOf(parentRef.Kind) {
			for _, c := range ps.Conditions {
				if c.Type == string(gatewayv1.RouteConditionAccepted) && c.Status == metav1.ConditionTrue {
					return true
				}
			}
		}
	}
	return false
}

func (s *HTTPRouteState) MatchesGateway(gw *gatewayv1.Gateway, controllerName string) bool {
	if s.HTTPRoute == nil {
		return false
	}

	for _, ps := range s.HTTPRoute.Status.Parents {
		if string(ps.ControllerName) == controllerName {
			if string(ps.ParentRef.Name) == gw.Name {
				// Note: for now we only check name, but should check namespace too if specified
				for _, c := range ps.Conditions {
					if c.Type == string(gatewayv1.RouteConditionAccepted) && c.Status == metav1.ConditionTrue {
						return true
					}
				}
			}
		}
	}

	return false
}
