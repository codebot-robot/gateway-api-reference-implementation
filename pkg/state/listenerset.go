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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// ListenerSetState encapsulates a ListenerSet resource.
type ListenerSetState struct {
	*gatewayv1.ListenerSet
}

// IsListenerSetParent reports whether the given ListenerSet references the given Gateway as its parent.
func IsListenerSetParent(ls *gatewayv1.ListenerSet, gw *gatewayv1.Gateway) bool {
	if ls == nil || gw == nil {
		return false
	}
	parentGroup := ValueOf(ls.Spec.ParentRef.Group)
	if parentGroup != "" && parentGroup != gatewayv1.GroupName {
		return false
	}
	parentKind := ValueOf(ls.Spec.ParentRef.Kind)
	if parentKind != "" && parentKind != "Gateway" {
		return false
	}
	if string(ls.Spec.ParentRef.Name) != gw.Name {
		return false
	}
	parentNs := ls.Namespace
	if ns := ValueOf(ls.Spec.ParentRef.Namespace); ns != "" {
		parentNs = string(ns)
	}
	return parentNs == gw.Namespace
}

// IsListenerSetAllowed checks whether the parent Gateway allows the ListenerSet based on AllowedListeners.
func IsListenerSetAllowed(ls *gatewayv1.ListenerSet, gw *gatewayv1.Gateway, namespaces map[string]*corev1.Namespace) bool {
	if ls == nil || gw == nil {
		return false
	}

	// By default, Gateways do not allow ListenerSets (AllowedListeners.Namespaces.From defaults to None).
	if gw.Spec.AllowedListeners == nil || gw.Spec.AllowedListeners.Namespaces == nil || gw.Spec.AllowedListeners.Namespaces.From == nil {
		return false
	}

	switch *gw.Spec.AllowedListeners.Namespaces.From {
	case gatewayv1.NamespacesFromNone:
		return false
	case gatewayv1.NamespacesFromSame:
		return ls.Namespace == gw.Namespace
	case gatewayv1.NamespacesFromAll:
		return true
	case gatewayv1.NamespacesFromSelector:
		if gw.Spec.AllowedListeners.Namespaces.Selector == nil {
			return false
		}
		selector, err := metav1.LabelSelectorAsSelector(gw.Spec.AllowedListeners.Namespaces.Selector)
		if err != nil {
			return false
		}
		var nsObj *corev1.Namespace
		if namespaces != nil {
			nsObj = namespaces[ls.Namespace]
		}
		if nsObj == nil {
			return false
		}
		return selector.Matches(labels.Set(nsObj.Labels))
	default:
		return false
	}
}

// ComputeListenerSetConditions computes the Accepted and Programmed conditions for a ListenerSet given its compiled effective listeners.
func ComputeListenerSetConditions(ls *gatewayv1.ListenerSet, gw *gatewayv1.Gateway, namespaces map[string]*corev1.Namespace, effectiveListeners []*EffectiveListener) (metav1.Condition, metav1.Condition) {
	if gw == nil {
		return NewCondition(
				string(gatewayv1.ListenerSetConditionAccepted),
				metav1.ConditionFalse,
				string(gatewayv1.ListenerSetReasonParentNotAccepted),
				"Parent Gateway not found or not accepted",
				ls.Generation,
			),
			NewCondition(
				string(gatewayv1.ListenerSetConditionProgrammed),
				metav1.ConditionFalse,
				string(gatewayv1.ListenerSetReasonParentNotAccepted),
				"Parent Gateway not found or not accepted",
				ls.Generation,
			)
	}

	if !IsListenerSetAllowed(ls, gw, namespaces) {
		return NewCondition(
				string(gatewayv1.ListenerSetConditionAccepted),
				metav1.ConditionFalse,
				string(gatewayv1.ListenerSetReasonNotAllowed),
				"ListenerSet is not allowed by parent Gateway allowedListeners configuration",
				ls.Generation,
			),
			NewCondition(
				string(gatewayv1.ListenerSetConditionProgrammed),
				metav1.ConditionFalse,
				string(gatewayv1.ListenerSetReasonNotAllowed),
				"ListenerSet is not allowed by parent Gateway allowedListeners configuration",
				ls.Generation,
			)
	}

	if effectiveListeners != nil {
		totalListeners := len(ls.Spec.Listeners)
		validCount := 0
		for _, el := range effectiveListeners {
			if el.Owner.Kind == "ListenerSet" && el.Owner.Namespace == ls.Namespace && el.Owner.Name == ls.Name {
				if el.IsValid() {
					validCount++
				}
			}
		}

		if totalListeners == 0 || validCount == 0 {
			return NewCondition(
					string(gatewayv1.ListenerSetConditionAccepted),
					metav1.ConditionFalse,
					string(gatewayv1.ListenerSetReasonListenersNotValid),
					"All listeners in ListenerSet are invalid or conflicted",
					ls.Generation,
				),
				NewCondition(
					string(gatewayv1.ListenerSetConditionProgrammed),
					metav1.ConditionFalse,
					string(gatewayv1.ListenerSetReasonListenersNotValid),
					"All listeners in ListenerSet are invalid or conflicted",
					ls.Generation,
				)
		}
	}

	return NewCondition(
			string(gatewayv1.ListenerSetConditionAccepted),
			metav1.ConditionTrue,
			string(gatewayv1.ListenerSetReasonAccepted),
			"ListenerSet accepted by reference implementation",
			ls.Generation,
		),
		NewCondition(
			string(gatewayv1.ListenerSetConditionProgrammed),
			metav1.ConditionTrue,
			string(gatewayv1.ListenerSetReasonProgrammed),
			"ListenerSet programmed by reference implementation",
			ls.Generation,
		)
}

// ComputeListenerSetAcceptedCondition computes the Accepted and Programmed conditions for a ListenerSet without listener aggregation.
func ComputeListenerSetAcceptedCondition(ls *gatewayv1.ListenerSet, gw *gatewayv1.Gateway, namespaces map[string]*corev1.Namespace) (metav1.Condition, metav1.Condition) {
	return ComputeListenerSetConditions(ls, gw, namespaces, nil)
}
