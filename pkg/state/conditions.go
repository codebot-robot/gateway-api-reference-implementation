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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// NewCondition constructs a metav1.Condition with the current transition time.
func NewCondition(condType string, status metav1.ConditionStatus, reason, message string, observedGeneration int64) metav1.Condition {
	return metav1.Condition{
		Type:               condType,
		Status:             status,
		ObservedGeneration: observedGeneration,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	}
}

// SetCondition updates or adds a condition in a slice of conditions.
// It preserves LastTransitionTime if the Status has not changed.
// It returns true if the condition was added or modified.
func SetCondition(conditions *[]metav1.Condition, newCond metav1.Condition) bool {
	if conditions == nil {
		return false
	}
	for i, existing := range *conditions {
		if existing.Type == newCond.Type {
			if existing.Status == newCond.Status {
				newCond.LastTransitionTime = existing.LastTransitionTime
			} else if newCond.LastTransitionTime.IsZero() {
				newCond.LastTransitionTime = metav1.Now()
			}
			if existing.Status == newCond.Status &&
				existing.Reason == newCond.Reason &&
				existing.Message == newCond.Message &&
				existing.ObservedGeneration == newCond.ObservedGeneration {
				return false
			}
			(*conditions)[i] = newCond
			return true
		}
	}
	if newCond.LastTransitionTime.IsZero() {
		newCond.LastTransitionTime = metav1.Now()
	}
	*conditions = append(*conditions, newCond)
	return true
}

// SetConditions updates existing conditions slice with new conditions,
// preserving LastTransitionTime when Status has not changed.
// It returns true if any condition was added, modified, or removed.
func SetConditions(conditions *[]metav1.Condition, newConditions []metav1.Condition) bool {
	if conditions == nil {
		return false
	}
	changed := false
	for _, newCond := range newConditions {
		if SetCondition(conditions, newCond) {
			changed = true
		}
	}
	if len(*conditions) != len(newConditions) {
		filtered := make([]metav1.Condition, 0, len(newConditions))
		for _, nc := range newConditions {
			for _, ec := range *conditions {
				if ec.Type == nc.Type {
					filtered = append(filtered, ec)
					break
				}
			}
		}
		if len(filtered) != len(*conditions) {
			*conditions = filtered
			changed = true
		}
	}
	return changed
}

// ConditionsEqual compares two condition slices for equality (ignoring LastTransitionTime).
func ConditionsEqual(a, b []metav1.Condition) bool {
	if len(a) != len(b) {
		return false
	}
	for _, cA := range a {
		found := false
		for _, cB := range b {
			if cA.Type == cB.Type {
				if cA.Status == cB.Status &&
					cA.ObservedGeneration == cB.ObservedGeneration &&
					cA.Reason == cB.Reason &&
					cA.Message == cB.Message {
					found = true
					break
				}
				return false
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func normalizeGroup(g *gatewayv1.Group) string {
	val := ValueOf(g)
	if val == "" || val == gatewayv1.GroupName {
		return gatewayv1.GroupName
	}
	return string(val)
}

func normalizeKind(k *gatewayv1.Kind) string {
	val := ValueOf(k)
	if val == "" || val == "Gateway" {
		return "Gateway"
	}
	return string(val)
}

func reflectParentReferenceEqual(a, b gatewayv1.ParentReference) bool {
	if a.Name != b.Name {
		return false
	}
	if normalizeGroup(a.Group) != normalizeGroup(b.Group) {
		return false
	}
	if normalizeKind(a.Kind) != normalizeKind(b.Kind) {
		return false
	}
	if ValueOf(a.Namespace) != ValueOf(b.Namespace) {
		return false
	}
	if ValueOf(a.SectionName) != ValueOf(b.SectionName) {
		return false
	}
	if ValueOf(a.Port) != ValueOf(b.Port) {
		return false
	}
	return true
}

// UpdateRouteParentStatuses updates an existing slice of RouteParentStatus with desired statuses,
// preserving LastTransitionTime for conditions whose Status has not changed.
// Entries in existing belonging to other controllers (or controllers not in controllerNames if specified)
// are preserved. Entries for the managed controller(s) that are no longer in desired are removed.
// It returns the updated slice and a boolean indicating whether any changes occurred.
func UpdateRouteParentStatuses(
	existing []gatewayv1.RouteParentStatus,
	desired []gatewayv1.RouteParentStatus,
	controllerNames ...gatewayv1.GatewayController,
) ([]gatewayv1.RouteParentStatus, bool) {
	managedControllers := make(map[gatewayv1.GatewayController]bool)
	for _, c := range controllerNames {
		managedControllers[c] = true
	}
	if len(managedControllers) == 0 {
		for _, d := range desired {
			managedControllers[d.ControllerName] = true
		}
	}

	updated := false

	var result []gatewayv1.RouteParentStatus

	for _, d := range desired {
		entry := gatewayv1.RouteParentStatus{
			ParentRef:      d.ParentRef,
			ControllerName: d.ControllerName,
			Conditions:     make([]metav1.Condition, len(d.Conditions)),
		}
		copy(entry.Conditions, d.Conditions)

		// Find matching existing parent status
		var matchingExisting *gatewayv1.RouteParentStatus
		for j := range existing {
			if existing[j].ControllerName == d.ControllerName && reflectParentReferenceEqual(existing[j].ParentRef, d.ParentRef) {
				matchingExisting = &existing[j]
				break
			}
		}

		if matchingExisting == nil {
			updated = true
			for k := range entry.Conditions {
				if entry.Conditions[k].LastTransitionTime.IsZero() {
					entry.Conditions[k].LastTransitionTime = metav1.Now()
				}
			}
		} else {
			if len(matchingExisting.Conditions) != len(d.Conditions) {
				updated = true
			}
			for k, dc := range d.Conditions {
				found := false
				for _, ec := range matchingExisting.Conditions {
					if ec.Type == dc.Type {
						found = true
						if ec.Status == dc.Status {
							entry.Conditions[k].LastTransitionTime = ec.LastTransitionTime
						} else {
							updated = true
							if entry.Conditions[k].LastTransitionTime.IsZero() {
								entry.Conditions[k].LastTransitionTime = metav1.Now()
							}
						}
						if ec.Status != dc.Status || ec.Reason != dc.Reason || ec.Message != dc.Message || ec.ObservedGeneration != dc.ObservedGeneration {
							updated = true
						}
						break
					}
				}
				if !found {
					updated = true
					if entry.Conditions[k].LastTransitionTime.IsZero() {
						entry.Conditions[k].LastTransitionTime = metav1.Now()
					}
				}
			}
		}

		result = append(result, entry)
	}

	// Check for existing entries: preserve entries from unmanaged controllers,
	// and detect removals of entries from managed controllers.
	for _, e := range existing {
		if managedControllers[e.ControllerName] {
			found := false
			for _, d := range desired {
				if d.ControllerName == e.ControllerName && reflectParentReferenceEqual(d.ParentRef, e.ParentRef) {
					found = true
					break
				}
			}
			if !found {
				updated = true
			}
		} else {
			result = append(result, e)
		}
	}

	if len(existing) != len(result) {
		updated = true
	}

	return result, updated
}
