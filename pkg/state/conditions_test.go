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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestSetCondition(t *testing.T) {
	var conditions []metav1.Condition

	t0 := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	c1 := NewCondition("Accepted", metav1.ConditionTrue, "Accepted", "Accepted message", 1)
	c1.LastTransitionTime = t0

	// Add condition
	changed := SetCondition(&conditions, c1)
	if !changed {
		t.Errorf("expected changed=true for new condition")
	}
	if len(conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(conditions))
	}

	// Re-add identical condition -> no change
	c1Same := NewCondition("Accepted", metav1.ConditionTrue, "Accepted", "Accepted message", 1)
	changed = SetCondition(&conditions, c1Same)
	if changed {
		t.Errorf("expected changed=false for identical condition")
	}
	if conditions[0].LastTransitionTime != t0 {
		t.Errorf("expected LastTransitionTime preserved, got %v", conditions[0].LastTransitionTime)
	}

	// Update condition message / reason only (same status) -> changed, but LastTransitionTime preserved
	c1Updated := NewCondition("Accepted", metav1.ConditionTrue, "Accepted", "New message", 1)
	changed = SetCondition(&conditions, c1Updated)
	if !changed {
		t.Errorf("expected changed=true when message changes")
	}
	if conditions[0].Message != "New message" {
		t.Errorf("expected message 'New message', got %s", conditions[0].Message)
	}
	if conditions[0].LastTransitionTime != t0 {
		t.Errorf("expected LastTransitionTime preserved when status same, got %v", conditions[0].LastTransitionTime)
	}

	// Update condition status (True -> False) -> changed, LastTransitionTime updated
	c1StatusChanged := NewCondition("Accepted", metav1.ConditionFalse, "Rejected", "Rejected message", 1)
	changed = SetCondition(&conditions, c1StatusChanged)
	if !changed {
		t.Errorf("expected changed=true when status changes")
	}
	if conditions[0].Status != metav1.ConditionFalse {
		t.Errorf("expected status False, got %v", conditions[0].Status)
	}
	if conditions[0].LastTransitionTime == t0 {
		t.Errorf("expected LastTransitionTime updated when status changes")
	}
}

func TestSetConditions(t *testing.T) {
	var conditions []metav1.Condition

	newConds := []metav1.Condition{
		NewCondition("Accepted", metav1.ConditionTrue, "Accepted", "Accepted", 1),
		NewCondition("ResolvedRefs", metav1.ConditionTrue, "ResolvedRefs", "ResolvedRefs", 1),
	}

	changed := SetConditions(&conditions, newConds)
	if !changed || len(conditions) != 2 {
		t.Fatalf("expected 2 conditions added, changed=%v", changed)
	}

	// Idempotent
	changed = SetConditions(&conditions, newConds)
	if changed {
		t.Errorf("expected changed=false for same conditions")
	}

	// Remove one condition
	reducedConds := []metav1.Condition{
		NewCondition("Accepted", metav1.ConditionTrue, "Accepted", "Accepted", 1),
	}
	changed = SetConditions(&conditions, reducedConds)
	if !changed || len(conditions) != 1 {
		t.Errorf("expected 1 condition remaining, changed=%v", changed)
	}
}

func TestConditionsEqual(t *testing.T) {
	c1 := []metav1.Condition{
		NewCondition("Accepted", metav1.ConditionTrue, "Accepted", "Msg", 1),
	}
	c2 := []metav1.Condition{
		NewCondition("Accepted", metav1.ConditionTrue, "Accepted", "Msg", 1),
	}
	c3 := []metav1.Condition{
		NewCondition("Accepted", metav1.ConditionFalse, "Accepted", "Msg", 1),
	}

	if !ConditionsEqual(c1, c2) {
		t.Errorf("expected c1 and c2 to be equal")
	}
	if ConditionsEqual(c1, c3) {
		t.Errorf("expected c1 and c3 NOT to be equal")
	}
}

func TestUpdateRouteParentStatuses(t *testing.T) {
	parentRef := gatewayv1.ParentReference{Name: "gw-1"}
	controller := gatewayv1.GatewayController("example.com/controller")

	t0 := metav1.NewTime(time.Now().Add(-5 * time.Minute))
	existing := []gatewayv1.RouteParentStatus{
		{
			ParentRef:      parentRef,
			ControllerName: controller,
			Conditions: []metav1.Condition{
				{
					Type:               "Accepted",
					Status:             metav1.ConditionTrue,
					Reason:             "Accepted",
					Message:            "Route accepted",
					ObservedGeneration: 1,
					LastTransitionTime: t0,
				},
			},
		},
	}

	desired := []gatewayv1.RouteParentStatus{
		{
			ParentRef:      parentRef,
			ControllerName: controller,
			Conditions: []metav1.Condition{
				NewCondition("Accepted", metav1.ConditionTrue, "Accepted", "Route accepted", 1),
				NewCondition("ResolvedRefs", metav1.ConditionTrue, "ResolvedRefs", "Resolved", 1),
			},
		},
	}

	result, updated := UpdateRouteParentStatuses(existing, desired)
	if !updated {
		t.Errorf("expected updated=true when adding ResolvedRefs condition")
	}
	if len(result[0].Conditions) != 2 {
		t.Fatalf("expected 2 conditions in result, got %d", len(result[0].Conditions))
	}
	if result[0].Conditions[0].LastTransitionTime != t0 {
		t.Errorf("expected Accepted LastTransitionTime preserved, got %v", result[0].Conditions[0].LastTransitionTime)
	}

	// Test ObservedGeneration bump
	desiredBumped := []gatewayv1.RouteParentStatus{
		{
			ParentRef:      parentRef,
			ControllerName: controller,
			Conditions: []metav1.Condition{
				NewCondition("Accepted", metav1.ConditionTrue, "Accepted", "Route accepted", 2),
				NewCondition("ResolvedRefs", metav1.ConditionTrue, "ResolvedRefs", "Resolved", 2),
			},
		},
	}
	resultBumped, updatedBumped := UpdateRouteParentStatuses(result, desiredBumped)
	if !updatedBumped {
		t.Errorf("expected updated=true when observedGeneration changes")
	}
	if resultBumped[0].Conditions[0].ObservedGeneration != 2 {
		t.Errorf("expected ObservedGeneration 2, got %d", resultBumped[0].Conditions[0].ObservedGeneration)
	}
	if resultBumped[0].Conditions[0].LastTransitionTime != t0 {
		t.Errorf("expected Accepted LastTransitionTime preserved on generation bump, got %v", resultBumped[0].Conditions[0].LastTransitionTime)
	}
}

func TestUpdateRouteParentStatuses_MultiController(t *testing.T) {
	myCtrl := gatewayv1.GatewayController("my-org/gateway-controller")
	otherCtrl := gatewayv1.GatewayController("other-org/other-controller")

	p1 := gatewayv1.ParentReference{Name: "gw-1"}
	p2 := gatewayv1.ParentReference{Name: "gw-2"}
	pOther := gatewayv1.ParentReference{Name: "gw-other"}

	t0 := metav1.NewTime(time.Now().Add(-10 * time.Minute))

	c1 := NewCondition("Accepted", metav1.ConditionTrue, "Accepted", "Accepted", 1)
	c1.LastTransitionTime = t0
	cOther := NewCondition("Accepted", metav1.ConditionTrue, "Accepted", "Accepted", 1)
	cOther.LastTransitionTime = t0

	existing := []gatewayv1.RouteParentStatus{
		{
			ParentRef:      p1,
			ControllerName: myCtrl,
			Conditions:     []metav1.Condition{c1},
		},
		{
			ParentRef:      pOther,
			ControllerName: otherCtrl,
			Conditions:     []metav1.Condition{cOther},
		},
	}

	// Case 1: myCtrl updates p1, pOther from otherCtrl must be preserved
	desired := []gatewayv1.RouteParentStatus{
		{
			ParentRef:      p1,
			ControllerName: myCtrl,
			Conditions: []metav1.Condition{
				NewCondition("Accepted", metav1.ConditionTrue, "Accepted", "Accepted", 1),
			},
		},
	}

	res, updated := UpdateRouteParentStatuses(existing, desired, myCtrl)
	if updated {
		t.Errorf("expected updated=false when nothing changed for myCtrl")
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 parent statuses (including other controller), got %d", len(res))
	}

	// Case 2: myCtrl replaces p1 with p2. p1 is removed, p2 added, pOther preserved.
	desiredP2 := []gatewayv1.RouteParentStatus{
		{
			ParentRef:      p2,
			ControllerName: myCtrl,
			Conditions: []metav1.Condition{
				NewCondition("Accepted", metav1.ConditionTrue, "Accepted", "Accepted", 1),
			},
		},
	}
	res2, updated2 := UpdateRouteParentStatuses(existing, desiredP2, myCtrl)
	if !updated2 {
		t.Errorf("expected updated=true when parentRef changed for myCtrl")
	}
	if len(res2) != 2 {
		t.Fatalf("expected 2 parent statuses, got %d", len(res2))
	}
	var foundP2, foundOther, foundP1 bool
	for _, p := range res2 {
		if p.ControllerName == myCtrl && p.ParentRef.Name == "gw-2" {
			foundP2 = true
		}
		if p.ControllerName == otherCtrl && p.ParentRef.Name == "gw-other" {
			foundOther = true
		}
		if p.ControllerName == myCtrl && p.ParentRef.Name == "gw-1" {
			foundP1 = true
		}
	}
	if !foundP2 || !foundOther || foundP1 {
		t.Errorf("expected p2 and pOther present and p1 removed, got foundP2=%v, foundOther=%v, foundP1=%v", foundP2, foundOther, foundP1)
	}

	// Case 3: myCtrl removes all its parentRefs. pOther must still be preserved.
	res3, updated3 := UpdateRouteParentStatuses(existing, nil, myCtrl)
	if !updated3 {
		t.Errorf("expected updated=true when myCtrl parentRefs removed")
	}
	if len(res3) != 1 || res3[0].ControllerName != otherCtrl {
		t.Errorf("expected only otherCtrl parent preserved, got %+v", res3)
	}
}
