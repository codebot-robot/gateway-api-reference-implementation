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

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/proxy"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

type ListenerSetReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	State            *state.State
	Proxy            *proxy.Proxy
	ControllerName   string
	OnGatewaysUpdate func([]*gatewayv1.Gateway)
}

func (r *ListenerSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	ls := &gatewayv1.ListenerSet{}
	if err := r.Get(ctx, req.NamespacedName, ls); err != nil {
		if apierrors.IsNotFound(err) {
			r.State.DeleteListenerSet(req.NamespacedName)
			r.updateProxy()
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	gwKey := ResolveNamespacedName(ls.Spec.ParentRef.Namespace, ls.Spec.ParentRef.Name, ls)
	var gw *gatewayv1.Gateway
	var fetchedGW gatewayv1.Gateway
	if err := r.Get(ctx, gwKey, &fetchedGW); err == nil {
		gc := &gatewayv1.GatewayClass{}
		if err := r.Get(ctx, client.ObjectKey{Name: string(fetchedGW.Spec.GatewayClassName)}, gc); err == nil {
			if string(gc.Spec.ControllerName) == r.ControllerName {
				gw = &fetchedGW
			}
		}
	}

	namespaces := r.State.GetNamespaces()
	acceptedCond, programmedCond := state.ComputeListenerSetAcceptedCondition(ls, gw, namespaces)
	newConditions := []metav1.Condition{programmedCond, acceptedCond}

	for i, newCond := range newConditions {
		newConditions[i].LastTransitionTime = metav1.Now()
		for _, oldCond := range ls.Status.Conditions {
			if oldCond.Type == newCond.Type && oldCond.Status == newCond.Status {
				newConditions[i].LastTransitionTime = oldCond.LastTransitionTime
				break
			}
		}
	}

	var newListenerStatuses []gatewayv1.ListenerEntryStatus
	if acceptedCond.Status == metav1.ConditionTrue {
		routes := r.State.GetHTTPRoutes()
		for _, listener := range ls.Spec.Listeners {
			attachedRoutes := 0
			for _, route := range routes {
				for _, parentRef := range route.Spec.ParentRefs {
					pGroup := state.ValueOf(parentRef.Group)
					pKind := state.ValueOf(parentRef.Kind)
					if (pGroup == "" || pGroup == gatewayv1.GroupName) && pKind == "ListenerSet" {
						pNs := route.Namespace
						if ns := state.ValueOf(parentRef.Namespace); ns != "" {
							pNs = string(ns)
						}
						if string(parentRef.Name) == ls.Name && pNs == ls.Namespace {
							if sn := state.ValueOf(parentRef.SectionName); sn == "" || string(sn) == string(listener.Name) {
								if port := state.ValueOf(parentRef.Port); port == 0 || port == listener.Port {
									if route.IsAcceptedForParentRef(parentRef, r.ControllerName) {
										routeHostnames := route.GetHostnames()
										listenerHostname := state.ValueOf(listener.Hostname)
										effectiveHostnames := state.IntersectHostnames(routeHostnames, string(listenerHostname))
										if len(effectiveHostnames) > 0 || len(routeHostnames) == 0 {
											attachedRoutes++
											break
										}
									}
								}
							}
						}
					}
				}
			}

			// TODO: In Part 2, compute dynamic conditions for each listener (e.g. conflicts, invalid secret/ReferenceGrant refs, protocol support, etc.) instead of hard-coding them to True.
			conds := []metav1.Condition{
				{
					Type:               string(gatewayv1.ListenerConditionResolvedRefs),
					Status:             metav1.ConditionTrue,
					ObservedGeneration: ls.Generation,
					Reason:             string(gatewayv1.ListenerReasonResolvedRefs),
					Message:            "All references resolved",
				},
				{
					Type:               string(gatewayv1.ListenerConditionAccepted),
					Status:             metav1.ConditionTrue,
					ObservedGeneration: ls.Generation,
					Reason:             string(gatewayv1.ListenerReasonAccepted),
					Message:            "Listener accepted",
				},
				{
					Type:               string(gatewayv1.ListenerConditionProgrammed),
					Status:             metav1.ConditionTrue,
					ObservedGeneration: ls.Generation,
					Reason:             string(gatewayv1.ListenerReasonProgrammed),
					Message:            "Listener programmed",
				},
			}

			var oldListener *gatewayv1.ListenerEntryStatus
			for _, ol := range ls.Status.Listeners {
				if ol.Name == listener.Name {
					oldListener = &ol
					break
				}
			}

			for i, newCond := range conds {
				conds[i].LastTransitionTime = metav1.Now()
				if oldListener != nil {
					for _, oldCond := range oldListener.Conditions {
						if oldCond.Type == newCond.Type && oldCond.Status == newCond.Status {
							conds[i].LastTransitionTime = oldCond.LastTransitionTime
							break
						}
					}
				}
			}

			newListenerStatuses = append(newListenerStatuses, gatewayv1.ListenerEntryStatus{
				Name:           listener.Name,
				SupportedKinds: []gatewayv1.RouteGroupKind{{Group: state.Ptr(gatewayv1.Group("gateway.networking.k8s.io")), Kind: "HTTPRoute"}},
				AttachedRoutes: int32(attachedRoutes),
				Conditions:     conds,
			})
		}
	}

	updated := false
	if len(ls.Status.Conditions) != len(newConditions) || len(ls.Status.Listeners) != len(newListenerStatuses) {
		updated = true
	} else {
		for i := range newConditions {
			matched := false
			for j := range ls.Status.Conditions {
				if ls.Status.Conditions[j].Type == newConditions[i].Type {
					if ls.Status.Conditions[j].Status == newConditions[i].Status &&
						ls.Status.Conditions[j].ObservedGeneration == newConditions[i].ObservedGeneration &&
						ls.Status.Conditions[j].Reason == newConditions[i].Reason &&
						ls.Status.Conditions[j].Message == newConditions[i].Message {
						matched = true
					}
					break
				}
			}
			if !matched {
				updated = true
				break
			}
		}
		if !updated {
			for i := range newListenerStatuses {
				if ls.Status.Listeners[i].Name != newListenerStatuses[i].Name ||
					ls.Status.Listeners[i].AttachedRoutes != newListenerStatuses[i].AttachedRoutes ||
					len(ls.Status.Listeners[i].Conditions) != len(newListenerStatuses[i].Conditions) {
					updated = true
					break
				}
				for j := range newListenerStatuses[i].Conditions {
					matched := false
					for k := range ls.Status.Listeners[i].Conditions {
						if ls.Status.Listeners[i].Conditions[k].Type == newListenerStatuses[i].Conditions[j].Type {
							if ls.Status.Listeners[i].Conditions[k].Status == newListenerStatuses[i].Conditions[j].Status &&
								ls.Status.Listeners[i].Conditions[k].ObservedGeneration == newListenerStatuses[i].Conditions[j].ObservedGeneration &&
								ls.Status.Listeners[i].Conditions[k].Reason == newListenerStatuses[i].Conditions[j].Reason {
								matched = true
							}
							break
						}
					}
					if !matched {
						updated = true
						break
					}
				}
				if updated {
					break
				}
			}
		}
	}

	r.State.UpsertListenerSet(ls)
	r.updateProxy()

	if updated {
		ls.Status.Conditions = newConditions
		ls.Status.Listeners = newListenerStatuses
		if err := r.Status().Update(ctx, ls); err != nil {
			l.Error(err, "unable to update ListenerSet status")
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *ListenerSetReconciler) updateProxy() {
	updateProxy(r.State, r.Proxy, r.ControllerName, r.OnGatewaysUpdate)
}

func (r *ListenerSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ControllerName == "" {
		return fmt.Errorf("ControllerName is required")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&gatewayv1.ListenerSet{}).
		Watches(&gatewayv1.Gateway{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
			gw := obj.(*gatewayv1.Gateway)
			var lsList gatewayv1.ListenerSetList
			if err := r.List(ctx, &lsList); err != nil {
				return nil
			}
			var requests []ctrl.Request
			for _, ls := range lsList.Items {
				gwKey := ResolveNamespacedName(ls.Spec.ParentRef.Namespace, ls.Spec.ParentRef.Name, &ls)
				if gwKey.Name == gw.Name && gwKey.Namespace == gw.Namespace {
					requests = append(requests, ctrl.Request{
						NamespacedName: types.NamespacedName{
							Namespace: ls.Namespace,
							Name:      ls.Name,
						},
					})
				}
			}
			return requests
		})).
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
			var lsList gatewayv1.ListenerSetList
			if err := r.List(ctx, &lsList); err != nil {
				return nil
			}
			var requests []ctrl.Request
			for _, ls := range lsList.Items {
				requests = append(requests, ctrl.Request{
					NamespacedName: types.NamespacedName{
						Namespace: ls.Namespace,
						Name:      ls.Name,
					},
				})
			}
			return requests
		})).
		Watches(&gatewayv1.HTTPRoute{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
			route := obj.(*gatewayv1.HTTPRoute)
			var requests []ctrl.Request
			for _, parentRef := range route.Spec.ParentRefs {
				pKind := state.ValueOf(parentRef.Kind)
				pGroup := state.ValueOf(parentRef.Group)
				if (pGroup == "" || pGroup == gatewayv1.GroupName) && pKind == "ListenerSet" {
					lsKey := ResolveNamespacedName(parentRef.Namespace, parentRef.Name, route)
					requests = append(requests, ctrl.Request{
						NamespacedName: lsKey,
					})
				}
			}
			return requests
		})).
		Complete(r)
}
