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
	"reflect"

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
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

type HTTPRouteReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	State  *state.State
	Proxy  *proxy.Proxy
}

func (r *HTTPRouteReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	route := &gatewayv1.HTTPRoute{}
	if err := r.Get(ctx, req.NamespacedName, route); err != nil {
		if apierrors.IsNotFound(err) {
			r.State.DeleteHTTPRoute(req.NamespacedName)
			r.updateProxy()
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// If the route is not accepted, we still update the state but it won't be used for proxying
	validationCondition := r.State.UpsertHTTPRoute(route)

	// Update status
	// For each parentRef, we should add a ParentStatus
	gateways := r.State.GetGateways()
	services := r.State.GetServices()
	referenceGrants := r.State.GetReferenceGrants()
	rs := state.HTTPRouteState{HTTPRoute: route}

	var newParents []gatewayv1.RouteParentStatus
	for _, parentRef := range route.Spec.ParentRefs {
		acceptedCondition := validationCondition
		if acceptedCondition.Status == metav1.ConditionTrue {
			acceptedCondition = rs.ComputeAcceptedCondition(parentRef, gateways)
		}

		newParents = append(newParents, gatewayv1.RouteParentStatus{
			ParentRef:      parentRef,
			ControllerName: ControllerName,
			Conditions: []metav1.Condition{
				acceptedCondition,
				rs.ComputeResolvedRefsCondition(services, referenceGrants),
			},
		})
	}

	updated := false
	if len(route.Status.Parents) != len(newParents) {
		updated = true
	} else {
		for i := range newParents {
			if !reflect.DeepEqual(route.Status.Parents[i].ParentRef, newParents[i].ParentRef) ||
				string(route.Status.Parents[i].ControllerName) != string(newParents[i].ControllerName) ||
				len(route.Status.Parents[i].Conditions) != len(newParents[i].Conditions) {
				updated = true
				break
			}
			for j := range newParents[i].Conditions {
				matched := false
				for k := range route.Status.Parents[i].Conditions {
					if route.Status.Parents[i].Conditions[k].Type == newParents[i].Conditions[j].Type {
						if route.Status.Parents[i].Conditions[k].Status == newParents[i].Conditions[j].Status &&
							route.Status.Parents[i].Conditions[k].ObservedGeneration == newParents[i].Conditions[j].ObservedGeneration &&
							route.Status.Parents[i].Conditions[k].Reason == newParents[i].Conditions[j].Reason &&
							route.Status.Parents[i].Conditions[k].Message == newParents[i].Conditions[j].Message {
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

	if updated {
		route.Status.Parents = newParents
		if err := r.Status().Update(ctx, route); err != nil {
			l.Error(err, "unable to update HTTPRoute status")
			return ctrl.Result{}, err
		}
	}

	r.State.UpsertHTTPRoute(route)
	r.updateProxy()

	l.Info("Updated HTTPRoute status and proxy")

	return ctrl.Result{}, nil
}

func (r *HTTPRouteReconciler) updateProxy() {
	updateProxy(r.State, r.Proxy)
}

func (r *HTTPRouteReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&gatewayv1.HTTPRoute{}).
		// A Gateway update invalidates all the HTTPRoutes that reference it
		Watches(&gatewayv1.Gateway{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
			gw := obj.(*gatewayv1.Gateway)
			var routeList gatewayv1.HTTPRouteList
			if err := r.List(ctx, &routeList); err != nil {
				return nil
			}
			var requests []ctrl.Request
			for _, route := range routeList.Items {
				for _, parentRef := range route.Spec.ParentRefs {
					targetNamespace := route.Namespace
					if parentNamespace := state.ValueOf(parentRef.Namespace); parentNamespace != "" {
						targetNamespace = string(parentNamespace)
					}
					if string(parentRef.Name) == gw.Name && targetNamespace == gw.Namespace {
						requests = append(requests, ctrl.Request{
							NamespacedName: types.NamespacedName{
								Namespace: route.Namespace,
								Name:      route.Name,
							},
						})
						break
					}
				}
			}
			return requests
		})).
		// A ReferenceGrant update invalidates all the HTTPRoutes in referenced namespaces
		Watches(&gatewayv1beta1.ReferenceGrant{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
			rg := obj.(*gatewayv1beta1.ReferenceGrant)
			var routeList gatewayv1.HTTPRouteList
			if err := r.List(ctx, &routeList); err != nil {
				return nil
			}
			var requests []ctrl.Request
			for _, route := range routeList.Items {
				for _, from := range rg.Spec.From {
					if (string(from.Group) == gatewayv1.GroupName || string(from.Group) == "") && string(from.Kind) == "HTTPRoute" && string(from.Namespace) == route.Namespace {
						requests = append(requests, ctrl.Request{
							NamespacedName: types.NamespacedName{
								Namespace: route.Namespace,
								Name:      route.Name,
							},
						})
						break
					}
				}
			}
			return requests
		})).
		// A Service update invalidates all the HTTPRoutes that reference it
		Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
			svc := obj.(*corev1.Service)
			var routeList gatewayv1.HTTPRouteList
			if err := r.List(ctx, &routeList); err != nil {
				return nil
			}
			var requests []ctrl.Request
			for _, route := range routeList.Items {
				matched := false
				for _, rule := range route.Spec.Rules {
					for _, bRef := range rule.BackendRefs {
						bNs := route.Namespace
						if bRef.Namespace != nil && string(*bRef.Namespace) != "" {
							bNs = string(*bRef.Namespace)
						}
						if string(bRef.Name) == svc.Name && bNs == svc.Namespace {
							requests = append(requests, ctrl.Request{
								NamespacedName: types.NamespacedName{
									Namespace: route.Namespace,
									Name:      route.Name,
								},
							})
							matched = true
							break
						}
					}
					if matched {
						break
					}
				}
			}
			return requests
		})).
		Complete(r)
}
