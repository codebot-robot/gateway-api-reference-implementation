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
	Scheme           *runtime.Scheme
	State            *state.State
	Proxy            *proxy.Proxy
	ControllerName   string
	OnGatewaysUpdate func([]*gatewayv1.Gateway)
}

func (r *HTTPRouteReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	controllerName := r.ControllerName

	route := &gatewayv1.HTTPRoute{}
	if err := r.Get(ctx, req.NamespacedName, route); err != nil {
		if apierrors.IsNotFound(err) {
			r.State.DeleteHTTPRoute(req.NamespacedName)
			r.updateProxy()
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	r.State.UpsertHTTPRoute(route)

	// TODO(incremental-state): Centralize model recomputation and diffing to avoid recompiling in both reconciler and updateProxy.
	compiled := r.State.CompileModel(controllerName)
	compiledRoute := compiled.HTTPRoutes[req.NamespacedName]

	newStatus, updated := state.ComputeDesiredHTTPRouteStatus(route, compiledRoute, controllerName)
	if updated {
		route.Status = newStatus
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
	updateProxy(r.State, r.Proxy, r.ControllerName, r.OnGatewaysUpdate)
}

func (r *HTTPRouteReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ControllerName == "" {
		return fmt.Errorf("ControllerName is required")
	}
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
					gwKey := ResolveNamespacedName(parentRef.Namespace, parentRef.Name, &route)
					if gwKey.Name == gw.Name && gwKey.Namespace == gw.Namespace {
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
						svcKey := ResolveNamespacedName(bRef.Namespace, bRef.Name, &route)
						if svcKey.Name == svc.Name && svcKey.Namespace == svc.Namespace {
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
		// A Namespace update invalidates all the HTTPRoutes in that namespace
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
			ns := obj.(*corev1.Namespace)
			var routeList gatewayv1.HTTPRouteList
			if err := r.List(ctx, &routeList, client.InNamespace(ns.Name)); err != nil {
				return nil
			}
			var requests []ctrl.Request
			for _, route := range routeList.Items {
				requests = append(requests, ctrl.Request{
					NamespacedName: types.NamespacedName{
						Namespace: route.Namespace,
						Name:      route.Name,
					},
				})
			}
			return requests
		})).
		// A ListenerSet update invalidates all HTTPRoutes that reference it
		Watches(&gatewayv1.ListenerSet{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
			ls := obj.(*gatewayv1.ListenerSet)
			var routeList gatewayv1.HTTPRouteList
			if err := r.List(ctx, &routeList); err != nil {
				return nil
			}
			var requests []ctrl.Request
			for _, route := range routeList.Items {
				for _, parentRef := range route.Spec.ParentRefs {
					if string(state.ValueOf(parentRef.Kind)) == "ListenerSet" {
						lsKey := ResolveNamespacedName(parentRef.Namespace, parentRef.Name, &route)
						if lsKey.Name == ls.Name && lsKey.Namespace == ls.Namespace {
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
			}
			return requests
		})).
		Complete(r)
}
