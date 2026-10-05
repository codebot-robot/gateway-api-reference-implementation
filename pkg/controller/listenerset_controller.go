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

	if gw != nil {
		r.State.UpsertGateway(gw)
	}
	r.State.UpsertListenerSet(ls)

	// TODO(incremental-state): Centralize model recomputation and diffing to avoid recompiling in both reconciler and updateProxy.
	compiled := r.State.CompileModel(r.ControllerName)
	compiledGw := compiled.Gateways[gwKey]

	namespaces := r.State.GetNamespaces()
	newStatus, updated := state.ComputeDesiredListenerSetStatus(ls, gw, namespaces, compiledGw)

	if updated {
		ls.Status = newStatus
		if err := r.Status().Update(ctx, ls); err != nil {
			l.Error(err, "unable to update ListenerSet status")
			return ctrl.Result{}, err
		}
	}

	r.State.UpsertListenerSet(ls)
	r.updateProxy()

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
		Watches(&gatewayv1.ListenerSet{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
			changedLS := obj.(*gatewayv1.ListenerSet)
			var lsList gatewayv1.ListenerSetList
			if err := r.List(ctx, &lsList); err != nil {
				return nil
			}
			var requests []ctrl.Request
			changedGwKey := ResolveNamespacedName(changedLS.Spec.ParentRef.Namespace, changedLS.Spec.ParentRef.Name, changedLS)
			for _, ls := range lsList.Items {
				if ls.Namespace == changedLS.Namespace && ls.Name == changedLS.Name {
					continue
				}
				gwKey := ResolveNamespacedName(ls.Spec.ParentRef.Namespace, ls.Spec.ParentRef.Name, &ls)
				if gwKey == changedGwKey {
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
		Watches(&gatewayv1beta1.ReferenceGrant{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
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
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
			secret := obj.(*corev1.Secret)
			var lsList gatewayv1.ListenerSetList
			if err := r.List(ctx, &lsList); err != nil {
				return nil
			}
			var requests []ctrl.Request
			for _, ls := range lsList.Items {
				matched := false
				for _, l := range ls.Spec.Listeners {
					if l.TLS != nil {
						for _, ref := range l.TLS.CertificateRefs {
							secKey := ResolveNamespacedName(ref.Namespace, ref.Name, &ls)
							if secKey.Namespace == secret.Namespace && secKey.Name == secret.Name {
								matched = true
								break
							}
						}
					}
					if matched {
						break
					}
				}
				if matched {
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
		Complete(r)
}
