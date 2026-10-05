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

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/source"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

type ListenerSetReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	State          *state.State
	ControllerName string
}

func (r *ListenerSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	ls := &gatewayv1.ListenerSet{}
	if err := r.Get(ctx, req.NamespacedName, ls); err != nil {
		if apierrors.IsNotFound(err) {
			if r.State != nil {
				r.State.DeleteListenerSet(req.NamespacedName)
			}
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if r.State != nil {
		gwKey := ResolveNamespacedName(ls.Spec.ParentRef.Namespace, ls.Spec.ParentRef.Name, ls)
		var fetchedGW gatewayv1.Gateway
		if err := r.Get(ctx, gwKey, &fetchedGW); err == nil {
			if _, ok := r.State.GetGatewayClass(string(fetchedGW.Spec.GatewayClassName)); !ok {
				var gc gatewayv1.GatewayClass
				if err := r.Get(ctx, types.NamespacedName{Name: string(fetchedGW.Spec.GatewayClassName)}, &gc); err == nil {
					r.State.UpsertGatewayClass(&gc)
				}
			}
			r.State.UpsertGateway(&fetchedGW)
		}

		r.State.UpsertListenerSet(ls)

		desired, ok := r.State.GetDesiredListenerSetStatus(req.NamespacedName)
		if !ok {
			compiled := r.State.CompileModel(r.ControllerName)
			var parentGW *gatewayv1.Gateway
			for _, g := range r.State.GetGateways() {
				if g != nil && g.Gateway != nil && g.Gateway.Namespace == gwKey.Namespace && g.Gateway.Name == gwKey.Name {
					parentGW = g.Gateway
					break
				}
			}
			desired = state.ComputeDesiredListenerSetStatus(ls, parentGW, r.State.GetNamespaces(), compiled.Gateways[gwKey])
		}
		if state.MergeListenerSetStatus(&ls.Status, desired) {
			if err := r.Status().Update(ctx, ls); err != nil {
				l.Error(err, "unable to update ListenerSet status")
				return ctrl.Result{}, err
			}
		}
	}

	return ctrl.Result{}, nil
}

func (r *ListenerSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ControllerName == "" {
		return fmt.Errorf("ControllerName is required")
	}

	bldr := ctrl.NewControllerManagedBy(mgr).
		For(&gatewayv1.ListenerSet{})

	if r.State != nil {
		bldr.WatchesRawSource(source.Channel(r.State.ListenerSetEvents(), &handler.EnqueueRequestForObject{}))
	}

	return bldr.Complete(r)
}
