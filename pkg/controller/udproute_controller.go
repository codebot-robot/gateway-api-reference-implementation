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
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

type UDPRouteReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	State          *state.State
	ControllerName string
}

func (r *UDPRouteReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	route := &gatewayv1.UDPRoute{}
	if err := r.Get(ctx, req.NamespacedName, route); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if r.State != nil {
		desired, ok := r.State.GetDesiredUDPRouteStatus(req.NamespacedName)
		if !ok {
			return ctrl.Result{}, nil
		}

		if state.MergeUDPRouteStatus(&route.Status, desired, route.Namespace, gatewayv1.GatewayController(r.ControllerName)) {
			if err := r.Status().Update(ctx, route); err != nil {
				l.Error(err, "unable to update UDPRoute status")
				return ctrl.Result{}, err
			}
		}
	}

	l.V(1).Info("Reconciled UDPRoute")

	return ctrl.Result{}, nil
}

func (r *UDPRouteReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ControllerName == "" {
		return fmt.Errorf("ControllerName is required")
	}
	bldr := ctrl.NewControllerManagedBy(mgr).
		For(&gatewayv1.UDPRoute{})

	if r.State != nil {
		bldr.WatchesRawSource(r.State.UDPRouteSource())
	}

	return bldr.Complete(r)
}
