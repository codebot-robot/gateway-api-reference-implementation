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

type BackendTLSPolicyReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	State          *state.State
	ControllerName string
}

func (r *BackendTLSPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	policy := &gatewayv1.BackendTLSPolicy{}
	if err := r.Get(ctx, req.NamespacedName, policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if r.State != nil {
		desired, ok := r.State.GetDesiredBackendTLSPolicyStatus(req.NamespacedName)
		if !ok {
			return ctrl.Result{}, nil
		}

		if state.MergeBackendTLSPolicyStatus(&policy.Status, desired, gatewayv1.GatewayController(r.ControllerName)) {
			if err := r.Status().Update(ctx, policy); err != nil {
				l.Error(err, "unable to update BackendTLSPolicy status")
				return ctrl.Result{}, err
			}
		}
	}

	return ctrl.Result{}, nil
}

func (r *BackendTLSPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ControllerName == "" {
		return fmt.Errorf("ControllerName is required")
	}
	bldr := ctrl.NewControllerManagedBy(mgr).
		For(&gatewayv1.BackendTLSPolicy{})

	if r.State != nil {
		bldr.WatchesRawSource(r.State.BackendTLSPolicySource())
	}

	return bldr.Complete(r)
}
