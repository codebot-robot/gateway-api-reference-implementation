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
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

type GatewayClassReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	State          *state.State
	ControllerName string
}

func (r *GatewayClassReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var gc gatewayv1.GatewayClass
	if err := r.Get(ctx, req.NamespacedName, &gc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if string(gc.Spec.ControllerName) != r.ControllerName {
		return ctrl.Result{}, nil
	}

	var desired gatewayv1.GatewayClassStatus
	if r.State != nil {
		d, ok := r.State.GetDesiredGatewayClassStatus(req.NamespacedName)
		if !ok {
			return ctrl.Result{}, nil
		}
		desired = d
	} else {
		desired = state.ComputeDesiredGatewayClassStatus(&gc, r.ControllerName)
	}

	if state.MergeGatewayClassStatus(&gc.Status, desired) {
		if err := r.Status().Update(ctx, &gc); err != nil {
			l.Error(err, "unable to update GatewayClass status")
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *GatewayClassReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ControllerName == "" {
		return fmt.Errorf("ControllerName is required")
	}
	bldr := ctrl.NewControllerManagedBy(mgr).
		For(&gatewayv1.GatewayClass{})
	if r.State != nil {
		bldr.WatchesRawSource(r.State.GatewayClassSource())
	}
	return bldr.Complete(r)
}

// AddressProvider returns the addresses to report in Gateway.status.addresses.
// An empty result means the address is not assigned yet.
type AddressProvider interface {
	GatewayAddresses(ctx context.Context, gw *gatewayv1.Gateway) ([]gatewayv1.GatewayStatusAddress, error)
}

// AddressWatcher is an optional interface that an AddressProvider can implement
// to register custom watches with the Gateway controller.
type AddressWatcher interface {
	SetupWatches(mgr ctrl.Manager, bldr *builder.Builder) error
}

type GatewayReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	State           *state.State
	ControllerName  string
	AddressProvider AddressProvider
}

func (r *GatewayReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	gw := &gatewayv1.Gateway{}
	if err := r.Get(ctx, req.NamespacedName, gw); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if r.State != nil {
		// Verify this Gateway is managed by us in State before requesting addresses
		gc, ok := r.State.GetGatewayClass(string(gw.Spec.GatewayClassName))
		if !ok || string(gc.Spec.ControllerName) != r.ControllerName {
			return ctrl.Result{}, nil
		}

		if r.AddressProvider != nil {
			providedAddresses, err := r.AddressProvider.GatewayAddresses(ctx, gw)
			if err != nil {
				l.Error(err, "unable to fetch gateway addresses from address provider")
				return ctrl.Result{}, err
			}
			r.State.SetGatewayAddresses(req.NamespacedName, providedAddresses)
		}

		desired, ok := r.State.GetDesiredGatewayStatus(req.NamespacedName)
		if !ok {
			return ctrl.Result{}, nil
		}

		if state.MergeGatewayStatus(&gw.Status, desired) {
			if err := r.Status().Update(ctx, gw); err != nil {
				l.Error(err, "unable to update Gateway status")
				return ctrl.Result{}, err
			}
			l.Info("Updated Gateway status", "addresses", gw.Status.Addresses)
		}
	}

	return ctrl.Result{}, nil
}

func (r *GatewayReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ControllerName == "" {
		return fmt.Errorf("ControllerName is required")
	}
	bldr := ctrl.NewControllerManagedBy(mgr).
		For(&gatewayv1.Gateway{})

	if r.State != nil {
		bldr.WatchesRawSource(r.State.GatewaySource())
	}

	if watcher, ok := r.AddressProvider.(AddressWatcher); ok {
		if err := watcher.SetupWatches(mgr, bldr); err != nil {
			return fmt.Errorf("error setting up address provider watches: %w", err)
		}
	}

	return bldr.Complete(r)
}
