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
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"

	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

type GatewayClassReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
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

	acceptedStatus := metav1.ConditionTrue
	acceptedReason := gatewayv1.GatewayClassReasonAccepted
	acceptedMessage := "GatewayClass accepted by reference implementation"

	if gc.Spec.ParametersRef != nil {
		acceptedStatus = metav1.ConditionFalse
		acceptedReason = gatewayv1.GatewayClassReasonInvalidParameters
		acceptedMessage = "Invalid parametersRef: parametersRef is not supported"
	}

	// Update status to Accepted
	newConditions := []metav1.Condition{
		{
			Type:               string(gatewayv1.GatewayClassConditionStatusAccepted),
			Status:             acceptedStatus,
			ObservedGeneration: gc.Generation,
			Reason:             string(acceptedReason),
			Message:            acceptedMessage,
		},
	}

	// Preserve LastTransitionTime if status has not changed
	for i, newCond := range newConditions {
		newConditions[i].LastTransitionTime = metav1.Now()
		for _, oldCond := range gc.Status.Conditions {
			if oldCond.Type == newCond.Type && oldCond.Status == newCond.Status {
				newConditions[i].LastTransitionTime = oldCond.LastTransitionTime
				break
			}
		}
	}

	updated := false
	if len(gc.Status.Conditions) != len(newConditions) {
		updated = true
	} else {
		for i := range newConditions {
			matched := false
			for j := range gc.Status.Conditions {
				if gc.Status.Conditions[j].Type == newConditions[i].Type {
					if gc.Status.Conditions[j].Status == newConditions[i].Status &&
						gc.Status.Conditions[j].ObservedGeneration == newConditions[i].ObservedGeneration &&
						gc.Status.Conditions[j].Reason == newConditions[i].Reason &&
						gc.Status.Conditions[j].Message == newConditions[i].Message {
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
	}

	if updated {
		for i := range newConditions {
			newConditions[i].LastTransitionTime = metav1.Now()
		}
		gc.Status.Conditions = newConditions
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
	return ctrl.NewControllerManagedBy(mgr).
		For(&gatewayv1.GatewayClass{}).
		Complete(r)
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
	Scheme           *runtime.Scheme
	State            *state.State
	Proxy            *proxy.Proxy
	ControllerName   string
	OnGatewaysUpdate func([]*gatewayv1.Gateway)
	AddressProvider  AddressProvider
	GatewayFilter    func(gw *gatewayv1.Gateway) bool
}

func (r *GatewayReconciler) inScope(gw *gatewayv1.Gateway) bool {
	if r.GatewayFilter != nil && !r.GatewayFilter(gw) {
		return false
	}
	return true
}

func (r *GatewayReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	controllerName := r.ControllerName

	gw := &gatewayv1.Gateway{}
	if err := r.Get(ctx, req.NamespacedName, gw); err != nil {
		if apierrors.IsNotFound(err) {
			r.State.DeleteGateway(req.NamespacedName)
			r.updateProxy()
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !r.inScope(gw) {
		r.State.DeleteGateway(req.NamespacedName)
		r.updateProxy()
		return ctrl.Result{}, nil
	}

	// Check if the GatewayClass is managed by us
	gc := &gatewayv1.GatewayClass{}
	if err := r.Get(ctx, client.ObjectKey{Name: string(gw.Spec.GatewayClassName)}, gc); err != nil {
		l.Error(err, "unable to fetch GatewayClass", "gatewayclass", gw.Spec.GatewayClassName)
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if string(gc.Spec.ControllerName) != controllerName {
		r.State.DeleteGateway(req.NamespacedName)
		r.updateProxy()
		return ctrl.Result{}, nil
	}

	r.State.UpsertGateway(gw)

	var providedAddresses []gatewayv1.GatewayStatusAddress
	if r.AddressProvider != nil {
		var err error
		providedAddresses, err = r.AddressProvider.GatewayAddresses(ctx, gw)
		if err != nil {
			l.Error(err, "unable to fetch gateway addresses from address provider")
			return ctrl.Result{}, err
		}
	}

	// TODO(incremental-state): Centralize model recomputation and diffing to avoid recompiling in both reconciler and updateProxy.
	compiled := r.State.CompileModel(controllerName)
	compiledGw := compiled.Gateways[req.NamespacedName]

	newStatus, updated := state.ComputeDesiredGatewayStatus(gw, compiledGw, providedAddresses)

	if updated {
		gw.Status = newStatus
		if err := r.Status().Update(ctx, gw); err != nil {
			l.Error(err, "unable to update Gateway status")
			return ctrl.Result{}, err
		}
	}

	r.State.UpsertGateway(gw)
	r.updateProxy()

	if len(gw.Status.Addresses) == 0 {
		l.V(1).Info("Gateway has no address assigned yet")
	} else {
		l.Info("Updated Gateway status", "addresses", gw.Status.Addresses)
	}

	return ctrl.Result{}, nil
}

func (r *GatewayReconciler) updateProxy() {
	updateProxy(r.State, r.Proxy, r.ControllerName, r.OnGatewaysUpdate)
}

func (r *GatewayReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ControllerName == "" {
		return fmt.Errorf("ControllerName is required")
	}
	bldr := ctrl.NewControllerManagedBy(mgr).
		For(&gatewayv1.Gateway{}).
		// A GatewayClass update invalidates all Gateways that reference it
		Watches(&gatewayv1.GatewayClass{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
			gc := obj.(*gatewayv1.GatewayClass)
			if string(gc.Spec.ControllerName) != r.ControllerName {
				return nil
			}
			var gwList gatewayv1.GatewayList
			if err := r.List(ctx, &gwList); err == nil {
				var requests []ctrl.Request
				for _, gw := range gwList.Items {
					if string(gw.Spec.GatewayClassName) == gc.Name {
						requests = append(requests, ctrl.Request{
							NamespacedName: types.NamespacedName{
								Namespace: gw.Namespace,
								Name:      gw.Name,
							},
						})
					}
				}
				return requests
			}
			return nil
		})).
		Watches(&gatewayv1.HTTPRoute{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
			// When an HTTPRoute changes, reconcile all Gateways it references
			route := obj.(*gatewayv1.HTTPRoute)
			var requests []ctrl.Request
			for _, parentRef := range route.Spec.ParentRefs {
				if string(state.ValueOf(parentRef.Group)) == "" || string(state.ValueOf(parentRef.Group)) == "gateway.networking.k8s.io" {
					if string(state.ValueOf(parentRef.Kind)) == "" || string(state.ValueOf(parentRef.Kind)) == "Gateway" {
						gwKey := ResolveNamespacedName(parentRef.Namespace, parentRef.Name, route)
						requests = append(requests, ctrl.Request{
							NamespacedName: gwKey,
						})
					}
				}
			}
			return requests
		})).
		// A ReferenceGrant update invalidates all gateways that might use it
		Watches(&gatewayv1beta1.ReferenceGrant{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
			rg := obj.(*gatewayv1beta1.ReferenceGrant)
			var gwList gatewayv1.GatewayList
			if err := r.List(ctx, &gwList); err == nil {
				var requests []ctrl.Request
				for _, gw := range gwList.Items {
					match := false
					for _, from := range rg.Spec.From {
						if (string(from.Group) == gatewayv1.GroupName || string(from.Group) == "") && string(from.Kind) == "Gateway" && string(from.Namespace) == gw.Namespace {
							match = true
							break
						}
					}
					if !match {
						for _, listener := range gw.Spec.Listeners {
							if listener.TLS != nil {
								for _, ref := range listener.TLS.CertificateRefs {
									refNs := gw.Namespace
									if ref.Namespace != nil && string(*ref.Namespace) != "" {
										refNs = string(*ref.Namespace)
									}
									if refNs == rg.Namespace {
										match = true
										break
									}
								}
							}
							if match {
								break
							}
						}
					}
					if match {
						requests = append(requests, ctrl.Request{
							NamespacedName: types.NamespacedName{
								Namespace: gw.Namespace,
								Name:      gw.Name,
							},
						})
					}
				}
				return requests
			}
			return nil
		})).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
			secret := obj.(*corev1.Secret)
			var gwList gatewayv1.GatewayList
			if err := r.List(ctx, &gwList); err == nil {
				var requests []ctrl.Request
				for _, gw := range gwList.Items {
					for _, listener := range gw.Spec.Listeners {
						if listener.TLS != nil {
							for _, ref := range listener.TLS.CertificateRefs {
								secretKey := ResolveNamespacedName(ref.Namespace, ref.Name, &gw)
								if secretKey.Namespace == secret.Namespace && secretKey.Name == secret.Name {
									requests = append(requests, ctrl.Request{
										NamespacedName: types.NamespacedName{
											Namespace: gw.Namespace,
											Name:      gw.Name,
										},
									})
									break
								}
							}
						}
					}
				}
				return requests
			}
			return nil
		})).
		Watches(&gatewayv1.ListenerSet{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
			ls := obj.(*gatewayv1.ListenerSet)
			gwKey := ResolveNamespacedName(ls.Spec.ParentRef.Namespace, ls.Spec.ParentRef.Name, ls)
			return []ctrl.Request{
				{
					NamespacedName: gwKey,
				},
			}
		})).
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
			var gwList gatewayv1.GatewayList
			if err := r.List(ctx, &gwList); err == nil {
				var requests []ctrl.Request
				for _, gw := range gwList.Items {
					if gw.Spec.AllowedListeners != nil && gw.Spec.AllowedListeners.Namespaces != nil &&
						state.ValueOf(gw.Spec.AllowedListeners.Namespaces.From) == gatewayv1.NamespacesFromSelector {
						requests = append(requests, ctrl.Request{
							NamespacedName: types.NamespacedName{
								Namespace: gw.Namespace,
								Name:      gw.Name,
							},
						})
					}
				}
				return requests
			}
			return nil
		}))

	if watcher, ok := r.AddressProvider.(AddressWatcher); ok {
		if err := watcher.SetupWatches(mgr, bldr); err != nil {
			return fmt.Errorf("error setting up address provider watches: %w", err)
		}
	}

	return bldr.Complete(r)
}
