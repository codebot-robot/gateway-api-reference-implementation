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
	"fmt"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/proxy"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// ResolveNamespacedName resolves an optional namespace pointer and a name against a source object.
// If namespace is nil or empty, src.GetNamespace() is used.
func ResolveNamespacedName[N ~string, M ~string](namespace *N, name M, src client.Object) types.NamespacedName {
	ns := ""
	if src != nil {
		ns = src.GetNamespace()
	}
	if namespace != nil && string(*namespace) != "" {
		ns = string(*namespace)
	}
	return types.NamespacedName{
		Namespace: ns,
		Name:      string(name),
	}
}

// ReconcilerOptions contains configuration for setting up GARI reconcilers.
type ReconcilerOptions struct {
	ControllerName   string
	OnGatewaysUpdate func([]*gatewayv1.Gateway)
	AddressProvider  AddressProvider
	GatewayFilter    func(gw *gatewayv1.Gateway) bool
}

// RegisterReconcilers registers all GARI reconcilers with the given Manager.
func RegisterReconcilers(mgr ctrl.Manager, st *state.State, p *proxy.Proxy, opts ReconcilerOptions) error {
	controllerName := opts.ControllerName
	if controllerName == "" {
		controllerName = DefaultControllerName
	}
	if err := (&HTTPRouteReconciler{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		State:            st,
		Proxy:            p,
		ControllerName:   controllerName,
		OnGatewaysUpdate: opts.OnGatewaysUpdate,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("error creating HTTPRoute controller: %w", err)
	}

	if err := (&GatewayClassReconciler{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		ControllerName: controllerName,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("error creating GatewayClass controller: %w", err)
	}

	if err := (&GatewayReconciler{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		State:            st,
		Proxy:            p,
		ControllerName:   controllerName,
		OnGatewaysUpdate: opts.OnGatewaysUpdate,
		AddressProvider:  opts.AddressProvider,
		GatewayFilter:    opts.GatewayFilter,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("error creating Gateway controller: %w", err)
	}

	if err := (&ServiceReconciler{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		State:            st,
		Proxy:            p,
		ControllerName:   controllerName,
		OnGatewaysUpdate: opts.OnGatewaysUpdate,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("error creating Service controller: %w", err)
	}

	if err := (&BackendTLSPolicyReconciler{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		State:            st,
		Proxy:            p,
		ControllerName:   controllerName,
		OnGatewaysUpdate: opts.OnGatewaysUpdate,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("error creating BackendTLSPolicy controller: %w", err)
	}

	if err := (&ConfigMapReconciler{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		State:            st,
		Proxy:            p,
		ControllerName:   controllerName,
		OnGatewaysUpdate: opts.OnGatewaysUpdate,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("error creating ConfigMap controller: %w", err)
	}

	if err := (&SecretReconciler{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		State:            st,
		Proxy:            p,
		ControllerName:   controllerName,
		OnGatewaysUpdate: opts.OnGatewaysUpdate,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("error creating Secret controller: %w", err)
	}

	if err := (&ReferenceGrantReconciler{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		State:            st,
		Proxy:            p,
		ControllerName:   controllerName,
		OnGatewaysUpdate: opts.OnGatewaysUpdate,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("error creating ReferenceGrant controller: %w", err)
	}

	if err := (&NamespaceReconciler{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		State:            st,
		Proxy:            p,
		ControllerName:   controllerName,
		OnGatewaysUpdate: opts.OnGatewaysUpdate,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("error creating Namespace controller: %w", err)
	}

	if err := (&ListenerSetReconciler{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		State:            st,
		Proxy:            p,
		ControllerName:   controllerName,
		OnGatewaysUpdate: opts.OnGatewaysUpdate,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("error creating ListenerSet controller: %w", err)
	}

	return nil
}

func updateProxy(st *state.State, p *proxy.Proxy, controllerName string, onGatewaysUpdate func([]*gatewayv1.Gateway)) {
	if controllerName == "" {
		controllerName = DefaultControllerName
	}
	compiled := st.CompileModel(controllerName)
	if p != nil {
		proxyListeners, proxyRoutes := state.BuildProxyConfig(compiled.GatewaysList())
		p.UpdateConfig(proxyListeners, proxyRoutes)

		certsMap, defaultCert := state.ExtractCertificates(compiled.GatewaysList(), st.GetSecrets(), st)
		p.UpdateCertificates(certsMap, defaultCert)
	}

	if onGatewaysUpdate != nil {
		onGatewaysUpdate(compiled.ResolvedGateways())
	}
}
