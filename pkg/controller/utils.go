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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/proxy"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
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

	return nil
}

func updateProxy(st *state.State, p *proxy.Proxy, controllerName string, onGatewaysUpdate func([]*gatewayv1.Gateway)) {
	if controllerName == "" {
		controllerName = DefaultControllerName
	}
	gateways := st.GetGateways()
	routes := st.GetHTTPRoutes()
	services := st.GetServices()
	backendTLSPolicies := st.GetBackendTLSPolicies()
	configMaps := st.GetConfigMaps()
	secrets := st.GetSecrets()

	var proxyRoutes []state.InternalRoute
	var proxyListeners []state.InternalListener
	for _, gw := range gateways {
		gwListeners, gwRoutes := gw.BuildInternalState(routes, services, backendTLSPolicies, configMaps, st, controllerName)
		proxyListeners = append(proxyListeners, gwListeners...)
		proxyRoutes = append(proxyRoutes, gwRoutes...)
	}
	p.UpdateConfig(proxyListeners, proxyRoutes)

	certsMap := make(map[string]*tls.Certificate)
	var defaultCert *tls.Certificate

	for _, gw := range gateways {
		for _, listener := range gw.Spec.Listeners {
			if (listener.Protocol == gatewayv1.HTTPSProtocolType || listener.Protocol == gatewayv1.TLSProtocolType) && listener.TLS != nil {
				for _, ref := range listener.TLS.CertificateRefs {
					group := state.ValueOf(ref.Group)
					kind := state.ValueOf(ref.Kind)
					if kind == "" {
						kind = "Secret"
					}
					if (group == "" || group == "core") && kind == "Secret" {
						secretKey := ResolveNamespacedName(ref.Namespace, ref.Name, gw)
						from := state.Reference{
							GroupKind: schema.GroupKind{Group: gatewayv1.GroupName, Kind: "Gateway"},
							Namespace: gw.Namespace,
						}
						to := state.Reference{
							GroupKind: schema.GroupKind{Group: string(group), Kind: string(kind)},
							Namespace: secretKey.Namespace,
							Name:      secretKey.Name,
						}
						if secretKey.Namespace != gw.Namespace && !st.IsReferencePermitted(from, to) {
							continue
						}
						secret, ok := secrets[secretKey]
						if ok && secret != nil {
							certBytes := secret.Data[corev1.TLSCertKey]
							keyBytes := secret.Data[corev1.TLSPrivateKeyKey]
							if len(certBytes) > 0 && len(keyBytes) > 0 {
								tlsCert, err := tls.X509KeyPair(certBytes, keyBytes)
								if err == nil {
									certCopy := tlsCert
									if len(certCopy.Certificate) > 0 {
										leaf, err := x509.ParseCertificate(certCopy.Certificate[0])
										if err == nil {
											certCopy.Leaf = leaf
											if leaf.Subject.CommonName != "" {
												certsMap[strings.ToLower(leaf.Subject.CommonName)] = &certCopy
											}
											for _, dnsName := range leaf.DNSNames {
												certsMap[strings.ToLower(dnsName)] = &certCopy
											}
										}
									}
									if listener.Hostname != nil && string(*listener.Hostname) != "" {
										certsMap[strings.ToLower(string(*listener.Hostname))] = &certCopy
									}
									if listener.Hostname == nil || string(*listener.Hostname) == "" || defaultCert == nil {
										defaultCert = &certCopy
									}
								}
							}
						}
					}
				}
			}
		}
	}

	p.UpdateCertificates(certsMap, defaultCert)

	if onGatewaysUpdate != nil {
		var resolvedGws []*gatewayv1.Gateway
		for _, gw := range gateways {
			if gw.Gateway != nil {
				resolvedGws = append(resolvedGws, gw.Gateway.DeepCopy())
			}
		}
		onGatewaysUpdate(resolvedGws)
	}
}
