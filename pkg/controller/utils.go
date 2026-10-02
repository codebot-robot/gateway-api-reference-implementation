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
	"strings"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/proxy"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func updateProxy(st *state.State, p *proxy.Proxy) {
	gateways := st.GetGateways()
	routes := st.GetHTTPRoutes()
	services := st.GetServices()
	backendTLSPolicies := st.GetBackendTLSPolicies()
	configMaps := st.GetConfigMaps()
	secrets := st.GetSecrets()
	referenceGrants := st.GetReferenceGrants()

	var proxyRoutes []state.InternalRoute
	for _, gw := range gateways {
		proxyRoutes = append(proxyRoutes, gw.BuildInternalRoutes(routes, services, backendTLSPolicies, configMaps, referenceGrants, ControllerName)...)
	}
	p.UpdateRoutes(proxyRoutes)

	certsMap := make(map[string]*tls.Certificate)
	var defaultCert *tls.Certificate

	for _, gw := range gateways {
		for _, listener := range gw.Spec.Listeners {
			if (listener.Protocol == gatewayv1.HTTPSProtocolType || listener.Protocol == gatewayv1.TLSProtocolType) && listener.TLS != nil {
				for _, ref := range listener.TLS.CertificateRefs {
					group := state.ValueOf(ref.Group)
					kind := state.ValueOf(ref.Kind)
					if (group == "" || group == "core") && (kind == "" || kind == "Secret") {
						ns := gw.Namespace
						if ref.Namespace != nil && string(*ref.Namespace) != "" {
							ns = string(*ref.Namespace)
						}
						if ns != gw.Namespace && !state.IsReferencePermitted(gatewayv1.GroupName, "Gateway", gw.Namespace, string(group), string(kind), ns, string(ref.Name), referenceGrants) {
							continue
						}
						secret, ok := secrets[types.NamespacedName{Namespace: ns, Name: string(ref.Name)}]
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
									if defaultCert == nil {
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
}
