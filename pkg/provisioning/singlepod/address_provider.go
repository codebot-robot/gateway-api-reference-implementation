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

package singlepod

import (
	"context"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

const (
	// DefaultServiceName is the default name of the Service providing the single-pod proxy LoadBalancer address.
	DefaultServiceName = "gari-proxy"

	// DefaultServiceNamespace is the default namespace of the proxy Service.
	DefaultServiceNamespace = "default"
)

// AddressProvider provides Gateway addresses by looking up the LoadBalancer ingress
// of a shared single-pod proxy Service (such as default/gari-proxy).
type AddressProvider struct {
	client    client.Client
	namespace string
	name      string
}

// NewAddressProvider creates a new AddressProvider for a single-pod proxy Service.
// If client is nil, it will be populated during SetupWatches from the controller Manager.
func NewAddressProvider(c client.Client, namespace, name string) *AddressProvider {
	if namespace == "" {
		namespace = DefaultServiceNamespace
	}
	if name == "" {
		name = DefaultServiceName
	}
	return &AddressProvider{
		client:    c,
		namespace: namespace,
		name:      name,
	}
}

// GatewayAddresses returns the LoadBalancer IP and/or Hostname addresses of the configured Service.
func (p *AddressProvider) GatewayAddresses(ctx context.Context, gw *gatewayv1.Gateway) ([]gatewayv1.GatewayStatusAddress, error) {
	if p.client == nil {
		return nil, nil
	}

	var svc corev1.Service
	if err := p.client.Get(ctx, types.NamespacedName{Namespace: p.namespace, Name: p.name}, &svc); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	var addresses []gatewayv1.GatewayStatusAddress
	for _, ingress := range svc.Status.LoadBalancer.Ingress {
		if ingress.IP != "" {
			addresses = append(addresses, gatewayv1.GatewayStatusAddress{
				Type:  state.Ptr(gatewayv1.IPAddressType),
				Value: ingress.IP,
			})
		}
		if ingress.Hostname != "" {
			addresses = append(addresses, gatewayv1.GatewayStatusAddress{
				Type:  state.Ptr(gatewayv1.HostnameAddressType),
				Value: ingress.Hostname,
			})
		}
	}
	return addresses, nil
}

// SetupWatches registers a watch on the Service to enqueue all Gateways for reconciliation when the Service changes.
func (p *AddressProvider) SetupWatches(mgr ctrl.Manager, bldr *builder.Builder) error {
	if p.client == nil && mgr != nil {
		p.client = mgr.GetClient()
	}
	bldr.Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
		svc, ok := obj.(*corev1.Service)
		if !ok {
			return nil
		}
		if svc.Name == p.name && svc.Namespace == p.namespace {
			var gwList gatewayv1.GatewayList
			if err := p.client.List(ctx, &gwList); err == nil {
				var requests []ctrl.Request
				for _, gw := range gwList.Items {
					requests = append(requests, ctrl.Request{
						NamespacedName: types.NamespacedName{
							Namespace: gw.Namespace,
							Name:      gw.Name,
						},
					})
				}
				return requests
			}
		}
		return nil
	}))
	return nil
}
