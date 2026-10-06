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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/controller"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

const (
	// DefaultDataplaneImage is the default container image for per-Gateway data-plane deployments.
	DefaultDataplaneImage = "gari-controller:latest"

	// LabelGatewayName is the label identifying the Gateway name on provisioned resources.
	LabelGatewayName = "gateway.networking.k8s.io/gateway-name"

	// LabelGatewayNamespace is the label identifying the Gateway namespace on provisioned resources.
	LabelGatewayNamespace = "gateway.networking.k8s.io/gateway-namespace"

	// LabelManagedBy is the label indicating the resource is managed by the singlepod provisioner.
	LabelManagedBy = "app.kubernetes.io/managed-by"

	// ManagedByValue is the value of LabelManagedBy.
	ManagedByValue = "gari-singlepod"
)

// ResourceNameForGateway computes the resource name for a Gateway within its namespace.
func ResourceNameForGateway(gwName string) string {
	base := fmt.Sprintf("%s-gari", gwName)
	if len(base) <= 63 {
		return base
	}
	h := sha256.Sum256([]byte(gwName))
	suffix := "-" + hex.EncodeToString(h[:4])
	return base[:63-len(suffix)] + suffix
}

// Option configures an AddressProvider.
type Option func(*AddressProvider)

// WithDataplaneImage configures the data-plane container image.
func WithDataplaneImage(image string) Option {
	return func(p *AddressProvider) {
		p.dataplaneImage = image
	}
}

// WithEnableH2C configures whether H2C is enabled on data-plane instances.
func WithEnableH2C(enable bool) Option {
	return func(p *AddressProvider) {
		p.enableH2C = enable
	}
}

// AddressProvider manages per-Gateway ServiceAccounts, Deployments, and LoadBalancer Services in the Gateway's namespace.
type AddressProvider struct {
	client         client.Client
	dataplaneImage string
	enableH2C      bool
}

// NewAddressProvider creates a new singlepod AddressProvider.
func NewAddressProvider(c client.Client, opts ...Option) *AddressProvider {
	p := &AddressProvider{
		client:         c,
		dataplaneImage: DefaultDataplaneImage,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// GatewayAddresses reconciles the per-Gateway ServiceAccount, Deployment, and LoadBalancer Service in the Gateway's namespace,
// and returns the Service's LoadBalancer ingress addresses and whether the Deployment is ready.
//
// Note: GatewayAddresses currently handles provisioning (creation and updates) of the ServiceAccount, Deployment, and Service.
// This will be factored into a dedicated provisioner interface in the future.
func (p *AddressProvider) GatewayAddresses(ctx context.Context, gw *gatewayv1.Gateway, effectiveListeners []*state.EffectiveListener) ([]gatewayv1.GatewayStatusAddress, bool, error) {
	if p.client == nil {
		return nil, false, nil
	}

	name := ResourceNameForGateway(gw.Name)
	gwNamespace := gw.Namespace

	labels := map[string]string{
		LabelGatewayName:      gw.Name,
		LabelGatewayNamespace: gw.Namespace,
		LabelManagedBy:        ManagedByValue,
		"app":                 name,
	}
	if gw.Spec.Infrastructure != nil && gw.Spec.Infrastructure.Labels != nil {
		for k, v := range gw.Spec.Infrastructure.Labels {
			labels[string(k)] = string(v)
		}
	}

	var annotations map[string]string
	if gw.Spec.Infrastructure != nil && gw.Spec.Infrastructure.Annotations != nil {
		annotations = make(map[string]string)
		for k, v := range gw.Spec.Infrastructure.Annotations {
			annotations[string(k)] = string(v)
		}
	}

	ownerRef := metav1.OwnerReference{
		APIVersion: gatewayv1.GroupVersion.String(),
		Kind:       "Gateway",
		Name:       gw.Name,
		UID:        gw.UID,
		Controller: state.Ptr(true),
	}

	// 1. Reconcile per-Gateway ServiceAccount in Gateway namespace
	desiredSA := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       gwNamespace,
			Labels:          labels,
			Annotations:     annotations,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
		},
	}
	var existingSA corev1.ServiceAccount
	err := p.client.Get(ctx, types.NamespacedName{Namespace: gwNamespace, Name: name}, &existingSA)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if createErr := p.client.Create(ctx, desiredSA); createErr != nil {
				return nil, false, fmt.Errorf("failed to create ServiceAccount for Gateway %s/%s: %w", gw.Namespace, gw.Name, createErr)
			}
		} else {
			return nil, false, err
		}
	} else {
		needsSAUpdate := false
		if !reflectMapEqual(existingSA.Labels, desiredSA.Labels) {
			existingSA.Labels = desiredSA.Labels
			needsSAUpdate = true
		}
		if !reflectMapEqual(existingSA.Annotations, desiredSA.Annotations) {
			existingSA.Annotations = desiredSA.Annotations
			needsSAUpdate = true
		}
		if needsSAUpdate {
			if updateErr := p.client.Update(ctx, &existingSA); updateErr != nil {
				return nil, false, fmt.Errorf("failed to update ServiceAccount %s/%s: %w", existingSA.Namespace, existingSA.Name, updateErr)
			}
		}
	}

	// 2. Reconcile per-Gateway Deployment in Gateway namespace
	args := []string{
		"--dataplane-mode",
		fmt.Sprintf("--gateway-namespace=%s", gw.Namespace),
		fmt.Sprintf("--gateway-name=%s", gw.Name),
	}
	// TODO(#620): support per-listener advertised HTTP/3 port when a Gateway has multiple HTTPS listeners on different ports.
	for _, el := range effectiveListeners {
		if el.Protocol == gatewayv1.HTTPSProtocolType {
			args = append(args, fmt.Sprintf("--proxy-http3-advertised-port=%d", el.Port))
			break
		}
	}
	if p.enableH2C {
		args = append(args, "--enable-h2c")
	}

	image := p.dataplaneImage
	if image == "" {
		image = DefaultDataplaneImage
	}

	desiredDeploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       gwNamespace,
			Labels:          labels,
			Annotations:     annotations,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: state.Ptr(int32(1)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": name,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: annotations,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: name,
					Containers: []corev1.Container{
						{
							Name:            "dataplane",
							Image:           image,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Args:            args,
							Ports: []corev1.ContainerPort{
								{
									Name:          "http",
									ContainerPort: 8000,
									Protocol:      corev1.ProtocolTCP,
								},
								{
									Name:          "https",
									ContainerPort: 8443,
									Protocol:      corev1.ProtocolTCP,
								},
								{
									Name:          "http3",
									ContainerPort: 8443,
									Protocol:      corev1.ProtocolUDP,
								},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/readyz",
										Port: intstr.FromInt32(8081),
									},
								},
								InitialDelaySeconds: 1,
								PeriodSeconds:       2,
							},
						},
					},
				},
			},
		},
	}

	var existingDeploy appsv1.Deployment
	err = p.client.Get(ctx, types.NamespacedName{Namespace: gwNamespace, Name: name}, &existingDeploy)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if createErr := p.client.Create(ctx, desiredDeploy); createErr != nil {
				return nil, false, fmt.Errorf("failed to create Deployment for Gateway %s/%s: %w", gw.Namespace, gw.Name, createErr)
			}
		} else {
			return nil, false, err
		}
	} else {
		needsUpdate := false
		if !reflect.DeepEqual(existingDeploy.Spec.Template.Spec, desiredDeploy.Spec.Template.Spec) {
			existingDeploy.Spec.Template.Spec = desiredDeploy.Spec.Template.Spec
			needsUpdate = true
		}
		if !reflectMapEqual(existingDeploy.Labels, desiredDeploy.Labels) {
			existingDeploy.Labels = desiredDeploy.Labels
			needsUpdate = true
		}
		if !reflectMapEqual(existingDeploy.Annotations, desiredDeploy.Annotations) {
			existingDeploy.Annotations = desiredDeploy.Annotations
			needsUpdate = true
		}
		if !reflectMapEqual(existingDeploy.Spec.Template.Labels, desiredDeploy.Spec.Template.Labels) {
			existingDeploy.Spec.Template.Labels = desiredDeploy.Spec.Template.Labels
			needsUpdate = true
		}
		if !reflectMapEqual(existingDeploy.Spec.Template.Annotations, desiredDeploy.Spec.Template.Annotations) {
			existingDeploy.Spec.Template.Annotations = desiredDeploy.Spec.Template.Annotations
			needsUpdate = true
		}
		if needsUpdate {
			if updateErr := p.client.Update(ctx, &existingDeploy); updateErr != nil {
				return nil, false, fmt.Errorf("failed to update Deployment %s/%s: %w", existingDeploy.Namespace, existingDeploy.Name, updateErr)
			}
		}
	}

	// 3. Reconcile per-Gateway LoadBalancer Service in Gateway namespace
	// Derive ports from effective listeners (including ListenerSets), falling back to gw.Spec.Listeners if effective listeners not compiled yet
	uniquePorts := make(map[gatewayv1.PortNumber]gatewayv1.ProtocolType)
	if len(effectiveListeners) > 0 {
		for _, el := range effectiveListeners {
			if prev, ok := uniquePorts[el.Port]; !ok || prev != gatewayv1.HTTPSProtocolType {
				uniquePorts[el.Port] = el.Protocol
			}
		}
	} else {
		for _, l := range gw.Spec.Listeners {
			if prev, ok := uniquePorts[l.Port]; !ok || prev != gatewayv1.HTTPSProtocolType {
				uniquePorts[l.Port] = l.Protocol
			}
		}
	}

	var sortedPorts []int
	for port := range uniquePorts {
		sortedPorts = append(sortedPorts, int(port))
	}
	sort.Ints(sortedPorts)

	var svcPorts []corev1.ServicePort
	for _, portInt := range sortedPorts {
		port := gatewayv1.PortNumber(portInt)
		proto := uniquePorts[port]
		if proto == gatewayv1.HTTPSProtocolType {
			svcPorts = append(svcPorts, corev1.ServicePort{
				Name:       fmt.Sprintf("https-%d", port),
				Port:       int32(port),
				TargetPort: intstr.FromInt32(8443),
				Protocol:   corev1.ProtocolTCP,
			})
			svcPorts = append(svcPorts, corev1.ServicePort{
				Name:       fmt.Sprintf("http3-%d", port),
				Port:       int32(port),
				TargetPort: intstr.FromInt32(8443),
				Protocol:   corev1.ProtocolUDP,
			})
		} else {
			svcPorts = append(svcPorts, corev1.ServicePort{
				Name:       fmt.Sprintf("http-%d", port),
				Port:       int32(port),
				TargetPort: intstr.FromInt32(8000),
				Protocol:   corev1.ProtocolTCP,
			})
		}
	}

	desiredSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       gwNamespace,
			Labels:          labels,
			Annotations:     annotations,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeLoadBalancer,
			Selector: map[string]string{
				"app": name,
			},
			Ports: svcPorts,
		},
	}

	var existingSvc corev1.Service
	err = p.client.Get(ctx, types.NamespacedName{Namespace: gwNamespace, Name: name}, &existingSvc)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if len(svcPorts) > 0 {
				if createErr := p.client.Create(ctx, desiredSvc); createErr != nil {
					return nil, false, fmt.Errorf("failed to create Service for Gateway %s/%s: %w", gw.Namespace, gw.Name, createErr)
				}
			}
			return nil, false, nil
		}
		return nil, false, err
	}

	needsSvcUpdate := false
	if !reflectPortsEqual(existingSvc.Spec.Ports, desiredSvc.Spec.Ports) {
		existingSvc.Spec.Ports = desiredSvc.Spec.Ports
		needsSvcUpdate = true
	}
	if !reflectMapEqual(existingSvc.Spec.Selector, desiredSvc.Spec.Selector) {
		existingSvc.Spec.Selector = desiredSvc.Spec.Selector
		needsSvcUpdate = true
	}
	if !reflectMapEqual(existingSvc.Labels, desiredSvc.Labels) {
		existingSvc.Labels = desiredSvc.Labels
		needsSvcUpdate = true
	}
	if !reflectMapEqual(existingSvc.Annotations, desiredSvc.Annotations) {
		existingSvc.Annotations = desiredSvc.Annotations
		needsSvcUpdate = true
	}
	if needsSvcUpdate {
		if updateErr := p.client.Update(ctx, &existingSvc); updateErr != nil {
			return nil, false, fmt.Errorf("failed to update Service %s/%s: %w", existingSvc.Namespace, existingSvc.Name, updateErr)
		}
	}

	// 4. Check Readiness and LB Addresses
	deployReady := false
	if err := p.client.Get(ctx, types.NamespacedName{Namespace: gwNamespace, Name: name}, &existingDeploy); err == nil {
		if existingDeploy.Status.AvailableReplicas > 0 || existingDeploy.Status.ReadyReplicas > 0 {
			deployReady = true
		}
	}

	var addresses []gatewayv1.GatewayStatusAddress
	for _, ingress := range existingSvc.Status.LoadBalancer.Ingress {
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

	return addresses, deployReady, nil
}

// OnGatewayDeleted cleans up the ServiceAccount, Deployment, and Service for the deleted Gateway if they exist.
func (p *AddressProvider) OnGatewayDeleted(ctx context.Context, gwKey types.NamespacedName) error {
	if p.client == nil {
		return nil
	}

	name := ResourceNameForGateway(gwKey.Name)

	var svc corev1.Service
	if err := p.client.Get(ctx, types.NamespacedName{Namespace: gwKey.Namespace, Name: name}, &svc); err == nil {
		if err := p.client.Delete(ctx, &svc); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete Service %s/%s: %w", gwKey.Namespace, name, err)
		}
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check Service %s/%s: %w", gwKey.Namespace, name, err)
	}

	var deploy appsv1.Deployment
	if err := p.client.Get(ctx, types.NamespacedName{Namespace: gwKey.Namespace, Name: name}, &deploy); err == nil {
		if err := p.client.Delete(ctx, &deploy); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete Deployment %s/%s: %w", gwKey.Namespace, name, err)
		}
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check Deployment %s/%s: %w", gwKey.Namespace, name, err)
	}

	var sa corev1.ServiceAccount
	if err := p.client.Get(ctx, types.NamespacedName{Namespace: gwKey.Namespace, Name: name}, &sa); err == nil {
		if err := p.client.Delete(ctx, &sa); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete ServiceAccount %s/%s: %w", gwKey.Namespace, name, err)
		}
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check ServiceAccount %s/%s: %w", gwKey.Namespace, name, err)
	}

	return nil
}

// SweepOrphans garbage-collects provisioned Services, Deployments, and ServiceAccounts across all namespaces whose corresponding Gateway no longer exists.
func (p *AddressProvider) SweepOrphans(ctx context.Context) error {
	if p.client == nil {
		return nil
	}

	var errs []error

	var svcList corev1.ServiceList
	if err := p.client.List(ctx, &svcList, client.MatchingLabels{LabelManagedBy: ManagedByValue}); err != nil {
		errs = append(errs, fmt.Errorf("failed to list managed services: %w", err))
	} else {
		for _, svc := range svcList.Items {
			gwNs := svc.Labels[LabelGatewayNamespace]
			gwName := svc.Labels[LabelGatewayName]
			if gwNs == "" || gwName == "" {
				continue
			}
			var gw gatewayv1.Gateway
			err := p.client.Get(ctx, types.NamespacedName{Namespace: gwNs, Name: gwName}, &gw)
			if apierrors.IsNotFound(err) || (err == nil && gw.DeletionTimestamp != nil) {
				if delErr := p.client.Delete(ctx, &svc); delErr != nil && !apierrors.IsNotFound(delErr) {
					errs = append(errs, fmt.Errorf("failed to delete orphaned Service %s/%s: %w", svc.Namespace, svc.Name, delErr))
				}
			}
		}
	}

	var deployList appsv1.DeploymentList
	if err := p.client.List(ctx, &deployList, client.MatchingLabels{LabelManagedBy: ManagedByValue}); err != nil {
		errs = append(errs, fmt.Errorf("failed to list managed deployments: %w", err))
	} else {
		for _, deploy := range deployList.Items {
			gwNs := deploy.Labels[LabelGatewayNamespace]
			gwName := deploy.Labels[LabelGatewayName]
			if gwNs == "" || gwName == "" {
				continue
			}
			var gw gatewayv1.Gateway
			err := p.client.Get(ctx, types.NamespacedName{Namespace: gwNs, Name: gwName}, &gw)
			if apierrors.IsNotFound(err) || (err == nil && gw.DeletionTimestamp != nil) {
				if delErr := p.client.Delete(ctx, &deploy); delErr != nil && !apierrors.IsNotFound(delErr) {
					errs = append(errs, fmt.Errorf("failed to delete orphaned Deployment %s/%s: %w", deploy.Namespace, deploy.Name, delErr))
				}
			}
		}
	}

	var saList corev1.ServiceAccountList
	if err := p.client.List(ctx, &saList, client.MatchingLabels{LabelManagedBy: ManagedByValue}); err != nil {
		errs = append(errs, fmt.Errorf("failed to list managed serviceaccounts: %w", err))
	} else {
		for _, sa := range saList.Items {
			gwNs := sa.Labels[LabelGatewayNamespace]
			gwName := sa.Labels[LabelGatewayName]
			if gwNs == "" || gwName == "" {
				continue
			}
			var gw gatewayv1.Gateway
			err := p.client.Get(ctx, types.NamespacedName{Namespace: gwNs, Name: gwName}, &gw)
			if apierrors.IsNotFound(err) || (err == nil && gw.DeletionTimestamp != nil) {
				if delErr := p.client.Delete(ctx, &sa); delErr != nil && !apierrors.IsNotFound(delErr) {
					errs = append(errs, fmt.Errorf("failed to delete orphaned ServiceAccount %s/%s: %w", sa.Namespace, sa.Name, delErr))
				}
			}
		}
	}

	return errors.Join(errs...)
}

// SetupWatches registers watches on managed Services and Deployments, and starts the orphan sweep runnable.
func (p *AddressProvider) SetupWatches(mgr ctrl.Manager, bldr *builder.Builder) error {
	if p.client == nil && mgr != nil {
		p.client = mgr.GetClient()
	}

	if mgr != nil {
		err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
			if !mgr.GetCache().WaitForCacheSync(ctx) {
				return fmt.Errorf("failed to wait for cache sync in address provider")
			}
			return p.SweepOrphans(ctx)
		}))
		if err != nil {
			return fmt.Errorf("failed to register orphan sweep runnable: %w", err)
		}
	}

	mapFunc := func(ctx context.Context, obj client.Object) []ctrl.Request {
		labels := obj.GetLabels()
		if labels == nil || labels[LabelManagedBy] != ManagedByValue {
			return nil
		}
		gwName := labels[LabelGatewayName]
		gwNs := labels[LabelGatewayNamespace]
		if gwName == "" || gwNs == "" {
			return nil
		}
		return []ctrl.Request{
			{
				NamespacedName: types.NamespacedName{
					Namespace: gwNs,
					Name:      gwName,
				},
			},
		}
	}

	bldr.Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(mapFunc))
	bldr.Watches(&appsv1.Deployment{}, handler.EnqueueRequestsFromMapFunc(mapFunc))

	return nil
}

var (
	_ controller.AddressProvider      = (*AddressProvider)(nil)
	_ controller.GatewayDeleteHandler = (*AddressProvider)(nil)
	_ controller.AddressWatcher       = (*AddressProvider)(nil)
)

func reflectPortsEqual(a, b []corev1.ServicePort) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Port != b[i].Port || a[i].Protocol != b[i].Protocol || a[i].TargetPort != b[i].TargetPort {
			return false
		}
	}
	return true
}

func reflectMapEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
