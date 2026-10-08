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
	"sort"
	"strings"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/controller"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	"k8s.io/client-go/util/retry"
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

	// FieldManager is the field manager name used for server-side apply of per-Gateway resources.
	FieldManager = "gari-provisioner"

	// LabelGatewayName is the label identifying the Gateway name on provisioned resources.
	LabelGatewayName = "gateway.networking.k8s.io/gateway-name"

	// LabelGatewayNamespace is the label identifying the Gateway namespace on provisioned resources.
	LabelGatewayNamespace = "gateway.networking.k8s.io/gateway-namespace"

	// LabelManagedBy is the label indicating the resource is managed by the singlepod provisioner.
	LabelManagedBy = "app.kubernetes.io/managed-by"

	// ManagedByValue is the value of LabelManagedBy.
	ManagedByValue = "gari-singlepod"

	// LabelAppName is the label identifying the data-plane component.
	LabelAppName = "app.kubernetes.io/name"

	// AppNameValue is the value for LabelAppName on data-plane components.
	AppNameValue = "gari-dataplane"

	// DataplaneClusterRoleName is the name of the ClusterRole for per-Gateway data-plane instances.
	DataplaneClusterRoleName = "gari-dataplane"

	// DataplaneClusterRoleBindingName is the name of the shared ClusterRoleBinding for data-plane service accounts.
	DataplaneClusterRoleBindingName = "gari-dataplane"
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

// WithAPIReader configures the direct API reader for the AddressProvider.
func WithAPIReader(reader client.Reader) Option {
	return func(p *AddressProvider) {
		p.apiReader = reader
	}
}

// AddressProvider manages per-Gateway ServiceAccounts, Deployments, LoadBalancer Services, and ClusterRoleBinding subjects.
type AddressProvider struct {
	client         client.Client
	apiReader      client.Reader
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

// isOwnedByGateway checks if the object was created for this Gateway via controller OwnerReference or labels.
func isOwnedByGateway(obj metav1.Object, gw *gatewayv1.Gateway) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Kind == "Gateway" && ref.Name == gw.Name && (ref.Controller != nil && *ref.Controller) {
			return true
		}
	}
	labels := obj.GetLabels()
	if labels != nil && labels[LabelManagedBy] == ManagedByValue && labels[LabelGatewayName] == gw.Name {
		if labels[LabelGatewayNamespace] == "" || labels[LabelGatewayNamespace] == gw.Namespace {
			return true
		}
	}
	return false
}

func buildLabelsAndAnnotations(gw *gatewayv1.Gateway) (map[string]string, map[string]string) {
	labels := make(map[string]string)
	var annotations map[string]string

	if gw.Spec.Infrastructure != nil {
		if gw.Spec.Infrastructure.Labels != nil {
			for k, v := range gw.Spec.Infrastructure.Labels {
				keyStr := string(k)
				if strings.HasPrefix(keyStr, "gateway.networking.k8s.io/") ||
					keyStr == LabelManagedBy ||
					keyStr == LabelAppName {
					continue
				}
				labels[keyStr] = string(v)
			}
		}
		if gw.Spec.Infrastructure.Annotations != nil {
			annotations = make(map[string]string)
			for k, v := range gw.Spec.Infrastructure.Annotations {
				keyStr := string(k)
				annotations[keyStr] = string(v)
			}
		}
	}

	// GARI-owned labels override user labels
	labels[LabelGatewayName] = gw.Name
	labels[LabelGatewayNamespace] = gw.Namespace
	labels[LabelManagedBy] = ManagedByValue
	labels[LabelAppName] = AppNameValue

	return labels, annotations
}

// ensureClusterRoleBindingSubject ensures that the given ServiceAccount is listed as a subject in the shared gari-dataplane ClusterRoleBinding.
func (p *AddressProvider) ensureClusterRoleBindingSubject(ctx context.Context, saNamespace, saName string) error {
	if p.client == nil {
		return nil
	}

	reader := p.apiReader
	if reader == nil {
		reader = p.client
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var crb rbacv1.ClusterRoleBinding
		if err := reader.Get(ctx, types.NamespacedName{Name: DataplaneClusterRoleBindingName}, &crb); err != nil {
			return err
		}

		subjectExists := false
		for _, s := range crb.Subjects {
			if s.Kind == "ServiceAccount" && s.Namespace == saNamespace && s.Name == saName {
				subjectExists = true
				break
			}
		}

		if subjectExists {
			return nil
		}

		crb.Subjects = append(crb.Subjects, rbacv1.Subject{
			Kind:      "ServiceAccount",
			Namespace: saNamespace,
			Name:      saName,
		})

		sortSubjects(crb.Subjects)

		return p.client.Update(ctx, &crb)
	})
}

// removeClusterRoleBindingSubject removes the given ServiceAccount from the shared gari-dataplane ClusterRoleBinding.
func (p *AddressProvider) removeClusterRoleBindingSubject(ctx context.Context, saNamespace, saName string) error {
	if p.client == nil {
		return nil
	}

	reader := p.apiReader
	if reader == nil {
		reader = p.client
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var crb rbacv1.ClusterRoleBinding
		if err := reader.Get(ctx, types.NamespacedName{Name: DataplaneClusterRoleBindingName}, &crb); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}

		var newSubjects []rbacv1.Subject
		found := false
		for _, s := range crb.Subjects {
			if s.Kind == "ServiceAccount" && s.Namespace == saNamespace && s.Name == saName {
				found = true
				continue
			}
			newSubjects = append(newSubjects, s)
		}

		if !found {
			return nil
		}

		crb.Subjects = newSubjects
		sortSubjects(crb.Subjects)

		return p.client.Update(ctx, &crb)
	})
}

func sortSubjects(subjects []rbacv1.Subject) {
	sort.Slice(subjects, func(i, j int) bool {
		if subjects[i].Namespace != subjects[j].Namespace {
			return subjects[i].Namespace < subjects[j].Namespace
		}
		return subjects[i].Name < subjects[j].Name
	})
}

// GatewayAddresses reconciles the per-Gateway ServiceAccount, ClusterRoleBinding subject, Deployment, and LoadBalancer Service,
// and returns the Service's LoadBalancer ingress addresses and whether the Deployment is ready.
func (p *AddressProvider) GatewayAddresses(ctx context.Context, gw *gatewayv1.Gateway, effectiveListeners []*state.EffectiveListener) ([]gatewayv1.GatewayStatusAddress, bool, error) {
	if p.client == nil {
		return nil, false, nil
	}

	name := ResourceNameForGateway(gw.Name)
	gwNamespace := gw.Namespace

	labels, annotations := buildLabelsAndAnnotations(gw)

	ownerRef := metav1ac.OwnerReference().
		WithAPIVersion(gatewayv1.GroupVersion.String()).
		WithKind("Gateway").
		WithName(gw.Name).
		WithUID(gw.UID).
		WithController(true).
		WithBlockOwnerDeletion(true)

	// 1. Reconcile per-Gateway ServiceAccount in Gateway namespace
	var existingSA corev1.ServiceAccount
	err := p.client.Get(ctx, types.NamespacedName{Namespace: gwNamespace, Name: name}, &existingSA)
	if err == nil {
		if !isOwnedByGateway(&existingSA, gw) {
			return nil, false, &controller.OwnershipConflictError{
				Message: fmt.Sprintf("conflict: existing ServiceAccount %s/%s is not owned by Gateway %s", gwNamespace, name, gw.Name),
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, false, err
	}

	saApply := corev1ac.ServiceAccount(name, gwNamespace).
		WithOwnerReferences(ownerRef).
		WithLabels(labels)
	if len(annotations) > 0 {
		saApply.WithAnnotations(annotations)
	}
	if err := p.client.Apply(ctx, saApply, client.FieldOwner(FieldManager), client.ForceOwnership); err != nil {
		return nil, false, fmt.Errorf("failed to apply ServiceAccount for Gateway %s/%s: %w", gw.Namespace, gw.Name, err)
	}

	// 2. Ensure ServiceAccount is in shared gari-dataplane ClusterRoleBinding
	if err := p.ensureClusterRoleBindingSubject(ctx, gwNamespace, name); err != nil {
		return nil, false, fmt.Errorf("failed to add subject to ClusterRoleBinding %s: %w", DataplaneClusterRoleBindingName, err)
	}

	// 3. Reconcile per-Gateway Deployment in Gateway namespace
	var existingDeploy appsv1.Deployment
	err = p.client.Get(ctx, types.NamespacedName{Namespace: gwNamespace, Name: name}, &existingDeploy)
	if err == nil {
		if !isOwnedByGateway(&existingDeploy, gw) {
			return nil, false, &controller.OwnershipConflictError{
				Message: fmt.Sprintf("conflict: existing Deployment %s/%s is not owned by Gateway %s", gwNamespace, name, gw.Name),
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, false, err
	}

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

	podTemplate := corev1ac.PodTemplateSpec().
		WithLabels(labels).
		WithSpec(corev1ac.PodSpec().
			WithServiceAccountName(name).
			WithContainers(corev1ac.Container().
				WithName("dataplane").
				WithImage(image).
				WithImagePullPolicy(corev1.PullIfNotPresent).
				WithArgs(args...).
				WithPorts(
					corev1ac.ContainerPort().
						WithName("http").
						WithContainerPort(8000).
						WithProtocol(corev1.ProtocolTCP),
					corev1ac.ContainerPort().
						WithName("https").
						WithContainerPort(8443).
						WithProtocol(corev1.ProtocolTCP),
					corev1ac.ContainerPort().
						WithName("http3").
						WithContainerPort(8443).
						WithProtocol(corev1.ProtocolUDP),
				).
				WithReadinessProbe(corev1ac.Probe().
					WithHTTPGet(corev1ac.HTTPGetAction().
						WithPath("/readyz").
						WithPort(intstr.FromInt32(8081)),
					).
					WithInitialDelaySeconds(1).
					WithPeriodSeconds(2),
				),
			),
		)
	if len(annotations) > 0 {
		podTemplate.WithAnnotations(annotations)
	}

	deployApply := appsv1ac.Deployment(name, gwNamespace).
		WithOwnerReferences(ownerRef).
		WithLabels(labels).
		WithSpec(appsv1ac.DeploymentSpec().
			WithReplicas(1).
			WithSelector(metav1ac.LabelSelector().
				WithMatchLabels(map[string]string{
					LabelGatewayName: gw.Name,
					LabelAppName:     AppNameValue,
				}),
			).
			WithTemplate(podTemplate),
		)
	if len(annotations) > 0 {
		deployApply.WithAnnotations(annotations)
	}
	if err := p.client.Apply(ctx, deployApply, client.FieldOwner(FieldManager), client.ForceOwnership); err != nil {
		return nil, false, fmt.Errorf("failed to apply Deployment for Gateway %s/%s: %w", gw.Namespace, gw.Name, err)
	}

	// 4. Reconcile per-Gateway LoadBalancer Service in Gateway namespace
	var existingSvc corev1.Service
	err = p.client.Get(ctx, types.NamespacedName{Namespace: gwNamespace, Name: name}, &existingSvc)
	if err == nil {
		if !isOwnedByGateway(&existingSvc, gw) {
			return nil, false, &controller.OwnershipConflictError{
				Message: fmt.Sprintf("conflict: existing Service %s/%s is not owned by Gateway %s", gwNamespace, name, gw.Name),
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, false, err
	}

	uniquePorts := make(map[gatewayv1.PortNumber]gatewayv1.ProtocolType)
	if len(effectiveListeners) > 0 {
		for _, el := range effectiveListeners {
			if prev, ok := uniquePorts[el.Port]; !ok {
				uniquePorts[el.Port] = el.Protocol
			} else if el.Protocol == gatewayv1.HTTPSProtocolType {
				uniquePorts[el.Port] = gatewayv1.HTTPSProtocolType
			} else if el.Protocol == gatewayv1.TLSProtocolType && prev != gatewayv1.HTTPSProtocolType {
				uniquePorts[el.Port] = gatewayv1.TLSProtocolType
			}
		}
	} else {
		for _, l := range gw.Spec.Listeners {
			if prev, ok := uniquePorts[l.Port]; !ok {
				uniquePorts[l.Port] = l.Protocol
			} else if l.Protocol == gatewayv1.HTTPSProtocolType {
				uniquePorts[l.Port] = gatewayv1.HTTPSProtocolType
			} else if l.Protocol == gatewayv1.TLSProtocolType && prev != gatewayv1.HTTPSProtocolType {
				uniquePorts[l.Port] = gatewayv1.TLSProtocolType
			}
		}
	}

	var sortedPorts []int
	for port := range uniquePorts {
		sortedPorts = append(sortedPorts, int(port))
	}
	sort.Ints(sortedPorts)

	var svcPorts []*corev1ac.ServicePortApplyConfiguration
	for _, portInt := range sortedPorts {
		port := gatewayv1.PortNumber(portInt)
		proto := uniquePorts[port]
		if proto == gatewayv1.HTTPSProtocolType {
			svcPorts = append(svcPorts,
				corev1ac.ServicePort().
					WithName(fmt.Sprintf("https-%d", port)).
					WithPort(int32(port)).
					WithTargetPort(intstr.FromInt32(8443)).
					WithProtocol(corev1.ProtocolTCP),
				corev1ac.ServicePort().
					WithName(fmt.Sprintf("http3-%d", port)).
					WithPort(int32(port)).
					WithTargetPort(intstr.FromInt32(8443)).
					WithProtocol(corev1.ProtocolUDP),
			)
		} else if proto == gatewayv1.TLSProtocolType {
			svcPorts = append(svcPorts,
				corev1ac.ServicePort().
					WithName(fmt.Sprintf("tls-%d", port)).
					WithPort(int32(port)).
					WithTargetPort(intstr.FromInt32(8443)).
					WithProtocol(corev1.ProtocolTCP),
			)
		} else {
			svcPorts = append(svcPorts,
				corev1ac.ServicePort().
					WithName(fmt.Sprintf("http-%d", port)).
					WithPort(int32(port)).
					WithTargetPort(intstr.FromInt32(8000)).
					WithProtocol(corev1.ProtocolTCP),
			)
		}
	}

	if len(svcPorts) > 0 {
		svcApply := corev1ac.Service(name, gwNamespace).
			WithOwnerReferences(ownerRef).
			WithLabels(labels).
			WithSpec(corev1ac.ServiceSpec().
				WithType(corev1.ServiceTypeLoadBalancer).
				WithSelector(map[string]string{
					LabelGatewayName: gw.Name,
					LabelAppName:     AppNameValue,
				}).
				WithPorts(svcPorts...),
			)
		if len(annotations) > 0 {
			svcApply.WithAnnotations(annotations)
		}
		if err := p.client.Apply(ctx, svcApply, client.FieldOwner(FieldManager), client.ForceOwnership); err != nil {
			return nil, false, fmt.Errorf("failed to apply Service for Gateway %s/%s: %w", gw.Namespace, gw.Name, err)
		}
	}

	// 5. Check Readiness and LB Addresses
	deployReady := false
	if err := p.client.Get(ctx, types.NamespacedName{Namespace: gwNamespace, Name: name}, &existingDeploy); err == nil {
		if existingDeploy.Status.AvailableReplicas > 0 || existingDeploy.Status.ReadyReplicas > 0 {
			deployReady = true
		}
	}

	var addresses []gatewayv1.GatewayStatusAddress
	if err := p.client.Get(ctx, types.NamespacedName{Namespace: gwNamespace, Name: name}, &existingSvc); err == nil {
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
	}

	return addresses, deployReady, nil
}

// OnGatewayDeleted cleans up the ServiceAccount, Deployment, Service, and ClusterRoleBinding subject for the deleted Gateway if they exist and are owned by it.
func (p *AddressProvider) OnGatewayDeleted(ctx context.Context, gwKey types.NamespacedName) error {
	if p.client == nil {
		return nil
	}

	name := ResourceNameForGateway(gwKey.Name)

	dummyGW := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      gwKey.Name,
			Namespace: gwKey.Namespace,
		},
	}

	var svc corev1.Service
	if err := p.client.Get(ctx, types.NamespacedName{Namespace: gwKey.Namespace, Name: name}, &svc); err == nil {
		if isOwnedByGateway(&svc, dummyGW) {
			if err := p.client.Delete(ctx, &svc); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to delete Service %s/%s: %w", gwKey.Namespace, name, err)
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check Service %s/%s: %w", gwKey.Namespace, name, err)
	}

	var deploy appsv1.Deployment
	if err := p.client.Get(ctx, types.NamespacedName{Namespace: gwKey.Namespace, Name: name}, &deploy); err == nil {
		if isOwnedByGateway(&deploy, dummyGW) {
			if err := p.client.Delete(ctx, &deploy); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to delete Deployment %s/%s: %w", gwKey.Namespace, name, err)
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check Deployment %s/%s: %w", gwKey.Namespace, name, err)
	}

	var sa corev1.ServiceAccount
	if err := p.client.Get(ctx, types.NamespacedName{Namespace: gwKey.Namespace, Name: name}, &sa); err == nil {
		if isOwnedByGateway(&sa, dummyGW) {
			if err := p.client.Delete(ctx, &sa); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to delete ServiceAccount %s/%s: %w", gwKey.Namespace, name, err)
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check ServiceAccount %s/%s: %w", gwKey.Namespace, name, err)
	}

	if err := p.removeClusterRoleBindingSubject(ctx, gwKey.Namespace, name); err != nil {
		return fmt.Errorf("failed to remove subject from ClusterRoleBinding %s: %w", DataplaneClusterRoleBindingName, err)
	}

	return nil
}

// SweepOrphans garbage-collects provisioned Services, Deployments, ServiceAccounts, and ClusterRoleBinding subjects across all namespaces whose corresponding Gateway no longer exists.
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

	// Sweep subjects in shared gari-dataplane ClusterRoleBinding
	reader := p.apiReader
	if reader == nil {
		reader = p.client
	}
	var crb rbacv1.ClusterRoleBinding
	if err := reader.Get(ctx, types.NamespacedName{Name: DataplaneClusterRoleBindingName}, &crb); err == nil {
		var validSubjects []rbacv1.Subject
		changed := false
		for _, s := range crb.Subjects {
			if s.Kind != "ServiceAccount" {
				validSubjects = append(validSubjects, s)
				continue
			}
			// Check if ServiceAccount still exists and has managed label
			var sa corev1.ServiceAccount
			err := reader.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: s.Name}, &sa)
			if apierrors.IsNotFound(err) || (sa.Labels != nil && sa.Labels[LabelManagedBy] == ManagedByValue && sa.Labels[LabelGatewayName] != "") {
				if apierrors.IsNotFound(err) {
					changed = true
					continue
				}
				gwName := sa.Labels[LabelGatewayName]
				var gw gatewayv1.Gateway
				if gwErr := p.client.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: gwName}, &gw); apierrors.IsNotFound(gwErr) || (gwErr == nil && gw.DeletionTimestamp != nil) {
					changed = true
					continue
				}
			}
			validSubjects = append(validSubjects, s)
		}
		if changed {
			crb.Subjects = validSubjects
			sortSubjects(crb.Subjects)
			if updErr := p.client.Update(ctx, &crb); updErr != nil && !apierrors.IsNotFound(updErr) {
				errs = append(errs, fmt.Errorf("failed to update ClusterRoleBinding %s during orphan sweep: %w", DataplaneClusterRoleBindingName, updErr))
			}
		}
	}

	return errors.Join(errs...)
}

// SetupWatches registers watches on managed Services and Deployments, and starts the orphan sweep runnable.
func (p *AddressProvider) SetupWatches(mgr ctrl.Manager, bldr *builder.Builder) error {
	if mgr != nil {
		if p.client == nil {
			p.client = mgr.GetClient()
		}
		p.apiReader = mgr.GetAPIReader()
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
