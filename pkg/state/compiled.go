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

package state

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"slices"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

// ListenerOwner identifies the resource (Gateway or ListenerSet) that defines a listener.
type ListenerOwner struct {
	Kind      gatewayv1.Kind
	Namespace string
	Name      string
}

// ListenerSpec represents the common configuration specification of a listener across Gateway and ListenerSet.
type ListenerSpec struct {
	Name          gatewayv1.SectionName
	Port          gatewayv1.PortNumber
	Protocol      gatewayv1.ProtocolType
	Hostname      *gatewayv1.Hostname
	TLS           *gatewayv1.ListenerTLSConfig
	AllowedRoutes *gatewayv1.AllowedRoutes
}

// ListenerToSpec converts a Gateway Listener to ListenerSpec.
func ListenerToSpec(l gatewayv1.Listener) ListenerSpec {
	return ListenerSpec{
		Name:          l.Name,
		Port:          l.Port,
		Protocol:      l.Protocol,
		Hostname:      l.Hostname,
		TLS:           l.TLS,
		AllowedRoutes: l.AllowedRoutes,
	}
}

// ListenerEntryToSpec converts a ListenerSet ListenerEntry to ListenerSpec.
func ListenerEntryToSpec(l gatewayv1.ListenerEntry) ListenerSpec {
	return ListenerSpec{
		Name:          l.Name,
		Port:          l.Port,
		Protocol:      l.Protocol,
		Hostname:      l.Hostname,
		TLS:           l.TLS,
		AllowedRoutes: l.AllowedRoutes,
	}
}

// EffectiveListener represents a single compiled listener on a Gateway,
// originating either directly from the Gateway spec or attached via an allowed ListenerSet.
type EffectiveListener struct {
	Owner            ListenerOwner
	ParentGateway    types.NamespacedName
	Name             gatewayv1.SectionName
	Port             gatewayv1.PortNumber
	Protocol         gatewayv1.ProtocolType
	Hostname         *gatewayv1.Hostname
	TLS              *gatewayv1.ListenerTLSConfig
	AllowedRoutes    *gatewayv1.AllowedRoutes
	SupportedKinds   []gatewayv1.RouteGroupKind
	Conditions       []metav1.Condition
	AttachedRoutes   int32
	Routes           []InternalRoute
	TLSBackends      map[string][]InternalTLSBackend
	BoundTCPRoute    *types.NamespacedName
	TCPBackends      []InternalTCPBackend
	BoundUDPRoute    *types.NamespacedName
	UDPBackends      []InternalUDPBackend
	ClientValidation *InternalClientValidation
	Generation       int64
}

// IsAccepted returns true if the listener has an Accepted condition with status True.
func (el *EffectiveListener) IsAccepted() bool {
	for _, c := range el.Conditions {
		if c.Type == string(gatewayv1.ListenerConditionAccepted) && c.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

// IsProgrammed returns true if the listener has a Programmed condition with status True.
func (el *EffectiveListener) IsProgrammed() bool {
	for _, c := range el.Conditions {
		if c.Type == string(gatewayv1.ListenerConditionProgrammed) && c.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

// IsConflicted returns true if the listener has a Conflicted condition with status True.
func (el *EffectiveListener) IsConflicted() bool {
	for _, c := range el.Conditions {
		if c.Type == string(gatewayv1.ListenerConditionConflicted) && c.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

// QualifiedName returns the listener name, qualified with owner namespace and name for ListenerSets.
func (el *EffectiveListener) QualifiedName() string {
	if el.Owner.Kind == "Gateway" {
		return string(el.Name)
	}
	return fmt.Sprintf("%s/%s/%s", el.Owner.Namespace, el.Owner.Name, el.Name)
}

// ToInternalListener converts the effective listener to an InternalListener for proxy routing.
func (el *EffectiveListener) ToInternalListener() InternalListener {
	var tlsMode *gatewayv1.TLSModeType
	if el.TLS != nil {
		if el.TLS.Mode != nil {
			tlsMode = el.TLS.Mode
		} else if el.Protocol == gatewayv1.TLSProtocolType {
			m := gatewayv1.TLSModeTerminate
			tlsMode = &m
		}
	}
	var tlsBackends map[string][]InternalTLSBackend
	for k, v := range el.TLSBackends {
		if len(v) > 0 {
			if tlsBackends == nil {
				tlsBackends = make(map[string][]InternalTLSBackend)
			}
			copied := make([]InternalTLSBackend, len(v))
			copy(copied, v)
			tlsBackends[k] = copied
		}
	}
	var tcpBackends []InternalTCPBackend
	if len(el.TCPBackends) > 0 {
		tcpBackends = make([]InternalTCPBackend, len(el.TCPBackends))
		copy(tcpBackends, el.TCPBackends)
	}
	var udpBackends []InternalUDPBackend
	if len(el.UDPBackends) > 0 {
		udpBackends = make([]InternalUDPBackend, len(el.UDPBackends))
		copy(udpBackends, el.UDPBackends)
	}
	var clientValidation *InternalClientValidation
	if el.ClientValidation != nil {
		clientValidation = el.ClientValidation.DeepCopy()
	}
	return InternalListener{
		Name:             el.QualifiedName(),
		Protocol:         el.Protocol,
		Port:             el.Port,
		Hostname:         string(ValueOf(el.Hostname)),
		GatewayName:      el.ParentGateway,
		Routes:           el.Routes,
		TLSMode:          tlsMode,
		TLSBackends:      tlsBackends,
		TCPBackends:      tcpBackends,
		UDPBackends:      udpBackends,
		ClientValidation: clientValidation,
	}
}

// CompiledGateway contains the compiled state for a single Gateway.
type CompiledGateway struct {
	Gateway               *gatewayv1.Gateway
	EffectiveListeners    []*EffectiveListener
	AttachedListenerSets  int32
	Conditions            []metav1.Condition
	BackendClientCert     *tls.Certificate
	ResolvedRefsCondition metav1.Condition
}

// CompiledRoute contains the compiled state for an HTTPRoute.
type CompiledRoute struct {
	HTTPRoute        *gatewayv1.HTTPRoute
	RouteState       *HTTPRouteState
	ParentConditions []metav1.Condition
}

// CompiledTLSRoute contains the compiled state for a TLSRoute.
type CompiledTLSRoute struct {
	TLSRoute         *gatewayv1.TLSRoute
	RouteState       *TLSRouteState
	ParentConditions []metav1.Condition
}

// CompiledGRPCRoute contains the compiled state for a GRPCRoute.
type CompiledGRPCRoute struct {
	GRPCRoute        *gatewayv1.GRPCRoute
	RouteState       *GRPCRouteState
	ParentConditions []metav1.Condition
}

// CompiledTCPRoute contains the compiled state for a TCPRoute.
type CompiledTCPRoute struct {
	TCPRoute         *gatewayv1.TCPRoute
	RouteState       *TCPRouteState
	ParentConditions []metav1.Condition
}

// CompiledUDPRoute contains the compiled state for a UDPRoute.
type CompiledUDPRoute struct {
	UDPRoute         *gatewayv1.UDPRoute
	RouteState       *UDPRouteState
	ParentConditions []metav1.Condition
}

// CompiledModel is the complete compiled model of Gateways, ListenerSets, HTTPRoutes, TLSRoutes, GRPCRoutes, TCPRoutes, and UDPRoutes.
type CompiledModel struct {
	Gateways     map[types.NamespacedName]*CompiledGateway
	HTTPRoutes   map[types.NamespacedName]*CompiledRoute
	TLSRoutes    map[types.NamespacedName]*CompiledTLSRoute
	GRPCRoutes   map[types.NamespacedName]*CompiledGRPCRoute
	TCPRoutes    map[types.NamespacedName]*CompiledTCPRoute
	UDPRoutes    map[types.NamespacedName]*CompiledUDPRoute
	ListenerSets map[types.NamespacedName]*gatewayv1.ListenerSet
}

// GatewaysList returns all compiled gateways as a slice.
func (cm *CompiledModel) GatewaysList() []*CompiledGateway {
	if cm == nil {
		return nil
	}
	res := make([]*CompiledGateway, 0, len(cm.Gateways))
	for _, cg := range cm.Gateways {
		res = append(res, cg)
	}
	sort.Slice(res, func(i, j int) bool {
		if res[i].Gateway.Namespace != res[j].Gateway.Namespace {
			return res[i].Gateway.Namespace < res[j].Gateway.Namespace
		}
		return res[i].Gateway.Name < res[j].Gateway.Name
	})
	return res
}

// ResolvedGateways returns deep copies of all Gateways in the model.
func (cm *CompiledModel) ResolvedGateways() []*gatewayv1.Gateway {
	if cm == nil {
		return nil
	}
	var res []*gatewayv1.Gateway
	for _, cg := range cm.GatewaysList() {
		if cg.Gateway != nil {
			res = append(res, cg.Gateway.DeepCopy())
		}
	}
	return res
}

// GatewayReadiness represents the data-plane readiness state of a Gateway.
type GatewayReadiness struct {
	Ready   bool
	Reason  string
	Message string
}

// ModelInputs contains all inputs required to build a CompiledModel and ComputeOutputs.
type ModelInputs struct {
	Revision              uint64
	Gateways              []*gatewayv1.Gateway
	GatewayClasses        []*gatewayv1.GatewayClass
	GatewayAddresses      map[types.NamespacedName][]gatewayv1.GatewayStatusAddress
	GatewayReadiness      map[types.NamespacedName]GatewayReadiness
	ProvisioningErrors    map[types.NamespacedName]string
	SupportedAddressTypes []gatewayv1.AddressType
	ListenerSets          []*gatewayv1.ListenerSet
	HTTPRoutes            []*gatewayv1.HTTPRoute
	TLSRoutes             []*gatewayv1.TLSRoute
	GRPCRoutes            []*gatewayv1.GRPCRoute
	TCPRoutes             []*gatewayv1.TCPRoute
	UDPRoutes             []*gatewayv1.UDPRoute
	Services              map[types.NamespacedName]*corev1.Service
	BackendTLSPolicies    []*gatewayv1.BackendTLSPolicy
	ConfigMaps            map[types.NamespacedName]*corev1.ConfigMap
	Secrets               map[types.NamespacedName]*corev1.Secret
	Namespaces            map[string]*corev1.Namespace
	ReferenceGrants       map[types.NamespacedName]*gatewayv1beta1.ReferenceGrant
	RefValidator          ReferenceGrantValidator
	ControllerName        string
	AllInOneMode          bool
}

// Outputs represents all computed outputs produced by ComputeOutputs.
type Outputs struct {
	Revision                 uint64
	GatewayStatuses          map[types.NamespacedName]gatewayv1.GatewayStatus
	HTTPRouteStatuses        map[types.NamespacedName]gatewayv1.HTTPRouteStatus
	TLSRouteStatuses         map[types.NamespacedName]gatewayv1.TLSRouteStatus
	GRPCRouteStatuses        map[types.NamespacedName]gatewayv1.GRPCRouteStatus
	TCPRouteStatuses         map[types.NamespacedName]gatewayv1.TCPRouteStatus
	UDPRouteStatuses         map[types.NamespacedName]gatewayv1.UDPRouteStatus
	ListenerSetStatuses      map[types.NamespacedName]gatewayv1.ListenerSetStatus
	BackendTLSPolicyStatuses map[types.NamespacedName]gatewayv1.PolicyStatus
	GatewayClassStatuses     map[types.NamespacedName]gatewayv1.GatewayClassStatus
	ProxyListeners           []InternalListener
	ProxyRoutes              []InternalRoute
	CertificatesMap          map[string]*tls.Certificate
	DefaultCert              *tls.Certificate
	ResolvedGateways         []*gatewayv1.Gateway
	CompiledGateways         map[types.NamespacedName]*CompiledGateway
	DataplaneConfigs         map[types.NamespacedName]*DataplaneConfig
}

func isSupportedProtocol(protocol gatewayv1.ProtocolType) bool {
	switch protocol {
	case gatewayv1.HTTPProtocolType,
		gatewayv1.HTTPSProtocolType,
		gatewayv1.TLSProtocolType,
		gatewayv1.TCPProtocolType,
		gatewayv1.UDPProtocolType:
		return true
	default:
		return false
	}
}

func isValidRouteKindForProtocol(protocol gatewayv1.ProtocolType, group *gatewayv1.Group, kind gatewayv1.Kind) bool {
	grp := ValueOf(group)
	if grp != "" && grp != gatewayv1.GroupName {
		return false
	}
	switch protocol {
	case gatewayv1.HTTPProtocolType, gatewayv1.HTTPSProtocolType:
		return kind == "HTTPRoute" || kind == "GRPCRoute"
	case gatewayv1.TLSProtocolType:
		return kind == "TLSRoute"
	case gatewayv1.TCPProtocolType:
		return kind == "TCPRoute"
	case gatewayv1.UDPProtocolType:
		return kind == "UDPRoute"
	default:
		return false
	}
}

func defaultSupportedKindsForProtocol(protocol gatewayv1.ProtocolType) []gatewayv1.RouteGroupKind {
	switch protocol {
	case gatewayv1.HTTPProtocolType, gatewayv1.HTTPSProtocolType:
		return []gatewayv1.RouteGroupKind{{
			Group: Ptr(gatewayv1.Group(gatewayv1.GroupName)),
			Kind:  gatewayv1.Kind("HTTPRoute"),
		}}
	case gatewayv1.TLSProtocolType:
		return []gatewayv1.RouteGroupKind{{
			Group: Ptr(gatewayv1.Group(gatewayv1.GroupName)),
			Kind:  gatewayv1.Kind("TLSRoute"),
		}}
	case gatewayv1.TCPProtocolType:
		return []gatewayv1.RouteGroupKind{{
			Group: Ptr(gatewayv1.Group(gatewayv1.GroupName)),
			Kind:  gatewayv1.Kind("TCPRoute"),
		}}
	case gatewayv1.UDPProtocolType:
		return []gatewayv1.RouteGroupKind{{
			Group: Ptr(gatewayv1.Group(gatewayv1.GroupName)),
			Kind:  gatewayv1.Kind("UDPRoute"),
		}}
	default:
		return []gatewayv1.RouteGroupKind{}
	}
}

// EffectiveFrontendValidation returns the effective frontend TLS validation configuration for a listener on a Gateway.
// Frontend client certificate validation applies to HTTPS listeners only.
// If a perPort configuration matches the listener port, it overrides the default configuration.
func EffectiveFrontendValidation(gwTLS *gatewayv1.GatewayTLSConfig, port gatewayv1.PortNumber, protocol gatewayv1.ProtocolType) *gatewayv1.FrontendTLSValidation {
	if protocol != gatewayv1.HTTPSProtocolType || gwTLS == nil || gwTLS.Frontend == nil {
		return nil
	}

	for _, pp := range gwTLS.Frontend.PerPort {
		if pp.Port == port {
			return pp.TLS.Validation
		}
	}

	return gwTLS.Frontend.Default.Validation
}

// ValidateListener validates a listener specification, computing its supported kinds, conditions, and client validation.
func ValidateListener(
	listener ListenerSpec,
	owner ListenerOwner,
	gwNamespace string,
	generation int64,
	secrets map[types.NamespacedName]*corev1.Secret,
	configMaps map[types.NamespacedName]*corev1.ConfigMap,
	frontendValidation *gatewayv1.FrontendTLSValidation,
	refValidator ReferenceGrantValidator,
) (supportedKinds []gatewayv1.RouteGroupKind, conditions []metav1.Condition, clientValidation *InternalClientValidation) {
	if gwNamespace == "" {
		gwNamespace = owner.Namespace
	}

	isSupported := isSupportedProtocol(listener.Protocol)

	if !isSupported {
		supportedKinds = []gatewayv1.RouteGroupKind{}
		conditions = []metav1.Condition{
			NewCondition(
				string(gatewayv1.ListenerConditionProgrammed),
				metav1.ConditionFalse,
				string(gatewayv1.ListenerReasonInvalid),
				fmt.Sprintf("Protocol %q is not supported", listener.Protocol),
				generation,
			),
			NewCondition(
				string(gatewayv1.ListenerConditionAccepted),
				metav1.ConditionFalse,
				string(gatewayv1.ListenerReasonUnsupportedProtocol),
				fmt.Sprintf("Protocol %q is not supported", listener.Protocol),
				generation,
			),
		}
		return supportedKinds, conditions, nil
	}

	hasInvalidRouteKind := false
	if listener.AllowedRoutes != nil && len(listener.AllowedRoutes.Kinds) > 0 {
		supportedKinds = []gatewayv1.RouteGroupKind{}
		for _, k := range listener.AllowedRoutes.Kinds {
			if isValidRouteKindForProtocol(listener.Protocol, k.Group, k.Kind) {
				alreadyPresent := false
				for _, sk := range supportedKinds {
					if sk.Kind == k.Kind && ValueOf(sk.Group) == gatewayv1.GroupName {
						alreadyPresent = true
						break
					}
				}
				if !alreadyPresent {
					supportedKinds = append(supportedKinds, gatewayv1.RouteGroupKind{
						Group: Ptr(gatewayv1.Group(gatewayv1.GroupName)),
						Kind:  k.Kind,
					})
				}
			} else {
				hasInvalidRouteKind = true
			}
		}
	} else {
		supportedKinds = defaultSupportedKindsForProtocol(listener.Protocol)
	}

	var tlsInvalidReason string
	var tlsInvalidMessage string
	var tlsProgrammedMessage string

	needsTLSSecretValidation := isSupported && (listener.Protocol == gatewayv1.HTTPSProtocolType || (listener.Protocol == gatewayv1.TLSProtocolType && listener.TLS != nil && (listener.TLS.Mode == nil || *listener.TLS.Mode == gatewayv1.TLSModeTerminate))) && listener.TLS != nil
	if needsTLSSecretValidation {
		if len(listener.TLS.CertificateRefs) == 0 {
			tlsInvalidReason = string(gatewayv1.ListenerReasonInvalidCertificateRef)
			tlsInvalidMessage = "No certificate refs specified"
			tlsProgrammedMessage = "Invalid TLS configuration: no certificate refs specified"
		} else {
			for _, ref := range listener.TLS.CertificateRefs {
				group := ValueOf(ref.Group)
				kind := ValueOf(ref.Kind)
				if group != "" || (kind != "" && kind != "Secret") {
					tlsInvalidReason = string(gatewayv1.ListenerReasonInvalidCertificateRef)
					tlsInvalidMessage = fmt.Sprintf("Unsupported certificate ref group %q kind %q", group, kind)
					tlsProgrammedMessage = fmt.Sprintf("Invalid certificate ref group %q kind %q", group, kind)
					break
				}

				if kind == "" {
					kind = "Secret"
				}

				secretNs := owner.Namespace
				if ref.Namespace != nil && string(*ref.Namespace) != "" {
					secretNs = string(*ref.Namespace)
				}
				secretKey := types.NamespacedName{
					Namespace: secretNs,
					Name:      string(ref.Name),
				}

				if secretKey.Namespace != owner.Namespace {
					from := Reference{
						GroupKind: schema.GroupKind{Group: gatewayv1.GroupName, Kind: string(owner.Kind)},
						Namespace: owner.Namespace,
					}
					to := Reference{
						GroupKind: schema.GroupKind{Group: string(group), Kind: string(kind)},
						Namespace: secretKey.Namespace,
						Name:      secretKey.Name,
					}
					if refValidator == nil || !refValidator.IsReferencePermitted(from, to) {
						tlsInvalidReason = string(gatewayv1.ListenerReasonRefNotPermitted)
						tlsInvalidMessage = fmt.Sprintf("Cross-namespace reference to %s/%s is not permitted by any ReferenceGrant", secretKey.Namespace, secretKey.Name)
						tlsProgrammedMessage = tlsInvalidMessage
						break
					}
				}

				if secrets != nil {
					secret, ok := secrets[secretKey]
					if !ok || secret == nil {
						tlsInvalidReason = string(gatewayv1.ListenerReasonInvalidCertificateRef)
						tlsInvalidMessage = fmt.Sprintf("Secret %s/%s not found", secretKey.Namespace, secretKey.Name)
						tlsProgrammedMessage = tlsInvalidMessage
						break
					}

					certBytes := secret.Data[corev1.TLSCertKey]
					keyBytes := secret.Data[corev1.TLSPrivateKeyKey]
					if len(certBytes) == 0 || len(keyBytes) == 0 {
						tlsInvalidReason = string(gatewayv1.ListenerReasonInvalidCertificateRef)
						tlsInvalidMessage = fmt.Sprintf("Secret %s/%s is missing tls.crt or tls.key", secretKey.Namespace, secretKey.Name)
						tlsProgrammedMessage = tlsInvalidMessage
						break
					}

					if _, err := tls.X509KeyPair(certBytes, keyBytes); err != nil {
						tlsInvalidReason = string(gatewayv1.ListenerReasonInvalidCertificateRef)
						tlsInvalidMessage = fmt.Sprintf("Secret %s/%s contains invalid certificate or key: %v", secretKey.Namespace, secretKey.Name, err)
						tlsProgrammedMessage = tlsInvalidMessage
						break
					}
				}
			}
		}
	}

	var caInvalidReason string
	var caInvalidMessage string
	var caPEMs [][]byte

	if listener.Protocol == gatewayv1.HTTPSProtocolType && frontendValidation != nil {
		if len(frontendValidation.CACertificateRefs) == 0 {
			caInvalidReason = string(gatewayv1.ListenerReasonInvalidCACertificateRef)
			caInvalidMessage = "No CA certificate refs specified"
		} else {
			for _, caRef := range frontendValidation.CACertificateRefs {
				group := string(caRef.Group)
				kind := string(caRef.Kind)
				if group != "" || kind != "ConfigMap" {
					caInvalidReason = string(gatewayv1.ListenerReasonInvalidCACertificateKind)
					caInvalidMessage = fmt.Sprintf("Unsupported CA certificate ref group %q kind %q", group, kind)
					break
				}

				targetNs := gwNamespace
				if caRef.Namespace != nil && string(*caRef.Namespace) != "" {
					targetNs = string(*caRef.Namespace)
				}

				if targetNs != gwNamespace {
					from := Reference{
						GroupKind: schema.GroupKind{Group: gatewayv1.GroupName, Kind: "Gateway"},
						Namespace: gwNamespace,
					}
					to := Reference{
						GroupKind: schema.GroupKind{Group: "", Kind: "ConfigMap"},
						Namespace: targetNs,
						Name:      string(caRef.Name),
					}
					if refValidator == nil || !refValidator.IsReferencePermitted(from, to) {
						caInvalidReason = string(gatewayv1.ListenerReasonRefNotPermitted)
						caInvalidMessage = fmt.Sprintf("Cross-namespace reference to ConfigMap %s/%s is not permitted by any ReferenceGrant", targetNs, caRef.Name)
						break
					}
				}

				cmKey := types.NamespacedName{
					Namespace: targetNs,
					Name:      string(caRef.Name),
				}
				var cm *corev1.ConfigMap
				if configMaps != nil {
					cm = configMaps[cmKey]
				}
				if cm == nil {
					caInvalidReason = string(gatewayv1.ListenerReasonInvalidCACertificateRef)
					caInvalidMessage = fmt.Sprintf("ConfigMap %s/%s not found", targetNs, caRef.Name)
					break
				}

				data, err := ExtractCACertFromConfigMap(cm)
				if err != nil {
					caInvalidReason = string(gatewayv1.ListenerReasonInvalidCACertificateRef)
					caInvalidMessage = fmt.Sprintf("ConfigMap %s/%s is invalid: %v", targetNs, caRef.Name, err)
					break
				}
				caPEMs = append(caPEMs, data)
			}
		}
	}

	listenerProgrammedStatus := metav1.ConditionTrue
	listenerProgrammedReason := gatewayv1.ListenerReasonProgrammed
	listenerProgrammedMessage := "Listener programmed"

	listenerAcceptedStatus := metav1.ConditionTrue
	listenerAcceptedReason := gatewayv1.ListenerReasonAccepted
	listenerAcceptedMessage := "Listener accepted"

	resolvedRefsStatus := metav1.ConditionTrue
	resolvedRefsReason := gatewayv1.ListenerReasonResolvedRefs
	resolvedRefsMessage := "All references resolved"

	if hasInvalidRouteKind {
		resolvedRefsStatus = metav1.ConditionFalse
		resolvedRefsReason = gatewayv1.ListenerReasonInvalidRouteKinds
		resolvedRefsMessage = "One or more route kinds are not supported"

		listenerProgrammedStatus = metav1.ConditionFalse
		listenerProgrammedReason = gatewayv1.ListenerReasonInvalid
		listenerProgrammedMessage = "One or more route kinds are not supported"
	} else if tlsInvalidReason != "" {
		resolvedRefsStatus = metav1.ConditionFalse
		resolvedRefsReason = gatewayv1.ListenerConditionReason(tlsInvalidReason)
		resolvedRefsMessage = tlsInvalidMessage

		listenerProgrammedStatus = metav1.ConditionFalse
		listenerProgrammedReason = gatewayv1.ListenerReasonInvalid
		listenerProgrammedMessage = tlsProgrammedMessage
	} else if caInvalidReason != "" {
		resolvedRefsStatus = metav1.ConditionFalse
		resolvedRefsReason = gatewayv1.ListenerConditionReason(caInvalidReason)
		resolvedRefsMessage = caInvalidMessage

		listenerAcceptedStatus = metav1.ConditionFalse
		listenerAcceptedReason = gatewayv1.ListenerReasonNoValidCACertificate
		listenerAcceptedMessage = "No valid CA certificate for client certificate validation"

		listenerProgrammedStatus = metav1.ConditionFalse
		listenerProgrammedReason = gatewayv1.ListenerReasonNoValidCACertificate
		listenerProgrammedMessage = "No valid CA certificate for client certificate validation"
	} else if len(caPEMs) > 0 {
		mode := frontendValidation.Mode
		if mode == "" {
			mode = gatewayv1.AllowValidOnly
		}
		clientValidation = &InternalClientValidation{
			CACertPEMs: caPEMs,
			Mode:       mode,
		}
	}

	conditions = []metav1.Condition{
		NewCondition(
			string(gatewayv1.ListenerConditionProgrammed),
			listenerProgrammedStatus,
			string(listenerProgrammedReason),
			listenerProgrammedMessage,
			generation,
		),
		NewCondition(
			string(gatewayv1.ListenerConditionAccepted),
			listenerAcceptedStatus,
			string(listenerAcceptedReason),
			listenerAcceptedMessage,
			generation,
		),
		NewCondition(
			string(gatewayv1.ListenerConditionResolvedRefs),
			resolvedRefsStatus,
			string(resolvedRefsReason),
			resolvedRefsMessage,
			generation,
		),
	}

	return supportedKinds, conditions, clientValidation
}

// BuildEffectiveListener constructs an EffectiveListener from a listener specification.
func BuildEffectiveListener(
	listener ListenerSpec,
	owner ListenerOwner,
	parentGateway types.NamespacedName,
	generation int64,
	secrets map[types.NamespacedName]*corev1.Secret,
	configMaps map[types.NamespacedName]*corev1.ConfigMap,
	frontendValidation *gatewayv1.FrontendTLSValidation,
	refValidator ReferenceGrantValidator,
) *EffectiveListener {
	supportedKinds, conditions, clientValidation := ValidateListener(
		listener,
		owner,
		parentGateway.Namespace,
		generation,
		secrets,
		configMaps,
		frontendValidation,
		refValidator,
	)
	return &EffectiveListener{
		Owner:            owner,
		ParentGateway:    parentGateway,
		Name:             listener.Name,
		Port:             listener.Port,
		Protocol:         listener.Protocol,
		Hostname:         listener.Hostname,
		TLS:              listener.TLS,
		AllowedRoutes:    listener.AllowedRoutes,
		SupportedKinds:   supportedKinds,
		Conditions:       conditions,
		ClientValidation: clientValidation,
		AttachedRoutes:   0,
		Routes:           nil,
		Generation:       generation,
	}
}

// IsAddressTypeSupported returns true if the specified address type is in the supported list.
func IsAddressTypeSupported(addrType gatewayv1.AddressType, supported []gatewayv1.AddressType) bool {
	return slices.Contains(supported, addrType)
}

// HasUnsupportedAddress returns true and the first unsupported address type if any address
// in the provided slice is not supported by the provider.
// If addr.Type is nil or empty, it defaults to gatewayv1.IPAddressType.
func HasUnsupportedAddress(addresses []gatewayv1.GatewaySpecAddress, supported []gatewayv1.AddressType) (bool, gatewayv1.AddressType) {
	for _, addr := range addresses {
		addrType := gatewayv1.IPAddressType
		if addr.Type != nil && *addr.Type != "" {
			addrType = *addr.Type
		}
		if !IsAddressTypeSupported(addrType, supported) {
			return true, addrType
		}
	}
	return false, ""
}

// computeDesiredAddresses computes the addresses to report in status.addresses.
// If specific addresses (with values) are requested in spec.addresses, only bound requested
// addresses matching the requested type are reported.
// If {type: IPAddress} with no value is requested, or if no addresses are requested, all provided
// addresses matching the requested type(s) (or all provided addresses if none requested) are reported.
func computeDesiredAddresses(specAddresses []gatewayv1.GatewaySpecAddress, providedAddresses []gatewayv1.GatewayStatusAddress) []gatewayv1.GatewayStatusAddress {
	if len(specAddresses) == 0 {
		return providedAddresses
	}

	var requestedWithValues []gatewayv1.GatewaySpecAddress
	hasEmptyValue := false
	for _, sa := range specAddresses {
		if sa.Value != "" {
			requestedWithValues = append(requestedWithValues, sa)
		} else {
			hasEmptyValue = true
		}
	}

	if len(requestedWithValues) == 0 {
		return providedAddresses
	}

	var result []gatewayv1.GatewayStatusAddress
	for _, req := range requestedWithValues {
		reqType := gatewayv1.IPAddressType
		if req.Type != nil && *req.Type != "" {
			reqType = *req.Type
		}
		for _, prov := range providedAddresses {
			provType := gatewayv1.IPAddressType
			if prov.Type != nil && *prov.Type != "" {
				provType = *prov.Type
			}
			if prov.Value == req.Value && provType == reqType {
				t := req.Type
				if t == nil {
					t = Ptr(gatewayv1.IPAddressType)
				}
				result = append(result, gatewayv1.GatewayStatusAddress{
					Type:  t,
					Value: req.Value,
				})
				break
			}
		}
	}

	if hasEmptyValue {
		for _, prov := range providedAddresses {
			alreadyPresent := false
			for _, r := range result {
				if r.Value == prov.Value {
					alreadyPresent = true
					break
				}
			}
			if !alreadyPresent {
				result = append(result, prov)
			}
		}
	}

	return result
}

// resolveGatewayBackendClientCert validates and extracts the client certificate
// referenced by a Gateway's backend TLS configuration, returning the parsed certificate
// (if valid) and the ResolvedRefs condition for the Gateway.
func resolveGatewayBackendClientCert(
	gw *gatewayv1.Gateway,
	secrets map[types.NamespacedName]*corev1.Secret,
	refValidator ReferenceGrantValidator,
) (*tls.Certificate, metav1.Condition) {
	if gw == nil || gw.Spec.TLS == nil || gw.Spec.TLS.Backend == nil || gw.Spec.TLS.Backend.ClientCertificateRef == nil {
		gen := int64(0)
		if gw != nil {
			gen = gw.Generation
		}
		return nil, NewCondition(
			string(gatewayv1.GatewayConditionResolvedRefs),
			metav1.ConditionTrue,
			string(gatewayv1.GatewayReasonResolvedRefs),
			"Resolved references",
			gen,
		)
	}

	ref := gw.Spec.TLS.Backend.ClientCertificateRef
	group := ValueOf(ref.Group)
	kind := ValueOf(ref.Kind)
	if kind == "" {
		kind = "Secret"
	}

	if group != "" || kind != "Secret" {
		return nil, NewCondition(
			string(gatewayv1.GatewayConditionResolvedRefs),
			metav1.ConditionFalse,
			string(gatewayv1.GatewayReasonInvalidClientCertificateRef),
			fmt.Sprintf("Unsupported client certificate ref group %q kind %q", group, kind),
			gw.Generation,
		)
	}

	secretNs := gw.Namespace
	if ref.Namespace != nil && string(*ref.Namespace) != "" {
		secretNs = string(*ref.Namespace)
	}
	secretKey := types.NamespacedName{
		Namespace: secretNs,
		Name:      string(ref.Name),
	}

	if secretKey.Namespace != gw.Namespace {
		from := Reference{
			GroupKind: schema.GroupKind{Group: gatewayv1.GroupName, Kind: "Gateway"},
			Namespace: gw.Namespace,
		}
		to := Reference{
			GroupKind: schema.GroupKind{Group: string(group), Kind: string(kind)},
			Namespace: secretKey.Namespace,
			Name:      secretKey.Name,
		}
		if refValidator == nil || !refValidator.IsReferencePermitted(from, to) {
			return nil, NewCondition(
				string(gatewayv1.GatewayConditionResolvedRefs),
				metav1.ConditionFalse,
				string(gatewayv1.GatewayReasonRefNotPermitted),
				fmt.Sprintf("Cross-namespace reference to %s/%s is not permitted by any ReferenceGrant", secretKey.Namespace, secretKey.Name),
				gw.Generation,
			)
		}
	}

	var secret *corev1.Secret
	if secrets != nil {
		secret = secrets[secretKey]
	}
	if secret == nil {
		return nil, NewCondition(
			string(gatewayv1.GatewayConditionResolvedRefs),
			metav1.ConditionFalse,
			string(gatewayv1.GatewayReasonInvalidClientCertificateRef),
			fmt.Sprintf("Secret %s/%s not found", secretKey.Namespace, secretKey.Name),
			gw.Generation,
		)
	}

	certBytes := secret.Data[corev1.TLSCertKey]
	keyBytes := secret.Data[corev1.TLSPrivateKeyKey]
	if len(certBytes) == 0 || len(keyBytes) == 0 {
		return nil, NewCondition(
			string(gatewayv1.GatewayConditionResolvedRefs),
			metav1.ConditionFalse,
			string(gatewayv1.GatewayReasonInvalidClientCertificateRef),
			fmt.Sprintf("Secret %s/%s is missing tls.crt or tls.key", secretKey.Namespace, secretKey.Name),
			gw.Generation,
		)
	}

	tlsCert, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		return nil, NewCondition(
			string(gatewayv1.GatewayConditionResolvedRefs),
			metav1.ConditionFalse,
			string(gatewayv1.GatewayReasonInvalidClientCertificateRef),
			fmt.Sprintf("Secret %s/%s contains invalid certificate or key: %v", secretKey.Namespace, secretKey.Name, err),
			gw.Generation,
		)
	}

	return &tlsCert, NewCondition(
		string(gatewayv1.GatewayConditionResolvedRefs),
		metav1.ConditionTrue,
		string(gatewayv1.GatewayReasonResolvedRefs),
		"Resolved references",
		gw.Generation,
	)
}

// ComputeGatewayConditions computes the top-level conditions for a Gateway.
func ComputeGatewayConditions(
	gw *gatewayv1.Gateway,
	effectiveListeners []*EffectiveListener,
	hasAddress bool,
	readiness GatewayReadiness,
	provErr string,
	supportedAddressTypes []gatewayv1.AddressType,
	resolvedRefsCond metav1.Condition,
) []metav1.Condition {
	totalListeners := len(gw.Spec.Listeners)
	acceptedListenersCount := 0

	for _, el := range effectiveListeners {
		if el.Owner.Kind == "Gateway" {
			if el.IsAccepted() {
				acceptedListenersCount++
			}
		}
	}

	gwAcceptedStatus := metav1.ConditionTrue
	gwAcceptedReason := gatewayv1.GatewayReasonAccepted
	gwAcceptedMessage := "Gateway accepted by reference implementation"

	if gw.Spec.Infrastructure != nil && gw.Spec.Infrastructure.ParametersRef != nil {
		gwAcceptedStatus = metav1.ConditionFalse
		gwAcceptedReason = gatewayv1.GatewayReasonInvalidParameters
		gwAcceptedMessage = "Invalid infrastructure parametersRef: parametersRef is not supported"
	} else if unsupp, unsuppType := HasUnsupportedAddress(gw.Spec.Addresses, supportedAddressTypes); unsupp {
		gwAcceptedStatus = metav1.ConditionFalse
		gwAcceptedReason = gatewayv1.GatewayReasonUnsupportedAddress
		gwAcceptedMessage = fmt.Sprintf("Address type %q is not supported", unsuppType)
	} else if totalListeners == 0 {
		gwAcceptedStatus = metav1.ConditionFalse
		gwAcceptedReason = gatewayv1.GatewayReasonListenersNotValid
		gwAcceptedMessage = "No listeners configured on Gateway"
	} else if acceptedListenersCount == 0 {
		gwAcceptedStatus = metav1.ConditionFalse
		gwAcceptedReason = gatewayv1.GatewayReasonListenersNotValid
		gwAcceptedMessage = "No listeners are accepted"
	} else if acceptedListenersCount < totalListeners {
		gwAcceptedStatus = metav1.ConditionTrue
		gwAcceptedReason = gatewayv1.GatewayReasonListenersNotValid
		gwAcceptedMessage = "One or more listeners have invalid configuration"
	}

	gwProgrammedStatus := metav1.ConditionTrue
	gwProgrammedReason := gatewayv1.GatewayReasonProgrammed
	gwProgrammedMessage := "Gateway programmed by reference implementation"

	if gwAcceptedStatus == metav1.ConditionFalse {
		gwProgrammedStatus = metav1.ConditionFalse
		gwProgrammedReason = gatewayv1.GatewayReasonInvalid
		gwProgrammedMessage = "Gateway is not accepted"
	} else if provErr != "" {
		gwProgrammedStatus = metav1.ConditionFalse
		gwProgrammedReason = gatewayv1.GatewayReasonPending
		gwProgrammedMessage = provErr
	} else if !readiness.Ready && readiness.Reason == string(gatewayv1.GatewayReasonAddressNotUsable) {
		gwProgrammedStatus = metav1.ConditionFalse
		gwProgrammedReason = gatewayv1.GatewayReasonAddressNotUsable
		gwProgrammedMessage = "Requested address cannot be used"
		if readiness.Message != "" {
			gwProgrammedMessage = readiness.Message
		}
	} else if !hasAddress {
		gwProgrammedStatus = metav1.ConditionFalse
		gwProgrammedReason = gatewayv1.GatewayReasonAddressNotAssigned
		gwProgrammedMessage = "Waiting for address to be assigned to the Gateway"
	} else if !readiness.Ready {
		gwProgrammedStatus = metav1.ConditionFalse
		gwProgrammedReason = gatewayv1.GatewayReasonPending
		gwProgrammedMessage = "Waiting for data-plane deployment to become available"
		if readiness.Reason != "" {
			gwProgrammedReason = gatewayv1.GatewayConditionReason(readiness.Reason)
		}
		if readiness.Message != "" {
			gwProgrammedMessage = readiness.Message
		}
	}

	if resolvedRefsCond.Type == "" {
		gen := int64(0)
		if gw != nil {
			gen = gw.Generation
		}
		resolvedRefsCond = NewCondition(
			string(gatewayv1.GatewayConditionResolvedRefs),
			metav1.ConditionTrue,
			string(gatewayv1.GatewayReasonResolvedRefs),
			"Resolved references",
			gen,
		)
	}

	conds := []metav1.Condition{
		NewCondition(
			string(gatewayv1.GatewayConditionProgrammed),
			gwProgrammedStatus,
			string(gwProgrammedReason),
			gwProgrammedMessage,
			gw.Generation,
		),
		NewCondition(
			string(gatewayv1.GatewayConditionAccepted),
			gwAcceptedStatus,
			string(gwAcceptedReason),
			gwAcceptedMessage,
			gw.Generation,
		),
		resolvedRefsCond,
	}

	if hasInsecureFallbackListener(gw, effectiveListeners) {
		conds = append(conds, NewCondition(
			string(gatewayv1.GatewayConditionInsecureFrontendValidationMode),
			metav1.ConditionTrue,
			string(gatewayv1.GatewayReasonConfigurationChanged),
			"Frontend validation mode is set to AllowInsecureFallback",
			gw.Generation,
		))
	}

	return conds
}

func hasInsecureFallbackListener(gw *gatewayv1.Gateway, effectiveListeners []*EffectiveListener) bool {
	for _, el := range effectiveListeners {
		if el.ClientValidation != nil && el.ClientValidation.Mode == gatewayv1.AllowInsecureFallback {
			return true
		}
		if gw != nil && el.Protocol == gatewayv1.HTTPSProtocolType {
			if effVal := EffectiveFrontendValidation(gw.Spec.TLS, el.Port, el.Protocol); effVal != nil && effVal.Mode == gatewayv1.AllowInsecureFallback {
				return true
			}
		}
	}
	return false
}

// areProtocolsCompatible returns true if two listeners on the same port can co-exist.
// HTTP listeners can share a port with other HTTP listeners (differentiated by hostname).
// HTTPS listeners can share a port with other HTTPS listeners (differentiated by SNI/hostname).
// HTTPS and TLS listeners can also share a port (both routed by SNI).
func areProtocolsCompatible(p1, p2 gatewayv1.ProtocolType) bool {
	if p1 == p2 {
		if p1 == gatewayv1.HTTPProtocolType || p1 == gatewayv1.HTTPSProtocolType || p1 == gatewayv1.TLSProtocolType {
			return true
		}
		return false
	}
	if (p1 == gatewayv1.HTTPSProtocolType && p2 == gatewayv1.TLSProtocolType) ||
		(p1 == gatewayv1.TLSProtocolType && p2 == gatewayv1.HTTPSProtocolType) {
		return true
	}
	return false
}

func markListenerConflicted(el *EffectiveListener, reason gatewayv1.ListenerConditionReason, message string) {
	SetCondition(&el.Conditions, NewCondition(
		string(gatewayv1.ListenerConditionAccepted),
		metav1.ConditionFalse,
		string(reason),
		message,
		el.Generation,
	))
	SetCondition(&el.Conditions, NewCondition(
		string(gatewayv1.ListenerConditionProgrammed),
		metav1.ConditionFalse,
		string(reason),
		message,
		el.Generation,
	))
	SetCondition(&el.Conditions, NewCondition(
		string(gatewayv1.ListenerConditionConflicted),
		metav1.ConditionTrue,
		string(reason),
		message,
		el.Generation,
	))
}

// CompileModel compiles the effective listeners, route bindings, and statuses across all Gateways and ListenerSets.
func CompileModel(inputs ModelInputs) *CompiledModel {
	refValidator := inputs.RefValidator
	if refValidator == nil && len(inputs.ReferenceGrants) > 0 {
		refValidator = mapReferenceValidator{referenceGrants: inputs.ReferenceGrants}
	}

	cm := &CompiledModel{
		Gateways:     make(map[types.NamespacedName]*CompiledGateway),
		HTTPRoutes:   make(map[types.NamespacedName]*CompiledRoute),
		TLSRoutes:    make(map[types.NamespacedName]*CompiledTLSRoute),
		GRPCRoutes:   make(map[types.NamespacedName]*CompiledGRPCRoute),
		TCPRoutes:    make(map[types.NamespacedName]*CompiledTCPRoute),
		UDPRoutes:    make(map[types.NamespacedName]*CompiledUDPRoute),
		ListenerSets: make(map[types.NamespacedName]*gatewayv1.ListenerSet),
	}

	for _, ls := range inputs.ListenerSets {
		if ls != nil {
			cm.ListenerSets[types.NamespacedName{Namespace: ls.Namespace, Name: ls.Name}] = ls
		}
	}

	managedGatewayClasses := make(map[string]bool)
	if inputs.ControllerName != "" {
		for _, gc := range inputs.GatewayClasses {
			if gc != nil && string(gc.Spec.ControllerName) == inputs.ControllerName {
				managedGatewayClasses[gc.Name] = true
			}
		}
	}

	// 1. Compile each Gateway and its effective listeners in precedence order
	sortedGateways := make([]*gatewayv1.Gateway, len(inputs.Gateways))
	copy(sortedGateways, inputs.Gateways)
	sort.Slice(sortedGateways, func(i, j int) bool {
		if !sortedGateways[i].CreationTimestamp.Equal(&sortedGateways[j].CreationTimestamp) {
			return sortedGateways[i].CreationTimestamp.Before(&sortedGateways[j].CreationTimestamp)
		}
		if sortedGateways[i].Namespace != sortedGateways[j].Namespace {
			return sortedGateways[i].Namespace < sortedGateways[j].Namespace
		}
		return sortedGateways[i].Name < sortedGateways[j].Name
	})

	boundTCPPorts := make(map[gatewayv1.PortNumber]string)
	boundUDPPorts := make(map[gatewayv1.PortNumber]string)

	for _, gw := range sortedGateways {
		if gw == nil {
			continue
		}
		if inputs.ControllerName != "" && !managedGatewayClasses[string(gw.Spec.GatewayClassName)] {
			continue
		}
		gwKey := types.NamespacedName{Namespace: gw.Namespace, Name: gw.Name}
		cg := &CompiledGateway{
			Gateway: gw,
		}

		// Gateway's own listeners (highest precedence)
		for _, l := range gw.Spec.Listeners {
			frontendVal := EffectiveFrontendValidation(gw.Spec.TLS, l.Port, l.Protocol)
			el := BuildEffectiveListener(
				ListenerToSpec(l),
				ListenerOwner{Kind: "Gateway", Namespace: gw.Namespace, Name: gw.Name},
				gwKey,
				gw.Generation,
				inputs.Secrets,
				inputs.ConfigMaps,
				frontendVal,
				refValidator,
			)
			cg.EffectiveListeners = append(cg.EffectiveListeners, el)
		}

		// Allowed ListenerSets in precedence order:
		// 1. Creation time (oldest first)
		// 2. Alphabetically by "{namespace}/{name}"
		var allowedListenerSets []*gatewayv1.ListenerSet
		for _, ls := range inputs.ListenerSets {
			if ls == nil {
				continue
			}
			if IsListenerSetParent(ls, gw) && IsListenerSetAllowed(ls, gw, inputs.Namespaces) {
				allowedListenerSets = append(allowedListenerSets, ls)
			}
		}

		sort.Slice(allowedListenerSets, func(i, j int) bool {
			if !allowedListenerSets[i].CreationTimestamp.Equal(&allowedListenerSets[j].CreationTimestamp) {
				return allowedListenerSets[i].CreationTimestamp.Before(&allowedListenerSets[j].CreationTimestamp)
			}
			if allowedListenerSets[i].Namespace != allowedListenerSets[j].Namespace {
				return allowedListenerSets[i].Namespace < allowedListenerSets[j].Namespace
			}
			return allowedListenerSets[i].Name < allowedListenerSets[j].Name
		})

		for _, ls := range allowedListenerSets {
			for _, l := range ls.Spec.Listeners {
				frontendVal := EffectiveFrontendValidation(gw.Spec.TLS, l.Port, l.Protocol)
				el := BuildEffectiveListener(
					ListenerEntryToSpec(l),
					ListenerOwner{Kind: "ListenerSet", Namespace: ls.Namespace, Name: ls.Name},
					gwKey,
					ls.Generation,
					inputs.Secrets,
					inputs.ConfigMaps,
					frontendVal,
					refValidator,
				)
				cg.EffectiveListeners = append(cg.EffectiveListeners, el)
			}
		}

		// Conflict detection pass over all effective listeners on this Gateway in precedence order
		for i := 0; i < len(cg.EffectiveListeners); i++ {
			elCur := cg.EffectiveListeners[i]
			if elCur.IsConflicted() {
				continue
			}

			// Port conflicts with reserved proxy ports 8000/8443
			if elCur.Protocol == gatewayv1.TCPProtocolType && (elCur.Port == 8000 || elCur.Port == 8443) {
				msg := fmt.Sprintf("TCP listener port %d conflicts with reserved proxy port %d", elCur.Port, elCur.Port)
				markListenerConflicted(elCur, gatewayv1.ListenerReasonProtocolConflict, msg)
				continue
			}
			if elCur.Protocol == gatewayv1.UDPProtocolType && elCur.Port == 8443 {
				msg := fmt.Sprintf("UDP listener port %d conflicts with reserved proxy port %d", elCur.Port, elCur.Port)
				markListenerConflicted(elCur, gatewayv1.ListenerReasonProtocolConflict, msg)
				continue
			}

			for j := 0; j < i; j++ {
				elPrev := cg.EffectiveListeners[j]
				if elPrev.IsConflicted() {
					continue
				}
				if elPrev.Port != elCur.Port {
					continue
				}

				// Check protocol compatibility
				if !areProtocolsCompatible(elPrev.Protocol, elCur.Protocol) {
					var msg string
					if elPrev.Protocol == elCur.Protocol {
						msg = fmt.Sprintf("Multiple %q listeners cannot share port %d without hostname/SNI routing (conflicts with %q)", elCur.Protocol, elCur.Port, elPrev.QualifiedName())
					} else {
						msg = fmt.Sprintf("Protocol %q conflicts with higher-precedence listener %q protocol %q on port %d", elCur.Protocol, elPrev.QualifiedName(), elPrev.Protocol, elCur.Port)
					}
					markListenerConflicted(elCur, gatewayv1.ListenerReasonProtocolConflict, msg)
					break
				}

				// Check hostname conflict for compatible protocols
				hPrev := strings.ToLower(string(ValueOf(elPrev.Hostname)))
				hCur := strings.ToLower(string(ValueOf(elCur.Hostname)))
				if hPrev == hCur {
					msg := fmt.Sprintf("Hostname %q conflicts with higher-precedence listener %q on port %d", hCur, elPrev.QualifiedName(), elCur.Port)
					markListenerConflicted(elCur, gatewayv1.ListenerReasonHostnameConflict, msg)
					break
				}
			}
		}

		// In all-in-one mode, two Gateways with the same TCP/UDP port cannot both bind; first wins and second reports Programmed=False.
		if inputs.AllInOneMode {
			for _, el := range cg.EffectiveListeners {
				if el.Protocol == gatewayv1.TCPProtocolType && el.IsAccepted() && !el.IsConflicted() {
					if firstGW, exists := boundTCPPorts[el.Port]; exists {
						SetCondition(&el.Conditions, NewCondition(
							string(gatewayv1.ListenerConditionProgrammed),
							metav1.ConditionFalse,
							string(gatewayv1.ListenerReasonPortUnavailable),
							fmt.Sprintf("Port %d is already bound by Gateway %s in all-in-one mode", el.Port, firstGW),
							el.Generation,
						))
					} else {
						boundTCPPorts[el.Port] = gwKey.String()
					}
				}
				if el.Protocol == gatewayv1.UDPProtocolType && el.IsAccepted() && !el.IsConflicted() {
					if firstGW, exists := boundUDPPorts[el.Port]; exists {
						SetCondition(&el.Conditions, NewCondition(
							string(gatewayv1.ListenerConditionProgrammed),
							metav1.ConditionFalse,
							string(gatewayv1.ListenerReasonPortUnavailable),
							fmt.Sprintf("Port %d is already bound by Gateway %s in all-in-one mode", el.Port, firstGW),
							el.Generation,
						))
					} else {
						boundUDPPorts[el.Port] = gwKey.String()
					}
				}
			}
		}

		// Count only accepted ListenerSets (where at least one listener is valid/programmed and unconflicted)
		acceptedLSCount := int32(0)
		for _, ls := range allowedListenerSets {
			validCount := 0
			for _, el := range cg.EffectiveListeners {
				if el.Owner.Kind == "ListenerSet" && el.Owner.Namespace == ls.Namespace && el.Owner.Name == ls.Name {
					if el.IsAccepted() && el.IsProgrammed() && !el.IsConflicted() {
						validCount++
					}
				}
			}
			if validCount > 0 {
				acceptedLSCount++
			}
		}
		cg.AttachedListenerSets = acceptedLSCount

		hasAddress := false
		if inputs.GatewayAddresses != nil {
			hasAddress = len(inputs.GatewayAddresses[gwKey]) > 0
		}
		var provErr string
		if inputs.ProvisioningErrors != nil {
			provErr = inputs.ProvisioningErrors[gwKey]
		}
		clientCert, resolvedRefsCond := resolveGatewayBackendClientCert(gw, inputs.Secrets, refValidator)
		cg.BackendClientCert = clientCert
		cg.ResolvedRefsCondition = resolvedRefsCond
		cg.Conditions = ComputeGatewayConditions(gw, cg.EffectiveListeners, hasAddress, GatewayReadiness{Ready: true}, provErr, inputs.SupportedAddressTypes, resolvedRefsCond)
		cm.Gateways[gwKey] = cg
	}

	// 2. Compile HTTPRoutes and perform route binding
	for _, route := range inputs.HTTPRoutes {
		if route == nil {
			continue
		}
		routeKey := types.NamespacedName{Namespace: route.Namespace, Name: route.Name}
		rs := &HTTPRouteState{HTTPRoute: route}
		rs.Compile(inputs.Services, inputs.BackendTLSPolicies, inputs.ConfigMaps, refValidator)

		parentConditions := make([]metav1.Condition, len(route.Spec.ParentRefs))
		boundListenersForRoute := make(map[*EffectiveListener]bool)

		for pIdx, parentRef := range route.Spec.ParentRefs {
			parentConditions[pIdx] = bindRouteParentRef(
				route,
				rs,
				parentRef,
				cm,
				inputs.Namespaces,
				boundListenersForRoute,
			)
		}

		cm.HTTPRoutes[routeKey] = &CompiledRoute{
			HTTPRoute:        route,
			RouteState:       rs,
			ParentConditions: parentConditions,
		}
	}

	// 3. Compile TLSRoutes and perform route binding
	for _, route := range inputs.TLSRoutes {
		if route == nil {
			continue
		}
		routeKey := types.NamespacedName{Namespace: route.Namespace, Name: route.Name}
		rs := &TLSRouteState{TLSRoute: route}
		rs.Compile(inputs.Services, refValidator)

		parentConditions := make([]metav1.Condition, len(route.Spec.ParentRefs))
		boundListenersForRoute := make(map[*EffectiveListener]bool)

		for pIdx, parentRef := range route.Spec.ParentRefs {
			parentConditions[pIdx] = bindTLSRouteParentRef(
				route,
				rs,
				parentRef,
				cm,
				inputs.Namespaces,
				boundListenersForRoute,
			)
		}

		cm.TLSRoutes[routeKey] = &CompiledTLSRoute{
			TLSRoute:         route,
			RouteState:       rs,
			ParentConditions: parentConditions,
		}
	}

	// 4. Compile GRPCRoutes and perform route binding
	for _, route := range inputs.GRPCRoutes {
		if route == nil {
			continue
		}
		routeKey := types.NamespacedName{Namespace: route.Namespace, Name: route.Name}
		rs := &GRPCRouteState{GRPCRoute: route}
		rs.Compile(inputs.Services, inputs.BackendTLSPolicies, inputs.ConfigMaps, refValidator)

		parentConditions := make([]metav1.Condition, len(route.Spec.ParentRefs))
		boundListenersForRoute := make(map[*EffectiveListener]bool)

		for pIdx, parentRef := range route.Spec.ParentRefs {
			parentConditions[pIdx] = bindGRPCRouteParentRef(
				route,
				rs,
				parentRef,
				cm,
				inputs.Namespaces,
				boundListenersForRoute,
			)
		}

		cm.GRPCRoutes[routeKey] = &CompiledGRPCRoute{
			GRPCRoute:        route,
			RouteState:       rs,
			ParentConditions: parentConditions,
		}
	}

	// 5. Compile TCPRoutes and perform route binding
	for _, route := range inputs.TCPRoutes {
		if route == nil {
			continue
		}
		routeKey := types.NamespacedName{Namespace: route.Namespace, Name: route.Name}
		rs := &TCPRouteState{TCPRoute: route}
		rs.Compile(inputs.Services, refValidator)

		parentConditions := make([]metav1.Condition, len(route.Spec.ParentRefs))
		boundListenersForRoute := make(map[*EffectiveListener]bool)

		for pIdx, parentRef := range route.Spec.ParentRefs {
			parentConditions[pIdx] = bindTCPRouteParentRef(
				route,
				rs,
				parentRef,
				cm,
				inputs.Namespaces,
				boundListenersForRoute,
			)
		}

		cm.TCPRoutes[routeKey] = &CompiledTCPRoute{
			TCPRoute:         route,
			RouteState:       rs,
			ParentConditions: parentConditions,
		}
	}

	// 6. Compile UDPRoutes and perform route binding
	for _, route := range inputs.UDPRoutes {
		if route == nil {
			continue
		}
		routeKey := types.NamespacedName{Namespace: route.Namespace, Name: route.Name}
		rs := &UDPRouteState{UDPRoute: route}
		rs.Compile(inputs.Services, refValidator)

		parentConditions := make([]metav1.Condition, len(route.Spec.ParentRefs))
		boundListenersForRoute := make(map[*EffectiveListener]bool)

		for pIdx, parentRef := range route.Spec.ParentRefs {
			parentConditions[pIdx] = bindUDPRouteParentRef(
				route,
				rs,
				parentRef,
				cm,
				inputs.Namespaces,
				boundListenersForRoute,
			)
		}

		cm.UDPRoutes[routeKey] = &CompiledUDPRoute{
			UDPRoute:         route,
			RouteState:       rs,
			ParentConditions: parentConditions,
		}
	}

	return cm
}

type routeBindingConfig struct {
	namespace            string
	generation           int64
	hostnames            []string
	validationCond       *metav1.Condition
	isProtocolCompatible func(el *EffectiveListener) bool
	isKindAllowed        func(group *gatewayv1.Group, kind gatewayv1.Kind) bool
	onBind               func(el *EffectiveListener, effectiveHostnames []string)
}

func bindParentRef(
	cfg routeBindingConfig,
	parentRef gatewayv1.ParentReference,
	cm *CompiledModel,
	namespaces map[string]*corev1.Namespace,
	boundListenersForRoute map[*EffectiveListener]bool,
) metav1.Condition {
	if cfg.validationCond != nil && cfg.validationCond.Status == metav1.ConditionFalse {
		return *cfg.validationCond
	}

	group := ValueOf(parentRef.Group)
	if group != "" && group != gatewayv1.GroupName {
		return NewCondition(
			string(gatewayv1.RouteConditionAccepted),
			metav1.ConditionFalse,
			string(gatewayv1.RouteReasonNoMatchingParent),
			fmt.Sprintf("Unsupported parent group: %s", group),
			cfg.generation,
		)
	}

	kind := ValueOf(parentRef.Kind)
	if kind == "" {
		kind = "Gateway"
	}
	if kind != "Gateway" && kind != "ListenerSet" {
		return NewCondition(
			string(gatewayv1.RouteConditionAccepted),
			metav1.ConditionFalse,
			string(gatewayv1.RouteReasonNoMatchingParent),
			fmt.Sprintf("Unsupported parent kind: %s", kind),
			cfg.generation,
		)
	}

	targetNamespace := cfg.namespace
	if parentNamespace := ValueOf(parentRef.Namespace); parentNamespace != "" {
		targetNamespace = string(parentNamespace)
	}
	targetName := string(parentRef.Name)

	var candidateListeners []*EffectiveListener

	if kind == "Gateway" {
		cg := cm.Gateways[types.NamespacedName{Namespace: targetNamespace, Name: targetName}]
		if cg == nil {
			return NewCondition(
				string(gatewayv1.RouteConditionAccepted),
				metav1.ConditionFalse,
				string(gatewayv1.RouteReasonNoMatchingParent),
				"Gateway not found",
				cfg.generation,
			)
		}
		for _, el := range cg.EffectiveListeners {
			if el.Owner.Kind == "Gateway" && el.IsAccepted() {
				candidateListeners = append(candidateListeners, el)
			}
		}
	} else if kind == "ListenerSet" {
		targetLS := cm.ListenerSets[types.NamespacedName{Namespace: targetNamespace, Name: targetName}]
		if targetLS == nil {
			return NewCondition(
				string(gatewayv1.RouteConditionAccepted),
				metav1.ConditionFalse,
				string(gatewayv1.RouteReasonNoMatchingParent),
				"ListenerSet not found",
				cfg.generation,
			)
		}

		gwNs := targetLS.Namespace
		if ns := ValueOf(targetLS.Spec.ParentRef.Namespace); ns != "" {
			gwNs = string(ns)
		}
		cg := cm.Gateways[types.NamespacedName{Namespace: gwNs, Name: string(targetLS.Spec.ParentRef.Name)}]
		if cg == nil || !IsListenerSetAllowed(targetLS, cg.Gateway, namespaces) {
			return NewCondition(
				string(gatewayv1.RouteConditionAccepted),
				metav1.ConditionFalse,
				string(gatewayv1.RouteReasonNoMatchingParent),
				"Parent ListenerSet is not accepted by Gateway",
				cfg.generation,
			)
		}

		for _, el := range cg.EffectiveListeners {
			if el.Owner.Kind == "ListenerSet" && el.Owner.Namespace == targetLS.Namespace && el.Owner.Name == targetLS.Name && el.IsAccepted() {
				candidateListeners = append(candidateListeners, el)
			}
		}
	}

	hasMatchingListener := false
	hasAllowedListener := false
	hasMatchingHostname := false

	for _, el := range candidateListeners {
		if sectionName := ValueOf(parentRef.SectionName); sectionName != "" && sectionName != el.Name {
			continue
		}
		if port := ValueOf(parentRef.Port); port != 0 && port != el.Port {
			continue
		}
		hasMatchingListener = true

		// Check protocol compatibility
		if !cfg.isProtocolCompatible(el) {
			continue
		}

		// Check AllowedRoutes kinds
		if el.AllowedRoutes != nil && len(el.AllowedRoutes.Kinds) > 0 {
			kindAllowed := false
			for _, k := range el.AllowedRoutes.Kinds {
				if cfg.isKindAllowed(k.Group, k.Kind) {
					kindAllowed = true
					break
				}
			}
			if !kindAllowed {
				continue
			}
		}

		// Check AllowedRoutes namespaces
		if el.AllowedRoutes != nil && el.AllowedRoutes.Namespaces != nil && el.AllowedRoutes.Namespaces.From != nil {
			switch *el.AllowedRoutes.Namespaces.From {
			case gatewayv1.NamespacesFromSame:
				if cfg.namespace != el.Owner.Namespace {
					continue
				}
			case gatewayv1.NamespacesFromAll:
				// Allowed
			case gatewayv1.NamespacesFromSelector:
				if el.AllowedRoutes.Namespaces.Selector != nil {
					sel, err := metav1.LabelSelectorAsSelector(el.AllowedRoutes.Namespaces.Selector)
					if err != nil {
						continue
					}
					var nsObj *corev1.Namespace
					if namespaces != nil {
						nsObj = namespaces[cfg.namespace]
					}
					if nsObj != nil {
						if !sel.Matches(labels.Set(nsObj.Labels)) {
							continue
						}
					} else if cfg.namespace != el.Owner.Namespace && namespaces != nil {
						continue
					}
				}
			}
		} else {
			// Default is Same namespace
			if cfg.namespace != el.Owner.Namespace {
				continue
			}
		}

		hasAllowedListener = true

		effectiveHostnames := IntersectHostnames(cfg.hostnames, string(ValueOf(el.Hostname)))
		if len(effectiveHostnames) > 0 || len(cfg.hostnames) == 0 {
			hasMatchingHostname = true

			if !boundListenersForRoute[el] {
				boundListenersForRoute[el] = true
				el.AttachedRoutes++
				if cfg.onBind != nil {
					cfg.onBind(el, effectiveHostnames)
				}
			}
		}
	}

	if hasMatchingHostname {
		return NewCondition(
			string(gatewayv1.RouteConditionAccepted),
			metav1.ConditionTrue,
			string(gatewayv1.RouteReasonAccepted),
			"Route accepted by reference implementation",
			cfg.generation,
		)
	}
	if hasAllowedListener {
		return NewCondition(
			string(gatewayv1.RouteConditionAccepted),
			metav1.ConditionFalse,
			string(gatewayv1.RouteReasonNoMatchingListenerHostname),
			"No matching listener hostname",
			cfg.generation,
		)
	}
	if hasMatchingListener {
		return NewCondition(
			string(gatewayv1.RouteConditionAccepted),
			metav1.ConditionFalse,
			string(gatewayv1.RouteReasonNotAllowedByListeners),
			"Not allowed by listener permissions or protocol",
			cfg.generation,
		)
	}
	return NewCondition(
		string(gatewayv1.RouteConditionAccepted),
		metav1.ConditionFalse,
		string(gatewayv1.RouteReasonNoMatchingParent),
		"No matching listener for parentRef",
		cfg.generation,
	)
}

func bindRouteParentRef(
	route *gatewayv1.HTTPRoute,
	rs *HTTPRouteState,
	parentRef gatewayv1.ParentReference,
	cm *CompiledModel,
	namespaces map[string]*corev1.Namespace,
	boundListenersForRoute map[*EffectiveListener]bool,
) metav1.Condition {
	var valCond *metav1.Condition
	if rs.Internal != nil {
		valCond = &rs.Internal.ValidationCondition
	}
	return bindParentRef(
		routeBindingConfig{
			namespace:      route.Namespace,
			generation:     route.Generation,
			hostnames:      rs.GetHostnames(),
			validationCond: valCond,
			isProtocolCompatible: func(el *EffectiveListener) bool {
				return el.Protocol == gatewayv1.HTTPProtocolType || el.Protocol == gatewayv1.HTTPSProtocolType
			},
			isKindAllowed: IsHTTPRoute,
			onBind: func(el *EffectiveListener, effectiveHostnames []string) {
				ir := InternalRoute{
					Hostnames: effectiveHostnames,
					Rules:     rs.Internal.Rules,
				}
				el.Routes = append(el.Routes, ir)
			},
		},
		parentRef,
		cm,
		namespaces,
		boundListenersForRoute,
	)
}

func bindTLSRouteParentRef(
	route *gatewayv1.TLSRoute,
	rs *TLSRouteState,
	parentRef gatewayv1.ParentReference,
	cm *CompiledModel,
	namespaces map[string]*corev1.Namespace,
	boundListenersForRoute map[*EffectiveListener]bool,
) metav1.Condition {
	var valCond *metav1.Condition
	if rs.Internal != nil {
		valCond = &rs.Internal.ValidationCondition
	}
	return bindParentRef(
		routeBindingConfig{
			namespace:      route.Namespace,
			generation:     route.Generation,
			hostnames:      rs.GetHostnames(),
			validationCond: valCond,
			isProtocolCompatible: func(el *EffectiveListener) bool {
				if el.Protocol != gatewayv1.TLSProtocolType || el.TLS == nil {
					return false
				}
				if el.TLS.Mode == nil {
					return true
				}
				return *el.TLS.Mode == gatewayv1.TLSModePassthrough || *el.TLS.Mode == gatewayv1.TLSModeTerminate
			},
			isKindAllowed: IsTLSRoute,
			onBind: func(el *EffectiveListener, effectiveHostnames []string) {
				if el.TLSBackends == nil {
					el.TLSBackends = make(map[string][]InternalTLSBackend)
				}
				for _, eh := range effectiveHostnames {
					if rs.Internal != nil {
						for _, rule := range rs.Internal.Rules {
							if len(rule.Backends) > 0 {
								el.TLSBackends[eh] = append(el.TLSBackends[eh], rule.Backends...)
							}
						}
					}
				}
			},
		},
		parentRef,
		cm,
		namespaces,
		boundListenersForRoute,
	)
}

func bindGRPCRouteParentRef(
	route *gatewayv1.GRPCRoute,
	rs *GRPCRouteState,
	parentRef gatewayv1.ParentReference,
	cm *CompiledModel,
	namespaces map[string]*corev1.Namespace,
	boundListenersForRoute map[*EffectiveListener]bool,
) metav1.Condition {
	var valCond *metav1.Condition
	if rs.Internal != nil {
		valCond = &rs.Internal.ValidationCondition
	}
	return bindParentRef(
		routeBindingConfig{
			namespace:      route.Namespace,
			generation:     route.Generation,
			hostnames:      rs.GetHostnames(),
			validationCond: valCond,
			isProtocolCompatible: func(el *EffectiveListener) bool {
				return el.Protocol == gatewayv1.HTTPProtocolType || el.Protocol == gatewayv1.HTTPSProtocolType
			},
			isKindAllowed: IsGRPCRoute,
			onBind: func(el *EffectiveListener, effectiveHostnames []string) {
				ir := InternalRoute{
					Hostnames: effectiveHostnames,
					Rules:     rs.Internal.Rules,
				}
				el.Routes = append(el.Routes, ir)
			},
		},
		parentRef,
		cm,
		namespaces,
		boundListenersForRoute,
	)
}

func bindTCPRouteParentRef(
	route *gatewayv1.TCPRoute,
	rs *TCPRouteState,
	parentRef gatewayv1.ParentReference,
	cm *CompiledModel,
	namespaces map[string]*corev1.Namespace,
	boundListenersForRoute map[*EffectiveListener]bool,
) metav1.Condition {
	var valCond *metav1.Condition
	if rs.Internal != nil {
		valCond = &rs.Internal.ValidationCondition
	}
	return bindParentRef(
		routeBindingConfig{
			namespace:      route.Namespace,
			generation:     route.Generation,
			hostnames:      nil,
			validationCond: valCond,
			isProtocolCompatible: func(el *EffectiveListener) bool {
				return el.Protocol == gatewayv1.TCPProtocolType
			},
			isKindAllowed: IsTCPRoute,
			onBind: func(el *EffectiveListener, effectiveHostnames []string) {
				if rs.Internal != nil {
					routeKey := types.NamespacedName{Namespace: route.Namespace, Name: route.Name}
					if el.BoundTCPRoute == nil || *el.BoundTCPRoute == routeKey {
						el.BoundTCPRoute = &routeKey
						for _, rule := range rs.Internal.Rules {
							if len(rule.Backends) > 0 {
								el.TCPBackends = append(el.TCPBackends, rule.Backends...)
							}
						}
					}
				}
			},
		},
		parentRef,
		cm,
		namespaces,
		boundListenersForRoute,
	)
}

func bindUDPRouteParentRef(
	route *gatewayv1.UDPRoute,
	rs *UDPRouteState,
	parentRef gatewayv1.ParentReference,
	cm *CompiledModel,
	namespaces map[string]*corev1.Namespace,
	boundListenersForRoute map[*EffectiveListener]bool,
) metav1.Condition {
	var valCond *metav1.Condition
	if rs.Internal != nil {
		valCond = &rs.Internal.ValidationCondition
	}
	return bindParentRef(
		routeBindingConfig{
			namespace:      route.Namespace,
			generation:     route.Generation,
			hostnames:      nil,
			validationCond: valCond,
			isProtocolCompatible: func(el *EffectiveListener) bool {
				return el.Protocol == gatewayv1.UDPProtocolType
			},
			isKindAllowed: IsUDPRoute,
			onBind: func(el *EffectiveListener, effectiveHostnames []string) {
				if rs.Internal != nil {
					routeKey := types.NamespacedName{Namespace: route.Namespace, Name: route.Name}
					if el.BoundUDPRoute == nil || *el.BoundUDPRoute == routeKey {
						el.BoundUDPRoute = &routeKey
						for _, rule := range rs.Internal.Rules {
							if len(rule.Backends) > 0 {
								el.UDPBackends = append(el.UDPBackends, rule.Backends...)
							}
						}
					}
				}
			},
		},
		parentRef,
		cm,
		namespaces,
		boundListenersForRoute,
	)
}

// ExtractCertificates extracts TLS certificates once per effective listener across all compiled gateways.
func ExtractCertificates(
	gateways []*CompiledGateway,
	secrets map[types.NamespacedName]*corev1.Secret,
	refValidator ReferenceGrantValidator,
) (map[string]*tls.Certificate, *tls.Certificate) {
	certsMap := make(map[string]*tls.Certificate)
	var defaultCert *tls.Certificate

	for _, cg := range gateways {
		if cg == nil {
			continue
		}
		for _, el := range cg.EffectiveListeners {
			if !el.IsAccepted() || !el.IsProgrammed() {
				continue
			}
			if (el.Protocol != gatewayv1.HTTPSProtocolType && el.Protocol != gatewayv1.TLSProtocolType) || el.TLS == nil {
				continue
			}
			if el.Protocol == gatewayv1.TLSProtocolType && (el.TLS.Mode != nil && *el.TLS.Mode == gatewayv1.TLSModePassthrough) {
				continue
			}

			for _, ref := range el.TLS.CertificateRefs {
				group := ValueOf(ref.Group)
				kind := ValueOf(ref.Kind)
				if kind == "" {
					kind = "Secret"
				}
				if group != "" || kind != "Secret" {
					continue
				}

				secretNs := el.Owner.Namespace
				if ref.Namespace != nil && string(*ref.Namespace) != "" {
					secretNs = string(*ref.Namespace)
				}
				secretKey := types.NamespacedName{
					Namespace: secretNs,
					Name:      string(ref.Name),
				}

				if secretKey.Namespace != el.Owner.Namespace {
					from := Reference{
						GroupKind: schema.GroupKind{Group: gatewayv1.GroupName, Kind: string(el.Owner.Kind)},
						Namespace: el.Owner.Namespace,
					}
					to := Reference{
						GroupKind: schema.GroupKind{Group: string(group), Kind: string(kind)},
						Namespace: secretKey.Namespace,
						Name:      secretKey.Name,
					}
					if refValidator == nil || !refValidator.IsReferencePermitted(from, to) {
						continue
					}
				}

				secret := secrets[secretKey]
				if secret == nil {
					continue
				}

				certBytes := secret.Data[corev1.TLSCertKey]
				keyBytes := secret.Data[corev1.TLSPrivateKeyKey]
				if len(certBytes) == 0 || len(keyBytes) == 0 {
					continue
				}

				tlsCert, err := tls.X509KeyPair(certBytes, keyBytes)
				if err != nil {
					continue
				}

				certCopy := tlsCert
				if len(certCopy.Certificate) > 0 {
					leaf, err := x509.ParseCertificate(certCopy.Certificate[0])
					if err != nil {
						klog.V(2).Infof("failed to parse certificate for secret %s: %v", secretKey, err)
					} else {
						certCopy.Leaf = leaf
						if leaf.Subject.CommonName != "" {
							certsMap[strings.ToLower(leaf.Subject.CommonName)] = &certCopy
						}
						for _, dnsName := range leaf.DNSNames {
							certsMap[strings.ToLower(dnsName)] = &certCopy
						}
					}
				}
				if el.Hostname != nil && string(*el.Hostname) != "" {
					certsMap[strings.ToLower(string(*el.Hostname))] = &certCopy
				}
				if el.Owner.Kind == "Gateway" && (el.Hostname == nil || string(*el.Hostname) == "" || defaultCert == nil) {
					defaultCert = &certCopy
				}
			}
		}
	}

	return certsMap, defaultCert
}

// BuildProxyConfig constructs the proxy listeners and routes from compiled gateways.
func BuildProxyConfig(gateways []*CompiledGateway) ([]InternalListener, []InternalRoute) {
	var proxyListeners []InternalListener
	var proxyRoutes []InternalRoute

	for _, cg := range gateways {
		if cg == nil {
			continue
		}
		for _, el := range cg.EffectiveListeners {
			if !el.IsAccepted() || !el.IsProgrammed() {
				continue
			}
			if el.Protocol == gatewayv1.TCPProtocolType && !el.IsProgrammed() {
				continue
			}
			if el.Protocol == gatewayv1.UDPProtocolType && !el.IsProgrammed() {
				continue
			}
			iListener := el.ToInternalListener()
			proxyListeners = append(proxyListeners, iListener)
			proxyRoutes = append(proxyRoutes, el.Routes...)
		}
	}

	return proxyListeners, proxyRoutes
}

// ComputeOutputs computes all desired statuses, proxy configuration, certificates, and resolved gateways.
func ComputeOutputs(inputs ModelInputs) *Outputs {
	if inputs.RefValidator == nil && len(inputs.ReferenceGrants) > 0 {
		inputs.RefValidator = mapReferenceValidator{referenceGrants: inputs.ReferenceGrants}
	}

	compiled := CompileModel(inputs)

	outputs := &Outputs{
		Revision:                 inputs.Revision,
		GatewayStatuses:          make(map[types.NamespacedName]gatewayv1.GatewayStatus),
		HTTPRouteStatuses:        make(map[types.NamespacedName]gatewayv1.HTTPRouteStatus),
		TLSRouteStatuses:         make(map[types.NamespacedName]gatewayv1.TLSRouteStatus),
		GRPCRouteStatuses:        make(map[types.NamespacedName]gatewayv1.GRPCRouteStatus),
		TCPRouteStatuses:         make(map[types.NamespacedName]gatewayv1.TCPRouteStatus),
		UDPRouteStatuses:         make(map[types.NamespacedName]gatewayv1.UDPRouteStatus),
		ListenerSetStatuses:      make(map[types.NamespacedName]gatewayv1.ListenerSetStatus),
		BackendTLSPolicyStatuses: make(map[types.NamespacedName]gatewayv1.PolicyStatus),
		GatewayClassStatuses:     make(map[types.NamespacedName]gatewayv1.GatewayClassStatus),
	}

	// 1. Gateway Statuses
	for _, gw := range inputs.Gateways {
		if gw == nil {
			continue
		}
		gwKey := types.NamespacedName{Namespace: gw.Namespace, Name: gw.Name}
		cg := compiled.Gateways[gwKey]
		if cg != nil {
			var addrs []gatewayv1.GatewayStatusAddress
			if inputs.GatewayAddresses != nil {
				addrs = inputs.GatewayAddresses[gwKey]
			}
			readiness := GatewayReadiness{Ready: true}
			if inputs.GatewayReadiness != nil {
				if r, ok := inputs.GatewayReadiness[gwKey]; ok {
					readiness = r
				}
			}
			var provErr string
			if inputs.ProvisioningErrors != nil {
				provErr = inputs.ProvisioningErrors[gwKey]
			}
			outputs.GatewayStatuses[gwKey] = ComputeDesiredGatewayStatus(gw, cg, addrs, readiness, provErr, inputs.SupportedAddressTypes)
		}
	}

	// 2. ListenerSet Statuses
	for _, ls := range inputs.ListenerSets {
		if ls == nil {
			continue
		}
		lsKey := types.NamespacedName{Namespace: ls.Namespace, Name: ls.Name}
		var parentGW *gatewayv1.Gateway
		for _, g := range inputs.Gateways {
			if g != nil && IsListenerSetParent(ls, g) {
				parentGW = g
				break
			}
		}
		var compiledGw *CompiledGateway
		if parentGW != nil {
			parentKey := types.NamespacedName{Namespace: parentGW.Namespace, Name: parentGW.Name}
			compiledGw = compiled.Gateways[parentKey]
		}
		if parentGW != nil && (inputs.ControllerName == "" || compiledGw != nil) {
			outputs.ListenerSetStatuses[lsKey] = ComputeDesiredListenerSetStatus(ls, parentGW, inputs.Namespaces, compiledGw)
		}
	}

	// 3. HTTPRoute Statuses
	for _, route := range inputs.HTTPRoutes {
		if route == nil {
			continue
		}
		routeKey := types.NamespacedName{Namespace: route.Namespace, Name: route.Name}
		compiledRoute := compiled.HTTPRoutes[routeKey]
		outputs.HTTPRouteStatuses[routeKey] = ComputeDesiredHTTPRouteStatus(route, compiledRoute, inputs.ControllerName)
	}

	// 4. TLSRoute Statuses
	for _, route := range inputs.TLSRoutes {
		if route == nil {
			continue
		}
		routeKey := types.NamespacedName{Namespace: route.Namespace, Name: route.Name}
		compiledRoute := compiled.TLSRoutes[routeKey]
		outputs.TLSRouteStatuses[routeKey] = ComputeDesiredTLSRouteStatus(route, compiledRoute, inputs.ControllerName)
	}

	// 5. GRPCRoute Statuses
	for _, route := range inputs.GRPCRoutes {
		if route == nil {
			continue
		}
		routeKey := types.NamespacedName{Namespace: route.Namespace, Name: route.Name}
		compiledRoute := compiled.GRPCRoutes[routeKey]
		outputs.GRPCRouteStatuses[routeKey] = ComputeDesiredGRPCRouteStatus(route, compiledRoute, inputs.ControllerName)
	}

	// 6. TCPRoute Statuses
	for _, route := range inputs.TCPRoutes {
		if route == nil {
			continue
		}
		routeKey := types.NamespacedName{Namespace: route.Namespace, Name: route.Name}
		compiledRoute := compiled.TCPRoutes[routeKey]
		outputs.TCPRouteStatuses[routeKey] = ComputeDesiredTCPRouteStatus(route, compiledRoute, inputs.ControllerName)
	}

	// 7. UDPRoute Statuses
	for _, route := range inputs.UDPRoutes {
		if route == nil {
			continue
		}
		routeKey := types.NamespacedName{Namespace: route.Namespace, Name: route.Name}
		compiledRoute := compiled.UDPRoutes[routeKey]
		outputs.UDPRouteStatuses[routeKey] = ComputeDesiredUDPRouteStatus(route, compiledRoute, inputs.ControllerName)
	}

	// 8. BackendTLSPolicy Statuses
	for _, policy := range inputs.BackendTLSPolicies {
		if policy == nil {
			continue
		}
		policyKey := types.NamespacedName{Namespace: policy.Namespace, Name: policy.Name}
		outputs.BackendTLSPolicyStatuses[policyKey] = ComputeDesiredBackendTLSPolicyStatus(
			policy,
			compiled,
			inputs.ConfigMaps,
			inputs.BackendTLSPolicies,
			inputs.ControllerName,
		)
	}

	// 5. GatewayClass Statuses
	for _, gc := range inputs.GatewayClasses {
		if gc == nil {
			continue
		}
		if inputs.ControllerName == "" || string(gc.Spec.ControllerName) == inputs.ControllerName {
			gcKey := types.NamespacedName{Name: gc.Name}
			outputs.GatewayClassStatuses[gcKey] = ComputeDesiredGatewayClassStatus(gc, inputs.ControllerName)
		}
	}

	// 6. Proxy Config & Certs
	proxyListeners, proxyRoutes := BuildProxyConfig(compiled.GatewaysList())
	certsMap, defaultCert := ExtractCertificates(compiled.GatewaysList(), inputs.Secrets, inputs.RefValidator)

	outputs.ProxyListeners = proxyListeners
	outputs.ProxyRoutes = proxyRoutes
	outputs.CertificatesMap = certsMap
	outputs.DefaultCert = defaultCert
	outputs.ResolvedGateways = compiled.ResolvedGateways()
	outputs.CompiledGateways = compiled.Gateways

	outputs.DataplaneConfigs = make(map[types.NamespacedName]*DataplaneConfig)
	for gwKey, cg := range compiled.Gateways {
		dpConfig, err := BuildGatewayDataplaneConfig(cg, inputs.Secrets, inputs.RefValidator)
		if err == nil && dpConfig != nil {
			outputs.DataplaneConfigs[gwKey] = dpConfig
		}
	}

	return outputs
}

// ComputeDesiredGatewayStatus computes the desired GatewayStatus from the compiled model and address provider.
func ComputeDesiredGatewayStatus(
	gw *gatewayv1.Gateway,
	compiledGw *CompiledGateway,
	providedAddresses []gatewayv1.GatewayStatusAddress,
	readiness GatewayReadiness,
	provErr string,
	supportedAddressTypes []gatewayv1.AddressType,
) gatewayv1.GatewayStatus {
	desiredAddresses := computeDesiredAddresses(gw.Spec.Addresses, providedAddresses)
	hasAddress := len(desiredAddresses) > 0
	var effectiveListeners []*EffectiveListener
	attachedLSCount := int32(0)
	var resolvedRefsCond metav1.Condition
	if compiledGw != nil {
		effectiveListeners = compiledGw.EffectiveListeners
		attachedLSCount = compiledGw.AttachedListenerSets
		resolvedRefsCond = compiledGw.ResolvedRefsCondition
	}

	desiredConditions := ComputeGatewayConditions(gw, effectiveListeners, hasAddress, readiness, provErr, supportedAddressTypes, resolvedRefsCond)

	var desiredListenerStatuses []gatewayv1.ListenerStatus
	for _, l := range gw.Spec.Listeners {
		var matchedEl *EffectiveListener
		for _, el := range effectiveListeners {
			if el.Owner.Kind == "Gateway" && el.Name == l.Name {
				matchedEl = el
				break
			}
		}

		supportedKinds := defaultSupportedKindsForProtocol(l.Protocol)
		attachedRoutes := int32(0)
		var conds []metav1.Condition
		if matchedEl != nil {
			supportedKinds = matchedEl.SupportedKinds
			attachedRoutes = matchedEl.AttachedRoutes
			conds = make([]metav1.Condition, len(matchedEl.Conditions))
			copy(conds, matchedEl.Conditions)
		}

		desiredListenerStatuses = append(desiredListenerStatuses, gatewayv1.ListenerStatus{
			Name:           l.Name,
			SupportedKinds: supportedKinds,
			AttachedRoutes: attachedRoutes,
			Conditions:     conds,
		})
	}

	gwAccepted := false
	for _, cond := range desiredConditions {
		if cond.Type == string(gatewayv1.GatewayConditionAccepted) && cond.Status == metav1.ConditionTrue {
			gwAccepted = true
			break
		}
	}

	if !gwAccepted {
		desiredAddresses = nil
	}

	return gatewayv1.GatewayStatus{
		Conditions:           desiredConditions,
		Listeners:            desiredListenerStatuses,
		Addresses:            desiredAddresses,
		AttachedListenerSets: &attachedLSCount,
	}
}

// ComputeDesiredListenerSetStatus computes the desired ListenerSetStatus from the compiled model.
func ComputeDesiredListenerSetStatus(
	ls *gatewayv1.ListenerSet,
	parentGW *gatewayv1.Gateway,
	namespaces map[string]*corev1.Namespace,
	compiledGw *CompiledGateway,
) gatewayv1.ListenerSetStatus {
	var effectiveListeners []*EffectiveListener
	if compiledGw != nil {
		effectiveListeners = compiledGw.EffectiveListeners
	}

	acceptedCond, programmedCond := ComputeListenerSetConditions(ls, parentGW, namespaces, effectiveListeners)
	desiredConditions := []metav1.Condition{programmedCond, acceptedCond}

	var desiredListeners []gatewayv1.ListenerEntryStatus
	if parentGW != nil && IsListenerSetAllowed(ls, parentGW, namespaces) {
		for _, l := range ls.Spec.Listeners {
			var matchedEl *EffectiveListener
			if compiledGw != nil {
				for _, el := range compiledGw.EffectiveListeners {
					if el.Owner.Kind == "ListenerSet" && el.Owner.Namespace == ls.Namespace && el.Owner.Name == ls.Name && el.Name == l.Name {
						matchedEl = el
						break
					}
				}
			}

			if matchedEl == nil {
				continue
			}

			supportedKinds := matchedEl.SupportedKinds
			attachedRoutes := matchedEl.AttachedRoutes
			conds := make([]metav1.Condition, len(matchedEl.Conditions))
			copy(conds, matchedEl.Conditions)

			desiredListeners = append(desiredListeners, gatewayv1.ListenerEntryStatus{
				Name:           l.Name,
				SupportedKinds: supportedKinds,
				AttachedRoutes: attachedRoutes,
				Conditions:     conds,
			})
		}
	}

	return gatewayv1.ListenerSetStatus{
		Conditions: desiredConditions,
		Listeners:  desiredListeners,
	}
}

// ComputeDesiredHTTPRouteStatus computes the desired HTTPRouteStatus from the compiled model.
func ComputeDesiredHTTPRouteStatus(
	route *gatewayv1.HTTPRoute,
	compiledRoute *CompiledRoute,
	controllerName string,
) gatewayv1.HTTPRouteStatus {
	var desiredParents []gatewayv1.RouteParentStatus
	if compiledRoute != nil {
		for i, parentRef := range route.Spec.ParentRefs {
			cond := NewCondition(string(gatewayv1.RouteConditionAccepted), metav1.ConditionFalse, string(gatewayv1.RouteReasonNoMatchingParent), "Parent not found", route.Generation)
			if i < len(compiledRoute.ParentConditions) {
				cond = compiledRoute.ParentConditions[i]
			}
			desiredParents = append(desiredParents, gatewayv1.RouteParentStatus{
				ParentRef:      parentRef,
				ControllerName: gatewayv1.GatewayController(controllerName),
				Conditions: []metav1.Condition{
					cond,
					compiledRoute.RouteState.Internal.ResolvedRefsCondition,
				},
			})
		}
	}

	return gatewayv1.HTTPRouteStatus{
		Parents: desiredParents,
	}
}

// ComputeDesiredTLSRouteStatus computes the desired TLSRouteStatus from the compiled model.
func ComputeDesiredTLSRouteStatus(
	route *gatewayv1.TLSRoute,
	compiledRoute *CompiledTLSRoute,
	controllerName string,
) gatewayv1.TLSRouteStatus {
	var desiredParents []gatewayv1.RouteParentStatus
	if compiledRoute != nil {
		for i, parentRef := range route.Spec.ParentRefs {
			cond := NewCondition(string(gatewayv1.RouteConditionAccepted), metav1.ConditionFalse, string(gatewayv1.RouteReasonNoMatchingParent), "Parent not found", route.Generation)
			if i < len(compiledRoute.ParentConditions) {
				cond = compiledRoute.ParentConditions[i]
			}
			desiredParents = append(desiredParents, gatewayv1.RouteParentStatus{
				ParentRef:      parentRef,
				ControllerName: gatewayv1.GatewayController(controllerName),
				Conditions: []metav1.Condition{
					cond,
					compiledRoute.RouteState.Internal.ResolvedRefsCondition,
				},
			})
		}
	}

	return gatewayv1.TLSRouteStatus{
		RouteStatus: gatewayv1.RouteStatus{
			Parents: desiredParents,
		},
	}
}

// ComputeDesiredGRPCRouteStatus computes the desired GRPCRouteStatus from the compiled model.
func ComputeDesiredGRPCRouteStatus(
	route *gatewayv1.GRPCRoute,
	compiledRoute *CompiledGRPCRoute,
	controllerName string,
) gatewayv1.GRPCRouteStatus {
	var desiredParents []gatewayv1.RouteParentStatus
	if compiledRoute != nil {
		for i, parentRef := range route.Spec.ParentRefs {
			cond := NewCondition(string(gatewayv1.RouteConditionAccepted), metav1.ConditionFalse, string(gatewayv1.RouteReasonNoMatchingParent), "Parent not found", route.Generation)
			if i < len(compiledRoute.ParentConditions) {
				cond = compiledRoute.ParentConditions[i]
			}
			desiredParents = append(desiredParents, gatewayv1.RouteParentStatus{
				ParentRef:      parentRef,
				ControllerName: gatewayv1.GatewayController(controllerName),
				Conditions: []metav1.Condition{
					cond,
					compiledRoute.RouteState.Internal.ResolvedRefsCondition,
				},
			})
		}
	}

	return gatewayv1.GRPCRouteStatus{
		RouteStatus: gatewayv1.RouteStatus{
			Parents: desiredParents,
		},
	}
}

// ComputeDesiredTCPRouteStatus computes the desired TCPRouteStatus from the compiled model.
func ComputeDesiredTCPRouteStatus(
	route *gatewayv1.TCPRoute,
	compiledRoute *CompiledTCPRoute,
	controllerName string,
) gatewayv1.TCPRouteStatus {
	var desiredParents []gatewayv1.RouteParentStatus
	if compiledRoute != nil {
		for i, parentRef := range route.Spec.ParentRefs {
			cond := NewCondition(string(gatewayv1.RouteConditionAccepted), metav1.ConditionFalse, string(gatewayv1.RouteReasonNoMatchingParent), "Parent not found", route.Generation)
			if i < len(compiledRoute.ParentConditions) {
				cond = compiledRoute.ParentConditions[i]
			}
			desiredParents = append(desiredParents, gatewayv1.RouteParentStatus{
				ParentRef:      parentRef,
				ControllerName: gatewayv1.GatewayController(controllerName),
				Conditions: []metav1.Condition{
					cond,
					compiledRoute.RouteState.Internal.ResolvedRefsCondition,
				},
			})
		}
	}

	return gatewayv1.TCPRouteStatus{
		RouteStatus: gatewayv1.RouteStatus{
			Parents: desiredParents,
		},
	}
}

// ComputeDesiredUDPRouteStatus computes the desired UDPRouteStatus from the compiled model.
func ComputeDesiredUDPRouteStatus(
	route *gatewayv1.UDPRoute,
	compiledRoute *CompiledUDPRoute,
	controllerName string,
) gatewayv1.UDPRouteStatus {
	var desiredParents []gatewayv1.RouteParentStatus
	if compiledRoute != nil {
		for i, parentRef := range route.Spec.ParentRefs {
			cond := NewCondition(string(gatewayv1.RouteConditionAccepted), metav1.ConditionFalse, string(gatewayv1.RouteReasonNoMatchingParent), "Parent not found", route.Generation)
			if i < len(compiledRoute.ParentConditions) {
				cond = compiledRoute.ParentConditions[i]
			}
			desiredParents = append(desiredParents, gatewayv1.RouteParentStatus{
				ParentRef:      parentRef,
				ControllerName: gatewayv1.GatewayController(controllerName),
				Conditions: []metav1.Condition{
					cond,
					compiledRoute.RouteState.Internal.ResolvedRefsCondition,
				},
			})
		}
	}

	return gatewayv1.UDPRouteStatus{
		RouteStatus: gatewayv1.RouteStatus{
			Parents: desiredParents,
		},
	}
}

// ComputeDesiredBackendTLSPolicyStatus computes the desired BackendTLSPolicy PolicyStatus.
func ComputeDesiredBackendTLSPolicyStatus(
	policy *gatewayv1.BackendTLSPolicy,
	compiled *CompiledModel,
	configMaps map[types.NamespacedName]*corev1.ConfigMap,
	allPolicies []*gatewayv1.BackendTLSPolicy,
	controllerName string,
) gatewayv1.PolicyStatus {
	isConflicted := false
	var conflictingPolicy string
	for _, targetRef := range policy.Spec.TargetRefs {
		if string(targetRef.Group) != "" {
			continue
		}
		if string(targetRef.Kind) != "Service" {
			continue
		}

		targetSvcNamespace := policy.Namespace
		targetSvcName := string(targetRef.Name)
		targetSection := ValueOf(targetRef.SectionName)

		for _, p := range allPolicies {
			if p == nil || (p.Namespace == policy.Namespace && p.Name == policy.Name) {
				continue
			}

			for _, t := range p.Spec.TargetRefs {
				if string(t.Group) == "" && string(t.Kind) == "Service" {
					if p.Namespace == targetSvcNamespace && string(t.Name) == targetSvcName && ValueOf(t.SectionName) == targetSection {
						if p.CreationTimestamp.Time.Before(policy.CreationTimestamp.Time) {
							isConflicted = true
							conflictingPolicy = fmt.Sprintf("%s/%s", p.Namespace, p.Name)
							break
						}
						if p.CreationTimestamp.Time.Equal(policy.CreationTimestamp.Time) {
							if p.Namespace < policy.Namespace || (p.Namespace == policy.Namespace && p.Name < policy.Name) {
								isConflicted = true
								conflictingPolicy = fmt.Sprintf("%s/%s", p.Namespace, p.Name)
								break
							}
						}
					}
				}
			}
			if isConflicted {
				break
			}
		}
		if isConflicted {
			break
		}
	}

	var unresolvedRefs []string
	hasInvalidKind := false
	hasInvalidCACertRef := false

	if policy.Spec.Validation.WellKnownCACertificates != nil {
		if *policy.Spec.Validation.WellKnownCACertificates != gatewayv1.WellKnownCACertificatesSystem {
			hasInvalidKind = true
			unresolvedRefs = append(unresolvedRefs, string(*policy.Spec.Validation.WellKnownCACertificates))
		}
	} else {
		for _, caRef := range policy.Spec.Validation.CACertificateRefs {
			if string(caRef.Group) != "" || string(caRef.Kind) != "ConfigMap" {
				unresolvedRefs = append(unresolvedRefs, string(caRef.Name))
				hasInvalidKind = true
			} else {
				cmKey := types.NamespacedName{Namespace: policy.Namespace, Name: string(caRef.Name)}
				cm, ok := configMaps[cmKey]
				if !ok || cm == nil {
					unresolvedRefs = append(unresolvedRefs, string(caRef.Name))
					hasInvalidCACertRef = true
				} else {
					if _, err := ExtractCACertFromConfigMap(cm); err != nil {
						unresolvedRefs = append(unresolvedRefs, string(caRef.Name))
						hasInvalidCACertRef = true
					}
				}
			}
		}
	}

	acceptedStatus := metav1.ConditionTrue
	acceptedReason := string(gatewayv1.PolicyReasonAccepted)
	acceptedMessage := "Policy accepted"

	resolvedRefsStatus := metav1.ConditionTrue
	resolvedRefsReason := string(gatewayv1.BackendTLSPolicyReasonResolvedRefs)
	resolvedRefsMessage := "All references resolved"

	if hasInvalidKind || hasInvalidCACertRef {
		acceptedStatus = metav1.ConditionFalse
		acceptedReason = string(gatewayv1.BackendTLSPolicyReasonNoValidCACertificate)
		acceptedMessage = fmt.Sprintf("Unresolved or invalid CA certificate references: %v", unresolvedRefs)

		resolvedRefsStatus = metav1.ConditionFalse
		if hasInvalidKind {
			resolvedRefsReason = string(gatewayv1.BackendTLSPolicyReasonInvalidKind)
			resolvedRefsMessage = fmt.Sprintf("Unsupported or unknown CA certificate reference kind: %v", unresolvedRefs)
		} else {
			resolvedRefsReason = string(gatewayv1.BackendTLSPolicyReasonInvalidCACertificateRef)
			resolvedRefsMessage = fmt.Sprintf("Unresolved or invalid CA certificate references: %v", unresolvedRefs)
		}
	}

	if isConflicted {
		acceptedStatus = metav1.ConditionFalse
		acceptedReason = string(gatewayv1.PolicyReasonConflicted)
		acceptedMessage = fmt.Sprintf("Conflicted with older policy: %s", conflictingPolicy)
	}

	var ancestors []gatewayv1.PolicyAncestorStatus

	if compiled != nil {
		for _, cg := range compiled.Gateways {
			if cg == nil || cg.Gateway == nil {
				continue
			}
			gw := cg.Gateway

			usesPolicy := false
			for _, cr := range compiled.HTTPRoutes {
				if cr == nil || cr.HTTPRoute == nil {
					continue
				}
				route := cr.HTTPRoute

				routeMatchesGw := false
				for i, pref := range route.Spec.ParentRefs {
					if string(pref.Name) == gw.Name && (ValueOf(pref.Namespace) == "" || string(ValueOf(pref.Namespace)) == gw.Namespace) {
						if i < len(cr.ParentConditions) && cr.ParentConditions[i].Status == metav1.ConditionTrue {
							routeMatchesGw = true
							break
						}
					}
				}

				if routeMatchesGw {
					for _, rule := range route.Spec.Rules {
						for _, backendRef := range rule.BackendRefs {
							if string(ValueOf(backendRef.Kind)) == "Service" || ValueOf(backendRef.Kind) == "" {
								svcNs := route.Namespace
								if backendRef.Namespace != nil && string(*backendRef.Namespace) != "" {
									svcNs = string(*backendRef.Namespace)
								}
								for _, targetRef := range policy.Spec.TargetRefs {
									if string(targetRef.Group) == "" && string(targetRef.Kind) == "Service" &&
										svcNs == policy.Namespace && string(backendRef.Name) == string(targetRef.Name) {
										usesPolicy = true
										break
									}
								}
							}
							if usesPolicy {
								break
							}
						}
						if usesPolicy {
							break
						}
					}
				}
				if usesPolicy {
					break
				}
			}

			if usesPolicy {
				ancestors = append(ancestors, gatewayv1.PolicyAncestorStatus{
					AncestorRef: gatewayv1.ParentReference{
						Group:     Ptr(gatewayv1.Group(gatewayv1.GroupName)),
						Kind:      Ptr(gatewayv1.Kind("Gateway")),
						Namespace: Ptr(gatewayv1.Namespace(gw.Namespace)),
						Name:      gatewayv1.ObjectName(gw.Name),
					},
					ControllerName: gatewayv1.GatewayController(controllerName),
					Conditions: []metav1.Condition{
						{
							Type:               string(gatewayv1.PolicyConditionAccepted),
							Status:             acceptedStatus,
							ObservedGeneration: policy.Generation,
							Reason:             acceptedReason,
							Message:            acceptedMessage,
						},
						{
							Type:               string(gatewayv1.BackendTLSPolicyConditionResolvedRefs),
							Status:             resolvedRefsStatus,
							ObservedGeneration: policy.Generation,
							Reason:             resolvedRefsReason,
							Message:            resolvedRefsMessage,
						},
					},
				})
			}
		}
	}

	sort.Slice(ancestors, func(i, j int) bool {
		return CompareParentReference(ancestors[i].AncestorRef, ancestors[j].AncestorRef, policy.Namespace) < 0
	})

	return gatewayv1.PolicyStatus{Ancestors: ancestors}
}

// ComputeDesiredGatewayClassStatus computes the desired GatewayClass status.
func ComputeDesiredGatewayClassStatus(gc *gatewayv1.GatewayClass, controllerName string) gatewayv1.GatewayClassStatus {
	acceptedStatus := metav1.ConditionTrue
	acceptedReason := string(gatewayv1.GatewayClassReasonAccepted)
	acceptedMessage := "GatewayClass accepted by reference implementation"

	if gc.Spec.ParametersRef != nil {
		acceptedStatus = metav1.ConditionFalse
		acceptedReason = string(gatewayv1.GatewayClassReasonInvalidParameters)
		acceptedMessage = "Invalid parametersRef: parametersRef is not supported"
	}

	return gatewayv1.GatewayClassStatus{
		Conditions: []metav1.Condition{
			{
				Type:               string(gatewayv1.GatewayClassConditionStatusAccepted),
				Status:             acceptedStatus,
				ObservedGeneration: gc.Generation,
				Reason:             acceptedReason,
				Message:            acceptedMessage,
			},
		},
	}
}

// MergeGatewayStatus merges desired GatewayStatus into current, preserving LastTransitionTime.
func MergeGatewayStatus(current *gatewayv1.GatewayStatus, desired gatewayv1.GatewayStatus) bool {
	updated := false
	if SetConditions(&current.Conditions, desired.Conditions) {
		updated = true
	}
	if !reflectAddressesEqual(current.Addresses, desired.Addresses) {
		current.Addresses = desired.Addresses
		updated = true
	}
	if (current.AttachedListenerSets == nil && desired.AttachedListenerSets != nil) ||
		(current.AttachedListenerSets != nil && desired.AttachedListenerSets == nil) ||
		(current.AttachedListenerSets != nil && desired.AttachedListenerSets != nil && *current.AttachedListenerSets != *desired.AttachedListenerSets) {
		current.AttachedListenerSets = desired.AttachedListenerSets
		updated = true
	}

	var newListeners []gatewayv1.ListenerStatus
	for _, dl := range desired.Listeners {
		entry := gatewayv1.ListenerStatus{
			Name:           dl.Name,
			SupportedKinds: dl.SupportedKinds,
			AttachedRoutes: dl.AttachedRoutes,
			Conditions:     make([]metav1.Condition, len(dl.Conditions)),
		}
		copy(entry.Conditions, dl.Conditions)

		var matchedOld *gatewayv1.ListenerStatus
		for _, ol := range current.Listeners {
			if ol.Name == dl.Name {
				matchedOld = &ol
				break
			}
		}
		if matchedOld != nil {
			for k, dc := range entry.Conditions {
				for _, oc := range matchedOld.Conditions {
					if oc.Type == dc.Type {
						if oc.Status == dc.Status {
							entry.Conditions[k].LastTransitionTime = oc.LastTransitionTime
						}
						break
					}
				}
			}
		}
		for k := range entry.Conditions {
			if entry.Conditions[k].LastTransitionTime.IsZero() {
				entry.Conditions[k].LastTransitionTime = metav1.Now()
			}
		}
		newListeners = append(newListeners, entry)
	}
	if !reflectListenerStatusesEqual(current.Listeners, newListeners) {
		current.Listeners = newListeners
		updated = true
	}
	return updated
}

// MergeListenerSetStatus merges desired ListenerSetStatus into current, preserving LastTransitionTime.
func MergeListenerSetStatus(current *gatewayv1.ListenerSetStatus, desired gatewayv1.ListenerSetStatus) bool {
	updated := false
	if SetConditions(&current.Conditions, desired.Conditions) {
		updated = true
	}
	var newListeners []gatewayv1.ListenerEntryStatus
	for _, dl := range desired.Listeners {
		entry := gatewayv1.ListenerEntryStatus{
			Name:           dl.Name,
			SupportedKinds: dl.SupportedKinds,
			AttachedRoutes: dl.AttachedRoutes,
			Conditions:     make([]metav1.Condition, len(dl.Conditions)),
		}
		copy(entry.Conditions, dl.Conditions)

		var matchedOld *gatewayv1.ListenerEntryStatus
		for _, ol := range current.Listeners {
			if ol.Name == dl.Name {
				matchedOld = &ol
				break
			}
		}
		if matchedOld != nil {
			for k, dc := range entry.Conditions {
				for _, oc := range matchedOld.Conditions {
					if oc.Type == dc.Type {
						if oc.Status == dc.Status {
							entry.Conditions[k].LastTransitionTime = oc.LastTransitionTime
						}
						break
					}
				}
			}
		}
		for k := range entry.Conditions {
			if entry.Conditions[k].LastTransitionTime.IsZero() {
				entry.Conditions[k].LastTransitionTime = metav1.Now()
			}
		}
		newListeners = append(newListeners, entry)
	}
	if !reflectListenerEntryStatusesEqual(current.Listeners, newListeners) {
		current.Listeners = newListeners
		updated = true
	}
	return updated
}

// MergeHTTPRouteStatus merges desired HTTPRouteStatus into current, preserving LastTransitionTime and other controllers' parents.
// It returns true if semantic changes occurred that require updating status in the API server.
func MergeHTTPRouteStatus(current *gatewayv1.HTTPRouteStatus, desired gatewayv1.HTTPRouteStatus, routeNamespace string, controllerName gatewayv1.GatewayController) bool {
	newParents, updated := UpdateRouteParentStatuses(current.Parents, desired.Parents, routeNamespace, controllerName)
	current.Parents = newParents
	return updated
}

// MergeTLSRouteStatus merges desired TLSRouteStatus into current, preserving LastTransitionTime and other controllers' parents.
// It returns true if semantic changes occurred that require updating status in the API server.
func MergeTLSRouteStatus(current *gatewayv1.TLSRouteStatus, desired gatewayv1.TLSRouteStatus, routeNamespace string, controllerName gatewayv1.GatewayController) bool {
	newParents, updated := UpdateRouteParentStatuses(current.Parents, desired.Parents, routeNamespace, controllerName)
	current.Parents = newParents
	return updated
}

// MergeGRPCRouteStatus merges desired GRPCRouteStatus into current, preserving LastTransitionTime and other controllers' parents.
// It returns true if semantic changes occurred that require updating status in the API server.
func MergeGRPCRouteStatus(current *gatewayv1.GRPCRouteStatus, desired gatewayv1.GRPCRouteStatus, routeNamespace string, controllerName gatewayv1.GatewayController) bool {
	newParents, updated := UpdateRouteParentStatuses(current.Parents, desired.Parents, routeNamespace, controllerName)
	current.Parents = newParents
	return updated
}

// MergeTCPRouteStatus merges desired TCPRouteStatus into current, preserving LastTransitionTime and other controllers' parents.
// It returns true if semantic changes occurred that require updating status in the API server.
func MergeTCPRouteStatus(current *gatewayv1.TCPRouteStatus, desired gatewayv1.TCPRouteStatus, routeNamespace string, controllerName gatewayv1.GatewayController) bool {
	newParents, updated := UpdateRouteParentStatuses(current.Parents, desired.Parents, routeNamespace, controllerName)
	current.Parents = newParents
	return updated
}

// MergeUDPRouteStatus merges desired UDPRouteStatus into current, preserving LastTransitionTime and other controllers' parents.
// It returns true if semantic changes occurred that require updating status in the API server.
func MergeUDPRouteStatus(current *gatewayv1.UDPRouteStatus, desired gatewayv1.UDPRouteStatus, routeNamespace string, controllerName gatewayv1.GatewayController) bool {
	newParents, updated := UpdateRouteParentStatuses(current.Parents, desired.Parents, routeNamespace, controllerName)
	current.Parents = newParents
	return updated
}

// MergeBackendTLSPolicyStatus merges desired PolicyStatus into current, preserving LastTransitionTime and other controllers' ancestors.
func MergeBackendTLSPolicyStatus(current *gatewayv1.PolicyStatus, desired gatewayv1.PolicyStatus, controllerName gatewayv1.GatewayController) bool {
	newAncestors, updated := UpdatePolicyAncestors(current.Ancestors, desired.Ancestors, controllerName)
	if updated {
		current.Ancestors = newAncestors
	}
	return updated
}

// MergeGatewayClassStatus merges desired GatewayClassStatus into current, preserving LastTransitionTime.
func MergeGatewayClassStatus(current *gatewayv1.GatewayClassStatus, desired gatewayv1.GatewayClassStatus) bool {
	return SetConditions(&current.Conditions, desired.Conditions)
}

// GatewayStatusesEqual compares two GatewayStatus objects (ignoring LastTransitionTime).
func GatewayStatusesEqual(a, b gatewayv1.GatewayStatus) bool {
	if !ConditionsEqual(a.Conditions, b.Conditions) {
		return false
	}
	if !reflectAddressesEqual(a.Addresses, b.Addresses) {
		return false
	}
	if (a.AttachedListenerSets == nil) != (b.AttachedListenerSets == nil) {
		return false
	}
	if a.AttachedListenerSets != nil && b.AttachedListenerSets != nil && *a.AttachedListenerSets != *b.AttachedListenerSets {
		return false
	}
	return reflectListenerStatusesEqual(a.Listeners, b.Listeners)
}

// HTTPRouteStatusesEqual compares two HTTPRouteStatus objects (ignoring LastTransitionTime).
func HTTPRouteStatusesEqual(a, b gatewayv1.HTTPRouteStatus) bool {
	if len(a.Parents) != len(b.Parents) {
		return false
	}
	for i := range a.Parents {
		if !reflectParentReferenceEqual(a.Parents[i].ParentRef, b.Parents[i].ParentRef) ||
			a.Parents[i].ControllerName != b.Parents[i].ControllerName ||
			!ConditionsEqual(a.Parents[i].Conditions, b.Parents[i].Conditions) {
			return false
		}
	}
	return true
}

// TLSRouteStatusesEqual compares two TLSRouteStatus objects (ignoring LastTransitionTime).
func TLSRouteStatusesEqual(a, b gatewayv1.TLSRouteStatus) bool {
	if len(a.Parents) != len(b.Parents) {
		return false
	}
	for i := range a.Parents {
		if !reflectParentReferenceEqual(a.Parents[i].ParentRef, b.Parents[i].ParentRef) ||
			a.Parents[i].ControllerName != b.Parents[i].ControllerName ||
			!ConditionsEqual(a.Parents[i].Conditions, b.Parents[i].Conditions) {
			return false
		}
	}
	return true
}

// GRPCRouteStatusesEqual compares two GRPCRouteStatus objects (ignoring LastTransitionTime).
func GRPCRouteStatusesEqual(a, b gatewayv1.GRPCRouteStatus) bool {
	if len(a.Parents) != len(b.Parents) {
		return false
	}
	for i := range a.Parents {
		if !reflectParentReferenceEqual(a.Parents[i].ParentRef, b.Parents[i].ParentRef) ||
			a.Parents[i].ControllerName != b.Parents[i].ControllerName ||
			!ConditionsEqual(a.Parents[i].Conditions, b.Parents[i].Conditions) {
			return false
		}
	}
	return true
}

// TCPRouteStatusesEqual compares two TCPRouteStatus objects (ignoring LastTransitionTime).
func TCPRouteStatusesEqual(a, b gatewayv1.TCPRouteStatus) bool {
	if len(a.Parents) != len(b.Parents) {
		return false
	}
	for i := range a.Parents {
		if !reflectParentReferenceEqual(a.Parents[i].ParentRef, b.Parents[i].ParentRef) ||
			a.Parents[i].ControllerName != b.Parents[i].ControllerName ||
			!ConditionsEqual(a.Parents[i].Conditions, b.Parents[i].Conditions) {
			return false
		}
	}
	return true
}

// UDPRouteStatusesEqual compares two UDPRouteStatus objects (ignoring LastTransitionTime).
func UDPRouteStatusesEqual(a, b gatewayv1.UDPRouteStatus) bool {
	if len(a.Parents) != len(b.Parents) {
		return false
	}
	for i := range a.Parents {
		if !reflectParentReferenceEqual(a.Parents[i].ParentRef, b.Parents[i].ParentRef) ||
			a.Parents[i].ControllerName != b.Parents[i].ControllerName ||
			!ConditionsEqual(a.Parents[i].Conditions, b.Parents[i].Conditions) {
			return false
		}
	}
	return true
}

// ListenerSetStatusesEqual compares two ListenerSetStatus objects (ignoring LastTransitionTime).
func ListenerSetStatusesEqual(a, b gatewayv1.ListenerSetStatus) bool {
	if !ConditionsEqual(a.Conditions, b.Conditions) {
		return false
	}
	return reflectListenerEntryStatusesEqual(a.Listeners, b.Listeners)
}

// PolicyStatusesEqual compares two PolicyStatus objects (ignoring LastTransitionTime).
func PolicyStatusesEqual(a, b gatewayv1.PolicyStatus) bool {
	if len(a.Ancestors) != len(b.Ancestors) {
		return false
	}
	for i := range a.Ancestors {
		if !reflectParentReferenceEqual(a.Ancestors[i].AncestorRef, b.Ancestors[i].AncestorRef) ||
			a.Ancestors[i].ControllerName != b.Ancestors[i].ControllerName ||
			!ConditionsEqual(a.Ancestors[i].Conditions, b.Ancestors[i].Conditions) {
			return false
		}
	}
	return true
}

// GatewayClassStatusesEqual compares two GatewayClassStatus objects (ignoring LastTransitionTime).
func GatewayClassStatusesEqual(a, b gatewayv1.GatewayClassStatus) bool {
	return ConditionsEqual(a.Conditions, b.Conditions)
}

func reflectAddressesEqual(a, b []gatewayv1.GatewayStatusAddress) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Value != b[i].Value || ValueOf(a[i].Type) != ValueOf(b[i].Type) {
			return false
		}
	}
	return true
}

func reflectListenerStatusesEqual(a, b []gatewayv1.ListenerStatus) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].AttachedRoutes != b[i].AttachedRoutes {
			return false
		}
		if len(a[i].SupportedKinds) != len(b[i].SupportedKinds) {
			return false
		}
		for j := range a[i].SupportedKinds {
			if a[i].SupportedKinds[j].Kind != b[i].SupportedKinds[j].Kind || ValueOf(a[i].SupportedKinds[j].Group) != ValueOf(b[i].SupportedKinds[j].Group) {
				return false
			}
		}
		if !ConditionsEqual(a[i].Conditions, b[i].Conditions) {
			return false
		}
	}
	return true
}

func reflectListenerEntryStatusesEqual(a, b []gatewayv1.ListenerEntryStatus) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].AttachedRoutes != b[i].AttachedRoutes {
			return false
		}
		if len(a[i].SupportedKinds) != len(b[i].SupportedKinds) {
			return false
		}
		for j := range a[i].SupportedKinds {
			if a[i].SupportedKinds[j].Kind != b[i].SupportedKinds[j].Kind || ValueOf(a[i].SupportedKinds[j].Group) != ValueOf(b[i].SupportedKinds[j].Group) {
				return false
			}
		}
		if !ConditionsEqual(a[i].Conditions, b[i].Conditions) {
			return false
		}
	}
	return true
}

// CompileModel on State compiles the state snapshot into a CompiledModel.
func (s *State) CompileModel(controllerName string) *CompiledModel {
	gatewaysMap := s.GetGateways()
	var gateways []*gatewayv1.Gateway
	for _, gwState := range gatewaysMap {
		if gwState != nil && gwState.Gateway != nil {
			gateways = append(gateways, gwState.Gateway)
		}
	}
	sort.Slice(gateways, func(i, j int) bool {
		if gateways[i].Namespace != gateways[j].Namespace {
			return gateways[i].Namespace < gateways[j].Namespace
		}
		return gateways[i].Name < gateways[j].Name
	})

	listenerSetsList := s.GetListenerSets()
	var listenerSets []*gatewayv1.ListenerSet
	for _, lsState := range listenerSetsList {
		if lsState != nil && lsState.ListenerSet != nil {
			listenerSets = append(listenerSets, lsState.ListenerSet)
		}
	}

	routesList := s.GetHTTPRoutes()
	var routes []*gatewayv1.HTTPRoute
	for _, rState := range routesList {
		if rState != nil && rState.HTTPRoute != nil {
			routes = append(routes, rState.HTTPRoute)
		}
	}

	var backendTLSPolicies []*gatewayv1.BackendTLSPolicy
	for _, b := range s.GetBackendTLSPolicies() {
		if b != nil {
			backendTLSPolicies = append(backendTLSPolicies, b)
		}
	}

	var gatewayClasses []*gatewayv1.GatewayClass
	for _, gc := range s.GetGatewayClasses() {
		if gc != nil {
			gatewayClasses = append(gatewayClasses, gc)
		}
	}

	return CompileModel(ModelInputs{
		Revision:           s.Revision(),
		Gateways:           gateways,
		GatewayClasses:     gatewayClasses,
		GatewayAddresses:   s.GetGatewayAddresses(),
		ListenerSets:       listenerSets,
		HTTPRoutes:         routes,
		Services:           s.GetServices(),
		BackendTLSPolicies: backendTLSPolicies,
		ConfigMaps:         s.GetConfigMaps(),
		Secrets:            s.GetSecrets(),
		Namespaces:         s.GetNamespaces(),
		ReferenceGrants:    nil,
		RefValidator:       s,
		ControllerName:     controllerName,
	})
}
