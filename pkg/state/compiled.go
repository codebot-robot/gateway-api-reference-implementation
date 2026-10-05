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
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
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
	Owner          ListenerOwner
	ParentGateway  types.NamespacedName
	Name           gatewayv1.SectionName
	Port           gatewayv1.PortNumber
	Protocol       gatewayv1.ProtocolType
	Hostname       *gatewayv1.Hostname
	TLS            *gatewayv1.ListenerTLSConfig
	AllowedRoutes  *gatewayv1.AllowedRoutes
	SupportedKinds []gatewayv1.RouteGroupKind
	Conditions     []metav1.Condition
	AttachedRoutes int32
	Routes         []InternalRoute
	Generation     int64
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
	return InternalListener{
		Name:        el.QualifiedName(),
		Protocol:    el.Protocol,
		Port:        el.Port,
		Hostname:    string(ValueOf(el.Hostname)),
		GatewayName: el.ParentGateway,
		Routes:      el.Routes,
	}
}

// CompiledGateway contains the compiled state for a single Gateway.
type CompiledGateway struct {
	Gateway              *gatewayv1.Gateway
	EffectiveListeners   []*EffectiveListener
	AttachedListenerSets int32
	Conditions           []metav1.Condition
}

// CompiledRoute contains the compiled state for an HTTPRoute.
type CompiledRoute struct {
	HTTPRoute        *gatewayv1.HTTPRoute
	RouteState       *HTTPRouteState
	ParentConditions []metav1.Condition
}

// CompiledModel is the complete compiled model of Gateways, ListenerSets, and HTTPRoutes.
type CompiledModel struct {
	Gateways     map[types.NamespacedName]*CompiledGateway
	HTTPRoutes   map[types.NamespacedName]*CompiledRoute
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

// ModelInputs contains all inputs required to build a CompiledModel.
type ModelInputs struct {
	Gateways           []*gatewayv1.Gateway
	ListenerSets       []*gatewayv1.ListenerSet
	HTTPRoutes         []*gatewayv1.HTTPRoute
	Services           map[types.NamespacedName]*corev1.Service
	BackendTLSPolicies []*gatewayv1.BackendTLSPolicy
	ConfigMaps         map[types.NamespacedName]*corev1.ConfigMap
	Secrets            map[types.NamespacedName]*corev1.Secret
	Namespaces         map[string]*corev1.Namespace
	RefValidator       ReferenceGrantValidator
	ControllerName     string
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

// ValidateListener validates a listener specification, computing its supported kinds and conditions.
func ValidateListener(
	listener ListenerSpec,
	owner ListenerOwner,
	generation int64,
	secrets map[types.NamespacedName]*corev1.Secret,
	refValidator ReferenceGrantValidator,
) (supportedKinds []gatewayv1.RouteGroupKind, conditions []metav1.Condition) {
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
		return supportedKinds, conditions
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

	return supportedKinds, conditions
}

// BuildEffectiveListener constructs an EffectiveListener from a listener specification.
func BuildEffectiveListener(
	listener ListenerSpec,
	owner ListenerOwner,
	parentGateway types.NamespacedName,
	generation int64,
	secrets map[types.NamespacedName]*corev1.Secret,
	refValidator ReferenceGrantValidator,
) *EffectiveListener {
	supportedKinds, conditions := ValidateListener(listener, owner, generation, secrets, refValidator)
	return &EffectiveListener{
		Owner:          owner,
		ParentGateway:  parentGateway,
		Name:           listener.Name,
		Port:           listener.Port,
		Protocol:       listener.Protocol,
		Hostname:       listener.Hostname,
		TLS:            listener.TLS,
		AllowedRoutes:  listener.AllowedRoutes,
		SupportedKinds: supportedKinds,
		Conditions:     conditions,
		AttachedRoutes: 0,
		Routes:         nil,
		Generation:     generation,
	}
}

// ComputeGatewayConditions computes the top-level conditions for a Gateway.
func ComputeGatewayConditions(gw *gatewayv1.Gateway, effectiveListeners []*EffectiveListener, hasAddress bool) []metav1.Condition {
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
	} else if !hasAddress {
		gwProgrammedStatus = metav1.ConditionFalse
		gwProgrammedReason = gatewayv1.GatewayReasonAddressNotAssigned
		gwProgrammedMessage = "Waiting for address to be assigned to the Gateway"
	}

	return []metav1.Condition{
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
	}
}

// areProtocolsCompatible returns true if two listeners on the same port can co-exist.
// HTTP listeners can share a port with other HTTP listeners (differentiated by hostname).
// HTTPS listeners can share a port with other HTTPS listeners (differentiated by SNI/hostname).
// TODO: HTTPS and TLS listeners can also share a port (both routed by SNI); enable when TLSRoute is supported.
func areProtocolsCompatible(p1, p2 gatewayv1.ProtocolType) bool {
	if p1 == p2 {
		if p1 == gatewayv1.HTTPProtocolType || p1 == gatewayv1.HTTPSProtocolType || p1 == gatewayv1.TLSProtocolType {
			return true
		}
		return false
	}
	// TODO: Support HTTPS and TLS protocol sharing on the same port once TLSRoute is implemented.
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
	cm := &CompiledModel{
		Gateways:     make(map[types.NamespacedName]*CompiledGateway),
		HTTPRoutes:   make(map[types.NamespacedName]*CompiledRoute),
		ListenerSets: make(map[types.NamespacedName]*gatewayv1.ListenerSet),
	}

	for _, ls := range inputs.ListenerSets {
		if ls != nil {
			cm.ListenerSets[types.NamespacedName{Namespace: ls.Namespace, Name: ls.Name}] = ls
		}
	}

	// 1. Compile each Gateway and its effective listeners
	for _, gw := range inputs.Gateways {
		if gw == nil {
			continue
		}
		gwKey := types.NamespacedName{Namespace: gw.Namespace, Name: gw.Name}
		cg := &CompiledGateway{
			Gateway: gw,
		}

		// Gateway's own listeners (highest precedence)
		for _, l := range gw.Spec.Listeners {
			el := BuildEffectiveListener(
				ListenerToSpec(l),
				ListenerOwner{Kind: "Gateway", Namespace: gw.Namespace, Name: gw.Name},
				gwKey,
				gw.Generation,
				inputs.Secrets,
				inputs.RefValidator,
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
				el := BuildEffectiveListener(
					ListenerEntryToSpec(l),
					ListenerOwner{Kind: "ListenerSet", Namespace: ls.Namespace, Name: ls.Name},
					gwKey,
					ls.Generation,
					inputs.Secrets,
					inputs.RefValidator,
				)
				cg.EffectiveListeners = append(cg.EffectiveListeners, el)
			}
		}

		// Conflict detection pass over all effective listeners on this Gateway in precedence order
		for i := 0; i < len(cg.EffectiveListeners); i++ {
			elCur := cg.EffectiveListeners[i]
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

		cg.Conditions = ComputeGatewayConditions(gw, cg.EffectiveListeners, len(gw.Status.Addresses) > 0)
		cm.Gateways[gwKey] = cg
	}

	// 2. Compile HTTPRoutes and perform route binding
	for _, route := range inputs.HTTPRoutes {
		if route == nil {
			continue
		}
		routeKey := types.NamespacedName{Namespace: route.Namespace, Name: route.Name}
		rs := &HTTPRouteState{HTTPRoute: route}
		rs.Compile(inputs.Services, inputs.BackendTLSPolicies, inputs.ConfigMaps, inputs.RefValidator)

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

	return cm
}

func bindRouteParentRef(
	route *gatewayv1.HTTPRoute,
	rs *HTTPRouteState,
	parentRef gatewayv1.ParentReference,
	cm *CompiledModel,
	namespaces map[string]*corev1.Namespace,
	boundListenersForRoute map[*EffectiveListener]bool,
) metav1.Condition {
	if rs.Internal != nil && rs.Internal.ValidationCondition.Status == metav1.ConditionFalse {
		return rs.Internal.ValidationCondition
	}

	group := ValueOf(parentRef.Group)
	if group != "" && group != gatewayv1.GroupName {
		return NewCondition(
			string(gatewayv1.RouteConditionAccepted),
			metav1.ConditionFalse,
			string(gatewayv1.RouteReasonNoMatchingParent),
			fmt.Sprintf("Unsupported parent group: %s", group),
			route.Generation,
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
			route.Generation,
		)
	}

	targetNamespace := route.Namespace
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
				route.Generation,
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
				route.Generation,
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
				route.Generation,
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
		if el.Protocol != gatewayv1.HTTPProtocolType && el.Protocol != gatewayv1.HTTPSProtocolType {
			continue
		}

		// Check AllowedRoutes kinds
		if el.AllowedRoutes != nil && len(el.AllowedRoutes.Kinds) > 0 {
			kindAllowed := false
			for _, k := range el.AllowedRoutes.Kinds {
				if IsHTTPRoute(k.Group, k.Kind) {
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
				if route.Namespace != el.Owner.Namespace {
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
						nsObj = namespaces[route.Namespace]
					}
					if nsObj != nil {
						if !sel.Matches(labels.Set(nsObj.Labels)) {
							continue
						}
					} else if route.Namespace != el.Owner.Namespace && namespaces != nil {
						continue
					}
				}
			}
		} else {
			// Default is Same namespace
			if route.Namespace != el.Owner.Namespace {
				continue
			}
		}

		hasAllowedListener = true

		effectiveHostnames := IntersectHostnames(rs.GetHostnames(), string(ValueOf(el.Hostname)))
		if len(effectiveHostnames) > 0 || len(route.Spec.Hostnames) == 0 {
			hasMatchingHostname = true

			if !boundListenersForRoute[el] {
				boundListenersForRoute[el] = true
				el.AttachedRoutes++
				ir := InternalRoute{
					Hostnames: effectiveHostnames,
					Rules:     rs.Internal.Rules,
				}
				el.Routes = append(el.Routes, ir)
			}
		}
	}

	if hasMatchingHostname {
		return NewCondition(
			string(gatewayv1.RouteConditionAccepted),
			metav1.ConditionTrue,
			string(gatewayv1.RouteReasonAccepted),
			"Route accepted by reference implementation",
			route.Generation,
		)
	}
	if hasAllowedListener {
		return NewCondition(
			string(gatewayv1.RouteConditionAccepted),
			metav1.ConditionFalse,
			string(gatewayv1.RouteReasonNoMatchingListenerHostname),
			"No matching listener hostname",
			route.Generation,
		)
	}
	if hasMatchingListener {
		return NewCondition(
			string(gatewayv1.RouteConditionAccepted),
			metav1.ConditionFalse,
			string(gatewayv1.RouteReasonNotAllowedByListeners),
			"Not allowed by listener permissions or protocol",
			route.Generation,
		)
	}
	return NewCondition(
		string(gatewayv1.RouteConditionAccepted),
		metav1.ConditionFalse,
		string(gatewayv1.RouteReasonNoMatchingParent),
		"No matching listener for parentRef",
		route.Generation,
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
			if !el.IsAccepted() {
				continue
			}
			if (el.Protocol != gatewayv1.HTTPSProtocolType && el.Protocol != gatewayv1.TLSProtocolType) || el.TLS == nil {
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
			if !el.IsAccepted() {
				continue
			}
			iListener := el.ToInternalListener()
			proxyListeners = append(proxyListeners, iListener)
			proxyRoutes = append(proxyRoutes, el.Routes...)
		}
	}

	return proxyListeners, proxyRoutes
}

// ComputeDesiredGatewayStatus computes the desired GatewayStatus from the compiled model and address provider.
func ComputeDesiredGatewayStatus(
	gw *gatewayv1.Gateway,
	compiledGw *CompiledGateway,
	providedAddresses []gatewayv1.GatewayStatusAddress,
) (gatewayv1.GatewayStatus, bool) {
	desiredStatus := gw.Status.DeepCopy()
	if desiredStatus == nil {
		desiredStatus = &gatewayv1.GatewayStatus{}
	}

	hasAddress := len(providedAddresses) > 0
	var effectiveListeners []*EffectiveListener
	attachedLSCount := int32(0)
	if compiledGw != nil {
		effectiveListeners = compiledGw.EffectiveListeners
		attachedLSCount = compiledGw.AttachedListenerSets
	}

	desiredConditions := ComputeGatewayConditions(gw, effectiveListeners, hasAddress)

	// Build desired listener statuses for Gateway's own listeners
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

		// Find old listener status to preserve LastTransitionTime
		var oldListener *gatewayv1.ListenerStatus
		for _, ol := range desiredStatus.Listeners {
			if ol.Name == l.Name {
				oldListener = &ol
				break
			}
		}

		if oldListener != nil {
			for i, nc := range conds {
				for _, oc := range oldListener.Conditions {
					if oc.Type == nc.Type && oc.Status == nc.Status && !oc.LastTransitionTime.IsZero() {
						conds[i].LastTransitionTime = oc.LastTransitionTime
						break
					}
				}
			}
		}
		for i := range conds {
			if conds[i].LastTransitionTime.IsZero() {
				conds[i].LastTransitionTime = metav1.Now()
			}
		}

		desiredListenerStatuses = append(desiredListenerStatuses, gatewayv1.ListenerStatus{
			Name:           l.Name,
			SupportedKinds: supportedKinds,
			AttachedRoutes: attachedRoutes,
			Conditions:     conds,
		})
	}

	// Gateway accepted check for addresses
	gwAccepted := false
	for _, cond := range desiredConditions {
		if cond.Type == string(gatewayv1.GatewayConditionAccepted) && cond.Status == metav1.ConditionTrue {
			gwAccepted = true
			break
		}
	}

	var desiredAddresses []gatewayv1.GatewayStatusAddress
	if gwAccepted {
		desiredAddresses = providedAddresses
	}

	// Update conditions preserving LastTransitionTime
	conditionsChanged := SetConditions(&desiredStatus.Conditions, desiredConditions)

	updated := conditionsChanged

	// Check addresses
	if !reflectAddressesEqual(desiredStatus.Addresses, desiredAddresses) {
		desiredStatus.Addresses = desiredAddresses
		updated = true
	}

	// Check listeners
	if !reflectListenerStatusesEqual(desiredStatus.Listeners, desiredListenerStatuses) {
		desiredStatus.Listeners = desiredListenerStatuses
		updated = true
	}

	// Check AttachedListenerSets
	if desiredStatus.AttachedListenerSets == nil || *desiredStatus.AttachedListenerSets != attachedLSCount {
		desiredStatus.AttachedListenerSets = &attachedLSCount
		updated = true
	}

	return *desiredStatus, updated
}

// ComputeDesiredListenerSetStatus computes the desired ListenerSetStatus from the compiled model.
func ComputeDesiredListenerSetStatus(
	ls *gatewayv1.ListenerSet,
	parentGW *gatewayv1.Gateway,
	namespaces map[string]*corev1.Namespace,
	compiledGw *CompiledGateway,
) (gatewayv1.ListenerSetStatus, bool) {
	desiredStatus := ls.Status.DeepCopy()
	if desiredStatus == nil {
		desiredStatus = &gatewayv1.ListenerSetStatus{}
	}

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
				// If an allowed ListenerSet listener is not found in the compiled model,
				// it indicates an internal inconsistency (e.g. missing compiled Gateway model).
				// We skip setting status for this listener rather than attempting a fallback
				// re-validation without secrets or a ReferenceGrant validator, which would
				// incorrectly report TLS certificate references as invalid.
				continue
			}

			supportedKinds := matchedEl.SupportedKinds
			attachedRoutes := matchedEl.AttachedRoutes
			conds := make([]metav1.Condition, len(matchedEl.Conditions))
			copy(conds, matchedEl.Conditions)

			var oldListener *gatewayv1.ListenerEntryStatus
			for _, ol := range desiredStatus.Listeners {
				if ol.Name == l.Name {
					oldListener = &ol
					break
				}
			}

			if oldListener != nil {
				for i, nc := range conds {
					for _, oc := range oldListener.Conditions {
						if oc.Type == nc.Type && oc.Status == nc.Status && !oc.LastTransitionTime.IsZero() {
							conds[i].LastTransitionTime = oc.LastTransitionTime
							break
						}
					}
				}
			}
			for i := range conds {
				if conds[i].LastTransitionTime.IsZero() {
					conds[i].LastTransitionTime = metav1.Now()
				}
			}

			desiredListeners = append(desiredListeners, gatewayv1.ListenerEntryStatus{
				Name:           l.Name,
				SupportedKinds: supportedKinds,
				AttachedRoutes: attachedRoutes,
				Conditions:     conds,
			})
		}
	}

	conditionsChanged := SetConditions(&desiredStatus.Conditions, desiredConditions)
	updated := conditionsChanged

	if !reflectListenerEntryStatusesEqual(desiredStatus.Listeners, desiredListeners) {
		desiredStatus.Listeners = desiredListeners
		updated = true
	}

	return *desiredStatus, updated
}

// ComputeDesiredHTTPRouteStatus computes the desired HTTPRouteStatus from the compiled model.
func ComputeDesiredHTTPRouteStatus(
	route *gatewayv1.HTTPRoute,
	compiledRoute *CompiledRoute,
	controllerName string,
) (gatewayv1.HTTPRouteStatus, bool) {
	desiredStatus := route.Status.DeepCopy()
	if desiredStatus == nil {
		desiredStatus = &gatewayv1.HTTPRouteStatus{}
	}

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

	newParents, updated := UpdateRouteParentStatuses(desiredStatus.Parents, desiredParents, route.Namespace, gatewayv1.GatewayController(controllerName))
	desiredStatus.Parents = newParents

	return *desiredStatus, updated
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

// CompileModelHelper on State compiles the state snapshot into a CompiledModel.
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

	return CompileModel(ModelInputs{
		Gateways:           gateways,
		ListenerSets:       listenerSets,
		HTTPRoutes:         routes,
		Services:           s.GetServices(),
		BackendTLSPolicies: backendTLSPolicies,
		ConfigMaps:         s.GetConfigMaps(),
		Secrets:            s.GetSecrets(),
		Namespaces:         s.GetNamespaces(),
		RefValidator:       s,
		ControllerName:     controllerName,
	})
}
