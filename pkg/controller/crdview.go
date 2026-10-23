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
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
	gatewayconsts "sigs.k8s.io/gateway-api/pkg/consts"
)

// CRD names for Gateway API resources.
const (
	GatewayClassCRDName     = "gatewayclasses.gateway.networking.k8s.io"
	GatewayCRDName          = "gateways.gateway.networking.k8s.io"
	HTTPRouteCRDName        = "httproutes.gateway.networking.k8s.io"
	TLSRouteCRDName         = "tlsroutes.gateway.networking.k8s.io"
	GRPCRouteCRDName        = "grpcroutes.gateway.networking.k8s.io"
	TCPRouteCRDName         = "tcproutes.gateway.networking.k8s.io"
	UDPRouteCRDName         = "udproutes.gateway.networking.k8s.io"
	BackendTLSPolicyCRDName = "backendtlspolicies.gateway.networking.k8s.io"
	ListenerSetCRDName      = "listenersets.gateway.networking.k8s.io"
	ReferenceGrantCRDName   = "referencegrants.gateway.networking.k8s.io"
)

// CustomResourceDefinitionGVK is the GroupVersionKind for CustomResourceDefinitions.
var CustomResourceDefinitionGVK = schema.GroupVersionKind{
	Group:   "apiextensions.k8s.io",
	Version: "v1",
	Kind:    "CustomResourceDefinition",
}

var crdGVK = CustomResourceDefinitionGVK

// ServedKinds represents the set of Gateway API kinds served at the version GARI uses.
// It is a comparable struct of booleans.
type ServedKinds struct {
	GatewayClass     bool
	Gateway          bool
	HTTPRoute        bool
	TLSRoute         bool
	GRPCRoute        bool
	TCPRoute         bool
	UDPRoute         bool
	BackendTLSPolicy bool
	ListenerSet      bool
	ReferenceGrant   bool
}

// CRDSnapshot represents an immutable snapshot of Gateway API CRD presence and bundle version.
// It is a comparable value so snapshots can be compared directly using ==.
type CRDSnapshot struct {
	ServedKinds   ServedKinds
	BundleVersion string // empty string if not present
}

// IsKindServed reports whether the given GroupKind is served.
func (s CRDSnapshot) IsKindServed(gk schema.GroupKind) bool {
	switch gk {
	case schema.GroupKind{Group: gatewayv1.GroupName, Kind: "GatewayClass"}:
		return s.ServedKinds.GatewayClass
	case schema.GroupKind{Group: gatewayv1.GroupName, Kind: "Gateway"}:
		return s.ServedKinds.Gateway
	case schema.GroupKind{Group: gatewayv1.GroupName, Kind: "HTTPRoute"}:
		return s.ServedKinds.HTTPRoute
	case schema.GroupKind{Group: gatewayv1.GroupName, Kind: "TLSRoute"}:
		return s.ServedKinds.TLSRoute
	case schema.GroupKind{Group: gatewayv1.GroupName, Kind: "GRPCRoute"}:
		return s.ServedKinds.GRPCRoute
	case schema.GroupKind{Group: gatewayv1.GroupName, Kind: "TCPRoute"}:
		return s.ServedKinds.TCPRoute
	case schema.GroupKind{Group: gatewayv1.GroupName, Kind: "UDPRoute"}:
		return s.ServedKinds.UDPRoute
	case schema.GroupKind{Group: gatewayv1.GroupName, Kind: "BackendTLSPolicy"}:
		return s.ServedKinds.BackendTLSPolicy
	case schema.GroupKind{Group: gatewayv1.GroupName, Kind: "ListenerSet"}:
		return s.ServedKinds.ListenerSet
	case schema.GroupKind{Group: gatewayv1beta1.GroupName, Kind: "ReferenceGrant"}:
		return s.ServedKinds.ReferenceGrant
	default:
		return false
	}
}

// DisabledKinds returns the sorted names of optional Gateway API kinds that are not served.
func (s CRDSnapshot) DisabledKinds() []string {
	var disabled []string
	if !s.ServedKinds.TLSRoute {
		disabled = append(disabled, "TLSRoute")
	}
	if !s.ServedKinds.GRPCRoute {
		disabled = append(disabled, "GRPCRoute")
	}
	if !s.ServedKinds.TCPRoute {
		disabled = append(disabled, "TCPRoute")
	}
	if !s.ServedKinds.UDPRoute {
		disabled = append(disabled, "UDPRoute")
	}
	if !s.ServedKinds.BackendTLSPolicy {
		disabled = append(disabled, "BackendTLSPolicy")
	}
	if !s.ServedKinds.ListenerSet {
		disabled = append(disabled, "ListenerSet")
	}
	if !s.ServedKinds.ReferenceGrant {
		disabled = append(disabled, "ReferenceGrant")
	}
	sort.Strings(disabled)
	return disabled
}

// ServedGroupKinds returns a map of GroupKinds to whether they are served.
func (s CRDSnapshot) ServedGroupKinds() map[schema.GroupKind]bool {
	m := make(map[schema.GroupKind]bool)
	if s.ServedKinds.GatewayClass {
		m[schema.GroupKind{Group: gatewayv1.GroupName, Kind: "GatewayClass"}] = true
	}
	if s.ServedKinds.Gateway {
		m[schema.GroupKind{Group: gatewayv1.GroupName, Kind: "Gateway"}] = true
	}
	if s.ServedKinds.HTTPRoute {
		m[schema.GroupKind{Group: gatewayv1.GroupName, Kind: "HTTPRoute"}] = true
	}
	if s.ServedKinds.TLSRoute {
		m[schema.GroupKind{Group: gatewayv1.GroupName, Kind: "TLSRoute"}] = true
	}
	if s.ServedKinds.GRPCRoute {
		m[schema.GroupKind{Group: gatewayv1.GroupName, Kind: "GRPCRoute"}] = true
	}
	if s.ServedKinds.TCPRoute {
		m[schema.GroupKind{Group: gatewayv1.GroupName, Kind: "TCPRoute"}] = true
	}
	if s.ServedKinds.UDPRoute {
		m[schema.GroupKind{Group: gatewayv1.GroupName, Kind: "UDPRoute"}] = true
	}
	if s.ServedKinds.BackendTLSPolicy {
		m[schema.GroupKind{Group: gatewayv1.GroupName, Kind: "BackendTLSPolicy"}] = true
	}
	if s.ServedKinds.ListenerSet {
		m[schema.GroupKind{Group: gatewayv1.GroupName, Kind: "ListenerSet"}] = true
	}
	if s.ServedKinds.ReferenceGrant {
		m[schema.GroupKind{Group: gatewayv1beta1.GroupName, Kind: "ReferenceGrant"}] = true
	}
	return m
}

// ValidateRequired verifies that all required Gateway API CRDs (GatewayClass, Gateway, HTTPRoute) are served.
func (s CRDSnapshot) ValidateRequired() error {
	if !s.ServedKinds.GatewayClass {
		return fmt.Errorf("required Gateway API CRD GatewayClass (%s/GatewayClass) is not installed", gatewayv1.GroupName)
	}
	if !s.ServedKinds.Gateway {
		return fmt.Errorf("required Gateway API CRD Gateway (%s/Gateway) is not installed", gatewayv1.GroupName)
	}
	if !s.ServedKinds.HTTPRoute {
		return fmt.Errorf("required Gateway API CRD HTTPRoute (%s/HTTPRoute) is not installed", gatewayv1.GroupName)
	}
	return nil
}

// CRDViewReader provides access to the current CRDSnapshot.
type CRDViewReader interface {
	Snapshot() CRDSnapshot
}

type crdRecord struct {
	ResourceVersion string
	BundleVersion   string
}

// CRDView manages the Gateway API CRD view: tracking served kinds, bundle version,
// and reacting to CRD changes at runtime.
type CRDView struct {
	mu                     sync.RWMutex
	crds                   map[string]crdRecord
	snapshot               CRDSnapshot
	onBundleVersionChanged func(newVersion string)
	crdChanges             atomic.Uint64
}

// NewCRDView initializes a new CRDView using the RESTMapper as the single source of truth
// for served kinds at the version GARI uses. If reader is provided, it reads the initial
// bundle version from the Gateway CRD metadata.
func NewCRDView(ctx context.Context, mapper meta.RESTMapper, scheme *runtime.Scheme, reader client.Reader) (*CRDView, error) {
	var servedKinds ServedKinds
	if mapper != nil && scheme != nil {
		var err error
		servedKinds, err = SnapshotFromRESTMapper(mapper, scheme)
		if err != nil {
			return nil, err
		}
	} else {
		servedKinds = ServedKinds{
			GatewayClass:     true,
			Gateway:          true,
			HTTPRoute:        true,
			TLSRoute:         true,
			GRPCRoute:        true,
			TCPRoute:         true,
			UDPRoute:         true,
			BackendTLSPolicy: true,
			ListenerSet:      true,
			ReferenceGrant:   true,
		}
	}

	v := &CRDView{
		crds: make(map[string]crdRecord),
		snapshot: CRDSnapshot{
			ServedKinds: servedKinds,
		},
	}

	if reader != nil {
		gatewayMeta := &metav1.PartialObjectMetadata{}
		gatewayMeta.SetGroupVersionKind(CustomResourceDefinitionGVK)
		if err := reader.Get(ctx, client.ObjectKey{Name: GatewayCRDName}, gatewayMeta); err != nil {
			if !meta.IsNoMatchError(err) && client.IgnoreNotFound(err) != nil {
				setupLog.V(1).Info("unable to get Gateway CRD metadata", "error", err)
			}
		} else {
			bv := gatewayMeta.Annotations[gatewayconsts.BundleVersionAnnotation]
			v.crds[GatewayCRDName] = crdRecord{
				ResourceVersion: gatewayMeta.ResourceVersion,
				BundleVersion:   bv,
			}
			v.snapshot.BundleVersion = bv
		}
	}

	return v, nil
}

// NewCRDViewFromSnapshot creates a CRDView with a pre-configured snapshot (useful for unit testing).
func NewCRDViewFromSnapshot(snapshot CRDSnapshot) *CRDView {
	v := &CRDView{
		crds:     make(map[string]crdRecord),
		snapshot: snapshot,
	}
	if snapshot.BundleVersion != "" {
		v.crds[GatewayCRDName] = crdRecord{
			BundleVersion: snapshot.BundleVersion,
		}
	}
	return v
}

// SetOnBundleVersionChanged sets a callback function to be called when the bundle version changes.
func (v *CRDView) SetOnBundleVersionChanged(fn func(newVersion string)) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.onBundleVersionChanged = fn
}

// CRDChangeCount returns the number of CRD change events observed.
func (v *CRDView) CRDChangeCount() uint64 {
	return v.crdChanges.Load()
}

// Snapshot returns the current immutable CRDSnapshot.
func (v *CRDView) Snapshot() CRDSnapshot {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.snapshot
}

// RegisterWithManager registers the metadata-only CRD informer handler with mgr.
func (v *CRDView) RegisterWithManager(mgr ctrl.Manager) error {
	if mgr == nil || mgr.GetCache() == nil {
		return nil
	}
	crdMeta := &metav1.PartialObjectMetadata{}
	crdMeta.SetGroupVersionKind(CustomResourceDefinitionGVK)
	informer, err := mgr.GetCache().GetInformer(context.Background(), crdMeta)
	if err != nil {
		return fmt.Errorf("failed to get informer for CustomResourceDefinition metadata: %w", err)
	}
	_, err = informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    v.HandleAdd,
		UpdateFunc: v.HandleUpdate,
		DeleteFunc: v.HandleDelete,
	})
	if err != nil {
		return fmt.Errorf("failed to add event handler for CustomResourceDefinition metadata: %w", err)
	}
	return nil
}

// HandleAdd processes CRD creation events.
func (v *CRDView) HandleAdd(obj any) {
	metaObj, ok := toPartialObjectMetadata(obj)
	if !ok || !isGatewayAPICRD(metaObj.Name) {
		return
	}

	var bv string
	if metaObj.Name == GatewayCRDName && metaObj.Annotations != nil {
		bv = metaObj.Annotations[gatewayconsts.BundleVersionAnnotation]
	}

	v.mu.Lock()
	existing, found := v.crds[metaObj.Name]
	if found && existing.ResourceVersion == metaObj.ResourceVersion {
		v.mu.Unlock()
		return
	}
	v.crds[metaObj.Name] = crdRecord{
		ResourceVersion: metaObj.ResourceVersion,
		BundleVersion:   bv,
	}
	v.crdChanges.Add(1)

	var bvChanged bool
	var newBV string
	var cb func(string)
	if metaObj.Name == GatewayCRDName && v.snapshot.BundleVersion != bv {
		oldBV := v.snapshot.BundleVersion
		v.snapshot.BundleVersion = bv
		bvChanged = true
		newBV = bv
		cb = v.onBundleVersionChanged
		setupLog.Info("Gateway API bundle-version changed", "oldVersion", oldBV, "newVersion", newBV)
	}
	v.mu.Unlock()

	setupLog.Info("Gateway API CRD changed; restart controller deployment to update served kinds", "crd", metaObj.Name)

	if bvChanged && cb != nil {
		cb(newBV)
	}
}

// HandleUpdate processes CRD update events.
func (v *CRDView) HandleUpdate(oldObj, newObj any) {
	v.HandleAdd(newObj)
}

// HandleDelete processes CRD deletion events.
func (v *CRDView) HandleDelete(obj any) {
	if tombstone, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	metaObj, ok := toPartialObjectMetadata(obj)
	if !ok || !isGatewayAPICRD(metaObj.Name) {
		return
	}

	v.mu.Lock()
	if _, found := v.crds[metaObj.Name]; !found {
		v.mu.Unlock()
		return
	}
	delete(v.crds, metaObj.Name)
	v.crdChanges.Add(1)

	var bvChanged bool
	var cb func(string)
	if metaObj.Name == GatewayCRDName && v.snapshot.BundleVersion != "" {
		oldBV := v.snapshot.BundleVersion
		v.snapshot.BundleVersion = ""
		bvChanged = true
		cb = v.onBundleVersionChanged
		setupLog.Info("Gateway API bundle-version changed", "oldVersion", oldBV, "newVersion", "")
	}
	v.mu.Unlock()

	setupLog.Info("Gateway API CRD deleted; restart controller deployment to update served kinds", "crd", metaObj.Name)

	if bvChanged && cb != nil {
		cb("")
	}
}

func isGatewayAPICRD(name string) bool {
	return strings.HasSuffix(name, ".gateway.networking.k8s.io")
}

func toPartialObjectMetadata(obj any) (*metav1.PartialObjectMetadata, bool) {
	if pom, ok := obj.(*metav1.PartialObjectMetadata); ok {
		return pom, true
	}
	if clientObj, ok := obj.(client.Object); ok {
		return &metav1.PartialObjectMetadata{
			ObjectMeta: metav1.ObjectMeta{
				Name:            clientObj.GetName(),
				Namespace:       clientObj.GetNamespace(),
				Annotations:     clientObj.GetAnnotations(),
				ResourceVersion: clientObj.GetResourceVersion(),
			},
		}, true
	}
	return nil, false
}

// CRDMetadataTransform drops managedFields and all annotations except the Gateway API
// bundle-version annotation before PartialObjectMetadata is committed to the informer cache.
func CRDMetadataTransform(obj any) (any, error) {
	if tombstone, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	metaObj, ok := obj.(metav1.Object)
	if !ok {
		return obj, nil
	}
	metaObj.SetManagedFields(nil)
	annotations := metaObj.GetAnnotations()
	if val, ok := annotations[gatewayconsts.BundleVersionAnnotation]; ok && val != "" {
		metaObj.SetAnnotations(map[string]string{
			gatewayconsts.BundleVersionAnnotation: val,
		})
	} else {
		metaObj.SetAnnotations(nil)
	}
	return obj, nil
}

// SnapshotFromRESTMapper checks the cluster's served API resources using the RESTMapper.
// It verifies that all required and optional Gateway API kinds are served at the specific
// API versions GARI builds against, serving as the single source of truth for served kinds.
func SnapshotFromRESTMapper(mapper meta.RESTMapper, scheme *runtime.Scheme) (ServedKinds, error) {
	type versionedObject struct {
		obj client.Object
		set func(s *ServedKinds, val bool)
	}

	requiredObjects := []versionedObject{
		{obj: &gatewayv1.GatewayClass{}, set: func(s *ServedKinds, v bool) { s.GatewayClass = v }},
		{obj: &gatewayv1.Gateway{}, set: func(s *ServedKinds, v bool) { s.Gateway = v }},
		{obj: &gatewayv1.HTTPRoute{}, set: func(s *ServedKinds, v bool) { s.HTTPRoute = v }},
	}
	optionalObjects := []versionedObject{
		{obj: &gatewayv1.TLSRoute{}, set: func(s *ServedKinds, v bool) { s.TLSRoute = v }},
		{obj: &gatewayv1.GRPCRoute{}, set: func(s *ServedKinds, v bool) { s.GRPCRoute = v }},
		{obj: &gatewayv1.TCPRoute{}, set: func(s *ServedKinds, v bool) { s.TCPRoute = v }},
		{obj: &gatewayv1.UDPRoute{}, set: func(s *ServedKinds, v bool) { s.UDPRoute = v }},
		{obj: &gatewayv1.BackendTLSPolicy{}, set: func(s *ServedKinds, v bool) { s.BackendTLSPolicy = v }},
		{obj: &gatewayv1.ListenerSet{}, set: func(s *ServedKinds, v bool) { s.ListenerSet = v }},
		{obj: &gatewayv1beta1.ReferenceGrant{}, set: func(s *ServedKinds, v bool) { s.ReferenceGrant = v }},
	}

	var served ServedKinds
	for _, vo := range requiredObjects {
		gvk, err := apiutil.GVKForObject(vo.obj, scheme)
		if err != nil {
			return ServedKinds{}, fmt.Errorf("error getting GVK for %T: %w", vo.obj, err)
		}
		if _, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version); err != nil {
			return ServedKinds{}, fmt.Errorf("required Gateway API CRD %s (%s) is not installed: %w", gvk.Kind, gvk.GroupKind().String(), err)
		}
		vo.set(&served, true)
	}

	for _, vo := range optionalObjects {
		gvk, err := apiutil.GVKForObject(vo.obj, scheme)
		if err != nil {
			return ServedKinds{}, fmt.Errorf("error getting GVK for %T: %w", vo.obj, err)
		}
		if _, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version); err != nil {
			if meta.IsNoMatchError(err) {
				continue
			}
			return ServedKinds{}, fmt.Errorf("failed to check CRD for %s (%s): %w", gvk.Kind, gvk.GroupKind().String(), err)
		}
		vo.set(&served, true)
	}

	return served, nil
}
