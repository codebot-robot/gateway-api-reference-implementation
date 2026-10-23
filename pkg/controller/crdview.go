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

var (
	crdGVK = schema.GroupVersionKind{
		Group:   "apiextensions.k8s.io",
		Version: "v1",
		Kind:    "CustomResourceDefinition",
	}

	gatewayAPICRDToGroupKind = map[string]schema.GroupKind{
		GatewayClassCRDName:     {Group: gatewayv1.GroupName, Kind: "GatewayClass"},
		GatewayCRDName:          {Group: gatewayv1.GroupName, Kind: "Gateway"},
		HTTPRouteCRDName:        {Group: gatewayv1.GroupName, Kind: "HTTPRoute"},
		TLSRouteCRDName:         {Group: gatewayv1.GroupName, Kind: "TLSRoute"},
		GRPCRouteCRDName:        {Group: gatewayv1.GroupName, Kind: "GRPCRoute"},
		TCPRouteCRDName:         {Group: gatewayv1.GroupName, Kind: "TCPRoute"},
		UDPRouteCRDName:         {Group: gatewayv1.GroupName, Kind: "UDPRoute"},
		BackendTLSPolicyCRDName: {Group: gatewayv1.GroupName, Kind: "BackendTLSPolicy"},
		ListenerSetCRDName:      {Group: gatewayv1.GroupName, Kind: "ListenerSet"},
		ReferenceGrantCRDName:   {Group: gatewayv1beta1.GroupName, Kind: "ReferenceGrant"},
	}

	optionalGatewayAPIKinds = []schema.GroupKind{
		{Group: gatewayv1.GroupName, Kind: "TLSRoute"},
		{Group: gatewayv1.GroupName, Kind: "GRPCRoute"},
		{Group: gatewayv1.GroupName, Kind: "TCPRoute"},
		{Group: gatewayv1.GroupName, Kind: "UDPRoute"},
		{Group: gatewayv1.GroupName, Kind: "BackendTLSPolicy"},
		{Group: gatewayv1.GroupName, Kind: "ListenerSet"},
		{Group: gatewayv1beta1.GroupName, Kind: "ReferenceGrant"},
	}
)

// CRDSnapshot represents an immutable snapshot of Gateway API CRD state on the cluster.
type CRDSnapshot struct {
	// ServedKinds maps GroupKind to whether the kind is served.
	ServedKinds map[schema.GroupKind]bool
	// DisabledKinds lists names of optional Gateway API kinds that are not served.
	DisabledKinds []string
	// BundleVersion is the Gateway API CRD bundle version from
	// the gateway.networking.k8s.io/bundle-version annotation on the Gateway CRD, or nil.
	BundleVersion *string
}

// IsKindServed reports whether the given GroupKind is served.
func (s *CRDSnapshot) IsKindServed(gk schema.GroupKind) bool {
	if s == nil || s.ServedKinds == nil {
		return false
	}
	return s.ServedKinds[gk]
}

// ValidateRequired verifies that all required Gateway API CRDs (GatewayClass, Gateway, HTTPRoute) are served.
func (s *CRDSnapshot) ValidateRequired() error {
	required := []struct {
		kind string
		gk   schema.GroupKind
	}{
		{kind: "GatewayClass", gk: schema.GroupKind{Group: gatewayv1.GroupName, Kind: "GatewayClass"}},
		{kind: "Gateway", gk: schema.GroupKind{Group: gatewayv1.GroupName, Kind: "Gateway"}},
		{kind: "HTTPRoute", gk: schema.GroupKind{Group: gatewayv1.GroupName, Kind: "HTTPRoute"}},
	}
	for _, req := range required {
		if !s.IsKindServed(req.gk) {
			return fmt.Errorf("required Gateway API CRD %s (%s) is not installed", req.kind, req.gk.String())
		}
	}
	return nil
}

// EqualKinds reports whether s and other have the same served kinds.
func (s *CRDSnapshot) EqualKinds(other *CRDSnapshot) bool {
	if s == nil || other == nil {
		return s == other
	}
	if len(s.ServedKinds) != len(other.ServedKinds) {
		return false
	}
	for gk, served := range s.ServedKinds {
		if other.ServedKinds[gk] != served {
			return false
		}
	}
	return true
}

// EqualBundleVersion reports whether s and other have the same bundle version.
func (s *CRDSnapshot) EqualBundleVersion(other *CRDSnapshot) bool {
	if s == nil || other == nil {
		return s == other
	}
	if s.BundleVersion == nil && other.BundleVersion == nil {
		return true
	}
	if s.BundleVersion != nil && other.BundleVersion != nil {
		return *s.BundleVersion == *other.BundleVersion
	}
	return false
}

// CRDViewReader provides access to the current CRDSnapshot.
type CRDViewReader interface {
	Snapshot() *CRDSnapshot
}

// crdMetadata is a lightweight representation of CRD metadata stored by CRDView.
// Storing only name, resourceVersion, and filtered annotations saves significant memory
// by avoiding retention of managedFields and large client-side apply annotations.
type crdMetadata struct {
	Name            string
	ResourceVersion string
	Annotations     map[string]string
}

// CRDView manages the Gateway API CRD view: tracking served kinds, bundle version,
// and reacting to CRD changes at runtime.
type CRDView struct {
	mu                     sync.RWMutex
	crds                   map[string]*crdMetadata
	snapshot               *CRDSnapshot
	restartFunc            func()
	onBundleVersionChanged []func(newVersion *string)
}

// CRDViewOption configures a CRDView.
type CRDViewOption func(*CRDView, *crdViewConfig)

type crdViewConfig struct {
	mapper meta.RESTMapper
	scheme *runtime.Scheme
}

// WithRestartFunc configures an optional callback invoked when the set of served
// Gateway API kinds changes at runtime (for example, when an optional CRD is
// installed or a CRD is removed).
//
// An automated restart is not performed by default because restarting all controller
// replicas simultaneously upon CRD changes can cause thundering herd problems against
// the Kubernetes API server. In production environments, administrators should perform
// a controlled rolling restart of the controller deployment when new CRD kinds are installed.
// WithRestartFunc can be used in tests to detect served-kind transitions, or in custom
// environments where an automated restart or external coordination is explicitly desired.
func WithRestartFunc(fn func()) CRDViewOption {
	return func(v *CRDView, _ *crdViewConfig) {
		v.restartFunc = fn
	}
}

// WithRESTMapper provides a RESTMapper and scheme fallback for testing or discovery.
func WithRESTMapper(mapper meta.RESTMapper, scheme *runtime.Scheme) CRDViewOption {
	return func(_ *CRDView, cfg *crdViewConfig) {
		cfg.mapper = mapper
		cfg.scheme = scheme
	}
}

// NewCRDView initializes a new CRDView by querying CRD metadata using reader.
func NewCRDView(ctx context.Context, reader client.Reader, opts ...CRDViewOption) (*CRDView, error) {
	v := &CRDView{
		crds: make(map[string]*crdMetadata),
	}
	cfg := &crdViewConfig{}
	for _, opt := range opts {
		opt(v, cfg)
	}

	crdsFound, err := v.initFromReader(ctx, reader, cfg)
	if err != nil {
		return nil, err
	}

	if !crdsFound && cfg.mapper != nil && cfg.scheme != nil {
		snapshot, err := snapshotFromRESTMapper(cfg.mapper, cfg.scheme)
		if err != nil {
			return nil, err
		}
		v.snapshot = snapshot
		return v, nil
	}

	v.snapshot = v.computeSnapshotLocked()
	return v, nil
}

// initFromReader queries CRD metadata from the given reader and populates v.crds by calling HandleAdd.
// Returns true if any Gateway API CRDs were found.
func (v *CRDView) initFromReader(ctx context.Context, reader client.Reader, cfg *crdViewConfig) (bool, error) {
	if reader == nil {
		return false, nil
	}

	// TODO: Calling reader.List allocates memory for the full list of CRD metadata.
	// Switch to a version that calls us back for each object.
	crdList := &metav1.PartialObjectMetadataList{}
	crdList.SetGroupVersionKind(crdGVK)
	if err := reader.List(ctx, crdList); err != nil {
		if cfg.mapper == nil {
			return false, fmt.Errorf("failed to list CustomResourceDefinition metadata: %w", err)
		}
		setupLog.V(1).Info("unable to list CRD metadata from reader, falling back to RESTMapper", "error", err)
		return false, nil
	}

	for i := range crdList.Items {
		v.HandleAdd(&crdList.Items[i])
	}

	v.mu.RLock()
	crdsFound := len(v.crds) > 0
	v.mu.RUnlock()
	return crdsFound, nil
}

// NewCRDViewFromSnapshot creates a CRDView with a pre-configured snapshot (useful for unit testing).
func NewCRDViewFromSnapshot(snapshot *CRDSnapshot, opts ...CRDViewOption) *CRDView {
	v := &CRDView{
		crds:     make(map[string]*crdMetadata),
		snapshot: snapshot,
	}
	cfg := &crdViewConfig{}
	for _, opt := range opts {
		opt(v, cfg)
	}
	return v
}

// SetRestartFunc sets the restart function.
func (v *CRDView) SetRestartFunc(fn func()) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.restartFunc = fn
}

// OnBundleVersionChanged registers a callback to be notified when the bundle version changes.
func (v *CRDView) OnBundleVersionChanged(fn func(newVersion *string)) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.onBundleVersionChanged = append(v.onBundleVersionChanged, fn)
}

// Snapshot returns the current immutable CRDSnapshot.
func (v *CRDView) Snapshot() *CRDSnapshot {
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
	crdMeta.SetGroupVersionKind(crdGVK)
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
	metaObj, ok := toCRDMetadata(obj)
	if !ok || !isGatewayAPICRD(metaObj.Name) {
		return
	}

	v.mu.Lock()
	existing, found := v.crds[metaObj.Name]
	if found && existing.ResourceVersion == metaObj.ResourceVersion {
		v.mu.Unlock()
		return
	}
	v.crds[metaObj.Name] = metaObj
	if v.snapshot == nil {
		v.mu.Unlock()
		return
	}
	oldSnapshot := v.snapshot
	newSnapshot := v.computeSnapshotLocked()
	v.snapshot = newSnapshot
	restartFn := v.restartFunc
	bvCallbacks := make([]func(newVersion *string), len(v.onBundleVersionChanged))
	copy(bvCallbacks, v.onBundleVersionChanged)
	v.mu.Unlock()

	v.evaluateChange(oldSnapshot, newSnapshot, restartFn, bvCallbacks)
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
	metaObj, ok := toCRDMetadata(obj)
	if !ok || !isGatewayAPICRD(metaObj.Name) {
		return
	}

	v.mu.Lock()
	if _, found := v.crds[metaObj.Name]; !found {
		v.mu.Unlock()
		return
	}
	delete(v.crds, metaObj.Name)
	if v.snapshot == nil {
		v.mu.Unlock()
		return
	}
	oldSnapshot := v.snapshot
	newSnapshot := v.computeSnapshotLocked()
	v.snapshot = newSnapshot
	restartFn := v.restartFunc
	bvCallbacks := make([]func(newVersion *string), len(v.onBundleVersionChanged))
	copy(bvCallbacks, v.onBundleVersionChanged)
	v.mu.Unlock()

	v.evaluateChange(oldSnapshot, newSnapshot, restartFn, bvCallbacks)
}

func (v *CRDView) evaluateChange(oldSnapshot, newSnapshot *CRDSnapshot, restartFn func(), bvCallbacks []func(newVersion *string)) {
	if oldSnapshot == nil {
		return
	}
	if !oldSnapshot.EqualKinds(newSnapshot) {
		if restartFn != nil {
			setupLog.Info("Gateway API served kinds changed; invoking restart function",
				"oldDisabled", oldSnapshot.DisabledKinds,
				"newDisabled", newSnapshot.DisabledKinds)
			restartFn()
			return
		}
		setupLog.Info("Gateway API served kinds changed; restart controller deployment to reconcile newly installed kinds",
			"oldDisabled", oldSnapshot.DisabledKinds,
			"newDisabled", newSnapshot.DisabledKinds)
	}
	if !oldSnapshot.EqualBundleVersion(newSnapshot) {
		oldVer := "<nil>"
		newVer := "<nil>"
		if oldSnapshot.BundleVersion != nil {
			oldVer = *oldSnapshot.BundleVersion
		}
		if newSnapshot.BundleVersion != nil {
			newVer = *newSnapshot.BundleVersion
		}
		setupLog.Info("Gateway API bundle-version changed", "oldVersion", oldVer, "newVersion", newVer)
		for _, cb := range bvCallbacks {
			cb(newSnapshot.BundleVersion)
		}
	}
}

func (v *CRDView) computeSnapshotLocked() *CRDSnapshot {
	servedKinds := make(map[schema.GroupKind]bool)
	for crdName, gk := range gatewayAPICRDToGroupKind {
		if _, ok := v.crds[crdName]; ok {
			servedKinds[gk] = true
		}
	}

	var disabledKinds []string
	for _, opt := range optionalGatewayAPIKinds {
		if !servedKinds[opt] {
			disabledKinds = append(disabledKinds, opt.Kind)
		}
	}
	sort.Strings(disabledKinds)

	var bundleVersion *string
	if gatewayCRD, ok := v.crds[GatewayCRDName]; ok {
		if val, exists := gatewayCRD.Annotations[gatewayconsts.BundleVersionAnnotation]; exists && val != "" {
			bundleVersion = &val
		}
	}

	return &CRDSnapshot{
		ServedKinds:   servedKinds,
		DisabledKinds: disabledKinds,
		BundleVersion: bundleVersion,
	}
}

func isGatewayAPICRD(name string) bool {
	return strings.HasSuffix(name, ".gateway.networking.k8s.io")
}

func toCRDMetadata(obj any) (*crdMetadata, bool) {
	if m, ok := obj.(*crdMetadata); ok {
		return m, true
	}
	if pom, ok := obj.(*metav1.PartialObjectMetadata); ok {
		return &crdMetadata{
			Name:            pom.Name,
			ResourceVersion: pom.ResourceVersion,
			Annotations:     filterAnnotations(pom.Annotations),
		}, true
	}
	if clientObj, ok := obj.(client.Object); ok {
		return &crdMetadata{
			Name:            clientObj.GetName(),
			ResourceVersion: clientObj.GetResourceVersion(),
			Annotations:     filterAnnotations(clientObj.GetAnnotations()),
		}, true
	}
	return nil, false
}

// filterAnnotations returns a copy of annotations omitting unneeded entries,
// specifically client-side apply annotations such as kubectl.kubernetes.io/last-applied-configuration.
func filterAnnotations(annotations map[string]string) map[string]string {
	if len(annotations) == 0 {
		return nil
	}
	filtered := make(map[string]string)
	for k, v := range annotations {
		if strings.HasPrefix(k, "kubectl.kubernetes.io/") {
			continue
		}
		filtered[k] = v
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

func snapshotFromRESTMapper(mapper meta.RESTMapper, scheme *runtime.Scheme) (*CRDSnapshot, error) {
	requiredObjects := []client.Object{
		&gatewayv1.GatewayClass{},
		&gatewayv1.Gateway{},
		&gatewayv1.HTTPRoute{},
	}
	optionalObjects := []client.Object{
		&gatewayv1.TLSRoute{},
		&gatewayv1.GRPCRoute{},
		&gatewayv1.TCPRoute{},
		&gatewayv1.UDPRoute{},
		&gatewayv1.BackendTLSPolicy{},
		&gatewayv1.ListenerSet{},
		&gatewayv1beta1.ReferenceGrant{},
	}

	served := make(map[schema.GroupKind]bool)
	for _, obj := range requiredObjects {
		gvk, err := apiutil.GVKForObject(obj, scheme)
		if err != nil {
			return nil, fmt.Errorf("error getting GVK for %T: %w", obj, err)
		}
		if _, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version); err != nil {
			return nil, fmt.Errorf("required Gateway API CRD %s (%s) is not installed: %w", gvk.Kind, gvk.GroupKind().String(), err)
		}
		served[gvk.GroupKind()] = true
	}

	var disabledKinds []string
	for _, obj := range optionalObjects {
		gvk, err := apiutil.GVKForObject(obj, scheme)
		if err != nil {
			return nil, fmt.Errorf("error getting GVK for %T: %w", obj, err)
		}
		if _, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version); err != nil {
			if meta.IsNoMatchError(err) {
				disabledKinds = append(disabledKinds, gvk.Kind)
				continue
			}
			return nil, fmt.Errorf("failed to check CRD for %s (%s): %w", gvk.Kind, gvk.GroupKind().String(), err)
		}
		served[gvk.GroupKind()] = true
	}
	sort.Strings(disabledKinds)

	return &CRDSnapshot{
		ServedKinds:   served,
		DisabledKinds: disabledKinds,
	}, nil
}
