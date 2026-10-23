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
	"strings"
	"testing"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
	gatewayconsts "sigs.k8s.io/gateway-api/pkg/consts"
)

func makeTestCRD(name string, bundleVersion string) *metav1.PartialObjectMetadata {
	m := &metav1.PartialObjectMetadata{
		TypeMeta: metav1.TypeMeta{
			APIVersion: CustomResourceDefinitionGVK.GroupVersion().String(),
			Kind:       CustomResourceDefinitionGVK.Kind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			ResourceVersion: "1",
		},
	}
	if bundleVersion != "" {
		m.Annotations = map[string]string{
			gatewayconsts.BundleVersionAnnotation: bundleVersion,
		}
	}
	return m
}

func requiredGatewayAPICRDs(bundleVersion string) []*metav1.PartialObjectMetadata {
	return []*metav1.PartialObjectMetadata{
		makeTestCRD(GatewayClassCRDName, ""),
		makeTestCRD(GatewayCRDName, bundleVersion),
		makeTestCRD(HTTPRouteCRDName, ""),
	}
}

func newTestSchemeWithApiextensions() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = apiextensionsv1.AddToScheme(s)
	_ = gatewayv1.AddToScheme(s)
	_ = gatewayv1beta1.AddToScheme(s)
	return s
}

// TestSnapshotFromRESTMapper_CRDExistsWithDifferentVersion tests that when a CRD exists
// on the cluster but only serves a different version than GARI uses (e.g. TLSRoute at v1alpha2
// instead of v1), it is correctly identified as not served.
func TestSnapshotFromRESTMapper_CRDExistsWithDifferentVersion(t *testing.T) {
	scheme := newTestSchemeWithApiextensions()

	// Cluster serves required CRDs at v1, but TLSRoute only at v1alpha2
	mapper := newTestRESTMapper(
		gwV1.WithKind("GatewayClass"),
		gwV1.WithKind("Gateway"),
		gwV1.WithKind("HTTPRoute"),
		schema.GroupVersion{Group: gatewayv1.GroupName, Version: "v1alpha2"}.WithKind("TLSRoute"),
	)

	servedKinds, err := SnapshotFromRESTMapper(mapper, scheme)
	if err != nil {
		t.Fatalf("unexpected SnapshotFromRESTMapper error: %v", err)
	}

	snapshot := CRDSnapshot{ServedKinds: servedKinds}
	if snapshot.IsKindServed(schema.GroupKind{Group: gatewayv1.GroupName, Kind: "TLSRoute"}) {
		t.Errorf("expected TLSRoute NOT to be served because cluster only serves v1alpha2, but GARI requires v1")
	}

	disabled := snapshot.DisabledKinds()
	foundTLSRoute := false
	for _, k := range disabled {
		if k == "TLSRoute" {
			foundTLSRoute = true
			break
		}
	}
	if !foundTLSRoute {
		t.Errorf("expected TLSRoute to be in disabled kinds, got %v", disabled)
	}
}

// TestCRDSnapshot_Comparable tests that CRDSnapshot values can be compared directly using ==.
func TestCRDSnapshot_Comparable(t *testing.T) {
	s1 := CRDSnapshot{
		ServedKinds: ServedKinds{
			GatewayClass: true,
			Gateway:      true,
			HTTPRoute:    true,
		},
		BundleVersion: "v1.6.3",
	}

	s2 := CRDSnapshot{
		ServedKinds: ServedKinds{
			GatewayClass: true,
			Gateway:      true,
			HTTPRoute:    true,
		},
		BundleVersion: "v1.6.3",
	}

	s3 := CRDSnapshot{
		ServedKinds: ServedKinds{
			GatewayClass: true,
			Gateway:      true,
			HTTPRoute:    true,
			TLSRoute:     true,
		},
		BundleVersion: "v1.6.3",
	}

	s4 := CRDSnapshot{
		ServedKinds: ServedKinds{
			GatewayClass: true,
			Gateway:      true,
			HTTPRoute:    true,
		},
		BundleVersion: "v1.6.4",
	}

	if s1 != s2 {
		t.Errorf("expected s1 == s2")
	}
	if s1 == s3 {
		t.Errorf("expected s1 != s3 (different served kinds)")
	}
	if s1 == s4 {
		t.Errorf("expected s1 != s4 (different bundle version)")
	}
}

// TestCRDView_UpgradingBundleVersionUpdatesSupportedVersion tests that upgrading the
// bundle-version annotation updates the snapshot and notifies State without restart.
func TestCRDView_UpgradingBundleVersionUpdatesSupportedVersion(t *testing.T) {
	ctx := t.Context()
	scheme := newTestSchemeWithApiextensions()

	initCRDs := requiredGatewayAPICRDs("v1.6.0")
	var initObjs []runtime.Object
	for _, c := range initCRDs {
		initObjs = append(initObjs, c)
	}

	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-gc",
			Generation: 1,
		},
		Spec: gatewayv1.GatewayClassSpec{
			ControllerName: "test-controller",
		},
	}
	initObjs = append(initObjs, gc)

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(initObjs...).
		WithStatusSubresource(gc).
		Build()

	mapper := newTestRESTMapper(
		gwV1.WithKind("GatewayClass"),
		gwV1.WithKind("Gateway"),
		gwV1.WithKind("HTTPRoute"),
	)

	crdView, err := NewCRDView(ctx, mapper, scheme, cl)
	if err != nil {
		t.Fatalf("unexpected NewCRDView error: %v", err)
	}

	if crdView.Snapshot().BundleVersion != "v1.6.0" {
		t.Fatalf("expected initial bundle version v1.6.0, got %q", crdView.Snapshot().BundleVersion)
	}

	st := state.NewState()
	st.SetControllerName("test-controller")
	initBV := crdView.Snapshot().BundleVersion
	st.SetGatewayCRDBundleVersion(&initBV)
	st.UpsertGatewayClass(gc)
	st.Recompute()

	// Hook crdView to state
	crdView.SetOnBundleVersionChanged(func(newVersion string) {
		st.SetGatewayCRDBundleVersion(&newVersion)
	})

	r := &GatewayClassReconciler{
		Client:         cl,
		Scheme:         scheme,
		State:          st,
		ControllerName: "test-controller",
		CRDView:        crdView,
	}

	// Reconcile initial GatewayClass
	_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-gc"}})
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	var resGC gatewayv1.GatewayClass
	if err := cl.Get(ctx, types.NamespacedName{Name: "test-gc"}, &resGC); err != nil {
		t.Fatalf("failed to get GatewayClass: %v", err)
	}
	cond := findCondition(resGC.Status.Conditions, string(gatewayv1.GatewayClassConditionStatusSupportedVersion))
	if cond == nil {
		t.Fatalf("expected SupportedVersion condition, got nil")
	}
	if !strings.Contains(cond.Message, `"v1.6.0" is supported`) {
		t.Errorf("expected initial message to contain v1.6.0 is supported, got: %s", cond.Message)
	}

	// Upgrade bundle version annotation from v1.6.0 to v1.6.3
	oldGatewayCRD := makeTestCRD(GatewayCRDName, "v1.6.0")
	newGatewayCRD := makeTestCRD(GatewayCRDName, "v1.6.3")
	newGatewayCRD.ResourceVersion = "2"

	crdView.HandleUpdate(oldGatewayCRD, newGatewayCRD)

	if crdView.Snapshot().BundleVersion != "v1.6.3" {
		t.Errorf("expected snapshot bundle version to be v1.6.3, got %q", crdView.Snapshot().BundleVersion)
	}

	// State was updated via callback; recompute outputs
	st.Recompute()

	// Reconcile GatewayClass again
	_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-gc"}})
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	if err := cl.Get(ctx, types.NamespacedName{Name: "test-gc"}, &resGC); err != nil {
		t.Fatalf("failed to get GatewayClass: %v", err)
	}
	cond = findCondition(resGC.Status.Conditions, string(gatewayv1.GatewayClassConditionStatusSupportedVersion))
	if cond == nil {
		t.Fatalf("expected SupportedVersion condition, got nil")
	}
	if !strings.Contains(cond.Message, `"v1.6.3" is supported`) {
		t.Errorf("expected updated message to contain v1.6.3 is supported, got: %s", cond.Message)
	}
}

// TestCRDView_RuntimeCRDChangesIncrementCounter tests that runtime CRD additions, updates,
// and deletions increment the change counter and log the change without failing.
func TestCRDView_RuntimeCRDChangesIncrementCounter(t *testing.T) {
	snapshot := CRDSnapshot{
		ServedKinds: ServedKinds{
			GatewayClass: true,
			Gateway:      true,
			HTTPRoute:    true,
		},
		BundleVersion: "v1.6.3",
	}
	crdView := NewCRDViewFromSnapshot(snapshot)

	if crdView.CRDChangeCount() != 0 {
		t.Fatalf("expected initial change count 0, got %d", crdView.CRDChangeCount())
	}

	// Add an optional CRD
	tlsCRD := makeTestCRD(TLSRouteCRDName, "v1.6.3")
	tlsCRD.ResourceVersion = "1"
	crdView.HandleAdd(tlsCRD)

	if crdView.CRDChangeCount() != 1 {
		t.Errorf("expected change count 1 after add, got %d", crdView.CRDChangeCount())
	}

	// Resync with same ResourceVersion should be ignored
	crdView.HandleAdd(tlsCRD)
	if crdView.CRDChangeCount() != 1 {
		t.Errorf("expected change count still 1 after resync, got %d", crdView.CRDChangeCount())
	}

	// Update CRD with new ResourceVersion
	tlsCRDUpdated := makeTestCRD(TLSRouteCRDName, "v1.6.3")
	tlsCRDUpdated.ResourceVersion = "2"
	crdView.HandleUpdate(tlsCRD, tlsCRDUpdated)

	if crdView.CRDChangeCount() != 2 {
		t.Errorf("expected change count 2 after update, got %d", crdView.CRDChangeCount())
	}

	// Delete CRD
	crdView.HandleDelete(tlsCRDUpdated)
	if crdView.CRDChangeCount() != 3 {
		t.Errorf("expected change count 3 after delete, got %d", crdView.CRDChangeCount())
	}

	// Unrelated CRD should be ignored
	unrelatedCRD := makeTestCRD("certificates.cert-manager.io", "v1.0.0")
	crdView.HandleAdd(unrelatedCRD)
	if crdView.CRDChangeCount() != 3 {
		t.Errorf("expected change count still 3 after unrelated CRD, got %d", crdView.CRDChangeCount())
	}
}

// TestCRDMetadataTransform_DropsManagedFieldsAndAnnotations tests that CRDMetadataTransform
// clears managedFields and drops all annotations except the Gateway API bundle-version annotation.
func TestCRDMetadataTransform_DropsManagedFieldsAndAnnotations(t *testing.T) {
	crd := &metav1.PartialObjectMetadata{
		TypeMeta: metav1.TypeMeta{
			APIVersion: CustomResourceDefinitionGVK.GroupVersion().String(),
			Kind:       CustomResourceDefinitionGVK.Kind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            GatewayCRDName,
			ResourceVersion: "100",
			ManagedFields: []metav1.ManagedFieldsEntry{
				{
					Manager:    "kubectl",
					Operation:  metav1.ManagedFieldsOperationApply,
					APIVersion: "apiextensions.k8s.io/v1",
				},
			},
			Annotations: map[string]string{
				"kubectl.kubernetes.io/last-applied-configuration": `{"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinition"}`,
				"kubectl.kubernetes.io/client-side-filter":         "true",
				gatewayconsts.BundleVersionAnnotation:              "v1.6.3",
				"custom.company.org/team":                          "networking",
			},
		},
	}

	transformed, err := CRDMetadataTransform(crd)
	if err != nil {
		t.Fatalf("unexpected CRDMetadataTransform error: %v", err)
	}

	metaObj := transformed.(*metav1.PartialObjectMetadata)
	if metaObj.ManagedFields != nil {
		t.Errorf("expected ManagedFields to be nil, got: %v", metaObj.ManagedFields)
	}
	if _, hasLastApplied := metaObj.Annotations["kubectl.kubernetes.io/last-applied-configuration"]; hasLastApplied {
		t.Errorf("expected kubectl.kubernetes.io/last-applied-configuration to be dropped")
	}
	if _, hasCustom := metaObj.Annotations["custom.company.org/team"]; hasCustom {
		t.Errorf("expected custom annotation to be dropped")
	}
	if bv, ok := metaObj.Annotations[gatewayconsts.BundleVersionAnnotation]; !ok || bv != "v1.6.3" {
		t.Errorf("expected bundle version annotation v1.6.3, got %q", bv)
	}
}

// TestCRDInformer_CacheTransformInspectStore tests that an informer configured with
// CRDMetadataTransform stores only stripped metadata in its cache store.
func TestCRDInformer_CacheTransformInspectStore(t *testing.T) {
	crd := &metav1.PartialObjectMetadata{
		TypeMeta: metav1.TypeMeta{
			APIVersion: CustomResourceDefinitionGVK.GroupVersion().String(),
			Kind:       CustomResourceDefinitionGVK.Kind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            GatewayCRDName,
			ResourceVersion: "1",
			ManagedFields: []metav1.ManagedFieldsEntry{
				{
					Manager:   "kubectl",
					Operation: metav1.ManagedFieldsOperationApply,
				},
			},
			Annotations: map[string]string{
				"kubectl.kubernetes.io/last-applied-configuration": `{"apiVersion":"apiextensions.k8s.io/v1"}`,
				gatewayconsts.BundleVersionAnnotation:              "v1.6.3",
			},
		},
	}

	indexer := toolscache.NewIndexer(toolscache.MetaNamespaceKeyFunc, toolscache.Indexers{})
	fifo := toolscache.NewDeltaFIFOWithOptions(toolscache.DeltaFIFOOptions{
		KnownObjects: indexer,
		Transformer:  toolscache.TransformFunc(CRDMetadataTransform),
	})

	if err := fifo.Add(crd); err != nil {
		t.Fatalf("failed to add to fifo: %v", err)
	}

	_, err := fifo.Pop(func(obj any, isInInitialList bool) error {
		deltas := obj.(toolscache.Deltas)
		for _, d := range deltas {
			if err := indexer.Add(d.Object); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to pop from fifo: %v", err)
	}

	items := indexer.List()
	if len(items) != 1 {
		t.Fatalf("expected 1 item in store, got %d", len(items))
	}

	storedObj, ok := items[0].(*metav1.PartialObjectMetadata)
	if !ok {
		t.Fatalf("expected *metav1.PartialObjectMetadata in store, got %T", items[0])
	}

	if storedObj.ManagedFields != nil {
		t.Errorf("expected informer cache to store nil ManagedFields, got: %v", storedObj.ManagedFields)
	}
	if _, exists := storedObj.Annotations["kubectl.kubernetes.io/last-applied-configuration"]; exists {
		t.Errorf("expected informer cache to not contain last-applied-configuration")
	}
	if bv := storedObj.Annotations[gatewayconsts.BundleVersionAnnotation]; bv != "v1.6.3" {
		t.Errorf("expected informer cache to retain bundle version v1.6.3, got %q", bv)
	}
}

// Acceptance criteria: The cache holds no CRD specs (metadata only), if the watch approach is chosen.
func TestCRDView_RegistersMetadataOnlyInformer(t *testing.T) {
	scheme := newTestSchemeWithApiextensions()

	var capturedObj any
	fakeCache := &informerCaptureCache{
		scheme: scheme,
		onGetInformer: func(obj any) {
			capturedObj = obj
		},
	}

	mgr := &mockManager{
		scheme: scheme,
		cache:  fakeCache,
	}

	crdView := NewCRDViewFromSnapshot(CRDSnapshot{})
	if err := crdView.RegisterWithManager(mgr); err != nil {
		t.Fatalf("unexpected RegisterWithManager error: %v", err)
	}

	if capturedObj == nil {
		t.Fatalf("expected GetInformer to be called with CRD object, got nil")
	}

	metaObj, ok := capturedObj.(*metav1.PartialObjectMetadata)
	if !ok {
		t.Fatalf("expected GetInformer to be called with *metav1.PartialObjectMetadata, got %T (CRD specs must not be cached)", capturedObj)
	}

	gvk := metaObj.GetObjectKind().GroupVersionKind()
	if gvk != CustomResourceDefinitionGVK {
		t.Errorf("expected GVK %v, got %v", CustomResourceDefinitionGVK, gvk)
	}
}

type informerCaptureCache struct {
	fakeCache
	scheme        *runtime.Scheme
	onGetInformer func(obj any)
}

func (c *informerCaptureCache) GetInformer(ctx context.Context, obj client.Object, opts ...ctrlcache.InformerGetOption) (ctrlcache.Informer, error) {
	if c.onGetInformer != nil {
		c.onGetInformer(obj)
	}
	return &fakeInformer{}, nil
}
