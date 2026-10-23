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
			APIVersion: crdGVK.GroupVersion().String(),
			Kind:       crdGVK.Kind,
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

// Acceptance criteria: installing an optional CRD after startup triggers the restart path
func TestCRDView_InstallingOptionalCRDTriggersRestart(t *testing.T) {
	var ctx context.Context = t.Context()
	scheme := newTestSchemeWithApiextensions()

	// Initial startup with only required CRDs (optional kinds absent)
	initCRDs := requiredGatewayAPICRDs("v1.6.3")
	var initObjs []runtime.Object
	for _, c := range initCRDs {
		initObjs = append(initObjs, c)
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(initObjs...).
		Build()

	var restarted bool
	restartChan := make(chan struct{}, 1)
	crdView, err := NewCRDView(ctx, cl, WithRestartFunc(func() {
		restarted = true
		restartChan <- struct{}{}
	}))
	if err != nil {
		t.Fatalf("unexpected NewCRDView error: %v", err)
	}

	snapshot := crdView.Snapshot()
	if err := snapshot.ValidateRequired(); err != nil {
		t.Fatalf("expected required CRDs to be valid: %v", err)
	}
	if snapshot.IsKindServed(schema.GroupKind{Group: gatewayv1.GroupName, Kind: "TLSRoute"}) {
		t.Fatalf("expected TLSRoute to not be served initially")
	}

	// Install optional CRD TLSRoute after startup
	tlsRouteCRD := makeTestCRD(TLSRouteCRDName, "v1.6.3")
	tlsRouteCRD.ResourceVersion = "2"
	crdView.HandleAdd(tlsRouteCRD)

	if !restarted {
		t.Errorf("expected installing an optional CRD after startup to trigger the restart path")
	}
	if !crdView.Snapshot().IsKindServed(schema.GroupKind{Group: gatewayv1.GroupName, Kind: "TLSRoute"}) {
		t.Errorf("expected TLSRoute to be served in updated snapshot")
	}
}

// Acceptance criteria: upgrading the bundle-version annotation updates SupportedVersion without a restart
func TestCRDView_UpgradingBundleVersionUpdatesSupportedVersionWithoutRestart(t *testing.T) {
	ctx := t.Context()
	scheme := newTestSchemeWithApiextensions()

	// Initial startup with required CRDs having bundle version v1.6.0
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

	var restarted bool
	crdView, err := NewCRDView(ctx, cl, WithRestartFunc(func() {
		restarted = true
	}))
	if err != nil {
		t.Fatalf("unexpected NewCRDView error: %v", err)
	}

	st := state.NewState()
	st.SetControllerName("test-controller")
	st.SetGatewayCRDBundleVersion(crdView.Snapshot().BundleVersion)
	st.UpsertGatewayClass(gc)
	st.Recompute()

	// Hook crdView to state
	crdView.OnBundleVersionChanged(func(newVersion *string) {
		st.SetGatewayCRDBundleVersion(newVersion)
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

	if restarted {
		t.Errorf("expected upgrading bundle-version annotation NOT to trigger restart")
	}
	if crdView.Snapshot().BundleVersion == nil || *crdView.Snapshot().BundleVersion != "v1.6.3" {
		t.Errorf("expected snapshot bundle version to be v1.6.3, got %v", crdView.Snapshot().BundleVersion)
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

// Acceptance criteria: removing a required CRD triggers the restart path (startup then fails fast)
func TestCRDView_RemovingRequiredCRDTriggersRestartAndFailsFast(t *testing.T) {
	ctx := t.Context()
	scheme := newTestSchemeWithApiextensions()

	initCRDs := requiredGatewayAPICRDs("v1.6.3")
	var initObjs []runtime.Object
	for _, c := range initCRDs {
		initObjs = append(initObjs, c)
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(initObjs...).
		Build()

	var restarted bool
	crdView, err := NewCRDView(ctx, cl, WithRestartFunc(func() {
		restarted = true
	}))
	if err != nil {
		t.Fatalf("unexpected NewCRDView error: %v", err)
	}

	if err := crdView.Snapshot().ValidateRequired(); err != nil {
		t.Fatalf("expected required CRDs to be valid initially: %v", err)
	}

	// Remove a required CRD: HTTPRoute
	httpRouteCRD := makeTestCRD(HTTPRouteCRDName, "")
	crdView.HandleDelete(httpRouteCRD)

	if !restarted {
		t.Errorf("expected removing a required CRD to trigger the restart path")
	}

	// On restart, the controller validates required CRDs and fails fast
	startupErr := crdView.Snapshot().ValidateRequired()
	if startupErr == nil {
		t.Fatalf("expected startup validation to fail after removing HTTPRoute, got nil")
	}
	if !strings.Contains(startupErr.Error(), "HTTPRoute") {
		t.Errorf("expected error to name HTTPRoute, got: %v", startupErr)
	}
}

func TestCRDView_RemovingOptionalCRDTriggersRestart(t *testing.T) {
	ctx := t.Context()
	scheme := newTestSchemeWithApiextensions()

	initCRDs := append(requiredGatewayAPICRDs("v1.6.3"), makeTestCRD(TLSRouteCRDName, "v1.6.3"))
	var initObjs []runtime.Object
	for _, c := range initCRDs {
		initObjs = append(initObjs, c)
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(initObjs...).
		Build()

	var restarted bool
	crdView, err := NewCRDView(ctx, cl, WithRestartFunc(func() {
		restarted = true
	}))
	if err != nil {
		t.Fatalf("unexpected NewCRDView error: %v", err)
	}

	if !crdView.Snapshot().IsKindServed(schema.GroupKind{Group: gatewayv1.GroupName, Kind: "TLSRoute"}) {
		t.Fatalf("expected TLSRoute to be served initially")
	}

	// Remove optional CRD: TLSRoute
	tlsRouteCRD := makeTestCRD(TLSRouteCRDName, "")
	crdView.HandleDelete(tlsRouteCRD)

	if !restarted {
		t.Errorf("expected removing an optional CRD to trigger the restart path")
	}
	if crdView.Snapshot().IsKindServed(schema.GroupKind{Group: gatewayv1.GroupName, Kind: "TLSRoute"}) {
		t.Errorf("expected TLSRoute to not be served after deletion")
	}
}

func TestCRDView_UnrelatedCRDIgnored(t *testing.T) {
	ctx := t.Context()
	scheme := newTestSchemeWithApiextensions()

	initCRDs := requiredGatewayAPICRDs("v1.6.3")
	var initObjs []runtime.Object
	for _, c := range initCRDs {
		initObjs = append(initObjs, c)
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(initObjs...).
		Build()

	var restarted bool
	var bvChanged bool
	crdView, err := NewCRDView(ctx, cl, WithRestartFunc(func() {
		restarted = true
	}))
	if err != nil {
		t.Fatalf("unexpected NewCRDView error: %v", err)
	}
	crdView.OnBundleVersionChanged(func(newVersion *string) {
		bvChanged = true
	})

	// Add an unrelated CRD (e.g. cert-manager)
	unrelatedCRD := makeTestCRD("certificates.cert-manager.io", "v1.0.0")
	crdView.HandleAdd(unrelatedCRD)

	if restarted {
		t.Errorf("unrelated CRD should not trigger restart")
	}
	if bvChanged {
		t.Errorf("unrelated CRD should not trigger bundle version change")
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

	crdView := NewCRDViewFromSnapshot(&CRDSnapshot{})
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
	if gvk != crdGVK {
		t.Errorf("expected GVK %v, got %v", crdGVK, gvk)
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

func TestCRDView_FiltersManagedFieldsAndClientSideAnnotations(t *testing.T) {
	crd := &metav1.PartialObjectMetadata{
		TypeMeta: metav1.TypeMeta{
			APIVersion: crdGVK.GroupVersion().String(),
			Kind:       crdGVK.Kind,
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

	crdView := NewCRDViewFromSnapshot(&CRDSnapshot{})
	crdView.HandleAdd(crd)

	crdView.mu.RLock()
	stored, exists := crdView.crds[GatewayCRDName]
	crdView.mu.RUnlock()

	if !exists {
		t.Fatalf("expected Gateway CRD to be stored")
	}
	if _, hasLastApplied := stored.Annotations["kubectl.kubernetes.io/last-applied-configuration"]; hasLastApplied {
		t.Errorf("expected kubectl.kubernetes.io/last-applied-configuration to be filtered out")
	}
	if _, hasClientSide := stored.Annotations["kubectl.kubernetes.io/client-side-filter"]; hasClientSide {
		t.Errorf("expected kubectl.kubernetes.io/client-side-filter to be filtered out")
	}
	if bv, ok := stored.Annotations[gatewayconsts.BundleVersionAnnotation]; !ok || bv != "v1.6.3" {
		t.Errorf("expected bundle version annotation v1.6.3, got %q", bv)
	}
	if team, ok := stored.Annotations["custom.company.org/team"]; !ok || team != "networking" {
		t.Errorf("expected custom annotation to be preserved, got %q", team)
	}
}

func TestCRDView_DefaultDoesNotRestartOnServedKindsChange(t *testing.T) {
	ctx := t.Context()
	scheme := newTestSchemeWithApiextensions()

	initCRDs := requiredGatewayAPICRDs("v1.6.3")
	var initObjs []runtime.Object
	for _, c := range initCRDs {
		initObjs = append(initObjs, c)
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(initObjs...).
		Build()

	// Create CRDView without WithRestartFunc (default behavior)
	crdView, err := NewCRDView(ctx, cl)
	if err != nil {
		t.Fatalf("unexpected NewCRDView error: %v", err)
	}

	if crdView.Snapshot().IsKindServed(schema.GroupKind{Group: gatewayv1.GroupName, Kind: "TLSRoute"}) {
		t.Fatalf("expected TLSRoute not to be served initially")
	}

	// Install optional CRD TLSRoute at runtime
	tlsRouteCRD := makeTestCRD(TLSRouteCRDName, "v1.6.3")
	tlsRouteCRD.ResourceVersion = "2"

	// Should update snapshot and NOT panic or exit
	crdView.HandleAdd(tlsRouteCRD)

	if !crdView.Snapshot().IsKindServed(schema.GroupKind{Group: gatewayv1.GroupName, Kind: "TLSRoute"}) {
		t.Errorf("expected TLSRoute to be served in updated snapshot")
	}
}
