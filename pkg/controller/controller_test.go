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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/proxy"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

func TestGatewayClassReconciler_CustomControllerName(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

	gcManaged := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "managed-gc",
		},
		Spec: gatewayv1.GatewayClassSpec{
			ControllerName: "custom.domain/controller",
		},
	}
	gcIgnored := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ignored-gc",
		},
		Spec: gatewayv1.GatewayClassSpec{
			ControllerName: "other.domain/controller",
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(gcManaged, gcIgnored).
		WithStatusSubresource(gcManaged, gcIgnored).
		Build()

	r := &GatewayClassReconciler{
		Client:         client,
		Scheme:         scheme,
		ControllerName: "custom.domain/controller",
	}

	ctx := t.Context()

	// 1. Reconcile ignored class -> should not update status
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "ignored-gc"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var resIgnored gatewayv1.GatewayClass
	_ = client.Get(ctx, types.NamespacedName{Name: "ignored-gc"}, &resIgnored)
	if len(resIgnored.Status.Conditions) != 0 {
		t.Errorf("expected no conditions on ignored GatewayClass, got %d", len(resIgnored.Status.Conditions))
	}

	// 2. Reconcile managed class -> should update status to Accepted
	_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "managed-gc"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var resManaged gatewayv1.GatewayClass
	_ = client.Get(ctx, types.NamespacedName{Name: "managed-gc"}, &resManaged)
	if len(resManaged.Status.Conditions) == 0 {
		t.Fatalf("expected conditions on managed GatewayClass, got 0")
	}
	if resManaged.Status.Conditions[0].Type != string(gatewayv1.GatewayClassConditionStatusAccepted) ||
		resManaged.Status.Conditions[0].Status != metav1.ConditionTrue {
		t.Errorf("expected Accepted=True condition, got %+v", resManaged.Status.Conditions[0])
	}
}

func TestHTTPRouteReconciler_CustomControllerName(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

	st := state.NewState()
	p := proxy.NewProxy()

	customController := "custom.domain/controller"

	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-route",
			Namespace: "default",
		},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{
						Name: "some-gw",
					},
				},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(route).
		WithStatusSubresource(route).
		Build()

	r := &HTTPRouteReconciler{
		Client:         client,
		Scheme:         scheme,
		State:          st,
		Proxy:          p,
		ControllerName: customController,
	}

	ctx := t.Context()
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-route"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updatedRoute gatewayv1.HTTPRoute
	_ = client.Get(ctx, types.NamespacedName{Namespace: "default", Name: "test-route"}, &updatedRoute)
	if len(updatedRoute.Status.Parents) != 1 {
		t.Fatalf("expected 1 parent status, got %d", len(updatedRoute.Status.Parents))
	}
	if string(updatedRoute.Status.Parents[0].ControllerName) != customController {
		t.Errorf("expected ControllerName %q, got %q", customController, updatedRoute.Status.Parents[0].ControllerName)
	}
}

func TestServiceAndSecretAndConfigMapReconcilers(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)

	st := state.NewState()
	p := proxy.NewProxy()

	hookCalled := false
	onUpdate := func(gws []*gatewayv1.Gateway) {
		hookCalled = true
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-svc",
			Namespace: "default",
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-secret",
			Namespace: "default",
		},
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cm",
			Namespace: "default",
		},
	}
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "my-ns",
			Labels: map[string]string{"env": "test"},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(svc, secret, cm, ns).
		Build()

	svcReconciler := &ServiceReconciler{
		Client:           client,
		Scheme:           scheme,
		State:            st,
		Proxy:            p,
		ControllerName:   "test-controller",
		OnGatewaysUpdate: onUpdate,
	}
	secretReconciler := &SecretReconciler{
		Client:           client,
		Scheme:           scheme,
		State:            st,
		Proxy:            p,
		ControllerName:   "test-controller",
		OnGatewaysUpdate: onUpdate,
	}
	cmReconciler := &ConfigMapReconciler{
		Client:           client,
		Scheme:           scheme,
		State:            st,
		Proxy:            p,
		ControllerName:   "test-controller",
		OnGatewaysUpdate: onUpdate,
	}
	nsReconciler := &NamespaceReconciler{
		Client:           client,
		Scheme:           scheme,
		State:            st,
		Proxy:            p,
		ControllerName:   "test-controller",
		OnGatewaysUpdate: onUpdate,
	}

	ctx := t.Context()

	hookCalled = false
	_, err := svcReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "my-svc"}})
	if err != nil || !hookCalled {
		t.Fatalf("ServiceReconciler failed or hook not called: err=%v, hookCalled=%v", err, hookCalled)
	}

	hookCalled = false
	_, err = secretReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "my-secret"}})
	if err != nil || !hookCalled {
		t.Fatalf("SecretReconciler failed or hook not called: err=%v, hookCalled=%v", err, hookCalled)
	}

	hookCalled = false
	_, err = cmReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "my-cm"}})
	if err != nil || !hookCalled {
		t.Fatalf("ConfigMapReconciler failed or hook not called: err=%v, hookCalled=%v", err, hookCalled)
	}

	hookCalled = false
	_, err = nsReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "my-ns"}})
	if err != nil || !hookCalled {
		t.Fatalf("NamespaceReconciler failed or hook not called: err=%v, hookCalled=%v", err, hookCalled)
	}
	if len(st.GetNamespaces()) != 1 || st.GetNamespaces()["my-ns"] == nil {
		t.Fatalf("expected namespace in state, got %v", st.GetNamespaces())
	}
}

func TestReconcilerSetupWithManager_RequiresControllerName(t *testing.T) {
	reconcilers := []struct {
		name     string
		setupErr error
	}{
		{
			name:     "GatewayClassReconciler",
			setupErr: (&GatewayClassReconciler{}).SetupWithManager(nil),
		},
		{
			name:     "GatewayReconciler",
			setupErr: (&GatewayReconciler{}).SetupWithManager(nil),
		},
		{
			name:     "HTTPRouteReconciler",
			setupErr: (&HTTPRouteReconciler{}).SetupWithManager(nil),
		},
		{
			name:     "BackendTLSPolicyReconciler",
			setupErr: (&BackendTLSPolicyReconciler{}).SetupWithManager(nil),
		},
		{
			name:     "ServiceReconciler",
			setupErr: (&ServiceReconciler{}).SetupWithManager(nil),
		},
		{
			name:     "ConfigMapReconciler",
			setupErr: (&ConfigMapReconciler{}).SetupWithManager(nil),
		},
		{
			name:     "SecretReconciler",
			setupErr: (&SecretReconciler{}).SetupWithManager(nil),
		},
		{
			name:     "ReferenceGrantReconciler",
			setupErr: (&ReferenceGrantReconciler{}).SetupWithManager(nil),
		},
		{
			name:     "NamespaceReconciler",
			setupErr: (&NamespaceReconciler{}).SetupWithManager(nil),
		},
	}

	for _, tc := range reconcilers {
		if tc.setupErr == nil {
			t.Errorf("%s: expected error when ControllerName is empty, got nil", tc.name)
		}
	}
}

func generateTestCertPEM(t *testing.T) ([]byte, []byte) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate private key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test Org"},
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"example.com"},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	return certPEM, keyPEM
}

func TestGatewayReconciler_TLSReferenceGrant(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)
	_ = gatewayv1beta1.AddToScheme(scheme)

	st := state.NewState()
	p := proxy.NewProxy()

	certPEM, keyPEM := generateTestCertPEM(t)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cert",
			Namespace: "secret-ns",
		},
		Data: map[string][]byte{
			corev1.TLSCertKey:       certPEM,
			corev1.TLSPrivateKeyKey: keyPEM,
		},
	}
	st.UpsertSecret(secret)

	gwClass := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-gc",
		},
		Spec: gatewayv1.GatewayClassSpec{
			ControllerName: "test-controller",
		},
	}

	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-gw",
			Namespace: "gw-ns",
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "test-gc",
			Listeners: []gatewayv1.Listener{
				{
					Name:     "https",
					Port:     443,
					Protocol: gatewayv1.HTTPSProtocolType,
					TLS: &gatewayv1.ListenerTLSConfig{
						CertificateRefs: []gatewayv1.SecretObjectReference{
							{
								Namespace: state.Ptr(gatewayv1.Namespace("secret-ns")),
								Name:      "my-cert",
							},
						},
					},
				},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(gwClass, gw, secret).
		WithStatusSubresource(gw).
		Build()

	r := &GatewayReconciler{
		Client:         client,
		Scheme:         scheme,
		State:          st,
		Proxy:          p,
		ControllerName: "test-controller",
	}

	ctx := t.Context()

	// 1. Reconcile without ReferenceGrant -> ResolvedRefs should be False / RefNotPermitted
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "gw-ns", Name: "test-gw"}})
	if err != nil {
		t.Fatalf("unexpected error reconciling gateway: %v", err)
	}

	var reconciledGW gatewayv1.Gateway
	if err := client.Get(ctx, types.NamespacedName{Namespace: "gw-ns", Name: "test-gw"}, &reconciledGW); err != nil {
		t.Fatalf("failed to get gateway: %v", err)
	}

	if len(reconciledGW.Status.Listeners) != 1 {
		t.Fatalf("expected 1 listener status, got %d", len(reconciledGW.Status.Listeners))
	}
	resolvedRefsCond := findCondition(reconciledGW.Status.Listeners[0].Conditions, string(gatewayv1.ListenerConditionResolvedRefs))
	if resolvedRefsCond == nil {
		t.Fatalf("expected ResolvedRefs condition on listener, got none")
	}
	if resolvedRefsCond.Status != metav1.ConditionFalse || resolvedRefsCond.Reason != string(gatewayv1.ListenerReasonRefNotPermitted) {
		t.Errorf("expected ResolvedRefs=False/RefNotPermitted, got Status=%s, Reason=%s", resolvedRefsCond.Status, resolvedRefsCond.Reason)
	}

	// 2. Add ReferenceGrant permitting Gateway in gw-ns to access Secret in secret-ns
	rg := &gatewayv1beta1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "allow-gw-secret",
			Namespace: "secret-ns",
		},
		Spec: gatewayv1beta1.ReferenceGrantSpec{
			From: []gatewayv1beta1.ReferenceGrantFrom{
				{
					Group:     gatewayv1.GroupName,
					Kind:      "Gateway",
					Namespace: "gw-ns",
				},
			},
			To: []gatewayv1beta1.ReferenceGrantTo{
				{
					Group: "",
					Kind:  "Secret",
					Name:  state.Ptr(gatewayv1.ObjectName("my-cert")),
				},
			},
		},
	}
	st.UpsertReferenceGrant(rg)

	// Reconcile again -> ResolvedRefs should be True
	_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "gw-ns", Name: "test-gw"}})
	if err != nil {
		t.Fatalf("unexpected error reconciling gateway: %v", err)
	}

	if err := client.Get(ctx, types.NamespacedName{Namespace: "gw-ns", Name: "test-gw"}, &reconciledGW); err != nil {
		t.Fatalf("failed to get gateway: %v", err)
	}

	resolvedRefsCond = findCondition(reconciledGW.Status.Listeners[0].Conditions, string(gatewayv1.ListenerConditionResolvedRefs))
	if resolvedRefsCond == nil {
		t.Fatalf("expected ResolvedRefs condition on listener, got none")
	}
	if resolvedRefsCond.Status != metav1.ConditionTrue || resolvedRefsCond.Reason != string(gatewayv1.ListenerReasonResolvedRefs) {
		t.Errorf("expected ResolvedRefs=True/ResolvedRefs, got Status=%s, Reason=%s", resolvedRefsCond.Status, resolvedRefsCond.Reason)
	}
}

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}
