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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

func generateTestCertPEM(t *testing.T, hostnames ...string) ([]byte, []byte) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate private key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: hostnames[0],
		},
		DNSNames:  hostnames,
		NotBefore: time.Now().Add(-time.Hour),
		NotAfter:  time.Now().Add(time.Hour * 24),
		KeyUsage:  x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	return certPEM, keyPEM
}

func TestCompileModel_EffectiveListeners(t *testing.T) {
	sameFrom := gatewayv1.NamespacesFromSame
	noneFrom := gatewayv1.NamespacesFromNone
	allFrom := gatewayv1.NamespacesFromAll
	selectorFrom := gatewayv1.NamespacesFromSelector

	tests := []struct {
		name                   string
		gateway                *gatewayv1.Gateway
		listenerSets           []*gatewayv1.ListenerSet
		namespaces             map[string]*corev1.Namespace
		expectedListenersCount int
		expectedAttachedLS     int32
		expectedOwnerKinds     []gatewayv1.Kind
	}{
		{
			name: "Gateway without ListenerSets",
			gateway: &gatewayv1.Gateway{
				ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "default"},
				Spec: gatewayv1.GatewaySpec{
					Listeners: []gatewayv1.Listener{
						{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
					},
				},
			},
			expectedListenersCount: 1,
			expectedAttachedLS:     0,
			expectedOwnerKinds:     []gatewayv1.Kind{"Gateway"},
		},
		{
			name: "Gateway with AllowedListeners None rejects ListenerSets",
			gateway: &gatewayv1.Gateway{
				ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "default"},
				Spec: gatewayv1.GatewaySpec{
					AllowedListeners: &gatewayv1.AllowedListeners{
						Namespaces: &gatewayv1.ListenerNamespaces{From: &noneFrom},
					},
					Listeners: []gatewayv1.Listener{
						{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
					},
				},
			},
			listenerSets: []*gatewayv1.ListenerSet{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ls1", Namespace: "default"},
					Spec: gatewayv1.ListenerSetSpec{
						ParentRef: gatewayv1.ParentGatewayReference{Name: "gw"},
						Listeners: []gatewayv1.ListenerEntry{
							{Name: "ls-http", Port: 8080, Protocol: gatewayv1.HTTPProtocolType},
						},
					},
				},
			},
			expectedListenersCount: 1,
			expectedAttachedLS:     0,
			expectedOwnerKinds:     []gatewayv1.Kind{"Gateway"},
		},
		{
			name: "Gateway with AllowedListeners Same allows same-namespace ListenerSet only",
			gateway: &gatewayv1.Gateway{
				ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "default"},
				Spec: gatewayv1.GatewaySpec{
					AllowedListeners: &gatewayv1.AllowedListeners{
						Namespaces: &gatewayv1.ListenerNamespaces{From: &sameFrom},
					},
					Listeners: []gatewayv1.Listener{
						{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
					},
				},
			},
			listenerSets: []*gatewayv1.ListenerSet{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ls-same", Namespace: "default"},
					Spec: gatewayv1.ListenerSetSpec{
						ParentRef: gatewayv1.ParentGatewayReference{Name: "gw"},
						Listeners: []gatewayv1.ListenerEntry{
							{Name: "ls-http", Port: 8080, Protocol: gatewayv1.HTTPProtocolType},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ls-other", Namespace: "other"},
					Spec: gatewayv1.ListenerSetSpec{
						ParentRef: gatewayv1.ParentGatewayReference{Name: "gw", Namespace: Ptr(gatewayv1.Namespace("default"))},
						Listeners: []gatewayv1.ListenerEntry{
							{Name: "ls-other-http", Port: 8081, Protocol: gatewayv1.HTTPProtocolType},
						},
					},
				},
			},
			expectedListenersCount: 2,
			expectedAttachedLS:     1,
			expectedOwnerKinds:     []gatewayv1.Kind{"Gateway", "ListenerSet"},
		},
		{
			name: "Gateway with AllowedListeners All allows cross-namespace ListenerSets in precedence order",
			gateway: &gatewayv1.Gateway{
				ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "default"},
				Spec: gatewayv1.GatewaySpec{
					AllowedListeners: &gatewayv1.AllowedListeners{
						Namespaces: &gatewayv1.ListenerNamespaces{From: &allFrom},
					},
					Listeners: []gatewayv1.Listener{
						{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
					},
				},
			},
			listenerSets: []*gatewayv1.ListenerSet{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "ls-b",
						Namespace:         "ns-b",
						CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
					},
					Spec: gatewayv1.ListenerSetSpec{
						ParentRef: gatewayv1.ParentGatewayReference{Name: "gw", Namespace: Ptr(gatewayv1.Namespace("default"))},
						Listeners: []gatewayv1.ListenerEntry{
							{Name: "http-b", Port: 8081, Protocol: gatewayv1.HTTPProtocolType},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "ls-a",
						Namespace:         "ns-a",
						CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * time.Hour)),
					},
					Spec: gatewayv1.ListenerSetSpec{
						ParentRef: gatewayv1.ParentGatewayReference{Name: "gw", Namespace: Ptr(gatewayv1.Namespace("default"))},
						Listeners: []gatewayv1.ListenerEntry{
							{Name: "http-a", Port: 8080, Protocol: gatewayv1.HTTPProtocolType},
						},
					},
				},
			},
			expectedListenersCount: 3,
			expectedAttachedLS:     2,
			expectedOwnerKinds:     []gatewayv1.Kind{"Gateway", "ListenerSet", "ListenerSet"},
		},
		{
			name: "Gateway with AllowedListeners Selector filters by namespace labels",
			gateway: &gatewayv1.Gateway{
				ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "default"},
				Spec: gatewayv1.GatewaySpec{
					AllowedListeners: &gatewayv1.AllowedListeners{
						Namespaces: &gatewayv1.ListenerNamespaces{
							From: &selectorFrom,
							Selector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"env": "prod"},
							},
						},
					},
					Listeners: []gatewayv1.Listener{
						{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
					},
				},
			},
			namespaces: map[string]*corev1.Namespace{
				"prod-ns": {
					ObjectMeta: metav1.ObjectMeta{Name: "prod-ns", Labels: map[string]string{"env": "prod"}},
				},
				"dev-ns": {
					ObjectMeta: metav1.ObjectMeta{Name: "dev-ns", Labels: map[string]string{"env": "dev"}},
				},
			},
			listenerSets: []*gatewayv1.ListenerSet{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ls-prod", Namespace: "prod-ns"},
					Spec: gatewayv1.ListenerSetSpec{
						ParentRef: gatewayv1.ParentGatewayReference{Name: "gw", Namespace: Ptr(gatewayv1.Namespace("default"))},
						Listeners: []gatewayv1.ListenerEntry{
							{Name: "prod-http", Port: 8080, Protocol: gatewayv1.HTTPProtocolType},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ls-dev", Namespace: "dev-ns"},
					Spec: gatewayv1.ListenerSetSpec{
						ParentRef: gatewayv1.ParentGatewayReference{Name: "gw", Namespace: Ptr(gatewayv1.Namespace("default"))},
						Listeners: []gatewayv1.ListenerEntry{
							{Name: "dev-http", Port: 8081, Protocol: gatewayv1.HTTPProtocolType},
						},
					},
				},
			},
			expectedListenersCount: 2,
			expectedAttachedLS:     1,
			expectedOwnerKinds:     []gatewayv1.Kind{"Gateway", "ListenerSet"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compiled := CompileModel(ModelInputs{
				Gateways:     []*gatewayv1.Gateway{tt.gateway},
				ListenerSets: tt.listenerSets,
				Namespaces:   tt.namespaces,
			})

			gwKey := types.NamespacedName{Namespace: tt.gateway.Namespace, Name: tt.gateway.Name}
			cg := compiled.Gateways[gwKey]
			if cg == nil {
				t.Fatalf("expected compiled gateway for %v, got nil", gwKey)
			}

			if len(cg.EffectiveListeners) != tt.expectedListenersCount {
				t.Errorf("EffectiveListeners count = %d, want %d", len(cg.EffectiveListeners), tt.expectedListenersCount)
			}
			if cg.AttachedListenerSets != tt.expectedAttachedLS {
				t.Errorf("AttachedListenerSets = %d, want %d", cg.AttachedListenerSets, tt.expectedAttachedLS)
			}

			for i, el := range cg.EffectiveListeners {
				if i < len(tt.expectedOwnerKinds) && el.Owner.Kind != tt.expectedOwnerKinds[i] {
					t.Errorf("listener %d Owner.Kind = %s, want %s", i, el.Owner.Kind, tt.expectedOwnerKinds[i])
				}
			}
		})
	}
}

func TestCompileModel_RouteBinding(t *testing.T) {
	allFrom := gatewayv1.NamespacesFromAll
	sameFrom := gatewayv1.NamespacesFromSame
	selectorFrom := gatewayv1.NamespacesFromSelector

	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "default"},
		Spec: gatewayv1.GatewaySpec{
			AllowedListeners: &gatewayv1.AllowedListeners{
				Namespaces: &gatewayv1.ListenerNamespaces{From: &allFrom},
			},
			Listeners: []gatewayv1.Listener{
				{
					Name:     "http-80",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
					Hostname: Ptr(gatewayv1.Hostname("example.com")),
				},
				{
					Name:     "http-8080",
					Port:     8080,
					Protocol: gatewayv1.HTTPProtocolType,
					Hostname: Ptr(gatewayv1.Hostname("*.example.com")),
				},
				{
					Name:     "http-same-ns",
					Port:     8081,
					Protocol: gatewayv1.HTTPProtocolType,
					AllowedRoutes: &gatewayv1.AllowedRoutes{
						Namespaces: &gatewayv1.RouteNamespaces{From: &sameFrom},
					},
				},
				{
					Name:     "http-selector-ns",
					Port:     8082,
					Protocol: gatewayv1.HTTPProtocolType,
					AllowedRoutes: &gatewayv1.AllowedRoutes{
						Namespaces: &gatewayv1.RouteNamespaces{
							From: &selectorFrom,
							Selector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"team": "frontend"},
							},
						},
					},
				},
				{
					Name:     "tcp-9000",
					Port:     9000,
					Protocol: gatewayv1.TCPProtocolType,
				},
			},
		},
	}

	ls := &gatewayv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Name: "ls-app", Namespace: "app-ns"},
		Spec: gatewayv1.ListenerSetSpec{
			ParentRef: gatewayv1.ParentGatewayReference{Name: "gw", Namespace: Ptr(gatewayv1.Namespace("default"))},
			Listeners: []gatewayv1.ListenerEntry{
				{
					Name:     "ls-http",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
					Hostname: Ptr(gatewayv1.Hostname("app.example.com")),
				},
			},
		},
	}

	namespaces := map[string]*corev1.Namespace{
		"default": {
			ObjectMeta: metav1.ObjectMeta{Name: "default"},
		},
		"app-ns": {
			ObjectMeta: metav1.ObjectMeta{Name: "app-ns", Labels: map[string]string{"team": "frontend"}},
		},
		"other-ns": {
			ObjectMeta: metav1.ObjectMeta{Name: "other-ns", Labels: map[string]string{"team": "backend"}},
		},
	}

	tests := []struct {
		name               string
		route              *gatewayv1.HTTPRoute
		expectedStatus     metav1.ConditionStatus
		expectedReason     string
		expectedBoundCount int
	}{
		{
			name: "Route binds to Gateway listener with matching hostname",
			route: &gatewayv1.HTTPRoute{
				ObjectMeta: metav1.ObjectMeta{Name: "r1", Namespace: "default"},
				Spec: gatewayv1.HTTPRouteSpec{
					CommonRouteSpec: gatewayv1.CommonRouteSpec{
						ParentRefs: []gatewayv1.ParentReference{
							{Name: "gw"},
						},
					},
					Hostnames: []gatewayv1.Hostname{"example.com"},
				},
			},
			expectedStatus:     metav1.ConditionTrue,
			expectedReason:     string(gatewayv1.RouteReasonAccepted),
			expectedBoundCount: 1,
		},
		{
			name: "Route binds to Gateway listener by SectionName",
			route: &gatewayv1.HTTPRoute{
				ObjectMeta: metav1.ObjectMeta{Name: "r2", Namespace: "default"},
				Spec: gatewayv1.HTTPRouteSpec{
					CommonRouteSpec: gatewayv1.CommonRouteSpec{
						ParentRefs: []gatewayv1.ParentReference{
							{Name: "gw", SectionName: Ptr(gatewayv1.SectionName("http-8080"))},
						},
					},
					Hostnames: []gatewayv1.Hostname{"api.example.com"},
				},
			},
			expectedStatus:     metav1.ConditionTrue,
			expectedReason:     string(gatewayv1.RouteReasonAccepted),
			expectedBoundCount: 1,
		},
		{
			name: "Route fails with NoMatchingParent when SectionName does not exist",
			route: &gatewayv1.HTTPRoute{
				ObjectMeta: metav1.ObjectMeta{Name: "r3", Namespace: "default"},
				Spec: gatewayv1.HTTPRouteSpec{
					CommonRouteSpec: gatewayv1.CommonRouteSpec{
						ParentRefs: []gatewayv1.ParentReference{
							{Name: "gw", SectionName: Ptr(gatewayv1.SectionName("non-existent"))},
						},
					},
				},
			},
			expectedStatus:     metav1.ConditionFalse,
			expectedReason:     string(gatewayv1.RouteReasonNoMatchingParent),
			expectedBoundCount: 0,
		},
		{
			name: "Route fails with NoMatchingListenerHostname when hostnames are disjoint",
			route: &gatewayv1.HTTPRoute{
				ObjectMeta: metav1.ObjectMeta{Name: "r4", Namespace: "default"},
				Spec: gatewayv1.HTTPRouteSpec{
					CommonRouteSpec: gatewayv1.CommonRouteSpec{
						ParentRefs: []gatewayv1.ParentReference{
							{Name: "gw", SectionName: Ptr(gatewayv1.SectionName("http-80"))},
						},
					},
					Hostnames: []gatewayv1.Hostname{"otherdomain.org"},
				},
			},
			expectedStatus:     metav1.ConditionFalse,
			expectedReason:     string(gatewayv1.RouteReasonNoMatchingListenerHostname),
			expectedBoundCount: 0,
		},
		{
			name: "Route fails with NotAllowedByListeners for incompatible listener protocol",
			route: &gatewayv1.HTTPRoute{
				ObjectMeta: metav1.ObjectMeta{Name: "r5", Namespace: "default"},
				Spec: gatewayv1.HTTPRouteSpec{
					CommonRouteSpec: gatewayv1.CommonRouteSpec{
						ParentRefs: []gatewayv1.ParentReference{
							{Name: "gw", SectionName: Ptr(gatewayv1.SectionName("tcp-9000"))},
						},
					},
				},
			},
			expectedStatus:     metav1.ConditionFalse,
			expectedReason:     string(gatewayv1.RouteReasonNotAllowedByListeners),
			expectedBoundCount: 0,
		},
		{
			name: "Route in other namespace fails AllowedRoutes Same check",
			route: &gatewayv1.HTTPRoute{
				ObjectMeta: metav1.ObjectMeta{Name: "r6", Namespace: "other-ns"},
				Spec: gatewayv1.HTTPRouteSpec{
					CommonRouteSpec: gatewayv1.CommonRouteSpec{
						ParentRefs: []gatewayv1.ParentReference{
							{Name: "gw", Namespace: Ptr(gatewayv1.Namespace("default")), SectionName: Ptr(gatewayv1.SectionName("http-same-ns"))},
						},
					},
				},
			},
			expectedStatus:     metav1.ConditionFalse,
			expectedReason:     string(gatewayv1.RouteReasonNotAllowedByListeners),
			expectedBoundCount: 0,
		},
		{
			name: "Route in matching namespace passes AllowedRoutes Selector check",
			route: &gatewayv1.HTTPRoute{
				ObjectMeta: metav1.ObjectMeta{Name: "r7", Namespace: "app-ns"},
				Spec: gatewayv1.HTTPRouteSpec{
					CommonRouteSpec: gatewayv1.CommonRouteSpec{
						ParentRefs: []gatewayv1.ParentReference{
							{Name: "gw", Namespace: Ptr(gatewayv1.Namespace("default")), SectionName: Ptr(gatewayv1.SectionName("http-selector-ns"))},
						},
					},
				},
			},
			expectedStatus:     metav1.ConditionTrue,
			expectedReason:     string(gatewayv1.RouteReasonAccepted),
			expectedBoundCount: 1,
		},
		{
			name: "Route binds to ListenerSet parentRef",
			route: &gatewayv1.HTTPRoute{
				ObjectMeta: metav1.ObjectMeta{Name: "r8", Namespace: "app-ns"},
				Spec: gatewayv1.HTTPRouteSpec{
					CommonRouteSpec: gatewayv1.CommonRouteSpec{
						ParentRefs: []gatewayv1.ParentReference{
							{
								Group: Ptr(gatewayv1.Group(gatewayv1.GroupName)),
								Kind:  Ptr(gatewayv1.Kind("ListenerSet")),
								Name:  "ls-app",
							},
						},
					},
					Hostnames: []gatewayv1.Hostname{"app.example.com"},
				},
			},
			expectedStatus:     metav1.ConditionTrue,
			expectedReason:     string(gatewayv1.RouteReasonAccepted),
			expectedBoundCount: 1,
		},
		{
			name: "Route fails when ListenerSet parentRef does not exist",
			route: &gatewayv1.HTTPRoute{
				ObjectMeta: metav1.ObjectMeta{Name: "r9", Namespace: "app-ns"},
				Spec: gatewayv1.HTTPRouteSpec{
					CommonRouteSpec: gatewayv1.CommonRouteSpec{
						ParentRefs: []gatewayv1.ParentReference{
							{
								Group: Ptr(gatewayv1.Group(gatewayv1.GroupName)),
								Kind:  Ptr(gatewayv1.Kind("ListenerSet")),
								Name:  "non-existent-ls",
							},
						},
					},
				},
			},
			expectedStatus:     metav1.ConditionFalse,
			expectedReason:     string(gatewayv1.RouteReasonNoMatchingParent),
			expectedBoundCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compiled := CompileModel(ModelInputs{
				Gateways:     []*gatewayv1.Gateway{gw},
				ListenerSets: []*gatewayv1.ListenerSet{ls},
				HTTPRoutes:   []*gatewayv1.HTTPRoute{tt.route},
				Namespaces:   namespaces,
			})

			rKey := types.NamespacedName{Namespace: tt.route.Namespace, Name: tt.route.Name}
			cRoute := compiled.HTTPRoutes[rKey]
			if cRoute == nil {
				t.Fatalf("expected compiled route for %v, got nil", rKey)
			}

			if len(cRoute.ParentConditions) == 0 {
				t.Fatalf("expected at least 1 parent condition, got 0")
			}

			cond := cRoute.ParentConditions[0]
			if cond.Status != tt.expectedStatus {
				t.Errorf("ParentCondition Status = %s, want %s (message: %s)", cond.Status, tt.expectedStatus, cond.Message)
			}
			if cond.Reason != tt.expectedReason {
				t.Errorf("ParentCondition Reason = %s, want %s", cond.Reason, tt.expectedReason)
			}
		})
	}
}

func TestExtractCertificates(t *testing.T) {
	certPEM1, keyPEM1 := generateTestCertPEM(t, "example.com")
	certPEM2, keyPEM2 := generateTestCertPEM(t, "app.example.com")

	secrets := map[types.NamespacedName]*corev1.Secret{
		{Namespace: "gw-ns", Name: "gw-cert"}: {
			ObjectMeta: metav1.ObjectMeta{Namespace: "gw-ns", Name: "gw-cert"},
			Data: map[string][]byte{
				corev1.TLSCertKey:       certPEM1,
				corev1.TLSPrivateKeyKey: keyPEM1,
			},
		},
		{Namespace: "ls-ns", Name: "ls-cert"}: {
			ObjectMeta: metav1.ObjectMeta{Namespace: "ls-ns", Name: "ls-cert"},
			Data: map[string][]byte{
				corev1.TLSCertKey:       certPEM2,
				corev1.TLSPrivateKeyKey: keyPEM2,
			},
		},
		{Namespace: "secret-ns", Name: "cross-cert"}: {
			ObjectMeta: metav1.ObjectMeta{Namespace: "secret-ns", Name: "cross-cert"},
			Data: map[string][]byte{
				corev1.TLSCertKey:       certPEM1,
				corev1.TLSPrivateKeyKey: keyPEM1,
			},
		},
	}

	allFrom := gatewayv1.NamespacesFromAll
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "gw-ns"},
		Spec: gatewayv1.GatewaySpec{
			AllowedListeners: &gatewayv1.AllowedListeners{
				Namespaces: &gatewayv1.ListenerNamespaces{From: &allFrom},
			},
			Listeners: []gatewayv1.Listener{
				{
					Name:     "https-gw",
					Port:     443,
					Protocol: gatewayv1.HTTPSProtocolType,
					Hostname: Ptr(gatewayv1.Hostname("example.com")),
					TLS: &gatewayv1.ListenerTLSConfig{
						CertificateRefs: []gatewayv1.SecretObjectReference{
							{Name: "gw-cert"},
						},
					},
				},
				{
					Name:     "https-cross",
					Port:     8443,
					Protocol: gatewayv1.HTTPSProtocolType,
					Hostname: Ptr(gatewayv1.Hostname("cross.example.com")),
					TLS: &gatewayv1.ListenerTLSConfig{
						CertificateRefs: []gatewayv1.SecretObjectReference{
							{
								Namespace: Ptr(gatewayv1.Namespace("secret-ns")),
								Name:      "cross-cert",
							},
						},
					},
				},
			},
		},
	}

	ls := &gatewayv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Name: "ls", Namespace: "ls-ns"},
		Spec: gatewayv1.ListenerSetSpec{
			ParentRef: gatewayv1.ParentGatewayReference{Name: "gw", Namespace: Ptr(gatewayv1.Namespace("gw-ns"))},
			Listeners: []gatewayv1.ListenerEntry{
				{
					Name:     "https-ls",
					Port:     9443,
					Protocol: gatewayv1.HTTPSProtocolType,
					Hostname: Ptr(gatewayv1.Hostname("app.example.com")),
					TLS: &gatewayv1.ListenerTLSConfig{
						CertificateRefs: []gatewayv1.SecretObjectReference{
							{Name: "ls-cert"},
						},
					},
				},
			},
		},
	}

	st := NewState()
	// Allow cross-namespace Secret reference for Gateway
	st.UpsertReferenceGrant(&gatewayv1beta1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: "secret-ns", Name: "allow-gw"},
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
					Name:  Ptr(gatewayv1.ObjectName("cross-cert")),
				},
			},
		},
	})

	compiled := CompileModel(ModelInputs{
		Gateways:     []*gatewayv1.Gateway{gw},
		ListenerSets: []*gatewayv1.ListenerSet{ls},
		Secrets:      secrets,
		RefValidator: st,
	})

	certsMap, defaultCert := ExtractCertificates(compiled.GatewaysList(), secrets, st)

	if certsMap["example.com"] == nil {
		t.Errorf("expected certificate for example.com")
	}
	if certsMap["app.example.com"] == nil {
		t.Errorf("expected certificate for app.example.com")
	}
	if certsMap["cross.example.com"] == nil {
		t.Errorf("expected certificate for cross.example.com permitted by ReferenceGrant")
	}
	if defaultCert == nil {
		t.Errorf("expected defaultCert to be set")
	}
}

func TestCompileModel_AttachedRoutesCount(t *testing.T) {
	allFrom := gatewayv1.NamespacesFromAll
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "default"},
		Spec: gatewayv1.GatewaySpec{
			AllowedListeners: &gatewayv1.AllowedListeners{
				Namespaces: &gatewayv1.ListenerNamespaces{From: &allFrom},
			},
			Listeners: []gatewayv1.Listener{
				{
					Name:     "http",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
				},
			},
		},
	}

	ls := &gatewayv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Name: "ls", Namespace: "default"},
		Spec: gatewayv1.ListenerSetSpec{
			ParentRef: gatewayv1.ParentGatewayReference{Name: "gw"},
			Listeners: []gatewayv1.ListenerEntry{
				{
					Name:     "http-ls",
					Port:     8080,
					Protocol: gatewayv1.HTTPProtocolType,
				},
			},
		},
	}

	r1 := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "r1", Namespace: "default"},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{Name: "gw"},
				},
			},
		},
	}

	r2 := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "r2", Namespace: "default"},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{Name: "gw"},
					// Duplicate parentRef to same gateway
					{Name: "gw", SectionName: Ptr(gatewayv1.SectionName("http"))},
				},
			},
		},
	}

	r3 := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "r3", Namespace: "default"},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{
						Group: Ptr(gatewayv1.Group(gatewayv1.GroupName)),
						Kind:  Ptr(gatewayv1.Kind("ListenerSet")),
						Name:  "ls",
					},
				},
			},
		},
	}

	compiled := CompileModel(ModelInputs{
		Gateways:     []*gatewayv1.Gateway{gw},
		ListenerSets: []*gatewayv1.ListenerSet{ls},
		HTTPRoutes:   []*gatewayv1.HTTPRoute{r1, r2, r3},
	})

	cg := compiled.Gateways[types.NamespacedName{Namespace: "default", Name: "gw"}]
	if cg == nil {
		t.Fatalf("expected compiled gateway, got nil")
	}

	var gwEl *EffectiveListener
	var lsEl *EffectiveListener
	for _, el := range cg.EffectiveListeners {
		if el.Owner.Kind == "Gateway" && el.Name == "http" {
			gwEl = el
		}
		if el.Owner.Kind == "ListenerSet" && el.Name == "http-ls" {
			lsEl = el
		}
	}

	if gwEl == nil || gwEl.AttachedRoutes != 2 {
		t.Errorf("expected 2 attached routes on gw listener, got %v", gwEl)
	}
	if lsEl == nil || lsEl.AttachedRoutes != 1 {
		t.Errorf("expected 1 attached route on ls listener, got %v", lsEl)
	}
}

func TestStatusComputation_PureFunctions(t *testing.T) {
	sameFrom := gatewayv1.NamespacesFromSame
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "default", Generation: 1},
		Spec: gatewayv1.GatewaySpec{
			AllowedListeners: &gatewayv1.AllowedListeners{
				Namespaces: &gatewayv1.ListenerNamespaces{From: &sameFrom},
			},
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}

	ls := &gatewayv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Name: "ls", Namespace: "default", Generation: 1},
		Spec: gatewayv1.ListenerSetSpec{
			ParentRef: gatewayv1.ParentGatewayReference{Name: "gw"},
			Listeners: []gatewayv1.ListenerEntry{
				{Name: "http-ls", Port: 8080, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}

	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "r1", Namespace: "default", Generation: 1},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{Name: "gw"},
					{
						Group: Ptr(gatewayv1.Group(gatewayv1.GroupName)),
						Kind:  Ptr(gatewayv1.Kind("ListenerSet")),
						Name:  "ls",
					},
				},
			},
		},
	}

	compiled := CompileModel(ModelInputs{
		Gateways:     []*gatewayv1.Gateway{gw},
		ListenerSets: []*gatewayv1.ListenerSet{ls},
		HTTPRoutes:   []*gatewayv1.HTTPRoute{route},
		Namespaces:   map[string]*corev1.Namespace{"default": {ObjectMeta: metav1.ObjectMeta{Name: "default"}}},
	})

	// 1. Gateway Status
	addresses := []gatewayv1.GatewayStatusAddress{{Value: "192.0.2.1"}}
	gwStatus := ComputeDesiredGatewayStatus(gw, compiled.Gateways[types.NamespacedName{Namespace: "default", Name: "gw"}], addresses)
	if len(gwStatus.Conditions) != 2 {
		t.Errorf("expected 2 Gateway conditions, got %d", len(gwStatus.Conditions))
	}
	if len(gwStatus.Addresses) != 1 || gwStatus.Addresses[0].Value != "192.0.2.1" {
		t.Errorf("expected Gateway address 192.0.2.1, got %v", gwStatus.Addresses)
	}
	if gwStatus.AttachedListenerSets == nil || *gwStatus.AttachedListenerSets != 1 {
		t.Errorf("expected AttachedListenerSets = 1, got %v", gwStatus.AttachedListenerSets)
	}
	if len(gwStatus.Listeners) != 1 || gwStatus.Listeners[0].AttachedRoutes != 1 {
		t.Errorf("expected 1 attached route on Gateway listener status, got %v", gwStatus.Listeners)
	}

	// 2. ListenerSet Status
	lsStatus := ComputeDesiredListenerSetStatus(ls, gw, nil, compiled.Gateways[types.NamespacedName{Namespace: "default", Name: "gw"}])
	if len(lsStatus.Conditions) != 2 {
		t.Errorf("expected 2 ListenerSet conditions, got %d", len(lsStatus.Conditions))
	}
	if len(lsStatus.Listeners) != 1 || lsStatus.Listeners[0].AttachedRoutes != 1 {
		t.Errorf("expected 1 attached route on ListenerSet listener status, got %v", lsStatus.Listeners)
	}

	// 3. HTTPRoute Status
	routeStatus := ComputeDesiredHTTPRouteStatus(route, compiled.HTTPRoutes[types.NamespacedName{Namespace: "default", Name: "r1"}], "example.net/gateway-controller")
	if len(routeStatus.Parents) != 2 {
		t.Fatalf("expected 2 parent statuses on route, got %d", len(routeStatus.Parents))
	}
	for i, p := range routeStatus.Parents {
		if len(p.Conditions) != 2 {
			t.Errorf("parent %d expected 2 conditions, got %d", i, len(p.Conditions))
		}
	}
}

func TestValidateListener_CertificateRefGroup(t *testing.T) {
	certPEM, keyPEM := generateTestCertPEM(t, "example.com")
	secrets := map[types.NamespacedName]*corev1.Secret{
		{Namespace: "default", Name: "my-secret"}: {
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "my-secret"},
			Data: map[string][]byte{
				corev1.TLSCertKey:       certPEM,
				corev1.TLSPrivateKeyKey: keyPEM,
			},
		},
	}

	coreGroup := gatewayv1.Group("core")
	emptyGroup := gatewayv1.Group("")
	customGroup := gatewayv1.Group("example.com")

	tests := []struct {
		name               string
		group              *gatewayv1.Group
		kind               *gatewayv1.Kind
		expectProgrammed   metav1.ConditionStatus
		expectResolvedRefs metav1.ConditionStatus
		expectReason       string
	}{
		{
			name:               "nil group accepted",
			group:              nil,
			kind:               Ptr(gatewayv1.Kind("Secret")),
			expectProgrammed:   metav1.ConditionTrue,
			expectResolvedRefs: metav1.ConditionTrue,
			expectReason:       string(gatewayv1.ListenerReasonResolvedRefs),
		},
		{
			name:               "empty group accepted",
			group:              &emptyGroup,
			kind:               Ptr(gatewayv1.Kind("Secret")),
			expectProgrammed:   metav1.ConditionTrue,
			expectResolvedRefs: metav1.ConditionTrue,
			expectReason:       string(gatewayv1.ListenerReasonResolvedRefs),
		},
		{
			name:               "core group rejected",
			group:              &coreGroup,
			kind:               Ptr(gatewayv1.Kind("Secret")),
			expectProgrammed:   metav1.ConditionFalse,
			expectResolvedRefs: metav1.ConditionFalse,
			expectReason:       string(gatewayv1.ListenerReasonInvalidCertificateRef),
		},
		{
			name:               "custom group rejected",
			group:              &customGroup,
			kind:               Ptr(gatewayv1.Kind("Secret")),
			expectProgrammed:   metav1.ConditionFalse,
			expectResolvedRefs: metav1.ConditionFalse,
			expectReason:       string(gatewayv1.ListenerReasonInvalidCertificateRef),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			listener := gatewayv1.Listener{
				Name:     "https",
				Port:     443,
				Protocol: gatewayv1.HTTPSProtocolType,
				TLS: &gatewayv1.ListenerTLSConfig{
					CertificateRefs: []gatewayv1.SecretObjectReference{
						{
							Group: tc.group,
							Kind:  tc.kind,
							Name:  "my-secret",
						},
					},
				},
			}

			_, conds := ValidateListener(
				ListenerToSpec(listener),
				ListenerOwner{Kind: "Gateway", Namespace: "default", Name: "gw"},
				1,
				secrets,
				nil,
			)

			var progCond, resCond *metav1.Condition
			for i := range conds {
				if conds[i].Type == string(gatewayv1.ListenerConditionProgrammed) {
					progCond = &conds[i]
				}
				if conds[i].Type == string(gatewayv1.ListenerConditionResolvedRefs) {
					resCond = &conds[i]
				}
			}

			if progCond == nil || progCond.Status != tc.expectProgrammed {
				t.Errorf("expected Programmed condition %v, got %v", tc.expectProgrammed, progCond)
			}
			if resCond == nil || resCond.Status != tc.expectResolvedRefs {
				t.Errorf("expected ResolvedRefs condition %v, got %v", tc.expectResolvedRefs, resCond)
			}
			if resCond != nil && resCond.Reason != tc.expectReason {
				t.Errorf("expected ResolvedRefs reason %q, got %q", tc.expectReason, resCond.Reason)
			}
		})
	}
}

func TestCompileModel_PrecedenceAndConflicts(t *testing.T) {
	sameFrom := gatewayv1.NamespacesFromSame
	allFrom := gatewayv1.NamespacesFromAll
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(10 * time.Minute)
	t2 := t0.Add(20 * time.Minute)

	tests := []struct {
		name                 string
		gateway              *gatewayv1.Gateway
		listenerSets         []*gatewayv1.ListenerSet
		expectedAttachedLS   int32
		expectedConflicted   map[string]gatewayv1.ListenerConditionReason
		expectedUnconflicted []string
		expectedLSStatus     map[string]metav1.ConditionStatus
		expectedLSReason     map[string]string
	}{
		{
			name: "Hostname conflict: Gateway listener wins over ListenerSet listener",
			gateway: &gatewayv1.Gateway{
				ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "default"},
				Spec: gatewayv1.GatewaySpec{
					AllowedListeners: &gatewayv1.AllowedListeners{
						Namespaces: &gatewayv1.ListenerNamespaces{From: &sameFrom},
					},
					Listeners: []gatewayv1.Listener{
						{Name: "gw-http", Port: 80, Protocol: gatewayv1.HTTPProtocolType, Hostname: Ptr(gatewayv1.Hostname("example.com"))},
					},
				},
			},
			listenerSets: []*gatewayv1.ListenerSet{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ls1", Namespace: "default", CreationTimestamp: metav1.NewTime(t0)},
					Spec: gatewayv1.ListenerSetSpec{
						ParentRef: gatewayv1.ParentGatewayReference{Name: "gw"},
						Listeners: []gatewayv1.ListenerEntry{
							{Name: "ls-conflict", Port: 80, Protocol: gatewayv1.HTTPProtocolType, Hostname: Ptr(gatewayv1.Hostname("example.com"))},
						},
					},
				},
			},
			expectedAttachedLS: 0,
			expectedConflicted: map[string]gatewayv1.ListenerConditionReason{
				"default/ls1/ls-conflict": gatewayv1.ListenerReasonHostnameConflict,
			},
			expectedUnconflicted: []string{"gw-http"},
			expectedLSStatus: map[string]metav1.ConditionStatus{
				"ls1": metav1.ConditionFalse,
			},
			expectedLSReason: map[string]string{
				"ls1": string(gatewayv1.ListenerSetReasonListenersNotValid),
			},
		},
		{
			name: "Protocol conflict: Gateway HTTP wins over ListenerSet TCP on port 80",
			gateway: &gatewayv1.Gateway{
				ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "default"},
				Spec: gatewayv1.GatewaySpec{
					AllowedListeners: &gatewayv1.AllowedListeners{
						Namespaces: &gatewayv1.ListenerNamespaces{From: &sameFrom},
					},
					Listeners: []gatewayv1.Listener{
						{Name: "gw-http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
					},
				},
			},
			listenerSets: []*gatewayv1.ListenerSet{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ls1", Namespace: "default", CreationTimestamp: metav1.NewTime(t0)},
					Spec: gatewayv1.ListenerSetSpec{
						ParentRef: gatewayv1.ParentGatewayReference{Name: "gw"},
						Listeners: []gatewayv1.ListenerEntry{
							{Name: "ls-tcp", Port: 80, Protocol: gatewayv1.TCPProtocolType},
						},
					},
				},
			},
			expectedAttachedLS: 0,
			expectedConflicted: map[string]gatewayv1.ListenerConditionReason{
				"default/ls1/ls-tcp": gatewayv1.ListenerReasonProtocolConflict,
			},
			expectedUnconflicted: []string{"gw-http"},
			expectedLSStatus: map[string]metav1.ConditionStatus{
				"ls1": metav1.ConditionFalse,
			},
			expectedLSReason: map[string]string{
				"ls1": string(gatewayv1.ListenerSetReasonListenersNotValid),
			},
		},
		{
			name: "Precedence by CreationTimestamp: older ListenerSet wins hostname conflict over younger",
			gateway: &gatewayv1.Gateway{
				ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "default"},
				Spec: gatewayv1.GatewaySpec{
					AllowedListeners: &gatewayv1.AllowedListeners{
						Namespaces: &gatewayv1.ListenerNamespaces{From: &allFrom},
					},
					Listeners: []gatewayv1.Listener{
						{Name: "gw-other", Port: 8080, Protocol: gatewayv1.HTTPProtocolType},
					},
				},
			},
			listenerSets: []*gatewayv1.ListenerSet{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ls-younger", Namespace: "default", CreationTimestamp: metav1.NewTime(t2)},
					Spec: gatewayv1.ListenerSetSpec{
						ParentRef: gatewayv1.ParentGatewayReference{Name: "gw"},
						Listeners: []gatewayv1.ListenerEntry{
							{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType, Hostname: Ptr(gatewayv1.Hostname("app.io"))},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ls-older", Namespace: "default", CreationTimestamp: metav1.NewTime(t1)},
					Spec: gatewayv1.ListenerSetSpec{
						ParentRef: gatewayv1.ParentGatewayReference{Name: "gw"},
						Listeners: []gatewayv1.ListenerEntry{
							{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType, Hostname: Ptr(gatewayv1.Hostname("app.io"))},
						},
					},
				},
			},
			expectedAttachedLS: 1,
			expectedConflicted: map[string]gatewayv1.ListenerConditionReason{
				"default/ls-younger/http": gatewayv1.ListenerReasonHostnameConflict,
			},
			expectedUnconflicted: []string{"gw-other", "default/ls-older/http"},
			expectedLSStatus: map[string]metav1.ConditionStatus{
				"ls-older":   metav1.ConditionTrue,
				"ls-younger": metav1.ConditionFalse,
			},
			expectedLSReason: map[string]string{
				"ls-older":   string(gatewayv1.ListenerSetReasonAccepted),
				"ls-younger": string(gatewayv1.ListenerSetReasonListenersNotValid),
			},
		},
		{
			name: "Partial vs full conflict: ListenerSet with one valid listener stays Accepted=True",
			gateway: &gatewayv1.Gateway{
				ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "default"},
				Spec: gatewayv1.GatewaySpec{
					AllowedListeners: &gatewayv1.AllowedListeners{
						Namespaces: &gatewayv1.ListenerNamespaces{From: &sameFrom},
					},
					Listeners: []gatewayv1.Listener{
						{Name: "gw-http", Port: 80, Protocol: gatewayv1.HTTPProtocolType, Hostname: Ptr(gatewayv1.Hostname("gw.io"))},
					},
				},
			},
			listenerSets: []*gatewayv1.ListenerSet{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ls-mixed", Namespace: "default", CreationTimestamp: metav1.NewTime(t0)},
					Spec: gatewayv1.ListenerSetSpec{
						ParentRef: gatewayv1.ParentGatewayReference{Name: "gw"},
						Listeners: []gatewayv1.ListenerEntry{
							{Name: "conflicted-http", Port: 80, Protocol: gatewayv1.HTTPProtocolType, Hostname: Ptr(gatewayv1.Hostname("gw.io"))},
							{Name: "valid-http", Port: 80, Protocol: gatewayv1.HTTPProtocolType, Hostname: Ptr(gatewayv1.Hostname("valid.io"))},
						},
					},
				},
			},
			expectedAttachedLS: 1,
			expectedConflicted: map[string]gatewayv1.ListenerConditionReason{
				"default/ls-mixed/conflicted-http": gatewayv1.ListenerReasonHostnameConflict,
			},
			expectedUnconflicted: []string{"gw-http", "default/ls-mixed/valid-http"},
			expectedLSStatus: map[string]metav1.ConditionStatus{
				"ls-mixed": metav1.ConditionTrue,
			},
			expectedLSReason: map[string]string{
				"ls-mixed": string(gatewayv1.ListenerSetReasonAccepted),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			compiled := CompileModel(ModelInputs{
				Gateways:     []*gatewayv1.Gateway{tc.gateway},
				ListenerSets: tc.listenerSets,
				Namespaces:   map[string]*corev1.Namespace{"default": {ObjectMeta: metav1.ObjectMeta{Name: "default"}}},
			})

			gwKey := types.NamespacedName{Namespace: tc.gateway.Namespace, Name: tc.gateway.Name}
			cg := compiled.Gateways[gwKey]
			if cg == nil {
				t.Fatalf("compiled gateway not found")
			}

			if cg.AttachedListenerSets != tc.expectedAttachedLS {
				t.Errorf("expected AttachedListenerSets = %d, got %d", tc.expectedAttachedLS, cg.AttachedListenerSets)
			}

			// Check listeners
			for _, el := range cg.EffectiveListeners {
				qName := el.QualifiedName()
				expectedReason, shouldBeConflicted := tc.expectedConflicted[qName]
				if shouldBeConflicted {
					if !el.IsConflicted() {
						t.Errorf("listener %s expected to be conflicted", qName)
					}
					var confCond, accCond, progCond *metav1.Condition
					for i := range el.Conditions {
						if el.Conditions[i].Type == string(gatewayv1.ListenerConditionConflicted) {
							confCond = &el.Conditions[i]
						}
						if el.Conditions[i].Type == string(gatewayv1.ListenerConditionAccepted) {
							accCond = &el.Conditions[i]
						}
						if el.Conditions[i].Type == string(gatewayv1.ListenerConditionProgrammed) {
							progCond = &el.Conditions[i]
						}
					}
					if confCond == nil || confCond.Status != metav1.ConditionTrue || confCond.Reason != string(expectedReason) {
						t.Errorf("listener %s expected Conflicted=True reason %s, got %v", qName, expectedReason, confCond)
					}
					if accCond == nil || accCond.Status != metav1.ConditionFalse || accCond.Reason != string(expectedReason) {
						t.Errorf("listener %s expected Accepted=False reason %s, got %v", qName, expectedReason, accCond)
					}
					if progCond == nil || progCond.Status != metav1.ConditionFalse || progCond.Reason != string(expectedReason) {
						t.Errorf("listener %s expected Programmed=False reason %s, got %v", qName, expectedReason, progCond)
					}
				}
			}

			for _, unconf := range tc.expectedUnconflicted {
				var found *EffectiveListener
				for _, el := range cg.EffectiveListeners {
					if el.QualifiedName() == unconf {
						found = el
						break
					}
				}
				if found == nil {
					t.Fatalf("unconflicted listener %s not found in effective listeners", unconf)
				}
				if found.IsConflicted() {
					t.Errorf("listener %s expected to NOT be conflicted, but is", unconf)
				}
				if !found.IsAccepted() {
					t.Errorf("listener %s expected to be accepted, but is not", unconf)
				}
			}

			// Check ListenerSet desired status
			for _, ls := range tc.listenerSets {
				lsStatus := ComputeDesiredListenerSetStatus(ls, tc.gateway, map[string]*corev1.Namespace{"default": {ObjectMeta: metav1.ObjectMeta{Name: "default"}}}, cg)
				expectedStatus, ok := tc.expectedLSStatus[ls.Name]
				if ok {
					var accCond *metav1.Condition
					for i := range lsStatus.Conditions {
						if lsStatus.Conditions[i].Type == string(gatewayv1.ListenerSetConditionAccepted) {
							accCond = &lsStatus.Conditions[i]
						}
					}
					if accCond == nil || accCond.Status != expectedStatus {
						t.Errorf("ListenerSet %s expected Accepted=%v, got %v", ls.Name, expectedStatus, accCond)
					}
					if expectedReason, ok := tc.expectedLSReason[ls.Name]; ok {
						if accCond != nil && accCond.Reason != expectedReason {
							t.Errorf("ListenerSet %s expected Reason=%s, got %s", ls.Name, expectedReason, accCond.Reason)
						}
					}
				}
			}
		})
	}
}

func TestCompileModel_ListenerSetReferenceGrant(t *testing.T) {
	certPEM, keyPEM := generateTestCertPEM(t, "cross-ns.example.com")
	secrets := map[types.NamespacedName]*corev1.Secret{
		{Namespace: "secret-ns", Name: "my-secret"}: {
			ObjectMeta: metav1.ObjectMeta{Namespace: "secret-ns", Name: "my-secret"},
			Data: map[string][]byte{
				corev1.TLSCertKey:       certPEM,
				corev1.TLSPrivateKeyKey: keyPEM,
			},
		},
	}

	stWithGrant := NewState()
	stWithGrant.UpsertReferenceGrant(&gatewayv1beta1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Name: "grant", Namespace: "secret-ns"},
		Spec: gatewayv1beta1.ReferenceGrantSpec{
			From: []gatewayv1beta1.ReferenceGrantFrom{
				{
					Group:     gatewayv1.GroupName,
					Kind:      "ListenerSet",
					Namespace: "ls-ns",
				},
			},
			To: []gatewayv1beta1.ReferenceGrantTo{
				{
					Group: "",
					Kind:  "Secret",
					Name:  Ptr(gatewayv1.ObjectName("my-secret")),
				},
			},
		},
	})

	allFrom := gatewayv1.NamespacesFromAll
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "gw-ns"},
		Spec: gatewayv1.GatewaySpec{
			AllowedListeners: &gatewayv1.AllowedListeners{
				Namespaces: &gatewayv1.ListenerNamespaces{From: &allFrom},
			},
			Listeners: []gatewayv1.Listener{
				{Name: "http", Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
	}

	ls := &gatewayv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Name: "ls-tls", Namespace: "ls-ns"},
		Spec: gatewayv1.ListenerSetSpec{
			ParentRef: gatewayv1.ParentGatewayReference{
				Name:      "gw",
				Namespace: Ptr(gatewayv1.Namespace("gw-ns")),
			},
			Listeners: []gatewayv1.ListenerEntry{
				{
					Name:     "https",
					Port:     443,
					Protocol: gatewayv1.HTTPSProtocolType,
					TLS: &gatewayv1.ListenerTLSConfig{
						CertificateRefs: []gatewayv1.SecretObjectReference{
							{
								Namespace: Ptr(gatewayv1.Namespace("secret-ns")),
								Name:      "my-secret",
							},
						},
					},
				},
			},
		},
	}

	namespaces := map[string]*corev1.Namespace{
		"gw-ns":     {ObjectMeta: metav1.ObjectMeta{Name: "gw-ns"}},
		"ls-ns":     {ObjectMeta: metav1.ObjectMeta{Name: "ls-ns"}},
		"secret-ns": {ObjectMeta: metav1.ObjectMeta{Name: "secret-ns"}},
	}

	t.Run("Without ReferenceGrant", func(t *testing.T) {
		compiled := CompileModel(ModelInputs{
			Gateways:     []*gatewayv1.Gateway{gw},
			ListenerSets: []*gatewayv1.ListenerSet{ls},
			Secrets:      secrets,
			Namespaces:   namespaces,
			RefValidator: NewState(), // empty validator
		})

		cg := compiled.Gateways[types.NamespacedName{Namespace: "gw-ns", Name: "gw"}]
		if cg == nil {
			t.Fatalf("compiled gateway not found")
		}
		if cg.AttachedListenerSets != 0 {
			t.Errorf("expected AttachedListenerSets = 0 without grant, got %d", cg.AttachedListenerSets)
		}

		lsStatus := ComputeDesiredListenerSetStatus(ls, gw, namespaces, cg)
		var accCond, progCond *metav1.Condition
		for i := range lsStatus.Conditions {
			if lsStatus.Conditions[i].Type == string(gatewayv1.ListenerSetConditionAccepted) {
				accCond = &lsStatus.Conditions[i]
			}
			if lsStatus.Conditions[i].Type == string(gatewayv1.ListenerSetConditionProgrammed) {
				progCond = &lsStatus.Conditions[i]
			}
		}
		if accCond == nil || accCond.Status != metav1.ConditionFalse || accCond.Reason != string(gatewayv1.ListenerSetReasonListenersNotValid) {
			t.Errorf("expected ListenerSet Accepted=False (ListenersNotValid), got %v", accCond)
		}
		if progCond == nil || progCond.Status != metav1.ConditionFalse || progCond.Reason != string(gatewayv1.ListenerSetReasonListenersNotValid) {
			t.Errorf("expected ListenerSet Programmed=False (ListenersNotValid), got %v", progCond)
		}

		if len(lsStatus.Listeners) != 1 {
			t.Fatalf("expected 1 listener in ListenerSet status, got %d", len(lsStatus.Listeners))
		}
		var resCond *metav1.Condition
		for i := range lsStatus.Listeners[0].Conditions {
			if lsStatus.Listeners[0].Conditions[i].Type == string(gatewayv1.ListenerConditionResolvedRefs) {
				resCond = &lsStatus.Listeners[0].Conditions[i]
			}
		}
		if resCond == nil || resCond.Status != metav1.ConditionFalse || resCond.Reason != string(gatewayv1.ListenerReasonRefNotPermitted) {
			t.Errorf("expected listener ResolvedRefs=False (RefNotPermitted), got %v", resCond)
		}
	})

	t.Run("With ReferenceGrant", func(t *testing.T) {
		compiled := CompileModel(ModelInputs{
			Gateways:     []*gatewayv1.Gateway{gw},
			ListenerSets: []*gatewayv1.ListenerSet{ls},
			Secrets:      secrets,
			Namespaces:   namespaces,
			RefValidator: stWithGrant,
		})

		cg := compiled.Gateways[types.NamespacedName{Namespace: "gw-ns", Name: "gw"}]
		if cg == nil {
			t.Fatalf("compiled gateway not found")
		}
		if cg.AttachedListenerSets != 1 {
			t.Errorf("expected AttachedListenerSets = 1 with grant, got %d", cg.AttachedListenerSets)
		}

		lsStatus := ComputeDesiredListenerSetStatus(ls, gw, namespaces, cg)
		var accCond, progCond *metav1.Condition
		for i := range lsStatus.Conditions {
			if lsStatus.Conditions[i].Type == string(gatewayv1.ListenerSetConditionAccepted) {
				accCond = &lsStatus.Conditions[i]
			}
			if lsStatus.Conditions[i].Type == string(gatewayv1.ListenerSetConditionProgrammed) {
				progCond = &lsStatus.Conditions[i]
			}
		}
		if accCond == nil || accCond.Status != metav1.ConditionTrue || accCond.Reason != string(gatewayv1.ListenerSetReasonAccepted) {
			t.Errorf("expected ListenerSet Accepted=True (Accepted), got %v", accCond)
		}
		if progCond == nil || progCond.Status != metav1.ConditionTrue || progCond.Reason != string(gatewayv1.ListenerSetReasonProgrammed) {
			t.Errorf("expected ListenerSet Programmed=True (Programmed), got %v", progCond)
		}

		if len(lsStatus.Listeners) != 1 {
			t.Fatalf("expected 1 listener in ListenerSet status, got %d", len(lsStatus.Listeners))
		}
		var resCond *metav1.Condition
		for i := range lsStatus.Listeners[0].Conditions {
			if lsStatus.Listeners[0].Conditions[i].Type == string(gatewayv1.ListenerConditionResolvedRefs) {
				resCond = &lsStatus.Listeners[0].Conditions[i]
			}
		}
		if resCond == nil || resCond.Status != metav1.ConditionTrue || resCond.Reason != string(gatewayv1.ListenerReasonResolvedRefs) {
			t.Errorf("expected listener ResolvedRefs=True (ResolvedRefs), got %v", resCond)
		}
	})
}

func TestCompileModel_GatewayUnresolvedCertRouteAttachment(t *testing.T) {
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "default"},
		Spec: gatewayv1.GatewaySpec{
			Listeners: []gatewayv1.Listener{
				{
					Name:     "https",
					Port:     443,
					Protocol: gatewayv1.HTTPSProtocolType,
					Hostname: Ptr(gatewayv1.Hostname("example.com")),
					TLS: &gatewayv1.ListenerTLSConfig{
						CertificateRefs: []gatewayv1.SecretObjectReference{
							{
								Name: "nonexistent-secret",
							},
						},
					},
				},
			},
		},
	}

	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "route", Namespace: "default"},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{
						Name: "gw",
					},
				},
			},
			Hostnames: []gatewayv1.Hostname{"example.com"},
			Rules: []gatewayv1.HTTPRouteRule{
				{
					BackendRefs: []gatewayv1.HTTPBackendRef{
						{
							BackendRef: gatewayv1.BackendRef{
								BackendObjectReference: gatewayv1.BackendObjectReference{
									Name: "svc",
									Port: Ptr(gatewayv1.PortNumber(8080)),
								},
							},
						},
					},
				},
			},
		},
	}

	compiled := CompileModel(ModelInputs{
		Gateways:   []*gatewayv1.Gateway{gw},
		HTTPRoutes: []*gatewayv1.HTTPRoute{route},
		Services: map[types.NamespacedName]*corev1.Service{
			{Namespace: "default", Name: "svc"}: {ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "svc"}},
		},
		Secrets: map[types.NamespacedName]*corev1.Secret{}, // secret is missing
	})

	gwKey := types.NamespacedName{Namespace: "default", Name: "gw"}
	cg := compiled.Gateways[gwKey]
	if cg == nil {
		t.Fatalf("compiled gateway not found")
	}

	// Gateway should remain Accepted=True
	var gwAccCond *metav1.Condition
	for i := range cg.Conditions {
		if cg.Conditions[i].Type == string(gatewayv1.GatewayConditionAccepted) {
			gwAccCond = &cg.Conditions[i]
		}
	}
	if gwAccCond == nil || gwAccCond.Status != metav1.ConditionTrue {
		t.Errorf("expected Gateway Accepted=True, got %v", gwAccCond)
	}

	// Listener should have ResolvedRefs=False, but AttachedRoutes=1
	if len(cg.EffectiveListeners) != 1 {
		t.Fatalf("expected 1 effective listener, got %d", len(cg.EffectiveListeners))
	}
	el := cg.EffectiveListeners[0]
	if el.AttachedRoutes != 1 {
		t.Errorf("expected listener AttachedRoutes=1, got %d", el.AttachedRoutes)
	}
	if !el.IsAccepted() {
		t.Errorf("expected listener IsAccepted()=true")
	}
	if el.IsProgrammed() {
		t.Errorf("expected listener IsProgrammed()=false (due to unresolved ref)")
	}

	// Route should be Accepted=True
	routeKey := types.NamespacedName{Namespace: "default", Name: "route"}
	cr := compiled.HTTPRoutes[routeKey]
	if cr == nil {
		t.Fatalf("compiled route not found")
	}
	routeStatus := ComputeDesiredHTTPRouteStatus(route, cr, "example.net/gateway-controller")
	if len(routeStatus.Parents) != 1 {
		t.Fatalf("expected 1 parent in route status, got %d", len(routeStatus.Parents))
	}
	var routeAccCond *metav1.Condition
	for i := range routeStatus.Parents[0].Conditions {
		if routeStatus.Parents[0].Conditions[i].Type == string(gatewayv1.RouteConditionAccepted) {
			routeAccCond = &routeStatus.Parents[0].Conditions[i]
		}
	}
	if routeAccCond == nil || routeAccCond.Status != metav1.ConditionTrue {
		t.Errorf("expected Route Accepted=True, got %v", routeAccCond)
	}
}

func TestCompileModel_ListenerSetAllowedRoutesNamespaces(t *testing.T) {
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "gw-ns"},
		Spec: gatewayv1.GatewaySpec{
			AllowedListeners: &gatewayv1.AllowedListeners{
				Namespaces: &gatewayv1.ListenerNamespaces{
					From: Ptr(gatewayv1.NamespacesFromAll),
				},
			},
			Listeners: []gatewayv1.Listener{
				{
					Name:     "gw-listener",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
					Hostname: Ptr(gatewayv1.Hostname("gw.example.com")),
				},
			},
		},
	}

	rg := &gatewayv1beta1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Name: "rg", Namespace: "gw-ns"},
		Spec: gatewayv1beta1.ReferenceGrantSpec{
			From: []gatewayv1beta1.ReferenceGrantFrom{
				{
					Group:     gatewayv1.GroupName,
					Kind:      "ListenerSet",
					Namespace: "ls-ns",
				},
			},
			To: []gatewayv1beta1.ReferenceGrantTo{
				{
					Group: gatewayv1.GroupName,
					Kind:  "Gateway",
				},
			},
		},
	}

	ls := &gatewayv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Name: "ls", Namespace: "ls-ns"},
		Spec: gatewayv1.ListenerSetSpec{
			ParentRef: gatewayv1.ParentGatewayReference{
				Name:      "gw",
				Namespace: Ptr(gatewayv1.Namespace("gw-ns")),
			},
			Listeners: []gatewayv1.ListenerEntry{
				{
					Name:     "listener-all",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
					Hostname: Ptr(gatewayv1.Hostname("all.example.com")),
					AllowedRoutes: &gatewayv1.AllowedRoutes{
						Namespaces: &gatewayv1.RouteNamespaces{
							From: Ptr(gatewayv1.NamespacesFromAll),
						},
					},
				},
				{
					Name:     "listener-same",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
					Hostname: Ptr(gatewayv1.Hostname("same.example.com")),
					AllowedRoutes: &gatewayv1.AllowedRoutes{
						Namespaces: &gatewayv1.RouteNamespaces{
							From: Ptr(gatewayv1.NamespacesFromSame),
						},
					},
				},
				{
					Name:     "listener-sel",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
					Hostname: Ptr(gatewayv1.Hostname("sel.example.com")),
					AllowedRoutes: &gatewayv1.AllowedRoutes{
						Namespaces: &gatewayv1.RouteNamespaces{
							From: Ptr(gatewayv1.NamespacesFromSelector),
							Selector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"allowed": "yes"},
							},
						},
					},
				},
			},
		},
	}

	makeRoute := func(ns, name, sectionName, hostname string) *gatewayv1.HTTPRoute {
		r := &gatewayv1.HTTPRoute{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: gatewayv1.HTTPRouteSpec{
				CommonRouteSpec: gatewayv1.CommonRouteSpec{
					ParentRefs: []gatewayv1.ParentReference{
						{
							Kind:      Ptr(gatewayv1.Kind("ListenerSet")),
							Name:      "ls",
							Namespace: Ptr(gatewayv1.Namespace("ls-ns")),
						},
					},
				},
				Hostnames: []gatewayv1.Hostname{gatewayv1.Hostname(hostname)},
				Rules: []gatewayv1.HTTPRouteRule{
					{
						BackendRefs: []gatewayv1.HTTPBackendRef{
							{
								BackendRef: gatewayv1.BackendRef{
									BackendObjectReference: gatewayv1.BackendObjectReference{
										Name: "svc",
										Port: Ptr(gatewayv1.PortNumber(8080)),
									},
								},
							},
						},
					},
				},
			},
		}
		if sectionName != "" {
			r.Spec.ParentRefs[0].SectionName = Ptr(gatewayv1.SectionName(sectionName))
		}
		return r
	}

	routeInLSNS := makeRoute("ls-ns", "route-same-ns", "listener-same", "same.example.com")
	routeInGWNS := makeRoute("gw-ns", "route-gw-ns", "listener-same", "same.example.com")
	routeInSelNS := makeRoute("sel-ns", "route-sel-ns", "listener-sel", "sel.example.com")
	routeInOtherNS := makeRoute("other-ns", "route-other-ns", "listener-sel", "sel.example.com")

	namespaces := map[string]*corev1.Namespace{
		"gw-ns":    {ObjectMeta: metav1.ObjectMeta{Name: "gw-ns"}},
		"ls-ns":    {ObjectMeta: metav1.ObjectMeta{Name: "ls-ns"}},
		"sel-ns":   {ObjectMeta: metav1.ObjectMeta{Name: "sel-ns", Labels: map[string]string{"allowed": "yes"}}},
		"other-ns": {ObjectMeta: metav1.ObjectMeta{Name: "other-ns", Labels: map[string]string{"allowed": "no"}}},
	}

	s := NewState()
	s.UpsertReferenceGrant(rg)

	compiled := CompileModel(ModelInputs{
		Gateways:     []*gatewayv1.Gateway{gw},
		ListenerSets: []*gatewayv1.ListenerSet{ls},
		RefValidator: s,
		HTTPRoutes:   []*gatewayv1.HTTPRoute{routeInLSNS, routeInGWNS, routeInSelNS, routeInOtherNS},
		Namespaces:   namespaces,
	})

	// 1. routeInLSNS targeting listener-same: Accepted=True
	crSame := compiled.HTTPRoutes[types.NamespacedName{Namespace: "ls-ns", Name: "route-same-ns"}]
	if crSame == nil || len(crSame.ParentConditions) != 1 || crSame.ParentConditions[0].Status != metav1.ConditionTrue {
		t.Fatalf("expected routeInLSNS Accepted=True, got %v", crSame)
	}

	// 2. routeInGWNS targeting listener-same: Accepted=False (Reason: NotAllowedByListeners)
	// Because listener-same is scoped to ListenerSet's namespace (ls-ns), not the Gateway's namespace (gw-ns).
	crGW := compiled.HTTPRoutes[types.NamespacedName{Namespace: "gw-ns", Name: "route-gw-ns"}]
	if crGW == nil || len(crGW.ParentConditions) != 1 {
		t.Fatalf("expected routeInGWNS compiled route, got %v", crGW)
	}
	if crGW.ParentConditions[0].Status != metav1.ConditionFalse || crGW.ParentConditions[0].Reason != string(gatewayv1.RouteReasonNotAllowedByListeners) {
		t.Errorf("expected routeInGWNS Accepted=False (NotAllowedByListeners), got %v", crGW.ParentConditions[0])
	}

	// 3. routeInSelNS targeting listener-sel: Accepted=True
	crSel := compiled.HTTPRoutes[types.NamespacedName{Namespace: "sel-ns", Name: "route-sel-ns"}]
	if crSel == nil || len(crSel.ParentConditions) != 1 || crSel.ParentConditions[0].Status != metav1.ConditionTrue {
		t.Fatalf("expected routeInSelNS Accepted=True, got %v", crSel)
	}

	// 4. routeInOtherNS targeting listener-sel: Accepted=False (Reason: NotAllowedByListeners)
	crOther := compiled.HTTPRoutes[types.NamespacedName{Namespace: "other-ns", Name: "route-other-ns"}]
	if crOther == nil || len(crOther.ParentConditions) != 1 {
		t.Fatalf("expected routeInOtherNS compiled route, got %v", crOther)
	}
	if crOther.ParentConditions[0].Status != metav1.ConditionFalse || crOther.ParentConditions[0].Reason != string(gatewayv1.RouteReasonNotAllowedByListeners) {
		t.Errorf("expected routeInOtherNS Accepted=False (NotAllowedByListeners), got %v", crOther.ParentConditions[0])
	}
}

func TestCompileModel_ListenerSetGatewayParentSectionNameNotFound(t *testing.T) {
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "default"},
		Spec: gatewayv1.GatewaySpec{
			AllowedListeners: &gatewayv1.AllowedListeners{
				Namespaces: &gatewayv1.ListenerNamespaces{
					From: Ptr(gatewayv1.NamespacesFromSame),
				},
			},
			Listeners: []gatewayv1.Listener{
				{
					Name:     "gw-listener",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
					Hostname: Ptr(gatewayv1.Hostname("gw.example.com")),
				},
			},
		},
	}

	ls := &gatewayv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Name: "ls", Namespace: "default"},
		Spec: gatewayv1.ListenerSetSpec{
			ParentRef: gatewayv1.ParentGatewayReference{
				Name: "gw",
			},
			Listeners: []gatewayv1.ListenerEntry{
				{
					Name:     "ls-listener",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
					Hostname: Ptr(gatewayv1.Hostname("ls.example.com")),
				},
			},
		},
	}

	// Route targeting Gateway with sectionName "ls-listener" (exists on ListenerSet, not Gateway)
	routeViaGW := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "route-via-gw", Namespace: "default"},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{
						Name:        "gw",
						SectionName: Ptr(gatewayv1.SectionName("ls-listener")),
					},
				},
			},
			Rules: []gatewayv1.HTTPRouteRule{
				{
					BackendRefs: []gatewayv1.HTTPBackendRef{
						{
							BackendRef: gatewayv1.BackendRef{
								BackendObjectReference: gatewayv1.BackendObjectReference{
									Name: "svc",
									Port: Ptr(gatewayv1.PortNumber(8080)),
								},
							},
						},
					},
				},
			},
		},
	}

	// Route targeting ListenerSet with sectionName "ls-listener"
	routeViaLS := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "route-via-ls", Namespace: "default"},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{
						Kind:        Ptr(gatewayv1.Kind("ListenerSet")),
						Name:        "ls",
						SectionName: Ptr(gatewayv1.SectionName("ls-listener")),
					},
				},
			},
			Rules: []gatewayv1.HTTPRouteRule{
				{
					BackendRefs: []gatewayv1.HTTPBackendRef{
						{
							BackendRef: gatewayv1.BackendRef{
								BackendObjectReference: gatewayv1.BackendObjectReference{
									Name: "svc",
									Port: Ptr(gatewayv1.PortNumber(8080)),
								},
							},
						},
					},
				},
			},
		},
	}

	compiled := CompileModel(ModelInputs{
		Gateways:     []*gatewayv1.Gateway{gw},
		ListenerSets: []*gatewayv1.ListenerSet{ls},
		HTTPRoutes:   []*gatewayv1.HTTPRoute{routeViaGW, routeViaLS},
	})

	crGW := compiled.HTTPRoutes[types.NamespacedName{Namespace: "default", Name: "route-via-gw"}]
	if crGW == nil || len(crGW.ParentConditions) != 1 {
		t.Fatalf("expected routeViaGW compiled route, got %v", crGW)
	}
	if crGW.ParentConditions[0].Status != metav1.ConditionFalse || crGW.ParentConditions[0].Reason != string(gatewayv1.RouteReasonNoMatchingParent) {
		t.Errorf("expected routeViaGW Accepted=False (NoMatchingParent), got %v", crGW.ParentConditions[0])
	}

	crLS := compiled.HTTPRoutes[types.NamespacedName{Namespace: "default", Name: "route-via-ls"}]
	if crLS == nil || len(crLS.ParentConditions) != 1 {
		t.Fatalf("expected routeViaLS compiled route, got %v", crLS)
	}
	if crLS.ParentConditions[0].Status != metav1.ConditionTrue || crLS.ParentConditions[0].Reason != string(gatewayv1.RouteReasonAccepted) {
		t.Errorf("expected routeViaLS Accepted=True (Accepted), got %v", crLS.ParentConditions[0])
	}
}

func TestCompileModel_ListenerSetRouteStatusScopedToParentRef(t *testing.T) {
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "default"},
		Spec: gatewayv1.GatewaySpec{
			AllowedListeners: &gatewayv1.AllowedListeners{
				Namespaces: &gatewayv1.ListenerNamespaces{
					From: Ptr(gatewayv1.NamespacesFromSame),
				},
			},
			Listeners: []gatewayv1.Listener{
				{
					Name:     "gw-listener",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
					Hostname: Ptr(gatewayv1.Hostname("gw.example.com")),
				},
			},
		},
	}

	ls := &gatewayv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Name: "ls", Namespace: "default"},
		Spec: gatewayv1.ListenerSetSpec{
			ParentRef: gatewayv1.ParentGatewayReference{
				Name: "gw",
			},
			Listeners: []gatewayv1.ListenerEntry{
				{
					Name:     "ls-listener",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
					Hostname: Ptr(gatewayv1.Hostname("ls.example.com")),
				},
			},
		},
	}

	routeGWOnly := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "route-gw", Namespace: "default"},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{Name: "gw"},
				},
			},
		},
	}

	routeLSOnly := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "route-ls", Namespace: "default"},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{
						Kind: Ptr(gatewayv1.Kind("ListenerSet")),
						Name: "ls",
					},
				},
			},
		},
	}

	compiled := CompileModel(ModelInputs{
		Gateways:     []*gatewayv1.Gateway{gw},
		ListenerSets: []*gatewayv1.ListenerSet{ls},
		HTTPRoutes:   []*gatewayv1.HTTPRoute{routeGWOnly, routeLSOnly},
	})

	crGW := compiled.HTTPRoutes[types.NamespacedName{Namespace: "default", Name: "route-gw"}]
	statusGW := ComputeDesiredHTTPRouteStatus(routeGWOnly, crGW, "example.net/gateway-controller")
	if len(statusGW.Parents) != 1 {
		t.Fatalf("expected exactly 1 parent in status for routeGWOnly, got %d", len(statusGW.Parents))
	}
	if statusGW.Parents[0].ParentRef.Name != "gw" || ValueOf(statusGW.Parents[0].ParentRef.Kind) != "" {
		t.Errorf("expected parentRef gw, got %+v", statusGW.Parents[0].ParentRef)
	}

	crLS := compiled.HTTPRoutes[types.NamespacedName{Namespace: "default", Name: "route-ls"}]
	statusLS := ComputeDesiredHTTPRouteStatus(routeLSOnly, crLS, "example.net/gateway-controller")
	if len(statusLS.Parents) != 1 {
		t.Fatalf("expected exactly 1 parent in status for routeLSOnly, got %d", len(statusLS.Parents))
	}
	if statusLS.Parents[0].ParentRef.Name != "ls" || ValueOf(statusLS.Parents[0].ParentRef.Kind) != "ListenerSet" {
		t.Errorf("expected parentRef ls (ListenerSet), got %+v", statusLS.Parents[0].ParentRef)
	}
}

func TestCompileModel_ListenerSetDualParentRefIndependence(t *testing.T) {
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "default"},
		Spec: gatewayv1.GatewaySpec{
			AllowedListeners: &gatewayv1.AllowedListeners{
				Namespaces: &gatewayv1.ListenerNamespaces{
					From: Ptr(gatewayv1.NamespacesFromSame),
				},
			},
			Listeners: []gatewayv1.Listener{
				{
					Name:     "gw-listener",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
					Hostname: Ptr(gatewayv1.Hostname("gw.example.com")),
				},
			},
		},
	}

	ls := &gatewayv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Name: "ls", Namespace: "default"},
		Spec: gatewayv1.ListenerSetSpec{
			ParentRef: gatewayv1.ParentGatewayReference{
				Name: "gw",
			},
			Listeners: []gatewayv1.ListenerEntry{
				{
					Name:     "ls-listener",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
					Hostname: Ptr(gatewayv1.Hostname("ls.example.com")),
				},
			},
		},
	}

	// Route with both parentRefs valid
	routeBothValid := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "route-both", Namespace: "default"},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{Name: "gw"},
					{
						Kind: Ptr(gatewayv1.Kind("ListenerSet")),
						Name: "ls",
					},
				},
			},
		},
	}

	// Route with Gateway parentRef invalid (sectionName ls-listener) and ListenerSet parentRef valid
	routeOneValid := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "route-one", Namespace: "default"},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{
						Name:        "gw",
						SectionName: Ptr(gatewayv1.SectionName("ls-listener")),
					},
					{
						Kind:        Ptr(gatewayv1.Kind("ListenerSet")),
						Name:        "ls",
						SectionName: Ptr(gatewayv1.SectionName("ls-listener")),
					},
				},
			},
		},
	}

	compiled := CompileModel(ModelInputs{
		Gateways:     []*gatewayv1.Gateway{gw},
		ListenerSets: []*gatewayv1.ListenerSet{ls},
		HTTPRoutes:   []*gatewayv1.HTTPRoute{routeBothValid, routeOneValid},
	})

	// Check route-both
	crBoth := compiled.HTTPRoutes[types.NamespacedName{Namespace: "default", Name: "route-both"}]
	if crBoth == nil || len(crBoth.ParentConditions) != 2 {
		t.Fatalf("expected 2 parent conditions for route-both, got %v", crBoth)
	}
	if crBoth.ParentConditions[0].Status != metav1.ConditionTrue {
		t.Errorf("expected parentRef[0] Accepted=True, got %v", crBoth.ParentConditions[0])
	}
	if crBoth.ParentConditions[1].Status != metav1.ConditionTrue {
		t.Errorf("expected parentRef[1] Accepted=True, got %v", crBoth.ParentConditions[1])
	}

	// Check route-one
	crOne := compiled.HTTPRoutes[types.NamespacedName{Namespace: "default", Name: "route-one"}]
	if crOne == nil || len(crOne.ParentConditions) != 2 {
		t.Fatalf("expected 2 parent conditions for route-one, got %v", crOne)
	}
	if crOne.ParentConditions[0].Status != metav1.ConditionFalse || crOne.ParentConditions[0].Reason != string(gatewayv1.RouteReasonNoMatchingParent) {
		t.Errorf("expected parentRef[0] Accepted=False (NoMatchingParent), got %v", crOne.ParentConditions[0])
	}
	if crOne.ParentConditions[1].Status != metav1.ConditionTrue || crOne.ParentConditions[1].Reason != string(gatewayv1.RouteReasonAccepted) {
		t.Errorf("expected parentRef[1] Accepted=True (Accepted), got %v", crOne.ParentConditions[1])
	}
}
