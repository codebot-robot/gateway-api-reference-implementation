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
	gwStatus, updated := ComputeDesiredGatewayStatus(gw, compiled.Gateways[types.NamespacedName{Namespace: "default", Name: "gw"}], addresses)
	if !updated {
		t.Errorf("expected Gateway status updated=true")
	}
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
	lsStatus, updated := ComputeDesiredListenerSetStatus(ls, gw, nil, compiled.Gateways[types.NamespacedName{Namespace: "default", Name: "gw"}])
	if !updated {
		t.Errorf("expected ListenerSet status updated=true")
	}
	if len(lsStatus.Conditions) != 2 {
		t.Errorf("expected 2 ListenerSet conditions, got %d", len(lsStatus.Conditions))
	}
	if len(lsStatus.Listeners) != 1 || lsStatus.Listeners[0].AttachedRoutes != 1 {
		t.Errorf("expected 1 attached route on ListenerSet listener status, got %v", lsStatus.Listeners)
	}

	// 3. HTTPRoute Status
	routeStatus, updated := ComputeDesiredHTTPRouteStatus(route, compiled.HTTPRoutes[types.NamespacedName{Namespace: "default", Name: "r1"}], "example.net/gateway-controller")
	if !updated {
		t.Errorf("expected HTTPRoute status updated=true")
	}
	if len(routeStatus.Parents) != 2 {
		t.Fatalf("expected 2 parent statuses on route, got %d", len(routeStatus.Parents))
	}
	for i, p := range routeStatus.Parents {
		if len(p.Conditions) != 2 {
			t.Errorf("parent %d expected 2 conditions, got %d", i, len(p.Conditions))
		}
	}
}
