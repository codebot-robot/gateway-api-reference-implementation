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

package gari

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/controller"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/proxy"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()
	if opts.ControllerName != controller.DefaultControllerName {
		t.Errorf("expected controller name %q, got %q", controller.DefaultControllerName, opts.ControllerName)
	}
	if opts.ProxyAddr != ":8000" {
		t.Errorf("expected proxy addr :8000, got %q", opts.ProxyAddr)
	}
	if opts.ProxyHTTPSAddr != ":8443" {
		t.Errorf("expected proxy https addr :8443, got %q", opts.ProxyHTTPSAddr)
	}
	if opts.MetricsAddr != ":8080" {
		t.Errorf("expected metrics addr :8080, got %q", opts.MetricsAddr)
	}
	if opts.HealthProbeBindAddress != ":8081" {
		t.Errorf("expected probe addr :8081, got %q", opts.HealthProbeBindAddress)
	}
}

func TestServerCustomHTTPListener(t *testing.T) {
	// Create an in-memory TCP listener
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer httpLis.Close()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Backend", "ok")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("backend response"))
	}))
	defer backend.Close()

	opts := Options{
		HTTPListener:   httpLis,
		ProxyHTTPSAddr: "", // disable default HTTPS listener for this test
		Scheme:         DefaultScheme(),
		ControllerName: "custom.io/gateway-controller",
	}

	st := state.NewState()
	p := proxy.NewProxy()

	server := &Server{
		opts:  opts,
		state: st,
		proxy: p,
	}

	host, portStr, _ := net.SplitHostPort(backend.Listener.Addr().String())
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	// Configure a route directly on proxy
	p.UpdateRoutes([]state.InternalRoute{
		{
			Rules: []state.InternalRule{
				{
					Backends: []state.InternalBackend{
						{
							Host:   host,
							Port:   int32(port),
							Weight: 1,
						},
					},
				},
			},
		},
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.StartProxyServers(ctx)
	}()

	client := &http.Client{Timeout: 2 * time.Second}
	reqURL := "http://" + httpLis.Addr().String() + "/test"

	// Wait briefly for server to accept connections
	var resp *http.Response
	for i := 0; i < 20; i++ {
		resp, err = client.Get(reqURL)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("failed to connect to custom HTTP listener: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "backend response" {
		t.Errorf("expected 'backend response', got %q", string(body))
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("unexpected error from StartProxyServers: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Errorf("timed out waiting for proxy shutdown")
	}
}

func TestServerCustomHTTPSListener(t *testing.T) {
	httpsLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer httpsLis.Close()

	opts := Options{
		HTTPSListener: httpsLis,
		ProxyAddr:     "", // disable default HTTP listener for this test
		Scheme:        DefaultScheme(),
	}

	st := state.NewState()
	p := proxy.NewProxy()

	server := &Server{
		opts:  opts,
		state: st,
		proxy: p,
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.StartProxyServers(ctx)
	}()

	// TLS client that trusts self-signed certs
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr, Timeout: 2 * time.Second}
	reqURL := "https://" + httpsLis.Addr().String() + "/notfound"

	var resp *http.Response
	for i := 0; i < 20; i++ {
		resp, err = client.Get(reqURL)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("failed to connect to custom HTTPS listener: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("unexpected error from StartProxyServers: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Errorf("timed out waiting for proxy shutdown")
	}
}

func TestOnGatewaysUpdateHookAndCustomControllerName(t *testing.T) {
	var mu sync.Mutex
	var lastGateways []*gatewayv1.Gateway
	hookCallCount := 0

	customControllerName := "example.com/custom-controller"

	opts := Options{
		ControllerName: customControllerName,
		OnGatewaysUpdate: func(gateways []*gatewayv1.Gateway) {
			mu.Lock()
			defer mu.Unlock()
			hookCallCount++
			lastGateways = gateways
		},
	}

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.AddToScheme(scheme)
	opts.Scheme = scheme

	st := state.NewState()
	p := proxy.NewProxy()

	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-gateway",
			Namespace: "default",
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "custom-class",
			Listeners: []gatewayv1.Listener{
				{
					Name:     "https",
					Hostname: state.Ptr(gatewayv1.Hostname("example.com")),
					Port:     443,
					Protocol: gatewayv1.HTTPSProtocolType,
				},
				{
					Name:     "http",
					Hostname: state.Ptr(gatewayv1.Hostname("app.example.com")),
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
				},
			},
		},
	}

	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "custom-class",
		},
		Spec: gatewayv1.GatewayClassSpec{
			ControllerName: gatewayv1.GatewayController(customControllerName),
		},
	}

	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-route",
			Namespace: "default",
		},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{
						Name: "test-gateway",
					},
				},
			},
			Hostnames: []gatewayv1.Hostname{"example.com"},
		},
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gari-proxy",
			Namespace: "default",
		},
		Status: corev1.ServiceStatus{
			LoadBalancer: corev1.LoadBalancerStatus{
				Ingress: []corev1.LoadBalancerIngress{
					{IP: "1.2.3.4"},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(gw, gc, route, svc).
		WithStatusSubresource(gw, gc, route).
		Build()

	gwReconciler := &controller.GatewayReconciler{
		Client:           fakeClient,
		Scheme:           scheme,
		State:            st,
		Proxy:            p,
		ControllerName:   customControllerName,
		OnGatewaysUpdate: opts.OnGatewaysUpdate,
	}

	ctx := t.Context()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-gateway"}}

	_, err := gwReconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if hookCallCount == 0 {
		t.Fatalf("expected OnGatewaysUpdate hook to be called, got 0 calls")
	}

	if len(lastGateways) != 1 {
		t.Fatalf("expected 1 resolved gateway, got %d", len(lastGateways))
	}

	resolved := lastGateways[0]
	if resolved.Name != "test-gateway" || resolved.Namespace != "default" {
		t.Errorf("unexpected gateway name/namespace: %s/%s", resolved.Namespace, resolved.Name)
	}
	if len(resolved.Spec.Listeners) != 2 {
		t.Fatalf("expected 2 listeners, got %d", len(resolved.Spec.Listeners))
	}
	if string(*resolved.Spec.Listeners[0].Hostname) != "example.com" {
		t.Errorf("expected hostname example.com, got %v", *resolved.Spec.Listeners[0].Hostname)
	}
	if string(*resolved.Spec.Listeners[1].Hostname) != "app.example.com" {
		t.Errorf("expected hostname app.example.com, got %v", *resolved.Spec.Listeners[1].Hostname)
	}
}

func TestNewWithManager(t *testing.T) {
	opts := Options{
		RestConfig: &rest.Config{
			Host: "http://localhost:8080",
		},
		MetricsAddr:            "0",
		HealthProbeBindAddress: "0",
	}

	// Verify complete sets defaults
	err := opts.complete()
	if err != nil {
		t.Fatalf("complete returned error: %v", err)
	}
	if opts.ControllerName != controller.DefaultControllerName {
		t.Errorf("expected default controller name, got %s", opts.ControllerName)
	}
	if opts.Scheme == nil {
		t.Errorf("expected scheme to be initialized")
	}
}
