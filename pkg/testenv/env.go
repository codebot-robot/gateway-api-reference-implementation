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

package testenv

import (
	"errors"
	"fmt"
	"go/build"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1alpha2 "sigs.k8s.io/gateway-api/apis/v1alpha2"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

// DefaultScheme creates a runtime.Scheme populated with Kubernetes core,
// apiextensions, and Gateway API v1, v1beta1, and v1alpha2 types.
func DefaultScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(apiextensionsv1.AddToScheme(s))
	utilruntime.Must(gatewayv1.AddToScheme(s))
	utilruntime.Must(gatewayv1beta1.AddToScheme(s))
	utilruntime.Must(gatewayv1alpha2.AddToScheme(s))
	return s
}

// GatewayAPICRDDir locates the Gateway API experimental CRD directory.
// It checks GATEWAY_API_CRD_DIR, queries go list, and inspects module cache directories.
func GatewayAPICRDDir() (string, error) {
	if dir := os.Getenv("GATEWAY_API_CRD_DIR"); dir != "" {
		if fi, err := os.Stat(dir); err == nil && fi.IsDir() {
			return dir, nil
		}
	}

	cmd := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "sigs.k8s.io/gateway-api")
	out, err := cmd.Output()
	if err == nil {
		modDir := strings.TrimSpace(string(out))
		if modDir != "" {
			crdDir := filepath.Join(modDir, "config", "crd", "experimental")
			if fi, err := os.Stat(crdDir); err == nil && fi.IsDir() {
				return crdDir, nil
			}
		}
	}

	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		gopath = build.Default.GOPATH
	}
	for _, gp := range filepath.SplitList(gopath) {
		pattern := filepath.Join(gp, "pkg", "mod", "sigs.k8s.io", "gateway-api@*", "config", "crd", "experimental")
		matches, globErr := filepath.Glob(pattern)
		if globErr == nil && len(matches) > 0 {
			sort.Strings(matches)
			return matches[len(matches)-1], nil
		}
	}

	if err != nil {
		return "", fmt.Errorf("could not locate sigs.k8s.io/gateway-api experimental CRD directory: %w", err)
	}
	return "", fmt.Errorf("could not locate sigs.k8s.io/gateway-api experimental CRD directory")
}

// Option configures an Environment.
type Option func(*options)

type options struct {
	scheme                *runtime.Scheme
	crdDirectory          string
	binaryAssetsDirectory string
	downloadBinaryAssets  bool
}

// WithScheme overrides the runtime.Scheme used by the test environment.
func WithScheme(s *runtime.Scheme) Option {
	return func(o *options) {
		o.scheme = s
	}
}

// WithCRDDirectory overrides the CRD directory path.
func WithCRDDirectory(dir string) Option {
	return func(o *options) {
		o.crdDirectory = dir
	}
}

// WithBinaryAssetsDirectory overrides the envtest binary assets directory.
func WithBinaryAssetsDirectory(dir string) Option {
	return func(o *options) {
		o.binaryAssetsDirectory = dir
	}
}

// WithDownloadBinaryAssets configures whether envtest downloads binary assets if not found locally.
func WithDownloadBinaryAssets(download bool) Option {
	return func(o *options) {
		o.downloadBinaryAssets = download
	}
}

// Environment manages an envtest control plane with Gateway API CRDs installed.
type Environment struct {
	Env          *envtest.Environment
	Config       *rest.Config
	Client       client.Client
	Scheme       *runtime.Scheme
	CRDDirectory string
}

// New creates and initializes an Environment without starting it.
func New(opts ...Option) (*Environment, error) {
	cfg := &options{
		scheme:               DefaultScheme(),
		downloadBinaryAssets: true,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	crdDir := cfg.crdDirectory
	if crdDir == "" {
		var err error
		crdDir, err = GatewayAPICRDDir()
		if err != nil {
			return nil, fmt.Errorf("resolving gateway-api CRD directory: %w", err)
		}
	}

	env := &envtest.Environment{
		Scheme:                   cfg.scheme,
		CRDDirectoryPaths:        []string{crdDir},
		ErrorIfCRDPathMissing:    true,
		ControlPlaneStartTimeout: 60 * time.Second,
		ControlPlaneStopTimeout:  60 * time.Second,
	}

	if cfg.binaryAssetsDirectory != "" {
		env.BinaryAssetsDirectory = cfg.binaryAssetsDirectory
	} else if os.Getenv("KUBEBUILDER_ASSETS") != "" {
		// Handled natively by envtest via KUBEBUILDER_ASSETS.
	} else {
		env.DownloadBinaryAssets = cfg.downloadBinaryAssets
	}

	return &Environment{
		Env:          env,
		Scheme:       cfg.scheme,
		CRDDirectory: crdDir,
	}, nil
}

// Start boots the envtest control plane and constructs a Kubernetes client.
func (e *Environment) Start() (*rest.Config, error) {
	cfg, err := e.Env.Start()
	if err != nil {
		return nil, fmt.Errorf("starting envtest control plane: %w", err)
	}
	e.Config = cfg

	cl, err := client.New(cfg, client.Options{Scheme: e.Scheme})
	if err != nil {
		stopErr := e.Env.Stop()
		return nil, errors.Join(fmt.Errorf("creating client: %w", err), stopErr)
	}
	e.Client = cl

	return cfg, nil
}

// Stop shuts down the envtest control plane.
func (e *Environment) Stop() error {
	if e.Env == nil {
		return nil
	}
	return e.Env.Stop()
}

// Start launches a new test environment for the duration of the test,
// automatically stopping it via t.Cleanup.
func Start(t testing.TB, opts ...Option) *Environment {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping envtest integration test in short mode")
	}

	env, err := New(opts...)
	if err != nil {
		t.Fatalf("testenv.New failed: %v", err)
	}

	if _, err := env.Start(); err != nil {
		t.Fatalf("testenv.Start failed: %v", err)
	}

	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Logf("env.Stop warning: %v", err)
		}
	})

	return env
}
