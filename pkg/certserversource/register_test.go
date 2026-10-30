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

package certserversource

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/gari"
)

func TestDefaultConfig(t *testing.T) {
	t.Setenv("CERTSERVER_ADDRESS", "certserver.example.com:8443")
	t.Setenv("CERTSERVER_CA_BUNDLE_PATH", "/etc/ssl/ca.crt")
	t.Setenv("CERTSERVER_TOKEN_PATH", "/var/run/token")

	cfg := DefaultConfig()
	if cfg.ServerAddress != "certserver.example.com:8443" {
		t.Errorf("expected ServerAddress 'certserver.example.com:8443', got %q", cfg.ServerAddress)
	}
	if cfg.CABundlePath != "/etc/ssl/ca.crt" {
		t.Errorf("expected CABundlePath '/etc/ssl/ca.crt', got %q", cfg.CABundlePath)
	}
	if cfg.TokenPath != "/var/run/token" {
		t.Errorf("expected TokenPath '/var/run/token', got %q", cfg.TokenPath)
	}
}

func TestBindFlags(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var cfg Config
	BindFlags(fs, &cfg)

	args := []string{
		"--certserver-address=127.0.0.1:8443",
		"--certserver-ca-bundle-path=/tmp/ca.crt",
		"--certserver-token-path=/tmp/token",
	}
	if err := fs.Parse(args); err != nil {
		t.Fatalf("fs.Parse failed: %v", err)
	}

	if cfg.ServerAddress != "127.0.0.1:8443" {
		t.Errorf("expected ServerAddress '127.0.0.1:8443', got %q", cfg.ServerAddress)
	}
	if cfg.CABundlePath != "/tmp/ca.crt" {
		t.Errorf("expected CABundlePath '/tmp/ca.crt', got %q", cfg.CABundlePath)
	}
	if cfg.TokenPath != "/tmp/token" {
		t.Errorf("expected TokenPath '/tmp/token', got %q", cfg.TokenPath)
	}
}

func TestRegister_Unconfigured(t *testing.T) {
	opts := gari.DefaultOptions()
	cfg := Config{}

	if err := Register(&opts, cfg); err != nil {
		t.Fatalf("expected nil error for empty config, got %v", err)
	}
	if opts.CertificateSources != nil && opts.CertificateSources[CertificateSourceName] != nil {
		t.Errorf("expected certserver source not to be registered when unconfigured")
	}
}

func TestRegister_MissingAddress(t *testing.T) {
	opts := gari.DefaultOptions()
	cfg := Config{
		TokenPath: "/var/run/token",
	}

	err := Register(&opts, cfg)
	if err == nil {
		t.Fatalf("expected error when address is empty, got nil")
	}
	if !strings.Contains(err.Error(), "--certserver-address is required") {
		t.Errorf("expected error message to mention --certserver-address, got %v", err)
	}
}

func TestRegister_MissingToken(t *testing.T) {
	opts := gari.DefaultOptions()
	cfg := Config{
		ServerAddress: "localhost:8443",
	}

	err := Register(&opts, cfg)
	if err == nil {
		t.Fatalf("expected error when token is empty, got nil")
	}
	if !strings.Contains(err.Error(), "--certserver-token-path is required") {
		t.Errorf("expected error message to mention --certserver-token-path, got %v", err)
	}
}

func TestRegister_SuccessAndManagerHook(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("test-token"), 0600); err != nil {
		t.Fatalf("failed to write token file: %v", err)
	}

	prevSetupCalled := false
	opts := gari.DefaultOptions()
	opts.SetupWithManager = func(mgr ctrl.Manager, s *gari.Server) error {
		prevSetupCalled = true
		return nil
	}

	cfg := Config{
		ServerAddress: "127.0.0.1:8443",
		TokenPath:     tokenFile,
	}

	if err := Register(&opts, cfg); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if opts.CertificateSources == nil || opts.CertificateSources[CertificateSourceName] == nil {
		t.Fatalf("expected CertificateSources[%q] to be registered", CertificateSourceName)
	}

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1.Install(scheme)

	skipNameValidation := true
	mgr, err := ctrl.NewManager(&rest.Config{}, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{
			SkipNameValidation: &skipNameValidation,
		},
	})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	if err := opts.SetupWithManager(mgr, nil); err != nil {
		t.Fatalf("SetupWithManager failed: %v", err)
	}

	if !prevSetupCalled {
		t.Errorf("expected previous SetupWithManager to be chained and called")
	}
}
