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
	"fmt"
	"os"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/gke-labs/gateway-api-reference-implementation/certserver/api/client"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/gari"
)

// CertificateSourceName is the identifier used to register the certserver CertificateSource in GARI.
const CertificateSourceName = "certserver"

// Config configures the certserver CertificateSource.
type Config struct {
	// ServerAddress is the host:port of the certserver gRPC service.
	ServerAddress string

	// CABundlePath is the path to the CA bundle PEM file to verify certserver TLS.
	CABundlePath string

	// TokenPath is the path to the projected service-account token file.
	TokenPath string
}

// DefaultConfig returns a Config populated with default environment variable values.
func DefaultConfig() Config {
	return Config{
		ServerAddress: os.Getenv("CERTSERVER_ADDRESS"),
		CABundlePath:  os.Getenv("CERTSERVER_CA_BUNDLE_PATH"),
		TokenPath:     os.Getenv("CERTSERVER_TOKEN_PATH"),
	}
}

// BindFlags binds certserver CLI flags directly to cfg.
func BindFlags(fs *flag.FlagSet, cfg *Config) {
	fs.StringVar(&cfg.ServerAddress, "certserver-address", cfg.ServerAddress, "Address of the certserver gRPC service")
	fs.StringVar(&cfg.CABundlePath, "certserver-ca-bundle-path", cfg.CABundlePath, "Path to the CA bundle PEM file to verify certserver TLS")
	fs.StringVar(&cfg.TokenPath, "certserver-token-path", cfg.TokenPath, "Path to the projected service-account token file for controller identity")
}

// Register validates cfg, initializes a certserver client when an address is configured,
// and registers it as the "certserver" CertificateSource on opts, along with a manager lifecycle hook.
func Register(opts *gari.Options, cfg Config) error {
	if cfg.ServerAddress == "" && cfg.TokenPath == "" && cfg.CABundlePath == "" {
		return nil
	}
	if cfg.ServerAddress == "" {
		return fmt.Errorf("--certserver-address is required when certserver flags are set")
	}
	if cfg.TokenPath == "" {
		return fmt.Errorf("--certserver-token-path is required when certserver flags are set")
	}

	cli, err := client.New(client.Config{
		ServerAddress: cfg.ServerAddress,
		CABundlePath:  cfg.CABundlePath,
		TokenPath:     cfg.TokenPath,
	})
	if err != nil {
		return fmt.Errorf("initializing certserver client: %w", err)
	}

	src := NewSource(cli)

	if opts.CertificateSources == nil {
		opts.CertificateSources = make(map[string]gari.CertificateSource)
	}
	opts.CertificateSources[CertificateSourceName] = src

	prevSetup := opts.SetupWithManager
	opts.SetupWithManager = func(mgr ctrl.Manager, s *gari.Server) error {
		if prevSetup != nil {
			if err := prevSetup(mgr, s); err != nil {
				return err
			}
		}
		if err := mgr.Add(src); err != nil {
			return fmt.Errorf("registering certserver client with manager: %w", err)
		}
		return nil
	}

	return nil
}
