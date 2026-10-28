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

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/admin"
)

func usage(w io.Writer) {
	if _, err := fmt.Fprintf(w, `gcpgatewayctl: admin tool for Google Cloud Load Balancing and NEGs in gcpgateway.

Usage:
  gcpgatewayctl frontend create [flags]
  gcpgatewayctl frontend delete [flags]

Subcommands:
  frontend create    Provision GCP load balancer and zonal NEGs idempotently
  frontend delete    Tear down GCP load balancer and zonal NEGs idempotently
`); err != nil {
		return
	}
}

// Run parses CLI arguments and runs the specified admin subcommand.
func Run(ctx context.Context, args []string, out, errOut io.Writer) error {
	return RunWithClient(ctx, nil, args, out, errOut)
}

// RunWithClient allows passing an optional pre-configured AdminClient (useful for testing).
func RunWithClient(ctx context.Context, client admin.AdminClient, args []string, out, errOut io.Writer) error {
	if len(args) < 2 || args[0] != "frontend" {
		usage(errOut)
		if len(args) == 0 {
			return nil
		}
		return fmt.Errorf("expected 'frontend create' or 'frontend delete'")
	}

	subcmd := args[1]
	switch subcmd {
	case "create":
		cfg := &admin.FrontendConfig{}
		fs := flag.NewFlagSet("frontend create", flag.ContinueOnError)
		fs.SetOutput(errOut)
		cfg.BindCreateFlags(fs)
		if err := fs.Parse(args[2:]); err != nil {
			return err
		}

		if client == nil {
			var err error
			client, err = admin.NewGCEAdminClient(ctx)
			if err != nil {
				return fmt.Errorf("initializing GCP compute client: %w", err)
			}
		}

		_, err := admin.CreateFrontend(ctx, client, cfg, out)
		return err

	case "delete":
		cfg := &admin.FrontendConfig{}
		fs := flag.NewFlagSet("frontend delete", flag.ContinueOnError)
		fs.SetOutput(errOut)
		cfg.BindDeleteFlags(fs)
		if err := fs.Parse(args[2:]); err != nil {
			return err
		}

		if client == nil {
			var err error
			client, err = admin.NewGCEAdminClient(ctx)
			if err != nil {
				return fmt.Errorf("initializing GCP compute client: %w", err)
			}
		}

		_, err := admin.DeleteFrontend(ctx, client, cfg, out)
		return err

	default:
		usage(errOut)
		return fmt.Errorf("unknown frontend subcommand %q (expected 'create' or 'delete')", subcmd)
	}
}

func main() {
	ctx := context.Background()
	if err := Run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
