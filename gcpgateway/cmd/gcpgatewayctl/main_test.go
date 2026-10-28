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
	"bytes"
	"strings"
	"testing"

	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/admin"
)

func TestRun_HelpAndInvalid(t *testing.T) {
	var out, errOut bytes.Buffer
	ctx := t.Context()

	// Empty args
	if err := Run(ctx, []string{}, &out, &errOut); err != nil {
		t.Fatalf("expected nil for empty args, got %v", err)
	}

	// Invalid subcommand
	errOut.Reset()
	if err := Run(ctx, []string{"frontend", "invalid"}, &out, &errOut); err == nil {
		t.Fatalf("expected error for invalid subcommand, got nil")
	}

	// Missing required flags in create
	errOut.Reset()
	fake := admin.NewFakeAdminClient()
	err := RunWithClient(ctx, fake, []string{"frontend", "create", "--dry-run"}, &out, &errOut)
	if err == nil {
		t.Fatalf("expected error when project/region missing")
	}
	if !strings.Contains(err.Error(), "--project and --region are required") {
		t.Errorf("expected missing flags error, got: %v", err)
	}
}

func TestRun_DryRun(t *testing.T) {
	var out, errOut bytes.Buffer
	ctx := t.Context()
	fake := admin.NewFakeAdminClient()

	// Dry-run create
	args := []string{
		"frontend", "create",
		"--project=test-proj",
		"--region=us-central1",
		"--zones=us-central1-a,us-central1-b",
		"--cluster-id=cluster-1",
		"--namespace=default",
		"--gateway-name=my-gw",
		"--dry-run",
	}
	if err := RunWithClient(ctx, fake, args, &out, &errOut); err != nil {
		t.Fatalf("Run frontend create --dry-run failed: %v", err)
	}
	if !strings.Contains(out.String(), "=== Dry Run Plan Complete ===") {
		t.Errorf("expected plan output, got: %s", out.String())
	}
	if fake.MutatingCallCount != 0 {
		t.Errorf("expected 0 mutating calls, got %d", fake.MutatingCallCount)
	}

	// Dry-run delete
	out.Reset()
	errOut.Reset()
	args = []string{
		"frontend", "delete",
		"--project=test-proj",
		"--region=us-central1",
		"--zones=us-central1-a,us-central1-b",
		"--cluster-id=cluster-1",
		"--namespace=default",
		"--gateway-name=my-gw",
		"--dry-run",
	}
	if err := RunWithClient(ctx, fake, args, &out, &errOut); err != nil {
		t.Fatalf("Run frontend delete --dry-run failed: %v", err)
	}
	if !strings.Contains(out.String(), "=== Dry Run Teardown Plan Complete ===") {
		t.Errorf("expected plan output, got: %s", out.String())
	}
	if fake.MutatingCallCount != 0 {
		t.Errorf("expected 0 mutating calls, got %d", fake.MutatingCallCount)
	}
}
