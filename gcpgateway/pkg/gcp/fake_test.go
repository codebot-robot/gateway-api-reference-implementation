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

package gcp

import (
	"strings"
	"testing"

	compute "google.golang.org/api/compute/v1"
)

func TestFakeComputeClient_GetBackendServiceHealth_BareNameFails(t *testing.T) {
	fake := NewFakeComputeClient()
	ctx := t.Context()

	_, err := fake.GetBackendServiceHealth(ctx, "my-project", "bs-443", "gcpgw-71f674c6-8443")
	if err == nil {
		t.Fatal("expected error when GetBackendServiceHealth is called with bare NEG name, got nil")
	}
	if !strings.Contains(err.Error(), "The URL is malformed") {
		t.Fatalf("expected malformed URL error message, got: %v", err)
	}
	if !strings.Contains(err.Error(), "googleapi: Error 400") {
		t.Fatalf("expected 400 status error, got: %v", err)
	}
}

func TestFakeComputeClient_GetBackendServiceHealth_FullURL(t *testing.T) {
	fake := NewFakeComputeClient()
	ctx := t.Context()
	project := "my-project"
	zone := "us-central1-a"
	negName := "gcpgw-71f674c6-8443"
	bsName := "bs-443"
	negURL := "https://www.googleapis.com/compute/v1/projects/" + project + "/zones/" + zone + "/networkEndpointGroups/" + negName

	fake.AddNEG(project, zone, &compute.NetworkEndpointGroup{
		Name: negName,
	})
	if err := fake.AttachNetworkEndpoints(ctx, project, zone, negName, []*compute.NetworkEndpoint{
		{Instance: "node-1", IpAddress: "10.0.1.5", Port: 8443},
	}); err != nil {
		t.Fatalf("AttachNetworkEndpoints failed: %v", err)
	}

	// 1. Default healthy response when querying with full URL
	h, err := fake.GetBackendServiceHealth(ctx, project, bsName, negURL)
	if err != nil {
		t.Fatalf("unexpected error with full NEG URL: %v", err)
	}
	if len(h.HealthStatus) != 1 || h.HealthStatus[0].HealthState != "HEALTHY" {
		t.Fatalf("expected 1 HEALTHY endpoint, got: %+v", h.HealthStatus)
	}

	// 2. Explicit mock registered by bare NEG name
	fake.SetBackendServiceHealth(project, bsName, negName, &compute.BackendServiceGroupHealth{
		HealthStatus: []*compute.HealthStatus{
			{Instance: "node-1", IpAddress: "10.0.1.5", Port: 8443, HealthState: "UNHEALTHY"},
		},
	})
	h, err = fake.GetBackendServiceHealth(ctx, project, bsName, negURL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(h.HealthStatus) != 1 || h.HealthStatus[0].HealthState != "UNHEALTHY" {
		t.Fatalf("expected UNHEALTHY mock by bare name, got: %+v", h.HealthStatus)
	}

	// 3. Explicit mock registered by full NEG URL
	fake.SetBackendServiceHealth(project, bsName, negURL, &compute.BackendServiceGroupHealth{
		HealthStatus: []*compute.HealthStatus{
			{Instance: "node-1", IpAddress: "10.0.1.5", Port: 8443, HealthState: "DRAINING"},
		},
	})
	h, err = fake.GetBackendServiceHealth(ctx, project, bsName, negURL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(h.HealthStatus) != 1 || h.HealthStatus[0].HealthState != "DRAINING" {
		t.Fatalf("expected DRAINING mock by full URL, got: %+v", h.HealthStatus)
	}
}
