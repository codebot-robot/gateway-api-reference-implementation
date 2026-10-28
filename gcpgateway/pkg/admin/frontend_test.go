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

package admin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/metadata"
	compute "google.golang.org/api/compute/v1"
)

func newTestConfig() *FrontendConfig {
	return &FrontendConfig{
		Project:         "test-project",
		Region:          "us-central1",
		Network:         "default",
		Subnet:          "default",
		ClusterID:       "cluster-1",
		Namespace:       "default",
		GatewayName:     "my-gw",
		Prefix:          "gcpgw",
		Zones:           "us-central1-a,us-central1-b",
		DrainingTimeout: 35,
	}
}

func TestCreateFrontend_FromEmpty(t *testing.T) {
	fake := NewFakeAdminClient()
	cfg := newTestConfig()
	var out bytes.Buffer

	res, err := CreateFrontend(context.Background(), fake, cfg, &out)
	if err != nil {
		t.Fatalf("CreateFrontend failed: %v", err)
	}

	if res.IPAddress != "34.120.254.10" {
		t.Errorf("expected IP 34.120.254.10, got %q", res.IPAddress)
	}
	if len(fake.Addresses) != 1 {
		t.Errorf("expected 1 address, got %d", len(fake.Addresses))
	}
	if len(fake.HealthChecks) != 1 {
		t.Errorf("expected 1 health check, got %d", len(fake.HealthChecks))
	}
	if len(fake.Firewalls) != 1 {
		t.Errorf("expected 1 firewall rule, got %d", len(fake.Firewalls))
	}
	if len(fake.NEGs) != 4 { // 2 ports x 2 zones
		t.Errorf("expected 4 NEGs, got %d", len(fake.NEGs))
	}
	if len(fake.BackendServices) != 2 { // 443 and 80
		t.Errorf("expected 2 backend services, got %d", len(fake.BackendServices))
	}
	for bsName, bs := range fake.BackendServices {
		if len(bs.Backends) != 2 {
			t.Errorf("backend service %s expected 2 backends, got %d", bsName, len(bs.Backends))
		}
	}
	if len(fake.TargetTcpProxies) != 2 {
		t.Errorf("expected 2 target TCP proxies, got %d", len(fake.TargetTcpProxies))
	}
	if len(fake.ForwardingRules) != 2 {
		t.Errorf("expected 2 forwarding rules, got %d", len(fake.ForwardingRules))
	}

	outStr := out.String()
	if !strings.Contains(outStr, "=== Setup Complete ===") {
		t.Errorf("expected output to contain '=== Setup Complete ==='")
	}
	if !strings.Contains(outStr, "Front-end Static IP: 34.120.254.10") {
		t.Errorf("expected output to contain static IP")
	}
	if !strings.Contains(outStr, "gcpgateway-params-my-gw") {
		t.Errorf("expected output to contain ConfigMap manifest")
	}
}

func TestCreateFrontend_EverythingAlreadyExists(t *testing.T) {
	fake := NewFakeAdminClient()
	cfg := newTestConfig()

	// Initial run
	_, err := CreateFrontend(context.Background(), fake, cfg, nil)
	if err != nil {
		t.Fatalf("first CreateFrontend failed: %v", err)
	}

	addrCount := len(fake.Addresses)
	hcCount := len(fake.HealthChecks)
	fwCount := len(fake.Firewalls)
	negCount := len(fake.NEGs)
	bsCount := len(fake.BackendServices)
	proxyCount := len(fake.TargetTcpProxies)
	frCount := len(fake.ForwardingRules)

	var out bytes.Buffer
	// Second run (idempotency check)
	res, err := CreateFrontend(context.Background(), fake, cfg, &out)
	if err != nil {
		t.Fatalf("second CreateFrontend failed: %v", err)
	}

	if res.IPAddress != "34.120.254.10" {
		t.Errorf("expected IP 34.120.254.10, got %q", res.IPAddress)
	}
	// Verify no new resources were created
	if len(fake.Addresses) != addrCount || len(fake.HealthChecks) != hcCount ||
		len(fake.Firewalls) != fwCount || len(fake.NEGs) != negCount ||
		len(fake.BackendServices) != bsCount || len(fake.TargetTcpProxies) != proxyCount ||
		len(fake.ForwardingRules) != frCount {
		t.Errorf("resource counts changed after second run")
	}

	outStr := out.String()
	if !strings.Contains(outStr, "[exists]") {
		t.Errorf("expected output to report existing resources")
	}
}

func TestCreateFrontend_SomeResourcesExist(t *testing.T) {
	fake := NewFakeAdminClient()
	cfg := newTestConfig()

	// Pre-create Address and HealthCheck
	ipName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, "ip")
	fake.Addresses[ipName] = &compute.Address{
		Name:      ipName,
		Address:   "35.200.100.50",
		IpVersion: "IPV4",
	}
	hcName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, "hc")
	fake.HealthChecks[hcName] = &compute.HealthCheck{
		Name: hcName,
	}

	var out bytes.Buffer
	res, err := CreateFrontend(context.Background(), fake, cfg, &out)
	if err != nil {
		t.Fatalf("CreateFrontend failed: %v", err)
	}

	if res.IPAddress != "35.200.100.50" {
		t.Errorf("expected pre-existing IP 35.200.100.50, got %q", res.IPAddress)
	}
	outStr := out.String()
	if !strings.Contains(outStr, fmt.Sprintf("[exists] Static IP address %s already exists", ipName)) {
		t.Errorf("expected existing IP log")
	}
	if !strings.Contains(outStr, fmt.Sprintf("[exists] Health check %s already exists", hcName)) {
		t.Errorf("expected existing HC log")
	}
	// Missing resources must now exist
	if len(fake.Firewalls) != 1 {
		t.Errorf("expected firewall rule to be created")
	}
	if len(fake.NEGs) != 4 {
		t.Errorf("expected 4 NEGs to be created")
	}
	if len(fake.ForwardingRules) != 2 {
		t.Errorf("expected 2 forwarding rules to be created")
	}
}

func TestDeleteFrontend_All(t *testing.T) {
	fake := NewFakeAdminClient()
	cfg := newTestConfig()

	// Provision first
	_, err := CreateFrontend(context.Background(), fake, cfg, nil)
	if err != nil {
		t.Fatalf("CreateFrontend failed: %v", err)
	}

	var out bytes.Buffer
	deleted, err := DeleteFrontend(context.Background(), fake, cfg, &out)
	if err != nil {
		t.Fatalf("DeleteFrontend failed: %v", err)
	}

	if len(deleted) == 0 {
		t.Fatalf("expected deleted resources, got 0")
	}

	// Verify all stores are clean
	if len(fake.Addresses) != 0 || len(fake.HealthChecks) != 0 ||
		len(fake.Firewalls) != 0 || len(fake.NEGs) != 0 ||
		len(fake.BackendServices) != 0 || len(fake.TargetTcpProxies) != 0 ||
		len(fake.ForwardingRules) != 0 {
		t.Errorf("not all resources were deleted: %+v", fake)
	}

	outStr := out.String()
	if !strings.Contains(outStr, "=== Teardown Complete ===") {
		t.Errorf("expected output to contain '=== Teardown Complete ==='")
	}
}

func TestDeleteFrontend_SomeAlreadyGoneAndForeignPreserved(t *testing.T) {
	fake := NewFakeAdminClient()
	cfg := newTestConfig()

	// Provision first
	_, err := CreateFrontend(context.Background(), fake, cfg, nil)
	if err != nil {
		t.Fatalf("CreateFrontend failed: %v", err)
	}

	// Remove Address and HealthCheck in advance (already gone)
	ipName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, "ip")
	delete(fake.Addresses, ipName)
	hcName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, "hc")
	delete(fake.HealthChecks, hcName)

	// Add foreign NEG with mismatched metadata
	foreignDesc := makeDescription("foreign-cluster", "default", "my-gw", 8443)
	fake.NEGs["us-central1-a/gcpgw-foreign-neg"] = &compute.NetworkEndpointGroup{
		Name:        "gcpgw-foreign-neg",
		Description: foreignDesc,
	}

	// Add resource with no description (unparseable)
	fake.ForwardingRules["gcpgw-no-desc"] = &compute.ForwardingRule{
		Name:        "gcpgw-no-desc",
		Description: "",
	}

	deleted, err := DeleteFrontend(context.Background(), fake, cfg, nil)
	if err != nil {
		t.Fatalf("DeleteFrontend failed: %v", err)
	}
	if len(deleted) == 0 {
		t.Fatalf("expected some resources deleted")
	}

	// Foreign NEG must still exist
	if _, ok := fake.NEGs["us-central1-a/gcpgw-foreign-neg"]; !ok {
		t.Errorf("foreign NEG was deleted, should be preserved")
	}
	// No-desc rule must still exist
	if _, ok := fake.ForwardingRules["gcpgw-no-desc"]; !ok {
		t.Errorf("unparseable forwarding rule was deleted, should be preserved")
	}

	// Running DeleteFrontend again is a clean no-op
	deletedSecond, err := DeleteFrontend(context.Background(), fake, cfg, nil)
	if err != nil {
		t.Fatalf("second DeleteFrontend failed: %v", err)
	}
	if len(deletedSecond) != 0 {
		t.Errorf("expected 0 deleted on second run, got %d", len(deletedSecond))
	}
}

func TestOperationsEndWithError(t *testing.T) {
	fake := NewFakeAdminClient()
	cfg := newTestConfig()
	fake.InjectOperationError = errors.New("simulated error: QUOTA_EXCEEDED")

	_, err := CreateFrontend(context.Background(), fake, cfg, nil)
	if err == nil {
		t.Fatalf("expected error from CreateFrontend, got nil")
	}
	if !strings.Contains(err.Error(), "QUOTA_EXCEEDED") {
		t.Errorf("expected QUOTA_EXCEEDED in error, got: %v", err)
	}

	// Delete with error
	fake.InjectOperationError = errors.New("simulated error: PERMISSION_DENIED")
	_, err = DeleteFrontend(context.Background(), fake, cfg, nil)
	if err == nil {
		t.Fatalf("expected error from DeleteFrontend, got nil")
	}
	if !strings.Contains(err.Error(), "PERMISSION_DENIED") {
		t.Errorf("expected PERMISSION_DENIED in error, got: %v", err)
	}
}

func TestDryRun_MakesNoCalls(t *testing.T) {
	fake := NewFakeAdminClient()
	cfg := newTestConfig()
	cfg.DryRun = true

	var out bytes.Buffer
	res, err := CreateFrontend(context.Background(), fake, cfg, &out)
	if err != nil {
		t.Fatalf("CreateFrontend dry-run failed: %v", err)
	}
	if res == nil {
		t.Fatalf("expected result from dry-run")
	}
	if fake.CallCount != 0 {
		t.Errorf("expected 0 calls to client in create dry-run, got %d", fake.CallCount)
	}
	if !strings.Contains(out.String(), "[dry-run] Plan to create front-end resources") {
		t.Errorf("expected plan output in create dry-run")
	}

	out.Reset()
	deleted, err := DeleteFrontend(context.Background(), fake, cfg, &out)
	if err != nil {
		t.Fatalf("DeleteFrontend dry-run failed: %v", err)
	}
	if len(deleted) != 0 {
		t.Errorf("expected 0 deleted in delete dry-run, got %d", len(deleted))
	}
	if fake.CallCount != 0 {
		t.Errorf("expected 0 calls to client in delete dry-run, got %d", fake.CallCount)
	}
	if !strings.Contains(out.String(), "[dry-run] Plan to delete front-end resources") {
		t.Errorf("expected plan output in delete dry-run")
	}
}

func TestNEGDescriptionsAndNaming(t *testing.T) {
	fake := NewFakeAdminClient()
	cfg := newTestConfig()

	_, err := CreateFrontend(context.Background(), fake, cfg, nil)
	if err != nil {
		t.Fatalf("CreateFrontend failed: %v", err)
	}

	for _, port := range []int{8443, 8080} {
		expectedName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, strconv.Itoa(port))
		for _, zone := range cfg.ParsedZones() {
			neg := fake.NEGs[negKey(zone, expectedName)]
			if neg == nil {
				t.Fatalf("NEG %s in zone %s not found in fake", expectedName, zone)
			}
			if neg.Name != expectedName {
				t.Errorf("expected NEG name %q, got %q", expectedName, neg.Name)
			}

			// Validate NEG description parses with metadata.Parse
			parsed, err := metadata.Parse(neg.Description)
			if err != nil {
				t.Fatalf("metadata.Parse failed on description %q: %v", neg.Description, err)
			}

			if !parsed.Matches(cfg.ClusterID, cfg.Namespace, cfg.GatewayName, port) {
				t.Errorf("metadata mismatch: got %+v, expected cluster=%s ns=%s gw=%s port=%d",
					parsed, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, port)
			}
		}
	}
}

func TestCredentialsAndTokensNeverLogged(t *testing.T) {
	fake := NewFakeAdminClient()
	cfg := newTestConfig()

	var out bytes.Buffer
	_, err := CreateFrontend(context.Background(), fake, cfg, &out)
	if err != nil {
		t.Fatalf("CreateFrontend failed: %v", err)
	}

	createOutput := out.String()
	sensitiveTerms := []string{"Bearer", "authorization", "secret", "token", "password", "private_key"}
	for _, term := range sensitiveTerms {
		if strings.Contains(strings.ToLower(createOutput), term) {
			t.Errorf("create output contains sensitive keyword %q", term)
		}
	}

	out.Reset()
	_, err = DeleteFrontend(context.Background(), fake, cfg, &out)
	if err != nil {
		t.Fatalf("DeleteFrontend failed: %v", err)
	}
	deleteOutput := out.String()
	for _, term := range sensitiveTerms {
		if strings.Contains(strings.ToLower(deleteOutput), term) {
			t.Errorf("delete output contains sensitive keyword %q", term)
		}
	}
}
