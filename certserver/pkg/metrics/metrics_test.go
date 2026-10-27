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

package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetrics_Lifecycle(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	// Record issuances
	m.RecordIssuance(true)
	m.RecordIssuance(false)

	// Record renewals
	m.RecordRenewal(true)
	m.RecordRenewal(false)

	// Set certificate state counts
	m.SetCertificatesCount(StateValid, 3)
	m.SetCertificatesCount(StateRenewing, 1)
	m.SetCertificatesCount(StateExpired, 0)

	// Set and remove expiry seconds
	m.SetCertificateExpiry("example.com", 3600)
	m.RemoveCertificateExpiry("example.com")

	// Verify metric values can be gathered
	metricFamilies, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}

	foundIssuance := false
	for _, mf := range metricFamilies {
		if mf.GetName() == "certserver_issuance_total" {
			foundIssuance = true
			for _, metric := range mf.GetMetric() {
				var status string
				for _, lbl := range metric.GetLabel() {
					if lbl.GetName() == "status" {
						status = lbl.GetValue()
					}
				}
				if status == StatusSuccess && metric.GetCounter().GetValue() != 1 {
					t.Errorf("expected 1 success issuance, got %f", metric.GetCounter().GetValue())
				}
				if status == StatusFailure && metric.GetCounter().GetValue() != 1 {
					t.Errorf("expected 1 failure issuance, got %f", metric.GetCounter().GetValue())
				}
			}
		}
	}

	if !foundIssuance {
		t.Error("certserver_issuance_total metric not found")
	}

	// Verify nil-safety
	var nilMetrics *Metrics
	nilMetrics.RecordIssuance(true)
	nilMetrics.RecordRenewal(true)
	nilMetrics.SetCertificateExpiry("x", 1)
	nilMetrics.RemoveCertificateExpiry("x")
	nilMetrics.SetCertificatesCount(StateValid, 1)
}
