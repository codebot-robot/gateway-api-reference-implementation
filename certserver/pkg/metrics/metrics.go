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
	"github.com/prometheus/client_golang/prometheus"
)

// State represents the state of a managed certificate.
type State string

const (
	StateValid    State = "valid"
	StateRenewing State = "renewing"
	StateExpired  State = "expired"

	StatusSuccess = "success"
	StatusFailure = "failure"
)

// Metrics holds the Prometheus metric collectors for certserver.
type Metrics struct {
	CertificatesByState  *prometheus.GaugeVec
	TimeToExpiry         *prometheus.GaugeVec
	IssuanceTotal        *prometheus.CounterVec
	RenewalTotal         *prometheus.CounterVec
	registeredCollectors []prometheus.Collector
}

// New creates and registers certserver metrics with the provided registerer.
// If registerer is nil, prometheus.DefaultRegisterer is used.
func New(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	m := &Metrics{
		CertificatesByState: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "certserver",
				Name:      "certificates",
				Help:      "Number of managed certificates by state (valid, renewing, expired).",
			},
			[]string{"state"},
		),
		TimeToExpiry: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "certserver",
				Name:      "certificate_expiry_seconds",
				Help:      "Seconds remaining until certificate expiration.",
			},
			[]string{"hostnames"},
		),
		IssuanceTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "certserver",
				Name:      "issuance_total",
				Help:      "Total number of initial certificate issuance attempts by status (success, failure).",
			},
			[]string{"status"},
		),
		RenewalTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "certserver",
				Name:      "renewal_total",
				Help:      "Total number of certificate renewal attempts by status (success, failure).",
			},
			[]string{"status"},
		),
	}

	m.CertificatesByState.WithLabelValues(string(StateValid)).Set(0)
	m.CertificatesByState.WithLabelValues(string(StateRenewing)).Set(0)
	m.CertificatesByState.WithLabelValues(string(StateExpired)).Set(0)

	collectors := []prometheus.Collector{
		m.CertificatesByState,
		m.TimeToExpiry,
		m.IssuanceTotal,
		m.RenewalTotal,
	}
	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			// Collector may already be registered in the provided registry.
			continue
		}
	}
	m.registeredCollectors = collectors

	return m
}

// RecordIssuance records the outcome of an initial certificate issuance attempt.
func (m *Metrics) RecordIssuance(success bool) {
	if m == nil {
		return
	}
	status := StatusSuccess
	if !success {
		status = StatusFailure
	}
	m.IssuanceTotal.WithLabelValues(status).Inc()
}

// RecordRenewal records the outcome of a certificate renewal attempt.
func (m *Metrics) RecordRenewal(success bool) {
	if m == nil {
		return
	}
	status := StatusSuccess
	if !success {
		status = StatusFailure
	}
	m.RenewalTotal.WithLabelValues(status).Inc()
}

// SetCertificateExpiry records the remaining seconds until expiration for a hostname set.
func (m *Metrics) SetCertificateExpiry(hostnames string, secondsRemaining float64) {
	if m == nil {
		return
	}
	m.TimeToExpiry.WithLabelValues(hostnames).Set(secondsRemaining)
}

// RemoveCertificateExpiry removes the expiry metric entry for a hostname set.
func (m *Metrics) RemoveCertificateExpiry(hostnames string) {
	if m == nil {
		return
	}
	m.TimeToExpiry.DeleteLabelValues(hostnames)
}

// SetCertificatesCount updates the count of certificates in a given state.
func (m *Metrics) SetCertificatesCount(state State, count float64) {
	if m == nil {
		return
	}
	m.CertificatesByState.WithLabelValues(string(state)).Set(count)
}
