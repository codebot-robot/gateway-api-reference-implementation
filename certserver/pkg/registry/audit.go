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

package registry

import (
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"
)

// AuditDecision indicates whether a request was permitted or refused.
type AuditDecision string

const (
	// DecisionAllowed indicates the request passed authentication and authorization checks.
	DecisionAllowed AuditDecision = "allowed"

	// DecisionRefused indicates the request was denied.
	DecisionRefused AuditDecision = "refused"
)

// AuditEntry represents one structured audit log line per decision.
// Notice: Audit entries NEVER contain sensitive credentials, tokens, or private keys.
type AuditEntry struct {
	Timestamp  time.Time      `json:"timestamp"`
	Method     string         `json:"method,omitempty"`
	Identity   CallerIdentity `json:"identity,omitempty"`
	StatusCode string         `json:"status_code,omitempty"`
	Duration   time.Duration  `json:"duration,omitempty"`
	Gateway    string         `json:"gateway,omitempty"`
	Hostnames  []string       `json:"hostnames,omitempty"`
	Decision   AuditDecision  `json:"decision,omitempty"`
	Reason     string         `json:"reason,omitempty"`
}

// AuditLogger writes structured audit log entries to an output writer.
type AuditLogger struct {
	// mu guards writes to out.
	mu  sync.Mutex
	out io.Writer
}

// NewAuditLogger creates a new AuditLogger writing to the provided io.Writer.
// If out is nil, os.Stdout is used.
func NewAuditLogger(out io.Writer) *AuditLogger {
	if out == nil {
		out = os.Stdout
	}
	return &AuditLogger{out: out}
}

// Log writes one single-line JSON audit record.
func (l *AuditLogger) Log(entry AuditEntry) error {
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now().UTC()
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	data = append(data, '\n')
	_, err = l.out.Write(data)
	return err
}
