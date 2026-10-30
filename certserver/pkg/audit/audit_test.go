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

package audit

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/registry"
)

func TestRecord_NilSafe(t *testing.T) {
	var rec *Record
	// Must not panic on nil receiver
	rec.Set("gw", []string{"host"}, DecisionAllowed, "reason")
	rec.SetGateway("gw")
	rec.SetHostnames([]string{"host"})
	rec.SetDecision(DecisionAllowed)
	rec.SetReason("reason")
	rec.SetIdentity(registry.CallerIdentity{})
	rec.SetMethod("GET")
	rec.SetStatusCode("OK")
	rec.SetDuration(time.Second)

	if rec.Gateway() != "" {
		t.Errorf("expected empty gateway, got %q", rec.Gateway())
	}
	if rec.Hostnames() != nil {
		t.Errorf("expected nil hostnames, got %v", rec.Hostnames())
	}
	if rec.Decision() != "" {
		t.Errorf("expected empty decision, got %q", rec.Decision())
	}
	if rec.Reason() != "" {
		t.Errorf("expected empty reason, got %q", rec.Reason())
	}
	if rec.Method() != "" {
		t.Errorf("expected empty method, got %q", rec.Method())
	}
	if rec.StatusCode() != "" {
		t.Errorf("expected empty status code, got %q", rec.StatusCode())
	}
	if rec.Duration() != 0 {
		t.Errorf("expected 0 duration, got %v", rec.Duration())
	}

	entry := rec.ToEntry()
	if !entry.Timestamp.IsZero() {
		t.Errorf("expected zero timestamp on nil record ToEntry")
	}
}

func TestRecord_SetAndToEntry(t *testing.T) {
	rec := NewRecord()
	id := registry.CallerIdentity{
		ClusterID:      "test-cluster",
		Namespace:      "test-ns",
		ServiceAccount: "test-sa",
	}
	rec.SetIdentity(id)
	rec.SetMethod("/certserver.v1.CertificateService/GetCertificate")
	rec.Set("default/my-gateway", []string{"example.com", "foo.com"}, DecisionAllowed, "issued")
	rec.SetStatusCode("OK")
	rec.SetDuration(15 * time.Millisecond)

	entry := rec.ToEntry()
	if entry.Method != "/certserver.v1.CertificateService/GetCertificate" {
		t.Errorf("unexpected method: %s", entry.Method)
	}
	if entry.Gateway != "default/my-gateway" {
		t.Errorf("unexpected gateway: %s", entry.Gateway)
	}
	if len(entry.Hostnames) != 2 || entry.Hostnames[0] != "example.com" {
		t.Errorf("unexpected hostnames: %v", entry.Hostnames)
	}
	if entry.Decision != DecisionAllowed {
		t.Errorf("unexpected decision: %s", entry.Decision)
	}
	if entry.Reason != "issued" {
		t.Errorf("unexpected reason: %s", entry.Reason)
	}
	if entry.StatusCode != "OK" {
		t.Errorf("unexpected status code: %s", entry.StatusCode)
	}
	if entry.Duration != 15*time.Millisecond {
		t.Errorf("unexpected duration: %v", entry.Duration)
	}
	if entry.Identity != id {
		t.Errorf("unexpected identity: %+v", entry.Identity)
	}
}

func TestContextHelpers(t *testing.T) {
	ctx := t.Context()
	if FromContext(ctx) != nil {
		t.Fatalf("expected nil record from empty context")
	}

	rec := NewRecord()
	rec.SetGateway("test-gw")
	ctx = NewContext(ctx, rec)

	got := FromContext(ctx)
	if got == nil {
		t.Fatalf("expected non-nil record from context")
	}
	if got.Gateway() != "test-gw" {
		t.Errorf("expected gateway 'test-gw', got %q", got.Gateway())
	}
}

func TestHTTPMiddleware_Success(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := FromContext(r.Context())
		if rec == nil {
			t.Errorf("expected record in context")
		}
		rec.Set("test-gw", []string{"example.com"}, DecisionAllowed, "challenge solved")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	wrapped := WrapHTTP(logger, handler)

	req := httptest.NewRequest(http.MethodGet, "/.well-known/acme-challenge/tok123", nil)
	rr := httptest.NewRecorder()

	wrapped.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected exactly 1 audit line, got %d: %s", len(lines), buf.String())
	}

	var entry Entry
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("failed to unmarshal audit entry: %v", err)
	}

	if entry.Method != http.MethodGet {
		t.Errorf("expected method GET, got %s", entry.Method)
	}
	if entry.StatusCode != "OK" {
		t.Errorf("expected status code OK, got %s", entry.StatusCode)
	}
	if entry.Gateway != "test-gw" {
		t.Errorf("expected gateway test-gw, got %s", entry.Gateway)
	}
	if len(entry.Hostnames) != 1 || entry.Hostnames[0] != "example.com" {
		t.Errorf("expected hostnames [example.com], got %v", entry.Hostnames)
	}
	if entry.Decision != DecisionAllowed {
		t.Errorf("expected decision allowed, got %s", entry.Decision)
	}
	if entry.Reason != "challenge solved" {
		t.Errorf("expected reason 'challenge solved', got %s", entry.Reason)
	}
	if entry.Duration <= 0 {
		t.Errorf("expected positive duration, got %v", entry.Duration)
	}
}

func TestHTTPMiddleware_RefusedNotFound(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := FromContext(r.Context())
		rec.Set("test-gw", []string{"example.com"}, DecisionRefused, "token not found")
		http.NotFound(w, r)
	})

	wrapped := WrapHTTP(logger, handler)

	req := httptest.NewRequest(http.MethodGet, "/.well-known/acme-challenge/invalid", nil)
	rr := httptest.NewRecorder()

	wrapped.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rr.Code)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected exactly 1 audit line, got %d: %s", len(lines), buf.String())
	}

	var entry Entry
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("failed to unmarshal audit entry: %v", err)
	}

	if entry.StatusCode != "NotFound" {
		t.Errorf("expected status code NotFound, got %s", entry.StatusCode)
	}
	if entry.Decision != DecisionRefused {
		t.Errorf("expected decision refused, got %s", entry.Decision)
	}
	if entry.Reason != "token not found" {
		t.Errorf("expected reason 'token not found', got %s", entry.Reason)
	}
}

func TestHTTPMiddleware_Panic(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := FromContext(r.Context())
		rec.SetGateway("panic-gw")
		panic("something went critically wrong")
	})

	wrapped := WrapHTTP(logger, handler)

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	rr := httptest.NewRecorder()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic to be propagated")
		}

		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		if len(lines) != 1 {
			t.Fatalf("expected exactly 1 audit line on panic, got %d: %s", len(lines), buf.String())
		}

		var entry Entry
		if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
			t.Fatalf("failed to unmarshal audit entry: %v", err)
		}

		if entry.StatusCode != "Internal" {
			t.Errorf("expected status code Internal, got %s", entry.StatusCode)
		}
		if entry.Decision != DecisionRefused {
			t.Errorf("expected decision refused, got %s", entry.Decision)
		}
		if entry.Reason != "panic" {
			t.Errorf("expected reason 'panic', got %s", entry.Reason)
		}
		if entry.Gateway != "panic-gw" {
			t.Errorf("expected gateway 'panic-gw', got %s", entry.Gateway)
		}
	}()

	wrapped.ServeHTTP(rr, req)
}
