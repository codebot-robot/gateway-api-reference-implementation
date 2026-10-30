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
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/registry"
)

// Decision indicates whether a request was permitted or refused.
type Decision = registry.AuditDecision

const (
	// DecisionAllowed indicates the request passed authentication and authorization checks.
	DecisionAllowed = registry.DecisionAllowed

	// DecisionRefused indicates the request was denied.
	DecisionRefused = registry.DecisionRefused
)

// Entry represents one structured audit log line per decision.
type Entry = registry.AuditEntry

// Logger writes structured audit log entries to an output writer.
type Logger = registry.AuditLogger

// NewLogger creates a new audit Logger writing to out.
func NewLogger(out io.Writer) *Logger {
	return registry.NewAuditLogger(out)
}

// Record holds in-flight audit information for a single request / RPC.
// Handlers and interceptors annotate the record throughout request processing.
type Record struct {
	// mu guards all record fields below against concurrent read or write access.
	mu         sync.RWMutex
	timestamp  time.Time
	method     string
	identity   registry.CallerIdentity
	statusCode string
	duration   time.Duration
	gateway    string
	hostnames  []string
	decision   Decision
	reason     string
}

// NewRecord creates an empty Record.
func NewRecord() *Record {
	return &Record{
		timestamp: time.Now().UTC(),
	}
}

// Set annotates the Record with handler-provided fields: gateway, hostnames, decision, and reason.
func (r *Record) Set(gateway string, hostnames []string, decision Decision, reason string) *Record {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gateway = gateway
	if hostnames != nil {
		r.hostnames = make([]string, len(hostnames))
		copy(r.hostnames, hostnames)
	} else {
		r.hostnames = nil
	}
	r.decision = decision
	r.reason = reason
	return r
}

// SetGateway sets the Gateway resource identifier.
func (r *Record) SetGateway(gw string) *Record {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gateway = gw
	return r
}

// SetHostnames sets the hostnames requested.
func (r *Record) SetHostnames(hostnames []string) *Record {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if hostnames != nil {
		r.hostnames = make([]string, len(hostnames))
		copy(r.hostnames, hostnames)
	} else {
		r.hostnames = nil
	}
	return r
}

// SetDecision sets the audit decision.
func (r *Record) SetDecision(d Decision) *Record {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.decision = d
	return r
}

// SetReason sets the decision reason.
func (r *Record) SetReason(reason string) *Record {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reason = reason
	return r
}

// SetIdentity sets the verified caller identity.
func (r *Record) SetIdentity(id registry.CallerIdentity) *Record {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.identity = id
	return r
}

// SetMethod sets the RPC or HTTP method.
func (r *Record) SetMethod(method string) *Record {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.method = method
	return r
}

// SetStatusCode sets the status code (e.g. gRPC status string or mapped HTTP status).
func (r *Record) SetStatusCode(code string) *Record {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statusCode = code
	return r
}

// SetDuration sets the duration of the request.
func (r *Record) SetDuration(d time.Duration) *Record {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.duration = d
	return r
}

// Gateway returns the current gateway value.
func (r *Record) Gateway() string {
	if r == nil {
		return ""
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.gateway
}

// Hostnames returns a copy of current hostnames.
func (r *Record) Hostnames() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.hostnames == nil {
		return nil
	}
	res := make([]string, len(r.hostnames))
	copy(res, r.hostnames)
	return res
}

// Decision returns the current audit decision.
func (r *Record) Decision() Decision {
	if r == nil {
		return ""
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.decision
}

// Reason returns the current reason.
func (r *Record) Reason() string {
	if r == nil {
		return ""
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.reason
}

// Identity returns the caller identity.
func (r *Record) Identity() registry.CallerIdentity {
	if r == nil {
		return registry.CallerIdentity{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.identity
}

// Method returns the method string.
func (r *Record) Method() string {
	if r == nil {
		return ""
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.method
}

// StatusCode returns the status code string.
func (r *Record) StatusCode() string {
	if r == nil {
		return ""
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.statusCode
}

// Duration returns the elapsed duration.
func (r *Record) Duration() time.Duration {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.duration
}

// ToEntry converts the Record into a snapshot Entry suitable for logging.
func (r *Record) ToEntry() Entry {
	if r == nil {
		return Entry{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	ts := r.timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	var hostnames []string
	if r.hostnames != nil {
		hostnames = make([]string, len(r.hostnames))
		copy(hostnames, r.hostnames)
	}

	return Entry{
		Timestamp:  ts,
		Method:     r.method,
		Identity:   r.identity,
		StatusCode: r.statusCode,
		Duration:   r.duration,
		Gateway:    r.gateway,
		Hostnames:  hostnames,
		Decision:   r.decision,
		Reason:     r.reason,
	}
}

type recordContextKey struct{}

// NewContext embeds the Record into ctx.
func NewContext(ctx context.Context, rec *Record) context.Context {
	return context.WithValue(ctx, recordContextKey{}, rec)
}

// FromContext extracts the Record from ctx. If no record is present, it returns nil.
// All methods on Record are nil-safe.
func FromContext(ctx context.Context) *Record {
	if ctx == nil {
		return nil
	}
	rec, _ := ctx.Value(recordContextKey{}).(*Record)
	return rec
}

// HTTPStatusToCode maps an HTTP status code to canonical string status code.
func HTTPStatusToCode(status int) string {
	switch status {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent:
		return "OK"
	case http.StatusBadRequest:
		return "InvalidArgument"
	case http.StatusUnauthorized:
		return "Unauthenticated"
	case http.StatusForbidden:
		return "PermissionDenied"
	case http.StatusNotFound:
		return "NotFound"
	case http.StatusMethodNotAllowed:
		return "MethodNotAllowed"
	case http.StatusConflict:
		return "AlreadyExists"
	case http.StatusTooManyRequests:
		return "ResourceExhausted"
	case http.StatusInternalServerError:
		return "Internal"
	case http.StatusNotImplemented:
		return "Unimplemented"
	case http.StatusServiceUnavailable:
		return "Unavailable"
	case http.StatusGatewayTimeout:
		return "DeadlineExceeded"
	default:
		if status >= 200 && status < 300 {
			return "OK"
		}
		return fmt.Sprintf("%d", status)
	}
}

type statusResponseWriter struct {
	http.ResponseWriter
	statusCode  int
	wroteHeader bool
}

func (w *statusResponseWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.statusCode = code
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusResponseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

func (w *statusResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// HTTPMiddleware returns an http.Handler middleware that audits every HTTP request to logger.
// It writes exactly one structured audit line per request, matching the gRPC audit format.
func HTTPMiddleware(logger *Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if logger == nil {
				next.ServeHTTP(w, r)
				return
			}
			if FromContext(r.Context()) != nil {
				next.ServeHTTP(w, r)
				return
			}

			start := time.Now()
			rec := NewRecord()
			rec.SetMethod(r.Method)
			rec.timestamp = start.UTC()

			ctx := NewContext(r.Context(), rec)
			r = r.WithContext(ctx)

			rw := &statusResponseWriter{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}

			defer func() {
				if rcv := recover(); rcv != nil {
					log.Printf("certserver: panic in HTTP handler: %v", rcv)
					rec.SetStatusCode("Internal")
					rec.SetDecision(DecisionRefused)
					rec.SetReason("panic")
					rec.SetDuration(time.Since(start))
					if err := logger.Log(rec.ToEntry()); err != nil {
						log.Printf("certserver: failed to write audit log: %v", err)
					}
					panic(rcv)
				}

				rec.SetDuration(time.Since(start))
				if rec.StatusCode() == "" {
					rec.SetStatusCode(HTTPStatusToCode(rw.statusCode))
				}
				if rec.Decision() == "" {
					if rw.statusCode >= 200 && rw.statusCode < 400 {
						rec.SetDecision(DecisionAllowed)
					} else {
						rec.SetDecision(DecisionRefused)
					}
				}
				if err := logger.Log(rec.ToEntry()); err != nil {
					log.Printf("certserver: failed to write audit log: %v", err)
				}
			}()

			next.ServeHTTP(rw, r)
		})
	}
}

// WrapHTTP wraps an http.Handler with the audit HTTPMiddleware.
func WrapHTTP(logger *Logger, h http.Handler) http.Handler {
	return HTTPMiddleware(logger)(h)
}
