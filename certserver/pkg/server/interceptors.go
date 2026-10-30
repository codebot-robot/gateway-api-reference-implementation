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

package server

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/audit"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/registry"
)

type callerIdentityKey struct{}
type clusterConfigKey struct{}

// WithCallerIdentity stores the verified CallerIdentity in ctx.
func WithCallerIdentity(ctx context.Context, id registry.CallerIdentity) context.Context {
	return context.WithValue(ctx, callerIdentityKey{}, id)
}

// CallerIdentityFromContext extracts verified CallerIdentity from ctx.
func CallerIdentityFromContext(ctx context.Context) (registry.CallerIdentity, bool) {
	if ctx == nil {
		return registry.CallerIdentity{}, false
	}
	id, ok := ctx.Value(callerIdentityKey{}).(registry.CallerIdentity)
	return id, ok
}

// WithClusterConfig stores the verified *registry.ClusterConfig in ctx.
func WithClusterConfig(ctx context.Context, c *registry.ClusterConfig) context.Context {
	return context.WithValue(ctx, clusterConfigKey{}, c)
}

// ClusterConfigFromContext extracts verified *registry.ClusterConfig from ctx.
func ClusterConfigFromContext(ctx context.Context) (*registry.ClusterConfig, bool) {
	if ctx == nil {
		return nil, false
	}
	c, ok := ctx.Value(clusterConfigKey{}).(*registry.ClusterConfig)
	return c, ok
}

// extractTokenFromMetadata extracts bearer token from gRPC metadata.
func extractTokenFromMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return ""
	}

	raw := strings.TrimSpace(authHeaders[0])
	const prefix = "bearer "
	if strings.HasPrefix(strings.ToLower(raw), prefix) {
		return strings.TrimSpace(raw[len(prefix):])
	}
	return raw
}

// extractClusterIDFromMetadata extracts cluster ID from gRPC metadata.
func extractClusterIDFromMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get("cluster-id")
	if len(vals) > 0 && strings.TrimSpace(vals[0]) != "" {
		return strings.TrimSpace(vals[0])
	}
	return ""
}

type tokenGetter interface {
	GetToken() string
}

type clusterIDGetter interface {
	GetClusterId() string
}

// AuditUnaryInterceptor creates a unary interceptor that writes exactly one structured
// audit line per RPC. It wraps around auth and handler logic, capturing duration, status code,
// caller identity, and handler annotations. It also writes the line on error or panic.
func AuditUnaryInterceptor(logger *registry.AuditLogger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		if logger == nil {
			return handler(ctx, req)
		}

		start := time.Now()
		rec := audit.NewRecord()
		rec.SetMethod(info.FullMethod)
		ctx = audit.NewContext(ctx, rec)

		defer func() {
			if r := recover(); r != nil {
				log.Printf("certserver: gRPC unary panic recovered: %v", r)
				rec.SetStatusCode(codes.Internal.String())
				rec.SetDecision(audit.DecisionRefused)
				rec.SetReason("panic")
				rec.SetDuration(time.Since(start))
				if logErr := logger.Log(rec.ToEntry()); logErr != nil {
					log.Printf("certserver: failed to write audit log: %v", logErr)
				}
				panic(r)
			}

			rec.SetDuration(time.Since(start))
			if err != nil {
				st, _ := status.FromError(err)
				if rec.StatusCode() == "" {
					rec.SetStatusCode(st.Code().String())
				}
				if rec.Decision() == "" {
					rec.SetDecision(audit.DecisionRefused)
				}
				if rec.Reason() == "" {
					rec.SetReason(st.Message())
				}
			} else {
				if rec.StatusCode() == "" {
					rec.SetStatusCode(codes.OK.String())
				}
				if rec.Decision() == "" {
					rec.SetDecision(audit.DecisionAllowed)
				}
			}
			if logErr := logger.Log(rec.ToEntry()); logErr != nil {
				log.Printf("certserver: failed to write audit log: %v", logErr)
			}
		}()

		return handler(ctx, req)
	}
}

// AuthUnaryInterceptor verifies service-account tokens against the cluster registry.
// On success, it puts verified identity into ctx. On failure, it rejects unauthenticated
// calls with a generic error, leaving failure details only in the audit record.
func AuthUnaryInterceptor(reg *registry.Registry) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		rec := audit.FromContext(ctx)

		if reg == nil {
			if rec != nil {
				rec.SetDecision(audit.DecisionRefused)
				rec.SetReason("cluster registry not configured")
			}
			return nil, status.Errorf(codes.Unauthenticated, "authentication failed")
		}

		token := extractTokenFromMetadata(ctx)
		if token == "" {
			if tg, ok := req.(tokenGetter); ok {
				token = strings.TrimSpace(tg.GetToken())
			}
		}

		if token == "" {
			if rec != nil {
				rec.SetDecision(audit.DecisionRefused)
				rec.SetReason("missing authorization token")
			}
			return nil, status.Errorf(codes.Unauthenticated, "authentication failed")
		}

		targetClusterID := extractClusterIDFromMetadata(ctx)
		if targetClusterID == "" {
			if cg, ok := req.(clusterIDGetter); ok {
				targetClusterID = strings.TrimSpace(cg.GetClusterId())
			}
		}

		identity, cluster, err := reg.VerifyToken(ctx, token, targetClusterID)
		if err != nil {
			if rec != nil {
				rec.SetDecision(audit.DecisionRefused)
				rec.SetReason(fmt.Sprintf("token verification failed: %v", err))
			}
			return nil, status.Errorf(codes.Unauthenticated, "authentication failed")
		}

		if rec != nil {
			rec.SetIdentity(identity)
		}

		ctx = WithCallerIdentity(ctx, identity)
		if cluster != nil {
			ctx = WithClusterConfig(ctx, cluster)
		}

		return handler(ctx, req)
	}
}

type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}

// AuditStreamInterceptor creates a stream interceptor that writes exactly one structured
// audit line per stream RPC.
func AuditStreamInterceptor(logger *registry.AuditLogger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		if logger == nil {
			return handler(srv, ss)
		}

		start := time.Now()
		rec := audit.NewRecord()
		rec.SetMethod(info.FullMethod)
		ctx := audit.NewContext(ss.Context(), rec)
		wrapped := &wrappedServerStream{ServerStream: ss, ctx: ctx}

		defer func() {
			if r := recover(); r != nil {
				log.Printf("certserver: gRPC stream panic recovered: %v", r)
				rec.SetStatusCode(codes.Internal.String())
				rec.SetDecision(audit.DecisionRefused)
				rec.SetReason("panic")
				rec.SetDuration(time.Since(start))
				if logErr := logger.Log(rec.ToEntry()); logErr != nil {
					log.Printf("certserver: failed to write audit log: %v", logErr)
				}
				panic(r)
			}

			rec.SetDuration(time.Since(start))
			if err != nil {
				st, _ := status.FromError(err)
				if rec.StatusCode() == "" {
					rec.SetStatusCode(st.Code().String())
				}
				if rec.Decision() == "" {
					rec.SetDecision(audit.DecisionRefused)
				}
				if rec.Reason() == "" {
					rec.SetReason(st.Message())
				}
			} else {
				if rec.StatusCode() == "" {
					rec.SetStatusCode(codes.OK.String())
				}
				if rec.Decision() == "" {
					rec.SetDecision(audit.DecisionAllowed)
				}
			}
			if logErr := logger.Log(rec.ToEntry()); logErr != nil {
				log.Printf("certserver: failed to write audit log: %v", logErr)
			}
		}()

		return handler(srv, wrapped)
	}
}

// AuthStreamInterceptor verifies stream RPC tokens and populates verified identity into ctx.
func AuthStreamInterceptor(reg *registry.Registry) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		rec := audit.FromContext(ctx)

		if reg == nil {
			if rec != nil {
				rec.SetDecision(audit.DecisionRefused)
				rec.SetReason("cluster registry not configured")
			}
			return status.Errorf(codes.Unauthenticated, "authentication failed")
		}

		token := extractTokenFromMetadata(ctx)
		if token == "" {
			if rec != nil {
				rec.SetDecision(audit.DecisionRefused)
				rec.SetReason("missing authorization token")
			}
			return status.Errorf(codes.Unauthenticated, "authentication failed")
		}

		targetClusterID := extractClusterIDFromMetadata(ctx)
		identity, cluster, err := reg.VerifyToken(ctx, token, targetClusterID)
		if err != nil {
			if rec != nil {
				rec.SetDecision(audit.DecisionRefused)
				rec.SetReason(fmt.Sprintf("token verification failed: %v", err))
			}
			return status.Errorf(codes.Unauthenticated, "authentication failed")
		}

		if rec != nil {
			rec.SetIdentity(identity)
		}

		ctx = WithCallerIdentity(ctx, identity)
		if cluster != nil {
			ctx = WithClusterConfig(ctx, cluster)
		}

		wrapped := &wrappedServerStream{ServerStream: ss, ctx: ctx}
		return handler(srv, wrapped)
	}
}
