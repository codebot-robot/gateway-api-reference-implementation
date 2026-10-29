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
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	certserverv1 "github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/api/v1"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/cache"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/manager"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/registry"
)

// Options configures the CertificateService gRPC server.
type Options struct {
	ListenAddr        string
	TLSCertFile       string
	TLSKeyFile        string
	TLSConfig         *tls.Config
	ReleaseMode       bool       // If true, returns private keys in GetCertificate responses (phase 1)
	MaxHostnames      int        // Maximum hostnames allowed per request (e.g. 100)
	MaxHostnameLength int        // Maximum length of a single hostname (e.g. 253)
	RateLimitQPS      rate.Limit // Per-identity rate limit (queries per second)
	RateLimitBurst    int        // Per-identity burst size
}

// Server implements certserverv1.CertificateServiceServer.
type Server struct {
	certserverv1.UnimplementedCertificateServiceServer

	opts         Options
	registry     *registry.Registry
	mgr          *manager.Manager
	bindingStore registry.BindingStore
	auditLogger  *registry.AuditLogger
	rateLimiter  *registry.IdentityRateLimiter

	// mu guards listener, grpcServer, and closed.
	mu         sync.Mutex
	listener   net.Listener
	grpcServer *grpc.Server
	closed     bool
}

// NewServer creates a new gRPC Server instance.
func NewServer(
	opts Options,
	reg *registry.Registry,
	mgr *manager.Manager,
	bindingStore registry.BindingStore,
	auditLogger *registry.AuditLogger,
) (*Server, error) {
	if reg == nil {
		return nil, errors.New("registry cannot be nil")
	}
	if mgr == nil {
		return nil, errors.New("manager cannot be nil")
	}
	if bindingStore == nil {
		bindingStore = registry.NewMemoryBindingStore()
	}
	if auditLogger == nil {
		auditLogger = registry.NewAuditLogger(nil)
	}

	if opts.MaxHostnames <= 0 {
		opts.MaxHostnames = 100
	}
	if opts.MaxHostnameLength <= 0 {
		opts.MaxHostnameLength = 253
	}

	limiter := registry.NewIdentityRateLimiter(opts.RateLimitQPS, opts.RateLimitBurst)

	return &Server{
		opts:         opts,
		registry:     reg,
		mgr:          mgr,
		bindingStore: bindingStore,
		auditLogger:  auditLogger,
		rateLimiter:  limiter,
	}, nil
}

// Start binds to the configured listen address, configures TLS, and starts serving gRPC requests.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errors.New("server already closed")
	}

	// 1. Configure TLS
	tlsCfg := s.opts.TLSConfig
	if tlsCfg == nil {
		if s.opts.TLSCertFile == "" || s.opts.TLSKeyFile == "" {
			return errors.New("either TLSConfig or both TLSCertFile and TLSKeyFile must be provided")
		}
		cert, err := tls.LoadX509KeyPair(s.opts.TLSCertFile, s.opts.TLSKeyFile)
		if err != nil {
			return fmt.Errorf("failed to load TLS server key pair: %w", err)
		}
		tlsCfg = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	}

	creds := credentials.NewTLS(tlsCfg)

	// 2. Listen on address
	lis, err := net.Listen("tcp", s.opts.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.opts.ListenAddr, err)
	}
	s.listener = lis

	// 3. Create gRPC server and register service
	s.grpcServer = grpc.NewServer(grpc.Creds(creds))
	certserverv1.RegisterCertificateServiceServer(s.grpcServer, s)

	go func() {
		if err := s.grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			log.Printf("certserver gRPC serve error: %v", err)
		}
	}()

	return nil
}

// Stop gracefully shuts down the gRPC server.
func (s *Server) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}
	s.closed = true

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.listener != nil {
		if err := s.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("certserver: error closing listener: %v", err)
		}
	}
}

// Addr returns the listener's network address, or empty string if not running.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return ""
}

// BindingStore returns the active Gateway binding store.
func (s *Server) BindingStore() registry.BindingStore {
	return s.bindingStore
}

func (s *Server) logAudit(entry registry.AuditEntry) {
	if err := s.auditLogger.Log(entry); err != nil {
		log.Printf("certserver: failed to write audit log: %v", err)
	}
}

// extractToken retrieves the bearer token from gRPC Authorization metadata or request field.
func extractToken(ctx context.Context, reqToken string) string {
	if strings.TrimSpace(reqToken) != "" {
		return strings.TrimSpace(reqToken)
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		authHeaders = md.Get("grpcgateway-authorization")
	}
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

// extractClusterID retrieves optional cluster ID from request field or gRPC metadata.
func extractClusterID(ctx context.Context, reqClusterID string) string {
	if strings.TrimSpace(reqClusterID) != "" {
		return strings.TrimSpace(reqClusterID)
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	for _, key := range []string{"cluster-id", "x-cluster-id"} {
		vals := md.Get(key)
		if len(vals) > 0 && strings.TrimSpace(vals[0]) != "" {
			return strings.TrimSpace(vals[0])
		}
	}
	return ""
}

// GetCertificate implements certserverv1.CertificateServiceServer.
func (s *Server) GetCertificate(ctx context.Context, req *certserverv1.GetCertificateRequest) (*certserverv1.GetCertificateResponse, error) {
	gateway := strings.TrimSpace(req.GetGateway())
	dataPlaneSA := strings.TrimSpace(req.GetDataPlaneServiceAccount())
	rawHostnames := req.GetHostnames()

	// 1. Extract Bearer token
	token := extractToken(ctx, req.GetToken())
	if token == "" {
		s.logAudit(registry.AuditEntry{
			Gateway:   gateway,
			Hostnames: rawHostnames,
			Decision:  registry.DecisionRefused,
			Reason:    "missing authorization token",
		})
		return nil, status.Errorf(codes.Unauthenticated, "missing authorization token")
	}

	// 2. Identify cluster ID if specified
	targetClusterID := extractClusterID(ctx, req.GetClusterId())

	// 3. Cryptographically verify token and extract caller identity
	identity, cluster, err := s.registry.VerifyToken(ctx, token, targetClusterID)
	if err != nil {
		s.logAudit(registry.AuditEntry{
			Identity:  identity,
			Gateway:   gateway,
			Hostnames: rawHostnames,
			Decision:  registry.DecisionRefused,
			Reason:    fmt.Sprintf("token verification failed: %v", err),
		})
		return nil, status.Errorf(codes.Unauthenticated, "authentication failed")
	}

	// 4. Enforce per-identity rate limits
	if !s.rateLimiter.Allow(identity) {
		s.logAudit(registry.AuditEntry{
			Identity:  identity,
			Gateway:   gateway,
			Hostnames: rawHostnames,
			Decision:  registry.DecisionRefused,
			Reason:    "per-identity rate limit exceeded",
		})
		return nil, status.Errorf(codes.ResourceExhausted, "rate limit exceeded for identity %s", identity)
	}

	// 5. Authorize caller as the cluster's designated GARI controller
	if !registry.IsAuthorizedController(cluster, identity) {
		s.logAudit(registry.AuditEntry{
			Identity:  identity,
			Gateway:   gateway,
			Hostnames: rawHostnames,
			Decision:  registry.DecisionRefused,
			Reason:    fmt.Sprintf("caller %s is not an authorized controller for cluster %s", identity, cluster.ClusterID),
		})
		return nil, status.Errorf(codes.PermissionDenied, "caller %s is not an authorized controller for cluster %s", identity, cluster.ClusterID)
	}

	// 6. Validate Gateway, DataPlane SA, and hostnames arguments
	if gateway == "" {
		s.logAudit(registry.AuditEntry{
			Identity:  identity,
			Gateway:   gateway,
			Hostnames: rawHostnames,
			Decision:  registry.DecisionRefused,
			Reason:    "gateway parameter is required",
		})
		return nil, status.Errorf(codes.InvalidArgument, "gateway parameter is required")
	}
	if dataPlaneSA == "" {
		s.logAudit(registry.AuditEntry{
			Identity:  identity,
			Gateway:   gateway,
			Hostnames: rawHostnames,
			Decision:  registry.DecisionRefused,
			Reason:    "data_plane_service_account parameter is required",
		})
		return nil, status.Errorf(codes.InvalidArgument, "data_plane_service_account parameter is required")
	}
	if len(rawHostnames) == 0 {
		s.logAudit(registry.AuditEntry{
			Identity:  identity,
			Gateway:   gateway,
			Hostnames: rawHostnames,
			Decision:  registry.DecisionRefused,
			Reason:    "no hostnames provided",
		})
		return nil, status.Errorf(codes.InvalidArgument, "no hostnames provided")
	}

	// 7. Normalize hostnames using the shared manager normaliser
	normalized, hostnamesKey, err := manager.NormalizeHostnames(rawHostnames, s.opts.MaxHostnames, s.opts.MaxHostnameLength)
	if err != nil {
		s.logAudit(registry.AuditEntry{
			Identity:  identity,
			Gateway:   gateway,
			Hostnames: rawHostnames,
			Decision:  registry.DecisionRefused,
			Reason:    fmt.Sprintf("hostname normalization failed: %v", err),
		})
		return nil, status.Errorf(codes.InvalidArgument, "invalid hostnames: %v", err)
	}

	// 8. Check per-cluster hostname policy (allow / deny lists)
	if err := registry.CheckHostnamesPolicy(cluster, normalized); err != nil {
		s.logAudit(registry.AuditEntry{
			Identity:  identity,
			Gateway:   gateway,
			Hostnames: normalized,
			Decision:  registry.DecisionRefused,
			Reason:    fmt.Sprintf("policy refused: %v", err),
		})
		return nil, status.Errorf(codes.PermissionDenied, "hostname policy refused: %v", err)
	}

	// 9. Record Gateway binding mapping
	s.bindingStore.RecordBinding(registry.Binding{
		ClusterID:               cluster.ClusterID,
		Gateway:                 gateway,
		DataPlaneServiceAccount: dataPlaneSA,
		Hostnames:               normalized,
		RecordedAt:              time.Now(),
	})

	// 10. Check certificate cache via manager interface
	certData, err := s.mgr.GetCertificate(hostnamesKey, s.opts.ReleaseMode)
	if err == nil && certData != nil {
		// Cache hit! Return certificate and private key (in release mode)
		s.logAudit(registry.AuditEntry{
			Identity:  identity,
			Gateway:   gateway,
			Hostnames: normalized,
			Decision:  registry.DecisionAllowed,
			Reason:    "certificate returned from cache",
		})

		return &certserverv1.GetCertificateResponse{
			CertificateChain: certData.CertificateChain,
			PrivateKey:       certData.PrivateKey,
		}, nil
	}

	if !errors.Is(err, cache.ErrNotFound) {
		log.Printf("certserver: error reading certificate from cache for %s: %v", hostnamesKey, err)
		s.logAudit(registry.AuditEntry{
			Identity:  identity,
			Gateway:   gateway,
			Hostnames: normalized,
			Decision:  registry.DecisionRefused,
			Reason:    fmt.Sprintf("failed to read certificate from cache: %v", err),
		})
		return nil, status.Errorf(codes.Internal, "internal error retrieving certificate")
	}

	// 11. Cache miss: start issuance only if none in flight, and return retryable status
	if _, err := s.mgr.EnsureIssuance(ctx, normalized); err != nil {
		log.Printf("certserver: failed to initiate issuance for %s: %v", hostnamesKey, err)
	}

	s.logAudit(registry.AuditEntry{
		Identity:  identity,
		Gateway:   gateway,
		Hostnames: normalized,
		Decision:  registry.DecisionAllowed,
		Reason:    "certificate issuance started; returning retryable status",
	})

	return nil, status.Errorf(codes.Unavailable, "certificate issuance in progress for %s; retry later", hostnamesKey)
}
