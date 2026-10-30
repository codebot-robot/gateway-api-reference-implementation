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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/certserver/api/jwt"
)

const (
	// CertServerAudience is the required audience for ServiceAccount tokens presented to certserver.
	CertServerAudience = "certserver"
)

// Registry manages trusted Kubernetes clusters and token verification.
type Registry struct {
	jwksCache       *jwt.JWKSCache
	verifier        *jwt.Verifier
	httpClient      *http.Client
	unknownKidLimit time.Duration
	refreshInterval time.Duration
	timeNow         func() time.Time

	// mu guards clustersByID and clustersByIssuer.
	mu               sync.RWMutex
	clustersByID     map[string]*ClusterConfig
	clustersByIssuer map[string]*ClusterConfig
}

// Option configures a Registry.
type Option func(*Registry)

// WithHTTPClient sets a custom HTTP client for OIDC discovery and JWKS fetching.
func WithHTTPClient(client *http.Client) Option {
	return func(r *Registry) {
		if client != nil {
			r.httpClient = client
		}
	}
}

// WithUnknownKidRateLimit sets the minimum duration between JWKS refreshes for unknown kids.
func WithUnknownKidRateLimit(d time.Duration) Option {
	return func(r *Registry) {
		if d > 0 {
			r.unknownKidLimit = d
		}
	}
}

// WithRefreshInterval sets the periodic JWKS background refresh interval.
func WithRefreshInterval(d time.Duration) Option {
	return func(r *Registry) {
		if d > 0 {
			r.refreshInterval = d
		}
	}
}

// WithTimeNow overrides time.Now for deterministic testing.
func WithTimeNow(fn func() time.Time) Option {
	return func(r *Registry) {
		if fn != nil {
			r.timeNow = fn
		}
	}
}

// NormalizeIssuer normalizes an issuer URL (trims whitespace and trailing slash).
func NormalizeIssuer(iss string) string {
	return strings.TrimRight(strings.TrimSpace(iss), "/")
}

// NewRegistry initializes a Registry from a slice of ClusterConfigs.
func NewRegistry(clusters []ClusterConfig, opts ...Option) (*Registry, error) {
	r := &Registry{
		clustersByID:     make(map[string]*ClusterConfig),
		clustersByIssuer: make(map[string]*ClusterConfig),
		timeNow:          time.Now,
	}
	for _, opt := range opts {
		opt(r)
	}

	var issuerConfigs []jwt.IssuerConfig
	for i := range clusters {
		c := clusters[i]
		if err := c.Validate(); err != nil {
			return nil, err
		}

		if _, exists := r.clustersByID[c.ClusterID]; exists {
			return nil, fmt.Errorf("duplicate cluster ID %q", c.ClusterID)
		}
		r.clustersByID[c.ClusterID] = &c

		normIss := NormalizeIssuer(c.IssuerURL)
		if _, exists := r.clustersByIssuer[normIss]; exists {
			return nil, fmt.Errorf("duplicate issuer URL %q", c.IssuerURL)
		}
		r.clustersByIssuer[normIss] = &c

		// Configure JWKS source
		issCfg := jwt.IssuerConfig{
			IssuerURL: c.IssuerURL,
			JWKSFile:  c.JWKSFile,
			JWKSURL:   c.JWKSURL,
		}
		issuerConfigs = append(issuerConfigs, issCfg)
	}

	// Initialize JWKS Cache
	var jwksOpts []jwt.JWKSCacheOption
	if r.httpClient != nil {
		jwksOpts = append(jwksOpts, jwt.WithHTTPClient(r.httpClient))
	}
	if r.unknownKidLimit > 0 {
		jwksOpts = append(jwksOpts, jwt.WithUnknownKidRateLimit(r.unknownKidLimit))
	}
	if r.refreshInterval > 0 {
		jwksOpts = append(jwksOpts, jwt.WithRefreshInterval(r.refreshInterval))
	}
	if r.timeNow != nil {
		jwksOpts = append(jwksOpts, jwt.WithJWKSTimeNow(r.timeNow))
	}

	cache, err := jwt.NewJWKSCache(issuerConfigs, jwksOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize JWKS cache: %w", err)
	}
	r.jwksCache = cache

	var verifierOpts []jwt.VerifierOption
	verifierOpts = append(verifierOpts, jwt.WithAudience(CertServerAudience))
	if r.timeNow != nil {
		verifierOpts = append(verifierOpts, jwt.WithTimeNow(r.timeNow))
	}
	r.verifier = jwt.NewVerifier(cache, verifierOpts...)

	return r, nil
}

// Close closes the underlying JWKS cache background loops.
func (r *Registry) Close() error {
	if r.jwksCache != nil {
		return r.jwksCache.Close()
	}
	return nil
}

// GetCluster returns the cluster configuration for clusterID.
func (r *Registry) GetCluster(clusterID string) (*ClusterConfig, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.clustersByID[clusterID]
	return c, ok
}

// GetClusterByIssuer returns the cluster configuration for an issuer URL.
func (r *Registry) GetClusterByIssuer(issuer string) (*ClusterConfig, bool) {
	norm := NormalizeIssuer(issuer)
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.clustersByIssuer[norm]
	return c, ok
}

type unverifiedPayload struct {
	Iss string `json:"iss"`
	Sub string `json:"sub"`
}

func parseUnverifiedPayload(token string) (*unverifiedPayload, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, errors.New("invalid token format: expected 3 parts")
	}
	s := strings.TrimRight(parts[1], "=")
	payloadBytes, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("decoding token payload: %w", err)
	}
	var p unverifiedPayload
	if err := json.Unmarshal(bytes.TrimSpace(payloadBytes), &p); err != nil {
		return nil, fmt.Errorf("parsing token payload JSON: %w", err)
	}
	return &p, nil
}

// VerifyToken verifies a ServiceAccount token against the appropriate cluster's JWKS and issuer.
// If requestedClusterID is non-empty, the token's issuer MUST match that cluster's entry.
// Returns the verified CallerIdentity and matching ClusterConfig.
func (r *Registry) VerifyToken(ctx context.Context, token string, requestedClusterID string) (CallerIdentity, *ClusterConfig, error) {
	if strings.TrimSpace(token) == "" {
		return CallerIdentity{}, nil, errors.New("token is empty")
	}

	// 1. Inspect unverified payload to extract issuer
	unverified, err := parseUnverifiedPayload(token)
	if err != nil {
		return CallerIdentity{}, nil, fmt.Errorf("invalid token: %w", err)
	}
	if unverified.Iss == "" {
		return CallerIdentity{}, nil, errors.New("token missing required 'iss' claim")
	}

	var cluster *ClusterConfig

	// 2. Identify and validate target cluster
	if requestedClusterID != "" {
		targetCluster, ok := r.GetCluster(requestedClusterID)
		if !ok {
			return CallerIdentity{}, nil, fmt.Errorf("unknown or unconfigured cluster ID %q", requestedClusterID)
		}
		// "token from cluster A presented as cluster B": iss must match the cluster entry
		if NormalizeIssuer(unverified.Iss) != NormalizeIssuer(targetCluster.IssuerURL) {
			return CallerIdentity{}, nil, fmt.Errorf("token issuer %q does not match cluster %q issuer %q", unverified.Iss, requestedClusterID, targetCluster.IssuerURL)
		}
		cluster = targetCluster
	} else {
		// Lookup cluster by issuer URL
		targetCluster, ok := r.GetClusterByIssuer(unverified.Iss)
		if !ok {
			return CallerIdentity{}, nil, fmt.Errorf("untrusted or unconfigured token issuer: %q", unverified.Iss)
		}
		cluster = targetCluster
	}

	// 3. Perform cryptographic signature, algorithm, audience, and expiration verification
	claims, err := r.verifier.Verify(ctx, token)
	if err != nil {
		return CallerIdentity{}, nil, fmt.Errorf("token verification failed: %w", err)
	}

	// Double-check verified issuer against cluster
	if NormalizeIssuer(claims.Issuer) != NormalizeIssuer(cluster.IssuerURL) {
		return CallerIdentity{}, nil, fmt.Errorf("verified issuer %q does not match cluster %q issuer %q", claims.Issuer, cluster.ClusterID, cluster.IssuerURL)
	}

	// 4. Extract namespace and service account from subject
	ns, sa, err := ParseSubject(claims.Subject)
	if err != nil {
		return CallerIdentity{}, nil, fmt.Errorf("invalid token subject: %w", err)
	}

	identity := CallerIdentity{
		ClusterID:      cluster.ClusterID,
		Namespace:      ns,
		ServiceAccount: sa,
	}

	return identity, cluster, nil
}
