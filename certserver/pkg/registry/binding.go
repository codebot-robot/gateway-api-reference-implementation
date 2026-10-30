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
	"sync"
	"time"
)

// Binding records an association between a Gateway, its data-plane service account,
// its originating cluster, and its requested hostnames.
type Binding struct {
	ClusterID               string    `json:"cluster_id"`
	Gateway                 string    `json:"gateway"`
	DataPlaneServiceAccount string    `json:"data_plane_service_account"`
	Hostnames               []string  `json:"hostnames"`
	RecordedAt              time.Time `json:"recorded_at"`
}

// ValidatedBinding records that a Gateway has proven domain control for a hostname set.
type ValidatedBinding struct {
	ClusterID    string    `json:"cluster_id"`
	Gateway      string    `json:"gateway"`
	HostnamesKey string    `json:"hostnames_key"`
	ValidatedAt  time.Time `json:"validated_at"`
	ExpiresAt    time.Time `json:"expires_at"`
}

// BindingStore manages active Gateway bindings and successful binding validations.
type BindingStore interface {
	RecordBinding(b Binding)
	GetBinding(clusterID, gateway string) (Binding, bool)
	ListBindings() []Binding
	RecordValidatedBinding(vb ValidatedBinding)
	HasValidatedBinding(clusterID, gateway, hostnamesKey string, now time.Time) bool
	GetValidatedBinding(clusterID, gateway, hostnamesKey string) (ValidatedBinding, bool)
	Prune(now time.Time, maxAge time.Duration)
}

// MemoryBindingStore is a thread-safe in-memory implementation of BindingStore.
type MemoryBindingStore struct {
	// mu guards access to bindings and validatedBindings.
	mu                sync.RWMutex
	bindings          map[string]Binding          // key: clusterID + "/" + gateway
	validatedBindings map[string]ValidatedBinding // key: clusterID + "/" + gateway + "/" + hostnamesKey
}

// NewMemoryBindingStore creates an empty in-memory binding store.
func NewMemoryBindingStore() *MemoryBindingStore {
	return &MemoryBindingStore{
		bindings:          make(map[string]Binding),
		validatedBindings: make(map[string]ValidatedBinding),
	}
}

func bindingKey(clusterID, gateway string) string {
	return clusterID + "/" + gateway
}

func validatedBindingKey(clusterID, gateway, hostnamesKey string) string {
	return clusterID + "/" + gateway + "/" + hostnamesKey
}

// RecordBinding saves or updates a Gateway binding mapping.
func (s *MemoryBindingStore) RecordBinding(b Binding) {
	if b.RecordedAt.IsZero() {
		b.RecordedAt = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bindings[bindingKey(b.ClusterID, b.Gateway)] = b
}

// GetBinding retrieves the binding for a specific cluster and Gateway.
func (s *MemoryBindingStore) GetBinding(clusterID, gateway string) (Binding, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.bindings[bindingKey(clusterID, gateway)]
	return b, ok
}

// ListBindings returns all currently recorded bindings.
func (s *MemoryBindingStore) ListBindings() []Binding {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make([]Binding, 0, len(s.bindings))
	for _, b := range s.bindings {
		res = append(res, b)
	}
	return res
}

// RecordValidatedBinding saves a successful Gateway binding validation.
func (s *MemoryBindingStore) RecordValidatedBinding(vb ValidatedBinding) {
	if vb.ValidatedAt.IsZero() {
		vb.ValidatedAt = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.validatedBindings[validatedBindingKey(vb.ClusterID, vb.Gateway, vb.HostnamesKey)] = vb
}

// HasValidatedBinding reports whether the Gateway has a valid (unexpired) binding validation for hostnamesKey.
func (s *MemoryBindingStore) HasValidatedBinding(clusterID, gateway, hostnamesKey string, now time.Time) bool {
	s.mu.RLock()
	vb, ok := s.validatedBindings[validatedBindingKey(clusterID, gateway, hostnamesKey)]
	s.mu.RUnlock()
	if !ok {
		return false
	}
	if !vb.ExpiresAt.IsZero() && now.After(vb.ExpiresAt) {
		return false
	}
	return true
}

// GetValidatedBinding retrieves the validated binding record.
func (s *MemoryBindingStore) GetValidatedBinding(clusterID, gateway, hostnamesKey string) (ValidatedBinding, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	vb, ok := s.validatedBindings[validatedBindingKey(clusterID, gateway, hostnamesKey)]
	return vb, ok
}

// Prune removes expired validated bindings and recorded bindings older than maxAge.
// If maxAge <= 0, a default of 30 days is applied.
func (s *MemoryBindingStore) Prune(now time.Time, maxAge time.Duration) {
	if maxAge <= 0 {
		maxAge = 30 * 24 * time.Hour
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, b := range s.bindings {
		if !b.RecordedAt.IsZero() && now.Sub(b.RecordedAt) > maxAge {
			delete(s.bindings, k)
		}
	}
	for k, vb := range s.validatedBindings {
		if !vb.ExpiresAt.IsZero() && now.After(vb.ExpiresAt) {
			delete(s.validatedBindings, k)
		}
	}
}
