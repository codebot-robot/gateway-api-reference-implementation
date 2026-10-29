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

// BindingStore manages active Gateway bindings.
type BindingStore interface {
	RecordBinding(b Binding)
	GetBinding(clusterID, gateway string) (Binding, bool)
	ListBindings() []Binding
}

// MemoryBindingStore is a thread-safe in-memory implementation of BindingStore.
type MemoryBindingStore struct {
	// mu guards access to bindings.
	mu       sync.RWMutex
	bindings map[string]Binding // key: clusterID + "/" + gateway
}

// NewMemoryBindingStore creates an empty in-memory binding store.
func NewMemoryBindingStore() *MemoryBindingStore {
	return &MemoryBindingStore{
		bindings: make(map[string]Binding),
	}
}

func bindingKey(clusterID, gateway string) string {
	return clusterID + "/" + gateway
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
