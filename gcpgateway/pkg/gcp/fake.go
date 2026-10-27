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

package gcp

import (
	"context"
	"fmt"
	"strings"
	"sync"

	compute "google.golang.org/api/compute/v1"
)

// AttachCall records an AttachNetworkEndpoints invocation.
type AttachCall struct {
	Project   string
	Zone      string
	NEGName   string
	Endpoints []*compute.NetworkEndpoint
}

// DetachCall records a DetachNetworkEndpoints invocation.
type DetachCall struct {
	Project   string
	Zone      string
	NEGName   string
	Endpoints []*compute.NetworkEndpoint
}

// FakeComputeClient provides an in-memory test double for ComputeClient.
type FakeComputeClient struct {
	mu sync.RWMutex

	// negs maps (project/zone/negName) -> *compute.NetworkEndpointGroup
	negs map[string]*compute.NetworkEndpointGroup

	// endpoints maps (project/zone/negName) -> map[endpointKey]*compute.NetworkEndpoint
	endpoints map[string]map[string]*compute.NetworkEndpoint

	// health maps (project/backendService/groupURL) -> *compute.BackendServiceGroupHealth
	health map[string]*compute.BackendServiceGroupHealth

	// Injected errors
	InjectListError   error
	InjectAttachError error
	InjectDetachError error
	InjectHealthError error

	// Recorded calls
	AttachCalls []AttachCall
	DetachCalls []DetachCall
}

// NewFakeComputeClient creates a new FakeComputeClient.
func NewFakeComputeClient() *FakeComputeClient {
	return &FakeComputeClient{
		negs:      make(map[string]*compute.NetworkEndpointGroup),
		endpoints: make(map[string]map[string]*compute.NetworkEndpoint),
		health:    make(map[string]*compute.BackendServiceGroupHealth),
	}
}

func negKey(project, zone, negName string) string {
	return fmt.Sprintf("%s/%s/%s", project, zone, negName)
}

func endpointKey(ep *compute.NetworkEndpoint) string {
	return fmt.Sprintf("%s:%s:%d", ep.Instance, ep.IpAddress, ep.Port)
}

// AddNEG registers a fake NEG in the in-memory store.
func (f *FakeComputeClient) AddNEG(project, zone string, neg *compute.NetworkEndpointGroup) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := negKey(project, zone, neg.Name)
	f.negs[key] = neg
	if _, ok := f.endpoints[key]; !ok {
		f.endpoints[key] = make(map[string]*compute.NetworkEndpoint)
	}
}

// SetBackendServiceHealth configures the health response for a backend service and group.
func (f *FakeComputeClient) SetBackendServiceHealth(project, backendService, groupURL string, health *compute.BackendServiceGroupHealth) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := fmt.Sprintf("%s/%s/%s", project, backendService, groupURL)
	f.health[key] = health
}

func (f *FakeComputeClient) ListNEGs(ctx context.Context, project, zone, prefix string) ([]*compute.NetworkEndpointGroup, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.InjectListError != nil {
		return nil, f.InjectListError
	}

	var result []*compute.NetworkEndpointGroup
	zonePrefix := fmt.Sprintf("%s/%s/", project, zone)
	for key, neg := range f.negs {
		if strings.HasPrefix(key, zonePrefix) {
			if prefix == "" || strings.HasPrefix(neg.Name, prefix) {
				result = append(result, neg)
			}
		}
	}
	return result, nil
}

func (f *FakeComputeClient) ListNetworkEndpoints(ctx context.Context, project, zone, negName string) ([]*compute.NetworkEndpoint, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.InjectListError != nil {
		return nil, f.InjectListError
	}

	key := negKey(project, zone, negName)
	if _, exists := f.negs[key]; !exists {
		return nil, ErrNEGNotFound
	}

	var result []*compute.NetworkEndpoint
	for _, ep := range f.endpoints[key] {
		result = append(result, ep)
	}
	return result, nil
}

func (f *FakeComputeClient) AttachNetworkEndpoints(ctx context.Context, project, zone, negName string, endpoints []*compute.NetworkEndpoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.InjectAttachError != nil {
		return f.InjectAttachError
	}

	key := negKey(project, zone, negName)
	if _, exists := f.negs[key]; !exists {
		return ErrNEGNotFound
	}

	f.AttachCalls = append(f.AttachCalls, AttachCall{
		Project:   project,
		Zone:      zone,
		NEGName:   negName,
		Endpoints: endpoints,
	})

	eps := f.endpoints[key]
	if eps == nil {
		eps = make(map[string]*compute.NetworkEndpoint)
		f.endpoints[key] = eps
	}
	for _, ep := range endpoints {
		eps[endpointKey(ep)] = ep
	}
	return nil
}

func (f *FakeComputeClient) DetachNetworkEndpoints(ctx context.Context, project, zone, negName string, endpoints []*compute.NetworkEndpoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.InjectDetachError != nil {
		return f.InjectDetachError
	}

	key := negKey(project, zone, negName)
	if _, exists := f.negs[key]; !exists {
		return ErrNEGNotFound
	}

	f.DetachCalls = append(f.DetachCalls, DetachCall{
		Project:   project,
		Zone:      zone,
		NEGName:   negName,
		Endpoints: endpoints,
	})

	eps := f.endpoints[key]
	if eps != nil {
		for _, ep := range endpoints {
			delete(eps, endpointKey(ep))
		}
	}
	return nil
}

func (f *FakeComputeClient) GetBackendServiceHealth(ctx context.Context, project, backendService, groupURL string) (*compute.BackendServiceGroupHealth, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.InjectHealthError != nil {
		return nil, f.InjectHealthError
	}

	// Like the real API, require a NEG URL: a bare NEG name is rejected with a 400.
	if !strings.Contains(groupURL, "/networkEndpointGroups/") {
		return nil, fmt.Errorf("googleapi: Error 400: Invalid value for field 'resourceGroup.group': '%s'. The URL is malformed.", groupURL)
	}
	negName := groupURL[strings.LastIndex(groupURL, "/")+1:]
	// Explicit mocks may be registered by URL or by NEG name.
	for _, g := range []string{groupURL, negName} {
		if h, exists := f.health[fmt.Sprintf("%s/%s/%s", project, backendService, g)]; exists {
			return h, nil
		}
	}

	// Default to returning healthy for all registered endpoints if not explicitly mocked
	var healthStatus []*compute.HealthStatus
	// Find NEG by groupURL
	for key, neg := range f.negs {
		if strings.HasSuffix(groupURL, neg.Name) || groupURL == neg.SelfLink {
			for _, ep := range f.endpoints[key] {
				healthStatus = append(healthStatus, &compute.HealthStatus{
					Instance:    ep.Instance,
					IpAddress:   ep.IpAddress,
					Port:        ep.Port,
					HealthState: "HEALTHY",
				})
			}
			break
		}
	}

	return &compute.BackendServiceGroupHealth{
		HealthStatus: healthStatus,
	}, nil
}
