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

package admin

import (
	"context"
	"fmt"
	"sort"
	"sync"

	compute "google.golang.org/api/compute/v1"
)

// FakeAdminClient provides an in-memory test double implementing AdminClient.
type FakeAdminClient struct {
	mu sync.RWMutex

	// CallCount tracks the total number of calls to any client method.
	CallCount int

	// InjectOperationError causes mutating operations (or all operations) to fail.
	InjectOperationError error

	// InjectCallError allows injecting errors for specific operations by key (e.g. "CreateAddress", "DeleteNEG").
	InjectCallError map[string]error

	// In-memory stores
	Addresses        map[string]*compute.Address
	HealthChecks     map[string]*compute.HealthCheck
	Firewalls        map[string]*compute.Firewall
	NEGs             map[string]*compute.NetworkEndpointGroup // key: zone + "/" + name
	BackendServices  map[string]*compute.BackendService
	TargetTcpProxies map[string]*compute.TargetTcpProxy
	ForwardingRules  map[string]*compute.ForwardingRule
	RegionZones      map[string][]string
}

// NewFakeAdminClient creates an initialized FakeAdminClient.
func NewFakeAdminClient() *FakeAdminClient {
	return &FakeAdminClient{
		InjectCallError:  make(map[string]error),
		Addresses:        make(map[string]*compute.Address),
		HealthChecks:     make(map[string]*compute.HealthCheck),
		Firewalls:        make(map[string]*compute.Firewall),
		NEGs:             make(map[string]*compute.NetworkEndpointGroup),
		BackendServices:  make(map[string]*compute.BackendService),
		TargetTcpProxies: make(map[string]*compute.TargetTcpProxy),
		ForwardingRules:  make(map[string]*compute.ForwardingRule),
		RegionZones:      make(map[string][]string),
	}
}

func (f *FakeAdminClient) recordCall(opName string) error {
	f.CallCount++
	if err, ok := f.InjectCallError[opName]; ok {
		return err
	}
	if f.InjectOperationError != nil {
		return f.InjectOperationError
	}
	return nil
}

// Addresses

func (f *FakeAdminClient) GetAddress(ctx context.Context, project, name string) (*compute.Address, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("GetAddress"); err != nil {
		return nil, err
	}
	if addr, ok := f.Addresses[name]; ok {
		return addr, nil
	}
	return nil, nil
}

func (f *FakeAdminClient) CreateAddress(ctx context.Context, project string, addr *compute.Address) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("CreateAddress"); err != nil {
		return err
	}
	cp := *addr
	if cp.Address == "" {
		cp.Address = "34.120.254.10"
	}
	f.Addresses[addr.Name] = &cp
	return nil
}

func (f *FakeAdminClient) DeleteAddress(ctx context.Context, project, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("DeleteAddress"); err != nil {
		return err
	}
	delete(f.Addresses, name)
	return nil
}

// Health Checks

func (f *FakeAdminClient) GetHealthCheck(ctx context.Context, project, name string) (*compute.HealthCheck, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("GetHealthCheck"); err != nil {
		return nil, err
	}
	if hc, ok := f.HealthChecks[name]; ok {
		return hc, nil
	}
	return nil, nil
}

func (f *FakeAdminClient) CreateHealthCheck(ctx context.Context, project string, hc *compute.HealthCheck) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("CreateHealthCheck"); err != nil {
		return err
	}
	cp := *hc
	f.HealthChecks[hc.Name] = &cp
	return nil
}

func (f *FakeAdminClient) DeleteHealthCheck(ctx context.Context, project, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("DeleteHealthCheck"); err != nil {
		return err
	}
	delete(f.HealthChecks, name)
	return nil
}

// Firewall Rules

func (f *FakeAdminClient) GetFirewall(ctx context.Context, project, name string) (*compute.Firewall, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("GetFirewall"); err != nil {
		return nil, err
	}
	if fw, ok := f.Firewalls[name]; ok {
		return fw, nil
	}
	return nil, nil
}

func (f *FakeAdminClient) CreateFirewall(ctx context.Context, project string, fw *compute.Firewall) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("CreateFirewall"); err != nil {
		return err
	}
	cp := *fw
	f.Firewalls[fw.Name] = &cp
	return nil
}

func (f *FakeAdminClient) DeleteFirewall(ctx context.Context, project, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("DeleteFirewall"); err != nil {
		return err
	}
	delete(f.Firewalls, name)
	return nil
}

// Network Endpoint Groups

func negKey(zone, name string) string {
	return fmt.Sprintf("%s/%s", zone, name)
}

func (f *FakeAdminClient) GetNEG(ctx context.Context, project, zone, name string) (*compute.NetworkEndpointGroup, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("GetNEG"); err != nil {
		return nil, err
	}
	if neg, ok := f.NEGs[negKey(zone, name)]; ok {
		return neg, nil
	}
	return nil, nil
}

func (f *FakeAdminClient) CreateNEG(ctx context.Context, project, zone string, neg *compute.NetworkEndpointGroup) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("CreateNEG"); err != nil {
		return err
	}
	cp := *neg
	f.NEGs[negKey(zone, neg.Name)] = &cp
	return nil
}

func (f *FakeAdminClient) DeleteNEG(ctx context.Context, project, zone, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("DeleteNEG"); err != nil {
		return err
	}
	delete(f.NEGs, negKey(zone, name))
	return nil
}

// Backend Services

func (f *FakeAdminClient) GetBackendService(ctx context.Context, project, name string) (*compute.BackendService, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("GetBackendService"); err != nil {
		return nil, err
	}
	if bs, ok := f.BackendServices[name]; ok {
		return bs, nil
	}
	return nil, nil
}

func (f *FakeAdminClient) CreateBackendService(ctx context.Context, project string, bs *compute.BackendService) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("CreateBackendService"); err != nil {
		return err
	}
	cp := *bs
	if len(bs.Backends) > 0 {
		cp.Backends = make([]*compute.Backend, len(bs.Backends))
		copy(cp.Backends, bs.Backends)
	}
	f.BackendServices[bs.Name] = &cp
	return nil
}

func (f *FakeAdminClient) UpdateBackendService(ctx context.Context, project, name string, bs *compute.BackendService) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("UpdateBackendService"); err != nil {
		return err
	}
	cp := *bs
	if len(bs.Backends) > 0 {
		cp.Backends = make([]*compute.Backend, len(bs.Backends))
		copy(cp.Backends, bs.Backends)
	}
	f.BackendServices[name] = &cp
	return nil
}

func (f *FakeAdminClient) DeleteBackendService(ctx context.Context, project, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("DeleteBackendService"); err != nil {
		return err
	}
	delete(f.BackendServices, name)
	return nil
}

// Target TCP Proxies

func (f *FakeAdminClient) GetTargetTCPProxy(ctx context.Context, project, name string) (*compute.TargetTcpProxy, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("GetTargetTCPProxy"); err != nil {
		return nil, err
	}
	if proxy, ok := f.TargetTcpProxies[name]; ok {
		return proxy, nil
	}
	return nil, nil
}

func (f *FakeAdminClient) CreateTargetTCPProxy(ctx context.Context, project string, proxy *compute.TargetTcpProxy) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("CreateTargetTCPProxy"); err != nil {
		return err
	}
	cp := *proxy
	f.TargetTcpProxies[proxy.Name] = &cp
	return nil
}

func (f *FakeAdminClient) DeleteTargetTCPProxy(ctx context.Context, project, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("DeleteTargetTCPProxy"); err != nil {
		return err
	}
	delete(f.TargetTcpProxies, name)
	return nil
}

// Forwarding Rules

func (f *FakeAdminClient) GetForwardingRule(ctx context.Context, project, name string) (*compute.ForwardingRule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("GetForwardingRule"); err != nil {
		return nil, err
	}
	if fr, ok := f.ForwardingRules[name]; ok {
		return fr, nil
	}
	return nil, nil
}

func (f *FakeAdminClient) CreateForwardingRule(ctx context.Context, project string, fr *compute.ForwardingRule) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("CreateForwardingRule"); err != nil {
		return err
	}
	cp := *fr
	f.ForwardingRules[fr.Name] = &cp
	return nil
}

func (f *FakeAdminClient) DeleteForwardingRule(ctx context.Context, project, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("DeleteForwardingRule"); err != nil {
		return err
	}
	delete(f.ForwardingRules, name)
	return nil
}

// Zones

func (f *FakeAdminClient) ListZones(ctx context.Context, project, region string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.recordCall("ListZones"); err != nil {
		return nil, err
	}
	if zones, ok := f.RegionZones[region]; ok && len(zones) > 0 {
		sorted := make([]string, len(zones))
		copy(sorted, zones)
		sort.Strings(sorted)
		return sorted, nil
	}
	return []string{region + "-a", region + "-b", region + "-c"}, nil
}
