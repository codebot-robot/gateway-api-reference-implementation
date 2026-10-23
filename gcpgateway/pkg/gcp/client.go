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
	"errors"
	"fmt"
	"strings"

	compute "google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
)

var (
	// ErrNEGNotFound is returned when a NEG does not exist.
	ErrNEGNotFound = errors.New("NEGNotFound: network endpoint group not found")

	// ErrPermissionDenied is returned when GCP credentials lack required permissions.
	ErrPermissionDenied = errors.New("PERMISSION_DENIED: caller lacks required IAM permissions")

	// ErrQuotaExceeded is returned when Compute API quotas are exhausted.
	ErrQuotaExceeded = errors.New("QuotaExceeded: compute API quota exceeded")

	// ErrInvalidIP is returned when a pod IP is not valid in the GCP VPC subnet.
	ErrInvalidIP = errors.New("InvalidEndpointIP: IP address is not valid for network endpoint")
)

// ComputeClient defines the subset of Google Cloud Compute API calls needed by gcpgateway.
type ComputeClient interface {
	// ListNEGs lists all zonal NetworkEndpointGroups in the given zone matching prefix.
	ListNEGs(ctx context.Context, project, zone, prefix string) ([]*compute.NetworkEndpointGroup, error)

	// ListNetworkEndpoints lists all network endpoints currently registered in the specified NEG.
	ListNetworkEndpoints(ctx context.Context, project, zone, negName string) ([]*compute.NetworkEndpoint, error)

	// AttachNetworkEndpoints attaches network endpoints to a zonal NEG.
	AttachNetworkEndpoints(ctx context.Context, project, zone, negName string, endpoints []*compute.NetworkEndpoint) error

	// DetachNetworkEndpoints detaches network endpoints from a zonal NEG.
	DetachNetworkEndpoints(ctx context.Context, project, zone, negName string, endpoints []*compute.NetworkEndpoint) error

	// GetBackendServiceHealth retrieves health status for a backend service and NEG.
	GetBackendServiceHealth(ctx context.Context, project, backendService, groupURL string) (*compute.BackendServiceGroupHealth, error)
}

// GCEComputeClient implements ComputeClient backed by google.golang.org/api/compute/v1.
type GCEComputeClient struct {
	svc *compute.Service
}

// NewGCEComputeClient creates a new GCEComputeClient from an initialized compute.Service.
func NewGCEComputeClient(svc *compute.Service) *GCEComputeClient {
	return &GCEComputeClient{svc: svc}
}

func (c *GCEComputeClient) ListNEGs(ctx context.Context, project, zone, prefix string) ([]*compute.NetworkEndpointGroup, error) {
	call := c.svc.NetworkEndpointGroups.List(project, zone)
	if prefix != "" {
		call = call.Filter(fmt.Sprintf("name = %s*", prefix))
	}
	var negs []*compute.NetworkEndpointGroup
	err := call.Pages(ctx, func(page *compute.NetworkEndpointGroupList) error {
		for _, neg := range page.Items {
			if prefix == "" || strings.HasPrefix(neg.Name, prefix) {
				negs = append(negs, neg)
			}
		}
		return nil
	})
	if err != nil {
		return nil, wrapGCPError(err)
	}
	return negs, nil
}

func (c *GCEComputeClient) ListNetworkEndpoints(ctx context.Context, project, zone, negName string) ([]*compute.NetworkEndpoint, error) {
	call := c.svc.NetworkEndpointGroups.ListNetworkEndpoints(project, zone, negName, &compute.NetworkEndpointGroupsListEndpointsRequest{})
	var endpoints []*compute.NetworkEndpoint
	err := call.Pages(ctx, func(page *compute.NetworkEndpointGroupsListNetworkEndpoints) error {
		for _, item := range page.Items {
			if item.NetworkEndpoint != nil {
				endpoints = append(endpoints, item.NetworkEndpoint)
			}
		}
		return nil
	})
	if err != nil {
		return nil, wrapGCPError(err)
	}
	return endpoints, nil
}

func (c *GCEComputeClient) AttachNetworkEndpoints(ctx context.Context, project, zone, negName string, endpoints []*compute.NetworkEndpoint) error {
	if len(endpoints) == 0 {
		return nil
	}
	req := &compute.NetworkEndpointGroupsAttachEndpointsRequest{
		NetworkEndpoints: endpoints,
	}
	op, err := c.svc.NetworkEndpointGroups.AttachNetworkEndpoints(project, zone, negName, req).Context(ctx).Do()
	if err != nil {
		return wrapGCPError(err)
	}
	_ = op
	return nil
}

func (c *GCEComputeClient) DetachNetworkEndpoints(ctx context.Context, project, zone, negName string, endpoints []*compute.NetworkEndpoint) error {
	if len(endpoints) == 0 {
		return nil
	}
	req := &compute.NetworkEndpointGroupsDetachEndpointsRequest{
		NetworkEndpoints: endpoints,
	}
	op, err := c.svc.NetworkEndpointGroups.DetachNetworkEndpoints(project, zone, negName, req).Context(ctx).Do()
	if err != nil {
		return wrapGCPError(err)
	}
	_ = op
	return nil
}

func (c *GCEComputeClient) GetBackendServiceHealth(ctx context.Context, project, backendService, groupURL string) (*compute.BackendServiceGroupHealth, error) {
	req := &compute.ResourceGroupReference{
		Group: groupURL,
	}
	health, err := c.svc.BackendServices.GetHealth(project, backendService, req).Context(ctx).Do()
	if err != nil {
		return nil, wrapGCPError(err)
	}
	return health, nil
}

func wrapGCPError(err error) error {
	if err == nil {
		return nil
	}
	var gErr *googleapi.Error
	if errors.As(err, &gErr) {
		switch gErr.Code {
		case 404:
			return fmt.Errorf("%w: %s", ErrNEGNotFound, gErr.Message)
		case 403:
			for _, item := range gErr.Errors {
				if item.Reason == "rateLimitExceeded" || item.Reason == "quotaExceeded" {
					return fmt.Errorf("%w: %s", ErrQuotaExceeded, gErr.Message)
				}
			}
			return fmt.Errorf("%w: %s", ErrPermissionDenied, gErr.Message)
		case 429:
			return fmt.Errorf("%w: %s", ErrQuotaExceeded, gErr.Message)
		case 400:
			if strings.Contains(strings.ToLower(gErr.Message), "ip") || strings.Contains(strings.ToLower(gErr.Message), "subnet") {
				return fmt.Errorf("%w: %s", ErrInvalidIP, gErr.Message)
			}
		}
	}
	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "not found") || strings.Contains(errStr, "notfound") {
		return fmt.Errorf("%w: %v", ErrNEGNotFound, err)
	}
	if strings.Contains(errStr, "permission_denied") || strings.Contains(errStr, "permission denied") || strings.Contains(errStr, "access denied") {
		return fmt.Errorf("%w: %v", ErrPermissionDenied, err)
	}
	if strings.Contains(errStr, "quota") || strings.Contains(errStr, "rate limit") {
		return fmt.Errorf("%w: %v", ErrQuotaExceeded, err)
	}
	if strings.Contains(errStr, "invalid ip") || strings.Contains(errStr, "invalid endpoint ip") {
		return fmt.Errorf("%w: %v", ErrInvalidIP, err)
	}
	return err
}

// IsNEGNotFound checks if the error indicates a missing NEG.
func IsNEGNotFound(err error) bool {
	return errors.Is(err, ErrNEGNotFound)
}

// IsPermissionDenied checks if the error indicates an IAM permission denial.
func IsPermissionDenied(err error) bool {
	return errors.Is(err, ErrPermissionDenied)
}

// IsQuotaExceeded checks if the error indicates a quota exhaustion.
func IsQuotaExceeded(err error) bool {
	return errors.Is(err, ErrQuotaExceeded)
}

// IsInvalidIP checks if the error indicates an invalid pod IP.
func IsInvalidIP(err error) bool {
	return errors.Is(err, ErrInvalidIP)
}
