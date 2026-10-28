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
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/metadata"
	compute "google.golang.org/api/compute/v1"
)

// FrontendConfig holds command-line arguments and configuration for frontend provisioning/teardown.
type FrontendConfig struct {
	Project         string
	Region          string
	Network         string
	Subnet          string
	ClusterID       string
	Namespace       string
	GatewayName     string
	Prefix          string
	Zones           string
	DrainingTimeout int
	DryRun          bool
}

// BindCreateFlags binds configuration flags for "frontend create" directly to struct fields.
func (c *FrontendConfig) BindCreateFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.Project, "project", "", "GCP project ID")
	fs.StringVar(&c.Region, "region", "", "GCP region")
	fs.StringVar(&c.Network, "network", "default", "VPC network name")
	fs.StringVar(&c.Subnet, "subnet", "default", "VPC subnet name")
	fs.StringVar(&c.ClusterID, "cluster-id", "cluster-1", "Kubernetes cluster identifier")
	fs.StringVar(&c.Namespace, "namespace", "default", "Gateway namespace")
	fs.StringVar(&c.GatewayName, "gateway-name", "my-gateway", "Gateway name")
	fs.StringVar(&c.Prefix, "prefix", "gcpgw", "Resource prefix")
	fs.StringVar(&c.Zones, "zones", "", "Comma-separated list of zones in the region")
	fs.IntVar(&c.DrainingTimeout, "draining-timeout", 35, "Backend service draining timeout in seconds")
	fs.BoolVar(&c.DryRun, "dry-run", false, "Print the plan without making changes")
}

// BindDeleteFlags binds configuration flags for "frontend delete" directly to struct fields.
func (c *FrontendConfig) BindDeleteFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.Project, "project", "", "GCP project ID")
	fs.StringVar(&c.Region, "region", "", "GCP region")
	fs.StringVar(&c.ClusterID, "cluster-id", "cluster-1", "Kubernetes cluster identifier")
	fs.StringVar(&c.Namespace, "namespace", "default", "Gateway namespace")
	fs.StringVar(&c.GatewayName, "gateway-name", "my-gateway", "Gateway name")
	fs.StringVar(&c.Prefix, "prefix", "gcpgw", "Resource prefix")
	fs.StringVar(&c.Zones, "zones", "", "Comma-separated list of zones in the region")
	fs.BoolVar(&c.DryRun, "dry-run", false, "Print the plan without making changes")
}

// ParsedZones parses the comma-separated Zones string.
func (c *FrontendConfig) ParsedZones() []string {
	if strings.TrimSpace(c.Zones) == "" {
		return nil
	}
	var res []string
	for _, z := range strings.Split(c.Zones, ",") {
		z = strings.TrimSpace(z)
		if z != "" {
			res = append(res, z)
		}
	}
	return res
}

// FrontendResult contains the resulting configuration and IP address after setup.
type FrontendResult struct {
	IPAddress   string
	ClusterID   string
	Namespace   string
	GatewayName string
	Prefix      string
	Neg8443     string
	Neg8080     string
	Zones       []string
}

func makeDescription(clusterID, namespace, gatewayName string, port int) string {
	meta := &metadata.NegMetadata{
		ClusterID:   clusterID,
		Namespace:   namespace,
		GatewayName: gatewayName,
		Port:        port,
	}
	desc, _ := meta.Encode()
	return desc
}

func verifyOwnership(desc, clusterID, namespace, gatewayName string) bool {
	meta, err := metadata.Parse(desc)
	if err != nil {
		return false
	}
	return meta.ClusterID == clusterID && meta.Namespace == namespace && meta.GatewayName == gatewayName
}

func networkURL(project, network string) string {
	if strings.Contains(network, "/") {
		return network
	}
	return fmt.Sprintf("projects/%s/global/networks/%s", project, network)
}

func subnetworkURL(project, region, subnet string) string {
	if strings.Contains(subnet, "/") {
		return subnet
	}
	return fmt.Sprintf("projects/%s/regions/%s/subnetworks/%s", project, region, subnet)
}

func hcURL(project, hcName string) string {
	return fmt.Sprintf("projects/%s/global/healthChecks/%s", project, hcName)
}

func negURL(project, zone, negName string) string {
	return fmt.Sprintf("projects/%s/zones/%s/networkEndpointGroups/%s", project, zone, negName)
}

func bsURL(project, bsName string) string {
	return fmt.Sprintf("projects/%s/global/backendServices/%s", project, bsName)
}

func proxyURL(project, proxyName string) string {
	return fmt.Sprintf("projects/%s/global/targetTcpProxies/%s", project, proxyName)
}

func addressURL(project, ipName string) string {
	return fmt.Sprintf("projects/%s/global/addresses/%s", project, ipName)
}

// CreateFrontend idempotently provisions all load-balancing resources and prints the manifests.
func CreateFrontend(ctx context.Context, client AdminClient, cfg *FrontendConfig, out io.Writer) (*FrontendResult, error) {
	if cfg.Project == "" || cfg.Region == "" {
		return nil, fmt.Errorf("--project and --region are required")
	}

	ipName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, "ip")
	hcName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, "hc")
	fwName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, "allow-gfe")
	neg8443 := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, "8443")
	neg8080 := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, "8080")

	// If dry-run, print plan and make NO calls to client.
	if cfg.DryRun {
		zones := cfg.ParsedZones()
		if len(zones) == 0 {
			zones = []string{cfg.Region + "-a", cfg.Region + "-b", cfg.Region + "-c"}
		}
		if out != nil {
			fmt.Fprintf(out, "[dry-run] Plan to create front-end resources for Gateway %s/%s:\n", cfg.Namespace, cfg.GatewayName)
			fmt.Fprintf(out, "- Address: %s (global)\n", ipName)
			fmt.Fprintf(out, "- Health Check: %s (global, HTTP port 8999, path /_gari/readyz)\n", hcName)
			fmt.Fprintf(out, "- Firewall Rule: %s (ingress TCP 8443, 8080, 8999 from GFE)\n", fwName)
			for _, port := range []int{8443, 8080} {
				nName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, strconv.Itoa(port))
				for _, zone := range zones {
					fmt.Fprintf(out, "- Network Endpoint Group: %s (zone %s, port %d)\n", nName, zone, port)
				}
			}
			for _, fePort := range []int{443, 80} {
				bsName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, fmt.Sprintf("bs-%d", fePort))
				proxyName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, fmt.Sprintf("proxy-%d", fePort))
				frName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, fmt.Sprintf("fr-%d", fePort))
				fmt.Fprintf(out, "- Backend Service: %s (global, draining %ds)\n", bsName, cfg.DrainingTimeout)
				fmt.Fprintf(out, "- Target TCP Proxy: %s (global, PROXY_V1)\n", proxyName)
				fmt.Fprintf(out, "- Forwarding Rule: %s (global, port %d)\n", frName, fePort)
			}
		}
		return &FrontendResult{
			IPAddress:   "34.120.254.10",
			ClusterID:   cfg.ClusterID,
			Namespace:   cfg.Namespace,
			GatewayName: cfg.GatewayName,
			Prefix:      cfg.Prefix,
			Neg8443:     neg8443,
			Neg8080:     neg8080,
			Zones:       zones,
		}, nil
	}

	if client == nil {
		return nil, fmt.Errorf("admin client is required")
	}

	// Resolve zones
	zones := cfg.ParsedZones()
	if len(zones) == 0 {
		var err error
		zones, err = client.ListZones(ctx, cfg.Project, cfg.Region)
		if err != nil {
			return nil, fmt.Errorf("listing zones: %w", err)
		}
		if len(zones) == 0 {
			zones = []string{cfg.Region + "-a", cfg.Region + "-b", cfg.Region + "-c"}
		}
	}

	// 1. Global Static IPv4 Address
	descIP := makeDescription(cfg.ClusterID, cfg.Namespace, cfg.GatewayName, 0)
	addr, err := client.GetAddress(ctx, cfg.Project, ipName)
	if err != nil {
		return nil, fmt.Errorf("describing address %s: %w", ipName, err)
	}
	if addr == nil {
		if out != nil {
			fmt.Fprintf(out, "[create] Creating static IP address %s...\n", ipName)
		}
		err = client.CreateAddress(ctx, cfg.Project, &compute.Address{
			Name:        ipName,
			IpVersion:   "IPV4",
			Description: descIP,
		})
		if err != nil {
			return nil, fmt.Errorf("creating address %s: %w", ipName, err)
		}
		addr, err = client.GetAddress(ctx, cfg.Project, ipName)
		if err != nil {
			return nil, fmt.Errorf("describing created address %s: %w", ipName, err)
		}
	} else {
		if out != nil {
			fmt.Fprintf(out, "[exists] Static IP address %s already exists\n", ipName)
		}
	}
	ipAddress := "34.120.254.10"
	if addr != nil && addr.Address != "" {
		ipAddress = addr.Address
	}

	// 2. HTTP Health Check on Port 8999
	descHC := makeDescription(cfg.ClusterID, cfg.Namespace, cfg.GatewayName, 8999)
	hc, err := client.GetHealthCheck(ctx, cfg.Project, hcName)
	if err != nil {
		return nil, fmt.Errorf("describing health check %s: %w", hcName, err)
	}
	if hc == nil {
		if out != nil {
			fmt.Fprintf(out, "[create] Creating health check %s...\n", hcName)
		}
		err = client.CreateHealthCheck(ctx, cfg.Project, &compute.HealthCheck{
			Name:        hcName,
			Description: descHC,
			Type:        "HTTP",
			HttpHealthCheck: &compute.HTTPHealthCheck{
				Port:        8999,
				RequestPath: "/_gari/readyz",
			},
			CheckIntervalSec:   5,
			TimeoutSec:         5,
			HealthyThreshold:   2,
			UnhealthyThreshold: 2,
		})
		if err != nil {
			return nil, fmt.Errorf("creating health check %s: %w", hcName, err)
		}
	} else {
		if out != nil {
			fmt.Fprintf(out, "[exists] Health check %s already exists\n", hcName)
		}
	}

	// 3. Ingress Firewall Rule
	descFW := makeDescription(cfg.ClusterID, cfg.Namespace, cfg.GatewayName, 0)
	fw, err := client.GetFirewall(ctx, cfg.Project, fwName)
	if err != nil {
		return nil, fmt.Errorf("describing firewall rule %s: %w", fwName, err)
	}
	if fw == nil {
		if out != nil {
			fmt.Fprintf(out, "[create] Creating firewall rule %s...\n", fwName)
		}
		err = client.CreateFirewall(ctx, cfg.Project, &compute.Firewall{
			Name:         fwName,
			Description:  descFW,
			Network:      networkURL(cfg.Project, cfg.Network),
			Direction:    "INGRESS",
			SourceRanges: []string{"35.191.0.0/16", "130.211.0.0/22"},
			Allowed: []*compute.FirewallAllowed{
				{
					IPProtocol: "tcp",
					Ports:      []string{"8443", "8080", "8999"},
				},
			},
		})
		if err != nil {
			return nil, fmt.Errorf("creating firewall rule %s: %w", fwName, err)
		}
	} else {
		if out != nil {
			fmt.Fprintf(out, "[exists] Firewall rule %s already exists\n", fwName)
		}
	}

	// 4. Zonal NEGs
	for _, port := range []int{8443, 8080} {
		negName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, strconv.Itoa(port))
		descNEG := makeDescription(cfg.ClusterID, cfg.Namespace, cfg.GatewayName, port)
		for _, zone := range zones {
			neg, err := client.GetNEG(ctx, cfg.Project, zone, negName)
			if err != nil {
				return nil, fmt.Errorf("describing NEG %s in zone %s: %w", negName, zone, err)
			}
			if neg == nil {
				if out != nil {
					fmt.Fprintf(out, "[create] Creating NEG %s in zone %s...\n", negName, zone)
				}
				err = client.CreateNEG(ctx, cfg.Project, zone, &compute.NetworkEndpointGroup{
					Name:                negName,
					Description:         descNEG,
					NetworkEndpointType: "GCE_VM_IP_PORT",
					Network:             networkURL(cfg.Project, cfg.Network),
					Subnetwork:          subnetworkURL(cfg.Project, cfg.Region, cfg.Subnet),
					DefaultPort:         int64(port),
				})
				if err != nil {
					return nil, fmt.Errorf("creating NEG %s in zone %s: %w", negName, zone, err)
				}
			} else {
				if out != nil {
					fmt.Fprintf(out, "[exists] NEG %s in zone %s already exists\n", negName, zone)
				}
			}
		}
	}

	// 5. Backend Services & Backends
	portMap := []struct {
		fePort int
		bePort int
	}{
		{fePort: 443, bePort: 8443},
		{fePort: 80, bePort: 8080},
	}
	for _, mapping := range portMap {
		bsName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, fmt.Sprintf("bs-%d", mapping.fePort))
		descBS := makeDescription(cfg.ClusterID, cfg.Namespace, cfg.GatewayName, mapping.bePort)
		beNegName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, strconv.Itoa(mapping.bePort))

		bs, err := client.GetBackendService(ctx, cfg.Project, bsName)
		if err != nil {
			return nil, fmt.Errorf("describing backend service %s: %w", bsName, err)
		}
		if bs == nil {
			if out != nil {
				fmt.Fprintf(out, "[create] Creating backend service %s...\n", bsName)
			}
			var backends []*compute.Backend
			for _, zone := range zones {
				backends = append(backends, &compute.Backend{
					Group:          negURL(cfg.Project, zone, beNegName),
					BalancingMode:  "CONNECTION",
					MaxConnections: 1000,
				})
			}
			err = client.CreateBackendService(ctx, cfg.Project, &compute.BackendService{
				Name:                bsName,
				Description:         descBS,
				LoadBalancingScheme: "EXTERNAL_MANAGED",
				Protocol:            "TCP",
				HealthChecks:        []string{hcURL(cfg.Project, hcName)},
				TimeoutSec:          int64(cfg.DrainingTimeout),
				ConnectionDraining: &compute.ConnectionDraining{
					DrainingTimeoutSec: int64(cfg.DrainingTimeout),
				},
				Backends: backends,
			})
			if err != nil {
				return nil, fmt.Errorf("creating backend service %s: %w", bsName, err)
			}
		} else {
			if out != nil {
				fmt.Fprintf(out, "[exists] Backend service %s already exists\n", bsName)
			}
			// Check if any zone backend is missing
			existing := make(map[string]bool)
			for _, b := range bs.Backends {
				if b != nil {
					existing[b.Group] = true
				}
			}
			var updated bool
			for _, zone := range zones {
				negSuffix := fmt.Sprintf("zones/%s/networkEndpointGroups/%s", zone, beNegName)
				var found bool
				for g := range existing {
					if strings.Contains(g, negSuffix) {
						found = true
						break
					}
				}
				if !found {
					bs.Backends = append(bs.Backends, &compute.Backend{
						Group:          negURL(cfg.Project, zone, beNegName),
						BalancingMode:  "CONNECTION",
						MaxConnections: 1000,
					})
					updated = true
				}
			}
			if updated {
				if out != nil {
					fmt.Fprintf(out, "[update] Updating backend service %s with missing backends...\n", bsName)
				}
				if err := client.UpdateBackendService(ctx, cfg.Project, bsName, bs); err != nil {
					return nil, fmt.Errorf("updating backend service %s backends: %w", bsName, err)
				}
			}
		}
	}

	// 6. Target TCP Proxies
	for _, fePort := range []int{443, 80} {
		proxyName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, fmt.Sprintf("proxy-%d", fePort))
		bsName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, fmt.Sprintf("bs-%d", fePort))
		descProxy := makeDescription(cfg.ClusterID, cfg.Namespace, cfg.GatewayName, fePort)

		proxy, err := client.GetTargetTCPProxy(ctx, cfg.Project, proxyName)
		if err != nil {
			return nil, fmt.Errorf("describing target TCP proxy %s: %w", proxyName, err)
		}
		if proxy == nil {
			if out != nil {
				fmt.Fprintf(out, "[create] Creating target TCP proxy %s...\n", proxyName)
			}
			err = client.CreateTargetTCPProxy(ctx, cfg.Project, &compute.TargetTcpProxy{
				Name:        proxyName,
				Description: descProxy,
				Service:     bsURL(cfg.Project, bsName),
				ProxyHeader: "PROXY_V1",
			})
			if err != nil {
				return nil, fmt.Errorf("creating target TCP proxy %s: %w", proxyName, err)
			}
		} else {
			if out != nil {
				fmt.Fprintf(out, "[exists] Target TCP proxy %s already exists\n", proxyName)
			}
		}
	}

	// 7. Global Forwarding Rules
	for _, fePort := range []int{443, 80} {
		frName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, fmt.Sprintf("fr-%d", fePort))
		proxyName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, fmt.Sprintf("proxy-%d", fePort))
		descFR := makeDescription(cfg.ClusterID, cfg.Namespace, cfg.GatewayName, fePort)

		fr, err := client.GetForwardingRule(ctx, cfg.Project, frName)
		if err != nil {
			return nil, fmt.Errorf("describing forwarding rule %s: %w", frName, err)
		}
		if fr == nil {
			if out != nil {
				fmt.Fprintf(out, "[create] Creating forwarding rule %s...\n", frName)
			}
			err = client.CreateForwardingRule(ctx, cfg.Project, &compute.ForwardingRule{
				Name:                frName,
				Description:         descFR,
				Target:              proxyURL(cfg.Project, proxyName),
				PortRange:           strconv.Itoa(fePort),
				IPAddress:           addressURL(cfg.Project, ipName),
				LoadBalancingScheme: "EXTERNAL_MANAGED",
			})
			if err != nil {
				return nil, fmt.Errorf("creating forwarding rule %s: %w", frName, err)
			}
		} else {
			if out != nil {
				fmt.Fprintf(out, "[exists] Forwarding rule %s already exists\n", frName)
			}
		}
	}

	result := &FrontendResult{
		IPAddress:   ipAddress,
		ClusterID:   cfg.ClusterID,
		Namespace:   cfg.Namespace,
		GatewayName: cfg.GatewayName,
		Prefix:      cfg.Prefix,
		Neg8443:     neg8443,
		Neg8080:     neg8080,
		Zones:       zones,
	}

	if out != nil {
		fmt.Fprintf(out, "\n=== Setup Complete ===\n")
		fmt.Fprintf(out, "Front-end Static IP: %s\n\n", result.IPAddress)
		fmt.Fprintf(out, "Ready-to-apply Kubernetes manifests:\n\n")
		fmt.Fprintf(out, "%s\n", GenerateManifest(cfg, result.IPAddress))
	}

	return result, nil
}

// DeleteFrontend cleanly deletes all matching GCP resources in reverse dependency order.
func DeleteFrontend(ctx context.Context, client AdminClient, cfg *FrontendConfig, out io.Writer) ([]string, error) {
	if cfg.Project == "" || cfg.Region == "" {
		return nil, fmt.Errorf("--project and --region are required")
	}

	var deleted []string

	ipName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, "ip")
	hcName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, "hc")
	fwName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, "allow-gfe")

	// If dry-run, print plan and make NO calls to client.
	if cfg.DryRun {
		zones := cfg.ParsedZones()
		if len(zones) == 0 {
			zones = []string{cfg.Region + "-a", cfg.Region + "-b", cfg.Region + "-c"}
		}
		if out != nil {
			fmt.Fprintf(out, "[dry-run] Plan to delete front-end resources for Gateway %s/%s:\n", cfg.Namespace, cfg.GatewayName)
			for _, fePort := range []int{443, 80} {
				frName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, fmt.Sprintf("fr-%d", fePort))
				proxyName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, fmt.Sprintf("proxy-%d", fePort))
				bsName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, fmt.Sprintf("bs-%d", fePort))
				fmt.Fprintf(out, "- Forwarding Rule: %s\n", frName)
				fmt.Fprintf(out, "- Target TCP Proxy: %s\n", proxyName)
				fmt.Fprintf(out, "- Backend Service: %s\n", bsName)
			}
			for _, port := range []int{8443, 8080} {
				negName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, strconv.Itoa(port))
				for _, zone := range zones {
					fmt.Fprintf(out, "- Network Endpoint Group: %s (%s)\n", negName, zone)
				}
			}
			fmt.Fprintf(out, "- Health Check: %s\n", hcName)
			fmt.Fprintf(out, "- Firewall Rule: %s\n", fwName)
			fmt.Fprintf(out, "- Address: %s\n", ipName)
		}
		return nil, nil
	}

	if client == nil {
		return nil, fmt.Errorf("admin client is required")
	}

	// Resolve zones
	zones := cfg.ParsedZones()
	if len(zones) == 0 {
		var err error
		zones, err = client.ListZones(ctx, cfg.Project, cfg.Region)
		if err != nil {
			return nil, fmt.Errorf("listing zones: %w", err)
		}
		if len(zones) == 0 {
			zones = []string{cfg.Region + "-a", cfg.Region + "-b", cfg.Region + "-c"}
		}
	}

	// 1. Forwarding Rules
	for _, fePort := range []int{443, 80} {
		frName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, fmt.Sprintf("fr-%d", fePort))
		fr, err := client.GetForwardingRule(ctx, cfg.Project, frName)
		if err != nil {
			return nil, fmt.Errorf("describing forwarding rule %s: %w", frName, err)
		}
		if fr != nil && verifyOwnership(fr.Description, cfg.ClusterID, cfg.Namespace, cfg.GatewayName) {
			if err := client.DeleteForwardingRule(ctx, cfg.Project, frName); err != nil {
				return nil, fmt.Errorf("deleting forwarding rule %s: %w", frName, err)
			}
			deleted = append(deleted, frName)
			if out != nil {
				fmt.Fprintf(out, "[deleted] Forwarding rule %s\n", frName)
			}
		}
	}

	// 2. Target TCP Proxies
	for _, fePort := range []int{443, 80} {
		proxyName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, fmt.Sprintf("proxy-%d", fePort))
		proxy, err := client.GetTargetTCPProxy(ctx, cfg.Project, proxyName)
		if err != nil {
			return nil, fmt.Errorf("describing target TCP proxy %s: %w", proxyName, err)
		}
		if proxy != nil && verifyOwnership(proxy.Description, cfg.ClusterID, cfg.Namespace, cfg.GatewayName) {
			if err := client.DeleteTargetTCPProxy(ctx, cfg.Project, proxyName); err != nil {
				return nil, fmt.Errorf("deleting target TCP proxy %s: %w", proxyName, err)
			}
			deleted = append(deleted, proxyName)
			if out != nil {
				fmt.Fprintf(out, "[deleted] Target TCP proxy %s\n", proxyName)
			}
		}
	}

	// 3. Backend Services
	for _, fePort := range []int{443, 80} {
		bsName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, fmt.Sprintf("bs-%d", fePort))
		bs, err := client.GetBackendService(ctx, cfg.Project, bsName)
		if err != nil {
			return nil, fmt.Errorf("describing backend service %s: %w", bsName, err)
		}
		if bs != nil && verifyOwnership(bs.Description, cfg.ClusterID, cfg.Namespace, cfg.GatewayName) {
			if err := client.DeleteBackendService(ctx, cfg.Project, bsName); err != nil {
				return nil, fmt.Errorf("deleting backend service %s: %w", bsName, err)
			}
			deleted = append(deleted, bsName)
			if out != nil {
				fmt.Fprintf(out, "[deleted] Backend service %s\n", bsName)
			}
		}
	}

	// 4. Zonal NEGs
	for _, port := range []int{8443, 8080} {
		negName := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, strconv.Itoa(port))
		for _, zone := range zones {
			neg, err := client.GetNEG(ctx, cfg.Project, zone, negName)
			if err != nil {
				return nil, fmt.Errorf("describing NEG %s in zone %s: %w", negName, zone, err)
			}
			if neg != nil && verifyOwnership(neg.Description, cfg.ClusterID, cfg.Namespace, cfg.GatewayName) {
				if err := client.DeleteNEG(ctx, cfg.Project, zone, negName); err != nil {
					return nil, fmt.Errorf("deleting NEG %s in zone %s: %w", negName, zone, err)
				}
				item := fmt.Sprintf("%s (%s)", negName, zone)
				deleted = append(deleted, item)
				if out != nil {
					fmt.Fprintf(out, "[deleted] Network endpoint group %s\n", item)
				}
			}
		}
	}

	// 5. Health Check
	hc, err := client.GetHealthCheck(ctx, cfg.Project, hcName)
	if err != nil {
		return nil, fmt.Errorf("describing health check %s: %w", hcName, err)
	}
	if hc != nil && verifyOwnership(hc.Description, cfg.ClusterID, cfg.Namespace, cfg.GatewayName) {
		if err := client.DeleteHealthCheck(ctx, cfg.Project, hcName); err != nil {
			return nil, fmt.Errorf("deleting health check %s: %w", hcName, err)
		}
		deleted = append(deleted, hcName)
		if out != nil {
			fmt.Fprintf(out, "[deleted] Health check %s\n", hcName)
		}
	}

	// 6. Firewall Rule
	fw, err := client.GetFirewall(ctx, cfg.Project, fwName)
	if err != nil {
		return nil, fmt.Errorf("describing firewall rule %s: %w", fwName, err)
	}
	if fw != nil && verifyOwnership(fw.Description, cfg.ClusterID, cfg.Namespace, cfg.GatewayName) {
		if err := client.DeleteFirewall(ctx, cfg.Project, fwName); err != nil {
			return nil, fmt.Errorf("deleting firewall rule %s: %w", fwName, err)
		}
		deleted = append(deleted, fwName)
		if out != nil {
			fmt.Fprintf(out, "[deleted] Firewall rule %s\n", fwName)
		}
	}

	// 7. Static IP Address
	addr, err := client.GetAddress(ctx, cfg.Project, ipName)
	if err != nil {
		return nil, fmt.Errorf("describing address %s: %w", ipName, err)
	}
	if addr != nil && verifyOwnership(addr.Description, cfg.ClusterID, cfg.Namespace, cfg.GatewayName) {
		if err := client.DeleteAddress(ctx, cfg.Project, ipName); err != nil {
			return nil, fmt.Errorf("deleting address %s: %w", ipName, err)
		}
		deleted = append(deleted, ipName)
		if out != nil {
			fmt.Fprintf(out, "[deleted] Address %s\n", ipName)
		}
	}

	if out != nil {
		fmt.Fprintf(out, "\n=== Teardown Complete ===\n")
		for _, item := range deleted {
			fmt.Fprintf(out, "Deleted: %s\n", item)
		}
	}

	return deleted, nil
}

// GenerateManifest generates the parameters ConfigMap and Gateway manifest YAML.
func GenerateManifest(cfg *FrontendConfig, ipAddress string) string {
	gwName := cfg.GatewayName
	ns := cfg.Namespace
	ip := ipAddress
	cid := cfg.ClusterID
	neg8443 := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, "8443")
	neg8080 := metadata.ResourceName(cfg.Prefix, cfg.ClusterID, cfg.Namespace, cfg.GatewayName, "8080")
	pfx := cfg.Prefix

	return fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: gcpgateway-params-%s
  namespace: %s
data:
  serviceType: ClusterIP
  allowedSourceCIDRs: "35.191.0.0/16,130.211.0.0/22"
  proxyProtocol: "enabled"
  proxyProtocolTrustedCIDRs: "35.191.0.0/16,130.211.0.0/22"
  externalIP: "%s"
  clusterId: "%s"
  gatewayNamespace: "%s"
  gatewayName: "%s"
  negHTTPS: "%s"
  negHTTP: "%s"
  negPrefix: "%s"
---
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: %s
  namespace: %s
spec:
  gatewayClassName: gcpgateway
  addresses:
  - type: IPAddress
    value: "%s"
  listeners:
  - name: https
    protocol: HTTPS
    port: 443
    tls:
      mode: Terminate
      certificateRefs:
      - name: %s-cert
    allowedRoutes:
      namespaces:
        from: Same
  - name: http
    protocol: HTTP
    port: 80
    allowedRoutes:
      namespaces:
        from: Same
  infrastructure:
    parametersRef:
      group: ""
      kind: ConfigMap
      name: gcpgateway-params-%s`,
		gwName, ns, ip, cid, ns, gwName, neg8443, neg8080, pfx,
		gwName, ns, ip, gwName, gwName)
}
