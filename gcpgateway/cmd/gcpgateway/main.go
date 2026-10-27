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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/controller"
	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/gcp"
	"github.com/gke-labs/gateway-api-reference-implementation/gcpgateway/pkg/registrar"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/gari"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/provisioning/singlepod"
	compute "google.golang.org/api/compute/v1"
	corev1 "k8s.io/api/core/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	// DefaultDrainSeconds is the default sleep duration for connection draining.
	DefaultDrainSeconds = 35

	// DefaultSentinelPath is the default path to write the drain sentinel.
	DefaultSentinelPath = "/var/run/gcpgateway/draining"

	// DefaultNegPrefix is the default resource prefix.
	DefaultNegPrefix = "gcpgw"

	// DefaultControllerName is the default GatewayClass controller name for gcpgateway.
	DefaultControllerName = "gke-labs.io/gcpgateway"

	// LoadBalancerReadyGate is the readiness gate added to data plane pods.
	LoadBalancerReadyGate = "gcpgateway.gke-labs.io/load-balancer-ready"
)

// RunPreStopHook executes the pre-stop lifecycle hook: writes sentinel file and sleeps.
func RunPreStopHook(args []string) error {
	fs := flag.NewFlagSet("pre-stop", flag.ContinueOnError)
	drainSeconds := fs.Int("drain-seconds", DefaultDrainSeconds, "Seconds to wait while draining connections")
	sentinelFile := fs.String("sentinel-file", DefaultSentinelPath, "Path to the draining sentinel file")

	if err := fs.Parse(args); err != nil {
		return err
	}

	dir := filepath.Dir(*sentinelFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("creating sentinel directory %q: %w", dir, err)
	}

	if err := os.WriteFile(*sentinelFile, []byte("draining\n"), 0644); err != nil {
		return fmt.Errorf("writing drain sentinel %q: %w", *sentinelFile, err)
	}

	klog.Infof("Pre-stop hook: wrote sentinel to %q, sleeping for %d seconds...", *sentinelFile, *drainSeconds)
	time.Sleep(time.Duration(*drainSeconds) * time.Second)
	klog.Info("Pre-stop hook: drain period finished, exiting cleanly.")
	return nil
}

// Run parses arguments and executes the requested subcommand or mode.
func Run(ctx context.Context, args []string) error {
	if len(args) >= 2 && args[0] == "hook" && args[1] == "pre-stop" {
		return RunPreStopHook(args[2:])
	}

	for _, arg := range args {
		if arg == "-dataplane-mode" || arg == "--dataplane-mode" || strings.HasPrefix(arg, "-dataplane-mode=") || strings.HasPrefix(arg, "--dataplane-mode=") {
			return runDataplane(ctx, args)
		}
	}

	return runController(ctx, args)
}

func runDataplane(ctx context.Context, args []string) error {
	opts := gari.DefaultOptions()
	opts.DataplaneMode = true

	var (
		dataplaneMode                bool
		gatewayNamespace             string
		gatewayName                  string
		allowedSourceCIDRsStr        string
		proxyProtocolTrustedCIDRsStr string
	)

	fs := flag.NewFlagSet("dataplane", flag.ContinueOnError)
	fs.BoolVar(&dataplaneMode, "dataplane-mode", true, "Run in data-plane mode")
	fs.StringVar(&gatewayNamespace, "gateway-namespace", "", "Gateway namespace")
	fs.StringVar(&gatewayName, "gateway-name", "", "Gateway name")
	fs.StringVar(&allowedSourceCIDRsStr, "allowed-source-cidrs", "", "Allowed source CIDRs")
	fs.StringVar(&opts.ProxyProtocol, "proxy-protocol", opts.ProxyProtocol, "Enable PROXY protocol")
	fs.StringVar(&proxyProtocolTrustedCIDRsStr, "proxy-protocol-trusted-cidrs", "", "Trusted PROXY protocol CIDRs")
	fs.StringVar(&opts.DrainSentinelPath, "drain-sentinel-path", DefaultSentinelPath, "Path to drain sentinel file")
	fs.StringVar(&opts.MetricsAddr, "metrics-bind-address", opts.MetricsAddr, "Metrics bind address")
	fs.StringVar(&opts.HealthProbeBindAddress, "health-probe-bind-address", opts.HealthProbeBindAddress, "Health probe bind address")
	fs.StringVar(&opts.ProxyAddr, "proxy-bind-address", opts.ProxyAddr, "HTTP proxy bind address")
	fs.StringVar(&opts.ProxyHTTPSAddr, "proxy-https-bind-address", opts.ProxyHTTPSAddr, "HTTPS proxy bind address")
	fs.BoolVar(&opts.DebugErrorResponses, "debug-error-responses", opts.DebugErrorResponses, "Include detailed error messages in unmatched responses")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if gatewayNamespace == "" || gatewayName == "" {
		return fmt.Errorf("--gateway-namespace and --gateway-name are required in --dataplane-mode")
	}

	if allowedSourceCIDRsStr != "" {
		for _, part := range strings.Split(allowedSourceCIDRsStr, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				opts.AllowedSourceCIDRs = append(opts.AllowedSourceCIDRs, trimmed)
			}
		}
	}
	if proxyProtocolTrustedCIDRsStr != "" {
		for _, part := range strings.Split(proxyProtocolTrustedCIDRsStr, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				opts.ProxyProtocolTrustedCIDRs = append(opts.ProxyProtocolTrustedCIDRs, trimmed)
			}
		}
	}

	opts.DataplaneGatewayNamespace = gatewayNamespace
	opts.DataplaneGatewayName = gatewayName
	opts.DisableStatusUpdates = true
	opts.LeaderElection = false
	opts.ProxyHTTP3Addr = opts.ProxyHTTPSAddr
	opts.AddressProvider = nil

	return gari.Run(ctx, opts)
}

// SetupController parses args and builds the gari.Options for controller mode.
// If computeClient is nil:
//   - if --fake-compute is set, gcp.NewFakeComputeClient() is used.
//   - otherwise compute.NewService(ctx) is called, returning an error on failure.
func SetupController(ctx context.Context, args []string, computeClient gcp.ComputeClient) (gari.Options, error) {
	var (
		clusterID      string
		negPrefix      string
		project        string
		region         string
		zonesStr       string
		controllerName string
		dataplaneImg   string
		drainSeconds        int
		leaderElect         bool
		fakeCompute         bool
		debugErrorResponses bool
	)

	defaultClusterID := os.Getenv("CLUSTER_ID")
	if defaultClusterID == "" {
		defaultClusterID = "cluster-1"
	}
	defaultNegPrefix := os.Getenv("NEG_PREFIX")
	if defaultNegPrefix == "" {
		defaultNegPrefix = DefaultNegPrefix
	}
	defaultProject := os.Getenv("PROJECT_ID")
	defaultRegion := os.Getenv("REGION")
	defaultImg := os.Getenv("GARI_IMAGE")
	if defaultImg == "" {
		defaultImg = singlepod.DefaultDataplaneImage
	}

	fs := flag.NewFlagSet("controller", flag.ContinueOnError)
	fs.StringVar(&clusterID, "cluster-id", defaultClusterID, "Unique Kubernetes cluster identifier")
	fs.StringVar(&negPrefix, "neg-prefix", defaultNegPrefix, "Prefix for zonal NEG names")
	fs.StringVar(&project, "project", defaultProject, "GCP Project ID")
	fs.StringVar(&region, "region", defaultRegion, "GCP Region")
	fs.StringVar(&zonesStr, "zones", "", "Comma-separated list of GCP zones")
	fs.StringVar(&controllerName, "controller-name", DefaultControllerName, "GatewayClass controller name")
	fs.StringVar(&dataplaneImg, "dataplane-image", defaultImg, "Data-plane container image")
	fs.IntVar(&drainSeconds, "drain-seconds", DefaultDrainSeconds, "Seconds to drain connections during rolling updates")
	fs.BoolVar(&leaderElect, "leader-elect", false, "Enable leader election")
	fs.BoolVar(&fakeCompute, "fake-compute", false, "Use in-memory fake Compute client for testing/offline mode")
	fs.BoolVar(&debugErrorResponses, "debug-error-responses", false, "Include detailed error messages in unmatched responses")

	if err := fs.Parse(args); err != nil {
		return gari.Options{}, err
	}

	gcpClient := computeClient
	if gcpClient == nil {
		if fakeCompute {
			gcpClient = gcp.NewFakeComputeClient()
		} else {
			computeSvc, err := compute.NewService(ctx)
			if err != nil {
				return gari.Options{}, fmt.Errorf("initializing Google Cloud Compute API client: %w", err)
			}
			gcpClient = gcp.NewGCEComputeClient(computeSvc)
		}
	}

	opts := gari.DefaultOptions()
	opts.ControllerName = controllerName
	opts.LeaderElection = leaderElect
	opts.ProxyAddr = ""
	opts.ProxyHTTPSAddr = ""
	opts.ProxyHTTP3Addr = ""
	opts.DrainSentinelPath = DefaultSentinelPath
	opts.DebugErrorResponses = debugErrorResponses

	drainVol := corev1ac.Volume().
		WithName("drain-volume").
		WithEmptyDir(corev1ac.EmptyDirVolumeSource().WithMedium(corev1.StorageMediumMemory))
	drainMount := corev1ac.VolumeMount().
		WithName("drain-volume").
		WithMountPath("/var/run/gcpgateway")

	preStopCmd := []string{
		"/gcpgateway",
		"hook",
		"pre-stop",
		fmt.Sprintf("--drain-seconds=%d", drainSeconds),
	}

	gracePeriodSeconds := int64(drainSeconds + 25)
	if gracePeriodSeconds < 60 {
		gracePeriodSeconds = 60
	}

	opts.AddressProvider = singlepod.NewAddressProvider(
		nil,
		singlepod.WithDataplaneImage(dataplaneImg),
		singlepod.WithExtraReadinessGates(LoadBalancerReadyGate),
		singlepod.WithPreStopCommand(preStopCmd...),
		singlepod.WithExtraVolumes([]*corev1ac.VolumeApplyConfiguration{drainVol}, []*corev1ac.VolumeMountApplyConfiguration{drainMount}),
		singlepod.WithTerminationGracePeriodSeconds(gracePeriodSeconds),
		singlepod.WithDebugErrorResponses(debugErrorResponses),
	)

	var zoneList []string
	if zonesStr != "" {
		for _, z := range strings.Split(zonesStr, ",") {
			if trimmed := strings.TrimSpace(z); trimmed != "" {
				zoneList = append(zoneList, trimmed)
			}
		}
	}

	opts.SetupWithManager = func(mgr ctrl.Manager) error {
		reg := registrar.NewRegistrar(mgr.GetClient(), gcpClient, registrar.Config{
			ClusterID: clusterID,
			NegPrefix: negPrefix,
			Project:   project,
			Zones:     zoneList,
		})

		rec := controller.NewGatewayReconciler(
			mgr.GetClient(),
			mgr.GetScheme(),
			gcpClient,
			reg,
			clusterID,
			negPrefix,
			project,
		)
		rec.ControllerName = controllerName
		rec.Zones = zoneList

		return rec.SetupWithManager(mgr)
	}

	return opts, nil
}

func runController(ctx context.Context, args []string) error {
	opts, err := SetupController(ctx, args, nil)
	if err != nil {
		return err
	}
	return gari.Run(ctx, opts)
}

func main() {
	ctx := ctrl.SetupSignalHandler()
	if err := Run(ctx, os.Args[1:]); err != nil {
		klog.Errorf("gcpgateway: %v", err)
		os.Exit(1)
	}
}
