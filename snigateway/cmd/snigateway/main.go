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
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/gari"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/certs"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/client"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/tunnel"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2/textlogger"
	ctrl "sigs.k8s.io/controller-runtime"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

type stringSliceFlag []string

func (s *stringSliceFlag) String() string {
	return strings.Join(*s, ",")
}

func (s *stringSliceFlag) Set(val string) error {
	*s = append(*s, val)
	return nil
}

const (
	// DefaultControllerName is the default GatewayClass controller name for snigateway.
	DefaultControllerName = "github.com/gke-labs/gateway-api-reference-implementation/snigateway"
)

var setupLog = ctrl.Log.WithName("setup")

func main() {
	var (
		frontendAddr        string
		caCertPath          string
		clientCertPath      string
		clientKeyPath       string
		internalHostname    string
		controllerName      string
		metricsAddr         string
		healthProbeAddr     string
		proxyAddr           string
		tunnelTransport     string
		tunnelPoolSize      int
		quicDialTimeout     time.Duration
		quicProbeInterval   time.Duration
		leaderElection      bool
		leaderElectionID    string
		gatewayAddresses    stringSliceFlag
		tokenFilePath       string
		debugErrorResponses bool
	)

	gariOpts := gari.DefaultOptions()

	flag.StringVar(&frontendAddr, "frontend", "127.0.0.1:443", "Address of the snigateway-frontend server (<ip-or-host>:<port>).")
	flag.Var(&gatewayAddresses, "gateway-address", "Address to report in Gateway status (repeatable, IP or hostname). Defaults to host part of --frontend.")
	flag.StringVar(&caCertPath, "ca-cert", "", "Path to the CA certificate PEM file used to verify snigateway-frontend.")
	flag.StringVar(&clientCertPath, "client-cert", "", "Path to the client certificate PEM file for mTLS authentication.")
	flag.StringVar(&clientKeyPath, "client-key", "", "Path to the client private key PEM file for mTLS authentication.")
	flag.StringVar(&tokenFilePath, "token-file", "", "Path to ServiceAccount token file for authentication.")
	flag.StringVar(&internalHostname, "internal-hostname", "snigateway.internal", "TLS ServerName for the snigateway-frontend mTLS management API.")
	flag.StringVar(&controllerName, "controller-name", DefaultControllerName, "The GatewayClass controller name managed by this instance.")
	flag.StringVar(&tunnelTransport, "tunnel-transport", tunnel.TransportModeAuto, "Tunnel transport to use (auto, quic, tcp, h2, wireguard).")
	flag.IntVar(&tunnelPoolSize, "tunnel-pool-size", tunnel.DefaultPoolSize, "Number of idle pre-dialed reverse tunnel connections to maintain with the frontend when using TCP pool or H2 transport.")
	flag.DurationVar(&quicDialTimeout, "quic-dial-timeout", 3*time.Second, "Timeout for dialing QUIC connections.")
	flag.DurationVar(&quicProbeInterval, "quic-probe-interval", 30*time.Second, "Interval for probing QUIC availability when in TCP fallback.")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&healthProbeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.StringVar(&proxyAddr, "proxy-bind-address", "", "Optional HTTP proxy bind address. Disabled by default.")
	flag.BoolVar(&leaderElection, "leader-elect", false, "Enable leader election for controller manager.")
	flag.StringVar(&leaderElectionID, "leader-election-id", "snigateway-controller", "The ID used for leader election.")
	flag.BoolVar(&debugErrorResponses, "debug-error-responses", false, "Include detailed error messages in unmatched responses.")
	flag.StringVar(&gariOpts.ACMEChallengeUpstreamURL, "acme-challenge-upstream-url", "", "URL of the upstream certificate server to forward ACME HTTP-01 challenge requests to.")
	flag.StringVar(&gariOpts.ACMEChallengeCABundlePath, "acme-challenge-ca-bundle-path", "", "Path to the CA bundle PEM file to verify the upstream certificate server.")
	flag.StringVar(&gariOpts.ACMEChallengeTokenPath, "acme-challenge-token-path", "", "Path to the projected service-account token file for data-plane identity.")

	logConfig := textlogger.NewConfig()
	logConfig.AddFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(textlogger.NewLogger(logConfig))

	if caCertPath == "" {
		setupLog.Error(fmt.Errorf("--ca-cert is required"), "missing required flag")
		os.Exit(1)
	}
	if tokenFilePath == "" {
		if clientCertPath == "" {
			setupLog.Error(fmt.Errorf("--client-cert is required when --token-file is not specified"), "missing required flag")
			os.Exit(1)
		}
		if clientKeyPath == "" {
			setupLog.Error(fmt.Errorf("--client-key is required when --token-file is not specified"), "missing required flag")
			os.Exit(1)
		}
	}

	caCertPEM, err := os.ReadFile(caCertPath)
	if err != nil {
		setupLog.Error(err, "failed to read CA certificate", "path", caCertPath)
		os.Exit(1)
	}

	var clientCertPEM, clientKeyPEM []byte
	if clientCertPath != "" {
		clientCertPEM, err = os.ReadFile(clientCertPath)
		if err != nil {
			setupLog.Error(err, "failed to read client certificate", "path", clientCertPath)
			os.Exit(1)
		}
	}

	if clientKeyPath != "" {
		clientKeyPEM, err = os.ReadFile(clientKeyPath)
		if err != nil {
			setupLog.Error(err, "failed to read client private key", "path", clientKeyPath)
			os.Exit(1)
		}
	}

	clientTLS, err := certs.NewClientTLSConfig(caCertPEM, clientCertPEM, clientKeyPEM, internalHostname)
	if err != nil {
		setupLog.Error(err, "failed to create client TLS config")
		os.Exit(1)
	}

	clientOpts := []client.ClientOption{
		client.WithInternalHostname(internalHostname),
	}
	if tokenFilePath != "" {
		clientOpts = append(clientOpts, client.WithTokenFile(tokenFilePath))
	}

	snigatewayClient := client.NewClient(frontendAddr, clientTLS, clientOpts...)
	tunnelMgr := tunnel.NewManager(snigatewayClient,
		tunnel.WithTransport(tunnelTransport),
		tunnel.WithPoolSize(tunnelPoolSize),
		tunnel.WithQUICDialTimeout(quicDialTimeout),
		tunnel.WithQUICProbeInterval(quicProbeInterval),
	)

	addrs := []string(gatewayAddresses)
	if len(addrs) == 0 {
		addrs = []string{tunnel.ParseFrontendHost(frontendAddr)}
	}

	addrProvider := tunnel.NewAddressProvider(tunnelMgr, addrs...)
	addrProvider.SetControllerName(controllerName)

	gariOpts.ControllerName = controllerName
	gariOpts.MetricsAddr = metricsAddr
	gariOpts.HealthProbeBindAddress = healthProbeAddr
	gariOpts.ProxyAddr = proxyAddr
	gariOpts.ProxyHTTPSAddr = ""
	gariOpts.HTTPSListener = tunnelMgr.Listener()
	gariOpts.LeaderElection = leaderElection
	gariOpts.LeaderElectionID = leaderElectionID
	gariOpts.DebugErrorResponses = debugErrorResponses
	gariOpts.AddressProvider = addrProvider
	gariOpts.OnGatewaysUpdate = func(gateways []*gatewayv1.Gateway) {
		tunnelMgr.UpdateGateways(gateways)
	}

	ctx := ctrl.SetupSignalHandler()
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return tunnelMgr.Run(ctx)
	})

	g.Go(func() error {
		return gari.Run(ctx, gariOpts)
	})

	if err := g.Wait(); err != nil {
		setupLog.Error(err, "fatal error running snigateway controller")
		os.Exit(1)
	}
}
