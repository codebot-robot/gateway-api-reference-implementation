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
	"os"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/gari"
	"k8s.io/klog/v2/textlogger"
	ctrl "sigs.k8s.io/controller-runtime"
)

var setupLog = ctrl.Log.WithName("setup")

func main() {
	opts := gari.DefaultOptions()

	flag.StringVar(&opts.MetricsAddr, "metrics-bind-address", opts.MetricsAddr, "The address the metric endpoint binds to.")
	flag.StringVar(&opts.HealthProbeBindAddress, "health-probe-bind-address", opts.HealthProbeBindAddress, "The address the probe endpoint binds to.")
	flag.StringVar(&opts.ProxyAddr, "proxy-bind-address", opts.ProxyAddr, "The address the proxy binds to.")
	flag.StringVar(&opts.ProxyHTTPSAddr, "proxy-https-bind-address", opts.ProxyHTTPSAddr, "The address the proxy binds to for HTTPS.")
	flag.BoolVar(&opts.EnableH2C, "enable-h2c", opts.EnableH2C, "Enable H2C support on the proxy server.")
	flag.BoolVar(&opts.LeaderElection, "leader-elect", opts.LeaderElection,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.StringVar(&opts.ControllerName, "controller-name", opts.ControllerName, "The GatewayClass controller name.")

	logConfig := textlogger.NewConfig()
	logConfig.AddFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(textlogger.NewLogger(logConfig))

	ctx := ctrl.SetupSignalHandler()
	if err := gari.Run(ctx, opts); err != nil {
		setupLog.Error(err, "fatal error")
		os.Exit(1)
	}
}
