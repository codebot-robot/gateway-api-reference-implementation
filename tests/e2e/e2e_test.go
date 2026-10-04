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

package e2e

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestGatewayAPI(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("RUN_E2E env var not set, skipping")
	}

	clusterName := os.Getenv("KIND_CLUSTER_NAME")
	if clusterName == "" {
		clusterName = "kind"
	}

	h := NewHarness(t, clusterName)
	h.Setup()

	// 1. Install Gateway API CRDs
	h.InstallGatewayAPI()

	// 2. Deploy Controller
	h.DeployController()

	// 3. Deploy Backend (Toolbox Server)
	h.DeployBackend()

	// 4. Create Gateway API Resources
	h.KubectlApplyContent(h.ExampleGatewayManifest())
	// Give the controller some time to reconcile
	time.Sleep(5 * time.Second)

	// 5. Run Client Pod (HTTP)
	clientPodName := "test-client"
	h.DeletePod(clientPodName)

	h.KubectlApplyContent(h.ClientManifest("http://gari-proxy", "example.com"))
	h.WaitForPodSuccess(clientPodName, 1*time.Minute)

	logs := h.GetPodLogs(clientPodName)
	t.Logf("Client logs (HTTP): %s", logs)

	// 6. Verify HTTP
	if !strings.Contains(logs, "Status: 200 OK") || (!strings.Contains(logs, "\"hostname\":\"example.com\"") && !strings.Contains(logs, "\"host\": \"example.com\"")) {
		controllerLogs := h.runCmd("kubectl", "logs", "deployment/gari-controller", "--namespace=default")
		t.Logf("Controller logs: %s", controllerLogs)
		if !strings.Contains(logs, "Status: 200 OK") {
			t.Errorf("Expected 200 OK, got: %s", logs)
		}
		if !strings.Contains(logs, "\"hostname\":\"example.com\"") && !strings.Contains(logs, "\"host\": \"example.com\"") {
			t.Errorf("Expected hostname example.com in response body, got: %s", logs)
		}
	}

	// 7. Run Client Pod (HTTPS - verify Alt-Svc header)
	httpsClientPodName := "test-client-https"
	h.DeletePod(httpsClientPodName)

	h.KubectlApplyContent(h.ClientManifestWithArgs(httpsClientPodName, "--insecure", "--sni", "example.com", "https://gari-proxy:443", "example.com"))
	h.WaitForPodSuccess(httpsClientPodName, 1*time.Minute)

	httpsLogs := h.GetPodLogs(httpsClientPodName)
	t.Logf("Client logs (HTTPS): %s", httpsLogs)
	if !strings.Contains(httpsLogs, "Status: 200 OK") {
		t.Errorf("Expected HTTPS 200 OK, got: %s", httpsLogs)
	}
	if !strings.Contains(httpsLogs, "Header-Alt-Svc: h3=\":443\"") {
		t.Errorf("Expected Alt-Svc header advertising h3=\":443\", got: %s", httpsLogs)
	}

	// 8. Run Client Pod (HTTP/3 over QUIC)
	h3ClientPodName := "test-client-http3"
	h.DeletePod(h3ClientPodName)

	h.KubectlApplyContent(h.ClientManifestWithArgs(h3ClientPodName, "--http3", "--insecure", "--sni", "example.com", "https://gari-proxy:443", "example.com"))
	h.WaitForPodSuccess(h3ClientPodName, 1*time.Minute)

	h3Logs := h.GetPodLogs(h3ClientPodName)
	t.Logf("Client logs (HTTP/3): %s", h3Logs)
	if !strings.Contains(h3Logs, "Status: 200 OK") || (!strings.Contains(h3Logs, "\"hostname\":\"example.com\"") && !strings.Contains(h3Logs, "\"host\": \"example.com\"")) {
		t.Errorf("Expected HTTP/3 200 OK with hostname example.com, got: %s", h3Logs)
	}
}
