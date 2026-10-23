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

package testenv

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGatewayAPICRDDir(t *testing.T) {
	crdDir, err := GatewayAPICRDDir()
	if err != nil {
		t.Fatalf("GatewayAPICRDDir failed: %v", err)
	}

	fi, err := os.Stat(crdDir)
	if err != nil || !fi.IsDir() {
		t.Fatalf("GatewayAPICRDDir returned invalid directory: %q (err: %v)", crdDir, err)
	}

	// Verify standard Gateway CRD exists in the directory
	gwCRD := filepath.Join(crdDir, "gateway.networking.k8s.io_gateways.yaml")
	if _, err := os.Stat(gwCRD); err != nil {
		t.Errorf("expected %s to exist in CRD directory: %v", gwCRD, err)
	}
}
