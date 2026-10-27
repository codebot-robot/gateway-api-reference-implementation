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

package metadata

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

type testVector struct {
	ClusterID    string         `json:"cluster_id"`
	Namespace    string         `json:"namespace"`
	GatewayName  string         `json:"gateway_name"`
	Prefix       string         `json:"prefix"`
	Suffix       string         `json:"suffix"`
	ExpectedHash string         `json:"expected_hash"`
	ExpectedName string         `json:"expected_name"`
	Metadata     map[string]any `json:"metadata"`
}

func TestSharedVectors(t *testing.T) {
	path := filepath.Join("..", "..", "testdata", "naming_vectors.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read %s: %v", path, err)
	}

	var vectors []testVector
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatalf("failed to unmarshal test vectors: %v", err)
	}

	for _, v := range vectors {
		t.Run(v.ExpectedName, func(t *testing.T) {
			actualHash := Hash(v.ClusterID, v.Namespace, v.GatewayName)
			if actualHash != v.ExpectedHash {
				t.Errorf("expected hash %q, got %q", v.ExpectedHash, actualHash)
			}

			actualName := ResourceName(v.Prefix, v.ClusterID, v.Namespace, v.GatewayName, v.Suffix)
			if actualName != v.ExpectedName {
				t.Errorf("expected resource name %q, got %q", v.ExpectedName, actualName)
			}

			metaBytes, err := json.Marshal(v.Metadata)
			if err != nil {
				t.Fatalf("failed to marshal metadata: %v", err)
			}

			parsed, err := Parse(string(metaBytes))
			if err != nil {
				t.Fatalf("failed to parse metadata: %v", err)
			}

			expectedPort := int(v.Metadata["port"].(float64))
			if !parsed.Matches(v.ClusterID, v.Namespace, v.GatewayName, expectedPort) {
				t.Errorf("metadata does not match expected fields: %+v", parsed)
			}
		})
	}
}

func TestPythonFrontendScripts(t *testing.T) {
	pythonPath, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not found in PATH")
	}

	cmd := exec.Command(pythonPath, "-m", "unittest", "discover", "-s", filepath.Join("hack", "tests"))
	cmd.Dir = filepath.Join("..", "..")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("python unit tests failed:\n%s", string(out))
	}
}
