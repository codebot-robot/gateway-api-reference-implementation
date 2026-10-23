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
	"os"
	"path/filepath"
	"testing"
)

func TestPreStopHook(t *testing.T) {
	tmpDir := t.TempDir()
	sentinelPath := filepath.Join(tmpDir, "sub", "draining")

	args := []string{
		"--drain-seconds=0",
		"--sentinel-file=" + sentinelPath,
	}

	if err := RunPreStopHook(args); err != nil {
		t.Fatalf("RunPreStopHook failed: %v", err)
	}

	data, err := os.ReadFile(sentinelPath)
	if err != nil {
		t.Fatalf("failed to read sentinel file: %v", err)
	}
	if string(data) != "draining\n" {
		t.Errorf("expected 'draining\\n', got %q", string(data))
	}
}

func TestRun_HookPreStop(t *testing.T) {
	tmpDir := t.TempDir()
	sentinelPath := filepath.Join(tmpDir, "draining")

	args := []string{
		"hook",
		"pre-stop",
		"--drain-seconds=0",
		"--sentinel-file=" + sentinelPath,
	}

	if err := Run(t.Context(), args); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if _, err := os.Stat(sentinelPath); err != nil {
		t.Fatalf("sentinel file not created: %v", err)
	}
}

func TestDataplaneMissingFlags(t *testing.T) {
	err := Run(t.Context(), []string{"--dataplane-mode"})
	if err == nil {
		t.Fatalf("expected error when namespace/name missing in dataplane mode, got nil")
	}
}
