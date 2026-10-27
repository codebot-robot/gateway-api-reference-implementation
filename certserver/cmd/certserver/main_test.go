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
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/testserver"
)

func TestParseFlags_DefaultsAndCustom(t *testing.T) {
	// 1. Defaults
	cfg, err := parseFlags([]string{})
	if err != nil {
		t.Fatalf("parseFlags default failed: %v", err)
	}

	if cfg.DirectoryURL != "https://acme-v02.api.letsencrypt.org/directory" {
		t.Errorf("unexpected directory URL: %s", cfg.DirectoryURL)
	}
	if cfg.MaxHostnames != 100 {
		t.Errorf("unexpected max hostnames: %d", cfg.MaxHostnames)
	}
	if cfg.MaxPendingOrders != 50 {
		t.Errorf("unexpected max pending orders: %d", cfg.MaxPendingOrders)
	}
	if cfg.ChallengeListenAddr != ":8080" {
		t.Errorf("unexpected challenge listen addr: %s", cfg.ChallengeListenAddr)
	}
	if cfg.MetricsListenAddr != ":9090" {
		t.Errorf("unexpected metrics listen addr: %s", cfg.MetricsListenAddr)
	}

	// 2. Custom
	customArgs := []string{
		"--directory-url=https://custom-acme.local/dir",
		"--eab-kid=custom-kid",
		"--eab-hmac-key-file=/tmp/custom-key-file",
		"--cache-dir=/tmp/custom-cache",
		"--challenge-listen-addr=:18080",
		"--metrics-listen-addr=:19090",
		"--max-hostnames=20",
		"--max-pending-orders=10",
	}

	cfg, err = parseFlags(customArgs)
	if err != nil {
		t.Fatalf("parseFlags custom failed: %v", err)
	}

	if cfg.DirectoryURL != "https://custom-acme.local/dir" {
		t.Errorf("unexpected custom directory URL: %s", cfg.DirectoryURL)
	}
	if cfg.EABKID != "custom-kid" {
		t.Errorf("unexpected custom EAB KID: %s", cfg.EABKID)
	}
	if cfg.EABHMACKeyFile != "/tmp/custom-key-file" {
		t.Errorf("unexpected custom EAB HMAC key file: %s", cfg.EABHMACKeyFile)
	}
	if cfg.CacheDir != "/tmp/custom-cache" {
		t.Errorf("unexpected custom cache dir: %s", cfg.CacheDir)
	}
	if cfg.ChallengeListenAddr != ":18080" {
		t.Errorf("unexpected custom challenge addr: %s", cfg.ChallengeListenAddr)
	}
	if cfg.MetricsListenAddr != ":19090" {
		t.Errorf("unexpected custom metrics addr: %s", cfg.MetricsListenAddr)
	}
	if cfg.MaxHostnames != 20 {
		t.Errorf("unexpected custom max hostnames: %d", cfg.MaxHostnames)
	}
	if cfg.MaxPendingOrders != 10 {
		t.Errorf("unexpected custom max pending orders: %d", cfg.MaxPendingOrders)
	}
}

func waitForReady(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			if closeErr := conn.Close(); closeErr != nil {
				// ignore
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for server to be ready on %s", addr)
}

func TestRun_LifecycleWithPebble(t *testing.T) {
	pebble := testserver.StartPebble(t, testserver.ServerConfig{})
	tempDir := t.TempDir()
	cacheDir := filepath.Join(tempDir, "cache")

	chalLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen for challenge addr failed: %v", err)
	}
	chalAddr := chalLn.Addr().String()
	if err := chalLn.Close(); err != nil {
		t.Fatalf("Close chalLn failed: %v", err)
	}

	metricsLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen for metrics addr failed: %v", err)
	}
	metricsAddr := metricsLn.Addr().String()
	if err := metricsLn.Close(); err != nil {
		t.Fatalf("Close metricsLn failed: %v", err)
	}

	cfg := &Config{
		DirectoryURL:        pebble.DirectoryURL,
		HTTPClient:          pebble.HTTPClient,
		CacheDir:            cacheDir,
		ChallengeListenAddr: chalAddr,
		MetricsListenAddr:   metricsAddr,
		MaxHostnames:        100,
		MaxPendingOrders:    50,
	}

	ctx, cancel := context.WithCancel(t.Context())

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, cfg)
	}()

	// Wait for readiness instead of sleep
	waitForReady(t, chalAddr)
	waitForReady(t, metricsAddr)

	// Trigger graceful shutdown
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for certserver run to shut down")
	}

	// Verify account key was saved to cache directory
	if _, err := os.Stat(filepath.Join(cacheDir, "account.key")); err != nil {
		t.Fatalf("expected account.key to be created in cache dir: %v", err)
	}
}
