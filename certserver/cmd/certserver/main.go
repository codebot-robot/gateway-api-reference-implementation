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
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	acmeclient "github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/acme"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/cache"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/challenge"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/manager"
	"github.com/gke-labs/gateway-api-reference-implementation/certserver/pkg/metrics"
)

type Config struct {
	DirectoryURL        string
	EABKID              string
	EABHMACKeyFile      string
	CacheDir            string
	ChallengeListenAddr string
	MetricsListenAddr   string
	MaxHostnames        int
	MaxPendingOrders    int64
	HTTPClient          *http.Client
}

func parseFlags(args []string) (*Config, error) {
	fs := flag.NewFlagSet("certserver", flag.ContinueOnError)

	cfg := &Config{}
	fs.StringVar(&cfg.DirectoryURL, "directory-url", "https://acme-v02.api.letsencrypt.org/directory", "ACME directory URL")
	fs.StringVar(&cfg.EABKID, "eab-kid", "", "External Account Binding Key Identifier (KID)")
	fs.StringVar(&cfg.EABHMACKeyFile, "eab-hmac-key-file", "", "Path to file containing External Account Binding HMAC key")
	fs.StringVar(&cfg.CacheDir, "cache-dir", "/var/cache/certserver", "Directory path for certificate and key disk cache")
	fs.StringVar(&cfg.ChallengeListenAddr, "challenge-listen-addr", ":8080", "HTTP listen address for /.well-known/acme-challenge/")
	fs.StringVar(&cfg.MetricsListenAddr, "metrics-listen-addr", ":9090", "HTTP listen address for Prometheus metrics")
	fs.IntVar(&cfg.MaxHostnames, "max-hostnames", 100, "Maximum number of hostnames permitted per certificate set")
	fs.Int64Var(&cfg.MaxPendingOrders, "max-pending-orders", 50, "Maximum number of concurrent pending ACME orders")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return cfg, nil
}

func run(ctx context.Context, cfg *Config) error {
	logger := log.New(os.Stdout, "certserver: ", log.LstdFlags)
	logger.Printf("Starting certserver with directory %s and cache %s", cfg.DirectoryURL, cfg.CacheDir)

	// Resolve EAB HMAC key from file or environment variable
	var eabHMACKey string
	if cfg.EABHMACKeyFile != "" {
		keyBytes, err := os.ReadFile(cfg.EABHMACKeyFile)
		if err != nil {
			return fmt.Errorf("failed to read EAB HMAC key file: %w", err)
		}
		eabHMACKey = strings.TrimSpace(string(keyBytes))
	} else if envKey := os.Getenv("CERT_SERVER_EAB_HMAC_KEY"); envKey != "" {
		eabHMACKey = strings.TrimSpace(envKey)
	} else if envKey := os.Getenv("EAB_HMAC_KEY"); envKey != "" {
		eabHMACKey = strings.TrimSpace(envKey)
	}

	// Initialize Disk Cache
	diskCache, err := cache.NewDiskCache(cfg.CacheDir)
	if err != nil {
		return fmt.Errorf("failed to initialize disk cache: %w", err)
	}

	// Load or generate ACME account key from cache directory
	accountKey, err := diskCache.LoadOrGenerateAccountKey()
	if err != nil {
		return fmt.Errorf("failed to load or generate account key: %w", err)
	}

	// Initialize Challenge Store and HTTP Handler
	challengeStore := challenge.NewMemoryStore()
	challengeHandler := challenge.NewHandler(challengeStore)

	challengeServer := &http.Server{
		Addr:    cfg.ChallengeListenAddr,
		Handler: challengeHandler,
	}

	// Initialize Prometheus Metrics
	reg := prometheus.DefaultRegisterer
	m := metrics.New(reg)

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{
		Addr:    cfg.MetricsListenAddr,
		Handler: metricsMux,
	}

	// Initialize ACME Client
	acmeCli, err := acmeclient.NewClient(ctx, acmeclient.Config{
		DirectoryURL: cfg.DirectoryURL,
		EABKID:       cfg.EABKID,
		EABHMACKey:   eabHMACKey,
		AccountKey:   accountKey,
		HTTPClient:   cfg.HTTPClient,
		UserAgent:    "gari-certserver/1.0",
	})
	if err != nil {
		return fmt.Errorf("failed to initialize ACME client: %w", err)
	}

	// Initialize Certificate Manager
	mgr := manager.NewManager(acmeCli, diskCache, challengeStore, m, func(o *manager.Options) {
		o.MaxHostnamesPerSet = cfg.MaxHostnames
		o.MaxPendingOrders = cfg.MaxPendingOrders
	})

	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("failed to start certificate manager: %w", err)
	}
	defer mgr.Stop()

	// Start HTTP servers in background
	go func() {
		logger.Printf("Serving ACME HTTP-01 challenges on %s", cfg.ChallengeListenAddr)
		if err := challengeServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Printf("Challenge server error: %v", err)
		}
	}()

	go func() {
		logger.Printf("Serving Prometheus metrics on %s/metrics", cfg.MetricsListenAddr)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Printf("Metrics server error: %v", err)
		}
	}()

	// Wait for shutdown signal
	<-ctx.Done()
	logger.Println("Shutting down certserver...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := challengeServer.Shutdown(shutdownCtx); err != nil {
		logger.Printf("Challenge server shutdown error: %v", err)
	}
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		logger.Printf("Metrics server shutdown error: %v", err)
	}

	return nil
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		log.Fatalf("Error parsing flags: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		log.Fatalf("certserver error: %v", err)
	}
}
