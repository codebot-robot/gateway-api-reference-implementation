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
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/api"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/certs"
	"github.com/gke-labs/gateway-api-reference-implementation/snigateway/pkg/frontend"
)

type stringSliceFlag []string

func (f *stringSliceFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *stringSliceFlag) Set(val string) error {
	*f = append(*f, val)
	return nil
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) > 0 && args[0] == "generate-certs" {
		return runGenerateCerts(args[1:])
	}

	fs := flag.NewFlagSet("snigateway-frontend", flag.ContinueOnError)

	var listenAddrs stringSliceFlag
	fs.Var(&listenAddrs, "listen", "Address to listen on (can be specified multiple times, default :443)")
	caCertPath := fs.String("ca-cert", "ca.crt", "Path to CA certificate PEM file")
	serverCertPath := fs.String("server-cert", "server.crt", "Path to server certificate PEM file")
	serverKeyPath := fs.String("server-key", "server.key", "Path to server private key PEM file")
	internalHostname := fs.String("internal-hostname", api.DefaultInternalHostname, "Internal SNI hostname for mTLS management API")
	connectTimeout := fs.Duration("connect-timeout", 10*time.Second, "Timeout for client to dial back and claim pending connection")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if len(listenAddrs) == 0 {
		listenAddrs = []string{":443"}
	}

	caCertPEM, err := os.ReadFile(*caCertPath)
	if err != nil {
		return fmt.Errorf("reading CA cert from %s: %w", *caCertPath, err)
	}

	serverCertPEM, err := os.ReadFile(*serverCertPath)
	if err != nil {
		return fmt.Errorf("reading server cert from %s: %w", *serverCertPath, err)
	}

	serverKeyPEM, err := os.ReadFile(*serverKeyPath)
	if err != nil {
		return fmt.Errorf("reading server key from %s: %w", *serverKeyPath, err)
	}

	serverTLS, err := certs.NewServerTLSConfig(caCertPEM, serverCertPEM, serverKeyPEM)
	if err != nil {
		return fmt.Errorf("creating server TLS config: %w", err)
	}

	srv, err := frontend.NewServer(frontend.ServerConfig{
		ListenAddrs:      listenAddrs,
		InternalHostname: *internalHostname,
		ServerTLSConfig:  serverTLS,
		ConnectTimeout:   *connectTimeout,
	})
	if err != nil {
		return fmt.Errorf("initializing frontend server: %w", err)
	}

	go func() {
		<-ctx.Done()
		log.Printf("Shutting down snigateway-frontend...")
		_ = srv.Close()
	}()

	log.Printf("Starting snigateway-frontend listening on %v (internal SNI: %s)...", listenAddrs, *internalHostname)
	if err := srv.ListenAndServe(); err != nil {
		return fmt.Errorf("server stopped: %w", err)
	}

	return nil
}

func runGenerateCerts(args []string) error {
	fs := flag.NewFlagSet("generate-certs", flag.ContinueOnError)
	dir := fs.String("dir", ".", "Directory to write generated certificates to")
	serverName := fs.String("server-name", api.DefaultInternalHostname, "Server SNI hostname")
	clientCN := fs.String("client-name", "snigateway-client", "Client certificate CommonName")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if err := certs.GenerateAndWriteCertificates(*dir, *serverName, *clientCN); err != nil {
		return fmt.Errorf("generating certificates: %w", err)
	}

	fmt.Printf("Successfully generated certificates in %s:\n", *dir)
	fmt.Printf("  CA:     ca.crt, ca.key\n")
	fmt.Printf("  Server: server.crt, server.key\n")
	fmt.Printf("  Client: client.crt, client.key\n")
	return nil
}
