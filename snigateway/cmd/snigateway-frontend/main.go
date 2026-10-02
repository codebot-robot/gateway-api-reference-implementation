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
	if len(os.Args) > 1 && os.Args[1] == "generate-certs" {
		runGenerateCerts(os.Args[2:])
		return
	}

	fs := flag.NewFlagSet("snigateway-frontend", flag.ExitOnError)

	var listenAddrs stringSliceFlag
	fs.Var(&listenAddrs, "listen", "Address to listen on (can be specified multiple times, default :443)")
	caCertPath := fs.String("ca-cert", "ca.crt", "Path to CA certificate PEM file")
	serverCertPath := fs.String("server-cert", "server.crt", "Path to server certificate PEM file")
	serverKeyPath := fs.String("server-key", "server.key", "Path to server private key PEM file")
	internalHostname := fs.String("internal-hostname", api.DefaultInternalHostname, "Internal SNI hostname for mTLS management API")
	connectTimeout := fs.Duration("connect-timeout", 10*time.Second, "Timeout for client to dial back and claim pending connection")

	if err := fs.Parse(os.Args[1:]); err != nil {
		log.Fatalf("Error parsing flags: %v", err)
	}

	if len(listenAddrs) == 0 {
		listenAddrs = []string{":443"}
	}

	caCertPEM, err := os.ReadFile(*caCertPath)
	if err != nil {
		log.Fatalf("Error reading CA cert from %s: %v", *caCertPath, err)
	}

	serverCertPEM, err := os.ReadFile(*serverCertPath)
	if err != nil {
		log.Fatalf("Error reading server cert from %s: %v", *serverCertPath, err)
	}

	serverKeyPEM, err := os.ReadFile(*serverKeyPath)
	if err != nil {
		log.Fatalf("Error reading server key from %s: %v", *serverKeyPath, err)
	}

	serverTLS, err := certs.NewServerTLSConfig(caCertPEM, serverCertPEM, serverKeyPEM)
	if err != nil {
		log.Fatalf("Error creating server TLS config: %v", err)
	}

	srv, err := frontend.NewServer(frontend.ServerConfig{
		ListenAddrs:      listenAddrs,
		InternalHostname: *internalHostname,
		ServerTLSConfig:  serverTLS,
		ConnectTimeout:   *connectTimeout,
	})
	if err != nil {
		log.Fatalf("Error initializing frontend server: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("Received signal %v, shutting down...", sig)
		_ = srv.Close()
	}()

	log.Printf("Starting snigateway-frontend listening on %v (internal SNI: %s)...", listenAddrs, *internalHostname)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("Server stopped: %v", err)
	}
}

func runGenerateCerts(args []string) {
	fs := flag.NewFlagSet("generate-certs", flag.ExitOnError)
	dir := fs.String("dir", ".", "Directory to write generated certificates to")
	serverName := fs.String("server-name", api.DefaultInternalHostname, "Server SNI hostname")
	clientCN := fs.String("client-name", "snigateway-client", "Client certificate CommonName")

	if err := fs.Parse(args); err != nil {
		log.Fatalf("Error parsing generate-certs flags: %v", err)
	}

	if err := certs.GenerateAndWriteCertificates(*dir, *serverName, *clientCN); err != nil {
		log.Fatalf("Error generating certificates: %v", err)
	}

	fmt.Printf("Successfully generated certificates in %s:\n", *dir)
	fmt.Printf("  CA:     %s, %s\n", certs.CACertFilename, certs.CAKeyFilename)
	fmt.Printf("  Server: %s, %s\n", certs.ServerCertFilename, certs.ServerKeyFilename)
	fmt.Printf("  Client: %s, %s\n", certs.ClientCertFilename, certs.ClientKeyFilename)
}
