# SNIGateway

`snigateway` provides a lightweight, pure Go SNI proxy and reverse-tunnel solution for exposing in-cluster TLS services via an external front-end node without terminating TLS for routed services.

## Overview

The `snigateway` architecture consists of two main components:
1. **`snigateway-frontend`**: Runs on a publicly accessible frontend node (e.g. edge node, VPS, or public VM):
   - Listens on one or more TLS ports (e.g. `:443`).
   - Peeks at the TLS ClientHello of incoming connections to determine the requested SNI hostname without consuming stream bytes.
   - If the SNI hostname is `snigateway.internal`, it serves the mTLS management API (authenticated via a shared CA).
   - For other hostnames, it matches against registered hostnames and splices the raw client TLS connection into a reverse tunnel dialed back by the cluster controller.
2. **`snigateway` (Controller)**: Runs inside a Kubernetes cluster (e.g. home lab, edge site, private network):
   - Embeds GARI (`pkg/gari`) in-process.
   - Establishes an outbound mTLS reverse tunnel connection to `snigateway-frontend`.
   - Uses GARI's `OnGatewaysUpdate` hook to dynamically announce the SNI hostnames for its HTTPS/TLS listeners.
   - Feeds reverse-tunnelled connections to GARI's HTTPS proxy server via a custom tunnel `net.Listener`, terminating TLS and routing HTTP traffic within the cluster.

## End-to-End Walkthrough

### 1. Generate Certificates

Generate the CA, server certificate (`snigateway.internal`), and client certificate/key:

```bash
go run ./cmd/snigateway-frontend generate-certs --dir certs
```

This generates:
- `ca.crt` / `ca.key`: Self-signed CA
- `server.crt` / `server.key`: Server certificate for `snigateway.internal`
- `client.crt` / `client.key`: Client certificate for mTLS authentication

### 2. Run `snigateway-frontend`

Run the frontend server on the public host:

```bash
go run ./cmd/snigateway-frontend \
  --listen ":443" \
  --ca-cert certs/ca.crt \
  --server-cert certs/server.crt \
  --server-key certs/server.key
```

### 3. Create the Client Certificate Secret in Kubernetes

Create a Kubernetes Secret containing the CA and client credentials generated in Step 1:

```bash
kubectl create secret generic snigateway-client-cert \
  --from-file=ca.crt=certs/ca.crt \
  --from-file=client.crt=certs/client.crt \
  --from-file=client.key=certs/client.key
```

### 4. Deploy `snigateway` Controller

Deploy the controller RBAC, GatewayClass, and Deployment:

```bash
kubectl apply -f k8s/controller.yaml
```

Update the `--frontend` argument in `k8s/controller.yaml` to point to the address (`<host-or-ip>:443`) of your `snigateway-frontend` instance.

### 5. Create Gateway, HTTPRoute, and Backend

Create a Gateway with an HTTPS listener and TLS certificate, and attach an HTTPRoute:

```bash
# Create TLS secret for your domain (e.g. app.example.com)
kubectl create secret tls example-tls-cert \
  --cert=path/to/app.example.com.crt \
  --key=path/to/app.example.com.key

# Apply example Gateway and HTTPRoute
kubectl apply -f k8s/example.yaml
```

### 6. Test with `curl`

Send an HTTPS request through the frontend node:

```bash
curl --resolve app.example.com:443:<frontend-ip> https://app.example.com/
```

The TLS handshake will be routed through the reverse tunnel and terminated inside the cluster by GARI.

## Components & Packages

- `snigateway/cmd/snigateway-frontend`: Frontend server binary with `generate-certs` subcommand.
- `snigateway/cmd/snigateway`: In-cluster controller binary embedding GARI and reverse-tunnel client.
- `snigateway/pkg/sni`: TLS ClientHello sniffing and parsing.
- `snigateway/pkg/frontend`: Registration table, mTLS API server, and reverse-tunnel splicing.
- `snigateway/pkg/certs`: In-memory and on-disk CA/server/client certificate generation.
- `snigateway/pkg/client`: Reusable client library for mTLS API and reverse-tunnel dialbacks.
- `snigateway/pkg/tunnel`: Tunnel listener, hostname extraction from Gateways, and connection manager.
- `snigateway/k8s/`: Kubernetes manifests (RBAC, GatewayClass, Deployment, example Gateway/Route).
