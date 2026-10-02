# SNIGateway

`snigateway` provides a lightweight, pure Go SNI proxy and reverse-tunnel solution for exposing in-cluster TLS services via an external front-end node without terminating TLS for routed services.

## Overview

The `snigateway-frontend` acts as an entrypoint on a publicly accessible or frontend node:
1. It listens on one or more TLS ports (e.g. `:443`).
2. For each incoming connection, it peeks at the TLS ClientHello without consuming bytes.
3. If the SNI hostname is `snigateway.internal`, it terminates TLS with mTLS (requiring a client certificate signed by a trusted CA) to serve the management API.
4. For other hostnames, it looks up the registration table (supporting exact hostnames and `*.example.com` wildcards) and splices the raw connection (including the initial ClientHello bytes) into a reverse tunnel dialed back by the backend client that registered the hostname. If no client is registered for the hostname, the connection is closed.

## Certificate Generation

To generate the CA, server certificate (`snigateway.internal`), and client certificate/key:

```bash
go run ./cmd/snigateway-frontend generate-certs --dir certs
```

This generates:
- `ca.crt` / `ca.key`: Self-signed CA
- `server.crt` / `server.key`: Server certificate for `snigateway.internal`
- `client.crt` / `client.key`: Client certificate for mTLS authentication

## Running `snigateway-frontend`

Run the frontend server with:

```bash
go run ./cmd/snigateway-frontend \
  --listen ":443" \
  --ca-cert certs/ca.crt \
  --server-cert certs/server.crt \
  --server-key certs/server.key
```

### Flags

- `--listen`: Repeatable flag specifying addresses to listen on (default: `:443`).
- `--ca-cert`: Path to CA certificate PEM file (default: `ca.crt`).
- `--server-cert`: Path to server certificate PEM file (default: `server.crt`).
- `--server-key`: Path to server private key PEM file (default: `server.key`).
- `--internal-hostname`: Internal SNI hostname for mTLS management API (default: `snigateway.internal`).
- `--connect-timeout`: Timeout for client dial-back claiming incoming connection (default: `10s`).

## API (mTLS, `snigateway.internal`)

- `PUT /v1/registration`: Sets the full list of hostnames served by the client (JSON body: `{"hostnames": ["foo.example.com", "*.bar.com"]}`). Registrations are active while the connections stream is open.
- `GET /v1/connections`: Long-lived streaming endpoint; emits newline-delimited JSON `{"id":"<conn-id>","hostname":"<sni>"}` when an incoming connection arrives for a registered hostname.
- `POST /v1/connections/<id>`: Client dials back using mTLS with HTTP/1.1 `Upgrade: snigateway-tunnel`. After upgrading with `101 Switching Protocols`, the connection is spliced directly to the client's connection.
