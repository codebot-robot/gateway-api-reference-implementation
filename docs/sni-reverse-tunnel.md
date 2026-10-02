# Example Accelerator: SNI Front-End with Reverse Tunnels

This document describes the design of the SNI reverse tunnel proxy (`snigateway`), which serves as an example of the "fallback + acceleration" model described in [docs/accelerated-operations.md](accelerated-operations.md) — albeit one based around **functionality offload** (offloading public ingress routing and connection termination) rather than performance offload.

In this model, a lightweight SNI proxy (`snigateway-frontend`) runs on a front-end node (e.g. a public VM or edge node), while clusters located in private networks, behind firewalls, or behind NAT reverse-tunnel the TLS services they want to expose to it.

## Motivation

Exposing a cluster to the internet normally needs a LoadBalancer Service, a
cloud load balancer, or inbound firewall rules to the nodes. Many
environments (home labs, edge sites, clusters behind NAT, dev clusters) have
none of these, but can easily run one small machine with a public address.

The front-end node does not need to understand HTTP, Gateway API, or
certificates. It only needs to:

1. accept TLS connections and read the SNI hostname from the ClientHello, and
2. forward the raw connection to whichever cluster announced that hostname.

## How it works

```
client ──TLS──► front-end node (snigateway-frontend) ══reverse tunnel══► GARI in cluster ──► backends
                 reads SNI only                                           terminates TLS,
                 manages mTLS API & dialback tunnels                      does all routing
```

1. **Announce.** GARI looks at the Gateway listeners it serves (HTTPS/TLS
   listeners with hostnames, and attached routes) and works out the set of
   SNI hostnames the cluster wants to receive.
2. **Tunnel & Registration.** The in-cluster controller connects to `snigateway-frontend` at `snigateway.internal` using mTLS (authenticated via a shared CA). It opens `GET /v1/connections` to maintain a long-lived connection event stream and calls `PUT /v1/registration` to register its hostnames (supporting exact hostnames and `*.example.com` wildcards). Because the connection is outbound from the cluster, there are no inbound firewall rules and no public IP needed on the cluster side.
3. **Route by SNI.** When a client connects to the front-end node, the SNI
   proxy peeks at the TLS ClientHello without consuming stream bytes, looks up the hostname in the registration table (exact match wins over wildcards), and notifies the client over `GET /v1/connections`.
4. **Dial-back & Splicing.** The cluster controller receives a `ConnectionEvent` with a unique connection ID and immediately dials back with a new mTLS connection to `POST /v1/connections/<id>` with `Upgrade: snigateway-tunnel`. The frontend upgrades the connection with `101 Switching Protocols` and splices the waiting client connection (including peeked ClientHello bytes) and the dialback tunnel.
5. **Terminate in the cluster.** GARI terminates TLS using the certificates
   from the Gateway listener, and then applies normal Gateway API routing.

As the Gateway configuration changes, GARI updates its announcements.

## Encryption & Security

The client's TLS session goes end-to-end to GARI in the cluster, so the front-end node and the reverse tunnel only ever see ciphertext. The only plaintext they see is the SNI hostname, which is already visible to any on-path observer (absent Encrypted Client Hello). The front-end node never holds private keys for routed domains.

Management operations (`snigateway.internal`) require **mutual TLS (mTLS)**: the front-end verifies client certificates signed by our private CA, ensuring only authorized clusters can register hostnames and claim tunnels.

## Relationship to acceleration

This example sits at the "steering" end of acceleration. The front-end
offloads connection acceptance and SNI-level demultiplexing, and GARI
remains the full implementation behind it. It is the simplest case to start
with because it needs no understanding of L7 features at all. Later
accelerated implementations can take on more (for example, terminating TLS
and handling simple HTTPRoutes at the edge), with GARI as the fallback for
everything else.

It also exercises the embedding model: the in-cluster tunnel client is a
small program that embeds GARI. It uses a GARI hook point to learn the
resolved listeners and hostnames, announces them to the front-end, and hands
the tunnelled connections to the embedded GARI proxy. The front-end itself
stays a simple, separate SNI proxy.

## Implementation (`snigateway`)

The `snigateway` module contains the frontend and supporting packages:
- `snigateway/cmd/snigateway-frontend`: The frontend server binary with `generate-certs` subcommand.
- `snigateway/cmd/snigateway`: In-cluster controller binary embedding GARI and reverse-tunnel client.
- `snigateway/pkg/sni`: TLS ClientHello sniffing and parsing.
- `snigateway/pkg/frontend`: Registration table, mTLS API server, and reverse-tunnel splicing.
- `snigateway/pkg/certs`: In-memory and on-disk CA/server/client certificate generation.
- `snigateway/pkg/client`: Reusable client library for in-cluster controllers.
- `snigateway/pkg/tunnel`: Tunnel listener, hostname extraction, and connection manager.
- `snigateway/k8s/`: Kubernetes manifests (RBAC, GatewayClass, Deployment, example Gateway/Route).
