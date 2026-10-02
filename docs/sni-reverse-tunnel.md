# Example Accelerator: SNI Front-End with Reverse Tunnels

This document describes an example of the
[fallback + acceleration](accelerated-operations.md) model: a lightweight SNI
proxy runs on a front-end node, and the cluster reverse-tunnels the TLS
services it wants to expose to it.

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
client ──TLS──► front-end node (SNI proxy) ══reverse tunnel══► GARI in cluster ──► backends
                 reads SNI only                                 terminates TLS,
                                                                does all routing
```

1. **Announce.** GARI looks at the Gateway listeners it serves (HTTPS/TLS
   listeners with hostnames, and attached routes) and works out the set of
   SNI hostnames the cluster wants to receive.
2. **Tunnel.** GARI opens an outbound connection to the front-end node and
   registers those hostnames. Because the connection is outbound from the
   cluster, there are no inbound firewall rules and no public IP on the
   cluster side.
3. **Route by SNI.** When a client connects to the front-end node, the SNI
   proxy peeks at the ClientHello, looks up the hostname, and forwards the
   bytes, unmodified, over the matching tunnel.
4. **Terminate in the cluster.** GARI terminates TLS using the certificates
   from the Gateway listener, and then applies normal Gateway API routing.

As the Gateway configuration changes, GARI updates its announcements.

## Encryption

Arguably the tunnel does not need encryption of its own. The client's TLS
session goes end-to-end to GARI, so the front-end node and the tunnel only
ever see ciphertext. The only plaintext they see is the SNI hostname, which
is already visible to any on-path observer (absent Encrypted Client Hello).
The front-end node never holds private keys.

What the tunnel *does* need is **authentication of announcements**. The
front-end must only accept a hostname registration from a party allowed to
serve that hostname. Otherwise anyone who can reach the front-end could
claim a hostname and receive its traffic. They still could not decrypt it
without the certificate's private key, but they could deny or disrupt
service. Options include mTLS on the tunnel connection, a shared token per
cluster, or a static allow-list of hostnames per cluster on the front-end.

## Relationship to acceleration

This example sits at the "steering" end of acceleration. The front-end
offloads connection acceptance and SNI-level demultiplexing, and GARI
remains the full implementation behind it. It is the simplest case to start
with because it needs no understanding of L7 features at all. Later
accelerators can take on more (for example, terminating TLS and handling
simple HTTPRoutes at the edge), with GARI as the fallback for everything else.

It also shows the extension point clearly: the front-end is driven entirely
by what GARI announces, which in turn is derived from the same resolved
state that drives GARI's own proxy.

## Open questions

- **Tunnel transport.** Options include multiplexing many streams over one
  connection (HTTP/2 CONNECT, yamux, QUIC) or one TCP connection per client
  connection.
- **Plain HTTP.** Port 80 has no SNI. The front-end could route on the Host
  header, which means a little HTTP parsing, or just redirect everything to
  HTTPS.
- **Client address.** Backends lose the real client IP unless we add PROXY
  protocol (or similar) inside the tunnel.
- **TLS passthrough.** TLSRoute passthrough fits naturally: the hostname
  announcement is the same, and GARI forwards the stream without terminating
  it.
- **Multiple clusters / HA.** Several clusters (or replicas) announcing the
  same hostname could give simple failover or load spreading.
