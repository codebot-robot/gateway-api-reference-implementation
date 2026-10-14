# gateway-api-reference-implementation

A minimal implementation of the Gateway API.

## Goals

The goal of this project is to create a simple, pure Go implementation of the [Gateway API](https://gateway-api.sigs.k8s.io/).

- **Reference Implementation**: This project aims to be a reference implementation rather than a high-performance one. It prioritizes clarity and correctness over speed.
- **Full Feature Support**: We want to support all Gateway API features.
- **Fallback Model**: We are exploring a model where this reference implementation can serve as a fallback for specialized implementations for configurations they cannot accelerate. See [docs/accelerated-operations.md](docs/accelerated-operations.md).
- **Pure Go**: The implementation should be written in pure Go.

## Features

### HTTP/3 (QUIC) Support

GARI supports serving HTTP/3 over QUIC on HTTPS listeners. Gateway API models HTTPS listeners, and GARI can serve the same hostnames, TLS certificates, and HTTPRoutes over both TCP (HTTP/1.1 and HTTP/2) and QUIC (HTTP/3).

HTTP/3 is opt-in and disabled by default.

#### Configuration Options

- **CLI Flags**:
  - `--proxy-http3-bind-address`: UDP address to bind for HTTP/3 (e.g. `:8443`). If empty, HTTP/3 is disabled.
  - `--proxy-http3-advertised-port`: Port advertised in the `Alt-Svc` header on HTTPS (TCP) responses (e.g. `443`). If `0`, the port is inferred from the bind address.

- **Programmatic Options (`pkg/gari.Options`)**:
  - `ProxyHTTP3Addr`: The UDP address string to bind for HTTP/3.
  - `ProxyHTTP3AdvertisedPort`: The port number advertised in `Alt-Svc` headers.
  - `HTTP3PacketConn`: A custom `net.PacketConn` to use for the HTTP/3 server (overriding `ProxyHTTP3Addr`).
  - `HTTP3QUICConfig`: An optional `*quic.Config` for fine-grained QUIC tuning (e.g., MTU / initial packet size).

When HTTP/3 is enabled, HTTPS (TCP) responses automatically include an `Alt-Svc: h3=":<advertised-port>"; ma=86400` header so compatible clients can discover and upgrade to HTTP/3.

### TCP Support (TCPRoute)

GARI supports `TCPRoute` routing raw TCP traffic on dedicated listener ports.

- **Per-Port Listeners & Splicing**: TCP listeners open individual accept sockets per distinct listener port and splice bidirectional TCP traffic to resolved backends immediately upon connection establishment without waiting for client data (safe for backends that speak first).
- **Weighted Routing**: Traffic is distributed across configured backendRefs according to weight; backends with weight `0` receive no traffic.
- **Per-Gateway Data Plane (Default / Conformance)**: In standard deployment (`singlepod`), each Gateway runs its own data plane Deployment and LoadBalancer Service mapping TCP listener ports directly to matching container ports without collapsing onto shared proxy ports.
- **Multiple Attached TCPRoutes**: Because raw TCP has no hostnames or paths to demultiplex on, when multiple TCPRoutes attach to the same TCP listener, only the oldest route's backends receive traffic. Subsequent routes targeting the same listener report `Accepted=True` and contribute to the listener's `attachedRoutes` count, but carry no traffic.
- **All-in-One Mode Limitation**: In all-in-one mode where multiple Gateways share a single controller and proxy process, two Gateways cannot both bind the same TCP port. The first Gateway (by creation timestamp precedence) wins, and any subsequent Gateway listener requesting the same port reports `Programmed=False` with reason `PortUnavailable`. Per-Gateway data plane mode (`singlepod`) is the recommended and conformance path.

### UDP Support (UDPRoute)

GARI supports `UDPRoute` routing UDP datagram traffic on dedicated listener ports.

- **Per-Port Listeners & Client-Address Sessions**: UDP listeners open a `net.PacketConn` per distinct listener port. Sessions are keyed by client address (`IP:port`); the first datagram from a client selects a backend according to configured weights, establishes a connected UDP socket, and forwards subsequent datagrams from that client to the same backend.
- **Weighted Routing**: Traffic is distributed across configured backendRefs according to weight; backends with weight `0` receive no traffic.
- **Session Lifecycle & Limits**: Sessions expire after an idle timeout (default 60s, configurable via `--udp-idle-timeout`) and are capped per listener (default 10000, configurable via `--udp-max-sessions`). New clients exceeding the cap are dropped without evicting active sessions.
- **Per-Gateway Data Plane (Default / Conformance)**: In standard deployment (`singlepod`), each Gateway runs its own data plane Deployment and LoadBalancer Service mapping UDP listener ports directly to matching container ports.
- **Reserved Port**: UDP port 8443 is reserved for HTTP/3; listeners specifying UDP 8443 report `Conflicted=True` with `ProtocolConflict`.
- **All-in-One Mode Limitation**: In all-in-one mode, the first Gateway wins a UDP port; subsequent Gateways requesting the same port report `Programmed=False` with reason `PortUnavailable`.

## Contributing

This project is licensed under the [Apache 2.0 License](LICENSE).

We welcome contributions! Please see [docs/contributing.md](docs/contributing.md) for more information.

We follow [Google's Open Source Community Guidelines](https://opensource.google.com/conduct/).

## Disclaimer

This is not an officially supported Google product.

This project is not eligible for the Google Open Source Software Vulnerability Rewards Program.