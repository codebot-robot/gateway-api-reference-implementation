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

## Contributing

This project is licensed under the [Apache 2.0 License](LICENSE).

We welcome contributions! Please see [docs/contributing.md](docs/contributing.md) for more information.

We follow [Google's Open Source Community Guidelines](https://opensource.google.com/conduct/).

## Disclaimer

This is not an officially supported Google product.

This project is not eligible for the Google Open Source Software Vulnerability Rewards Program.