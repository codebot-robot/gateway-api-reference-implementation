# Conformance Test Journal: HTTPRouteBackendProtocolWebSocket

## 1. Test Overview
- **Name**: `HTTPRouteBackendProtocolWebSocket`
- **Description**: Verifies that an HTTPRoute pointing to a backend Service with `appProtocol: kubernetes.io/ws` supports WebSocket connections (HTTP/1.1 Upgrade to WebSocket) and bidirectional frame forwarding between client and backend.
- **Manifests**:
  - `sigs.k8s.io/gateway-api/conformance/tests/httproute-backend-protocol-websocket.yaml`

## 2. Issue / Failure Analysis
- **Observed Behavior**: The test was not yet enabled in `tests/e2e/conformance_test.go`. The reverse proxy removed hop-by-hop headers (including `Upgrade` and `Connection`) and lacked connection hijacking for full-duplex TCP streaming on HTTP 101 Switching Protocols.
- **Root Cause**: The proxy implementation only supported standard HTTP roundtripping and HTTP/2 cleartext (h2c). When a WebSocket upgrade handshake arrived, `http.DefaultTransport` did not maintain a duplex tunnel, and hop-by-hop header removal stripped the required upgrade headers.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Added `isWebSocketRequest(r)` to detect WebSocket upgrade requests via `Upgrade: websocket` and `Connection: Upgrade` headers.
  2. Implemented `forwardWebSocket()` in `pkg/proxy/proxy.go` using `http.Hijacker` on the client connection and a dedicated TCP/TLS connection to the backend.
  3. Preserved `Upgrade` and `Connection` headers while forwarding the handshake request, validated the backend's HTTP 101 Switching Protocols response, and established bidirectional streaming (`io.Copy` between client and backend connections).
  4. Added unit test `TestProxyWebSocket` in `pkg/proxy/proxy_test.go`.
  5. Added `tests.HTTPRouteBackendProtocolWebSocket` to `selectedTests` in `tests/e2e/conformance_test.go` in alphabetical order.
- **Key Files Modified**:
  - `pkg/proxy/proxy.go`
  - `pkg/proxy/proxy_test.go`
  - `tests/e2e/conformance_test.go`

## 4. Validation & Results
- **Unit Tests**:
  - `TestProxyWebSocket`: Successfully verified HTTP 101 upgrade and bidirectional payload echo through proxy.
- **Conformance Logs**:
  ```
  --- PASS: TestConformance/HTTPRouteBackendProtocolWebSocket (0.13s)
  ```
