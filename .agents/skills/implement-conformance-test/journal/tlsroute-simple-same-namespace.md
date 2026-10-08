# Conformance Test Journal: TLSRouteSimpleSameNamespace

## 1. Test Overview
- **Name**: `TLSRouteSimpleSameNamespace`
- **Description**: Verifies that a TLSRoute with matching hostname and backendRefs attaches to a TLS Passthrough listener (`protocol: TLS`, `port: 443`, `hostname: "*.example.com"`, `tls.mode: Passthrough`, `allowedRoutes.kinds: [TLSRoute]`). A TLS connection with that SNI reaches the backend unterminated, presenting the backend's own certificate. The TLSRoute must report Accepted=True and ResolvedRefs=True for the parent.
- **Manifests**: `sigs.k8s.io/gateway-api/conformance/tests/tlsroute-simple-same-namespace.yaml` (v1.6.2)

## 2. Issue / Failure Analysis
- **Observed Behavior**: TLSRoute support was not yet implemented in the reference implementation. TLS listeners on port 443 did not route passthrough traffic to backends.
- **Root Cause**:
  1. `TLSRoute` was not included in `State` inputs or informer event handlers.
  2. Protocol compatibility in `pkg/state/compiled.go` did not allow TLS and HTTPS listeners to share port 443/8443.
  3. The data plane did not peek SNI on port 8443 to distinguish between HTTPS and TLS Passthrough connections.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Implemented `TLSRoute` in `pkg/state`: model inputs, `TLSRouteState`, compilation with backend reference resolution and validation, parentRef binding reusing `bindParentRef` and `IntersectHostnames`, status computation, and reconciler event sourcing.
  2. Updated `areProtocolsCompatible` to allow HTTPS and TLS listeners to share a port routed by SNI.
  3. Added `NewSNIListener` acceptor to `pkg/proxy` to peek ClientHello SNI without consuming it, matching the most specific listener; TLS Passthrough connections are spliced directly to resolved backend targets, while HTTPS connections are adapted to the HTTPS server.
  4. Updated `pkg/provisioning` singlepod address provider to map effective listeners with `protocol: TLS` to the 8443 TCP target.
  5. Added RBAC in `k8s/controller.yaml` for `tlsroutes` and `tlsroutes/status`.
  6. Added `tests.TLSRouteSimpleSameNamespace` to `selectedTests` in `tests/e2e/conformance_test.go`.
- **Key Files Modified**:
  - `pkg/state/tlsroute.go`
  - `pkg/state/compiled.go`
  - `pkg/state/state.go`
  - `pkg/controller/tlsroute_controller.go`
  - `pkg/controller/utils.go`
  - `pkg/proxy/sni.go`
  - `pkg/gari/gari.go`
  - `pkg/provisioning/singlepod/address_provider.go`
  - `k8s/controller.yaml`
  - `tests/e2e/conformance_test.go`

## 4. Validation & Results
- **Unit Tests**:
  - `pkg/state/tlsroute_test.go`: `TestTLSRoute_AttachedRoutes`
  - `pkg/proxy/sni_test.go`: `TestSNIListener_PassthroughAndHTTPSAndFailClosed`
- **Conformance Logs**: Verified with `ap e2e`.
