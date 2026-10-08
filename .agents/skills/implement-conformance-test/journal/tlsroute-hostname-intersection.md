# Conformance Test Journal: TLSRouteHostnameIntersection

## 1. Test Overview
- **Name**: `TLSRouteHostnameIntersection`
- **Description**: Verifies that Gateways with various TLS listener hostname configurations (exact `abc.example.com`, wildcard `*.example.com`, broader wildcard `*.com`, or empty) correctly intersect hostnames with attached TLSRoutes. Requests are routed by SNI to the route attached to the listener it intersects with, following exact > more specific wildcard > less specific wildcard precedence; non-intersecting SNI connections are refused.
- **Manifests**: `sigs.k8s.io/gateway-api/conformance/tests/tlsroute-hostname-intersection.yaml` (v1.6.2)

## 2. Issue / Failure Analysis
- **Observed Behavior**: TLSRoute routing and hostname intersection was not implemented for TLS Passthrough listeners.
- **Root Cause**:
  1. Need to reuse `IntersectHostnames` for TLSRoutes when binding to TLS listeners.
  2. Need `SelectTLSBackend` on `InternalListener` to resolve SNI according to Gateway API hostname precedence (Exact > longest matching wildcard > catch-all) across intersecting hostnames.
  3. Non-intersecting or unmatched SNI connections must fail closed and be rejected immediately.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Reused `IntersectHostnames` in `bindParentRef` for TLSRoutes, populating `TLSBackends` map per intersecting hostname.
  2. Implemented `SelectTLSBackend` and `SelectTLSBackends` on `InternalListener` honoring Gateway API precedence rules.
  3. Implemented SNI peeking and matching in `pkg/proxy/sni.go`, splicing matching passthrough connections to backends and failing closed for non-matching SNI.
  4. Added `tests.TLSRouteHostnameIntersection` to `selectedTests` in `tests/e2e/conformance_test.go`.
- **Key Files Modified**:
  - `pkg/state/compiled.go`
  - `pkg/state/gateway.go`
  - `pkg/proxy/sni.go`
  - `tests/e2e/conformance_test.go`

## 4. Validation & Results
- **Unit Tests**:
  - `pkg/state/tlsroute_test.go`: `TestTLSRoute_HostnameIntersection_SelectedBackendMatrix`
  - `pkg/state/tlsroute_test.go`: `TestTLSRoute_NoMatchingListenerHostname`
  - `pkg/proxy/sni_test.go`: `TestSNIListener_PassthroughAndHTTPSAndFailClosed`
- **Conformance Logs**: Verified with `ap e2e`.
