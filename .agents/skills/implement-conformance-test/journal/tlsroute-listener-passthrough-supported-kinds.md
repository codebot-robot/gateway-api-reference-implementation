# Conformance Test Journal: TLSRouteListenerPassthroughSupportedKinds

## 1. Test Overview
- **Name**: `TLSRouteListenerPassthroughSupportedKinds`
- **Description**: Verifies that a TLS Passthrough listener with `allowedRoutes.kinds: [TCPRoute, TLSRoute]` reports `supportedKinds: [TLSRoute]` (since TCPRoute is unsupported), `ResolvedRefs=False` with reason `InvalidRouteKinds`, and `AttachedRoutes: 0`.
- **Manifests**: `sigs.k8s.io/gateway-api/conformance/tests/tlsroute-listener-passthrough-supported-kinds.yaml` (v1.6.2)

## 2. Issue / Failure Analysis
- **Observed Behavior**: Tested condition generation for TLS listeners when allowed route kinds contain unsupported kinds (such as TCPRoute).
- **Root Cause**: Listeners with `protocol: TLS` support `TLSRoute`, but when an unsupported route kind like `TCPRoute` is specified in `allowedRoutes.kinds`, the listener must filter `supportedKinds` to only valid kinds and mark `ResolvedRefs` condition as `False` with reason `InvalidRouteKinds`.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Verified and ensured `ValidateListener` in `pkg/state/compiled.go` validates allowed route kinds against `isValidRouteKindForProtocol(TLSProtocolType, ...)` which accepts `TLSRoute`.
  2. For invalid kinds like `TCPRoute`, `hasInvalidRouteKind` is flagged, causing `ResolvedRefs=False` with reason `InvalidRouteKinds` and `Programmed=False` with reason `Invalid`.
  3. Added `tests.TLSRouteListenerPassthroughSupportedKinds` to `selectedTests` in `tests/e2e/conformance_test.go`.
- **Key Files Modified**:
  - `pkg/state/compiled.go`
  - `tests/e2e/conformance_test.go`

## 4. Validation & Results
- **Unit Tests**:
  - `pkg/state/tlsroute_test.go`: `TestTLSRoute_SupportedKinds_InvalidRouteKinds`
- **Conformance Logs**: Verified with `ap e2e`.
