# Conformance Test Journal: TLSRouteInvalidNoMatchingListenerHostname

## 1. Test Overview
- **Name**: `TLSRouteInvalidNoMatchingListenerHostname`
- **Description**: Verifies that TLSRoutes whose hostnames do not intersect exact or wildcard Passthrough listeners report `Accepted=False` with reason `NoMatchingListenerHostname`. Neither route has accepted parents, and the Gateways report `attachedRoutes: 0`.
- **Manifests**: `sigs.k8s.io/gateway-api/conformance/tests/tlsroute-invalid-no-matching-listener-hostname.yaml` (v1.6.2)

## 2. Issue / Failure Analysis
- **Observed Behavior**: When hostname intersection between route and listener produces no match, the listener should not accept the route, and `attachedRoutes` must remain 0.
- **Root Cause**: `bindParentRef` checks `IntersectHostnames(cfg.hostnames, string(ValueOf(el.Hostname)))`. If no listener hostnames match while the listener was otherwise allowed, it returns `Accepted=False` with reason `NoMatchingListenerHostname`.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Enabled `tests.TLSRouteInvalidNoMatchingListenerHostname` in `selectedTests` in `tests/e2e/conformance_test.go`.
  2. Existing `bindParentRef` and `IntersectHostnames` already support this requirement properly.
- **Key Files Modified**:
  - `tests/e2e/conformance_test.go`

## 4. Validation & Results
- **Unit Tests**:
  - `pkg/state/tlsroute_test.go`: `TestTLSRoute_NoMatchingListenerHostname`
- **Conformance Logs**: Verified pass under `ap e2e`.
