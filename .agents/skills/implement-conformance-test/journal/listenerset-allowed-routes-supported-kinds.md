# Conformance Test Journal: ListenerSetAllowedRoutesSupportedKinds

## 1. Test Overview
- **Name**: `ListenerSetAllowedRoutesSupportedKinds`
- **Description**: Verifies that a ListenerSet listener with `protocol: TLS`, `mode: Passthrough`, and `allowedRoutes.kinds: [HTTPRoute]` reports `ResolvedRefs=False` with reason `InvalidRouteKinds` and empty `supportedKinds`, while the parent Gateway remains `Accepted=True`.
- **Manifests**: `sigs.k8s.io/gateway-api/conformance/tests/listenerset-allowed-routes-supported-kinds.yaml` (v1.6.2)

## 2. Issue / Failure Analysis
- **Observed Behavior**: For a TLS Passthrough listener, `HTTPRoute` is an unsupported route kind. The listener must report `ResolvedRefs=False` / `InvalidRouteKinds` with empty `supportedKinds`. The parent Gateway listener (HTTP on port 80) is valid, so the parent Gateway must remain accepted.
- **Root Cause**: Listener validation via `ValidateListener` computes `supportedKinds` based on `isValidRouteKindForProtocol`. For TLS listeners, only `TLSRoute` is valid. Unsupported kinds trigger `hasInvalidRouteKind = true`, resulting in `ResolvedRefs=False` / `InvalidRouteKinds` and empty `supportedKinds`. Gateway conditions are computed from the Gateway's own listeners, remaining Accepted=True.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Added unit test `TestListenerSet_AllowedRoutesSupportedKinds_TLSInvalidKinds` in `pkg/state/listenerset_test.go`.
  2. Enabled `tests.ListenerSetAllowedRoutesSupportedKinds` in `selectedTests` in `tests/e2e/conformance_test.go`.
- **Key Files Modified**:
  - `pkg/state/listenerset_test.go`
  - `tests/e2e/conformance_test.go`

## 4. Validation & Results
- **Unit Tests**:
  - `pkg/state/listenerset_test.go`: `TestListenerSet_AllowedRoutesSupportedKinds_TLSInvalidKinds`
- **Conformance Logs**: Verified pass under `ap e2e`.
