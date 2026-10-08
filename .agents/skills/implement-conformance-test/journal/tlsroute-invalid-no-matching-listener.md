# Conformance Test Journal: TLSRouteInvalidNoMatchingListener

## 1. Test Overview
- **Name**: `TLSRouteInvalidNoMatchingListener`
- **Description**: Verifies that a TLSRoute attached to an HTTP listener or an HTTPS (Terminate) listener is rejected with `Accepted=False` and reason `NotAllowedByListeners`. A parentRef specifying a non-existent `sectionName` on a Passthrough Gateway is rejected with `Accepted=False` and reason `NoMatchingParent`. In all cases, the routes have no accepted parents and the Gateways report `attachedRoutes: 0`.
- **Manifests**: `sigs.k8s.io/gateway-api/conformance/tests/tlsroute-invalid-no-matching-listener.yaml` (v1.6.2)

## 2. Issue / Failure Analysis
- **Observed Behavior**: TLSRoute parent binding must only match listeners whose protocol is TLS with Passthrough mode. Mismatched protocols result in `NotAllowedByListeners`, and missing section names result in `NoMatchingParent`.
- **Root Cause**: `bindTLSRouteParentRef` configures `isProtocolCompatible` to require `el.Protocol == TLS && el.TLS.Mode == Passthrough`. If candidate listeners match by section/port but fail protocol compatibility, `hasMatchingListener` is set, yielding `NotAllowedByListeners`. When the section name is not found on any listener, `hasMatchingListener` remains false, yielding `NoMatchingParent`.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Added unit test in `pkg/state/tlsroute_test.go` verifying `NotAllowedByListeners` for HTTP and HTTPS listeners, and `NoMatchingParent` for invalid `sectionName`.
  2. Enabled `tests.TLSRouteInvalidNoMatchingListener` in `tests/e2e/conformance_test.go`.
- **Key Files Modified**:
  - `pkg/state/tlsroute_test.go`
  - `tests/e2e/conformance_test.go`

## 4. Validation & Results
- **Unit Tests**:
  - `pkg/state/tlsroute_test.go`: `TestTLSRoute_InvalidNoMatchingListener`
- **Conformance Logs**: Verified pass under `ap e2e`.
