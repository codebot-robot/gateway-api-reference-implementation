# Conformance Test Journal: TLSRouteInvalidBackendRefNonexistent

## 1. Test Overview
- **Name**: `TLSRouteInvalidBackendRefNonexistent`
- **Description**: Verifies that a TLSRoute referencing a nonexistent backend Service has parent status with condition `ResolvedRefs=False` and reason `BackendNotFound`. Additionally, verifies that incoming TLS connections for the route's hostname are rejected by the data plane (connection closed, fail-closed behavior).
- **Manifests**: `sigs.k8s.io/gateway-api/conformance/tests/tlsroute-invalid-backendref-nonexistent.yaml` (v1.6.2)

## 2. Issue / Failure Analysis
- **Observed Behavior**: `CompileTLSRoute` detects missing services and sets `ResolvedRefs=False` / `BackendNotFound`. However, to ensure clean data plane fail-closed semantics, listeners must not register empty backend slices when no backends resolve.
- **Root Cause**: When a route rule has unresolvable backends, `rule.Backends` is empty. The compiler must ensure that empty slices do not register hostnames with non-resolvable backends in `TLSBackends` or leak into `InternalListener`.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Updated `onBind` in `bindTLSRouteParentRef` and `EffectiveListener.ToInternalListener` in `pkg/state/compiled.go` to ensure only rules with non-empty resolved backends are added to `TLSBackends`.
  2. Verified SNI acceptor in `pkg/proxy/sni.go` immediately closes client connections when a Passthrough listener has no resolved backend for the requested SNI.
  3. Added `tests.TLSRouteInvalidBackendRefNonexistent` to `selectedTests` in `tests/e2e/conformance_test.go`.
- **Key Files Modified**:
  - `pkg/state/compiled.go`
  - `pkg/state/tlsroute_test.go`
  - `tests/e2e/conformance_test.go`

## 4. Validation & Results
- **Unit Tests**:
  - `pkg/state/tlsroute_test.go`: `TestTLSRoute_UnresolvableBackendRef`
  - `pkg/proxy/sni_test.go`: `TestSNIListener_PassthroughNoResolvableBackendClosed`
- **Conformance Logs**: Verified pass under `ap e2e`.
