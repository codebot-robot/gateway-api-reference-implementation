# Conformance Test Journal: TLSRouteInvalidBackendRefUnknownKind

## 1. Test Overview
- **Name**: `TLSRouteInvalidBackendRefUnknownKind`
- **Description**: Verifies that a TLSRoute with a backendRef targeting an unsupported Group/Kind (e.g. `group: unknownkind.example.com`, `kind: NonExistent`) reports parent condition `ResolvedRefs=False` with reason `InvalidKind`. The data plane must reject TLS connections destined for the route's hostname.
- **Manifests**: `sigs.k8s.io/gateway-api/conformance/tests/tlsroute-invalid-backendref-unknown-kind.yaml` (v1.6.2)

## 2. Issue / Failure Analysis
- **Observed Behavior**: `CompileTLSRoute` checks `backendRef.Group` and `backendRef.Kind`. Unsupported kinds set `ResolvedRefs=False` with reason `InvalidKind`, leaving `rule.Backends` empty.
- **Root Cause**: The compiler must ensure listeners have no backends assigned for this hostname, rejecting TLS connections cleanly in the SNI listener.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Verified `CompileTLSRoute` marks unsupported backend kinds as `ResolvedRefs=False` / `InvalidKind`.
  2. Ensured compiler filters out rules without backends from listener `TLSBackends`.
  3. Added `tests.TLSRouteInvalidBackendRefUnknownKind` to `selectedTests` in `tests/e2e/conformance_test.go`.
- **Key Files Modified**:
  - `pkg/state/compiled.go`
  - `pkg/state/tlsroute_test.go`
  - `tests/e2e/conformance_test.go`

## 4. Validation & Results
- **Unit Tests**:
  - `pkg/state/tlsroute_test.go`: `TestTLSRoute_UnresolvableBackendRef/unknown_backend_kind`
  - `pkg/proxy/sni_test.go`: `TestSNIListener_PassthroughNoResolvableBackendClosed`
- **Conformance Logs**: Verified pass under `ap e2e`.
