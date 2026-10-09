# Conformance Test Journal: TLSRouteMixedTerminationSameNamespace

## 1. Test Overview
- **Name**: `TLSRouteMixedTerminationSameNamespace`
- **Description**: Verifies that a single Gateway with two TLS listeners on the same port (8883) with different modes—one Terminate listener (`tls-terminate`, `hostname: tls.example.com`, with certificate) routing to `tcp-backend:3000` (plain text), and one Passthrough listener (`tls-passthrough`, `hostname: abc.example.com`) routing to `tcp-backend:8443` (TLS)—coexists without conflicts, routes connections by SNI, and correctly terminates or passes through traffic.
- **Manifests**: `tests/tlsroute-mixed-termination-same-namespace.yaml`

## 2. Issue / Failure Analysis
- **Observed Behavior**: TLSRoutes could not bind to Terminate listeners and the proxy could not terminate TLS traffic for TLSRoute listeners.
- **Root Cause**:
  1. `bindTLSRouteParentRef` disallowed binding to non-Passthrough TLS listeners.
  2. Data plane SNI handling lacked the Terminate path for TLS listeners.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Updated `bindTLSRouteParentRef` to allow both `TLSModePassthrough` and `TLSModeTerminate` listeners.
  2. Verified `areProtocolsCompatible` permits co-existence of TLS listeners with distinct hostnames on the same port without ProtocolConflict or HostnameConflict.
  3. Added `terminateTLSToBackend` in `pkg/proxy/sni.go` to handle `TLSModeTerminate` listeners by resolving certificates via `Proxy.GetCertificate`, performing TLS handshakes, and splicing the decrypted byte stream to plain-text TCP backends, while `TLSModePassthrough` continues raw byte splicing.
  4. Added `tests.TLSRouteMixedTerminationSameNamespace` to `selectedTests` in `tests/e2e/conformance_test.go`.
- **Key Files Modified**:
  - `pkg/state/tlsroute.go`
  - `pkg/state/compiled.go`
  - `pkg/state/gateway.go`
  - `pkg/proxy/sni.go`
  - `tests/e2e/conformance_test.go`

## 4. Validation & Results
- **Unit Tests**:
  - `pkg/state/tlsroute_test.go`: `TestTLSRoute_MixedTerminationGateway`
  - `pkg/proxy/sni_test.go`: `TestSNIListener_TerminateAndMixedAndMissingCert`
- **Conformance Logs**: Verified with `ap e2e`.
