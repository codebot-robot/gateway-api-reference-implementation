# Conformance Test Journal: TLSRouteTerminateSimpleSameNamespace

## 1. Test Overview
- **Name**: `TLSRouteTerminateSimpleSameNamespace`
- **Description**: Verifies that a TLSRoute attaches to a Gateway listener configured with TLS protocol in Terminate mode (`protocol: TLS`, `port: 8443`, `hostname: tls.example.com`, `tls.mode: Terminate`, `certificateRefs: [tls-terminate-checks-certificate]`) and routes traffic to a backend (`tcp-backend:3000`) over plain-text TCP. A client initiates a TLS connection to the Gateway, verifies the Gateway certificate against the suite CA, and the transmitted bytes arrive at the backend decrypted.
- **Manifests**: `tests/tlsroute-terminate-simple-same-namespace.yaml`

## 2. Issue / Failure Analysis
- **Observed Behavior**: TLSRoute only attached to listeners configured with TLS mode Passthrough; Terminate listeners were rejected as protocol-incompatible during route binding. Furthermore, the proxy data plane did not terminate TLS connections for TLSRoutes.
- **Root Cause**:
  1. `bindTLSRouteParentRef` in `pkg/state/compiled.go` restricted protocol compatibility to `el.TLS.Mode == TLSModePassthrough`.
  2. The SNI listener data plane in `pkg/proxy/sni.go` only handled `TLSModePassthrough` (splicing raw bytes to backend) and `HTTPSProtocolType` (passing to HTTPS server), failing to terminate TLS on the connection and splice decrypted TCP streams to plain TCP backends.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Updated `bindTLSRouteParentRef` to allow both `TLSModePassthrough` and `TLSModeTerminate` listeners.
  2. Carried weights and targets through `InternalTLSRule` using `InternalTLSBackend` structs.
  3. Added `terminateTLSToBackend` in `pkg/proxy/sni.go` to terminate TLS on peeked connections for Terminate listeners using `Proxy.GetCertificate`, failing closed if no certificate or backend is found, and splicing the decrypted TCP stream to the backend.
  4. Added `tests.TLSRouteTerminateSimpleSameNamespace` to `selectedTests` in `tests/e2e/conformance_test.go`.
- **Key Files Modified**:
  - `pkg/state/tlsroute.go`
  - `pkg/state/compiled.go`
  - `pkg/state/gateway.go`
  - `pkg/proxy/sni.go`
  - `pkg/proxy/proxy.go`
  - `tests/e2e/conformance_test.go`

## 4. Validation & Results
- **Unit Tests**:
  - `pkg/state/tlsroute_test.go`: `TestTLSRoute_BindsToTerminateListener`
  - `pkg/proxy/sni_test.go`: `TestSNIListener_TerminateAndMixedAndMissingCert`
- **Conformance Logs**: Verified with `ap e2e`.
