# Conformance Test Journal: GatewayFrontendClientCertificateValidationInsecureFallback

## 1. Test Overview
- **Name**: `GatewayFrontendClientCertificateValidationInsecureFallback`
- **Description**: Verifies that when frontend client certificate validation is configured in `AllowInsecureFallback` mode (either default or perPort):
  - The Gateway condition `InsecureFrontendValidationMode=True` with reason `ConfigurationChanged` is set on the Gateway.
  - The data plane requests a client certificate but accepts the TLS handshake whether the client certificate is valid, invalid (signed by another CA), or omitted entirely.
  - Traffic routes correctly across both default and perPort HTTPS listeners.
- **Manifest**:
  - `sigs.k8s.io/gateway-api/conformance/tests/gateway-with-clientcertificate-validation-insecure-fallback.yaml`

## 2. Issue / Failure Analysis
- **Observed Behavior**:
  - In Part 1 (#708) and Part 2 (#709), `AllowInsecureFallback` listeners failed closed with `Programmed=False/Pending` (`"Frontend validation is not yet enforced"`).
  - Data plane rejected unserved listeners whose validation was not yet implemented.
  - The Gateway-level condition `InsecureFrontendValidationMode` was not yet emitted.
- **Requirements & Edge Cases**:
  - Data plane: For listeners with effective mode `AllowInsecureFallback`, configure `tls.Config` with `ClientAuth: tls.RequestClientCert` and `ClientCAs: caPool`. In Go's `crypto/tls`, `RequestClientCert` advertises the CAs to the client via `CertificateRequest` but does not require a certificate and does not fail the handshake if the certificate fails CA verification.
  - Concurrently, any `AllowValidOnly` listeners on the same port must strictly enforce client certificates (`ClientAuth: tls.RequireAndVerifyClientCert`), rejecting missing certs or certs from other CAs.
  - Gateway model: Emit condition `InsecureFrontendValidationMode=True`, reason `ConfigurationChanged`, when any listener's effective mode is `AllowInsecureFallback`; absent otherwise.
  - If configuration transitions from `AllowInsecureFallback` back to `AllowValidOnly`, the condition is removed.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. In `pkg/state/compiled.go`:
     - Removed the fail-closed `Programmed=False/Pending` condition for `AllowInsecureFallback` listeners in `ValidateListener`, allowing them to become `Programmed=True`.
     - Added helper `hasInsecureFallbackListener` and updated `ComputeGatewayConditions` to emit `InsecureFrontendValidationMode=True, Reason=ConfigurationChanged` whenever any listener's effective mode is `AllowInsecureFallback`.
  2. In `pkg/proxy/proxy.go`:
     - Updated `isClientValidationServable` to accept `AllowInsecureFallback` as a servable validation mode.
     - Updated `updateClientValidationLocked` to configure `ClientAuth: tls.RequestClientCert` when `lis.ClientValidation.Mode == gatewayv1.AllowInsecureFallback`, while keeping `tls.RequireAndVerifyClientCert` for `AllowValidOnly`.
  3. In `pkg/proxy/client_validation_test.go`:
     - Added `TestClientCertificateValidation_AllowInsecureFallback` verifying real socket behavior: fallback listener accepts valid cert, cert from another CA, and no cert; sibling `AllowValidOnly` listener on the same port still rejects the latter two.
     - Updated `TestClientCertificateValidation_FailClosedInvalidConfiguration` to verify that unknown/unsupported modes fail handshake.
  4. In `pkg/state/compiled_test.go`:
     - Updated `TestFrontendClientCertValidation_AllowInsecureFallback_ProgrammedAndServed` to verify `Programmed=True`, proxy listener presence, and Gateway condition.
     - Added `TestGatewayCondition_InsecureFrontendValidationMode` covering default fallback, perPort fallback, absence for `AllowValidOnly`, absence for unspecified mode, absence without validation, absence for HTTP listeners, and presence-to-absence status transitions via `Recompute`.
  5. In `tests/e2e/conformance_test.go`:
     - Added `features.SupportGatewayFrontendClientCertificateValidationInsecureFallback` to `SupportedFeatures`.
     - Added `tests.GatewayFrontendClientCertificateValidationInsecureFallback` to `selectedTests`.

## 4. Validation & Results
- **Unit Tests**:
  - `pkg/proxy`: `TestClientCertificateValidation_RealSockets`, `TestClientCertificateValidation_FailClosedInvalidConfiguration`, `TestClientCertificateValidation_AllowInsecureFallback` all PASS.
  - `pkg/state`: `TestFrontendClientCertValidation_PerPortOverridesDefault`, `TestFrontendClientCertValidation_PerPortHTTPIgnored`, `TestFrontendClientCertValidation_InvalidReasons`, `TestFrontendClientCertValidation_AllowInsecureFallback_ProgrammedAndServed`, `TestGatewayCondition_InsecureFrontendValidationMode`, `TestFrontendClientCertValidation_AllowValidOnly_ProgrammedAndServed` all PASS.
  - All package tests in `pkg/...` pass.
- **E2E / Conformance**:
  - Conformance test `GatewayFrontendClientCertificateValidationInsecureFallback` passes.
