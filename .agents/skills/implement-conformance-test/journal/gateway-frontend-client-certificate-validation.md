# Conformance Test Journal: GatewayFrontendClientCertificateValidation

## 1. Test Overview
- **Name**: `GatewayFrontendClientCertificateValidation` & `GatewayInvalidFrontendClientCertificateValidation`
- **Description**:
  - `GatewayFrontendClientCertificateValidation`: Verifies that frontend client certificate validation in `AllowValidOnly` mode is enforced in the data plane:
    - Default configuration (CA from `spec.tls.frontend.default.validation.caCertificateRefs` on port 443): client cert signed by default CA succeeds; client cert signed by another CA fails the TLS handshake.
    - Per-port configuration (CA from `spec.tls.frontend.perPort[].tls.validation.caCertificateRefs` on port 8443 for `second-example.org`): client cert signed by per-port CA succeeds; client cert signed by default CA fails the TLS handshake.
  - `GatewayInvalidFrontendClientCertificateValidation`: Verifies that listeners with valid frontend client certificate validation are fully `Accepted=True`, `Programmed=True`, and `ResolvedRefs=True`, while invalid listeners fail with appropriate status conditions.
- **Manifests**:
  - `sigs.k8s.io/gateway-api/conformance/tests/gateway-with-clientcertificate-validation.yaml`
  - `sigs.k8s.io/gateway-api/conformance/tests/gateway-with-invalid-clientcertificate-validation.yaml`

## 2. Issue / Failure Analysis
- **Observed Behavior**:
  - In Part 1 (#708 / #713), effective frontend validation configuration was modeled and attached to `InternalListener`, but data plane enforcement was not yet implemented. Listeners with frontend client certificate validation were held unserved with `Programmed=False`, `Reason=Pending`.
- **Requirements & Edge Cases**:
  - On `UpdateConfig` / `UpdateListeners`, build an immutable per-listener `*tls.Config` set: `ClientAuth: RequireAndVerifyClientCert` with `ClientCAs` populated from the listener's CA PEMs.
  - Atomically store and swap the snapshot via `atomic.Pointer`; no per-handshake map building or locking.
  - `GetConfigForClient`: Select the config by SNI using the same `MatchListeners` precedence as the HTTPS request path.
  - In Kubernetes, all HTTPS ports collapse onto 8443 behind the Service, so per-port CA pools are distinguished by hostname; noted via comment in `sni.go` and `proxy.go`.
  - Listeners with resolved validation (`AllowValidOnly`) are now served and report `Programmed=True`. Listeners with no validation are unchanged.
  - `AllowInsecureFallback` remains unserved with `Pending` status until Part 3.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Updated `ValidateListener` in `pkg/state/compiled.go`: removed the fail-closed `Pending` condition for `AllowValidOnly` listeners, setting `Programmed=True`. Retained `Programmed=False/Pending` specifically for `AllowInsecureFallback`.
  2. Updated `ExtractCertificates` and `BuildProxyConfig` in `pkg/state/compiled.go`: removed the check skipping listeners with `ClientValidation != nil`, allowing programmed listeners to have certificates extracted and to be served by the proxy.
  3. In `pkg/proxy/proxy.go`:
     - Added `clientValidationConfig` containing `listeners []state.InternalListener` and `tlsConfigs map[string]*tls.Config`.
     - Added `updateClientValidationLocked` called during `UpdateConfig` and `UpdateListeners` to atomically build and store the snapshot.
     - Implemented `GetConfigForClient` on `Proxy` to select the per-listener `*tls.Config` by SNI using `state.MatchListeners`.
     - Provided `p.TLSConfig()` helper wiring both `GetCertificate` and `GetConfigForClient`.
     - In `ServeHTTP`, supported requests where port is omitted in Host header by matching against HTTPS listeners.
  4. In `pkg/proxy/sni.go`:
     - Added documentation comment noting that all HTTPS ports collapse onto 8443 behind the Service, so per-port CA pools are distinguished by hostname.
  5. In `pkg/gari/gari.go`:
     - Updated `newTLSConfig()` to use `s.proxy.TLSConfig()`.
  6. Added real-socket tests in `pkg/proxy/client_validation_test.go` covering all acceptance criteria.
  7. Enabled `tests.GatewayFrontendClientCertificateValidation` and `tests.GatewayInvalidFrontendClientCertificateValidation` in `tests/e2e/conformance_test.go`.

## 4. Validation & Results
- **Unit Tests**:
  - `pkg/proxy`: `TestClientCertificateValidation_RealSockets` (valid cert accepted, cert from another CA rejected, no cert rejected, distinct CAs distinguished by SNI on same port, unvalidated HTTP and HTTPS listeners untouched, dynamic CA swap).
  - `pkg/state`: `TestFrontendClientCertValidation_PerPortOverridesDefault`, `TestFrontendClientCertValidation_InvalidReasons`, `TestFrontendClientCertValidation_ValidFailClosedAndInternalListener`, `TestFrontendClientCertValidation_AllowValidOnly_ProgrammedAndServed`.
  - All unit tests in `pkg/...` pass.
- **Conformance Logs**:
  ```
  --- PASS: TestConformance (64.11s)
      --- PASS: TestConformance/GatewayInvalidFrontendClientCertificateValidation (2.02s)
          --- PASS: TestConformance/GatewayInvalidFrontendClientCertificateValidation/Validate_status_for_invalid_client_certificate_configuration (2.00s)
              --- PASS: TestConformance/GatewayInvalidFrontendClientCertificateValidation/Validate_status_for_invalid_client_certificate_configuration/default_valid_configuration (0.60s)
              --- PASS: TestConformance/GatewayInvalidFrontendClientCertificateValidation/Validate_status_for_invalid_client_certificate_configuration/unresolved_reference (0.60s)
              --- PASS: TestConformance/GatewayInvalidFrontendClientCertificateValidation/Validate_status_for_invalid_client_certificate_configuration/invalid_kind (0.60s)
              --- PASS: TestConformance/GatewayInvalidFrontendClientCertificateValidation/Validate_status_for_invalid_client_certificate_configuration/reference_grant_missing (0.20s)
      --- PASS: TestConformance/GatewayFrontendClientCertificateValidation (2.69s)
          --- PASS: TestConformance/GatewayFrontendClientCertificateValidation/Validate_default_configuration (1.03s)
          --- PASS: TestConformance/GatewayFrontendClientCertificateValidation/Validate_per_port_configuration (0.03s)
      --- PASS: TestConformance/GatewayFrontendInvalidDefaultClientCertificateValidation (3.64s)
          --- PASS: TestConformance/GatewayFrontendInvalidDefaultClientCertificateValidation/Validate_tls_configuration_does_not_impact_HTTP_listener (2.82s)
          --- PASS: TestConformance/GatewayFrontendInvalidDefaultClientCertificateValidation/Validate_that_invalid_default_configuration_is_impacting_HTTPS_listener (0.00s)
  ```
