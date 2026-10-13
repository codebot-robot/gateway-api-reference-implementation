# Conformance Test Journal: GatewayFrontendInvalidDefaultClientCertificateValidation

## 1. Test Overview
- **Name**: `GatewayFrontendInvalidDefaultClientCertificateValidation`
- **Description**: Verifies that when frontend client certificate validation is configured on a Gateway (`spec.tls.frontend.default.validation`) with an invalid/nonexistent ConfigMap CA certificate ref:
  1. The invalid configuration does not impact non-HTTPS listeners (such as an HTTP listener on port 80), which remain Accepted=True, Programmed=True, and continue serving traffic.
  2. The HTTPS listener has its conditions updated accordingly:
     - `ResolvedRefs=False`, `Reason=InvalidCACertificateRef`
     - `Accepted=False`, `Reason=NoValidCACertificate`
     - `Programmed=False`
  3. Traffic to the HTTPS listener fails closed and is not served.
- **Manifest**:
  - `sigs.k8s.io/gateway-api/conformance/tests/gateway-invalid-default-frontend-client-certificate-validation.yaml`

## 2. Issue / Failure Analysis
- **Observed Behavior**:
  - Frontend client certificate validation (`Gateway.spec.tls.frontend`) was not previously modeled or resolved in `pkg/state`.
- **Requirements & Edge Cases**:
  - Frontend client certificate validation applies strictly to HTTPS listeners. Non-HTTPS listeners (e.g. HTTP) must ignore frontend validation configs (including any `perPort` entries matching their port).
  - Effective validation resolution: `perPort` configuration for the listener port overrides `default`. If no `perPort` entry matches, fallback to `default`.
  - CA references refer to ConfigMaps containing a `ca.crt` key (same parsing as BackendTLSPolicy).
  - Cross-namespace ConfigMap references require a `ReferenceGrant` from `Gateway` to `ConfigMap`.
  - Condition reasons:
    - Nonexistent ConfigMap: `ResolvedRefs=False`/`InvalidCACertificateRef`
    - Non-ConfigMap kind: `ResolvedRefs=False`/`InvalidCACertificateKind`
    - Cross-namespace without grant: `ResolvedRefs=False`/`RefNotPermitted`
    - In each invalid case: `Accepted=False`/`NoValidCACertificate` and `Programmed=False`/`NoValidCACertificate`
  - Fail closed in Part 1: While client certificate enforcement in the data plane is implemented in Part 2 (#709), listeners whose frontend validation resolves successfully must fail closed (kept out of proxy listeners and reported with `Programmed=False`, `Reason=Pending`, message `"Frontend validation is not yet enforced"`).

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Extracted helper `ExtractCACertFromConfigMap(cm *corev1.ConfigMap) ([]byte, error)` in `pkg/state/utils.go` and reused it in `BackendTLSPolicy` validation in `pkg/state/httproute.go` and `pkg/state/compiled.go`.
  2. Defined `InternalClientValidation` in `pkg/state/gateway.go` and attached `ClientValidation *InternalClientValidation` to `InternalListener` and `EffectiveListener`.
  3. Implemented `EffectiveFrontendValidation` in `pkg/state/compiled.go` to resolve effective validation for HTTPS listeners with `perPort` overriding `default`.
  4. Updated `ValidateListener` and `BuildEffectiveListener` to validate frontend CA ConfigMap references (checking kinds, ReferenceGrants, ConfigMap existence and `ca.crt` PEM validity), setting corresponding listener conditions, and failing closed with `Programmed=False/Pending` when valid.
  5. Updated `BuildProxyConfig` and `ExtractCertificates` to omit listeners with frontend client certificate validation in Part 1.
  6. Added unit tests in `pkg/state/compiled_test.go` and `pkg/state/dataplane_test.go` covering all required scenarios.
  7. Enabled `features.SupportGatewayFrontendClientCertificateValidation` and `tests.GatewayFrontendInvalidDefaultClientCertificateValidation` in `tests/e2e/conformance_test.go`.
- **Key Files Modified / Added**:
  - `pkg/state/utils.go`
  - `pkg/state/gateway.go`
  - `pkg/state/httproute.go`
  - `pkg/state/compiled.go`
  - `pkg/state/compiled_test.go`
  - `pkg/state/dataplane_test.go`
  - `tests/e2e/conformance_test.go`
  - `.agents/skills/implement-conformance-test/journal/gateway-frontend-invalid-default-client-certificate-validation.md`

## 4. Validation & Results
- **Unit Tests**:
  - `TestFrontendClientCertValidation_PerPortOverridesDefault`: PASS
  - `TestFrontendClientCertValidation_PerPortHTTPIgnored`: PASS
  - `TestFrontendClientCertValidation_InvalidReasons`: PASS
  - `TestFrontendClientCertValidation_ValidFailClosedAndInternalListener`: PASS
  - `TestDataplaneConfig_Roundtrip_ClientValidation`: PASS
  - All unit tests in `pkg/...` pass.
- **Conformance Logs**:
  ```
  --- PASS: TestConformance (148.41s)
      --- PASS: TestConformance/GatewayFrontendInvalidDefaultClientCertificateValidation (10.20s)
          --- PASS: TestConformance/GatewayFrontendInvalidDefaultClientCertificateValidation/Validate_tls_configuration_does_not_impact_HTTP_listener (2.95s)
          --- PASS: TestConformance/GatewayFrontendInvalidDefaultClientCertificateValidation/Validate_that_invalid_default_configuration_is_impacting_HTTPS_listener (0.04s)
  ```
