# Conformance Test Journal: BackendTLSPolicySANValidation

## 1. Test Overview
- **Name**: `BackendTLSPolicySANValidation`
- **Description**: Verifies BackendTLSPolicy Subject Alternative Name (SAN) validation for backend TLS connections:
  - Supports `validation.subjectAltNames` with `type: Hostname` and `type: URI`, singly and combined.
  - When backend certificate contains a matching SAN, request succeeds (200 OK).
  - When backend certificate has mismatched SANs, handshake fails closed and request returns an HTTP error (502 Bad Gateway).
  - When `subjectAltNames` is specified, `validation.hostname` is used for TLS SNI only, and the certificate is authenticated against the configured SANs rather than the hostname.
- **Manifests**: `tests/backendtlspolicy-san.yaml` (v1.6.2)

## 2. Issue / Failure Analysis
- **Observed Behavior**:
  - `InternalTLSConfig` in `pkg/state` lacked a field for `SubjectAltNames`.
  - In `pkg/proxy`, `buildTransport` and `forwardWebSocket` performed standard Go TLS certificate verification (`InsecureSkipVerify: false`) against `ServerName = Hostname`. It did not inspect `SubjectAltNames`. Mismatched SAN requests therefore incorrectly succeeded because Go only checked that the cert matched `ServerName` and was signed by the CA.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Updated `InternalTLSConfig` in `pkg/state/gateway.go`:
     - Added `SubjectAltNames []gatewayv1.SubjectAltName`.
  2. Updated `resolveBackendTarget` in `pkg/state/httproute.go`:
     - Carried `selectedPolicy.Spec.Validation.SubjectAltNames` into the compiled `InternalTLSConfig`.
  3. Updated `pkg/proxy/proxy.go`:
     - Consolidated TLS client configuration building into `buildBackendTLSConfig` shared by `buildTransport` and `forwardWebSocket`.
     - When `len(backend.TLSConfig.SubjectAltNames) > 0`:
       - Set `ServerName = backend.TLSConfig.Hostname` to preserve SNI.
       - Set `InsecureSkipVerify = true` and defined a custom `VerifyConnection` callback:
         - Verifies certificate chain against the policy's CA cert pool (`rootCAs`) via `leaf.Verify(verifyOpts)` without forcing DNSName matching against `ServerName`.
         - Validates that at least one SAN in `SubjectAltNames` matches the leaf certificate via `verifySubjectAltNames`:
           - `HostnameSubjectAltNameType`: checks `leaf.VerifyHostname(expectedHost)` and case-insensitive equality against `leaf.DNSNames`.
           - `URISubjectAltNameType`: checks exact string match or URI component match (`Scheme`, `Host`, `Path`, `RawQuery`) against `leaf.URIs`.
         - Handshake fails closed if chain verification fails or if no SAN matches.
  4. Added unit test in `pkg/state/compiled_test.go`:
     - `TestBackendTLSPolicy_CompiledInternalTLSConfig_CarriesSubjectAltNames`: checks that compiled `InternalTLSConfig` carries the policy's `subjectAltNames`.
  5. Added unit tests in `pkg/proxy/proxy_test.go`:
     - `TestBackendTLSPolicy_SubjectAltNamesVerification`: using a locally generated CA and certificate, verifies:
       - matching DNS SAN succeeds;
       - matching URI SAN succeeds;
       - mismatched DNS SAN fails handshake;
       - mismatched URI SAN fails handshake;
       - multiple SANs with one match succeeds;
       - multiple SANs with no match fails handshake;
       - certificate matches SAN but not `hostname` still verifies (hostname is SNI only).
  6. Enabled `tests.BackendTLSPolicySANValidation` in `tests/e2e/conformance_test.go`.

## 4. Validation & Results
- **Unit Tests**:
  - `go test -v ./pkg/state/... -run TestBackendTLSPolicy_CompiledInternalTLSConfig_CarriesSubjectAltNames`: PASS
  - `go test -v ./pkg/proxy/... -run TestBackendTLSPolicy_SubjectAltNamesVerification`: PASS
- **Conformance Logs**:
  ```
  --- PASS: TestConformance/BackendTLSPolicySANValidation (12.28s)
      --- PASS: TestConformance/BackendTLSPolicySANValidation/HTTP_request_sent_to_Service_with_valid_BackendTLSPolicy_containing_dns_SAN_should_succeed (0.04s)
      --- PASS: TestConformance/BackendTLSPolicySANValidation/HTTP_request_sent_to_Service_targeted_by_BackendTLSPolicy_with_mismatched_dns_SAN_should_return_an_HTTP_error (0.05s)
      --- PASS: TestConformance/BackendTLSPolicySANValidation/HTTP_request_sent_to_Service_with_valid_BackendTLSPolicy_containing_uri_SAN_should_succeed (0.04s)
      --- PASS: TestConformance/BackendTLSPolicySANValidation/HTTP_request_sent_to_Service_targeted_by_BackendTLSPolicy_with_mismatched_uri_SAN_should_return_an_HTTP_error (0.04s)
      --- PASS: TestConformance/BackendTLSPolicySANValidation/HTTP_request_sent_to_Service_with_valid_BackendTLSPolicy_containing_multi_SAN_should_succeed (0.08s)
      --- PASS: TestConformance/BackendTLSPolicySANValidation/HTTP_request_sent_to_Service_targeted_by_BackendTLSPolicy_with_mismatched_multi_SAN_should_return_an_HTTP_error (0.20s)
  ```
