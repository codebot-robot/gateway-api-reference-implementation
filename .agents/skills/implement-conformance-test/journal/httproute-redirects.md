# Conformance Test Journal: HTTPRoute Redirects

## 1. Test Overview
- **Name**: `HTTPRouteRedirects` (`HTTPRoute303Redirect`, `HTTPRoute308Redirect`, `HTTPRouteRedirectHostAndStatus`, `HTTPRouteRedirectPath`, `HTTPRouteRedirectPort`, `HTTPRouteRedirectScheme`, `HTTPRouteRedirectPortAndScheme`)
- **Description**: Verifies that HTTPRoutes with `RequestRedirect` filters handle all redirect combinations according to the Gateway API specification:
  - Custom status codes (301, 302 default, 303, 307, 308)
  - Hostname overrides
  - Path modifications (`ReplaceFullPath` and `ReplacePrefixMatch`)
  - Port overrides and well-known port omission rules (e.g. port 80 for HTTP, port 443 for HTTPS)
  - Scheme overrides and scheme-based default port derivation
  - Multi-listener redirects across ports 80, 8080, and 443 (HTTPS with TLS certificates)
- **Manifests**:
  - `sigs.k8s.io/gateway-api/conformance/tests/httproute-303-redirect.yaml`
  - `sigs.k8s.io/gateway-api/conformance/tests/httproute-308-redirect.yaml`
  - `sigs.k8s.io/gateway-api/conformance/tests/httproute-redirect-host-and-status.yaml`
  - `sigs.k8s.io/gateway-api/conformance/tests/httproute-redirect-path.yaml`
  - `sigs.k8s.io/gateway-api/conformance/tests/httproute-redirect-port.yaml`
  - `sigs.k8s.io/gateway-api/conformance/tests/httproute-redirect-scheme.yaml`
  - `sigs.k8s.io/gateway-api/conformance/tests/httproute-redirect-port-and-scheme.yaml`

## 2. Issue / Failure Analysis
- **Observed Behavior**:
  1. Conformance tests for 303, 308, RedirectHostAndStatus, RedirectPath, RedirectPort, RedirectScheme, and RedirectPortAndScheme were not included in `selectedTests` in `tests/e2e/conformance_test.go`.
  2. The proxy redirect implementation did not follow the Gateway API port derivation rules when `scheme` was changed without an explicit `port` (or when standard ports 80/443 were specified), resulting in redundant or incorrect ports in the `Location` header.
  3. The proxy service (`gari-proxy`) did not expose port 8080 needed for Gateway listeners running on port 8080.
  4. The HTTPS proxy server used a static self-signed certificate rather than dynamically serving TLS certificates from Kubernetes `Secret` resources referenced by Gateway HTTPS listeners (`tls.certificateRefs`).

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Updated `pkg/proxy/proxy.go` `redirect` method:
     - Followed Gateway API port derivation rules: if `redirect.Scheme` is specified and `redirect.Port` is nil, derived well-known port (80 for http, 443 for https). If `redirect.Scheme` is empty, used the Gateway listener port from the request.
     - Stripped port from `Location` header for standard HTTP on port 80 and HTTPS on port 443.
  2. Added port 8080 to `gari-proxy` service in `k8s/controller.yaml`.
  3. Added `Secret` tracking to state (`pkg/state/state.go`), created `SecretReconciler` (`pkg/controller/secret_controller.go`), updated RBAC in `k8s/controller.yaml`, validated listener `CertificateRefs` in `pkg/controller/gateway_controller.go` (setting `ResolvedRefs` to `ConditionFalse` with `InvalidCertificateRef` / `RefNotPermitted` on errors), and updated proxy TLS handling (`pkg/controller/utils.go`, `pkg/proxy/proxy.go`, `cmd/gateway-api-reference-implementation/main.go`) to dynamically serve certificates referenced by HTTPS listeners via `tls.Config.GetCertificate`.
  4. Added comprehensive unit tests in `pkg/proxy/proxy_test.go` and `pkg/state/gateway_test.go`.
  5. Added `HTTPRoute303Redirect`, `HTTPRoute308Redirect`, `HTTPRouteRedirectHostAndStatus`, `HTTPRouteRedirectPath`, `HTTPRouteRedirectPort`, `HTTPRouteRedirectScheme`, and `HTTPRouteRedirectPortAndScheme` to `selectedTests` in `tests/e2e/conformance_test.go`.
  6. Verified all tests passing with `ap generate`, `ap test`, `ap lint`, and `ap e2e`.
- **Key Files Modified**:
  - `pkg/proxy/proxy.go`
  - `pkg/proxy/proxy_test.go`
  - `pkg/state/state.go`
  - `pkg/state/gateway_test.go`
  - `pkg/controller/secret_controller.go`
  - `pkg/controller/utils.go`
  - `cmd/gateway-api-reference-implementation/main.go`
  - `k8s/controller.yaml`
  - `tests/e2e/conformance_test.go`
  - `.agents/skills/implement-conformance-test/journal/httproute-redirects.md`

## 4. Validation & Results
- **Unit Tests**: Passed with `ap test` (all unit tests for proxy redirect logic and state building passing).
- **Static Analysis**: Passed with `ap lint`.
- **Conformance Logs**:
  ```
  --- PASS: TestConformance (129.71s)
      --- PASS: TestConformance/HTTPRoute303Redirect (0.02s)
          --- PASS: TestConformance/HTTPRoute303Redirect/0_request_to_'/see-other'_should_receive_one_of_[] (0.00s)
      --- PASS: TestConformance/HTTPRoute307Redirect (0.02s)
          --- PASS: TestConformance/HTTPRoute307Redirect/0_request_to_'/temporary'_should_receive_one_of_[] (0.00s)
      --- PASS: TestConformance/HTTPRoute308Redirect (0.02s)
          --- PASS: TestConformance/HTTPRoute308Redirect/0_request_to_'/permanent'_should_receive_one_of_[] (0.00s)
      --- PASS: TestConformance/HTTPRouteRedirectHostAndStatus (0.02s)
          --- PASS: TestConformance/HTTPRouteRedirectHostAndStatus/1_request_to_'/host-and-status'_should_receive_one_of_[] (0.00s)
          --- PASS: TestConformance/HTTPRouteRedirectHostAndStatus/0_request_to_'/hostname-redirect'_should_receive_one_of_[] (0.00s)
      --- PASS: TestConformance/HTTPRouteRedirectPath (0.12s)
          --- PASS: TestConformance/HTTPRouteRedirectPath/5_request_to_'/full-path-and-status'_should_receive_one_of_[] (0.00s)
          --- PASS: TestConformance/HTTPRouteRedirectPath/1_request_to_'/full/path/original'_should_receive_one_of_[] (0.00s)
          --- PASS: TestConformance/HTTPRouteRedirectPath/0_request_to_'/original-prefix/lemon'_should_receive_one_of_[] (0.00s)
          --- PASS: TestConformance/HTTPRouteRedirectPath/3_request_to_'/path-and-status'_should_receive_one_of_[] (0.00s)
          --- PASS: TestConformance/HTTPRouteRedirectPath/2_request_to_'/path-and-host'_should_receive_one_of_[] (0.00s)
          --- PASS: TestConformance/HTTPRouteRedirectPath/4_request_to_'/full-path-and-host'_should_receive_one_of_[] (0.00s)
      --- PASS: TestConformance/HTTPRouteRedirectPort (0.12s)
          --- PASS: TestConformance/HTTPRouteRedirectPort/0_request_to_'/port'_should_receive_one_of_[] (0.00s)
          --- PASS: TestConformance/HTTPRouteRedirectPort/3_request_to_'/port-and-host-and-status'_should_receive_one_of_[] (0.00s)
          --- PASS: TestConformance/HTTPRouteRedirectPort/1_request_to_'/port-and-host'_should_receive_one_of_[] (0.00s)
          --- PASS: TestConformance/HTTPRouteRedirectPort/2_request_to_'/port-and-status'_should_receive_one_of_[] (0.00s)
      --- PASS: TestConformance/HTTPRouteRedirectScheme (0.12s)
          --- PASS: TestConformance/HTTPRouteRedirectScheme/3_request_to_'/scheme-and-host-and-status'_should_receive_one_of_[] (0.00s)
          --- PASS: TestConformance/HTTPRouteRedirectScheme/0_request_to_'/scheme'_should_receive_one_of_[] (0.00s)
          --- PASS: TestConformance/HTTPRouteRedirectScheme/2_request_to_'/scheme-and-status'_should_receive_one_of_[] (0.00s)
          --- PASS: TestConformance/HTTPRouteRedirectScheme/1_request_to_'/scheme-and-host'_should_receive_one_of_[] (0.00s)
      --- PASS: TestConformance/HTTPRouteRedirectPortAndScheme (0.06s)
          --- PASS: TestConformance/HTTPRouteRedirectPortAndScheme/http-listener-on-8080/0_request_to_'/scheme-nil-and-port-nil'_should_receive_one_of_[] (0.00s)
          --- PASS: TestConformance/HTTPRouteRedirectPortAndScheme/http-listener-on-8080/1_request_to_'/scheme-nil-and-port-80'_should_receive_one_of_[] (0.00s)
          --- PASS: TestConformance/HTTPRouteRedirectPortAndScheme/http-listener-on-80/3_request_to_'/scheme-https-and-port-nil'_should_receive_one_of_[] (0.00s)
          --- PASS: TestConformance/HTTPRouteRedirectPortAndScheme/http-listener-on-80/1_request_to_'/scheme-nil-and-port-80'_should_receive_one_of_[] (0.01s)
          --- PASS: TestConformance/HTTPRouteRedirectPortAndScheme/http-listener-on-80/5_request_to_'/scheme-https-and-port-8443'_should_receive_one_of_[] (0.01s)
          --- PASS: TestConformance/HTTPRouteRedirectPortAndScheme/http-listener-on-8080/2_request_to_'/scheme-https-and-port-nil'_should_receive_one_of_[] (0.01s)
          --- PASS: TestConformance/HTTPRouteRedirectPortAndScheme/http-listener-on-80/0_request_to_'/scheme-nil-and-port-nil'_should_receive_one_of_[] (0.01s)
          --- PASS: TestConformance/HTTPRouteRedirectPortAndScheme/http-listener-on-80/2_request_to_'/scheme-nil-and-port-8080'_should_receive_one_of_[] (0.01s)
          --- PASS: TestConformance/HTTPRouteRedirectPortAndScheme/http-listener-on-80/4_request_to_'/scheme-https-and-port-443'_should_receive_one_of_[] (0.01s)
          --- PASS: TestConformance/HTTPRouteRedirectPortAndScheme/https-listener-on-443/1_request_to_'example.org/scheme-nil-and-port-443'_should_receive_one_of_[] (0.01s)
          --- PASS: TestConformance/HTTPRouteRedirectPortAndScheme/https-listener-on-443/4_request_to_'example.org/scheme-http-and-port-80'_should_receive_one_of_[] (0.01s)
          --- PASS: TestConformance/HTTPRouteRedirectPortAndScheme/https-listener-on-443/0_request_to_'example.org/scheme-nil-and-port-nil'_should_receive_one_of_[] (0.01s)
          --- PASS: TestConformance/HTTPRouteRedirectPortAndScheme/https-listener-on-443/5_request_to_'example.org/scheme-http-and-port-8080'_should_receive_one_of_[] (0.01s)
          --- PASS: TestConformance/HTTPRouteRedirectPortAndScheme/https-listener-on-443/3_request_to_'example.org/scheme-http-and-port-nil'_should_receive_one_of_[] (0.01s)
          --- PASS: TestConformance/HTTPRouteRedirectPortAndScheme/https-listener-on-443/2_request_to_'example.org/scheme-nil-and-port-8443'_should_receive_one_of_[] (0.01s)
  PASS
  ok      github.com/gke-labs/gateway-api-reference-implementation/tests/e2e     129.71s
  ```
