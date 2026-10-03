# Conformance Test Journal: HTTPRouteRetry

## 1. Test Overview
- **Name**: `HTTPRouteRetry`
- **Description**: Verifies that an HTTPRoute rule configured with a Retry policy (`retry.attempts`, `retry.codes`, `retry.backoff`) retries failed requests according to specified status codes and attempt limits, returning a successful response when the backend recovers within the retry budget and returning the original error when retries are exceeded.
- **Manifests**:
  - `sigs.k8s.io/gateway-api/conformance/tests/httproute-retry.yaml`

## 2. Issue / Failure Analysis
- **Observed Behavior**: The test was not yet enabled in `tests/e2e/conformance_test.go`. The reference implementation did not parse or store `HTTPRouteRule.Retry` into internal rule state, and the proxy did not implement retry loops. Additionally, the standard Gateway API CRD bundle (`standard-install.yaml`) omitted extended/experimental fields such as `retry` in `HTTPRouteRule`, causing Kubernetes apiserver to prune the retry configuration during manifests application.
- **Root Cause**:
  1. Missing data model `InternalRetry`, parser `ParseRetry()`, and validation for retry backoff duration in `pkg/state`.
  2. Missing retry loop in `Proxy.forward()` in `pkg/proxy/proxy.go` with request body buffering and response body draining between attempts.
  3. `harness.InstallGatewayAPI()` used `standard-install.yaml` rather than `experimental-install.yaml` with server-side apply.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Defined `InternalRetry` and `ParseRetry()` in `pkg/state/gateway.go`.
  2. Populated `Retry` on `InternalRule` during `CompileHTTPRoute()` in `pkg/state/httproute.go`, validating backoff duration and setting error condition if invalid.
  3. Updated `Proxy.forward()` in `pkg/proxy/proxy.go` to retry requests matching configured status codes up to `retry.Attempts` times, honoring `retry.Backoff` delays, buffering request bodies for replays, and draining responses before closing.
  4. Updated test harness in `tests/e2e/harness.go` to install Gateway API CRDs with `--server-side --force-conflicts` using `experimental-install.yaml`.
  5. Added unit tests `TestHTTPRouteValidate_Retry` in `pkg/state/httproute_test.go` and `TestProxyRetries` in `pkg/proxy/proxy_test.go`.
  6. Added `tests.HTTPRouteRetry` to `selectedTests` in `tests/e2e/conformance_test.go` in alphabetical order.
- **Key Files Modified**:
  - `pkg/state/gateway.go`
  - `pkg/state/httproute.go`
  - `pkg/state/httproute_test.go`
  - `pkg/proxy/proxy.go`
  - `pkg/proxy/proxy_test.go`
  - `tests/e2e/harness.go`
  - `tests/e2e/conformance_test.go`

## 4. Validation & Results
- **Unit Tests**:
  - `TestHTTPRouteValidate_Retry`: Validates parsing of valid/invalid retry configurations.
  - `TestProxyRetries`: Verifies retry behavior upon 500 errors, attempt limits, and backoff.
- **Conformance Logs**:
  ```
  --- PASS: TestConformance/HTTPRouteRetry (0.12s)
      --- PASS: TestConformance/HTTPRouteRetry/2_request_to_'/retry/code-500-attempts-3'_does_not_retry_on_503_when_retry_is_only_configured_for_status_code_500 (0.03s)
      --- PASS: TestConformance/HTTPRouteRetry/7_request_to_'/retry/code-all-attempts-2'_succeeds_after_1_retry_on_503_when_retry_is_configured_for_all_status_code_and_max_attempts_is_2 (0.03s)
      --- PASS: TestConformance/HTTPRouteRetry/3_request_to_'/retry/code-all-attempts-2'_succeeds_after_1_retry_on_500_when_retry_is_configured_for_all_status_code_and_max_attempts_is_2 (0.03s)
      --- PASS: TestConformance/HTTPRouteRetry/5_request_to_'/retry/code-all-attempts-2'_succeeds_after_1_retry_on_502_when_retry_is_configured_for_all_status_code_and_max_attempts_is_2 (0.03s)
      --- PASS: TestConformance/HTTPRouteRetry/9_request_to_'/retry/code-all-attempts-2'_succeeds_after_1_retry_on_504_when_retry_is_configured_for_all_status_code_and_max_attempts_is_2 (0.03s)
      --- PASS: TestConformance/HTTPRouteRetry/4_request_to_'/retry/code-all-attempts-2'_fails_when_required_retries_on_500_exceed_max_attempts (0.03s)
      --- PASS: TestConformance/HTTPRouteRetry/10_request_to_'/retry/code-all-attempts-2'_fails_when_required_retries_on_504_exceed_max_attempts (0.03s)
      --- PASS: TestConformance/HTTPRouteRetry/0_request_to_'/retry/code-500-attempts-3'_succeeds_after_2_retries_when_retry_is_configured_for_status_code_500_and_max_attempts_is_3 (0.03s)
      --- PASS: TestConformance/HTTPRouteRetry/6_request_to_'/retry/code-all-attempts-2'_fails_when_required_retries_on_502_exceed_max_attempts (0.03s)
      --- PASS: TestConformance/HTTPRouteRetry/8_request_to_'/retry/code-all-attempts-2'_fails_when_required_retries_on_503_exceed_max_attempts (0.03s)
      --- PASS: TestConformance/HTTPRouteRetry/1_request_to_'/retry/code-500-attempts-3'_fails_when_required_retries_on_500_exceed_max_attempts (0.03s)
  ```
