# Conformance Test Journal: HTTPRouteTimeoutRequest and HTTPRouteTimeoutBackendRequest

## 1. Test Overview
- **Name**: `HTTPRouteTimeoutRequest`, `HTTPRouteTimeoutBackendRequest`
- **Description**: Verifies that HTTPRoute rules with `timeouts.request` and `timeouts.backendRequest` properly enforce timeouts by returning HTTP 504 Gateway Timeout when responses are delayed past the deadline, and that 0s duration disables the timeout.
- **Manifests**:
  - `sigs.k8s.io/gateway-api/conformance/tests/httproute-timeout-request.yaml`
  - `sigs.k8s.io/gateway-api/conformance/tests/httproute-timeout-backend-request.yaml`

## 2. Issue / Failure Analysis
- **Observed Behavior**: The tests `tests.HTTPRouteTimeoutRequest` and `tests.HTTPRouteTimeoutBackendRequest` were not yet included in `tests/e2e/conformance_test.go` and the reference implementation did not parse or enforce `timeouts` on HTTPRoute rules.
- **Root Cause**: `HTTPRouteRule.Timeouts` was unhandled in `HTTPRouteState.Validate()` and `CompileHTTPRoute()`. The reverse proxy forwarded requests with default context and default error handling (which returns 502 Bad Gateway instead of 504 Gateway Timeout on deadline expiration).

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Defined `InternalTimeouts` and `ParseTimeouts()` in `pkg/state/gateway.go` to parse and validate timeout durations.
  2. Populated `Timeouts` on `InternalRule` during `CompileHTTPRoute()` in `pkg/state/httproute.go`, setting error state and `RouteConditionAccepted: False` with `UnsupportedValue` reason if timeout strings are invalid.
  3. Updated `Proxy.forward()` in `pkg/proxy/proxy.go` to directly forward requests using standard Go `http.RoundTripper`, wrapping the overall request context with `timeouts.Request` (if > 0) and the backend roundtrip request context with `timeouts.BackendRequest` (if > 0), returning `504 Gateway Timeout` when `context.DeadlineExceeded` or network timeout occurs.
  4. Added comprehensive unit tests in `pkg/state/httproute_test.go`, `pkg/state/gateway_test.go`, and `pkg/proxy/proxy_test.go`.
  5. Added `tests.HTTPRouteTimeoutBackendRequest` and `tests.HTTPRouteTimeoutRequest` to `selectedTests` in `tests/e2e/conformance_test.go` in alphabetical order.
  6. Validated with `ap generate`, `ap test`, `ap lint`, and `ap e2e`.
- **Key Files Modified**:
  - `pkg/state/httproute.go`
  - `pkg/state/httproute_test.go`
  - `pkg/state/gateway.go`
  - `pkg/state/gateway_test.go`
  - `pkg/proxy/proxy.go`
  - `pkg/proxy/proxy_test.go`
  - `tests/e2e/conformance_test.go`
  - `.agents/skills/implement-conformance-test/journal/httproute-timeouts.md`

## 4. Validation & Results
- **Unit Tests**:
  - `pkg/state/httproute_test.go`: `TestHTTPRouteValidate_Timeouts` and `TestCompileHTTPRoute` verify validation and rule compilation of valid and invalid duration values.
  - `pkg/proxy/proxy_test.go`: `TestProxyTimeouts` verifies proxy returns 200 within timeout, 504 on timeout exceeded for both request and backendRequest timeouts, handles 0s (disabled), and applies response header modifiers.
- **Conformance Logs**:
  ```
  --- PASS: TestConformance/HTTPRouteTimeoutBackendRequest (0.12s)
      --- PASS: TestConformance/HTTPRouteTimeoutBackendRequest/0_request_to_'/backend-timeout'_should_receive_one_of_[] (0.00s)
      --- PASS: TestConformance/HTTPRouteTimeoutBackendRequest/1_request_to_'/backend-timeout?delay=1s'_should_receive_one_of_[] (1.50s)
      --- PASS: TestConformance/HTTPRouteTimeoutBackendRequest/2_request_to_'/disable-backend-timeout?delay=1s'_should_receive_one_of_[] (3.01s)
  --- PASS: TestConformance/HTTPRouteTimeoutRequest (0.12s)
      --- PASS: TestConformance/HTTPRouteTimeoutRequest/0_request_to_'/request-timeout'_should_receive_one_of_[] (0.00s)
      --- PASS: TestConformance/HTTPRouteTimeoutRequest/1_request_to_'/request-timeout?delay=1s'_should_receive_one_of_[] (1.50s)
      --- PASS: TestConformance/HTTPRouteTimeoutRequest/2_request_to_'/disable-request-timeout?delay=1s'_should_receive_one_of_[] (3.01s)
  ```
