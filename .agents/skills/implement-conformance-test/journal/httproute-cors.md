# Conformance Test Journal: HTTPRouteCORS

## 1. Test Overview
- **Name**: HTTPRouteCORS
- **Description**: Verifies that an HTTPRoute with a CORS filter properly handles preflight (OPTIONS) requests and actual cross-origin requests, applying the appropriate `Access-Control-*` response headers based on allowed origins, methods, headers, credentials, and max-age settings.
- **Manifests**: `sigs.k8s.io/gateway-api/conformance/tests/httproute-cors.yaml`

## 2. Issue / Failure Analysis
- **Observed Behavior**: The CORS filter (`HTTPRouteFilterCORS`) was not supported or enabled in the conformance test suite.
- **Root Cause**:
  1. `HTTPRouteFilterCORS` filter was treated as an unsupported filter type in `pkg/state/httproute.go`, causing route acceptance validation failure with `RouteReasonUnsupportedValue`.
  2. `InternalRule` and `InternalBackend` in `pkg/state/gateway.go` did not store CORS configuration.
  3. Preflight `OPTIONS` requests were not handled by `pkg/proxy/proxy.go` with CORS response headers (answering preflight requests directly without backend forwarding).
  4. Response headers on simple / actual requests were not augmented with `Access-Control-Allow-Origin`, `Access-Control-Allow-Credentials`, and `Access-Control-Expose-Headers`.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Updated `InternalRule` and `InternalBackend` in `pkg/state/gateway.go` to include `CORS *gatewayv1.HTTPCORSFilter`.
  2. Updated `InternalMatch.Matches` in `pkg/state/gateway.go` to match preflight `OPTIONS` requests against method-specific rules using `Access-Control-Request-Method`.
  3. Added parsing of `gatewayv1.HTTPRouteFilterCORS` in `pkg/state/httproute.go` for both route rules and backendRefs.
  4. Implemented origin matching in `pkg/proxy/proxy.go` supporting exact origins, wildcard schemes/hosts (`*` and `*.domain.com`), and ports.
  5. Implemented CORS header evaluation in `pkg/proxy/proxy.go` (`Access-Control-Allow-Origin`, `Access-Control-Allow-Credentials`, `Access-Control-Allow-Methods`, `Access-Control-Allow-Headers`, `Access-Control-Expose-Headers`, and `Access-Control-Max-Age`).
  6. Handled preflight `OPTIONS` requests directly in `Proxy.ServeHTTP` returning HTTP 200 with appropriate CORS headers without forwarding to backends.
  7. Augmented forwarded responses in `Proxy.forward` with CORS response headers for matched origins.
  8. Added `tests.HTTPRouteCORS` to `selectedTests` in `tests/e2e/conformance_test.go` in alphabetical order.
- **Key Files Modified**:
  - `pkg/state/gateway.go`
  - `pkg/state/gateway_test.go`
  - `pkg/state/httproute.go`
  - `pkg/state/httproute_test.go`
  - `pkg/proxy/proxy.go`
  - `pkg/proxy/proxy_test.go`
  - `tests/e2e/conformance_test.go`

## 4. Validation & Results
- **Unit Tests**:
  - Added `TestBuildInternalRoutesCORS` and `TestMatchRouteCORSPreflightMethod` in `pkg/state/gateway_test.go`.
  - Updated `TestHTTPRouteValidate_Filters` in `pkg/state/httproute_test.go`.
  - Added `TestProxyCORS` in `pkg/proxy/proxy_test.go` covering preflight exact and wildcard origins, unauthorized origins, wildcard methods/headers, credentials, and simple GET requests.
- **Conformance Logs**:
  - Validated via `ap test`, `ap lint`, `ap generate`, and `ap e2e` running the full suite with `tests.HTTPRouteCORS`.
