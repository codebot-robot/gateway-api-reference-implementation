# Conformance Test Journal: HTTPRouteHTTPSListener and HTTPRouteHTTPSListenerDetectMisdirectedRequests

## 1. Test Overview
- **Name**: `HTTPRouteHTTPSListener`, `HTTPRouteHTTPSListenerDetectMisdirectedRequests`
- **Description**:
  - `HTTPRouteHTTPSListener`: Verifies that HTTPRoutes attach to HTTPS listeners on a Gateway, that TLS certificates are selected properly via SNI, and that traffic is routed based on listener and route hostnames.
  - `HTTPRouteHTTPSListenerDetectMisdirectedRequests`: Verifies that HTTPS listeners on the same port detect misdirected requests (e.g. from HTTP/2 connection reuse / coalescing) where the request Host header does not match the listener or SNI for which the connection was established, returning HTTP 421 Misdirected Request when appropriate.
- **Manifests**: `tests/httproute-https-listener.yaml`, `tests/httproute-https-listener-detect-misdirected-requests.yaml`

## 2. Issue / Failure Analysis
- **Observed Behavior**: The tests were not enabled in `tests/e2e/conformance_test.go`.
- **Root Cause**:
  1. The proxy previously did not associate routes with specific listeners or track the active listeners on the Gateway.
  2. The proxy did not evaluate TLS connection SNI (`r.TLS.ServerName`) against configured listener hostnames to determine the connection listener.
  3. The proxy did not check for misdirected requests on HTTPS connections (comparing the connection listener against the best-matching listener for the request `Host` header) to return HTTP 421 Misdirected Request.
  4. The HTTPS proxy server in `pkg/gari/gari.go` did not configure HTTP/2 ALPN negotiation (`h2`) on the TLS server, which is required for HTTP/2 connection reuse and conformance testing under `SupportGatewayHTTPSListenerDetectMisdirectedRequests`.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Introduced `InternalListener` and `MatchType` enum in `pkg/state/gateway.go`, holding listener metadata (`Name`, `Protocol`, `Port`, `Hostname`, `GatewayName`, `Routes`).
  2. Implemented `MatchesWildcard(pattern, host string) bool` and `MatchListener(listeners []InternalListener, host string) (*InternalListener, MatchType)` in `pkg/state/gateway.go` to match hostnames against listeners according to Gateway API precedence (Exact > Wildcard > Catch-all).
  3. Added `BuildInternalState` in `pkg/state/gateway.go` to construct both `[]InternalListener` (with attached routes per listener) and `[]InternalRoute`.
  4. Updated `Proxy` in `pkg/proxy/proxy.go` to store `listeners []state.InternalListener` and provide `UpdateListeners` and `UpdateConfig`.
  5. Updated `Proxy.ServeHTTP` in `pkg/proxy/proxy.go`:
     - For HTTPS requests (`r.TLS != nil`): matches `r.TLS.ServerName` against HTTPS listeners to identify `connListener`. Evaluates `r.Host` against HTTPS listeners to detect misdirected requests:
       - Returns HTTP 421 if another listener is an exact or more specific match for `r.Host`, or if `connListener` does not match `r.Host` and another listener matches `r.Host`.
       - Returns HTTP 404 if no listener matches `r.Host`.
       - Routes the request only within `connListener.Routes`.
     - For HTTP requests (`r.TLS == nil`): matches `r.Host` against HTTP listeners and routes within the matched listener's routes.
  6. Updated `updateProxy` in `pkg/controller/utils.go` to build `proxyListeners` and `proxyRoutes` via `BuildInternalState` and pass them to `Proxy.UpdateConfig`.
  7. Configured HTTP/2 server (`http2.ConfigureServer(srv, &http2.Server{})`) on the HTTPS proxy server in `pkg/gari/gari.go`.
  8. Added unit tests for `MatchesWildcard` and `MatchListener` in `pkg/state/match_test.go`, and `TestProxy_HTTPSListenerDetectMisdirectedRequests` in `pkg/proxy/proxy_test.go`.
  9. Added `tests.HTTPRouteHTTPSListener` and `tests.HTTPRouteHTTPSListenerDetectMisdirectedRequests` in alphabetical order in `tests/e2e/conformance_test.go`.
- **Key Files Modified**:
  - `pkg/state/gateway.go`
  - `pkg/state/match_test.go`
  - `pkg/proxy/proxy.go`
  - `pkg/proxy/proxy_test.go`
  - `pkg/controller/utils.go`
  - `pkg/gari/gari.go`
  - `tests/e2e/conformance_test.go`
  - `.agents/skills/implement-conformance-test/journal/httproute-https-listener.md`

## 4. Validation & Results
- **Unit Tests**:
  - Ran `go test ./...` which passed all unit tests across all packages including `TestMatchesWildcard`, `TestMatchListener`, and `TestProxy_HTTPSListenerDetectMisdirectedRequests`.
- **Conformance Logs**:
  - Ran `dev/tasks/test-e2e` (`ap e2e`) which passed all conformance tests including `HTTPRouteHTTPSListener` and `HTTPRouteHTTPSListenerDetectMisdirectedRequests`:
    ```
    --- PASS: TestConformance/HTTPRouteHTTPSListener (0.05s)
        --- PASS: TestConformance/HTTPRouteHTTPSListener/0_request_to_'example.org/'_should_go_to_infra-backend-v1 (0.01s)
        --- PASS: TestConformance/HTTPRouteHTTPSListener/1_request_to_'unknown-example.org/'_should_receive_one_of_[] (0.01s)
        --- PASS: TestConformance/HTTPRouteHTTPSListener/2_request_to_'second-example.org/'_should_go_to_infra-backend-v2 (0.01s)
    --- PASS: TestConformance/HTTPRouteHTTPSListenerDetectMisdirectedRequests (0.41s)
        --- PASS: TestConformance/HTTPRouteHTTPSListenerDetectMisdirectedRequests/0_request_to_'example.org/detect-misdirected-requests'_should_go_to_infra-backend-v1 (0.01s)
        --- PASS: TestConformance/HTTPRouteHTTPSListenerDetectMisdirectedRequests/1_request_to_'second-example.org/detect-misdirected-requests'_should_receive_one_of_[421] (0.01s)
        --- PASS: TestConformance/HTTPRouteHTTPSListenerDetectMisdirectedRequests/2_request_to_'unknown-example.org/detect-misdirected-requests'_should_receive_one_of_[404] (0.01s)
        --- PASS: TestConformance/HTTPRouteHTTPSListenerDetectMisdirectedRequests/3_request_to_'second-example.org/detect-misdirected-requests'_should_go_to_infra-backend-v2 (0.01s)
        --- PASS: TestConformance/HTTPRouteHTTPSListenerDetectMisdirectedRequests/4_request_to_'example.org/detect-misdirected-requests'_should_receive_one_of_[421] (0.01s)
        --- PASS: TestConformance/HTTPRouteHTTPSListenerDetectMisdirectedRequests/5_request_to_'unknown-example.org/detect-misdirected-requests'_should_receive_one_of_[421] (0.01s)
        --- PASS: TestConformance/HTTPRouteHTTPSListenerDetectMisdirectedRequests/6_request_to_'third-example.wildcard.org/detect-misdirected-requests'_should_go_to_infra-backend-v3 (0.01s)
        --- PASS: TestConformance/HTTPRouteHTTPSListenerDetectMisdirectedRequests/7_request_to_'fith-example.wildcard.org/detect-misdirected-requests'_should_go_to_infra-backend-v3 (0.01s)
        --- PASS: TestConformance/HTTPRouteHTTPSListenerDetectMisdirectedRequests/8_request_to_'fourth-example.wildcard.org/detect-misdirected-requests'_should_receive_one_of_[421] (0.01s)
        --- PASS: TestConformance/HTTPRouteHTTPSListenerDetectMisdirectedRequests/9_request_to_'second-example.org/detect-misdirected-requests'_should_receive_one_of_[421] (0.01s)
        --- PASS: TestConformance/HTTPRouteHTTPSListenerDetectMisdirectedRequests/10_request_to_'unknown-example.org/detect-misdirected-requests'_should_receive_one_of_[421] (0.01s)
        --- PASS: TestConformance/HTTPRouteHTTPSListenerDetectMisdirectedRequests/11_request_to_'fourth-example.wildcard.org/detect-misdirected-requests'_should_go_to_infra-backend-v1 (0.01s)
        --- PASS: TestConformance/HTTPRouteHTTPSListenerDetectMisdirectedRequests/12_request_to_'fith-example.wildcard.org/detect-misdirected-requests'_should_receive_one_of_[421] (0.01s)
        --- PASS: TestConformance/HTTPRouteHTTPSListenerDetectMisdirectedRequests/13_request_to_'example.org/detect-misdirected-requests'_should_go_to_infra-backend-v1 (0.01s)
        --- PASS: TestConformance/HTTPRouteHTTPSListenerDetectMisdirectedRequests/14_request_to_'unknown-example.org/detect-misdirected-requests'_should_receive_one_of_[404] (0.02s)
    ```
