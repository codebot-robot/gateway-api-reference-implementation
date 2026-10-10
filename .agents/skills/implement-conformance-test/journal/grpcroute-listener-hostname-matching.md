# Conformance Test Journal: GRPCRouteListenerHostnameMatching

## 1. Test Overview
- **Name**: `GRPCRouteListenerHostnameMatching`
- **Description**: Verifies that GRPCRoute correctly attaches to specific listeners using `parentRefs[].sectionName` and that incoming gRPC requests select the appropriate route based on hostname precedence (:authority matching).
- **Manifests**: `sigs.k8s.io/gateway-api/conformance/tests/grpcroute-listener-hostname-matching.yaml` (v1.6.3)

## 2. Issue / Failure Analysis
- **Observed Behavior**: `GRPCRouteListenerHostnameMatching` was not enabled in `tests/e2e/conformance_test.go` and GRPCRoute was not implemented.
- **Root Cause**: GRPCRoute support was missing in state compilation, binding, and proxy routing. When four listeners on port 80 define hostnames `bar.com`, `foo.bar.com`, `*.bar.com`, and `*.foo.com`, each route attached by section name must receive traffic according to hostname specificity matching. Furthermore, unmatched hostnames (e.g. `foo.com` or `no.matching.host`) must return `codes.Unimplemented` (HTTP 200 with `grpc-status: 12`).

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Bound GRPCRoutes to EffectiveListeners through `bindGRPCRouteParentRef`, respecting sectionName matching, allowed namespaces, and hostname intersection.
  2. Routed gRPC requests through the existing `MatchRoute` candidate evaluation in `pkg/state/gateway.go`, which implements hostname match precedence (non-wildcard length, full matching hostname length).
  3. Ensured that when host matching fails or no listener matches the requested authority, `respondNotFound` in `pkg/proxy/proxy.go` returns HTTP 200 with `Content-Type: application/grpc` and `grpc-status: 12` (`Unimplemented`).
  4. Enabled `tests.GRPCRouteListenerHostnameMatching` in `tests/e2e/conformance_test.go`.
- **Key Files Modified**:
  - `pkg/state/grpcroute.go`
  - `pkg/state/compiled.go`
  - `pkg/proxy/proxy.go`
  - `tests/e2e/conformance_test.go`

## 4. Validation & Results
- **Unit Tests**: Ran unit tests in `pkg/state` and `pkg/proxy` verifying listener binding and status.
- **Conformance Logs**: Successful pass logs showing the test passes under Gateway API v1.6.3:
  ```
  --- PASS: TestConformance/GRPCRouteListenerHostnameMatching (2.65s)
      --- PASS: TestConformance/GRPCRouteListenerHostnameMatching/1_request_to_'foo.bar.com/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_should_go_to_grpc-infra-backend-v2 (1.05s)
      --- PASS: TestConformance/GRPCRouteListenerHostnameMatching/3_request_to_'boo.bar.com/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_should_go_to_grpc-infra-backend-v3 (1.07s)
      --- PASS: TestConformance/GRPCRouteListenerHostnameMatching/6_request_to_'foo.com/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_should_receive_a_Unimplemented_(12) (2.00s)
      --- PASS: TestConformance/GRPCRouteListenerHostnameMatching/7_request_to_'no.matching.host/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_should_receive_a_Unimplemented_(12) (2.00s)
      --- PASS: TestConformance/GRPCRouteListenerHostnameMatching/0_request_to_'bar.com/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_should_go_to_grpc-infra-backend-v1 (2.01s)
      --- PASS: TestConformance/GRPCRouteListenerHostnameMatching/4_request_to_'multiple.prefixes.bar.com/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_should_go_to_grpc-infra-backend-v3 (2.01s)
      --- PASS: TestConformance/GRPCRouteListenerHostnameMatching/5_request_to_'multiple.prefixes.foo.com/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_should_go_to_grpc-infra-backend-v3 (2.01s)
      --- PASS: TestConformance/GRPCRouteListenerHostnameMatching/2_request_to_'baz.bar.com/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_should_go_to_grpc-infra-backend-v3 (2.01s)
  ```
