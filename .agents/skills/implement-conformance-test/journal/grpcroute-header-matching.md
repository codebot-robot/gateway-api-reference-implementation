# Conformance Test Journal: GRPCRouteHeaderMatching

## 1. Test Overview
- **Name**: `GRPCRouteHeaderMatching`
- **Description**: Verifies that GRPCRoute routes requests correctly based on header matching rules, including single header match, multiple header matches (AND), multiple match conditions (OR), header matching precedence, and returning `codes.Unimplemented` (HTTP 200 with `grpc-status: 12`) when no header rule matches.
- **Manifests**: `sigs.k8s.io/gateway-api/conformance/tests/grpcroute-header-matching.yaml` (v1.6.3)

## 2. Issue / Failure Analysis
- **Observed Behavior**: `GRPCRouteHeaderMatching` was not enabled in `tests/e2e/conformance_test.go` and GRPCRoute was not implemented.
- **Root Cause**: GRPCRoute support was missing in state compilation and proxy handling. GRPCRoute header matches must map to internal header matches, deduplicating equivalent header names within a match condition according to the Gateway API spec. Match precedence in `isBetterCandidate` must select the rule with more header matches when other criteria tie. Unmatched requests must return `codes.Unimplemented`.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Compiled `matches[].headers` in `pkg/state/grpcroute.go` into `InternalHeaderMatch` structs on `InternalRule.Matches`, deduplicating case-insensitive header names.
  2. Supported both Exact and RegularExpression header matching types.
  3. Ensured that header match count and rule ordering in `MatchRoute` / `isBetterCandidate` determine matching precedence as per specification.
  4. Handled unmatched gRPC requests by returning HTTP 200 with `Content-Type: application/grpc` and `grpc-status: 12` in `pkg/proxy/proxy.go`.
  5. Enabled `tests.GRPCRouteHeaderMatching` in `tests/e2e/conformance_test.go`.
- **Key Files Modified**:
  - `pkg/state/grpcroute.go`
  - `pkg/state/compiled.go`
  - `pkg/proxy/proxy.go`
  - `tests/e2e/conformance_test.go`

## 4. Validation & Results
- **Unit Tests**: Ran unit tests in `pkg/state` verifying header compilation and deduplication.
- **Conformance Logs**: Successful pass logs showing the test passes under Gateway API v1.6.3:
  ```
  --- PASS: TestConformance/GRPCRouteHeaderMatching (0.12s)
      --- PASS: TestConformance/GRPCRouteHeaderMatching/5_request_to_'/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_with_headers_'{Some-Other-Header:one}'_should_receive_a_Unimplemented_(12) (0.00s)
      --- PASS: TestConformance/GRPCRouteHeaderMatching/4_request_to_'/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_with_headers_'{Color:orange}'_should_receive_a_Unimplemented_(12) (0.00s)
      --- PASS: TestConformance/GRPCRouteHeaderMatching/10_request_to_'/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_with_headers_'{Color:purple}'_should_receive_a_Unimplemented_(12) (0.01s)
      --- PASS: TestConformance/GRPCRouteHeaderMatching/3_request_to_'/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_with_headers_'{Color:blue,Version:two}'_should_go_to_grpc-infra-backend-v2 (0.02s)
      --- PASS: TestConformance/GRPCRouteHeaderMatching/9_request_to_'/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_with_headers_'{Color:yellow}'_should_go_to_grpc-infra-backend-v2 (0.02s)
      --- PASS: TestConformance/GRPCRouteHeaderMatching/8_request_to_'/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_with_headers_'{Color:red}'_should_go_to_grpc-infra-backend-v2 (0.02s)
      --- PASS: TestConformance/GRPCRouteHeaderMatching/7_request_to_'/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_with_headers_'{Color:green}'_should_go_to_grpc-infra-backend-v1 (0.02s)
      --- PASS: TestConformance/GRPCRouteHeaderMatching/2_request_to_'/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_with_headers_'{Color:orange,Version:two}'_should_go_to_grpc-infra-backend-v1 (0.02s)
      --- PASS: TestConformance/GRPCRouteHeaderMatching/0_request_to_'/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_with_headers_'{Version:one}'_should_go_to_grpc-infra-backend-v1 (0.02s)
      --- PASS: TestConformance/GRPCRouteHeaderMatching/6_request_to_'/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_with_headers_'{Color:blue}'_should_go_to_grpc-infra-backend-v1 (0.02s)
  ```
