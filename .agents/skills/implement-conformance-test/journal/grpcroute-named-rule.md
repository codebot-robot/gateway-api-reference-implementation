# Conformance Test Journal: GRPCRouteNamedRule

## 1. Test Overview
- **Name**: `GRPCRouteNamedRule`
- **Description**: Verifies that a GRPCRoute whose rule defines `name: named-rule` with multiple method matches routes traffic correctly to the expected backends (`Echo` to v1, `EchoTwo` to v2) when the implementation supports the `GRPCRouteNamedRouteRule` feature.
- **Manifests**: `sigs.k8s.io/gateway-api/conformance/tests/grpcroute-named-rule.yaml` (v1.6.3)

## 2. Issue / Failure Analysis
- **Observed Behavior**: `tests.GRPCRouteNamedRule` was not enabled in `tests/e2e/conformance_test.go`.
- **Root Cause**: GRPCRoute named rules were already supported by `CompileGRPCRoute` (persisting `rule.Name` into `InternalRule.Name` and validating duplicate rule names), but the conformance test was omitted from `selectedTests` pending enablement.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Enabled `tests.GRPCRouteNamedRule` in `tests/e2e/conformance_test.go` under `selectedTests`.
  2. Added unit tests in `pkg/state/grpcroute_test.go` (`TestGRPCRoute_NamedRules`):
     - Verified that rule names are preserved on `InternalRule.Name` and unnamed rules retain `nil`.
     - Verified that duplicate rule names are detected and rejected during compilation, resulting in `RouteConditionAccepted` with status `False` and reason `RouteReasonUnsupportedValue`.
- **Key Files Modified**:
  - `tests/e2e/conformance_test.go`
  - `pkg/state/grpcroute_test.go`

## 4. Validation & Results
- **Unit Tests**:
  - `go test -v ./pkg/state -run TestGRPCRoute_NamedRules` passed.
- **Conformance Logs**:
  ```
  === RUN   TestConformance/GRPCRouteNamedRule
      conformance.go:77: 2026-10-10T20:56:27.237250436Z: Running GRPCRouteNamedRule, relying on the following features: Gateway-standard, GRPCRoute-standard, GRPCRouteNamedRouteRule-standard
  === RUN   TestConformance/GRPCRouteNamedRule/0_request_to_'/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_should_go_to_grpc-infra-backend-v1
  === RUN   TestConformance/GRPCRouteNamedRule/1_request_to_'/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/EchoTwo'_should_go_to_grpc-infra-backend-v2
      --- PASS: TestConformance/GRPCRouteNamedRule (0.12s)
          --- PASS: TestConformance/GRPCRouteNamedRule/0_request_to_'/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_should_go_to_grpc-infra-backend-v1 (0.01s)
          --- PASS: TestConformance/GRPCRouteNamedRule/1_request_to_'/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/EchoTwo'_should_go_to_grpc-infra-backend-v2 (0.01s)
  ```
