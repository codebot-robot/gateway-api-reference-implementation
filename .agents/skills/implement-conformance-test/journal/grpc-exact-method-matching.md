# Conformance Test Journal: GRPCExactMethodMatching

## 1. Test Overview
- **Name**: `GRPCExactMethodMatching`
- **Description**: Verifies that GRPCRoute matches incoming plaintext gRPC requests using exact method matching (`service` and `method`), routing requests to different backends based on method name, and returning `codes.Unimplemented` (HTTP 200 with `grpc-status: 12`) when no route matches.
- **Manifests**: `sigs.k8s.io/gateway-api/conformance/tests/grpcroute-exact-method-matching.yaml` (v1.6.3)

## 2. Issue / Failure Analysis
- **Observed Behavior**: `GRPCExactMethodMatching` was not enabled in `tests/e2e/conformance_test.go` and GRPCRoute was not implemented.
- **Root Cause**: GRPCRoute support was missing in state compilation, controller reconciliation, and proxy handling:
  1. `gatewayv1.GRPCRoute` was not tracked in `pkg/state`, not compiled into internal routes, and not reconciled in `pkg/controller`.
  2. In gRPC over HTTP/2, method matching uses paths formatted as `/<service>/<method>`. Without compiling `method{service, method}` to an Exact path match and `service` alone to a PathPrefix match on `/<service>/`, gRPC requests could not match.
  3. When an incoming request has `Content-Type: application/grpc` and matches no route, gRPC clients expect a trailers-only response with HTTP 200, `Content-Type: application/grpc`, and `grpc-status: 12` (`codes.Unimplemented`), whereas plain HTTP returned 404.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Implemented GRPCRoute state handling in `pkg/state/grpcroute.go`:
     - Compiles `matches[].method{service, method}` into path Exact match `/<service>/<method>`.
     - Compiles `matches[].method{service}` into path PathPrefix match `/<service>/`.
     - Compiles header matches and resolves backend references using generalized `resolveBackendTarget`.
     - Bound GRPCRoutes to HTTP and HTTPS listeners on Gateways via `bindGRPCRouteParentRef`.
  2. Implemented `GRPCRouteReconciler` in `pkg/controller/grpcroute_controller.go` and registered informers and reconcilers in `pkg/controller/utils.go`.
  3. Updated `pkg/proxy/proxy.go` to detect gRPC requests (`Content-Type: application/grpc` or `application/grpc+...`) and return HTTP 200 with `grpc-status: 12` / `grpc-message` headers (trailers-only) when no route matches.
  4. Forwarded response trailers using `http.TrailerPrefix` in `pkg/proxy/proxy.go` so trailing metadata from gRPC backends is delivered to clients.
  5. Updated RBAC in `k8s/controller.yaml` and `snigateway/k8s/controller.yaml` to include `grpcroutes` and `grpcroutes/status`.
  6. Added unit tests in `pkg/state/grpcroute_test.go` and `pkg/proxy/proxy_test.go`.
  7. Enabled `tests.GRPCExactMethodMatching` in `tests/e2e/conformance_test.go`.
- **Key Files Modified**:
  - `pkg/state/grpcroute.go`
  - `pkg/state/grpcroute_test.go`
  - `pkg/state/compiled.go`
  - `pkg/state/state.go`
  - `pkg/state/httproute.go`
  - `pkg/state/utils.go`
  - `pkg/controller/grpcroute_controller.go`
  - `pkg/controller/utils.go`
  - `pkg/proxy/proxy.go`
  - `pkg/proxy/proxy_test.go`
  - `k8s/controller.yaml`
  - `snigateway/k8s/controller.yaml`
  - `tests/e2e/conformance_test.go`

## 4. Validation & Results
- **Unit Tests**: Ran unit tests in `pkg/state` and `pkg/proxy` verifying method compilation, binding, status, and h2c proxying.
- **Conformance Logs**: Successful pass logs showing the test passes under Gateway API v1.6.3:
  ```
  --- PASS: TestConformance/GRPCExactMethodMatching (0.12s)
      --- PASS: TestConformance/GRPCExactMethodMatching/2_request_to_'/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/EchoThree'_should_receive_a_Unimplemented_(12) (0.00s)
      --- PASS: TestConformance/GRPCExactMethodMatching/0_request_to_'/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/Echo'_should_go_to_grpc-infra-backend-v1 (0.03s)
      --- PASS: TestConformance/GRPCExactMethodMatching/1_request_to_'/gateway_api_conformance.echo_basic.grpcecho.GrpcEcho/EchoTwo'_should_go_to_grpc-infra-backend-v2 (0.03s)
  ```
