# Conformance Test Journal: GRPCRouteWeight

## 1. Test Overview
- **Name**: `GRPCRouteWeight`
- **Description**: Verifies that GRPCRoute distributes gRPC requests across multiple backends according to their specified weights (70 / 30 / 0), confirming that traffic distribution remains within tolerance and backends with weight 0 never receive requests.
- **Manifests**: `sigs.k8s.io/gateway-api/conformance/tests/grpcroute-weight.yaml` (v1.6.3)

## 2. Issue / Failure Analysis
- **Observed Behavior**: `tests.GRPCRouteWeight` was not included in `selectedTests` in `tests/e2e/conformance_test.go`.
- **Root Cause**: Backend weights were already populated into `InternalBackend.Weight` by `CompileGRPCRoute`, and `pickBackend` in the proxy performed weighted selection. However, the conformance test had not yet been enabled in `tests/e2e/conformance_test.go`, and unit tests verifying backend weight retention and selection (including zero-weight filtering) were missing for GRPCRoute.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Enabled `tests.GRPCRouteWeight` in `tests/e2e/conformance_test.go` under `selectedTests`.
  2. Exposed `PickBackend` and `PickBackendWithRand` in `pkg/state/gateway.go` mirroring `PickTLSBackendWithRand`, ensuring backends with weight 0 or negative are never selected and allowing deterministic testing with seeded RNGs.
  3. Delegated `pickBackend` in `pkg/proxy/proxy.go` directly to `state.PickBackend`.
  4. Clamped negative weights to 0 during `CompileGRPCRoute` in `pkg/state/grpcroute.go`.
  5. Added unit tests in `pkg/state/grpcroute_test.go` (`TestGRPCRoute_WeightedBackends`):
     - Verified that backend weights (70, 30, 0) are preserved on compiled routes in `ComputeOutputs`.
     - Tested 10,000 draws using a seeded RNG with `PickBackendWithRand`, confirming ~70% and ~30% distributions and exactly 0 selections for the weight-0 backend.
- **Key Files Modified**:
  - `pkg/state/gateway.go`
  - `pkg/state/grpcroute.go`
  - `pkg/state/grpcroute_test.go`
  - `pkg/proxy/proxy.go`
  - `tests/e2e/conformance_test.go`

## 4. Validation & Results
- **Unit Tests**:
  - `go test -v ./pkg/state -run TestGRPCRoute_WeightedBackends` passed.
  - `go test ./pkg/...` passed.
- **Conformance Logs**:
  ```
  === RUN   TestConformance/GRPCRouteWeight
      conformance.go:77: 2026-10-10T20:56:27.363575475Z: Running GRPCRouteWeight, relying on the following features: Gateway-standard, GRPCRoute-standard
  === RUN   TestConformance/GRPCRouteWeight/Requests_should_have_a_distribution_that_matches_the_weight
      --- PASS: TestConformance/GRPCRouteWeight (0.41s)
          --- PASS: TestConformance/GRPCRouteWeight/Requests_should_have_a_distribution_that_matches_the_weight (0.29s)
  ```
