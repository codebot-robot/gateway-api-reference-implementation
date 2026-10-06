# Conformance Test Journal: HTTPRouteMultipleGateways

## 1. Test Overview
- **Name**: `HTTPRouteMultipleGateways`
- **Description**: Verifies that an HTTPRoute can bind to multiple Gateways (in the same namespace or across namespaces) and that distinct Gateways route traffic to their intended backends with proper isolation.
- **Manifests**: `sigs.k8s.io/gateway-api/conformance/tests/httproute-multiple-gateways.yaml` (v1.6.0)

## 2. Issue / Failure Analysis
- **Observed Behavior**: In single-pod fast-path mode, all Gateways previously shared a single `default/gari-proxy` LoadBalancer Service and shared proxy ports (`:8000`, `:8443`), routing solely by Host header. Gateways could not be isolated when two Gateways had overlapping catch-all paths (e.g., both having `/` on port 80).
- **Root Cause**: Fast-path mode lacked per-Gateway data-plane Deployment provisioning and per-Gateway LoadBalancer Services for traffic and listener isolation.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Implemented per-Gateway Deployment and `type: LoadBalancer` Service provisioning in `pkg/provisioning/singlepod` with deterministic naming (`gari-gw-<hash>`), Gateway identifying labels (`gateway.networking.k8s.io/gateway-name`, `gateway.networking.k8s.io/gateway-namespace`, `app.kubernetes.io/managed-by: gari-singlepod`), and service selector matching its dedicated Deployment.
  2. Derived Service ports dynamically from effective listeners from `State` outputs (falling back to spec listeners if not yet compiled).
  3. Added data-plane mode (`--dataplane-mode`, `--gateway-namespace`, `--gateway-name`, `DisableStatusUpdates: true`) so each per-Gateway pod reconciles and serves only its designated Gateway without colliding on Host headers or writing status back.
  4. Integrated data-plane Deployment availability into `State.SetGatewayReadiness`, gating the Gateway `Programmed` condition in `ComputeOutputs` on both address assignment and deployment readiness without fallback.
  5. Added separate read-only `gari-dataplane` ServiceAccount, ClusterRole, and ClusterRoleBinding for data-plane instances.
  6. Implemented label-based orphan sweeping and deletion cleanup via `GatewayDeleteHandler.OnGatewayDeleted` without cross-namespace finalizers.
  7. Removed the shared `gari-proxy` Service from `k8s/controller.yaml`, granted Deployment and Service CRUD permissions in ClusterRole RBAC, and updated e2e harness and tests to use Gateway `status.addresses` and polling.
  8. Enabled `tests.HTTPRouteMultipleGateways` in `tests/e2e/conformance_test.go`.
- **Key Files Modified**:
  - `pkg/provisioning/singlepod/address_provider.go`
  - `pkg/provisioning/singlepod/address_provider_test.go`
  - `pkg/controller/gateway_controller.go`
  - `pkg/controller/utils.go`
  - `pkg/state/state.go`
  - `pkg/state/compiled.go`
  - `pkg/state/compiled_test.go`
  - `pkg/gari/gari.go`
  - `cmd/gateway-api-reference-implementation/main.go`
  - `k8s/controller.yaml`
  - `tests/e2e/harness.go`
  - `tests/e2e/e2e_test.go`
  - `tests/e2e/conformance_test.go`

## 4. Validation & Results
- **Unit Tests**: Added unit tests for per-Gateway Deployment & Service provisioning, effective listener ports, readiness gating, and orphan sweeping; verified with `ap test` and `ap lint`.
- **Conformance Logs**:
  ```
  --- PASS: TestConformance (110.45s)
      --- PASS: TestConformance/HTTPRouteMultipleGateways (0.02s)
          --- PASS: TestConformance/HTTPRouteMultipleGateways/Gateway_same-namespace (0.01s)
              --- PASS: TestConformance/HTTPRouteMultipleGateways/Gateway_same-namespace/shared_route_is_accessible_and_routed_to_infra-backend-v1 (0.00s)
              --- PASS: TestConformance/HTTPRouteMultipleGateways/Gateway_same-namespace/dedicated_route_is_accessible_and_routed_to_infra-backend-v2 (0.00s)
          --- PASS: TestConformance/HTTPRouteMultipleGateways/Gateway_all-namespaces (0.11s)
              --- PASS: TestConformance/HTTPRouteMultipleGateways/Gateway_all-namespaces/shared_route_is_accessible_and_routed_to_infra-backend-v1 (0.00s)
              --- PASS: TestConformance/HTTPRouteMultipleGateways/Gateway_all-namespaces/dedicated_route_is_accessible_and_routed_to_infra-backend-v3 (0.00s)
  PASS
  ok      github.com/gke-labs/gateway-api-reference-implementation/tests/e2e     110.458s
  ```
