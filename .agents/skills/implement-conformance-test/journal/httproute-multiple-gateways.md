# Conformance Test Journal: HTTPRouteMultipleGateways

## 1. Test Overview
- **Name**: `HTTPRouteMultipleGateways`
- **Description**: Verifies that an HTTPRoute can bind to multiple Gateways (in the same namespace or across namespaces) and that distinct Gateways route traffic to their intended backends with proper isolation.
- **Manifests**: `sigs.k8s.io/gateway-api/conformance/tests/httproute-multiple-gateways.yaml` (v1.6.3)

## 2. Issue / Failure Analysis
- **Observed Behavior**: In single-pod fast-path mode, all Gateways previously shared a single `default/gari-proxy` LoadBalancer Service and shared proxy ports (`:8000`, `:8443`), routing solely by Host header. Gateways could not be isolated when two Gateways had overlapping catch-all paths (e.g., both having `/` on port 80). Per-Gateway data-plane provisioning was implemented in #620 to solve this isolation issue. However, while the journal and commit message in #620 indicated that `tests.HTTPRouteMultipleGateways` was enabled, it was inadvertently omitted from `selectedTests` in `tests/e2e/conformance_test.go` and remained unselected.
- **Root Cause**: Fast-path mode originally lacked per-Gateway data-plane Deployment provisioning and per-Gateway LoadBalancer Services for traffic and listener isolation. Once per-Gateway provisioning was in place (#620/#624), the test was expected to pass as-is, but had never actually been added to `selectedTests`.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Per-Gateway data-plane provisioning was implemented in #620 (dedicated per-Gateway Deployments and `type: LoadBalancer` Services, effective listener port derivation, data-plane mode with status updates disabled, deployment availability gating in `State.SetGatewayReadiness`, read-only `gari-dataplane` RBAC, and orphan sweeping).
  2. In #723, added `tests.HTTPRouteMultipleGateways` to `selectedTests` in `tests/e2e/conformance_test.go` in alphabetical order.
  3. Verified that the test passes as-is with no modifications required to the model or provisioning.
- **Key Files Modified**:
  - `tests/e2e/conformance_test.go`
  - `.agents/skills/implement-conformance-test/journal/httproute-multiple-gateways.md`

## 4. Validation & Results
- **Unit Tests**: Verified with `ap test` (all unit tests pass) and `ap generate`.
- **Conformance Logs**:
  ```
  === RUN   TestConformance
  ...
      --- PASS: TestConformance/HTTPRouteMultipleGateways (0.02s)
          --- PASS: TestConformance/HTTPRouteMultipleGateways/Gateway_same-namespace (0.11s)
              --- PASS: TestConformance/HTTPRouteMultipleGateways/Gateway_same-namespace/shared_route_is_accessible_and_routed_to_infra-backend-v1 (0.00s)
              --- PASS: TestConformance/HTTPRouteMultipleGateways/Gateway_same-namespace/dedicated_route_is_accessible_and_routed_to_infra-backend-v2 (0.00s)
          --- PASS: TestConformance/HTTPRouteMultipleGateways/Gateway_all-namespaces (0.12s)
              --- PASS: TestConformance/HTTPRouteMultipleGateways/Gateway_all-namespaces/shared_route_is_accessible_and_routed_to_infra-backend-v1 (0.01s)
              --- PASS: TestConformance/HTTPRouteMultipleGateways/Gateway_all-namespaces/dedicated_route_is_accessible_and_routed_to_infra-backend-v3 (0.00s)
  PASS
  ok  	github.com/gke-labs/gateway-api-reference-implementation/tests/e2e	235.235s
  ```
