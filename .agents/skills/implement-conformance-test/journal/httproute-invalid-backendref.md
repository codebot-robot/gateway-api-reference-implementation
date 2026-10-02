# Conformance Test Journal: HTTPRouteInvalidNonExistentBackendRef & HTTPRouteInvalidCrossNamespaceBackendRef

## 1. Test Overview
- **Names**:
  - `HTTPRouteInvalidNonExistentBackendRef`
  - `HTTPRouteInvalidCrossNamespaceBackendRef`
- **Description**:
  - `HTTPRouteInvalidNonExistentBackendRef`: Verifies that an HTTPRoute with a backendRef pointing to a nonexistent Service sets a `ResolvedRefs` status condition of `Status: False` with `Reason: BackendNotFound`, and requests matching that route receive an HTTP 500 status code.
  - `HTTPRouteInvalidCrossNamespaceBackendRef`: Verifies that an HTTPRoute with a cross-namespace backendRef to a Service in another namespace where no `ReferenceGrant` exists sets a `ResolvedRefs` status condition of `Status: False` with `Reason: RefNotPermitted`, and requests matching that route receive an HTTP 500 status code.
- **Manifests**:
  - `sigs.k8s.io/gateway-api/conformance/tests/httproute-invalid-nonexistent-backendref.yaml`
  - `sigs.k8s.io/gateway-api/conformance/tests/httproute-invalid-cross-namespace-backend-ref.yaml`

## 2. Issue / Failure Analysis
- **Observed Behavior**: The reference implementation did not check whether backend Services exist or whether cross-namespace backend references are permitted by a `ReferenceGrant`. As a result, the `ResolvedRefs` condition remained `Status: True` with `Reason: ResolvedRefs`, and the proxy did not emit the expected HTTP 500 error for invalid backend references.
- **Root Cause**:
  1. `HTTPRouteState.ComputeResolvedRefsCondition` did not have access to cluster Services or `ReferenceGrant` objects to evaluate existence or cross-namespace permissions.
  2. The controller did not reconcile or watch `ReferenceGrant` resources or cross-namespace Service changes.
  3. `GatewayState.BuildInternalRoutes` did not check for missing services or ungranted cross-namespace references when building internal rules and error states.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Added `ReferenceGrant` support in `pkg/state/state.go` with thread-safe storage, upsert, and delete operations.
  2. Implemented `IsReferencePermitted` helper in `pkg/state/referencegrant.go` to evaluate `ReferenceGrant` `From` and `To` rules for cross-namespace references.
  3. Updated `ComputeResolvedRefsCondition` in `pkg/state/httproute.go` to accept services and reference grants, validating Group/Kind (`InvalidKind`), cross-namespace permissions (`RefNotPermitted`), and backend existence (`BackendNotFound`).
  4. Updated `BuildInternalRoutes` in `pkg/state/gateway.go` and `pkg/controller/utils.go` to validate cross-namespace references and service existence, setting `iRule.Error` with HTTP status code 500 when invalid.
  5. Implemented `ReferenceGrantReconciler` in `pkg/controller/referencegrant_controller.go` and registered it in `cmd/gateway-api-reference-implementation/main.go`.
  6. Added watches for `ReferenceGrant` and cross-namespace `Service` references in `HTTPRouteReconciler` and `GatewayReconciler`.
  7. Updated ClusterRole permissions in `k8s/controller.yaml` to include `referencegrants`.
  8. Added comprehensive unit tests in `pkg/state/referencegrant_test.go`, `pkg/state/httproute_test.go`, and `pkg/state/gateway_test.go`.
  9. Enabled `tests.HTTPRouteInvalidCrossNamespaceBackendRef` and `tests.HTTPRouteInvalidNonExistentBackendRef` in `tests/e2e/conformance_test.go`.
- **Key Files Modified / Added**:
  - `pkg/state/referencegrant.go`
  - `pkg/state/referencegrant_test.go`
  - `pkg/state/state.go`
  - `pkg/state/httproute.go`
  - `pkg/state/httproute_test.go`
  - `pkg/state/gateway.go`
  - `pkg/state/gateway_test.go`
  - `pkg/controller/referencegrant_controller.go`
  - `pkg/controller/httproute_controller.go`
  - `pkg/controller/gateway_controller.go`
  - `pkg/controller/utils.go`
  - `cmd/gateway-api-reference-implementation/main.go`
  - `k8s/controller.yaml`
  - `tests/e2e/conformance_test.go`
  - `.agents/skills/implement-conformance-test/journal/httproute-invalid-backendref.md`

## 4. Validation & Results
- **Unit Tests**: All unit tests pass across `pkg/controller`, `pkg/proxy`, `pkg/state`.
- **Conformance Logs**:
  ```
  --- PASS: TestConformance (131.45s)
      --- PASS: TestConformance/HTTPRouteInvalidCrossNamespaceBackendRef (0.12s)
          --- PASS: TestConformance/HTTPRouteInvalidCrossNamespaceBackendRef/HTTPRoute_with_a_cross-namespace_BackendRef_and_no_ReferenceGrant_has_a_ResolvedRefs_Condition_with_status_False_and_Reason_RefNotPermitted (0.00s)
          --- PASS: TestConformance/HTTPRouteInvalidCrossNamespaceBackendRef/HTTP_Request_to_invalid_cross-namespace_backend_must_receive_a_500 (0.00s)
      --- PASS: TestConformance/HTTPRouteInvalidNonExistentBackendRef (0.12s)
          --- PASS: TestConformance/HTTPRouteInvalidNonExistentBackendRef/HTTPRoute_with_only_a_nonexistent_BackendRef_has_a_ResolvedRefs_Condition_with_status_False_and_Reason_BackendNotFound (0.00s)
          --- PASS: TestConformance/HTTPRouteInvalidNonExistentBackendRef/HTTP_Request_to_invalid_nonexistent_backend_receive_a_500 (0.00s)
  PASS
  ok      github.com/gke-labs/gateway-api-reference-implementation/tests/e2e
  ```
