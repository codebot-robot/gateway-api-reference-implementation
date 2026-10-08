# Conformance Test Journal: TLSRouteInvalidReferenceGrant

## 1. Test Overview
- **Name**: `TLSRouteInvalidReferenceGrant`
- **Description**: Verifies that a cross-namespace backendRef from a TLSRoute without a valid covering ReferenceGrant (e.g. ReferenceGrants with mismatched from-group, from-kind, to-group, to-kind, namespace, or name) is rejected with condition `ResolvedRefs=False` and reason `RefNotPermitted`.
- **Manifests**: `sigs.k8s.io/gateway-api/conformance/tests/tlsroute-invalid-reference-grant.yaml` (v1.6.2)

## 2. Issue / Failure Analysis
- **Observed Behavior**: Cross-namespace backend references in TLSRoutes require a matching ReferenceGrant in the target namespace from `gateway.networking.k8s.io/TLSRoute` to the target service.
- **Root Cause**: `CompileTLSRoute` checks `svcNamespace != route.Namespace` against `ReferenceGrantValidator`. If not permitted, it sets `RouteConditionResolvedRefs=False` with reason `RouteReasonRefNotPermitted`.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Verified `CompileTLSRoute` and `ReferenceGrantValidator` correctly evaluate cross-namespace permissions for `TLSRoute`.
  2. Added `tests.TLSRouteInvalidReferenceGrant` to `selectedTests` in `tests/e2e/conformance_test.go`.
- **Key Files Modified**:
  - `tests/e2e/conformance_test.go`

## 4. Validation & Results
- **Unit Tests**:
  - `pkg/state/tlsroute_test.go`: `TestTLSRoute_AttachedRoutes` (cross-namespace with and without ReferenceGrant)
- **Conformance Logs**: Verified pass under `ap e2e`.
