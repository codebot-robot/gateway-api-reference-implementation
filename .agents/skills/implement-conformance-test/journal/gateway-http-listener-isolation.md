# Conformance Test Journal: GatewayHTTPListenerIsolation

## 1. Test Overview
- **Name**: `GatewayHTTPListenerIsolation`
- **Description**: Verifies listener isolation for HTTP listeners on a Gateway with multiple listeners on the same port having overlapping hostnames (catch-all `""`, `*.example.com`, `*.foo.example.com`, `abc.foo.example.com`). Ensures requests route only to the most specific listener matching the request Host header and not across listeners.
- **Manifests**:
  - `tests/gateway-http-listener-isolation.yaml` (v1.6.2)
  - `tests/gateway-http-listener-isolation-with-hostname-intersection.yaml` (v1.6.2)

## 2. Issue / Failure Analysis
- **Observed Behavior**: The test was not yet included in `selectedTests` in `tests/e2e/conformance_test.go`.
- **Root Cause & Architecture Verification**:
  - The Gateway API specification requires that on Gateways with multiple HTTP listeners on the same port, the single most specific listener matching the request Host header is selected (Exact > Longest Wildcard > Catch-All).
  - Routes attached to less specific listeners must not receive requests matching a more specific listener even if their path rules match.
  - In our reference implementation, `pkg/state.MatchListeners` correctly calculates match specificity (`ExactMatch`, `WildcardMatch` with longest prefix/suffix match, `CatchAllMatch`) and isolates routing to the highest-specificity matching listener(s). In the compiled model, each `EffectiveListener` / `InternalListener` retains only its own attached `Routes`, ensuring listener isolation without merging across listeners.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Added `tests.GatewayHTTPListenerIsolation` to `selectedTests` in `tests/e2e/conformance_test.go` in alphabetical order.
  2. Added table-driven unit tests in `pkg/state/compiled_test.go` reproducing the complete 16-(Host, Path) conformance matrix across both variants (hostnames configured only on listeners, and hostnames intersecting with HTTPRoute hostnames) at the `ComputeOutputs` and proxy-route level.
  3. Verified all unit tests and conformance suite via `ap test` and `ap e2e`.
- **Key Files Modified**:
  - `tests/e2e/conformance_test.go`
  - `pkg/state/compiled_test.go`
  - `.agents/skills/implement-conformance-test/journal/gateway-http-listener-isolation.md`

## 4. Validation & Results
- **Unit Tests**: Ran `GOTOOLCHAIN=auto go test -v ./pkg/state -run TestGatewayHTTPListenerIsolation` and `go run github.com/gke-labs/gke-labs-infra/ap@latest test`. All 32 table-driven test cases passed.
- **Conformance Logs**:
  ```
  --- PASS: TestConformance (117.06s)
      --- PASS: TestConformance/GatewayHTTPListenerIsolation (19.05s)
          --- PASS: TestConformance/GatewayHTTPListenerIsolation/hostnames_are_configured_only_in_listeners (4.23s)
          --- PASS: TestConformance/GatewayHTTPListenerIsolation/intersecting_hostnames_are_configured_in_listeners_and_HTTPRoutes (2.18s)
  PASS
  ```
