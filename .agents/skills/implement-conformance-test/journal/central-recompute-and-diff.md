# Conformance & Architecture Journal: Incremental State Step 2 - Central Recompute and Diff

## 1. Overview
- **Objective**: Implement Step 2 of `docs/incremental-state.md` ("Central recompute and diff").
- **Key Goals**:
  1. Record all inputs (including `GatewayClass` and `GatewayAddresses`) into `State`.
  2. Pure computation of all outputs via `ComputeOutputs(inputs ModelInputs) *Outputs` without reading previously written statuses.
  3. Recompute centrally with debouncing/coalescing in `State`, diff desired status against previous outputs, and push events on per-kind event channels (`GatewayEvents()`, `HTTPRouteEvents()`, `ListenerSetEvents()`, `BackendTLSPolicyEvents()`, `GatewayClassEvents()`).
  4. Simplify reconcilers to record inputs and write merged statuses using `WatchesRawSource(source.Channel(...))` while deleting all cross-object mapping functions (`EnqueueRequestsFromMapFunc`).
  5. Update proxy configuration and invoke `OnGatewaysUpdate` centrally once per recompute when proxy outputs change.
  6. Add unit tests verifying diffing, dependency updates without mapping functions, and a benchmark for recomputation scale.

## 2. Issue / Architectural Analysis
- **Previous Architecture**:
  - Each controller manually fetched dependency objects or re-ran partial compilation in its own reconciler.
  - 19 hand-written `EnqueueRequestsFromMapFunc` watches existed across Gateway, HTTPRoute, ListenerSet, BackendTLSPolicy, and ConfigMap reconcilers.
  - Reconcilers invoked `updateProxy()` independently, causing duplicate compilations and proxy updates.
  - `CompileModel` checked `len(gw.Status.Addresses) > 0`, making computation depend on status writes rather than pure recorded inputs.
- **Root Cause & Design Solutions**:
  - Centralized input tracking with a monotonic revision counter stamped on genuine input modifications (ignoring status-only echoes).
  - Provided clean separation: `ComputeOutputs` purely calculates desired statuses and proxy configs; `State.Recompute()` diffs and notifies; reconcilers merge status (preserving `LastTransitionTime` and foreign controller entries) and write to API server.

## 3. Implementation Details
- **Input State Management & Monotonic Revision Counter (`pkg/state/state.go`)**:
  - Added `GatewayClass` and `GatewayAddresses` recording to `State`.
  - Added mutation methods (`Upsert...` / `Delete...` / `SetGatewayAddresses`) that inspect relevant fields (`Spec`, `Labels`, `Generation`, `Data`, `Ports`, `Addresses`) and increment `revision uint64` only when inputs actually change.
  - Added event channels for all status-owning kinds and a background coalescing loop (`State.Start`) that debounces bursts of events.
- **Pure Output Computation (`pkg/state/compiled.go`)**:
  - Implemented `ComputeOutputs(ModelInputs) *Outputs` calculating desired statuses for Gateways, HTTPRoutes, ListenerSets, BackendTLSPolicies, GatewayClasses, proxy listeners/routes, certificates, and resolved gateways.
  - Added equality functions (`GatewayStatusesEqual`, `HTTPRouteStatusesEqual`, `ListenerSetStatusesEqual`, `PolicyStatusesEqual`, `GatewayClassStatusesEqual`) and write-time merge functions (`MergeGatewayStatus`, `MergeListenerSetStatus`, `MergeHTTPRouteStatus`, `MergeBackendTLSPolicyStatus`, `MergeGatewayClassStatus`).
- **Reconcilers Simplified (`pkg/controller/*`)**:
  - Replaced all cross-object mapping functions with `bldr.WatchesRawSource(source.Channel(r.State.<Kind>Events(), &handler.EnqueueRequestForObject{}))`.
  - Reconcilers now strictly record inputs into `State` and write merged desired status from `State`.
  - Removed reconciler-side proxy updating logic.
- **Testing & Benchmarks**:
  - Added unit tests in `pkg/state/state_test.go` verifying:
    - Unchanged inputs produce no events.
    - Single changes produce events only for affected objects.
    - Namespace label changes update route acceptance and emit events without any mapping function.
    - ReferenceGrant deletions update route backend resolution without any mapping function.
    - ConfigMap updates update BackendTLSPolicy validation without any mapping function.
  - Added `BenchmarkState_Recompute` in `pkg/state/state_benchmark_test.go` testing 100 Gateways and 1000 HTTPRoutes (~6.7ms per full recompute).

## 4. Key Files Modified / Added
- `pkg/state/state.go`
- `pkg/state/compiled.go`
- `pkg/state/conditions.go`
- `pkg/state/referencegrant.go`
- `pkg/state/state_test.go`
- `pkg/state/state_benchmark_test.go`
- `pkg/controller/gateway_controller.go`
- `pkg/controller/httproute_controller.go`
- `pkg/controller/listenerset_controller.go`
- `pkg/controller/backendtlspolicy_controller.go`
- `pkg/controller/configmap_controller.go`
- `pkg/controller/namespace_controller.go`
- `pkg/controller/referencegrant_controller.go`
- `pkg/controller/secret_controller.go`
- `pkg/controller/service_controller.go`
- `pkg/controller/utils.go`
- `pkg/controller/controller_test.go`
- `pkg/gari/gari_test.go`
- `.agents/skills/implement-conformance-test/journal/central-recompute-and-diff.md`

## 5. Validation & Results
- **Unit Tests & Benchmark**:
  - All unit tests across all packages pass (`pkg/controller`, `pkg/gari`, `pkg/provisioning/singlepod`, `pkg/proxy`, `pkg/state`, `snigateway`).
  - `BenchmarkState_Recompute` executed at ~6.7ms for 100 Gateways and 1000 HTTPRoutes.
- **Linting & Code Generation**:
  - `ap generate` and `ap lint` passed cleanly with 0 errors/warnings.
- **Conformance & E2E**:
  - All Gateway API conformance tests and E2E suites passed under `ap e2e`.
