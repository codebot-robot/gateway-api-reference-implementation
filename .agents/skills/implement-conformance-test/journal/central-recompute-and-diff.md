# Conformance & Architecture Journal: Incremental State Step 2 - Central Recompute and Diff

## 1. Overview
- **Objective**: Implement Step 2 of `docs/incremental-state.md` ("Central recompute and diff").
- **Key Goals**:
  1. Record all inputs (including `GatewayClass` and `GatewayAddresses`) into `State`.
  2. Feed `State` straight from informer event handlers, marking state synced only after all informer registrations report `HasSynced() == true`.
  3. Pure computation of all outputs via `ComputeOutputs(inputs ModelInputs) *Outputs` without reading previously written statuses.
  4. Recompute centrally with debouncing/coalescing in `State`, diff desired status against previous outputs, and push events directly into controller workqueues via custom `EventSource` (never dropping events under load).
  5. Reconcilers only write status from computed outputs, skipping Gateways whose GatewayClass is not managed by us.
  6. Execute callbacks (`UpdateConfig`, `UpdateCertificates`, `onGatewaysUpdate`) outside `s.mu` to prevent deadlocks.
  7. Deterministic `status.parents` ordering (#627) preserved with no unnecessary API writes for order-only differences.
  8. Delete unneeded non-status reconcilers (Secret, Namespace, ConfigMap, Service, ReferenceGrant) and all 19 cross-object mapping functions.
  9. Add unit tests and recomputation benchmark.

## 2. Issue / Architectural Analysis
- **Previous Architecture**:
  - Each controller manually fetched dependency objects or re-ran partial compilation in its own reconciler.
  - 19 hand-written `EnqueueRequestsFromMapFunc` watches existed across Gateway, HTTPRoute, ListenerSet, BackendTLSPolicy, and ConfigMap reconcilers.
  - Reconcilers invoked `updateProxy()` independently, causing duplicate compilations and proxy updates.
  - `CompileModel` checked `len(gw.Status.Addresses) > 0`, making computation depend on status writes rather than pure recorded inputs.
- **Root Cause & Design Solutions**:
  - Centralized input tracking with a monotonic revision counter stamped on genuine input modifications (ignoring status-only echoes).
  - Provided clean separation: `ComputeOutputs` purely calculates desired statuses and proxy configs; `State.Recompute()` diffs and notifies; reconcilers merge status (preserving `LastTransitionTime` and foreign controller entries) and write to API server.
  - Feed `State` directly from informer event handlers (`AddEventHandler`), eliminating redundant reconcilers.
  - Custom `EventSource` sends reconcile requests directly to controller workqueues, avoiding channel drops.

## 3. Implementation Details
- **Informer Event Handlers & Sync (`pkg/controller/utils.go`, `pkg/state/state.go`)**:
  - Added informer event handlers for all 10 input types (`GatewayClass`, `Gateway`, `ListenerSet`, `HTTPRoute`, `BackendTLSPolicy`, `Service`, `Secret`, `ConfigMap`, `Namespace`, `ReferenceGrant`).
  - `State.Start` waits for all registrations to report `HasSynced() == true` before enabling output computation (`SetSynced(true)`), preventing partial-world flapping and startup 404s.
- **Lock-Free Callbacks (`pkg/state/state.go`)**:
  - Captured proxy configuration and gateway slice under `s.mu`, released `s.mu`, and executed `proxy.UpdateConfig`, `proxy.UpdateCertificates`, and `onGatewaysUpdate` outside the lock.
- **EventSource Workqueue Delivery (`pkg/state/state.go`)**:
  - Implemented `EventSource` implementing `source.TypedSource[reconcile.Request]` that enqueues directly to controller rate-limiting workqueues.
- **GatewayClass Filtering & Gateway Reconciler (`pkg/controller/gateway_controller.go`)**:
  - `GatewayReconciler` verifies `gc.Spec.ControllerName == r.ControllerName`. If not ours, deletes from `State` and exits without calling `AddressProvider` or writing status.
  - `CompileModel` initializes `managedGatewayClasses` to an empty map so Gateways are not compiled until their GatewayClass is known.
- **Deterministic Route Parents Ordering (`pkg/state/compiled.go`, `pkg/state/conditions.go`)**:
  - Kept #627's deterministic parentRef sorting and other controller preservation in `UpdateRouteParentStatuses` / `MergeHTTPRouteStatus`.
- **Testing & Benchmarks**:
  - Added unit tests in `pkg/state/state_test.go` and `pkg/controller/controller_test.go`.
  - Added `BenchmarkState_Recompute` in `pkg/state/state_benchmark_test.go` testing 100 Gateways and 1000 HTTPRoutes (~2.7ms per full recompute).

## 4. Key Files Modified / Added / Deleted
- `pkg/state/state.go`
- `pkg/state/compiled.go`
- `pkg/state/conditions.go`
- `pkg/state/gateway.go`
- `pkg/state/referencegrant.go`
- `pkg/state/state_test.go`
- `pkg/state/state_benchmark_test.go`
- `pkg/controller/gateway_controller.go`
- `pkg/controller/httproute_controller.go`
- `pkg/controller/listenerset_controller.go`
- `pkg/controller/backendtlspolicy_controller.go`
- `pkg/controller/utils.go`
- `pkg/controller/controller_test.go`
- (Deleted: `configmap_controller.go`, `namespace_controller.go`, `referencegrant_controller.go`, `secret_controller.go`, `service_controller.go`)
- `.agents/skills/implement-conformance-test/journal/central-recompute-and-diff.md`

## 5. Validation & Results
- **Unit Tests & Benchmark**:
  - All unit tests across all packages pass (`pkg/controller`, `pkg/gari`, `pkg/provisioning/singlepod`, `pkg/proxy`, `pkg/state`, `snigateway`).
  - `BenchmarkState_Recompute` executed at ~2.7ms for 100 Gateways and 1000 HTTPRoutes.
- **Linting & Code Generation**:
  - `ap generate` and `ap lint` passed cleanly with 0 errors/warnings.
- **Conformance & E2E**:
  - All Gateway API conformance tests and E2E suites passed under `ap e2e`.
