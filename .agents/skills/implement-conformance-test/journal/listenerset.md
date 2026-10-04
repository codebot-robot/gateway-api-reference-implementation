# Conformance Test Journal: ListenerSet Support (Part 1)

## 1. Test Overview & Objectives

This journal documents the implementation of foundational support for `ListenerSet` (GEP-1713, Gateway API v1.6.2 `apis/v1/listenerset_types.go`). Part 1 focuses on accepting ListenerSets on a parent Gateway and routing HTTP traffic through them across five conformance tests:

1. `ListenerSetDefaultNotAllowed`:
   - Verifies that by default (when `spec.allowedListeners` is omitted on the Gateway), any `ListenerSet` referencing the Gateway is disallowed.
   - Status expectations:
     - `ListenerSet.Status.Conditions`: `Accepted: False` (`Reason: NotAllowed`), `Programmed: False` (`Reason: NotAllowed`).
     - `Gateway.Status.AttachedListenerSets`: `0` (or omitted / 0).
2. `ListenerSetAllowedNamespaceNone`:
   - Verifies that when a Gateway explicitly specifies `spec.allowedListeners.namespaces.from: None`, ListenerSets from any namespace (including the Gateway's own namespace) are rejected with `Accepted: False` (`Reason: NotAllowed`) and `Programmed: False` (`Reason: NotAllowed`).
   - `Gateway.Status.AttachedListenerSets`: `0`.
3. `ListenerSetAllowedNamespaceSame`:
   - Verifies that when `spec.allowedListeners.namespaces.from: Same` is configured:
     - ListenerSets in the same namespace as the Gateway are accepted (`Accepted: True`, `Programmed: True`, and per-listener status entries with `Accepted: True`, `Programmed: True`, `ResolvedRefs: True`).
     - ListenerSets in different namespaces targeting the Gateway are rejected (`Accepted: False`, `Reason: NotAllowed`).
     - `Gateway.Status.AttachedListenerSets`: `1`.
4. `ListenerSetAllowedNamespaceSelector`:
   - Verifies that when `spec.allowedListeners.namespaces.from: Selector` is configured with a `selector.matchLabels`:
     - ListenerSets in namespaces with matching labels are accepted.
     - ListenerSets in non-matching namespaces are rejected with `Accepted: False` (`Reason: NotAllowed`).
     - `Gateway.Status.AttachedListenerSets`: `1`.
5. `ListenerSetHTTPRouting`:
   - Verifies complete end-to-end data-plane routing with multiple ListenerSets attached to a Gateway on port 80.
   - HTTPRoutes attach directly to a `ListenerSet` via `spec.parentRefs` (`group: gateway.networking.k8s.io`, `kind: ListenerSet`, `name: <listenerset-name>`).
   - Verifies hostname isolation, path routing, sectionName matching on ListenerSet listeners, and `AttachedRoutes` counts on both the Gateway and the ListenerSet listener statuses.

## 2. Key Learnings, Spec Subtleties & Surprises

- **Default AllowedListeners Behavior**:
  - Unlike routes (where default allowed namespaces is `Same`), for ListenerSets `spec.allowedListeners` defaults to allowing **no** ListenerSets. A Gateway must explicitly configure `allowedListeners.namespaces.from` (`Same`, `All`, or `Selector`) to permit attachments.
- **Gateway Status `AttachedListenerSets` Field**:
  - Gateways track the count of attached ListenerSets in `gw.Status.AttachedListenerSets` (pointer to `int32`).
  - Conformance helper `GatewayMustHaveAttachedListeners` polls `gw.Status.AttachedListenerSets == expectedCount`.
- **ListenerSet Conditions & Status Hierarchy**:
  - Top-level `status.conditions` on a `ListenerSet` must report both `Accepted` and `Programmed`.
  - When disallowed, reason is `gatewayv1.ListenerSetReasonNotAllowed` ("NotAllowed").
  - When accepted, `status.listeners` contains an entry for each listener in `spec.listeners` with `name`, `supportedKinds` (e.g. `gateway.networking.k8s.io/HTTPRoute`), `attachedRoutes`, and conditions (`Accepted`, `Programmed`, `ResolvedRefs`).
- **HTTPRoute ParentRef to ListenerSet**:
  - HTTPRoutes can use `parentRef.kind: ListenerSet`.
  - The route namespace is evaluated against the `ListenerSet`'s listener `allowedRoutes` (not directly the Gateway's listeners).
  - When resolving the `ListenerSet` from the route's `parentRef`, the namespace must match exactly (defaulting to the route's namespace if `parentRef.namespace` is omitted).
  - If the parent `ListenerSet` is not accepted by its parent Gateway, the route attached to that `ListenerSet` must evaluate to `Accepted: False` (`Reason: NoMatchingParent`).
- **Internal Listener Name Scoping**:
  - Because a Gateway listener and a ListenerSet listener can share the same listener name (e.g. `http`), `InternalListener` names generated from ListenerSets must be qualified (such as `<namespace>/<listenerset>/<listener>`) to prevent collisions in internal maps and logs.
- **TLS Certificate Isolation**:
  - ListenerSet HTTPS/TLS listeners can specify `certificateRefs`. However, fallback `defaultCert` selection in the proxy data-plane must remain strictly bound to the parent Gateway listeners so that cross-namespace ListenerSets cannot hijack or overwrite the Gateway's fallback certificate.
- **Fast-Path Data Plane & Port Mapping**:
  - In GARI's single-pod fast path, the controller maps listener ports to the proxy service. In Part 1, all conformance tests utilize ports already mapped in `k8s/controller.yaml` (ports 80, 443, 8080, 8090). Dynamic port provisioning is deferred to future work.

## 3. Pitfalls & Guidance for Follow-Up (Part 2 & Effective Listener Model)

As part of Issue #611 (Step 1 of `docs/incremental-state.md`), the codebase was refactored into a single **Effective Listener** compiled model in `pkg/state/compiled.go`. Future ListenerSet work (conflicts, route kinds/namespaces, ReferenceGrant, parent sectionName, dual parentRefs, route status scoping) should build on this model:

- **Unified Model Architecture (`pkg/state/compiled.go`)**:
  - `CompileModel`: Takes snapshots of Gateways, ListenerSets, HTTPRoutes, Services, BackendTLSPolicies, ConfigMaps, Secrets, Namespaces, and ReferenceGrantValidator. Compiles a `*CompiledModel` with `Gateways map[types.NamespacedName]*CompiledGateway` and `HTTPRoutes map[types.NamespacedName]*CompiledRoute`.
  - `EffectiveListener`: Represents a compiled listener originating from either a Gateway or an attached ListenerSet. Records `Owner` (`Kind`, `Namespace`, `Name`), `ParentGateway`, listener spec (`Name`, `Port`, `Protocol`, `Hostname`, `TLS`, `AllowedRoutes`), `SupportedKinds`, `Conditions`, `AttachedRoutes`, and `Routes`.
- **Where to Add Listener Validation & Conflicts**:
  - `ValidateListener` (`pkg/state/compiled.go`): Runs once over any listener spec (Gateway or ListenerSet). Protocol support, route kind validity, TLS configuration, secret existence, keypair checks, and cross-namespace `ReferenceGrant` checks (with `Owner.Kind` and `Owner.Namespace`) are all located here.
  - For conflict detection across a Gateway and its ListenerSets (Part 2), add a pass over `cg.EffectiveListeners` in `CompileModel`.
- **Where Route Binding Happens**:
  - `bindRouteParentRef` (`pkg/state/compiled.go`): Resolves parent (Gateway or ListenerSet -> parent Gateway), candidate effective listeners, sectionName/port matching, protocol compatibility, `AllowedRoutes` kinds, `AllowedRoutes` namespaces (evaluated against `el.Owner.Namespace`), and hostname intersection.
  - The binding pass increments `el.AttachedRoutes` and appends `InternalRoute` to `el.Routes` in one place.
- **Where Status Is Computed**:
  - Pure functions in `pkg/state/compiled.go`:
    - `ComputeDesiredGatewayStatus(gw, compiledGw, providedAddresses)`
    - `ComputeDesiredListenerSetStatus(ls, parentGW, namespaces, compiledGw)`
    - `ComputeDesiredHTTPRouteStatus(route, compiledRoute, controllerName)`
  - Controllers (`GatewayReconciler`, `ListenerSetReconciler`, `HTTPRouteReconciler`) call these pure functions and update Kubernetes status when `updated == true`.
- **Certificate Extraction**:
  - `ExtractCertificates(gateways, secrets, refValidator)`: Extracts TLS certificates across all effective listeners with owner-aware `ReferenceGrant` validation. Fallback `defaultCert` selection is restricted to listeners where `el.Owner.Kind == "Gateway"`.

## 4. Conformance Test Results

The suite successfully passes all enabled tests:
```
--- PASS: TestConformance (148.25s)
    --- PASS: TestConformance/ListenerSetAllowedNamespaceNone (2.61s)
    --- PASS: TestConformance/ListenerSetAllowedNamespaceSame (4.21s)
    --- PASS: TestConformance/ListenerSetAllowedNamespaceSelector (4.01s)
    --- PASS: TestConformance/ListenerSetDefaultNotAllowed (3.41s)
    --- PASS: TestConformance/ListenerSetHTTPRouting (4.38s)
```
