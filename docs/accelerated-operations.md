# Accelerated Operations: Fallback + Acceleration

This document describes the model we are working towards: GARI implements
every Gateway API feature, and high-performance implementations "accelerate"
the subset of configurations they support, falling back to GARI for
everything else.

## The problem

Gateway API is a specification without a reference implementation. Every
feature has to be built independently by every implementation, so:

- New features reach users slowly. A feature is not really usable until the
  implementation you run has added it, and then shipped a version with it.
- It is hard to know what you have. Whether a given HTTPRoute works depends on
  which implementation you run, which version, and which optional features it
  supports. Conformance reports help, but users usually find out by trying.
- Implementations spend effort on the long tail. Every implementation has to
  build (and maintain) rarely used features, which takes time away from
  making the common path fast.

## The idea

GARI flips this around. The reference implementation implements
*everything*, in pure Go, to a tolerable standard. It aims for clarity and
correctness rather than raw performance, and its target is to pass the full
conformance suite.

Top-tier implementations (Envoy, nginx, cloud load balancers, eBPF datapaths,
...) are still important. But instead of each one building every feature,
they should *embrace* the reference implementation:

- **GARI is the fallback.** Any configuration that is valid Gateway API works,
  because GARI can serve it.
- **Acceleration is an extension.** An accelerator declares which features it
  can handle. If an HTTPRoute uses only accelerated functionality, its traffic
  is offloaded to the accelerator. Otherwise it stays on GARI.

So a user never has to ask "does my implementation support this feature?"
The answer is always yes. The only question is whether it is *fast*, and
that is a performance question rather than a correctness question.

For accelerator authors, the bar to entry drops a lot. An accelerator can
start with exact/prefix path matching on a single backend and still be
useful, because everything else keeps working through GARI. It can then add
features in order of how much traffic they carry.

## How it fits the current architecture

GARI already has a clean boundary between control plane and data plane:

1. The controllers (`pkg/controller`) watch Gateway API and core Kubernetes
   objects and record them in `pkg/state`.
2. `pkg/state` resolves those objects (listener attachment, hostname
   intersection, backendRefs, filters, ...) into a list of
   `state.InternalRoute`.
3. The proxy (`pkg/proxy`) serves traffic from that list.

All the hard Gateway API semantics (attachment, precedence, status
conditions) live in steps 1 and 2. Acceleration fits at the same boundary:

```
Gateway API objects ──► pkg/state ──► []InternalRoute ──┬─► accelerator (supported routes)
                                                        └─► GARI proxy  (everything else)
```

An accelerator gets the same resolved model that GARI's own proxy consumes,
so it does not need to reimplement Gateway API semantics. It only has to
program its datapath for the routes it accepts.

## Deciding what to accelerate

Each accelerator advertises the capabilities it supports, for example:

- match types (exact/prefix path, headers, query params, method)
- filters (header modifiers, redirects, URL rewrites, mirroring, CORS)
- backend features (weights, BackendTLSPolicy, h2c, WebSocket)
- timeouts and retries

For each route, GARI checks the features it uses against those capabilities.
A route is accelerated only if *every* feature it uses is supported;
otherwise it stays on GARI. That decision is made per route, not per
Gateway, so one exotic route does not slow down the rest.

Open design questions:

- **Granularity.** Routes on the same listener interact through precedence
  rules (for example, a more specific match on a fallback route must win over
  a less specific match on an accelerated route). The unit of offload may
  need to be a set of routes that can be evaluated independently, such as a
  hostname on a listener, rather than a single HTTPRoute.
- **Traffic steering.** How traffic reaches GARI for non-accelerated routes:
  the accelerator forwarding unmatched requests to GARI, separate
  addresses/listeners, or SNI-level splitting (see
  [sni-reverse-tunnel.md](sni-reverse-tunnel.md)).
- **Status.** Whether, and how, to show users that a route is accelerated (for
  example, a condition or annotation on the HTTPRoute) so that "it works but
  it is slow" can be diagnosed.
- **Correctness.** An accelerator should behave like GARI for the routes it
  accepts. Running the conformance suite with the accelerator enabled, so
  that some tests hit the accelerator and others the fallback, is a natural
  way to check that.

## Examples

We plan to include accelerator examples in this repository, to prove that the
extension point is real rather than theoretical. The first is an SNI proxy on
a front-end node that reverse-tunnels traffic into the cluster; see
[sni-reverse-tunnel.md](sni-reverse-tunnel.md).
