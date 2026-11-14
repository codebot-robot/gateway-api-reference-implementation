# Design: Push Config Directly from Controller to Data Plane

## Context and Goals

Today the controller writes each Gateway's data-plane config to a Kubernetes
Secret (`config.json`), and the data plane watches that Secret. Direct push
from the controller to each data plane eliminates the intermediate Secret:

- **Faster.** Eliminates API server write and watch overhead between the
  controller and the data plane.
- **More observable.** The controller learns directly from the data plane which
  revision it applied, and why one failed.
- **Safer.** Keeps TLS private keys out of etcd and namespace Secrets, and
  removes the 1 MiB Secret size limit.

## Design Invariants

- The controller pushes compiled configuration directly to data planes over TLS.
- No component writes data-plane configurations or private keys to Secrets.
- The controller authenticates each data plane and delivers only its Gateway's config.
- The data plane acknowledges a revision only after its listeners accept connections.
- The data plane reports unready (`readyz=False`) whenever it lacks an applied config.

## 1. Who Connects to Whom

Two topology choices exist for establishing the configuration stream:

- **Controller dials data planes.** The controller discovers data-plane Pod IPs
  or EndpointSlices and dials an administrative port on each pod. Only the elected
  leader dials. This requires opening inbound admin ports on data-plane pods and
  configuring cross-namespace network policies to permit controller ingress.
- **Data planes dial controller.** Data planes dial a stable controller ClusterIP
  Service and maintain long-lived bidirectional gRPC streams. Standby controller
  replicas reject or proxy streams, or the Service endpoints track only the leader.
  This requires standard pod egress and opens no inbound ports on data planes.

The choice is the maintainer's. The recommendation is **data planes dial the
controller**: it simplifies firewalls, avoids pod endpoint discovery in the
controller, and naturally re-synchronizes restarting data planes on dial.

## 2. Authentication and Authorization

Each side proves its identity to ensure TLS private keys remain secure:

- **Data plane identity.** Data planes present projected ServiceAccount tokens
  (`audience: gari-controller`) over TLS. The controller verifies tokens via
  TokenReview or local JWT keys, identifying the pod's namespace and account.
- **Controller identity.** Data planes verify the controller's TLS certificate
  against the mounted cluster CA bundle (`ca.crt`).
- **Gateway scoping.** The controller verifies the ServiceAccount matches the
  Gateway (`ResourceNameForGateway(gw.Name)` in `gw.Namespace`). The controller
  sends only that Gateway's compiled config, protecting TLS private keys.

## 3. Start and Restart

When a data plane restarts while the controller is unreachable:

- **Serve nothing (fail closed).** The data plane binds no listeners and reports
  unready until it connects to the controller and receives its first config.
- **Serve from local cache.** The data plane writes applied config to an
  in-memory `emptyDir` volume (`/var/run/gari/cached-config.json`). On container
  restarts within the same pod, it loads the cache; fresh pods start empty.

Trade-offs: Serving nothing avoids storing private keys in container volumes
and prevents routing traffic to stale backends. Local caching preserves uptime
during controller outages but increases attack surface and risks split brain.

The choice is the maintainer's. The recommendation is **serve nothing (fail closed)**:
leader failovers take only seconds, making fail-closed safe and robust.

## 4. Acknowledgement

The push reply carries the applied revision or the reason the apply failed:

- **ACK/NACK framing.** The data plane replies with an ACK (`revision: R, status: Applied`)
  or a NACK (`revision: R, status: Failed, error: "<reason>"`).
- **Replaces polling.** Push replies replace the controller's HTTP polling of
  data-plane `/applied` endpoints with immediate, push-driven telemetry.
- **Programmed condition.** The controller derives `Programmed=True` when all
  active data-plane replicas acknowledge the desired revision ($R_{applied} == R_{desired}$)
  and their listeners accept connections. A NACK or timeout sets `Programmed=False`
  with reason `SyncError` and displays the data plane's error reason.

## 5. Readiness

When the data plane has no config yet, its `/readyz` probe returns HTTP 503
(unready) and it reports no applied revision. This directly follows the rule
that the data plane reports ready and reports an applied revision only after
its listeners bind and accept connections. Unconfigured data-plane pods remain
unready in Kubernetes, preventing services from routing traffic prematurely.

## 6. Other Data Planes

Non-standard data planes receive configuration through the same mechanism:

- **snigateway.** The data-plane container runs the tunnel client next to the
  proxy, connects via push to receive `DataplaneConfig`, and registers hostnames.
- **localgateway DNS.** The mDNS daemon receives published hostnames and addresses
  over push without mounting Secrets or holding Secret RBAC.
- **What is shared.** All data planes share the push client/server transport
  library (`pkg/dataplane/push`), the `DataplaneConfig` schema, token validation,
  and ACK/NACK lifecycle semantics.

## 7. Order of Work

The migration proceeds in small, backward-compatible steps:

1. **Protocol library.** Add the push transport package (`pkg/dataplane/push`)
   with gRPC definitions, token authentication, and ACK/NACK types.
2. **Data-plane push listener.** Add push receipt to `pkg/gari`, accepting
   pushed configs while keeping Secret watching active as a fallback.
3. **Controller push transmitter.** Update `singlepod.AddressProvider` to push
   configs directly to data planes alongside Secret publishing.
4. **Switch status derivation.** Derive `Programmed` from direct push ACKs.
   The in-flight applied-revision mirror and `/applied` polling continue until
   this step, which replaces them.
5. **Remove Secret path.** Remove `PublishDataplaneConfig` Secret generation,
   delete `dataplaneSecretReconciler`, and drop data-plane Secret RBAC.
