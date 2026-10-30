# Single-pod Provisioning Parameters via ConfigMap

In `singlepod` mode, each Gateway gets its own data plane Deployment and Kubernetes Service. By default, the generated Service is created with `type: LoadBalancer` and `externalTrafficPolicy: Cluster`.

For advanced deployment architectures—such as deploying behind an existing cloud load balancer via Google Cloud Network Endpoint Groups (NEGs) on GKE, or preserving client source IPs with direct L4—you can customize the Service type, traffic policy, and Service annotations via a `parametersRef` pointing to a `ConfigMap`.

## Parameters Reference

A `parametersRef` can be set on both `GatewayClass` (`spec.parametersRef`) and `Gateway` (`spec.infrastructure.parametersRef`). Gateway values override GatewayClass values.

The referent must be a `ConfigMap` in the core API group (`group: ""`):
- On `GatewayClass`, `spec.parametersRef.namespace` is required and points to the namespace containing the ConfigMap.
- On `Gateway`, `spec.infrastructure.parametersRef` resolves the ConfigMap in the same namespace as the Gateway.

### Supported Keys

The `ConfigMap.data` can contain the following keys:

| Key | Allowed Values | Default | Description |
| --- | --- | --- | --- |
| `serviceType` | `LoadBalancer`, `ClusterIP` | `LoadBalancer` | The Kubernetes `Service.spec.type`. |
| `externalTrafficPolicy` | `Cluster`, `Local` | `Cluster` | The `Service.spec.externalTrafficPolicy`. Only meaningful when `serviceType` is `LoadBalancer`. |
| `serviceAnnotations` | YAML or JSON map | (none) | Key-value annotations applied to the generated Service only (in addition to `spec.infrastructure.annotations`). |
| `acmeChallenge` | `enabled`, `disabled` | (disabled) | Enables ACME HTTP-01 challenge forwarding to certserver. Forwarding is active when the controller has an upstream configured (via `--acme-challenge-upstream-url`) and `acmeChallenge: enabled` is set on the Gateway or GatewayClass parameters ConfigMap. |

Unknown keys or invalid values will cause the GatewayClass or Gateway to be rejected with condition `Accepted=False` and reason `InvalidParameters`.

### ACME Challenge Forwarding

When data-plane pods forward ACME HTTP-01 challenge requests to an upstream `certserver` using their own identity:
- **How it is turned on**: Challenge forwarding is active for a Gateway when the controller has an upstream certificate server URL configured (via `--acme-challenge-upstream-url`) **and** the Gateway's parameters ConfigMap (`parametersRef`) has `acmeChallenge: enabled`.
- **What is provisioned**: When enabled, the singlepod provisioner adds a projected service-account token volume (`certserver-token`, audience `certserver`, 10-minute expiry) for the Gateway's own data-plane service account, mounts it at `/var/run/secrets/tokens/certserver`, and passes the challenge-forwarding flags (`--acme-challenge-upstream-url`, `--acme-challenge-token-path`, and optionally `--acme-challenge-ca-bundle-path`) to the data-plane Deployment.
- **Turning off**: Setting `acmeChallenge: disabled` or omitting it from the ConfigMap removes the projected volume, volume mount, and flags from the Deployment.

### ClusterIP Address Rule

When `serviceType` is set to `ClusterIP`:
- No external cloud load balancer is created for the Service.
- The address reported in `Gateway.status.addresses` comes from `spec.addresses` (the IP or Hostname of the external load balancer you operate).
- Addresses are reported once the data-plane Deployment is ready (`AvailableReplicas > 0` or `ReadyReplicas > 0`).
- If `spec.addresses` is empty, the Gateway reports `Programmed=False` with reason `AddressNotAssigned` explaining that a ClusterIP Gateway requires a static address.

---

## Example: GKE Deployment with ClusterIP and Standalone NEGs

When deploying GARI on Google Kubernetes Engine (GKE) behind an external Google Cloud Load Balancer (such as an external Application Load Balancer), you want the GARI data plane pods to be reachable only through your existing front door via Standalone Network Endpoint Groups (NEGs), rather than spinning up an unnecessary public passthrough Network Load Balancer.

### 1. Define the Parameters ConfigMap

Create a `ConfigMap` configuring `serviceType: ClusterIP` and the GKE NEG annotation (`cloud.google.com/neg`):

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: gke-neg-params
  namespace: default
data:
  serviceType: ClusterIP
  serviceAnnotations: |
    cloud.google.com/neg: '{"exposed_ports": {"80": {"name": "my-gateway-80"}, "443": {"name": "my-gateway-443"}}}'
```

> **Note:** GKE backend services reference NEGs by name, so specifying fixed NEG names simplifies backend service configuration. Two Services cannot claim the same NEG name within the same cluster and region.

### 2. Define the Gateway

Configure the `Gateway` with:
- `spec.infrastructure.parametersRef` pointing to the `ConfigMap`.
- `spec.addresses` containing the static external IP address of your Google Cloud Load Balancer.

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: gke-gateway
  namespace: default
spec:
  gatewayClassName: reference-class
  addresses:
  - type: IPAddress
    value: "34.120.254.10" # External IP of your Google Cloud Load Balancer
  listeners:
  - name: http
    protocol: HTTP
    port: 80
    allowedRoutes:
      namespaces:
        from: Same
  - name: https
    protocol: HTTPS
    port: 443
    tls:
      mode: Terminate
      certificateRefs:
      - name: gke-gateway-tls
    allowedRoutes:
      namespaces:
        from: Same
  infrastructure:
    parametersRef:
      group: ""
      kind: ConfigMap
      name: gke-neg-params
```

### 3. What Happens

1. The single-pod provisioner creates a `ClusterIP` Service named `gke-gateway` with the annotation `cloud.google.com/neg: '{"exposed_ports": {"80": {}, "443": {}}}'`.
2. The GKE NEG controller detects the annotation and automatically creates Standalone NEGs containing the endpoints of the `gke-gateway` data-plane pods.
3. You attach these NEGs as backends to your Google Cloud external Application Load Balancer backend service.
4. GARI reports `Programmed=True` and populates `status.addresses` with `34.120.254.10` once the data plane Deployment is available.
