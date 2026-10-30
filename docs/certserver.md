# Automatic TLS Certificates via certserver

GARI controllers (`gateway-api-reference-implementation` and `gcpgateway`) support obtaining TLS certificates automatically from `certserver` using GARI's pluggable certificate source mechanism and certserver's authenticated `GetCertificate` gRPC API.

This document describes how to configure `certserver`, set up controller manifests and flags, and request certificates on Gateway listeners.

---

## 1. Registering the Cluster's OIDC Issuer with certserver

`certserver` validates controller requests by authenticating projected ServiceAccount tokens offline against the originating cluster's OIDC discovery endpoint and JSON Web Key Set (JWKS).

Before a controller can request certificates, register the cluster in `certserver`'s configuration:

```yaml
clusters:
- clusterID: "cluster-prod-1"
  issuerURL: "https://container.googleapis.com/v1/projects/my-gcp-project/locations/us-central1/clusters/my-cluster"
  jwksURL: "https://container.googleapis.com/v1/projects/my-gcp-project/locations/us-central1/clusters/my-cluster/jwks"
  controllerNamespace: "default"
  controllerServiceAccount: "gari-controller"  # or gcpgateway-controller
  allowedHostnames:
  - "echo.example.com"
  - "*.example.com"
```

You can discover your Kubernetes cluster's OIDC issuer URL with:
```bash
kubectl get --raw /.well-known/openid-configuration
```

---

## 2. Controller Configuration and Manifests

### Projected ServiceAccount Token Volume and Deployment Configuration

Controllers authenticate to `certserver` using a projected service-account token volume with audience `certserver` and 10-minute validity.

The default GARI manifest `k8s/controller.yaml` is certserver-free so standard installs without certserver work out of the box. To enable certserver, add the projected token volume, volume mount, and `--certserver-*` flags to your controller Deployment (or apply as a kustomize overlay):

```yaml
      containers:
      - name: controller
        args:
        - --dataplane-image=$(DATAPLANE_IMAGE)
        - --certserver-address=certserver.gateway-system.svc:8443
        - --certserver-ca-bundle-path=/etc/ssl/certs/certserver-ca.crt
        - --certserver-token-path=/var/run/secrets/tokens/certserver/token
        volumeMounts:
        - name: certserver-token
          mountPath: /var/run/secrets/tokens/certserver
          readOnly: true
      volumes:
      - name: certserver-token
        projected:
          sources:
          - serviceAccountToken:
              audience: certserver
              expirationSeconds: 600
              path: token
```

### Command-Line Flags and Environment Variables

Every GARI controller binary supports the following certserver flags (and corresponding environment variables):

| Flag | Env Var | Description |
| --- | --- | --- |
| `--certserver-address` | `CERTSERVER_ADDRESS` | The `host:port` address of the certserver gRPC service (e.g. `certserver.gateway-system.svc:8443`). |
| `--certserver-ca-bundle-path` | `CERTSERVER_CA_BUNDLE_PATH` | Path to the CA bundle PEM file to verify certserver TLS (e.g. `/etc/ssl/certs/certserver-ca.crt`). |
| `--certserver-token-path` | `CERTSERVER_TOKEN_PATH` | Path to the projected service-account token file (e.g. `/var/run/secrets/tokens/certserver/token`). |

If certserver flags are omitted, the `certserver` source is not registered, and any listener requesting it will report an `Unknown certificate source "certserver"` condition.

---

## 3. Requesting Certificates on Gateway Listeners

To request a certificate from `certserver`, configure an HTTPS listener on a `Gateway` with:
- An exact, non-wildcard `hostname`.
- TLS mode `Terminate`.
- The option `gateway.gke-labs.io/certificate-source: certserver` under `tls.options`.

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: prod-gw
  namespace: default
spec:
  gatewayClassName: reference-class # or gcpgateway
  listeners:
  - name: https
    protocol: HTTPS
    port: 443
    hostname: "echo.example.com"
    tls:
      mode: Terminate
      options:
        gateway.gke-labs.io/certificate-source: certserver
```

---

## 4. Lifecycle, Caching, and Renewal

1. **Initial Request**: When the controller reconciles the Gateway listener, it initiates certificate issuance with `certserver`. While issuance is in progress, the listener condition reports `Pending`.
2. **Delivery & Deployment**: Once issued, the certificate and private key are cached in-memory and deployed to the Gateway data plane. The listener condition transitions to `Programmed=True`.
3. **Background Renewal**: Certificates are monitored and automatically renewed before expiry. If a renewal attempt fails while the existing certificate is still valid, the current valid certificate is preserved to avoid service disruption.
