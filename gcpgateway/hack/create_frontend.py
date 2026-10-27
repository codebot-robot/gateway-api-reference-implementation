#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Idempotently provisions Google Cloud Load Balancing resources and zonal NEGs for gcpgateway."""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, List, Optional


def make_hash(cluster_id: str, namespace: str, gateway_name: str) -> str:
    """Returns the 8-character lowercase hex SHA-256 hash of cluster_id/namespace/gateway_name."""
    raw = f"{cluster_id}/{namespace}/{gateway_name}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:8]


def make_resource_name(
    cluster_id: str,
    namespace: str,
    gateway_name: str,
    suffix: str,
    prefix: str = "gcpgw",
) -> str:
    """Formats the resource name as <prefix>-<8-char-hash>-<suffix>."""
    h = make_hash(cluster_id, namespace, gateway_name)
    if suffix:
        return f"{prefix}-{h}-{suffix}"
    return f"{prefix}-{h}"


def make_description(
    cluster_id: str,
    namespace: str,
    gateway_name: str,
    port: int = 0,
) -> str:
    """Encodes resource identity metadata as a JSON string for the description field."""
    return json.dumps(
        {
            "cluster-id": cluster_id,
            "namespace": namespace,
            "gateway-name": gateway_name,
            "port": port,
        },
        separators=(",", ":"),
    )


def default_run_gcloud(args: List[str], check: bool = True) -> Optional[Any]:
    cmd = ["gcloud"] + args + ["--format=json"]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        if not check:
            return None
        raise RuntimeError(f"Command {' '.join(cmd)} failed:\n{proc.stderr.strip()}")
    stdout = proc.stdout.strip()
    if not stdout:
        return {}
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return stdout


def get_access_token(runner: Callable[[List[str], bool], Optional[Any]]) -> str:
    """Retrieves an access token using gcloud auth print-access-token or environment variables."""
    env_token = os.environ.get("CLOUDSDK_AUTH_ACCESS_TOKEN") or os.environ.get("GOOGLE_OAUTH_ACCESS_TOKEN")
    if env_token:
        return env_token.strip()

    token_info = runner(["auth", "print-access-token"], True)
    if isinstance(token_info, dict):
        token = token_info.get("access_token") or token_info.get("token")
        if token:
            return token.strip()
    elif isinstance(token_info, str):
        return token_info.strip().strip('"')
    raise RuntimeError("Unable to extract access token from runner output")


# Note: We intentionally use standard library urllib with a Bearer token from 'gcloud auth print-access-token'
# rather than importing the official 'google-cloud-compute' library. This avoids introducing heavy external
# pip dependencies (google-cloud-compute, google-auth, protobuf, grpcio) for an operator script where all other
# resources are managed via gcloud CLI, and ensures authentication uses the active gcloud identity without
# requiring separate Application Default Credentials (ADC) setup.
def default_post_rest(url: str, body: Dict[str, Any], token: str) -> Dict[str, Any]:
    """Posts a JSON payload to a Google Cloud REST API endpoint using an OAuth 2.0 Bearer token."""
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            content = resp.read().decode("utf-8")
            return json.loads(content) if content else {}
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"REST POST to {url} failed with status {e.code}:\n{error_body}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"REST POST to {url} failed: {e.reason}") from e


def provision_frontend(
    project: str,
    region: str,
    cluster_id: str,
    namespace: str,
    gateway_name: str,
    network: str = "default",
    subnet: str = "default",
    prefix: str = "gcpgw",
    draining_timeout: int = 35,
    zones: Optional[List[str]] = None,
    runner: Callable[[List[str], bool], Optional[Any]] = default_run_gcloud,
    rest_poster: Optional[Callable[[str, Dict[str, Any]], Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Provisions all required GCP resources idempotently and returns the configuration dictionary."""
    if rest_poster is None:
        token: Optional[str] = None

        def _poster(u: str, b: Dict[str, Any]) -> Dict[str, Any]:
            nonlocal token
            if token is None:
                token = get_access_token(runner)
            return default_post_rest(u, b, token)

        rest_poster = _poster

    if not zones:
        zone_info = runner(
            ["compute", "zones", "list", f"--filter=region:({region})", f"--project={project}"],
            True,
        )
        if isinstance(zone_info, list) and zone_info:
            zones = sorted([z["name"] for z in zone_info if "name" in z])
        else:
            zones = [f"{region}-a", f"{region}-b", f"{region}-c"]

    # 1. Global Static IPv4 Address
    ip_name = make_resource_name(cluster_id, namespace, gateway_name, "ip", prefix)
    desc_ip = make_description(cluster_id, namespace, gateway_name, 0)
    ip_info = runner(
        ["compute", "addresses", "describe", ip_name, "--global", f"--project={project}"],
        False,
    )
    if not ip_info:
        runner(
            [
                "compute", "addresses", "create", ip_name,
                "--global", "--ip-version=IPV4",
                f"--description={desc_ip}",
                f"--project={project}",
            ],
            True,
        )
        ip_info = runner(
            ["compute", "addresses", "describe", ip_name, "--global", f"--project={project}"],
            True,
        )
    ip_address = ip_info.get("address", "34.120.254.10") if isinstance(ip_info, dict) else "34.120.254.10"

    # 2. HTTP Health Check on Port 8999
    hc_name = make_resource_name(cluster_id, namespace, gateway_name, "hc", prefix)
    desc_hc = make_description(cluster_id, namespace, gateway_name, 8999)
    if not runner(["compute", "health-checks", "describe", hc_name, "--global", f"--project={project}"], False):
        runner(
            [
                "compute", "health-checks", "create", "http", hc_name,
                "--global", "--port=8999", "--request-path=/_gari/readyz",
                "--check-interval=5s", "--timeout=5s", "--healthy-threshold=2", "--unhealthy-threshold=2",
                f"--description={desc_hc}",
                f"--project={project}",
            ],
            True,
        )

    # 3. Zonal NEGs
    for port in [8443, 8080]:
        neg_name = make_resource_name(cluster_id, namespace, gateway_name, str(port), prefix)
        desc_neg = make_description(cluster_id, namespace, gateway_name, port)
        for zone in zones:
            if not runner(["compute", "network-endpoint-groups", "describe", neg_name, f"--zone={zone}", f"--project={project}"], False):
                url = f"https://compute.googleapis.com/compute/v1/projects/{project}/zones/{zone}/networkEndpointGroups"
                net_url = network if ("/" in network) else f"projects/{project}/global/networks/{network}"
                body: Dict[str, Any] = {
                    "name": neg_name,
                    "description": desc_neg,
                    "networkEndpointType": "GCE_VM_IP_PORT",
                    "defaultPort": port,
                    "network": net_url,
                }
                if subnet:
                    sub_url = subnet if ("/" in subnet) else f"projects/{project}/regions/{region}/subnetworks/{subnet}"
                    body["subnetwork"] = sub_url

                op = rest_poster(url, body)
                op_name = op.get("name") if isinstance(op, dict) else None
                if op_name and isinstance(op, dict) and op.get("status") != "DONE":
                    op = runner(["compute", "operations", "wait", op_name, f"--zone={zone}", f"--project={project}"], True)
                if isinstance(op, dict) and op.get("error"):
                    errors = op["error"]
                    if isinstance(errors, dict) and "errors" in errors:
                        err_msgs = [e.get("message", str(e)) for e in errors["errors"] if isinstance(e, dict)]
                        msg = "; ".join(err_msgs) if err_msgs else json.dumps(errors)
                    else:
                        msg = json.dumps(errors)
                    raise RuntimeError(f"Failed to create network endpoint group {neg_name} in {zone}: {msg}")

    # 4. Backend Services & Backends
    port_map = {443: 8443, 80: 8080}
    for fe_port, be_port in port_map.items():
        bs_name = make_resource_name(cluster_id, namespace, gateway_name, f"bs-{fe_port}", prefix)
        desc_bs = make_description(cluster_id, namespace, gateway_name, be_port)
        bs_info = runner(["compute", "backend-services", "describe", bs_name, "--global", f"--project={project}"], False)
        if not bs_info:
            runner(
                [
                    "compute", "backend-services", "create", bs_name,
                    "--global",
                    "--load-balancing-scheme=EXTERNAL_MANAGED",
                    "--protocol=TCP",
                    f"--health-checks={hc_name}",
                    f"--timeout={draining_timeout}s",
                    f"--connection-draining-timeout={draining_timeout}s",
                    f"--description={desc_bs}",
                    f"--project={project}",
                ],
                True,
            )
            bs_info = runner(["compute", "backend-services", "describe", bs_name, "--global", f"--project={project}"], True)

        neg_name = make_resource_name(cluster_id, namespace, gateway_name, str(be_port), prefix)
        existing_backends = set()
        if isinstance(bs_info, dict):
            for b in bs_info.get("backends", []):
                if isinstance(b, dict) and "group" in b:
                    existing_backends.add(b["group"])

        for zone in zones:
            neg_url_suffix = f"zones/{zone}/networkEndpointGroups/{neg_name}"
            if not any(neg_url_suffix in b for b in existing_backends):
                runner(
                    [
                        "compute", "backend-services", "add-backend", bs_name,
                        "--global",
                        f"--network-endpoint-group={neg_name}",
                        f"--network-endpoint-group-zone={zone}",
                        "--balancing-mode=CONNECTION",
                        "--max-connections=1000",
                        f"--project={project}",
                    ],
                    True,
                )

    # 5. Target TCP Proxies
    for fe_port in [443, 80]:
        proxy_name = make_resource_name(cluster_id, namespace, gateway_name, f"proxy-{fe_port}", prefix)
        bs_name = make_resource_name(cluster_id, namespace, gateway_name, f"bs-{fe_port}", prefix)
        desc_proxy = make_description(cluster_id, namespace, gateway_name, fe_port)
        if not runner(["compute", "target-tcp-proxies", "describe", proxy_name, "--global", f"--project={project}"], False):
            runner(
                [
                    "compute", "target-tcp-proxies", "create", proxy_name,
                    "--global",
                    f"--backend-service={bs_name}",
                    "--proxy-header=PROXY_V1",
                    f"--description={desc_proxy}",
                    f"--project={project}",
                ],
                True,
            )

    # 6. Global Forwarding Rules
    for fe_port in [443, 80]:
        fr_name = make_resource_name(cluster_id, namespace, gateway_name, f"fr-{fe_port}", prefix)
        proxy_name = make_resource_name(cluster_id, namespace, gateway_name, f"proxy-{fe_port}", prefix)
        desc_fr = make_description(cluster_id, namespace, gateway_name, fe_port)
        if not runner(["compute", "forwarding-rules", "describe", fr_name, "--global", f"--project={project}"], False):
            runner(
                [
                    "compute", "forwarding-rules", "create", fr_name,
                    "--global",
                    f"--target-tcp-proxy={proxy_name}",
                    f"--ports={fe_port}",
                    f"--address={ip_name}",
                    "--load-balancing-scheme=EXTERNAL_MANAGED",
                    f"--description={desc_fr}",
                    f"--project={project}",
                ],
                True,
            )

    # 7. Ingress Firewall Rule
    fw_name = make_resource_name(cluster_id, namespace, gateway_name, "allow-gfe", prefix)
    desc_fw = make_description(cluster_id, namespace, gateway_name, 0)
    if not runner(["compute", "firewall-rules", "describe", fw_name, f"--project={project}"], False):
        runner(
            [
                "compute", "firewall-rules", "create", fw_name,
                f"--network={network}",
                "--action=ALLOW",
                "--direction=INGRESS",
                "--source-ranges=35.191.0.0/16,130.211.0.0/22",
                "--rules=tcp:8443,tcp:8080,tcp:8999",
                f"--description={desc_fw}",
                f"--project={project}",
            ],
            True,
        )

    return {
        "ip_address": ip_address,
        "cluster_id": cluster_id,
        "namespace": namespace,
        "gateway_name": gateway_name,
        "prefix": prefix,
        "neg_8443": make_resource_name(cluster_id, namespace, gateway_name, "8443", prefix),
        "neg_8080": make_resource_name(cluster_id, namespace, gateway_name, "8080", prefix),
        "zones": zones,
    }


def generate_manifest(cfg: Dict[str, Any]) -> str:
    """Generates the parameters ConfigMap and Gateway manifest YAML."""
    gw_name = cfg["gateway_name"]
    ns = cfg["namespace"]
    ip = cfg["ip_address"]
    cid = cfg["cluster_id"]
    neg_8443 = cfg["neg_8443"]
    neg_8080 = cfg["neg_8080"]
    pfx = cfg["prefix"]

    return f"""apiVersion: v1
kind: ConfigMap
metadata:
  name: gcpgateway-params-{gw_name}
  namespace: {ns}
data:
  serviceType: ClusterIP
  allowedSourceCIDRs: "35.191.0.0/16,130.211.0.0/22"
  proxyProtocol: "enabled"
  proxyProtocolTrustedCIDRs: "35.191.0.0/16,130.211.0.0/22"
  externalIP: "{ip}"
  clusterId: "{cid}"
  gatewayNamespace: "{ns}"
  gatewayName: "{gw_name}"
  negHTTPS: "{neg_8443}"
  negHTTP: "{neg_8080}"
  negPrefix: "{pfx}"
---
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: {gw_name}
  namespace: {ns}
spec:
  gatewayClassName: gcpgateway
  addresses:
  - type: IPAddress
    value: "{ip}"
  listeners:
  - name: https
    protocol: HTTPS
    port: 443
    tls:
      mode: Terminate
      certificateRefs:
      - name: {gw_name}-cert
    allowedRoutes:
      namespaces:
        from: Same
  - name: http
    protocol: HTTP
    port: 80
    allowedRoutes:
      namespaces:
        from: Same
  infrastructure:
    parametersRef:
      group: ""
      kind: ConfigMap
      name: gcpgateway-params-{gw_name}
"""


def main():
    parser = argparse.ArgumentParser(description="Idempotently create GCP front-end resources for gcpgateway")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--region", required=True, help="GCP region")
    parser.add_argument("--network", default="default", help="VPC network name")
    parser.add_argument("--subnet", default="default", help="VPC subnet name")
    parser.add_argument("--cluster-id", default="cluster-1", help="Kubernetes cluster identifier")
    parser.add_argument("--namespace", default="default", help="Gateway namespace")
    parser.add_argument("--gateway-name", default="my-gateway", help="Gateway name")
    parser.add_argument("--prefix", default="gcpgw", help="Resource prefix")
    parser.add_argument("--draining-timeout", type=int, default=35, help="Backend service draining timeout in seconds")
    parser.add_argument("--zones", help="Comma-separated list of zones in the region")

    args = parser.parse_args()
    zone_list = [z.strip() for z in args.zones.split(",")] if args.zones else None

    cfg = provision_frontend(
        project=args.project,
        region=args.region,
        cluster_id=args.cluster_id,
        namespace=args.namespace,
        gateway_name=args.gateway_name,
        network=args.network,
        subnet=args.subnet,
        prefix=args.prefix,
        draining_timeout=args.draining_timeout,
        zones=zone_list,
    )

    print("\n=== Setup Complete ===")
    print(f"Front-end Static IP: {cfg['ip_address']}\n")
    print("Ready-to-apply Kubernetes manifests:\n")
    print(generate_manifest(cfg))


if __name__ == "__main__":
    main()
