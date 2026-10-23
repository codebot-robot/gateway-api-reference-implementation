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

"""Idempotently tears down Google Cloud Load Balancing resources and zonal NEGs for gcpgateway."""

import argparse
import json
import subprocess
import sys
from typing import Any, Callable, Dict, List, Optional

from create_frontend import default_run_gcloud, make_resource_name


def verify_ownership(
    resource: Optional[Dict[str, Any]],
    cluster_id: str,
    namespace: str,
    gateway_name: str,
) -> bool:
    """Verifies that the resource description contains matching JSON metadata."""
    if not resource or not isinstance(resource, dict):
        return False

    desc_str = resource.get("description", "")
    if not desc_str:
        return False

    try:
        meta = json.loads(desc_str)
    except Exception:
        return False

    cid = meta.get("cluster-id") or meta.get("clusterId")
    ns = meta.get("namespace")
    gw = meta.get("gateway-name") or meta.get("gatewayName")

    return cid == cluster_id and ns == namespace and gw == gateway_name


def teardown_frontend(
    project: str,
    region: str,
    cluster_id: str,
    namespace: str,
    gateway_name: str,
    prefix: str = "gcpgw",
    zones: Optional[List[str]] = None,
    runner: Callable[[List[str], bool], Optional[Any]] = default_run_gcloud,
) -> List[str]:
    """Deletes all matching GCP resources in reverse dependency order, checking metadata ownership."""
    deleted_resources: List[str] = []

    if not zones:
        zone_info = runner(
            ["compute", "zones", "list", f"--filter=region:({region})", f"--project={project}"],
            True,
        )
        if isinstance(zone_info, list) and zone_info:
            zones = sorted([z["name"] for z in zone_info if "name" in z])
        else:
            zones = [f"{region}-a", f"{region}-b", f"{region}-c"]

    # 1. Forwarding Rules
    for fe_port in [443, 80]:
        fr_name = make_resource_name(cluster_id, namespace, gateway_name, f"fr-{fe_port}", prefix)
        fr_info = runner(["compute", "forwarding-rules", "describe", fr_name, "--global", f"--project={project}"], False)
        if fr_info and verify_ownership(fr_info, cluster_id, namespace, gateway_name):
            runner(["compute", "forwarding-rules", "delete", fr_name, "--global", "--quiet", f"--project={project}"], True)
            deleted_resources.append(fr_name)

    # 2. Target TCP Proxies
    for fe_port in [443, 80]:
        proxy_name = make_resource_name(cluster_id, namespace, gateway_name, f"proxy-{fe_port}", prefix)
        proxy_info = runner(["compute", "target-tcp-proxies", "describe", proxy_name, "--global", f"--project={project}"], False)
        if proxy_info and verify_ownership(proxy_info, cluster_id, namespace, gateway_name):
            runner(["compute", "target-tcp-proxies", "delete", proxy_name, "--global", "--quiet", f"--project={project}"], True)
            deleted_resources.append(proxy_name)

    # 3. Backend Services
    for fe_port in [443, 80]:
        bs_name = make_resource_name(cluster_id, namespace, gateway_name, f"bs-{fe_port}", prefix)
        bs_info = runner(["compute", "backend-services", "describe", bs_name, "--global", f"--project={project}"], False)
        if bs_info and verify_ownership(bs_info, cluster_id, namespace, gateway_name):
            runner(["compute", "backend-services", "delete", bs_name, "--global", "--quiet", f"--project={project}"], True)
            deleted_resources.append(bs_name)

    # 4. Zonal NEGs
    for port in [8443, 8080]:
        neg_name = make_resource_name(cluster_id, namespace, gateway_name, str(port), prefix)
        for zone in zones:
            neg_info = runner(["compute", "network-endpoint-groups", "describe", neg_name, f"--zone={zone}", f"--project={project}"], False)
            if neg_info and verify_ownership(neg_info, cluster_id, namespace, gateway_name):
                runner(["compute", "network-endpoint-groups", "delete", neg_name, f"--zone={zone}", "--quiet", f"--project={project}"], True)
                deleted_resources.append(f"{neg_name} ({zone})")

    # 5. Health Check
    hc_name = make_resource_name(cluster_id, namespace, gateway_name, "hc", prefix)
    hc_info = runner(["compute", "health-checks", "describe", hc_name, "--global", f"--project={project}"], False)
    if hc_info and verify_ownership(hc_info, cluster_id, namespace, gateway_name):
        runner(["compute", "health-checks", "delete", hc_name, "--global", "--quiet", f"--project={project}"], True)
        deleted_resources.append(hc_name)

    # 6. Firewall Rule
    fw_name = make_resource_name(cluster_id, namespace, gateway_name, "allow-gfe", prefix)
    fw_info = runner(["compute", "firewall-rules", "describe", fw_name, f"--project={project}"], False)
    if fw_info and verify_ownership(fw_info, cluster_id, namespace, gateway_name):
        runner(["compute", "firewall-rules", "delete", fw_name, "--quiet", f"--project={project}"], True)
        deleted_resources.append(fw_name)

    # 7. Static IP Address
    ip_name = make_resource_name(cluster_id, namespace, gateway_name, "ip", prefix)
    ip_info = runner(["compute", "addresses", "describe", ip_name, "--global", f"--project={project}"], False)
    if ip_info and verify_ownership(ip_info, cluster_id, namespace, gateway_name):
        runner(["compute", "addresses", "delete", ip_name, "--global", "--quiet", f"--project={project}"], True)
        deleted_resources.append(ip_name)

    return deleted_resources


def main():
    parser = argparse.ArgumentParser(description="Cleanly tear down GCP front-end resources for gcpgateway")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--region", required=True, help="GCP region")
    parser.add_argument("--cluster-id", default="cluster-1", help="Kubernetes cluster identifier")
    parser.add_argument("--namespace", default="default", help="Gateway namespace")
    parser.add_argument("--gateway-name", default="my-gateway", help="Gateway name")
    parser.add_argument("--prefix", default="gcpgw", help="Resource prefix")
    parser.add_argument("--zones", help="Comma-separated list of zones in the region")

    args = parser.parse_args()
    zone_list = [z.strip() for z in args.zones.split(",")] if args.zones else None

    deleted = teardown_frontend(
        project=args.project,
        region=args.region,
        cluster_id=args.cluster_id,
        namespace=args.namespace,
        gateway_name=args.gateway_name,
        prefix=args.prefix,
        zones=zone_list,
    )

    print("\n=== Teardown Complete ===")
    for item in deleted:
        print(f"Deleted: {item}")


if __name__ == "__main__":
    main()
