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

"""Unit tests for create_frontend.py and teardown_frontend.py using a fake gcloud runner."""

import json
import os
import sys
import unittest
from typing import Any, Dict, List, Optional, Tuple

# Add parent directory to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from create_frontend import (
    generate_manifest,
    make_description,
    make_hash,
    make_resource_name,
    provision_frontend,
)
from teardown_frontend import teardown_frontend, verify_ownership


class FakeGcloud:
    """In-memory fake implementation of gcloud CLI commands."""

    def __init__(self):
        # Key: (resource_type, name, scope) -> dict
        self.store: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        self.call_log: List[List[str]] = []

    def __call__(self, args: List[str], check: bool = True) -> Optional[Any]:
        self.call_log.append(args)

        # Parse command
        if len(args) < 3 or args[0] != "compute":
            return {}

        res_type = args[1]
        action = args[2]

        if res_type == "zones" and action == "list":
            return [{"name": "us-central1-a"}, {"name": "us-central1-b"}]

        # Determine name, scope, and flags
        name = ""
        scope = "global"
        desc = ""
        flags: Dict[str, str] = {}

        # Look for flags
        for arg in args[3:]:
            if arg == "--global":
                scope = "global"
            elif arg.startswith("--zone="):
                scope = arg.split("=", 1)[1]
            elif arg.startswith("--description="):
                desc = arg.split("=", 1)[1]
            elif arg.startswith("--"):
                parts = arg[2:].split("=", 1)
                flags[parts[0]] = parts[1] if len(parts) > 1 else "true"
            elif not name and not arg.startswith("-") and arg not in ["http", "tcp"]:
                name = arg

        key = (res_type, name, scope)

        if action == "describe":
            if key in self.store:
                return self.store[key]
            if not check:
                return None
            raise RuntimeError(f"Resource {key} not found")

        elif action in ["create", "add-backend"]:
            if action == "create":
                obj = {
                    "name": name,
                    "description": desc,
                    "address": "34.120.254.10" if res_type == "addresses" else None,
                    "backends": [],
                }
                self.store[key] = obj
                return obj
            elif action == "add-backend":
                bs_name = args[3]
                bs_key = ("backend-services", bs_name, "global")
                neg = flags.get("network-endpoint-group", "")
                neg_zone = flags.get("network-endpoint-group-zone", "")
                if bs_key in self.store:
                    self.store[bs_key].setdefault("backends", []).append({
                        "group": f"zones/{neg_zone}/networkEndpointGroups/{neg}",
                    })
                return {}

        elif action == "delete":
            if key in self.store:
                del self.store[key]
                return {}
            if not check:
                return None
            return {}

        return {}


class TestFrontendScripts(unittest.TestCase):

    def test_shared_test_vectors(self):
        """Validates that Python hashing and naming matches the shared test vectors JSON exactly."""
        vector_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            "testdata",
            "naming_vectors.json",
        )
        with open(vector_file, "r", encoding="utf-8") as f:
            vectors = json.load(f)

        for v in vectors:
            with self.subTest(name=v["expected_name"]):
                actual_hash = make_hash(v["cluster_id"], v["namespace"], v["gateway_name"])
                self.assertEqual(actual_hash, v["expected_hash"])

                actual_name = make_resource_name(
                    v["cluster_id"],
                    v["namespace"],
                    v["gateway_name"],
                    v["suffix"],
                    v["prefix"],
                )
                self.assertEqual(actual_name, v["expected_name"])

                actual_desc = make_description(
                    v["cluster_id"],
                    v["namespace"],
                    v["gateway_name"],
                    v["metadata"]["port"],
                )
                parsed = json.loads(actual_desc)
                self.assertEqual(parsed["cluster-id"], v["metadata"]["cluster-id"])
                self.assertEqual(parsed["namespace"], v["metadata"]["namespace"])
                self.assertEqual(parsed["gateway-name"], v["metadata"]["gateway-name"])
                self.assertEqual(parsed["port"], v["metadata"]["port"])

    def test_idempotent_provisioning_and_teardown(self):
        """Verifies create_frontend creates all resources, running a second time is a no-op,

        and teardown_frontend cleanly deletes them in reverse dependency order.
        """
        fake = FakeGcloud()

        # 1. First run of create_frontend
        cfg1 = provision_frontend(
            project="test-project",
            region="us-central1",
            cluster_id="cluster-1",
            namespace="default",
            gateway_name="my-gw",
            zones=["us-central1-a", "us-central1-b"],
            runner=fake,
        )

        self.assertEqual(cfg1["ip_address"], "34.120.254.10")
        self.assertIn("neg_8443", cfg1)
        self.assertIn("neg_8080", cfg1)

        # Count resources created
        initial_resource_count = len(fake.store)
        self.assertGreater(initial_resource_count, 5)

        # 2. Second run of create_frontend with same parameters (Idempotency test)
        call_count_before = len(fake.call_log)
        cfg2 = provision_frontend(
            project="test-project",
            region="us-central1",
            cluster_id="cluster-1",
            namespace="default",
            gateway_name="my-gw",
            zones=["us-central1-a", "us-central1-b"],
            runner=fake,
        )

        self.assertEqual(cfg1, cfg2)
        # Store should have the exact same number of resources (no duplicates)
        self.assertEqual(len(fake.store), initial_resource_count)

        # 3. Add foreign resources that must NOT be deleted by teardown
        foreign_desc = make_description("foreign-cluster", "default", "my-gw", 8443)
        fake.store[("network-endpoint-groups", "gcpgw-foreign-neg", "us-central1-a")] = {
            "name": "gcpgw-foreign-neg",
            "description": foreign_desc,
        }
        no_desc_key = ("forwarding-rules", "gcpgw-no-desc", "global")
        fake.store[no_desc_key] = {"name": "gcpgw-no-desc", "description": ""}

        # 4. First run of teardown_frontend
        deleted = teardown_frontend(
            project="test-project",
            region="us-central1",
            cluster_id="cluster-1",
            namespace="default",
            gateway_name="my-gw",
            zones=["us-central1-a", "us-central1-b"],
            runner=fake,
        )

        self.assertGreater(len(deleted), 0)

        # Foreign resources must remain untouched
        self.assertIn(("network-endpoint-groups", "gcpgw-foreign-neg", "us-central1-a"), fake.store)
        self.assertIn(no_desc_key, fake.store)

        # 5. Second run of teardown_frontend (Idempotency test)
        deleted_second = teardown_frontend(
            project="test-project",
            region="us-central1",
            cluster_id="cluster-1",
            namespace="default",
            gateway_name="my-gw",
            zones=["us-central1-a", "us-central1-b"],
            runner=fake,
        )
        self.assertEqual(len(deleted_second), 0)

    def test_manifest_output(self):
        """Verifies the generated YAML manifest format and required parameters."""
        cfg = {
            "ip_address": "34.120.254.10",
            "cluster_id": "cluster-1",
            "namespace": "prod",
            "gateway_name": "web-gw",
            "prefix": "gcpgw",
            "neg_8443": "gcpgw-12345678-8443",
            "neg_8080": "gcpgw-12345678-8080",
        }
        manifest = generate_manifest(cfg)
        self.assertIn("gcpgateway-params-web-gw", manifest)
        self.assertIn('externalIP: "34.120.254.10"', manifest)
        self.assertIn('allowedSourceCIDRs: "35.191.0.0/16,130.211.0.0/22"', manifest)
        self.assertIn('proxyProtocol: "enabled"', manifest)
        self.assertIn('negHTTPS: "gcpgw-12345678-8443"', manifest)


if __name__ == "__main__":
    unittest.main()
