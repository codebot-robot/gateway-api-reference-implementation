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

import io
import json
import os
import shutil
import subprocess
import sys
import unittest
import urllib.error
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

# Add parent directory to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from create_frontend import (
    default_post_rest,
    generate_manifest,
    get_access_token,
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

    def post_rest(self, url: str, body: Dict[str, Any]) -> Dict[str, Any]:
        self.call_log.append(["POST", url, json.dumps(body)])
        parts = url.split("/")
        # URL format: .../zones/<zone>/networkEndpointGroups
        zone = parts[parts.index("zones") + 1]
        name = body["name"]
        key = ("network-endpoint-groups", name, zone)
        self.store[key] = {
            "name": name,
            "description": body.get("description", ""),
            "network": body.get("network", ""),
            "subnetwork": body.get("subnetwork", ""),
            "defaultPort": body.get("defaultPort"),
        }
        return {"status": "DONE", "name": f"operation-create-{name}"}

    def __call__(self, args: List[str], check: bool = True) -> Optional[Any]:
        self.call_log.append(args)

        # Parse command
        if len(args) < 3 or args[0] != "compute":
            if len(args) >= 2 and args[0] == "auth" and args[1] == "print-access-token":
                return {"access_token": "fake-token"}
            return {}

        res_type = args[1]
        action = args[2]

        if res_type == "operations" and action == "wait":
            return {"status": "DONE"}

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

    def test_get_access_token(self):
        """Verifies get_access_token extracts access tokens from dict, string, and environment variables."""
        # 1. From dict runner output
        def dict_runner(args: List[str], check: bool = True):
            return {"access_token": "dict-token-123"}

        self.assertEqual(get_access_token(dict_runner), "dict-token-123")

        # 2. From raw string runner output
        def string_runner(args: List[str], check: bool = True):
            return "string-token-456\n"

        self.assertEqual(get_access_token(string_runner), "string-token-456")

        # 3. From environment variable
        with patch.dict(os.environ, {"CLOUDSDK_AUTH_ACCESS_TOKEN": "env-token-789"}):
            self.assertEqual(get_access_token(dict_runner), "env-token-789")

        # 4. Error on unparseable output
        def invalid_runner(args: List[str], check: bool = True):
            return 12345

        with self.assertRaises(RuntimeError):
            get_access_token(invalid_runner)

    def test_default_post_rest(self):
        """Verifies default_post_rest sends HTTP POST with JSON body and bearer authentication."""
        # Successful response
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({"status": "DONE", "name": "op-1"}).encode("utf-8")
        mock_response.__enter__.return_value = mock_response

        with patch("urllib.request.urlopen", return_value=mock_response) as mock_urlopen:
            resp = default_post_rest(
                "https://compute.googleapis.com/test",
                {"name": "test-neg"},
                "bearer-token",
            )
            self.assertEqual(resp.get("status"), "DONE")
            self.assertEqual(resp.get("name"), "op-1")

            mock_urlopen.assert_called_once()
            req = mock_urlopen.call_args[0][0]
            self.assertEqual(req.get_method(), "POST")
            self.assertEqual(req.headers.get("Authorization"), "Bearer bearer-token")
            self.assertEqual(req.headers.get("Content-type"), "application/json")
            self.assertEqual(json.loads(req.data.decode("utf-8")), {"name": "test-neg"})

        # HTTP error response
        error_file = io.BytesIO(b'{"error": {"message": "Invalid field"}}')
        http_error = urllib.error.HTTPError(
            url="https://compute.googleapis.com/test",
            code=400,
            msg="Bad Request",
            hdrs=None,
            fp=error_file,
        )
        with patch("urllib.request.urlopen", side_effect=http_error):
            with self.assertRaises(RuntimeError) as ctx:
                default_post_rest(
                    "https://compute.googleapis.com/test",
                    {"name": "test-neg"},
                    "bearer-token",
                )
            self.assertIn("Invalid field", str(ctx.exception))

    def test_gcloud_cli_syntax(self):
        """Validates all generated gcloud command arguments against gcloud --help if gcloud is installed."""
        gcloud_bin = shutil.which("gcloud")
        if not gcloud_bin:
            self.skipTest("gcloud binary not available in environment")

        fake = FakeGcloud()
        provision_frontend(
            project="test-project",
            region="us-central1",
            cluster_id="cluster-1",
            namespace="default",
            gateway_name="my-gw",
            zones=["us-central1-a", "us-central1-b"],
            runner=fake,
        )
        teardown_frontend(
            project="test-project",
            region="us-central1",
            cluster_id="cluster-1",
            namespace="default",
            gateway_name="my-gw",
            zones=["us-central1-a", "us-central1-b"],
            runner=fake,
        )

        help_cache = {}
        for cmd in fake.call_log:
            if not cmd or cmd[0] != "compute":
                continue

            subcmd = ["compute", cmd[1], cmd[2]]
            if len(cmd) >= 4 and cmd[1] == "health-checks" and cmd[2] == "create":
                subcmd.append(cmd[3])

            flags = [arg.split("=", 1)[0] for arg in cmd if arg.startswith("--")]

            subcmd_key = tuple(subcmd)
            if subcmd_key not in help_cache:
                proc = subprocess.run(
                    [gcloud_bin] + list(subcmd_key) + ["--help"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                help_cache[subcmd_key] = proc.stdout + proc.stderr

            help_text = help_cache[subcmd_key]
            for flag in flags:
                self.assertIn(
                    flag,
                    help_text,
                    f"Flag '{flag}' was not found in help text for {' '.join(subcmd)}",
                )


if __name__ == "__main__":
    unittest.main()
