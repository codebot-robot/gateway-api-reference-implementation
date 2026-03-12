import json
import urllib.request
import re
import subprocess

# get all tests
req = urllib.request.Request("https://api.github.com/repos/kubernetes-sigs/gateway-api/contents/conformance/tests?ref=release-1.5")
req.add_header("User-Agent", "python")
response = urllib.request.urlopen(req)
data = json.loads(response.read())

tests = set()
for item in data:
    if item["name"].endswith(".go"):
        f_url = item["download_url"]
        f_req = urllib.request.Request(f_url)
        f_req.add_header("User-Agent", "python")
        f_resp = urllib.request.urlopen(f_req)
        content = f_resp.read().decode("utf-8")
        matches = re.findall(r"ConformanceTests\s*=\s*append\(ConformanceTests,\s*([A-Za-z0-9_]+)\)", content)
        for m in matches:
            tests.add(m)

# get issues
output = subprocess.check_output(["gh", "issue", "list", "--search", "conformance test in:title", "--state", "all", "--limit", "200", "--json", "title", "-q", ".[].title"]).decode("utf-8")

issued_tests = set()
for line in output.split('\n'):
    if line.startswith("Pass ") and " conformance test" in line:
        test_name = line[5:-17]
        issued_tests.add(test_name)

missing = tests - issued_tests
for m in sorted(list(missing)):
    print(m)

