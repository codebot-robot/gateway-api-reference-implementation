#!/bin/bash
TESTS=$(cat /tmp/gateway-api/conformance/tests/*.go | grep -h 'var [a-zA-Z0-9]* = suite.ConformanceTest' | awk '{print $2}' | sort)
LOCAL="HTTPRouteSimpleSameNamespace HTTPRouteMatching HTTPRoutePathMatchOrder HTTPRouteExactPathMatching HTTPRouteMethodMatching HTTPRouteHeaderMatching HTTPRouteHostnameIntersection HTTPRouteInvalidBackendRefUnknownKind HTTPRouteBackendProtocolH2C BackendTLSPolicy BackendTLSPolicyConflictResolution"

for test in $TESTS; do
    if echo "$LOCAL" | grep -qw "$test"; then
        continue
    fi
    count=$(gh issue list --search "in:title \"Pass $test conformance test\"" --state all --json id | jq length)
    if [ "$count" -eq 0 ]; then
        echo "Found missing test with no issue: $test"
        exit 0
    fi
done
