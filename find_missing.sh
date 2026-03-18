#!/bin/bash
TESTS=$(cat /tmp/gateway-api/conformance/tests/*.go | grep -h 'var [a-zA-Z0-9]* = suite.ConformanceTest' | awk '{print $2}' | sort)
LOCAL="HTTPRouteSimpleSameNamespace HTTPRouteMatching HTTPRoutePathMatchOrder HTTPRouteExactPathMatching HTTPRouteMethodMatching HTTPRouteHeaderMatching HTTPRouteHostnameIntersection HTTPRouteInvalidBackendRefUnknownKind HTTPRouteBackendProtocolH2C BackendTLSPolicy BackendTLSPolicyConflictResolution"

for test in $TESTS; do
    if echo "$LOCAL" | grep -qw "$test"; then
        continue
    fi
    TITLES=$(gh issue list --search "in:title \"Pass $test conformance test\"" --state all --json title -t '{{range .}}{{.title}}{{"\n"}}{{end}}')
    
    FOUND=0
    if [ -n "$TITLES" ]; then
        while IFS= read -r title; do
            if [ "$title" = "Pass $test conformance test" ]; then
                FOUND=1
                break
            fi
        done <<< "$TITLES"
    fi

    if [ "$FOUND" -eq 0 ]; then
        echo "FOUND_MISSING_TEST=$test"
        break
    fi
done
