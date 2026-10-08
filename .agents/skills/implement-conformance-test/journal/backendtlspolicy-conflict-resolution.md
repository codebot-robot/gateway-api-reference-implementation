# Conformance Test Journal: BackendTLSPolicyConflictResolution

## 1. Test Overview
- **Name**: `BackendTLSPolicyConflictResolution`
- **Description**: Verifies BackendTLSPolicy conflict resolution semantics:
  1. Two policies targeting the same Service with no `sectionName`: the older one gets Accepted=True, the newer one Accepted=False with reason `Conflicted`, and traffic succeeds using the accepted policy.
  2. Two policies targeting the same Service with the same `sectionName`: the older one gets Accepted=True, the newer one Accepted=False with reason `Conflicted`, and traffic succeeds using the accepted policy.
  3. One policy with `sectionName: https-1` and one without `sectionName` targeting the same Service: both are Accepted=True (they do not conflict); requests on the `https-1` port (443) use the section-specific policy, and requests on the other port (8443) use the Service-wide policy.
- **Manifests**: `tests/backendtlspolicy-conflict-resolution.yaml` (v1.6.2)

## 2. Issue / Failure Analysis
- **Observed Behavior**:
  - `BackendTLSPolicyConflictResolution` failed on `BackendTLSPolicies_targeting_the_same_Service_with_and_without_a_section_name`:
    `BackendTLSPolicy_without_section_name_should_be_accepted` timed out because `ComputeDesiredBackendTLSPolicyStatus` compared target Service namespace and name without checking `targetRef.SectionName`. Consequently, a policy targeting `sectionName: https-1` and a Service-wide policy on the same Service were incorrectly treated as conflicting, causing the newer policy to receive `Accepted=False` with reason `Conflicted`.
  - In `resolveBackendTarget`, policy resolution iterated linearly over `sortedTLSPolicies` without prioritizing a section-specific policy over a Service-wide policy on a port matching the section name.

## 3. Implementation / Fix Strategy
- **Approach**:
  1. Updated `ComputeDesiredBackendTLSPolicyStatus` in `pkg/state/compiled.go`:
     - Compared `ValueOf(t.SectionName) == targetSection` when checking for conflicts between targetRefs. Two policies only conflict if they select the exact same Service and sectionName (or both select Service-wide with no sectionName).
  2. Updated `resolveBackendTarget` in `pkg/state/httproute.go`:
     - Two-pass selection: first search `sortedTLSPolicies` for a policy matching `backendRef.Name` and `targetRef.SectionName == portName`. If none matches, fallback to searching for a Service-wide policy where `targetRef.SectionName` is nil or empty.
     - Section-specific policy takes precedence on its port over a Service-wide policy, regardless of relative creation timestamps.
  3. Added unit tests in `pkg/state/compiled_test.go`:
     - `TestComputeDesiredBackendTLSPolicyStatus_SectionConflictResolution`: verifies earlier `creationTimestamp` wins and name is used as tie-break when creation timestamps are identical.
     - `TestBackendTLSPolicy_SectionAndServiceWide_NoConflict`: verifies a `sectionName` policy and a Service-wide policy on the same Service don't conflict, and compiled backends pick up the right policy on ports 443 and 8443.
  4. Enabled `tests.BackendTLSPolicyConflictResolution` in `tests/e2e/conformance_test.go`.

## 4. Validation & Results
- **Unit Tests**:
  - `go test -v ./pkg/state/... -run "TestComputeDesiredBackendTLSPolicyStatus_SectionConflictResolution|TestBackendTLSPolicy_SectionAndServiceWide_NoConflict"`: PASS
- **Conformance Logs**:
  ```
  --- PASS: TestConformance/BackendTLSPolicyConflictResolution (11.81s)
      --- PASS: TestConformance/BackendTLSPolicyConflictResolution/Conflicting_BackendTLSPolicies_targeting_the_same_Service_without_a_section_name (0.05s)
          --- PASS: TestConformance/BackendTLSPolicyConflictResolution/Conflicting_BackendTLSPolicies_targeting_the_same_Service_without_a_section_name/First_BackendTLSPolicy_should_be_accepted (0.00s)
          --- PASS: TestConformance/BackendTLSPolicyConflictResolution/Conflicting_BackendTLSPolicies_targeting_the_same_Service_without_a_section_name/Second_BackendTLSPolicy_should_have_a_false_Accepted_condition_with_reason_Conflicted_ (0.00s)
          --- PASS: TestConformance/BackendTLSPolicyConflictResolution/Conflicting_BackendTLSPolicies_targeting_the_same_Service_without_a_section_name/HTTP_request_sent_to_Service_using_the_accepted_BackendTLSPolicy_should_succeed (0.05s)
      --- PASS: TestConformance/BackendTLSPolicyConflictResolution/Conflicting_BackendTLSPolicies_targeting_the_same_Service_with_the_same_section_name (0.03s)
          --- PASS: TestConformance/BackendTLSPolicyConflictResolution/Conflicting_BackendTLSPolicies_targeting_the_same_Service_with_the_same_section_name/First_BackendTLSPolicy_should_be_accepted (0.00s)
          --- PASS: TestConformance/BackendTLSPolicyConflictResolution/Conflicting_BackendTLSPolicies_targeting_the_same_Service_with_the_same_section_name/Second_BackendTLSPolicy_should_have_a_false_Accepted_condition_with_reason_Conflicted_ (0.00s)
          --- PASS: TestConformance/BackendTLSPolicyConflictResolution/Conflicting_BackendTLSPolicies_targeting_the_same_Service_with_the_same_section_name/HTTP_request_sent_to_Service_using_the_accepted_BackendTLSPolicy_should_succeed (0.03s)
      --- PASS: TestConformance/BackendTLSPolicyConflictResolution/BackendTLSPolicies_targeting_the_same_Service_with_and_without_a_section_name (0.07s)
          --- PASS: TestConformance/BackendTLSPolicyConflictResolution/BackendTLSPolicies_targeting_the_same_Service_with_and_without_a_section_name/BackendTLSPolicy_with_section_name_should_be_accepted (0.00s)
          --- PASS: TestConformance/BackendTLSPolicyConflictResolution/BackendTLSPolicies_targeting_the_same_Service_with_and_without_a_section_name/BackendTLSPolicy_without_section_name_should_be_accepted (0.00s)
          --- PASS: TestConformance/BackendTLSPolicyConflictResolution/BackendTLSPolicies_targeting_the_same_Service_with_and_without_a_section_name/HTTP_request_sent_to_Service_using_the_BackendTLSPolicy_with_section_name_should_succeed (0.04s)
          --- PASS: TestConformance/BackendTLSPolicyConflictResolution/BackendTLSPolicies_targeting_the_same_Service_with_and_without_a_section_name/HTTP_request_sent_to_Service_using_the_BackendTLSPolicy_without_section_name_should_succeed (0.02s)
  ```
