#!/usr/bin/env bash
# Mutation check for the CEL admission rules: it neutralises each rule in a copy
# of the CRD manifests, runs the test that covers it against that copy, and
# fails unless the test fails. A rule whose test still passes is not tested.
#
#   hack/mutate-cel.sh
#
# For every rule in config/crd (hack/mutate-cel lists them) the script
#   1. writes a copy of the manifests with that one rule's expression replaced by
#      "true", which admits everything, and checks that exactly one file changed;
#   2. starts the control plane (the e2e suite's own, from internal/testenv) on
#      the copy and runs the tests named in the table below;
#   3. expects at least one of those tests to fail, and prints which.
# Before any mutant it runs the unmutated, re-serialised copy and expects every
# mapped test to pass, and to have run: "go test -run" with a pattern that
# matches nothing also exits 0. A rule with no row in the table, or a row for a
# rule that no longer exists, fails the script, so a new rule cannot be added
# untested.
#
# TestVerifyGeneratedCatchesDrift is not run: it fails on every mutant by design.
#
# It takes the same environment as hack/check.sh (HPCBURST_TOOLS_CACHE,
# HPCBURST_TOOLS_BIN for the Kubernetes binaries) and runs about 40 control
# planes one after the other, about ten minutes in all. The test binary is compiled
# once. HPCBURST_BUILD_LOCK, when set, names a lock file (the shared Go build
# lock) that the two compiles take; the runs themselves do not need it. Run it
# under nice. It uses the port block 29200-29219 like the e2e suite, so it must
# not run beside it.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

work=$(mktemp -d "${TMPDIR:-/tmp}/mutate-cel.XXXXXX")
trap 'rm -rf "$work"' EXIT

# rule id | tests that cover it (a go test -run pattern). Ids come from
# "go run ./hack/mutate-cel list config/crd".
table=$(cat <<'TABLE'
allocations:$.spec#0|^TestAllocationNamesAProvider$
allocations:$.spec#1|^TestAllocationNamesAProvider$
allocations:$.spec#2|^TestAllocationProviderIsImmutable$
allocations:$.spec.forProvider#0|^TestAllocationSubmissionParametersAreImmutable$|^TestAllocationKindOfLeaseIsImmutable$
allocations:$.spec.forProvider#1|^TestAllocationIsABatchJobOrACatalogEntry$
allocations:$.spec.forProvider.batch.memory#0|^TestAllocationMemoryIsNotNegative$
allocations:$.spec.forProvider.batch.timeLimit#0|^TestDurationsArePositiveAndFit$
allocations:$.spec.managementPolicies#0|^TestAllocationManagementPolicies$
allocations:$.spec.managementPolicies#1|^TestAllocationManagementPolicies$
allocations:$.spec.retentionPolicy#0|^TestAllocationRetentionPolicyRule$
allocations:$.spec.retentionPolicy#1|^TestAllocationRetentionUntilGoesWithKeep$
allocations:$.spec.retentionPolicy.idleLimit#0|^TestDurationsArePositiveAndFit$
allocations:$.status.atProvider.submission#0|^TestAllocationPhasesAndSubmissionStates$
instances:$.spec#0|^TestInstanceSpecIsImmutable$
instances:$.spec#1|^TestInstanceIdentityReference$
instances:$.spec.launch#0|^TestInstanceLaunchCommandAgreesWithMaxNumSeqs$
modeldeployments:$.spec#0|^TestServedDeploymentNeedsAWorkload$
modeldeployments:$.spec.leaseLimit#0|^TestDurationsArePositiveAndFit$
modeldeployments:$.spec.providers#0|^TestModelDeploymentProvidersAndInstances$
modeldeployments:$.spec.providers.names#0|^TestModelDeploymentProviderNamesAreOrderedAndUnique$
modeldeployments:$.spec.retentionPolicy#0|^TestRetentionPolicy$
modeldeployments:$.spec.retentionPolicy#1|^TestRetentionPolicy$
modeldeployments:$.spec.retentionPolicy.idleLimit#0|^TestDurationsArePositiveAndFit$
modeldeployments:$.spec.routing.loadBound#0|^TestRoutingLoadBoundIsAtLeastOne$
modeldeployments:$.spec.routing.loadBound.loose#0|^TestRoutingLoadBoundIsAtLeastOne$
modeldeployments:$.spec.routing.loadBound.loose#1|^TestRoutingLoadBoundHasAtMostTwoDecimals$
modeldeployments:$.spec.routing.loadBound.tight#0|^TestRoutingLoadBoundIsAtLeastOne$
modeldeployments:$.spec.routing.loadBound.tight#1|^TestRoutingLoadBoundHasAtMostTwoDecimals$
modeldeployments:$.spec.routing.maxRequestDuration#0|^TestDurationsArePositiveAndFit$
modeldeployments:$.spec.source#0|^TestModelDeploymentSourceIsServedOrManaged$
modeldeployments:$.spec.source.served#0|^TestTokenizerOtherThanTheCheckpointsIsPinned$
modeldeployments:$.spec.source.served#1|^TestTokenizerOtherThanTheCheckpointsIsPinned$
modeldeployments:$.spec.source.served.checkpoint.hubTokenRef#0|^TestCheckpointHubTokenReferenceNamesExactlyOneSource$
modeldeployments:$.spec.source.served.checkpoint.revision#0|^TestCheckpointRevisionIsAFullCommitID$
modeldeployments:$.spec.source.served.engine.image#0|^TestEngineImageIsARepositoryWithoutATag$
modeldeployments:$.spec.source.served.engine.imageDigest#0|^TestEngineImageDigest$
modeldeployments:$.spec.source.served.env[]#0|^TestEngineEnvironment$
modeldeployments:$.spec.source.served.env[]#1|^TestEngineEnvironment$|^TestEngineEnvironmentCredentialsAreRefusedOnUpdate$
modeldeployments:$.spec.source.served.flags[]#0|^TestEngineFlagsUseLongNamesAndCarryNoKeys$
modeldeployments:$.spec.source.served.flags[]#1|^TestEngineFlagsUseLongNamesAndCarryNoKeys$
modeldeployments:$.spec.source.served.flags[]#2|^TestEngineFlagsUseLongNamesAndCarryNoKeys$
modeldeployments:$.spec.source.served.flags[]#3|^TestEngineFlagsUseLongNamesAndCarryNoKeys$
modeldeployments:$.spec.source.served.flags[]#4|^TestBooleanEngineFlagsTakeTrueOrFalse$
modeldeployments:$.spec.source.served.flags[]#5|^TestBooleanEngineFlagsTakeTrueOrFalse$
modeldeployments:$.spec.source.served.flags[]#6|^TestEngineFlagsRefuseAbbreviationsOfRefusedNames$
modeldeployments:$.spec.source.served.flags[]#7|^TestEngineFlagsRefuseAbbreviationsOfRefusedNames$
modeldeployments:$.spec.source.served.flags[]#8|^TestEngineFlagsRefuseAbbreviationsOfRefusedNames$
modeldeployments:$.spec.source.served.flags[]#9|^TestEngineFlagsRefuseAbbreviationsOfRefusedNames$
modeldeployments:$.spec.source.served.flags[]#10|^TestEngineFlagsThatNameOtherCheckpointsAreRefused$
modeldeployments:$.spec.source.served.flags[]#11|^TestEngineFlagsRefuseEveryPrefixOfAKnownFlag$
modeldeployments:$.spec.source.served.flags[]#12|^TestMaxModelLenIsAPlainPositiveInteger$
modeldeployments:$.spec.source.served.flags[]#13|^TestParallelSizesArePlainPositiveIntegers$
modeldeployments:$.spec.workload.contextTokens#0|^TestContextTokensTypicalIsAtMostLongest$
platformprofiles:$.spec#0|^TestPlatformProfileTypedFields$
platformprofiles:$.spec#1|^TestPartitionQOSIsListed$
platformprofiles:$.spec.drainMargin#0|^TestDurationsArePositiveAndFit$
platformprofiles:$.spec.drainWindow#0|^TestDurationsArePositiveAndFit$
platformprofiles:$.spec.handoverMargin#0|^TestDurationsArePositiveAndFit$
platformprofiles:$.spec.health.agentTimeout#0|^TestDurationsArePositiveAndFit$
platformprofiles:$.spec.health.errorOnlyWindow#0|^TestDurationsArePositiveAndFit$
platformprofiles:$.spec.health.kvSkewTimeout#0|^TestDurationsArePositiveAndFit$
platformprofiles:$.spec.health.noProgressTimeout#0|^TestDurationsArePositiveAndFit$
platformprofiles:$.spec.health.probeInterval#0|^TestDurationsArePositiveAndFit$
platformprofiles:$.spec.health.registrationTimeout#0|^TestDurationsArePositiveAndFit$
platformprofiles:$.spec.health.startTimeout#0|^TestDurationsArePositiveAndFit$
platformprofiles:$.spec.inflightWindow#0|^TestDurationsArePositiveAndFit$
platformprofiles:$.spec.lostGrace#0|^TestDurationsArePositiveAndFit$
platformprofiles:$.spec.maxIdle#0|^TestDurationsArePositiveAndFit$
platformprofiles:$.spec.minJobAge#0|^TestDurationsArePositiveAndFit$
platformprofiles:$.spec.partitions[]#0|^TestPartitionTmpDirFeatureNeedsTmpDir$
platformprofiles:$.spec.partitions[].maxTime#0|^TestDurationsArePositiveAndFit$
platformprofiles:$.spec.partitions[].tmpDir#0|^TestPartitionTmpDirRefusesDotDotSegments$
platformprofiles:$.spec.qos[].maxCpusPerUser#0|^TestQOSMaxCpusPerUserIsPositive$
platformprofiles:$.spec.qos[].maxMemPerUser#0|^TestQOSMaxMemPerUserIsPositive$
platformprofiles:$.spec.qos[].maxWall#0|^TestDurationsArePositiveAndFit$
platformprofiles:$.spec.restart.backoff#0|^TestDurationsArePositiveAndFit$
platformprofiles:$.spec.restart.stopGrace#0|^TestDurationsArePositiveAndFit$
platformprofiles:$.spec.restart.window#0|^TestDurationsArePositiveAndFit$
platformprofiles:$.spec.stagingDir#0|^TestStagingDirRefusesDotDotSegments$
platformprofiles:$.spec.unknownBound#0|^TestDurationsArePositiveAndFit$
providers:$#0|^TestProviderNamesAreDNS1123Labels$
providers:$.spec.credentialsRef#0|^TestProviderCredentialsReferenceNamesExactlyOneSource$
TABLE
)

lock() {
  if [ -n "${HPCBURST_BUILD_LOCK:-}" ]; then flock "$HPCBURST_BUILD_LOCK" "$@"; else "$@"; fi
}

echo "mutate-cel: compiling the helper and the e2e test binary" >&2
lock go build -o "$work/mutate-cel" ./hack/mutate-cel
lock go test -c -o "$work/e2e.test" ./api/v1alpha1/e2e

# A tree laid out as the test binary expects: it reads ../../../config relative
# to its working directory and finds the repository root by go.mod.
tree=$work/tree
mkdir -p "$tree/hack" "$tree/api/v1alpha1/e2e" "$tree/config"
cp go.mod "$tree/"
cp hack/test-binaries.sh "$tree/hack/"
cp -R config/admission "$tree/config/"
# One test reads HBEP-3's flag table, so the known flag names it checks the
# rules against come from the specification.
mkdir -p "$tree/hbep/0003-resource-model-and-naming"
cp hbep/0003-resource-model-and-naming/README.md "$tree/hbep/0003-resource-model-and-naming/"

# patterns_of "a|b|c" with ^ and $ anchors: the test names, one per line.
test_names() { tr '|' '\n' <<<"$1" | sed -e 's/^\^//' -e 's/\$$//'; }

# run_suite PATTERN LOG: the e2e tests matching PATTERN against $tree/config/crd.
# The host is always the assembled stack, whatever the caller exported.
run_suite() {
  (cd "$tree/api/v1alpha1/e2e" &&
    env -u HPCBURST_TEST_HOST -u HPCBURST_TEST_BARE -u HPCBURST_KUBECONFIG \
      "$work/e2e.test" -test.v -test.count=1 -test.timeout=5m -test.run "$1") >"$2" 2>&1
}

mapfile -t listed < <("$work/mutate-cel" list config/crd | cut -f1)
declare -A pattern
fail=0
# The first "|" ends the id; the rest of the line, further "|" included, is the pattern.
while IFS='|' read -r id rest; do
  [ -n "$id" ] || continue
  pattern["$id"]=$rest
done <<<"$table"

# Every listed rule has a row, every row a rule.
for id in "${listed[@]}"; do
  if [ -z "${pattern["$id"]:-}" ]; then
    echo "UNMAPPED  $id: no test is named for this rule in hack/mutate-cel.sh" >&2
    fail=1
  fi
done
for id in "${!pattern[@]}"; do
  found=0
  for l in "${listed[@]}"; do
    if [ "$l" = "$id" ]; then found=1; fi
  done
  if [ "$found" != 1 ]; then
    echo "STALE     $id: the table names a rule that is not in config/crd" >&2
    fail=1
  fi
done
if [ "$fail" != 0 ]; then exit 1; fi

# Baseline: the re-serialised, unmutated manifests pass every mapped test, and
# each of those tests ran.
"$work/mutate-cel" neutralise config/crd "$work/base" -
rm -rf "$tree/config/crd"; cp -R "$work/base" "$tree/config/crd"
all=$(for id in "${listed[@]}"; do test_names "${pattern["$id"]}"; done | sort -u)
union=$(sed -e 's/^/^/' -e 's/$/$/' <<<"$all" | paste -sd'|')
echo "mutate-cel: baseline, $(wc -l <<<"$all") tests on the unmutated manifests" >&2
if ! run_suite "$union" "$work/baseline.log"; then
  echo "BASELINE FAILED: the unmutated manifests do not pass; the log follows" >&2
  tail -40 "$work/baseline.log" >&2
  exit 1
fi
while read -r name; do
  if ! grep -q -- "^--- PASS: $name " "$work/baseline.log"; then
    echo "BASELINE: $name did not run and pass" >&2
    fail=1
  fi
done <<<"$all"
if [ "$fail" != 0 ]; then exit 1; fi

killed=0
survived=0
for id in "${listed[@]}"; do
  rm -rf "$work/mutant"
  "$work/mutate-cel" neutralise config/crd "$work/mutant" "$id"
  # diff exits 1 when the trees differ, which is the expected case.
  changed=$( (diff -rq "$work/base" "$work/mutant" || true) | wc -l)
  if [ "$changed" != 1 ]; then
    echo "MUTANT   $id changed $changed files, want exactly 1" >&2
    exit 1
  fi
  rm -rf "$tree/config/crd"; cp -R "$work/mutant" "$tree/config/crd"

  log=$work/run.log
  status=0
  run_suite "${pattern["$id"]}" "$log" || status=$?
  # Failing top-level tests of the mapped set.
  mapfile -t failing < <(grep -E -- '^--- FAIL: ' "$log" | sed -E 's/^--- FAIL: ([^ ]+).*/\1/' | sort -u)
  hit=()
  for name in "${failing[@]}"; do
    while read -r want; do
      if [ "$name" = "$want" ]; then hit+=("$name"); fi
    done < <(test_names "${pattern["$id"]}")
  done
  if [ "$status" != 0 ] && [ "${#hit[@]}" -gt 0 ]; then
    n=$(grep -c -E -- '^\s+--- FAIL: ' "$log" || true)
    printf 'KILLED    %-62s by %s (%s failing subtests)\n' "$id" "${hit[*]}" "$n"
    killed=$((killed + 1))
  else
    printf 'SURVIVED  %-62s tests %s still pass (exit %s)\n' "$id" "${pattern["$id"]}" "$status"
    if [ "$status" != 0 ]; then tail -15 "$log" >&2; fi
    survived=$((survived + 1))
  fi
done

echo "mutate-cel: ${#listed[@]} rules, $killed killed, $survived survived" >&2
if [ "$survived" != 0 ]; then exit 1; fi
