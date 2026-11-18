#!/usr/bin/env bash
# The root module's tests: the one definition that hack/check.sh and CI's go and go own-timeout jobs run
# (issue #282; the groups run in go own-timeout since issue #295).
#
# go test's 10-minute package timeout stays: a raised one would hide a real hang for longer. So the tests
# that would bring a package close to it run after the packages, in groups, each group under a timeout of
# its own, and the packages' run leaves them out by their exact names:
#   - lease: internal/lifecycle's one test that takes about 46 s, because each of its 200 rounds stops a
#     manager in the state a 200 ms poll leaves it in, which is part of what it proves (PR #291);
#   - lifecycle-caches, lifecycle-copies and lifecycle-control-plane: internal/lifecycle's other tests that
#     start a control plane (envtest), about 170 s of the package's 435-447 s on main's slow runs, which
#     took it below a 1.5x margin (issue #295). Their time is real work and waits on observable state (a
#     manager's start and stop, Lease expiry, informer syncs), not a fixed sleep;
#   - lifecycle-apiserver and lifecycle-slow: internal/lifecycle's slowest remaining tests (issue #295, part
#     B), peeled out because the package's run reached 506 s of go test's 600 s on main's newest run of 17
#     Nov 2026. lifecycle-apiserver is three more tests that start a control plane (envtest, the package's
#     block). lifecycle-slow is eleven tests that start none: read through the package's helpers, they reach
#     no control plane, listener or port constant. They run reconcile passes over a fake provider and a fake
#     clock, with no sleep, so their time is CPU and moves with the runner's load (4.8 s to 33.9 s for one).
#     Each of the fourteen is a test whose worst across the five newest go-job logs was at least 9 s. Taking
#     them from those logs, the package's slowest logged run would have been 335 s, a margin of 1.79x (a
#     projection: CI's runs of this change are the measurement);
#   - lifecycle-slow2: eight more of internal/lifecycle's fake-clock tests (issue #295, part B2). Part B left the
#     package's slowest go-job run, in the three that CI ran on its head, at 465 s of go test's 600 s (278 s and
#     372 s in the other two), a 1.67x swing between runners, and the science agent ruled on 17 Nov 2026 that
#     the worst of three must be 400 s or less before more lifecycle tests land. These are the next eight by
#     their own (serial) time in that slowest run, each 8.9 s or more there. Like lifecycle-slow's, they run
#     reconcile passes over a fake provider and a fake clock: read through the package's helpers with a
#     type-checked call graph, they reach no control plane, listener or port constant, as lifecycle-slow's
#     do not. Taking them from that run, the package would have taken 357 s (235 s and 307 s in the other two)
#     (a projection: CI's runs of this change are the measurement). It is a group of its own, not more rows in
#     lifecycle-slow, because 3x of lifecycle-slow's sum of worsts with them would pass 850 s;
#   - lifecycle-slow3: six more of internal/lifecycle's fake-clock tests (issue #295, part B3). The first CI run
#     of part B2's head measured the package's run at 432 s on a slow runner (202 s on a fast one, a 2.14x swing),
#     above the 400 s that the worst of three had to meet, though the eight tests of lifecycle-slow2 were out of
#     it. Its top-level lines add up to 378 s and the rest, 54 s, is the two tests that have parallel subtests
#     (TestTheTwoJobGuardKeepsALongWaitToTwoAllocations and TestADeadStepLeavesTheDeploymentAtItsLastEnd): go
#     test does not count a parallel subtest's time in its parent, so their own lines read 0.00 s, yet their
#     eight subtests run side by side for 51-54 s at the package's end (206 s of subtest time over four cores).
#     Those two and the four serial tests next by their own time in that run (20.3 s, 14.6 s, 13.8 s and 13.4 s
#     there) leave the packages run, which would then have taken 316 s, below the 323 s that one reading must
#     not pass (540 s over the 1.67x runner swing), with the 400 s of the worst of three well clear. Like
#     lifecycle-slow's, the six run reconcile passes over a fake provider and a fake clock: read through the
#     package's helpers with a type-checked call graph, they reach no control plane, listener or port
#     constant, and the graph's only two hits are the ones that lifecycle-slow's own tests have (a process
#     kill in internal/slurm/sshrun, which the graph reaches by matching an interface call of Close to that
#     type's, not through any call the tests make). The group's parallel pair keeps its subtests parallel:
#     a group runs its tests by -run, and -run matches the top-level name only;
#   - lifecycle-slow4: thirty-four more of internal/lifecycle's fake-clock tests (issue #295, part B4). The
#     over-allocation work of #708, #721 and #724 added 81 tests to the package (100.3 s of its top-level lines on
#     train 11's CI run) and slowed rows that were already there (the extra's checks run in every pass, and the
#     handover, ranking and gate rows now also run with an extra), so the package's run read 332.9 s in the go job
#     of train 11 and 453.9 s in its crd and envtest job (the same tests, the second runner 1.36x slower), where
#     it had read 175.2 s on main before #708, and 499.3 s in the go job of train 12, on a runner 1.5x slower than
#     train 11's. What binds is the worst of three go-job readings at 400 s or less (part B2's standard; go test's own
#     limit is 600 s), and a slow runner is 1.5x to 1.67x. These are the thirty-four slowest, by their own time in
#     train 11's go-job run, of the rows those three PRs added or slowed (new tests of #708, #721 and #724, and
#     existing rows whose time on that run exceeds their time before #708 by more than the two runners' ratio
#     explains), 125.2 s in all. Taking them out, the package's run would have read 332.9 s - 125.2 s = 207.7 s in
#     the go job on train 11's runner (240 s is 400 s over 1.67x, 267 s is 400 s over 1.5x), 499.3 s - 125.2 s x 1.5 =
#     311.5 s on train 12's, and 453.9 s - 125.2 s x 1.36 = 283.6 s in the crd and envtest job, all under 400 s (a projection: CI's runs of this change are the measurement). The group's -timeout is 3x the sum of each test's worst in the go-job logs read that carry it
#     (before #708, #708's merge and train 11; a row that #721 or #724 added has train 11's reading alone, from the
#     fast runner), 163.5 s, which gives 490 s, so the worst of three has to stay under 327 s,
#     two thirds of it (#295's rule for a group that sets its own timeout). The thirty-four tests sum to 125.2 s in
#     train 11's go-job run and took 80.7 s run alone on a 4-CPU Sano node, where the package without the lanes reads
#     243 s against that run's 333 s, so the group is expected to read about 110 s in a lane on CI, 163 s at the most (the
#     sum of worsts), well under 327 s. The lane adds that to its 477 s in the crd and envtest job (the job read 1,386 s
#     of its 2,700 s on train 11, so about 1,500 s, 1,549 s at the most) and to go own-timeout's 572 s (of 1,800 s: 682 s,
#     735 s at the most). Like lifecycle-slow's, the thirty-four run reconcile passes over a fake provider and a fake clock: read
#     through the package's helpers with a type-checked call graph (golang.org/x/tools/go/packages, go/ssa and RTA over
#     every Test function), they reach no control plane (internal/testenv), listener (net.Listen*, httptest, an
#     http.Server's Serve), process (os/exec, os.StartProcess, syscall.Kill) or port constant (28000 to 29999). The
#     graph follows static calls, interface calls by the types the program instantiates, and closures and functions
#     taken as values from where they are taken, and it reads only this module's own functions: a call through a
#     function value is not followed, and a library's insides are not entered, since the real Kubernetes client that
#     an interface call of client.Client resolves to among the instantiated types would otherwise reach HTTP and
#     the credential plugin's exec from every test, while the fake client these tests use does neither. The same
#     graph reports TestManagerOnControlPlane, TestOperationsOnAControlPlaneWithTwoAgents, TestAgentCLIOnAControlPlane
#     and TestMetricsStandbyOnTwoManagers (testenv, exec, port constants, kill), so it can see what it checks for;
#   - controller-failover: cmd/hpcburst-controller's two HBEP-11 failover tests, 150 s of its 400 s, whose
#     takeover waits for two of controller-runtime's 15 s Leases to expire after a SIGKILL (issue #295);
#   - controller-identities: four of the same package's tests that prove the controllers' own identities
#     (issue #660, HBEP-10 Audit): each starts a control plane of its own and asks the API server what the three
#     identities may do, who the binary's managers run as, how a lost Operation commit is recovered under the
#     lifecycle identity, and that its writes stamp the owner label. Every other test of the package runs
#     under those identities too, through the harness; these four are the ones about the identities. Their
#     49 s (13.0, 11.2, 13.6 and 11.5 s in the go job of the head that added them, run 44203460856) had taken
#     the package's run to 384 s in the go job and 388 s in the crd and envtest job, over the 380 s trim line,
#     under the 400 s of the worst of three;
#   - envoy-timeouts: internal/agent's two real-Envoy tests that wait out Envoy's own defaults, about 6
#     minutes of the package's run: a worker that is silent for 310 s, past Envoy's 5 minute stream idle
#     timeout, and an upstream connection that idles 50 s before Envoy closes it (issue #429). Both wait on
#     observable state (a response, the worker's connection events), never a fixed sleep;
#   - livechecks-hostdialed and livechecks-dry: hack/livechecks' dry runs through hack/live-checks.sh, the
#     tests that build and run the real conformance and alpha test binaries and a real agent, which took
#     that package to the 10 minutes in the crd and envtest job (the science agent's ruling on PR #306);
#   - livechecks-tree: the five tests of that package whose worst across the five newest go-job logs was at
#     least 15 s (issue #295, part B): its package run reached 459 s, and without these five the slowest
#     logged run would have been 345 s, 1.74x (a projection, as above).
#
#   hack/go-test.sh packages        every package without the groups' tests, at go test's default timeout
#   hack/go-test.sh own [group]     one group alone, or every group in lanes; each test must really run and pass
#   hack/go-test.sh all             both in that order; the groups run even if the packages fail
#
# LANES (issue #295). "own" with no group runs the groups of DIFFERENT packages at the same time, so that the
# job costs its longest lane and not the sum of its groups (about 950-990 s on CI, which took go own-timeout
# and the crd and envtest job below a 1.5x margin of their limits). A lane is a package: its groups run one
# after another in table order, since two go test runs of one package share its fixed ports, its envtest
# block and its build. Every group keeps its own -timeout, its own checks and its own exit. A failing group
# stops neither its lane nor the others; the exit is the first non-zero group's in table order, and the
# output names every group that failed. Each lane's output goes to a file of its own and is printed whole
# when the lane ends, so that no two groups' lines interleave, under a ::group:: in GitHub Actions (a lane
# that failed stays unfolded). While the lanes run, one line is printed at each group's start and end, and a
# table at the end gives each group's start, end and WALL time (its build and checks included, so not a
# margin to -timeout: that is the ok line of the group's output), so that the overlap can be read from the
# log. A stop signal (INT, TERM, HUP) is passed to every lane's process group; each lane has one of its own
# (set -m), so nothing a lane started is left behind, and the partial output is printed. (SIGKILL of this
# script cannot be passed on.) "own <group>" runs that group alone, in the foreground, exactly as before.
#
# A warm-up build runs first in "own" (not in "all", whose package run has just compiled every test
# variant): one go test of every group package with -exec true, which compiles and links the test binaries
# and runs none of them, so that the lanes do not each compile the same dependencies at the same time. Its
# failure fails the step by name.
#
# A plain go test ./... still runs every test, in one package run.
set -uo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd) || exit 1
cd "$root" || exit 1

# group | package | its tests, by exact name | its timeout. A timeout is about 3x the group's worst run on
# CI (PR #291's rule): lease's 48.8 s on main's healthy runs (the science agent, 9 Nov 2026);
# livechecks-hostdialed's 45.1 s and livechecks-dry's 88.2 s, both in the crd and envtest job of PR #306's
# run on a25f9438 (42.8 s and 82.0 s in its go job). Issue #295's groups take the sum of each test's worst
# in the go job of main's five healthy runs of 10 Nov 2026 (42844202426 to 42849291185), a bound on the
# group's worst: lifecycle-caches 58.8 s, lifecycle-copies 62.1 s, lifecycle-control-plane 56.2 s and
# controller-failover 150.5 s. envoy-timeouts' 1200 s is about 3x its local run, not yet a CI one:
# TestEnvoyStreamIdleOutlivesTheDefault took 310.7 s (its two halves run at once, bound by the worker's
# silence of 310 s) and TestEnvoyClosesIdleUpstreamConnectionsFirst 50.7 s (bound by Envoy's 50 s upstream
# idle), 362 s in all. The first green CI run should replace it with 3x its own. Part B's groups of issue
# #295 take 3x the sum of each test's worst in the go job of main's five newest runs of 17 Nov 2026 (03:01Z
# to 08:55Z), a bound on the group's worst: lifecycle-apiserver 43.0 s (130 s), lifecycle-slow 172.8 s
# (520 s) and livechecks-tree 126.9 s (390 s). The selection rule is in the bullets above, and the tests'
# worst times are in the issue's record. Part B2's lifecycle-slow2 takes the sum of each test's worst over the
# three go-job runs of PR #692's head (run attempts 1 to 3 of one run, 17 Nov 2026), 109.1 s, which gives 330 s.
# Part B3's lifecycle-slow3 takes the times of the slow go-job run of part B2's head (432 s for the package,
# 17 Nov 2026): 20.3 s, 14.6 s, 13.8 s and 13.4 s for the four serial tests and 53.9 s for the two parallel
# ones, which is the part of the package's run that their top-level lines do not carry (432.2 s less the
# 378.3 s of the top-level lines). That is 115.9 s, which gives 350 s.
# Part B4's lifecycle-slow4 takes the sum of each test's worst over the go-job logs of main before #708 (f05028df), of
# main after #708 and #739 (5919a554) and of train 11, 163.5 s, which gives 490 s.
# Issue #660's controller-identities takes the four tests' times in the go job of run 44203460856 (18 Nov 2026,
# the head that added them): 13.0 s, 11.2 s, 13.6 s and 11.5 s, 49.3 s, which gives 150 s.
groups=(
  "lease|./internal/lifecycle|TestLifecycleManagerReleasesItsLeaseOnEveryStop|150s"
  "lifecycle-caches|./internal/lifecycle|TestCachesNeedsEveryWatchedInformer TestCachesRecoverFromAStalledDiscovery TestCachesStopDuringAStalledDiscovery TestCachesListIsWhatTheControllerWatches|180s"
  "lifecycle-copies|./internal/lifecycle|TestMetricsStandbyOnTwoManagers TestMetricsLostLeaseStopsBeforeANewCopyLeads TestMetricsCopiesStartAgainOnALostPort TestRaisedEventsOnTwoManagers|190s"
  "lifecycle-control-plane|./internal/lifecycle|TestManagerOnControlPlane TestVerificationRecordRoundTripsThroughTheAPIServer TestLifecycleLeaseReleaseUnderLatency TestStartLifecycleLeaderOfAManagerThatLeadsBeforeTheLeaseIsRead|170s"
  "lifecycle-apiserver|./internal/lifecycle|TestOperationsOnAControlPlaneWithTwoAgents TestMetricsKeeperModeThroughNewManager TestRecordedOutcomesPassTheAPIServersRules|130s"
  "lifecycle-slow|./internal/lifecycle|TestScaleDownWithASurplus TestARankedOutAllocationIsKeptUnderItsPolicy TestHandoverLateReplacementDegradesThenTakesOver TestHandoverReplacementCreatedAtItsPlannedTime TestAWaitingCandidateIsNotFailedByStartTimeout TestACandidateWithNoTunnelFailsAtItsDeadline TestNoOverlapOneSlotSuccessorWaitsForTheFlagToClear TestAWarmAllocationDoesNotCancelAPendingSurplus TestGateAWrongEntryReleasesAtMostTwoWhenInstancesGoOnWarmAllocations TestAMarkedPeerThatRestartsNeverOutranks TestAReplacementHedgesByItsWaits|520s"
  "lifecycle-slow2|./internal/lifecycle|TestReservedSlotKeepsAFailedSuccessorsSlotForItsRetry TestRetentionNoneOutlivesHardEnd TestReservedSlotKeepsACutShortSuccessorsSlotAcrossARestart TestMetricsNoProgressNeedsFreshKnownCounters TestNoOverlapModeBoundaries TestRetentionJobEndsAndLeavesListing TestTunnelDownAloneRestartsNothing TestMetricsDrainEndsOnAKnownZeroReadAfterTheDrainBegan|330s"
  "lifecycle-slow3|./internal/lifecycle|TestVerifyRefusedGenerationConfigStopsClass TestReadyInstanceStaysReadyWhenTunnelDrops TestGateAMissTheFullRecordCantHoldSuspendsItsEntry TestGateTwoProvidersOfOneClassSuspendAWrongEntryWithinTwoReleases TestTheTwoJobGuardKeepsALongWaitToTwoAllocations TestADeadStepLeavesTheDeploymentAtItsLastEnd|350s"
  "lifecycle-slow4|./internal/lifecycle|TestACandidateRanksAgainstItsServingPeer TestTheFirstToPassIsChosenAndTheLaterOutranksIt TestAnExtraIsRankedOutWhenItsPeerHasAReplacement TestAReplacementGoesOnAWarmAllocationOnlyIfItDrainsAtLeastTheLeadLater TestNoExtraWhereThePassMadeNoNewAllocationForAPrimary TestAKeptLoserIsNeverRankedAgainstAnotherPrimaryWhenItsWinnerLeaves TestGateDoesNotJudgeAWaitingCandidateAgainAfterItRestarts TestAnExtraThatPassesFirstWaitsForTheRanking TestAStartedLoserEndsAtTheWinWhenAnyOfItsConditionsFails TestAKeptLosersMarkIsReadAfterARestart TestHandoverRestartPlansTheSameTime TestAFailedPrimaryWithAReplacementLeavesTheWaitingExtraChosen TestAnOutrankingCandidateThatRestartsBeforeItsWinnerDrains TestHandoverControllerKilledAtEachStep TestGateRecordOnlyIsFinalForAWaitingCandidate TestAKeptLoserRankingPointCases TestAReReadyOutrankerWaitsTheFullWindowFromItsNewTransition TestHandoverBothServeThenPrimaryDrainsAndSecondReplacementIsPlanned TestAStartedLoserIsRankedAgainstItsWinnerAlone TestHandoverSameClassSameIDAndRollout TestTheExtraAndTheSlotBeyondItsOwnHandovers TestAStartedLoserIsNeverComparedWithAnotherPrimary TestAKeptLoserIsChosenWhenTheDeploymentIsShort TestAFailedPrimaryLeavesTheExtraAsThePrimary TestAnExtraWhoseJobStartsFirstIsChosenAndGetsTheReplacement TestAStartedLoserIsKept TestHandoverLeaseEndExtensionMovesDrainStart TestAStartedLoserThirtyMinutesAfterItsPredecessorIsNotPlacedOn TestAWarmPlacementCannotSlipBetweenTheCountAndTheHeadroom TestLoweringInstancesLeavesANonReadyPrimaryAsTheExtra TestAWinnerThatFailsLeavesTheLoserChosen TestAReplacementGoesOnAWarmAllocationOnlyIfItDrainsLater TestAKeptLoserThatFailsACheckIsReleased TestNoOverlapWarmAllocationTakesTheReplacement|490s"
  "controller-failover|./cmd/hpcburst-controller|TestControllerProcessesFailOverMidSubmit TestControllerProcessesFailOverBeforeSubmit|460s"
  "controller-identities|./cmd/hpcburst-controller|TestEachManagerOfTheBinaryRunsUnderItsOwnIdentity TestAnOperationsCommitIsRecoveredAndExecutedUnderTheLifecycleIdentity TestTheLifecycleIdentityStampsTheOwnerHashLabel TestControllerIdentitiesAreWhoTheyClaimAndNothingElseCan|150s"
  "envoy-timeouts|./internal/agent|TestEnvoyStreamIdleOutlivesTheDefault TestEnvoyClosesIdleUpstreamConnectionsFirst|1200s"
  "livechecks-hostdialed|./hack/livechecks|TestEachHostDialedFaultFailsTheRowThroughTheScript TestEveryHostDialedRowPassesThroughTheScriptOverTheFake TestAHostDialedRowWhoseSlotIsHeldIsNotRunThroughTheScript TestADryHostDialedBinaryRefusesLiveSettings|150s"
  "livechecks-dry|./hack/livechecks|TestEveryAlphaRowPassesItsDryRunAndEveryFaultFailsIt TestADryBinaryRefusesLiveSettings TestAJobPendingPastTheBoundIsNotRunOnEveryRow TestALiveRunWithNoListenerFailsLoudlyAndUsesNoCluster TestAnAccountingRowWithNoSlotThatPendedPastItsWaitIsNotRun TestEveryRowThatPostsNamesWhereTheFrontRanAndRefusesWithoutIt TestACatchUpThatCannotTellIsAFailureOnATwoJobRow TestASecondJobThatNeverStartedIsNotRunOnTheTwoJobRows TestEveryReachRowReportsEachOutcomeThroughTheScript TestADryReachBinaryRefusesLiveSettingsAndARealGateway TestAReachRowWhoseSlotIsHeldIsNotRun|270s"
  "livechecks-tree|./hack/livechecks|TestRegistryFailsOnTheRealTreeWhenAPartIsRemoved TestPlanMapsChangedFilesToTheLiveTestsThatRunThem TestTheBulkRowsFourVerdictsThroughTheScript TestTheFindingMarkerIsTheBulkRowsAlone TestMutateAlphaEndsAHangAtItsBoundAndRestoresTheFile|390s"
)

# field <row> <n>: the row's n-th field (1-based).
field() { local IFS='|'; local -a f; read -r -a f <<<"$1"; printf '%s' "${f[$(($2 - 1))]}"; }

# regexp <names...>: the anchored alternation of the exact names, as -run and -skip take it.
regexp() { local IFS='|'; printf '^(%s)$' "$*"; }

# every_name: every group's tests, one per line.
every_name() { local g; for g in "${groups[@]}"; do tr ' ' '\n' <<<"$(field "$g" 3)"; done; }

# declared_once <names...>: each name is a test declared in exactly one test file of the root module
# (spikes and third_party are modules of their own). -skip applies to every package, so a second test of
# the same name would vanish from every run without a word, and a rename would leave its group running
# nothing.
declared_once() {
  local name n bad=0
  for name in "$@"; do
    n=$(grep -rl --include='*_test.go' --exclude-dir=.git --exclude-dir=spikes --exclude-dir=third_party -E "^func ${name}\(" . | wc -l)
    if [ "$n" -ne 1 ]; then
      # It starts with FAIL so that hack/check.sh's filter of the output keeps it.
      echo "FAIL hack/go-test.sh: ${name} is declared in ${n} test files of the root module, not one: its name here must be a test's own, and unique"
      bad=1
    fi
  done
  return "$bad"
}

packages() {
  local -a names
  mapfile -t names < <(every_name)
  declared_once "${names[@]}" || return 1
  go test -count=1 -v -skip "$(regexp "${names[@]}")" ./...
}

# run_group <row>: the group's tests alone, under its timeout. It fails unless every one of them really
# passed and nothing in the run skipped: go test exits 0 when -run matches nothing, a skip reports as
# success, and a subtest that skips (a missing sshd) leaves its parent's PASS line in place.
run_group() {
  local g=$1 name pkg timeout out status bad=0 t
  name=$(field "$g" 1) pkg=$(field "$g" 2) timeout=$(field "$g" 4)
  local -a tests
  read -r -a tests <<<"$(field "$g" 3)"
  declared_once "${tests[@]}" || return 1
  out=$(mktemp) || return 1
  go test -count=1 -v -timeout "$timeout" -run "$(regexp "${tests[@]}")" "$pkg" 2>&1 | tee "$out"
  status=${PIPESTATUS[0]}
  if [ "$status" -eq 0 ]; then
    for t in "${tests[@]}"; do
      if ! grep -q -E "^--- PASS: ${t} \(" "$out"; then
        echo "FAIL hack/go-test.sh: ${t} did not run and pass in ${pkg}, though go test exited 0: it was renamed, removed or skipped, and its name here must match"
        bad=1
      fi
    done
    if grep -q -E '^[[:space:]]*--- SKIP: ' "$out"; then
      echo "FAIL hack/go-test.sh: the ${name} group skipped a test or a subtest in ${pkg}, which is coverage that did not run:"
      grep -E '^[[:space:]]*--- SKIP: ' "$out" | sed 's/^[[:space:]]*/  /'
      bad=1
    fi
    [ "$bad" -eq 0 ] || status=1
  fi
  rm -f "$out"
  return "$status"
}

# lane_packages: the distinct packages of the groups, in table order, one per line. A lane is a package.
lane_packages() {
  local g p seen=" "
  for g in "${groups[@]}"; do
    p=$(field "$g" 2)
    case "$seen" in
      *" $p "*) ;;
      *)
        seen+="$p "
        printf '%s\n' "$p"
        ;;
    esac
  done
}

# lane_groups <package>: the names of the package's groups, in table order, separated by ", ".
lane_groups() {
  local g list=""
  for g in "${groups[@]}"; do
    [ "$(field "$g" 2)" = "$1" ] || continue
    list+="${list:+, }$(field "$g" 1)"
  done
  printf '%s' "$list"
}

# run_lane <dir> <package>: the package's groups, one after another in table order, in a background
# subshell. A group's whole output goes to <dir>/<group>.log, and its result ("status start end", seconds
# since own began) to <dir>/<group>.result, written when the group ends and at no other time: a group with
# no result file did not end. <dir>/events gets a line at each start and end, which own_lanes prints while
# the lane runs.
run_lane() {
  local dir=$1 pkg=$2 g name began status
  for g in "${groups[@]}"; do
    [ "$(field "$g" 2)" = "$pkg" ] || continue
    name=$(field "$g" 1)
    began=$SECONDS
    echo "start $name $began" >> "$dir/events"
    run_group "$g" > "$dir/$name.log" 2>&1 < /dev/null
    status=$?
    echo "$status $began $SECONDS" > "$dir/$name.result"
    echo "end $name $status $SECONDS" >> "$dir/events"
  done
}

# show_events <index> <dir> <package>: the lane's start and end lines not printed yet, one line each. Only
# the parent prints, so that no line of it lands inside a lane's block.
show_events() {
  local i=$1 dir=$2 pkg=$3 total kind name a b
  total=$(($(wc -l < "$dir/events")))
  [ "$total" -gt "${events_seen[$i]}" ] || return 0
  while read -r kind name a b; do
    if [ "$kind" = start ]; then
      echo "own: lane ${pkg}: group ${name} started at +${a} s"
    else
      echo "own: lane ${pkg}: group ${name} ended with status ${a} at +${b} s"
    fi
  done < <(sed -n "$((${events_seen[$i]} + 1)),${total}p" "$dir/events")
  events_seen[$i]=$total
}

# print_lane <dir> <package>: the lane's groups' whole output, in table order. In GitHub Actions a lane
# whose groups all passed is folded; one that did not stays open, so that its failure is in view.
print_lane() {
  local dir=$1 pkg=$2 g name st rest folded=0
  if [ "${GITHUB_ACTIONS:-}" = true ]; then
    folded=1
    for g in "${groups[@]}"; do
      [ "$(field "$g" 2)" = "$pkg" ] || continue
      name=$(field "$g" 1)
      if ! { [ -f "$dir/$name.result" ] && read -r st rest < "$dir/$name.result" && [ "$st" = 0 ]; }; then
        folded=0
      fi
    done
  fi
  [ "$folded" -eq 0 ] || echo "::group::own: lane ${pkg} ($(lane_groups "$pkg"))"
  for g in "${groups[@]}"; do
    [ "$(field "$g" 2)" = "$pkg" ] || continue
    name=$(field "$g" 1)
    echo "=== own: group ${name} (${pkg}, -timeout $(field "$g" 4))"
    if [ -f "$dir/$name.log" ]; then
      cat "$dir/$name.log"
    else
      echo "(the group did not start)"
    fi
    [ -f "$dir/$name.result" ] || echo "(the group did not end: it was stopped, or its lane died)"
  done
  [ "$folded" -eq 0 ] || echo "::endgroup::"
}

# own_work is the scratch directory of a lanes run; the EXIT trap removes it. stop_code is the exit a stop
# signal asks for (128 + the signal), 0 while none came.
own_work=""
stop_code=0

# stop_lanes <code>: a stop signal reached this script. Remember it, and send TERM to the process group of
# every lane that still runs. Every lane has a group of its own (set -m), which holds its go test, the test
# binary and what that started. The groups come from "jobs -r -p", which lists only the jobs bash has not
# reaped, so no PID recorded earlier is ever signalled (a reaped lane's number can name another process).
stop_lanes() {
  local pg
  stop_code=$1
  for pg in $(jobs -r -p); do
    kill -TERM -- "-$pg" 2> /dev/null
  done
}

# own_lanes <prebuilt>: every group, the groups of different packages at the same time. With prebuilt 0 a
# warm-up build comes first (see the top of this file); "all" passes 1, since its package run has compiled
# everything already.
own_lanes() {
  local prebuilt=$1 i g name pkg dir running warm=0 began tag st b e serial=0 failed="" status=0 wall
  local -a lanes pids events_seen shown
  local -A dirs
  mapfile -t lanes < <(lane_packages)
  own_work=$(mktemp -d) || return 1
  trap 'rm -rf "$own_work"' EXIT
  trap 'stop_lanes 129' HUP
  trap 'stop_lanes 130' INT
  trap 'stop_lanes 143' TERM
  set -m

  if [ "$prebuilt" -eq 0 ] && [ "${#lanes[@]}" -gt 1 ]; then
    began=$SECONDS
    go test -count=1 -exec true "${lanes[@]}" > "$own_work/warmup.log" 2>&1 < /dev/null &
    wait "$!"
    warm=$?
    if [ "$stop_code" -eq 0 ]; then
      if [ "$warm" -eq 0 ]; then
        echo "own: warm-up build of ${#lanes[@]} packages took $((SECONDS - began)) s"
      else
        cat "$own_work/warmup.log"
        echo "FAIL hack/go-test.sh: the warm-up build of the groups' packages failed with status ${warm}: the lanes run anyway, each compiling for itself, and the step fails"
      fi
    fi
  fi
  if [ "$stop_code" -ne 0 ]; then
    echo "FAIL hack/go-test.sh: own stopped by a signal before the lanes started"
    return "$stop_code"
  fi

  SECONDS=0
  echo "own: ${#groups[@]} groups in ${#lanes[@]} lanes, one per package: a package's groups run one after another, the lanes at the same time"
  for i in "${!lanes[@]}"; do
    dir="$own_work/lane$i"
    if ! mkdir "$dir"; then
      # Lanes already started must not outlive the step: stop them and wait for them.
      stop_lanes 1
      wait
      return 1
    fi
    : > "$dir/events"
    dirs[${lanes[$i]}]=$dir
    events_seen[i]=0
    shown[i]=0
    run_lane "$dir" "${lanes[$i]}" &
    pids[i]=$!
    echo "own: lane ${lanes[$i]} started: $(lane_groups "${lanes[$i]}")"
  done

  while :; do
    running=" $(jobs -r -p | tr '\n' ' ')"
    for i in "${!lanes[@]}"; do
      show_events "$i" "${dirs[${lanes[$i]}]}" "${lanes[$i]}"
      if [ "${shown[$i]}" -eq 0 ] && [[ "$running" != *" ${pids[$i]} "* ]]; then
        print_lane "${dirs[${lanes[$i]}]}" "${lanes[$i]}"
        shown[i]=1
      fi
    done
    [ "$stop_code" -eq 0 ] || break
    [ "$(jobs -r -p | wc -l)" -gt 0 ] || break
    sleep 1
  done
  # Every exit path waits for every lane: nothing is left running when the step ends. After a stop the
  # lanes were sent TERM above, and what they printed so far is shown.
  for i in "${!lanes[@]}"; do
    wait "${pids[$i]}" 2> /dev/null
    if [ "${shown[$i]}" -eq 0 ]; then
      show_events "$i" "${dirs[${lanes[$i]}]}" "${lanes[$i]}"
      print_lane "${dirs[${lanes[$i]}]}" "${lanes[$i]}"
      shown[i]=1
    fi
  done
  set +m

  echo "== own: each group's start, end and WALL time in seconds since own began, which includes its build and its checks: read a margin to -timeout from the ok line of the group's own output above, not from this"
  for g in "${groups[@]}"; do
    name=$(field "$g" 1)
    pkg=$(field "$g" 2)
    dir=${dirs[$pkg]}
    if [ -f "$dir/$name.result" ] && read -r st b e < "$dir/$name.result"; then
      tag=ok
      [ "$st" -eq 0 ] || tag=FAIL
      printf '%-5s %-24s %-28s +%ss..+%ss, %ss wall (build and checks included), -timeout %s\n' "$tag" "$name" "$pkg" "$b" "$e" "$((e - b))" "$(field "$g" 4)"
      serial=$((serial + e - b))
      if [ "$st" -ne 0 ]; then
        failed+=" $name"
        [ "$status" -ne 0 ] || status=$st
      fi
    else
      printf '%-5s %-24s %-28s no result: the group did not end\n' FAIL "$name" "$pkg"
      failed+=" $name"
      [ "$status" -ne 0 ] || status=1
    fi
  done
  wall=$SECONDS
  echo "own: ${wall} s in all; the groups' wall times add to ${serial} s"
  if [ -n "$failed" ]; then
    echo "FAIL hack/go-test.sh: own: the groups that failed:${failed}"
    [ "${GITHUB_ACTIONS:-}" != true ] || echo "::error title=hack/go-test.sh own::the groups that failed:${failed}"
  fi
  [ "$status" -ne 0 ] || [ "$warm" -eq 0 ] || status=1
  [ "$stop_code" -eq 0 ] || status=$stop_code
  return "$status"
}

# own [group] [prebuilt]: that group alone, in the foreground, or every group in lanes, each even if
# another failed. prebuilt is 1 when the packages' run has just compiled everything ("all").
own() {
  local g want=${1:-} prebuilt=${2:-0} status=0 ran=0
  if [ -z "$want" ]; then
    own_lanes "$prebuilt"
    return
  fi
  for g in "${groups[@]}"; do
    if [ "$(field "$g" 1)" = "$want" ]; then
      ran=1
      run_group "$g"
      status=$?
    fi
  done
  if [ "$ran" -eq 0 ]; then
    echo "usage: hack/go-test.sh own [group]: there is no group ${want}" >&2
    return 2
  fi
  return "$status"
}

case "${1:-}" in
  packages)
    [ "$#" -eq 1 ] || { echo "usage: hack/go-test.sh packages" >&2; exit 2; }
    packages
    ;;
  own)
    [ "$#" -le 2 ] || { echo "usage: hack/go-test.sh own [group]" >&2; exit 2; }
    own "${2:-}"
    ;;
  all)
    [ "$#" -eq 1 ] || { echo "usage: hack/go-test.sh all" >&2; exit 2; }
    packages
    first=$?
    own "" 1
    second=$?
    [ "$first" -eq 0 ] || exit "$first"
    exit "$second"
    ;;
  *)
    echo "usage: hack/go-test.sh packages|own [group]|all" >&2
    exit 2
    ;;
esac
