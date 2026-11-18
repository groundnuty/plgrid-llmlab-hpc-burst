package testenv

// hack/go-test.sh is what hack/check.sh and CI's go job run (issue #282). It runs groups of tests on
// timeouts of their own and leaves those tests out of the packages' run, so a mistake in it could drop a
// test without a word: go test exits 0 when -run matches nothing, a skip reports as success, and -skip
// applies to every package. These tests run the script with a double in front of the real go on PATH,
// which records its arguments and answers as a test case says. They start nothing else and run offline.
// The expected flags, groups and names are literals here, not read from the script.

import (
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// outlivedWait is how long a script run may hold the output pipe open after the script exits.
const outlivedWait = 30 * time.Second

// The groups as this test expects them: the lease test (#291), internal/lifecycle's other control-plane
// tests in three groups and cmd/hpcburst-controller's failover tests (#295), and hack/livechecks' dry
// runs through the script (PR #306), host-dialed and the rest, and internal/agent's two real-Envoy tests
// that wait out Envoy's own defaults (issue #429). Part B of issue #295 adds the slowest tests of
// internal/lifecycle and hack/livechecks in three more groups: lifecycle-apiserver (envtest),
// lifecycle-slow (fake-clock, no control plane and no port) and livechecks-tree. Part B2 adds
// lifecycle-slow2, eight more fake-clock tests of internal/lifecycle, and part B3 lifecycle-slow3, six more:
// four serial ones and the two whose subtests run in parallel. Part B4 adds lifecycle-slow4, thirty-four more
// fake-clock tests of internal/lifecycle, the slowest rows that #708, #721 and #724 added or slowed.
const ownTestName = "TestLifecycleManagerReleasesItsLeaseOnEveryStop"

var (
	lifecycleCaches = []string{"TestCachesNeedsEveryWatchedInformer", "TestCachesRecoverFromAStalledDiscovery",
		"TestCachesStopDuringAStalledDiscovery", "TestCachesListIsWhatTheControllerWatches"}
	lifecycleCopies = []string{"TestMetricsStandbyOnTwoManagers", "TestMetricsLostLeaseStopsBeforeANewCopyLeads",
		"TestMetricsCopiesStartAgainOnALostPort", "TestRaisedEventsOnTwoManagers"}
	lifecycleControlPlane = []string{"TestManagerOnControlPlane", "TestVerificationRecordRoundTripsThroughTheAPIServer",
		"TestLifecycleLeaseReleaseUnderLatency", "TestStartLifecycleLeaderOfAManagerThatLeadsBeforeTheLeaseIsRead"}
	lifecycleAPIServer = []string{"TestOperationsOnAControlPlaneWithTwoAgents", "TestMetricsKeeperModeThroughNewManager",
		"TestRecordedOutcomesPassTheAPIServersRules"}
	lifecycleSlow = []string{"TestScaleDownWithASurplus", "TestARankedOutAllocationIsKeptUnderItsPolicy",
		"TestHandoverLateReplacementDegradesThenTakesOver", "TestHandoverReplacementCreatedAtItsPlannedTime",
		"TestAWaitingCandidateIsNotFailedByStartTimeout", "TestACandidateWithNoTunnelFailsAtItsDeadline",
		"TestNoOverlapOneSlotSuccessorWaitsForTheFlagToClear", "TestAWarmAllocationDoesNotCancelAPendingSurplus",
		"TestGateAWrongEntryReleasesAtMostTwoWhenInstancesGoOnWarmAllocations", "TestAMarkedPeerThatRestartsNeverOutranks",
		"TestAReplacementHedgesByItsWaits"}
	lifecycleSlow2 = []string{"TestReservedSlotKeepsAFailedSuccessorsSlotForItsRetry", "TestRetentionNoneOutlivesHardEnd",
		"TestReservedSlotKeepsACutShortSuccessorsSlotAcrossARestart", "TestMetricsNoProgressNeedsFreshKnownCounters",
		"TestNoOverlapModeBoundaries", "TestRetentionJobEndsAndLeavesListing", "TestTunnelDownAloneRestartsNothing",
		"TestMetricsDrainEndsOnAKnownZeroReadAfterTheDrainBegan"}
	lifecycleSlow3 = []string{"TestVerifyRefusedGenerationConfigStopsClass", "TestReadyInstanceStaysReadyWhenTunnelDrops",
		"TestGateAMissTheFullRecordCantHoldSuspendsItsEntry", "TestGateTwoProvidersOfOneClassSuspendAWrongEntryWithinTwoReleases",
		"TestTheTwoJobGuardKeepsALongWaitToTwoAllocations", "TestADeadStepLeavesTheDeploymentAtItsLastEnd"}
	lifecycleSlow4       = []string{"TestACandidateRanksAgainstItsServingPeer", "TestTheFirstToPassIsChosenAndTheLaterOutranksIt", "TestAnExtraIsRankedOutWhenItsPeerHasAReplacement", "TestAReplacementGoesOnAWarmAllocationOnlyIfItDrainsAtLeastTheLeadLater", "TestNoExtraWhereThePassMadeNoNewAllocationForAPrimary", "TestAKeptLoserIsNeverRankedAgainstAnotherPrimaryWhenItsWinnerLeaves", "TestGateDoesNotJudgeAWaitingCandidateAgainAfterItRestarts", "TestAnExtraThatPassesFirstWaitsForTheRanking", "TestAStartedLoserEndsAtTheWinWhenAnyOfItsConditionsFails", "TestAKeptLosersMarkIsReadAfterARestart", "TestHandoverRestartPlansTheSameTime", "TestAFailedPrimaryWithAReplacementLeavesTheWaitingExtraChosen", "TestAnOutrankingCandidateThatRestartsBeforeItsWinnerDrains", "TestHandoverControllerKilledAtEachStep", "TestGateRecordOnlyIsFinalForAWaitingCandidate", "TestAKeptLoserRankingPointCases", "TestAReReadyOutrankerWaitsTheFullWindowFromItsNewTransition", "TestHandoverBothServeThenPrimaryDrainsAndSecondReplacementIsPlanned", "TestAStartedLoserIsRankedAgainstItsWinnerAlone", "TestHandoverSameClassSameIDAndRollout", "TestTheExtraAndTheSlotBeyondItsOwnHandovers", "TestAStartedLoserIsNeverComparedWithAnotherPrimary", "TestAKeptLoserIsChosenWhenTheDeploymentIsShort", "TestAFailedPrimaryLeavesTheExtraAsThePrimary", "TestAnExtraWhoseJobStartsFirstIsChosenAndGetsTheReplacement", "TestAStartedLoserIsKept", "TestHandoverLeaseEndExtensionMovesDrainStart", "TestAStartedLoserThirtyMinutesAfterItsPredecessorIsNotPlacedOn", "TestAWarmPlacementCannotSlipBetweenTheCountAndTheHeadroom", "TestLoweringInstancesLeavesANonReadyPrimaryAsTheExtra", "TestAWinnerThatFailsLeavesTheLoserChosen", "TestAReplacementGoesOnAWarmAllocationOnlyIfItDrainsLater", "TestAKeptLoserThatFailsACheckIsReleased", "TestNoOverlapWarmAllocationTakesTheReplacement"}
	controllerFailover   = []string{"TestControllerProcessesFailOverMidSubmit", "TestControllerProcessesFailOverBeforeSubmit"}
	controllerIdentities = []string{"TestEachManagerOfTheBinaryRunsUnderItsOwnIdentity", "TestAnOperationsCommitIsRecoveredAndExecutedUnderTheLifecycleIdentity",
		"TestTheLifecycleIdentityStampsTheOwnerHashLabel", "TestControllerIdentitiesAreWhoTheyClaimAndNothingElseCan"}
	envoyTimeouts = []string{"TestEnvoyStreamIdleOutlivesTheDefault", "TestEnvoyClosesIdleUpstreamConnectionsFirst"}
	hostDialedDry = []string{"TestEachHostDialedFaultFailsTheRowThroughTheScript", "TestEveryHostDialedRowPassesThroughTheScriptOverTheFake",
		"TestAHostDialedRowWhoseSlotIsHeldIsNotRunThroughTheScript", "TestADryHostDialedBinaryRefusesLiveSettings"}
	otherDry = []string{"TestEveryAlphaRowPassesItsDryRunAndEveryFaultFailsIt", "TestADryBinaryRefusesLiveSettings", "TestAJobPendingPastTheBoundIsNotRunOnEveryRow",
		"TestALiveRunWithNoListenerFailsLoudlyAndUsesNoCluster", "TestAnAccountingRowWithNoSlotThatPendedPastItsWaitIsNotRun",
		"TestEveryRowThatPostsNamesWhereTheFrontRanAndRefusesWithoutIt", "TestACatchUpThatCannotTellIsAFailureOnATwoJobRow",
		"TestASecondJobThatNeverStartedIsNotRunOnTheTwoJobRows", "TestEveryReachRowReportsEachOutcomeThroughTheScript",
		"TestADryReachBinaryRefusesLiveSettingsAndARealGateway", "TestAReachRowWhoseSlotIsHeldIsNotRun"}
	livechecksTree = []string{"TestRegistryFailsOnTheRealTreeWhenAPartIsRemoved", "TestPlanMapsChangedFilesToTheLiveTestsThatRunThem",
		"TestTheBulkRowsFourVerdictsThroughTheScript", "TestTheFindingMarkerIsTheBulkRowsAlone",
		"TestMutateAlphaEndsAHangAtItsBoundAndRestoresTheFile"}
)

type group struct {
	name, pkg, timeout string
	tests              []string
}

var groups = []group{
	{"lease", "./internal/lifecycle", "150s", []string{ownTestName}},
	{"lifecycle-caches", "./internal/lifecycle", "180s", lifecycleCaches},
	{"lifecycle-copies", "./internal/lifecycle", "190s", lifecycleCopies},
	{"lifecycle-control-plane", "./internal/lifecycle", "170s", lifecycleControlPlane},
	{"lifecycle-apiserver", "./internal/lifecycle", "130s", lifecycleAPIServer},
	{"lifecycle-slow", "./internal/lifecycle", "520s", lifecycleSlow},
	{"lifecycle-slow2", "./internal/lifecycle", "330s", lifecycleSlow2},
	{"lifecycle-slow3", "./internal/lifecycle", "350s", lifecycleSlow3},
	{"lifecycle-slow4", "./internal/lifecycle", "490s", lifecycleSlow4},
	{"controller-failover", "./cmd/hpcburst-controller", "460s", controllerFailover},
	{"controller-identities", "./cmd/hpcburst-controller", "150s", controllerIdentities},
	{"envoy-timeouts", "./internal/agent", "1200s", envoyTimeouts},
	{"livechecks-hostdialed", "./hack/livechecks", "150s", hostDialedDry},
	{"livechecks-dry", "./hack/livechecks", "270s", otherDry},
	{"livechecks-tree", "./hack/livechecks", "390s", livechecksTree},
}

func anchored(names []string) string { return "^(" + strings.Join(names, "|") + ")$" }

func everyName() []string {
	var all []string
	for _, g := range groups {
		all = append(all, g.tests...)
	}
	return all
}

var wantPackages = "test -count=1 -v -skip " + anchored(everyName()) + " ./..."

func (g group) wantCall() string {
	return "test -count=1 -v -timeout " + g.timeout + " -run " + anchored(g.tests) + " " + g.pkg
}

// passed is what go test prints for a group whose tests all passed.
func (g group) passed() string {
	var b strings.Builder
	for _, t := range g.tests {
		b.WriteString("=== RUN   " + t + "\n--- PASS: " + t + " (1.20s)\n")
	}
	b.WriteString("PASS\nok  \texample/" + strings.TrimPrefix(g.pkg, "./") + "\t9.9s\n")
	return b.String()
}

// goDouble is what the double answers: for the packages' run (told apart by its -skip), and for each
// group's run, told apart by the group's first test name in -run. A group with no answer prints nothing
// and exits 0, as a -run that matched nothing does.
type goDouble struct {
	packagesCode string
	warmUpCode   string            // the warm-up build's exit code ("own" runs it first, "-exec true")
	outputs      map[string]string // group name -> output
	codes        map[string]string // group name -> exit code
}

func allPassed() goDouble {
	d := goDouble{packagesCode: "0", warmUpCode: "0", outputs: map[string]string{}, codes: map[string]string{}}
	for _, g := range groups {
		d.outputs[g.name], d.codes[g.name] = g.passed(), "0"
	}
	return d
}

func groupNamed(t *testing.T, name string) group {
	t.Helper()
	for _, g := range groups {
		if g.name == name {
			return g
		}
	}
	t.Fatalf("no group %s", name)
	return group{}
}

// runGoTestScript runs the script in root (the repository's, or a stand-in tree) with the double as go,
// and returns its output, its exit code and the double's calls, one line each. "own" with no group runs
// the groups in lanes, so the double's calls from different packages arrive in no fixed order, and it
// appends each as one line.
func runGoTestScript(t *testing.T, root string, d goDouble, args ...string) (string, int, []string) {
	t.Helper()
	return runGoTestScriptEnv(t, root, d, nil, args...)
}

// runGoTestScriptEnv is runGoTestScript with more environment for the script. GITHUB_ACTIONS is cleared
// first, since CI sets it and the script then folds each lane's output; a test that wants the folding
// sets it back.
func runGoTestScriptEnv(t *testing.T, root string, d goDouble, env []string, args ...string) (string, int, []string) {
	t.Helper()
	dir := t.TempDir()
	calls := filepath.Join(dir, "calls")
	answers := filepath.Join(dir, "answers")
	if err := os.Mkdir(answers, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, g := range groups {
		if out, ok := d.outputs[g.name]; ok {
			if err := os.WriteFile(filepath.Join(answers, "out-"+g.tests[0]), []byte(out), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(answers, "code-"+g.tests[0]), []byte(d.codes[g.name]), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$GO_DOUBLE_CALLS"
case "$*" in
  *-exec*) exit "$GO_DOUBLE_WARM_UP_CODE" ;;
  *-skip*) exit "$GO_DOUBLE_PACKAGES_CODE" ;;
esac
for f in "$GO_DOUBLE_ANSWERS"/out-*; do
  [ -e "$f" ] || continue
  first=${f##*/out-}
  case "$*" in
    *"($first|"*|*"($first)"*) cat "$f"; exit "$(cat "$GO_DOUBLE_ANSWERS/code-$first")" ;;
  esac
done
exit 0
`
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", append([]string{filepath.Join(root, "hack", "go-test.sh")}, args...)...)
	cmd.Env = append(append(os.Environ(),
		"GITHUB_ACTIONS=",
		"PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"GO_DOUBLE_CALLS="+calls, "GO_DOUBLE_ANSWERS="+answers,
		"GO_DOUBLE_PACKAGES_CODE="+d.packagesCode, "GO_DOUBLE_WARM_UP_CODE="+d.warmUpCode), env...)
	// A process the script leaves running (a lane's) keeps the output pipe open, and CombinedOutput would
	// wait for it for ever: bound the wait, and call it a failure.
	cmd.WaitDelay = outlivedWait
	began := time.Now()
	out, err := cmd.CombinedOutput()
	code := 0
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		code = exit.ExitCode()
	} else if err != nil && !errors.Is(err, exec.ErrWaitDelay) {
		t.Fatal(err)
	}
	if time.Since(began) >= outlivedWait {
		t.Errorf("a process the script started outlived it and held its output open for %s", outlivedWait)
	}
	raw, err := os.ReadFile(calls)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	var lines []string
	if s := strings.TrimSpace(string(raw)); s != "" {
		lines = strings.Split(s, "\n")
	}
	return string(out), code, lines
}

func realRoot(t *testing.T) string {
	t.Helper()
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// The packages' run leaves out every group's tests by their exact, anchored names and keeps go test's
// default timeout; each group runs alone, with its own timeout, in its own package.
func TestGoTestScriptPassesTheExactFlags(t *testing.T) {
	root := realRoot(t)
	out, code, calls := runGoTestScript(t, root, allPassed(), "packages")
	if code != 0 || len(calls) != 1 || calls[0] != wantPackages {
		t.Errorf("packages: exit %d, go called %q, want once with %q; output:\n%s", code, calls, wantPackages, out)
	}
	for _, g := range groups {
		out, code, calls := runGoTestScript(t, root, allPassed(), "own", g.name)
		if code != 0 || len(calls) != 1 || calls[0] != g.wantCall() {
			t.Errorf("own %s: exit %d, go called %q, want once with %q; output:\n%s", g.name, code, calls, g.wantCall(), out)
		}
	}
}

// A group passes only when every one of its tests has passed and nothing in it skipped, whatever go test
// exits with.
func TestGoTestScriptOwnNeedsEveryTestsPassLineAndNoSkip(t *testing.T) {
	root := realRoot(t)
	lease, hd := groupNamed(t, "lease"), groupNamed(t, "livechecks-hostdialed")
	leaseGuard := "FAIL hack/go-test.sh: " + ownTestName + " did not run and pass"
	for _, c := range []struct {
		name, group, output, code string
		wantCode                  int
		wantLine                  string // a line the guard prints, or "" for none
	}{
		{"it passed", "lease", lease.passed(), "0", 0, ""},
		{"it was renamed: -run matches nothing", "lease", "testing: warning: no tests to run\nPASS\nok  \texample\t0.1s [no tests to run]\n", "0", 1, leaseGuard},
		{"it skipped", "lease", "--- SKIP: " + ownTestName + " (0.00s)\nPASS\nok  \texample\t0.1s\n", "0", 1, leaseGuard},
		{"only a longer name passed", "lease", "--- PASS: " + ownTestName + "Again (1.00s)\nPASS\n", "0", 1, leaseGuard},
		{"only a subtest passed", "lease", "    --- PASS: " + ownTestName + " (1.00s)\nPASS\n", "0", 1, leaseGuard},
		{"it failed: go test's own exit is kept", "lease", "--- FAIL: " + ownTestName + " (1.00s)\nFAIL\n", "3", 3, ""},
		{"every test of a group passed", "livechecks-hostdialed", hd.passed(), "0", 0, ""},
		{"one test of a group did not run", "livechecks-hostdialed", strings.Replace(hd.passed(), "--- PASS: "+hostDialedDry[2]+" (", "--- SKIP: "+hostDialedDry[2]+" (", 1), "0", 1,
			"FAIL hack/go-test.sh: " + hostDialedDry[2] + " did not run and pass"},
		// A subtest that skips (RequireTools with no sshd) leaves its parent's PASS line in place.
		{"a subtest skipped", "livechecks-hostdialed", strings.Replace(hd.passed(), "--- PASS: "+hostDialedDry[0]+" (", "    --- SKIP: "+hostDialedDry[0]+"/no-u (0.00s)\n--- PASS: "+hostDialedDry[0]+" (", 1), "0", 1,
			"FAIL hack/go-test.sh: the livechecks-hostdialed group skipped a test or a subtest"},
	} {
		t.Run(c.name, func(t *testing.T) {
			d := allPassed()
			d.outputs[c.group], d.codes[c.group] = c.output, c.code
			out, code, _ := runGoTestScript(t, root, d, "own", c.group)
			if code != c.wantCode {
				t.Errorf("exit %d, want %d; output:\n%s", code, c.wantCode, out)
			}
			if c.wantLine != "" && !strings.Contains(out, c.wantLine) {
				t.Errorf("the guard did not say %q; output:\n%s", c.wantLine, out)
			}
			if c.wantLine == "" && strings.Contains(out, "FAIL hack/go-test.sh") {
				t.Errorf("a guard spoke where none should:\n%s", out)
			}
			if !strings.Contains(out, c.output) {
				t.Errorf("go test's own output was not passed on, so CI's log would lose it:\n%s", out)
			}
		})
	}
}

// "own" with no group, and "all", run every group, each even when an earlier one failed, and fail if any
// does; "all" runs the packages first. The groups of one package run in table order and the lanes of
// different packages in no fixed order (lanes_test.go runs them at the same time), so the calls are
// compared lane by lane, and as a set.
func TestGoTestScriptAllRunsEveryPart(t *testing.T) {
	root := realRoot(t)
	failed := func(group string) goDouble {
		d := allPassed()
		g := groupNamed(t, group)
		d.outputs[group], d.codes[group] = "--- FAIL: "+g.tests[0]+" (1.00s)\nFAIL\n", "1"
		return d
	}
	for _, c := range []struct {
		name     string
		mode     string
		d        goDouble
		wantCode int
	}{
		{"all: every part passes", "all", allPassed(), 0},
		{"all: the packages fail", "all", func() goDouble { d := allPassed(); d.packagesCode = "1"; return d }(), 1},
		{"all: the first group fails", "all", failed("lease"), 1},
		{"all: the last group fails", "all", failed("livechecks-tree"), 1},
		{"all: the envoy-timeouts group fails", "all", failed("envoy-timeouts"), 1},
		{"own: every group passes", "own", allPassed(), 0},
		{"own: the first group fails, the rest still run", "own", failed("lease"), 1},
		{"own: a group matched nothing", "own", func() goDouble { d := allPassed(); delete(d.outputs, "livechecks-hostdialed"); return d }(), 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			out, code, calls := runGoTestScript(t, root, c.d, c.mode)
			if code != c.wantCode {
				t.Errorf("exit %d, want %d; go called\n%s\noutput:\n%s", code, c.wantCode, strings.Join(calls, "\n"), out)
			}
			groupCalls := calls
			if c.mode == "all" {
				// The package run is first and alone: the groups' lanes start after it, with no warm-up of their
				// own, since the package run has compiled every test variant.
				if len(calls) == 0 || calls[0] != wantPackages {
					t.Fatalf("all: go's first call was not the packages' run %q; calls:\n%s", wantPackages, strings.Join(calls, "\n"))
				}
				groupCalls = calls[1:]
			}
			for _, p := range ownCallProblems(groupCalls, c.mode == "own") {
				t.Errorf("%s; go called\n%s\noutput:\n%s", p, strings.Join(calls, "\n"), out)
			}
		})
	}
}

func TestGoTestScriptRefusesAnUnknownModeOrGroup(t *testing.T) {
	root := realRoot(t)
	for _, args := range [][]string{nil, {"everything"}, {"own", "nosuch"}, {"own", "lease", "again"}, {"packages", "extra"}, {"all", "lease"}} {
		_, code, calls := runGoTestScript(t, root, allPassed(), args...)
		if code != 2 || len(calls) != 0 {
			t.Errorf("%q: exit %d, go called %q; want exit 2 and no call, so a typo in a caller does not pass with no test run", args, code, calls)
		}
	}
}

// Each name a group lists belongs to exactly one test in the root module, in the group's own package, so
// that the packages' -skip hides nothing else, in any package.
func TestGoTestScriptNamesRealTestsOnce(t *testing.T) {
	root := realRoot(t)
	for _, g := range groups {
		for _, name := range g.tests {
			found := declarations(t, root, name)
			if len(found) != 1 || "./"+filepath.Dir(found[0]) != g.pkg {
				t.Errorf("%s is declared in %q, want once, in %s", name, found, g.pkg)
			}
		}
	}
}

func declarations(t *testing.T, root, name string) []string {
	t.Helper()
	decl := regexp.MustCompile(`(?m)^func ` + name + `\(`)
	var found []string
	err := filepath.WalkDir(root, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if e.IsDir() {
			switch e.Name() {
			case ".git", "spikes", "third_party": // not the root module
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if decl.Match(src) {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			found = append(found, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

// The script refuses to run when a name it lists is declared twice, or nowhere: the -skip would hide a
// second test of that name, or the group would run nothing. A stand-in tree holds the script and test
// files, so that the real tree is never changed.
func TestGoTestScriptRefusesANameDeclaredTwiceOrNowhere(t *testing.T) {
	real := realRoot(t)
	script, err := os.ReadFile(filepath.Join(real, "hack", "go-test.sh"))
	if err != nil {
		t.Fatal(err)
	}
	stand := t.TempDir()
	write := func(rel, content string) {
		p := filepath.Join(stand, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	write("hack/go-test.sh", string(script))
	for _, name := range everyName() {
		write("t/"+name+"_test.go", "package t\n\nfunc "+name+"(t *testing.T) {}\n")
	}
	// A second declaration of one name, and none of another; spikes and third_party do not count.
	write("other/twice_test.go", "package other\n\nfunc "+hostDialedDry[1]+"(t *testing.T) {}\n")
	write("spikes/s1/x_test.go", "package x\n\nfunc "+otherDry[0]+"(t *testing.T) {}\n")
	if err := os.Remove(filepath.Join(stand, "t", otherDry[3]+"_test.go")); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"packages"}, {"own", "livechecks-hostdialed"}, {"own", "livechecks-dry"}} {
		out, code, calls := runGoTestScript(t, stand, allPassed(), args...)
		if code == 0 || len(calls) != 0 {
			t.Errorf("%q: exit %d, go called %q; want a failure before go runs:\n%s", args, code, calls, out)
		}
		wantNames := map[string][]string{"packages": {hostDialedDry[1], otherDry[3]}, "livechecks-hostdialed": {hostDialedDry[1]}, "livechecks-dry": {otherDry[3]}}[args[len(args)-1]]
		for _, n := range wantNames {
			if !strings.Contains(out, "FAIL hack/go-test.sh: "+n+" is declared in ") {
				t.Errorf("%q: the guard did not name %s:\n%s", args, n, out)
			}
		}
		if strings.Contains(out, otherDry[0]+" is declared") {
			t.Errorf("%q: a declaration under spikes was counted:\n%s", args, out)
		}
	}
	// The lease group's own name is declared once there: it runs.
	if _, code, calls := runGoTestScript(t, stand, allPassed(), "own", "lease"); code != 0 || len(calls) != 1 {
		t.Errorf("own lease in the stand-in tree: exit %d, calls %q", code, calls)
	}
}

// CI's go job and hack/check.sh both go through the script, every group included, so the two cannot drift,
// and neither runs the packages by hand: that would run the groups' tests twice, or lose their timeouts.
func TestGoTestScriptIsWhatCIAndCheckRun(t *testing.T) {
	root := realRoot(t)
	const byHand = "go test -count=1 -v ./..."
	// The go-own job runs every group in one step, "own"; the line must end there (a step for one group,
	// "own lease", would run that group twice beside the lanes: ciProblems reads CI's steps for that).
	ci := []string{"bash hack/go-test.sh packages", "run: bash hack/go-test.sh own\n"}
	for _, c := range []struct {
		file string
		want []string
	}{
		{filepath.Join(".github", "workflows", "ci.yml"), ci},
		{filepath.Join("hack", "check.sh"), []string{"bash hack/go-test.sh all"}},
	} {
		raw, err := os.ReadFile(filepath.Join(root, c.file))
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range c.want {
			if !strings.Contains(string(raw), w) {
				t.Errorf("%s does not run %q", c.file, strings.TrimSpace(w))
			}
		}
		if strings.Contains(string(raw), byHand) {
			t.Errorf("%s still runs %q by hand", c.file, byHand)
		}
	}
}
