package livechecks

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// The ruling of the science agent of 8 Nov 2026 (#272): a job that never started within its bound is NOT RUN, exit 3,
// on EVERY row, whichever bound ran out (a start wait, the wait in the queue, or a tunnel row's deployment end). A FAIL
// means the property the row tests was absent, and a queue wait tests nothing. This replaces #230's FAIL for a row
// with no slot.
//
// The table is read off the cluster table of the script, not off a list of rows, and every row of it must have an
// entry here with the bound that applies to it, written out from the ruling and the issues (#222, #194, #225), never
// read from the script: a row added later fails this test until its case is added, so a rule by row cannot drift in.
// A test of only the slot rows would prove nothing, since those were NOT RUN before the ruling: the rows with no slot
// (conformance-helios, alpha-athena-handover, tunnel-hour-athena, gpu-accounting-athena) are the ones it changed.

const (
	notRunStartWait10 = "NOT RUN (a job did not start within 10 min of its acceptance and was cancelled by its tag; not a failure)"
	notRunStartWait30 = "NOT RUN (a job did not start within 30 min of its acceptance and was cancelled by its tag; not a failure)"
	notRunPending30   = "NOT RUN (a job was still pending after 30 min in the queue and was cancelled by its tag; not a failure)"
	notRunPending2h   = "NOT RUN (a job was still pending after 120 min in the queue and was cancelled by its tag; not a failure)"
)

// neverStartedBound is the sentence each cluster row ends with when its job never starts, and whether the row has a
// one-job slot (so the table has both kinds).
var neverStartedBound = map[string]struct {
	line   string
	inSlot bool
}{
	"conformance-helios":       {notRunStartWait30, false},
	"conformance-helios-gh200": {notRunStartWait10, true},
	"conformance-athena":       {notRunStartWait10, true},
	"real-engine":              {notRunPending30, true},
	"alpha-athena-handover":    {notRunPending2h, false},
	"alpha-helios-nooverlap":   {notRunPending30, true},
	"tunnel-hour-helios":       {notRunPending30, true},
	"tunnel-hour-athena":       {notRunPending2h, false},
	"gpu-accounting-helios":    {notRunStartWait10, true},
	"gpu-accounting-athena":    {notRunPending2h, false},
	"reach-helios":             {notRunStartWait10, true},
	"reach-athena":             {notRunStartWait10, true},
}

var cleanupLeftNone = regexp.MustCompile(`cleanup: left behind 0 \(cancelled \d+\)`)

// neverStartedRun runs the row in the world its kind runs in under the stubs, with its test (or, for real-engine,
// its job) ending as a job that never started does.
func neverStartedRun(t *testing.T, name string, extra ...string) (*world, result) {
	t.Helper()
	w := slotWorld(t, name)
	switch {
	case name == "real-engine":
		// The job stays pending on a clock that moves an hour at each poll, past the bound the row has.
		extra = append(append(w.fakeClock(), "STUB_CLOCK_STEP=3600", "STUB_JOB_FOREVER=PENDING"), extra...)
	case name == "conformance-helios", name == "conformance-helios-gh200", name == "conformance-athena":
		extra = append([]string{"STUB_TEST_MODE=not-run"}, extra...)
	default:
		extra = append([]string{"STUB_ALPHA_MODE=not-run"}, extra...)
	}
	return w, w.run(extra, "--run", name, "--yes")
}

func TestEveryRowMapsAJobThatNeverStartedToNotRun(t *testing.T) {
	b, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	var slots, noSlots int
	for _, l := range block(t, string(b), "CLUSTER SHAPES") {
		name := strings.Split(l, "|")[0]
		seen[name] = true
		want, ok := neverStartedBound[name]
		if !ok {
			t.Errorf("%s is a row of the cluster table with no case here: say which bound ends it as NOT RUN when its job never starts", name)
			continue
		}
		if want.inSlot {
			slots++
		} else {
			noSlots++
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			w, res := neverStartedRun(t, name)
			if res.code != 3 {
				t.Fatalf("exit %d, want 3 (NOT RUN, whichever bound ran out):\n%s", res.code, res.out)
			}
			w.contains(res, name+": "+want.line)
			if !cleanupLeftNone.MatchString(res.out) {
				t.Errorf("the cleanup was not verified (want a line that says it left nothing behind):\n%s", res.out)
			}
			if strings.Contains(res.out, name+": FAIL") || strings.Contains(res.out, name+": PASS") {
				t.Errorf("a job that never started was also reported as a pass or a failure:\n%s", res.out)
			}
		})
	}
	for name := range neverStartedBound {
		if !seen[name] {
			t.Errorf("%s has a case here but is no row of the cluster table: the case is stale", name)
		}
	}
	// A table over slot rows only proves nothing: those rows were NOT RUN before the ruling.
	if slots == 0 || noSlots == 0 {
		t.Errorf("the table covers %d rows in a slot and %d with none, want both kinds", slots, noSlots)
	}
}

// The same job that never started is a failure when the cleanup cannot cancel it: the job is still on the platform,
// and a run that leaves one behind is a failure whichever bound ran out and with or without a slot. The cancel does
// nothing here (STUB_SCANCEL_KEEP), so the queue still holds the job after the cleanup.
func TestANeverStartedJobTheCleanupCannotCancelIsAFailureOnRowsWithAndWithoutASlot(t *testing.T) {
	for _, c := range []struct {
		row, mode, test string
	}{
		{"conformance-helios", "not-run-leave", "STUB_TEST_MODE"},
		{"conformance-helios-gh200", "not-run-leave", "STUB_TEST_MODE"},
		{"tunnel-hour-athena", "untagged-not-run", "STUB_ALPHA_MODE"},
		{"tunnel-hour-helios", "untagged-not-run", "STUB_ALPHA_MODE"},
	} {
		t.Run(c.row, func(t *testing.T) {
			t.Parallel()
			w := slotWorld(t, c.row)
			res := w.run(quick(c.test+"="+c.mode, "STUB_SCANCEL_KEEP=1"), "--run", c.row, "--yes")
			if res.code != 1 {
				t.Fatalf("exit %d, want 1 (a job left behind is a failure, even after a NOT RUN):\n%s", res.code, res.out)
			}
			if cleanupLeftNone.MatchString(res.out) || !strings.Contains(res.out, "cleanup: left behind 1") {
				t.Errorf("want a cleanup that says it left one job behind:\n%s", res.out)
			}
			if got := lines(w.stateFile("cancelled")); len(got) == 0 {
				t.Errorf("the cleanup did not try to cancel the job")
			}
		})
	}
}

// A local row has no queue to wait in, so no marker makes it a NOT RUN: not its own, and not one an earlier row of the
// same command left in its record directory (the script keeps that directory's name until the next cluster row).
func TestAMarkerAnEarlierRowLeftIsNeverALocalRowsNotRun(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run([]string{"STUB_TEST_MODE=not-run"}, "--run", "conformance-helios-gh200", "llmlab-smoke", "--yes")
	if !strings.Contains(res.out, "conformance-helios-gh200: "+notRunStartWait10) {
		t.Fatalf("the cluster row did not end NOT RUN, so this case looked at nothing:\n%s", res.out)
	}
	if !strings.Contains(res.out, "llmlab-smoke: FAIL") || strings.Contains(res.out, "llmlab-smoke: NOT RUN") {
		t.Errorf("a local row took an earlier row's marker for its own NOT RUN:\n%s", res.out)
	}
	if res.code != 1 {
		t.Errorf("exit %d, want 1 (a failure is above a NOT RUN):\n%s", res.code, res.out)
	}
}
