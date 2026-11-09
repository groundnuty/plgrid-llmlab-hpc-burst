package livechecks

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Issue #297: real-engine reads the job's history after its cancel, before it says NOT RUN. The read at the pending bound says
// PENDING, but the job can start in the seconds before the cleanup's cancel, and a started job is never NOT RUN (the science
// agent's ruling of 8 Nov 2026, #272; the rows with a test have read the job's record after their cancel since PR 284).
//
// These go through the script's real path, `--run real-engine --yes`, over the stub cluster: the job stays PENDING on a clock that
// moves an hour at each poll, the cleanup cancels it by its tag, and the script then asks sacct --json what became of it. What sacct
// says is the real GH200 reply of a job cancelled before it started (issue #267, PR #269), read from the tree each time. The started
// and unreadable cases are that same document with ONE value of it edited, and say which; none is a form built from what the script
// expects. The expected sentences are written out here, not read from the script.

const (
	neverStartedReplyPath = "internal/slurm/testdata/v0.0.42/sacct-never-started.json"
	noJobsReplyPath       = "internal/slurm/testdata/v0.0.42/sacct-no-jobs.json"
	errorsReplyPath       = "internal/slurm/testdata/v0.0.42/sacct-errors.json"

	reNotRunLine    = "real-engine: NOT RUN (a job was still pending after 30 min in the queue and was cancelled by its tag; not a failure)"
	reUnreadable    = "real-engine: FAIL (the job's history could not be read after its cancel, so it cannot be called a job that never started: "
	reStartedLine   = "real-engine: FAIL (the job had started by its cancel, with a start time of "
	reLeftInQueue   = "real-engine: FAIL (the cleanup left the job in the queue, so its history cannot show that it never started)"
	realStartText   = `"start": 4294967294,`
	realEndText     = `"end": 1793351462,`
	realStateText   = `"current": ["CANCELLED"]`
	realJobIDText   = `"job_id": 4311,`
	startedEpoch    = "1793351410" // 52 s before the capture's end, 10 s after its submission: a start that is a time
	startedAtItsEnd = "1793351462"
)

// realReply is the real never-started reply, after checking that it is the form the issue is about: without that, an edit of
// it, or a fixture that was replaced, would test nothing (the same precondition as internal/providerctl/noval_start_test.go).
func realReply(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), neverStartedReplyPath))
	if err != nil {
		t.Fatal(err)
	}
	doc := string(b)
	for _, want := range []string{realStartText, realEndText, realStateText, realJobIDText} {
		if strings.Count(doc, want) != 1 {
			t.Fatalf("precondition: %s does not hold %q once, so these tests start from something other than the real reply of a job cancelled before it started", neverStartedReplyPath, want)
		}
	}
	return doc
}

// edited is the real reply with one text of it replaced (it must occur once; realReply checked the four that are used).
func edited(t *testing.T, from, to string) string {
	t.Helper()
	doc := realReply(t)
	if strings.Count(doc, from) != 1 {
		t.Fatalf("the real reply does not hold %q once", from)
	}
	return strings.Replace(doc, from, to, 1)
}

func fixtureFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), path))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// historyRun runs real-engine to its pending bound, with sacct answering doc (the real reply, when empty) and env.
func historyRun(t *testing.T, doc string, env ...string) (*world, result) {
	t.Helper()
	w := newWorld(t)
	w.useStubGo()
	extra := append(w.fakeClock(), "STUB_CLOCK_STEP=3600", "STUB_JOB_FOREVER=PENDING", "LIVE_SACCT_GAP=0")
	if doc != "" {
		p := filepath.Join(w.state, "reply.json")
		write(t, p, doc, 0o644)
		extra = append(extra, "STUB_SACCT_JSON="+p)
	}
	return w, w.run(append(extra, env...), "--run", "real-engine", "--yes")
}

// afterItsCancel checks that the history was read, with the arguments the capture was made with, and only after the cancel: a read
// before it would be the read the bound already made.
func afterItsCancel(t *testing.T, w *world, reads int) {
	t.Helper()
	ev := lines(w.stateFile("events"))
	cancel, first := -1, -1
	for i, l := range ev {
		switch {
		case l == "scancel 7001" && cancel < 0:
			cancel = i
		case strings.HasPrefix(l, "sacct json ") && first < 0:
			first = i
		}
	}
	if cancel < 0 || first < 0 || first < cancel {
		t.Errorf("the history must be read after the cancel (scancel at event %d, first read at %d):\n%s", cancel, first, strings.Join(ev, "\n"))
	}
	argv := lines(w.stateFile("sacct-json-argv"))
	if len(argv) != reads {
		t.Errorf("sacct --json was asked %d times, want %d", len(argv), reads)
	}
	for _, a := range argv {
		if a != "-j 7001 --json -X -D" {
			t.Errorf("sacct was asked %q, want the job by its number, as the real capture was made: -j 7001 --json -X -D", a)
		}
	}
}

func cleanedUp(t *testing.T, w *world, res result) {
	t.Helper()
	if !cleanupLeftNone.MatchString(res.out) {
		t.Errorf("the cleanup did not verify that it left nothing behind:\n%s", res.out)
	}
	if got := lines(w.stateFile("cancelled")); len(got) != 1 || got[0] != "7001" {
		t.Errorf("cancelled %v, want the submitted job once", got)
	}
}

// Outcome 1: the history shows a job that never started, in the form the real reply has (a bare 4294967294 start, CANCELLED),
// and the run is NOT RUN, exit 3, as before. The cases that still end so: a login banner before the reply, accounting that
// answers nothing for its first two reads, and one that has not yet got the cancel on its first two (a job that is not CANCELLED
// yet is not a job that never started, so the read is asked again).
func TestRealEngineNotRunNeverStartedStaysNotRunAfterItsHistoryIsRead(t *testing.T) {
	for _, c := range []struct {
		name  string
		env   []string
		reads int
	}{
		{"the real reply", nil, 1},
		{"a login banner before it", []string{"STUB_BANNER=1"}, 1},
		{"accounting answers nothing for two reads", []string{"STUB_SACCT_JSON_EMPTY=2"}, 3},
		{"accounting shows the job PENDING for two reads, before it has the cancel", []string{"STUB_SACCT_JSON_SHOWS=PENDING", "STUB_SACCT_JSON_LAG=2"}, 3},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			realReply(t)
			w, res := historyRun(t, "", c.env...)
			if res.code != 3 {
				t.Fatalf("exit %d, want 3 (NOT RUN: the history shows the job never started):\n%s", res.code, res.out)
			}
			w.contains(res, reNotRunLine)
			cleanedUp(t, w, res)
			afterItsCancel(t, w, c.reads)
			if strings.Contains(res.out, "real-engine: FAIL") || strings.Contains(res.out, "real-engine: PASS") {
				t.Errorf("a job that never started was also reported as a pass or a failure:\n%s", res.out)
			}
		})
	}
}

// Outcome 2: the history shows a start, so the job started in the seconds before the cancel, and the run is a FAIL that names it,
// exit 1, never NOT RUN. Each is the real reply with its time.start edited: to a time before the end (a job cancelled while it ran),
// and, with the state and end a running job has, to a time that is not yet ended (sacct gives a running job no end: 0).
func TestRealEngineNotRunAJobThatStartedBeforeItsCancelIsAFailure(t *testing.T) {
	for _, c := range []struct {
		name string
		doc  func(t *testing.T) string
	}{
		{"a start before the end", func(t *testing.T) string { return edited(t, realStartText, `"start": `+startedEpoch+`,`) }},
		{"a start, no end yet, the state RUNNING", func(t *testing.T) string {
			doc := edited(t, realStartText, `"start": `+startedEpoch+`,`)
			doc = strings.Replace(doc, realEndText, `"end": 0,`, 1)
			return strings.Replace(doc, realStateText, `"current": ["RUNNING"]`, 1)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			w, res := historyRun(t, c.doc(t))
			if res.code != 1 {
				t.Fatalf("exit %d, want 1 (a started job is never NOT RUN):\n%s", res.code, res.out)
			}
			w.contains(res, reStartedLine+startedEpoch+" as sacct gives it: a started job is never NOT RUN)")
			cleanedUp(t, w, res)
			afterItsCancel(t, w, 1)
			if strings.Contains(res.out, "real-engine: NOT RUN") {
				t.Errorf("a job that started was reported NOT RUN:\n%s", res.out)
			}
		})
	}
}

// Outcome 3: a read that fails, or cannot tell, is a FAIL that names the unreadable state, exit 1, never NOT RUN and never a pass,
// after five reads (LIVE_SACCT_GAP apart, as fetch_real_engine asks sacct more than once). The replies are the repo's real-shape
// fixtures (no jobs, an error), and the real one with ONE value edited. A start that is not the bare NO_VAL is a time only when it is
// a number that is one: the forms nobody has seen here (a triple, a 64-bit marker, 0) are not read as a start nor as never started.
func TestRealEngineNotRunAHistoryItCannotReadIsAFailureThatNamesIt(t *testing.T) {
	for _, c := range []struct {
		name  string
		doc   func(t *testing.T) string
		env   []string
		label string
		reads int
	}{
		{"sacct fails", nil, []string{"STUB_SACCT_JSON_RC=1"}, "sacct failed", 5},
		{"sacct fails behind a login banner", nil, []string{"STUB_SACCT_JSON_RC=1", "STUB_BANNER=1"}, "sacct failed", 5},
		{"sacct answers nothing", nil, []string{"STUB_SACCT_JSON_EMPTY=99"}, "sacct returned nothing", 5},
		{"the reply has no job", func(t *testing.T) string { return fixtureFile(t, noJobsReplyPath) }, nil, "the reply does not list this job exactly once", 5},
		{"the reply carries an error", func(t *testing.T) string { return fixtureFile(t, errorsReplyPath) }, nil, "the reply carries no empty errors list", 5},
		{"the start is a triple", func(t *testing.T) string {
			return edited(t, realStartText, `"start": {"set": false, "infinite": false, "number": 0},`)
		}, nil, "its start is not a plain number", 5},
		{"the start is the 64-bit marker", func(t *testing.T) string { return edited(t, realStartText, `"start": 18446744073709551614,`) }, nil, "its start is a marker of a form not known", 5},
		{"the start is 0", func(t *testing.T) string { return edited(t, realStartText, `"start": 0,`) }, nil, "its start is a marker of a form not known", 5},
		{"the start is the end", func(t *testing.T) string { return edited(t, realStartText, `"start": `+startedAtItsEnd+`,`) }, nil, "its end is not after its start", 5},
		{"the job is not CANCELLED yet, and its start is not set", nil, []string{"STUB_SACCT_JSON_SHOWS=PENDING", "STUB_SACCT_JSON_LAG=99"}, "its start is not set but its state is not CANCELLED yet", 5},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			doc := ""
			if c.doc != nil {
				doc = c.doc(t)
			}
			w, res := historyRun(t, doc, c.env...)
			if res.code != 1 {
				t.Fatalf("exit %d, want 1 (missing evidence is no NOT RUN):\n%s", res.code, res.out)
			}
			w.contains(res, reUnreadable+c.label+")")
			cleanedUp(t, w, res)
			afterItsCancel(t, w, c.reads)
			if strings.Contains(res.out, "real-engine: NOT RUN") || strings.Contains(res.out, "real-engine: PASS") {
				t.Errorf("a history that cannot be read was reported as NOT RUN or a pass:\n%s", res.out)
			}
		})
	}
}

// The cleanup runs on every path, and a job it leaves behind is exit 1: when the cancel does nothing the job is still in the queue,
// its history is not that of a cancelled job, so none is read and none can make it NOT RUN. Written out here, apart from the
// three outcomes above, as the cleanup's own condition.
func TestRealEngineNotRunAJobTheCleanupLeavesIsAFailureAndItsHistoryIsNotRead(t *testing.T) {
	w, res := historyRun(t, "", "STUB_SCANCEL_KEEP=1")
	if res.code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
	}
	w.contains(res, reLeftInQueue, "cleanup: left behind 1", "a job was left behind")
	if strings.Contains(res.out, "real-engine: NOT RUN") {
		t.Errorf("a run that left its job in the queue was reported NOT RUN:\n%s", res.out)
	}
	if n := w.stateFile("sacct-json-argv"); n != "" {
		t.Errorf("the history of a job that is still in the queue was read:\n%s", n)
	}
	if got := lines(w.stateFile("cancelled")); len(got) == 0 {
		t.Errorf("the cleanup did not try to cancel the job")
	}
}
