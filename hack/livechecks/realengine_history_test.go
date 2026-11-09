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
// says is the repo's fixture of the reply for a job cancelled before it started, read from the tree each time. Its shape (the keys,
// their nesting, the bare NO_VAL start) is a real GH200 reply's (issue #267, PR #269); its VALUES are invented, a reconstruction of
// the captured shape (internal/slurm/testdata/README.md says so), not the literal reply. Every other case is that document with the
// values named in its case edited (one value, or, where the case says so, several that go together); none is a form built from what
// the script expects. The expected sentences are written out here, not read from the script.

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
	realRestartText = `"restart_cnt": 0,`
	realArrayText   = "\"array\": {\n    \"job_id\": 0,"
	realHetText     = `"het": {"job_id": 0,`
	realStepsText   = `"steps": [],`
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
	for _, want := range []string{realStartText, realEndText, realStateText, realJobIDText, realRestartText, realArrayText, realHetText, realStepsText} {
		if strings.Count(doc, want) != 1 {
			t.Fatalf("precondition: %s does not hold %q once, so these tests start from something other than the real reply of a job cancelled before it started", neverStartedReplyPath, want)
		}
	}
	return doc
}

// edited is the fixture with one text of it replaced (it must occur once; realReply checked the ones the cases use).
func edited(t *testing.T, from, to string) string {
	t.Helper()
	return editedAll(t, from, to)
}

// editedAll is the fixture with each text of from, to, from, to, ... replaced in turn; each must occur once in the document as it
// stands when its turn comes.
func editedAll(t *testing.T, pairs ...string) string {
	t.Helper()
	if len(pairs)%2 != 0 {
		t.Fatalf("editedAll wants from, to pairs, got %d texts", len(pairs))
	}
	doc := realReply(t)
	for i := 0; i < len(pairs); i += 2 {
		if strings.Count(doc, pairs[i]) != 1 {
			t.Fatalf("the fixture does not hold %q once", pairs[i])
		}
		doc = strings.Replace(doc, pairs[i], pairs[i+1], 1)
	}
	return doc
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

// Outcome 1: the history shows a job that never started, in the form of the fixture (the shape a real reply has: a bare 4294967294 start, CANCELLED, restart count 0),
// and the run is NOT RUN, exit 3, as before. The cases that still end so: a login banner before the reply, accounting that
// answers nothing for its first two reads, and one that has not yet got the cancel on its first two (a job that is not CANCELLED
// yet is not a job that never started, so the read is asked again).
func TestRealEngineNotRunNeverStartedStaysNotRunAfterItsHistoryIsRead(t *testing.T) {
	for _, c := range []struct {
		name  string
		env   []string
		reads int
	}{
		{"the fixture", nil, 1},
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
// exit 1, never NOT RUN. Each is the fixture with its time.start edited to a time: before the end (a job cancelled while it ran), and,
// in the second case, three values that go together (start, end 0 and the state RUNNING: what sacct gives a job that is still running).
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
// fixtures (no jobs, an error), and the never-started one with one value edited. A start that is not the bare NO_VAL is a time only when it is
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

// What the decode of the reply says when the document is not exactly this job's one record (review of PR 300, B2). The stub's job is 7001
// (the stub writes it over the fixture's 4311, so a case that means "this job" leaves 4311 where it is, and one that means "another
// job" writes a different number). Each case is a reply the first version of the check (a regex over the flattened text) read as a job
// that never started, or named by another label; each is a FAIL, exit 1, never NOT RUN.
const (
	reNotOneDocument = "the reply is not one JSON document"
	reNotThisJob     = "the reply does not list this job exactly once"
	reMoreThanThis   = "the reply lists more than this job"
	reArrayOrHet     = "the record does not show that it is neither an array nor a heterogeneous job"
	reHasSteps       = "the record lists steps"
	reRestartCount   = "its restart count is not a known zero"
)

func TestRealEngineNotRunAReplyThatIsNotExactlyThisJobsRecordIsAFailure(t *testing.T) {
	const cut = ",\n \"warnings\": []\n}"
	const endOfJobs = "  }\n ],\n \"meta\": {"
	for _, c := range []struct {
		name  string
		doc   func(t *testing.T) string
		label string
	}{
		{"the reply is cut off right after its errors list", func(t *testing.T) string { return edited(t, cut, "") }, reNotOneDocument},
		{"a start that is there twice", func(t *testing.T) string {
			return edited(t, realStartText, `"start": `+startedEpoch+",\n    "+realStartText)
		}, reNotOneDocument},
		{"a second job record, with no start in it", func(t *testing.T) string {
			return edited(t, endOfJobs, "  },\n  {\"job_id\": 7002, \"name\": \"hb-example-other\"}\n ],\n \"meta\": {")
		}, reMoreThanThis},
		{"only the record of another job, a plain one", func(t *testing.T) string { return edited(t, realJobIDText, `"job_id": 7002,`) }, reNotThisJob},
		{"only the record of another job, whose array carries this job's number", func(t *testing.T) string {
			return editedAll(t, realJobIDText, `"job_id": 7002,`, realArrayText, "\"array\": {\n    \"job_id\": 7001,")
		}, reNotThisJob},
		{"only the record of another job, whose het part carries this job's number", func(t *testing.T) string {
			return editedAll(t, realJobIDText, `"job_id": 7002,`, realHetText, `"het": {"job_id": 7001,`)
		}, reNotThisJob},
		{"this job's record, which is an array job (its array number is this job's)", func(t *testing.T) string {
			return edited(t, realArrayText, "\"array\": {\n    \"job_id\": 7001,")
		}, reArrayOrHet},
		{"this job's record, which is a heterogeneous job (its het number is this job's)", func(t *testing.T) string {
			return edited(t, realHetText, `"het": {"job_id": 7001,`)
		}, reArrayOrHet},
		{"this job's record, whose array number is another job's", func(t *testing.T) string {
			return edited(t, realArrayText, "\"array\": {\n    \"job_id\": 7050,")
		}, reArrayOrHet},
		{"this job's record, with no array number", func(t *testing.T) string { return edited(t, realArrayText, `"array": {`) }, reArrayOrHet},
		{"this job's record, which lists a step", func(t *testing.T) string {
			return edited(t, realStepsText, `"steps": [{"step": {"id": "batch"}}],`)
		}, reHasSteps},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			w, res := historyRun(t, c.doc(t))
			if res.code != 1 {
				t.Fatalf("exit %d, want 1 (a reply that is not exactly this job's record is no evidence that it never started):\n%s", res.code, res.out)
			}
			w.contains(res, reUnreadable+c.label+")")
			cleanedUp(t, w, res)
			afterItsCancel(t, w, 5)
			if strings.Contains(res.out, "real-engine: NOT RUN") || strings.Contains(res.out, "real-engine: PASS") {
				t.Errorf("a reply that is not this job's one record was reported NOT RUN or a pass:\n%s", res.out)
			}
		})
	}
}

// A job whose restart count is not known to be zero is not read as one that never started (review of PR 300, B1): a job that ran between
// two polls and was requeued can show, in accounting, only its latest execution, cancelled before it started. The count is read from
// the record as a whole number that is 0; a missing, malformed or other value is a FAIL naming it.
func TestRealEngineNotRunAJobWithoutAKnownZeroRestartCountIsNotNeverStarted(t *testing.T) {
	for _, c := range []struct{ name, text string }{
		{"a restart count of 1", `"restart_cnt": 1,`},
		{"no restart count", ``},
		{"a restart count of false", `"restart_cnt": false,`},
		{"a restart count that is the text 0", `"restart_cnt": "0",`},
		{"a restart count of null", `"restart_cnt": null,`},
		{"a restart count of 0.0", `"restart_cnt": 0.0,`},
		{"a restart count below zero", `"restart_cnt": -1,`},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			w, res := historyRun(t, edited(t, realRestartText, c.text))
			if res.code != 1 {
				t.Fatalf("exit %d, want 1 (a job with no known zero restart count is not known to have never started):\n%s", res.code, res.out)
			}
			w.contains(res, reUnreadable+reRestartCount+")")
			cleanedUp(t, w, res)
			afterItsCancel(t, w, 5)
			if strings.Contains(res.out, "real-engine: NOT RUN") {
				t.Errorf("a job without a known zero restart count was reported NOT RUN:\n%s", res.out)
			}
		})
	}
}

// The decode runs under python3, which the host must have: a host without it fails before the run submits anything (it would
// otherwise wait out the queue for a job and only then say it cannot read the history), and a python3 that fails or answers
// something this script does not understand at the read is a FAIL that says so, never NOT RUN.
func TestRealEngineNotRunWithoutPythonFailsBeforeItSubmitsAnything(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run([]string{"LIVE_PYTHON=" + filepath.Join(t.TempDir(), "no-such-python3")}, "--run", "real-engine", "--yes")
	if res.code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
	}
	w.contains(res, "real-engine: FAIL (python3 is not on this host, and the read of a pending job's history needs it)")
	if ev := w.stateFile("events"); strings.Contains(ev, "sbatch submit") || strings.Contains(ev, "go build") {
		t.Errorf("a run that cannot decode the history went on to build or submit:\n%s", ev)
	}
}

func TestRealEngineNotRunAPythonThatFailsOrIsNotUnderstoodIsAFailureThatSaysSo(t *testing.T) {
	for _, c := range []struct{ name, body, label string }{
		{"python3 exits with an error", "exit 3", "the reply could not be decoded (python3 did not run it)"},
		{"python3 answers nothing", "exit 0", "the decoder's answer was not understood"},
		{"python3 answers something else", "echo 'rec 1 2 CANCELLED 0 and more'", "the decoder's answer was not understood"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			py := filepath.Join(t.TempDir(), "python3")
			write(t, py, "#!/bin/bash\n"+c.body+"\n", 0o755)
			w, res := historyRun(t, "", "LIVE_PYTHON="+py)
			if res.code != 1 {
				t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
			}
			w.contains(res, reUnreadable+c.label+")")
			cleanedUp(t, w, res)
			afterItsCancel(t, w, 5)
			if strings.Contains(res.out, "real-engine: NOT RUN") {
				t.Errorf("a decode that failed was reported NOT RUN:\n%s", res.out)
			}
		})
	}
}

// The cleanup could not read the queue (review of PR 300, nit): that is not "the job was left in the queue", which says what was
// seen. The job is still cancelled by its known number, the run is a FAIL, exit 1, and no history is read.
func TestRealEngineNotRunAQueueTheCleanupCannotReadIsAFailureThatSaysSo(t *testing.T) {
	w, res := historyRun(t, "", "STUB_SQUEUE_LIST_FAILS_AFTER_POLL=1")
	if res.code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
	}
	w.contains(res,
		"real-engine: FAIL (the cleanup could not read the queue, so it cannot confirm that the job was cancelled, and its history cannot show that it never started)",
		"cleanup: the queue could not be read, so the cleanup could not look for this run's jobs by tag or confirm a cancel (cancelled 1 known job(s))")
	for _, no := range []string{"real-engine: NOT RUN", reLeftInQueue, "cleanup: left behind"} {
		if strings.Contains(res.out, no) {
			t.Errorf("a cleanup that could not read the queue printed %q:\n%s", no, res.out)
		}
	}
	if n := w.stateFile("sacct-json-argv"); n != "" {
		t.Errorf("the history was read although the cleanup could not confirm the cancel:\n%s", n)
	}
	if got := lines(w.stateFile("cancelled")); len(got) == 0 || got[0] != "7001" {
		t.Errorf("cancelled %v, want the known job", got)
	}
}
