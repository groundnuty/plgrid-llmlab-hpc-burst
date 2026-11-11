package alpha

// Row A of issue #376 (nowserve.go): its clock against the shipped profile, its start wait as its first clock, the
// requeue's line and reply, HBEP-3's tag against the contracts' helper, and its NOT RUN on a refused requeue. Expected
// values come from the profile file, HBEP-3 and the science agent's rulings on #376, written out here.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The plgrid-now rows run under profiles/athena-now.yaml as shipped (Q1): their clock is that file's margins, read from
// the file here, so rows.go holds no second copy that could drift. With the deployment's 5-minute deadline the drain
// window is 7 minutes and the lead 20 (PR #385), and row A's 48-minute lease is longer than the lead.
func TestTheNowRowsClockIsTheShippedProfiles(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "profiles", "athena-now.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var pp v1alpha1.PlatformProfile
	if err := yaml.UnmarshalStrict(b, &pp); err != nil {
		t.Fatal(err)
	}
	read := func(name string, d *v1alpha1.Duration) time.Duration {
		if d == nil {
			t.Fatalf("athena-now.yaml has no %s", name)
		}
		x, err := d.AsDuration()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return x
	}
	tm := AthenaNowTiming()
	if got := read("drainMargin", pp.Spec.DrainMargin); got != tm.DrainMargin {
		t.Errorf("the profile's drainMargin is %s, the rows' %s", got, tm.DrainMargin)
	}
	if got := read("handoverMargin", pp.Spec.HandoverMargin); got != tm.HandoverMargin {
		t.Errorf("the profile's handoverMargin is %s, the rows' %s", got, tm.HandoverMargin)
	}
	if pp.Spec.Health == nil {
		t.Fatal("athena-now.yaml has no health block")
	}
	if got := read("health.startTimeout", pp.Spec.Health.StartTimeout); got != tm.StartTimeout {
		t.Errorf("the profile's health.startTimeout is %s, the rows' %s", got, tm.StartTimeout)
	}
	maxReq := v1alpha1.Duration(tm.MaxRequestDuration.String())
	dw, err := v1alpha1.InstanceDrainWindow(pp.Spec, &v1alpha1.Routing{MaxRequestDuration: &maxReq})
	if err != nil {
		t.Fatal(err)
	}
	if dw != 7*time.Minute || DrainWindow(tm.MaxRequestDuration, tm.DrainMargin, 0) != dw {
		t.Errorf("the drain window is %s by the API's rule and %s by the rows', want 7 min", dw, DrainWindow(tm.MaxRequestDuration, tm.DrainMargin, 0))
	}
	r := rowOf("alpha-athena-now-serve")
	if r.Clock().Lead() != 20*time.Minute || r.Lease <= r.Clock().Lead() || r.Lease != 48*time.Minute {
		t.Errorf("row A's lead is %s and its lease %s, want 20 and 48 min", r.Clock().Lead(), r.Lease)
	}
	// Q3: the row's whole hold of the slot, its start wait included, is at most the hour.
	if r.StartWait != 10*time.Minute || r.SlotHold != 60*time.Minute || r.StartWait+r.Lease+2*time.Minute > time.Hour {
		t.Errorf("row A's start wait %s, lease %s and hold %s: the hour does not bound the hold", r.StartWait, r.Lease, r.SlotHold)
	}
	if r.ProfileName() != "athena-now" || r.Partition != "plgrid-now" || r.QOS != "" || r.MostJobs != 1 || r.GPUs != 1 || r.OwnerGo != OwnerGoAthenaNow {
		t.Errorf("row A is %+v", r)
	}
}

// A row with a start wait has it as its first clock, whatever the operator's wait (the plan's section 3; the script's
// header): 10 minutes for row A at a wait of 30 minutes or 2 hours, and the run's deadline its budget alone.
func TestTheStartWaitIsTheFirstClockOfARowThatHasOne(t *testing.T) {
	r := rowOf("alpha-athena-now-serve")
	for _, pending := range []time.Duration{time.Minute, 30 * time.Minute, 2 * time.Hour} {
		if b, pastEnd := FirstClock(r, pending, r.Lease, 7*time.Minute); b != 10*time.Minute || pastEnd {
			t.Errorf("at a wait of %s the first clock is %s (past the end %t), want the start wait, 10 min", pending, b, pastEnd)
		}
		if d := r.RunDeadline(pending); d != 115*time.Minute {
			t.Errorf("at a wait of %s the run's deadline is %s, want its budget, 115 min", pending, d)
		}
	}
	// The deployment's first end still bounds it when that comes first: a time limit of 12 minutes less 7 and 1.
	if b, pastEnd := FirstClock(r, time.Hour, 12*time.Minute, 7*time.Minute); b != 4*time.Minute || !pastEnd {
		t.Errorf("with a 12-minute time limit the first clock is %s (past the end %t), want 4 min, past the end", b, pastEnd)
	}
	// A row with none keeps the operator's wait.
	if b, _ := FirstClock(rowOf("alpha-athena-handover"), 30*time.Minute, MinLease(), 15*time.Minute); b != 30*time.Minute {
		t.Errorf("the handover's first clock is %s, want the wait asked, 30 min", b)
	}
	note := NotRunNote(r, time.Hour, &NotStartedError{Bound: 10 * time.Minute, StartWait: true})
	if want := "alpha-athena-now-serve: a job did not start within the start wait of 10m0s from the deployment's creation; this run's jobs were cancelled by tag\n"; note != want {
		t.Errorf("the note is %q, want %q", note, want)
	}
}

// The dry run of row A's job pending past its start wait: NOT RUN, its job cancelled by its tag at 10 minutes whatever
// wait was asked, the readings of the manual clock one second either side.
func TestRowAsJobPendingPastItsStartWaitIsNotRun(t *testing.T) {
	row := rowOf("alpha-athena-now-serve")
	cfg := Config{Row: row, Dry: true, DryFault: "pend-past-bound", Out: filepath.Join(t.TempDir(), "out"), PendingMax: 2 * time.Hour, FrontLabel: testFrontLabel}
	rec, err := NewRecorder(cfg.Out)
	if err != nil {
		t.Fatal(err)
	}
	var logged []string
	logf := func(format string, a ...any) {
		logged = append(logged, sprintf(format, a...))
	}
	failures, err := Run(context.Background(), cfg, newDrySteps(t, cfg, "nowserve"), rec, logf)
	var ns *NotStartedError
	if !errors.As(err, &ns) || !ns.StartWait || ns.Bound != 10*time.Minute || len(failures) != 0 || !NotRun(row, err) {
		t.Fatalf("failures %v, err %v; want the NOT RUN of the start wait of 10 min", failures, err)
	}
	for _, want := range []string{"stand-in: the job is still pending 9m59s after its submission", "stand-in: the run's jobs are cancelled by their tags 10m1s after the job's submission"} {
		if !containsLine(logged, want) {
			t.Errorf("the run did not log %q; it logged %q", want, logged)
		}
	}
}

func TestTheRequeueLineTakesAJobIDOfDigitsOnly(t *testing.T) {
	if l, err := RequeueHoldLine("4242"); err != nil || l != "scontrol requeuehold 4242" {
		t.Errorf("RequeueHoldLine(4242) = %q, %v", l, err)
	}
	for _, bad := range []string{"", "42 43", "42;rm", "-1", "4_2", "42\n", "abc"} {
		if _, err := RequeueHoldLine(bad); err == nil {
			t.Errorf("RequeueHoldLine(%q) took it", bad)
		}
	}
}

// The scheduler's reply: exit 0 is an accepted requeue, any other a refusal whose reason is one fixed label, which never
// carries the scheduler's own text (it can name the job's ID).
func TestARequeueReplyIsAcceptedOrRefusedWithAFixedLabel(t *testing.T) {
	if r := RequeueReplyOf(0, nil, nil); !r.Accepted || r.Refusal != "" {
		t.Errorf("exit 0 read as %+v", r)
	}
	for _, c := range []struct{ text, want string }{
		{"scontrol: error: Requested operation is presently disabled for job 12345", refusalDisabled},
		{"scontrol: error: Access/permission denied for job 12345", refusalDenied},
		{"slurm_load_jobs error: Invalid job id specified", refusalNotNow},
		{"scontrol: error: Job has already finished", refusalNotNow},
		{"scontrol: error: the fake supports only: scontrol release <jobid>[,<jobid>...]", refusalNoCommand},
		{"something else for job 12345 at /scratch/people/example", refusalOther},
	} {
		r := RequeueReplyOf(1, nil, []byte(c.text))
		if r.Accepted || r.Refusal != c.want {
			t.Errorf("%q read as %+v, want the refusal %q", c.text, r, c.want)
		}
		if regexp.MustCompile(`[0-9/]`).MatchString(r.Refusal) {
			t.Errorf("the label %q carries a digit or a path", r.Refusal)
		}
	}
}

// HBEP3Tag is computed from HBEP-3's text; the contracts' helper is an independent implementation of the same rule, so
// the two agree on names under and over the 40 characters a tag keeps.
func TestHBEP3TagIsTheContractsTag(t *testing.T) {
	for _, c := range []struct{ ns, name, uid string }{
		{"default", "alpha-0123456789-ab12c", "00000000-0000-4000-8000-000000000001"},
		{"default", strings.Repeat("n", 41), "00000000-0000-4000-8000-000000000002"},
		{"other", "a", "00000000-0000-4000-8000-000000000003"},
	} {
		if got, want := HBEP3Tag(c.ns, c.name, c.uid), v1alpha1.JobTag(c.ns, c.name, types.UID(c.uid)); got != want {
			t.Errorf("HBEP3Tag(%s/%s) = %q, the contracts' helper %q", c.ns, c.name, got, want)
		}
	}
}

// A refused requeue is NOT RUN, never a pass (Q5): with nothing else failed the run returns ErrRequeueRefused, after its
// record (verdict "not run") and its text to post, which says NOT RUN with the reason; with a failure beside it, the
// failure wins and the run is a FAIL.
func TestARefusedRequeueIsNotRunUnlessAnotherCheckFails(t *testing.T) {
	row := rowOf("alpha-athena-now-serve")
	for _, c := range []struct {
		fault      string
		wantNotRun bool
	}{{"requeue-refused", true}, {"requeue-refused-cut", false}, {"requeue-refused-no-drain", false}} {
		t.Run(c.fault, func(t *testing.T) {
			cfg := Config{Row: row, Dry: true, DryFault: c.fault, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
			rec, err := NewRecorder(cfg.Out)
			if err != nil {
				t.Fatal(err)
			}
			failures, err := Run(context.Background(), cfg, newDrySteps(t, cfg, "nowserve"), rec, t.Logf)
			record, _ := os.ReadFile(filepath.Join(cfg.Out, RecordFile))
			posts, _ := os.ReadFile(filepath.Join(cfg.Out, PostsFile))
			if c.wantNotRun {
				if !errors.Is(err, ErrRequeueRefused) || !NotRun(row, err) || len(failures) != 0 {
					t.Fatalf("failures %v, err %v; want the NOT RUN of a refused requeue", failures, err)
				}
				if !strings.Contains(string(record), `"verdict": "not run"`) && !strings.Contains(string(record), `"verdict":"not run"`) {
					t.Errorf("the record does not say not run:\n%s", record)
				}
				if !strings.Contains(string(posts), "alpha-athena-now-serve: NOT RUN") || !strings.Contains(string(posts), refusalDisabled) || strings.Contains(string(posts), ": PASS") {
					t.Errorf("the text to post does not say NOT RUN with the reason:\n%s", posts)
				}
				if note := NotRunNote(row, cfg.PendingMax, err); !strings.Contains(note, requeueRefusedWords+" ("+refusalDisabled+")") {
					t.Errorf("the marker's note %q does not say the requeue was refused, with the reason", note)
				}
				return
			}
			if err != nil || len(failures) == 0 || NotRun(row, err) {
				t.Fatalf("failures %v, err %v; want a FAIL that outranks the refusal", failures, err)
			}
			if !strings.Contains(string(posts), "alpha-athena-now-serve: FAIL") || !strings.Contains(string(posts), "refused by the scheduler") {
				t.Errorf("the text to post does not say FAIL beside the refusal:\n%s", posts)
			}
		})
	}
}

// The requeue's verdict has three parts (the science agent's ruling 4 on #376): the Instance Failed with reason Requeued;
// no second Allocation or job; the held job cancelled by the release. Each alone fails the run, from a healthy
// observation that passes.
func TestTheRequeueVerdictNeedsEachOfItsThreeParts(t *testing.T) {
	ok := RequeueObs{Attempted: true, Accepted: true, DrainingSeen: true, EndReason: v1alpha1.ReasonRequeued, AllocationReleased: true, JobEnded: true, JobRequeued: true}
	if f := requeueChecks(ok); len(f) != 0 {
		t.Fatalf("the healthy requeue fails: %v", f)
	}
	for name, c := range map[string]struct {
		mutate func(*RequeueObs)
		want   string
	}{
		"ended Drained":            {func(o *RequeueObs) { o.EndReason = v1alpha1.ReasonDrained }, "did not end with reason Requeued"},
		"no end seen":              {func(o *RequeueObs) { o.EndReason = "" }, "did not end with reason Requeued"},
		"a replacement Allocation": {func(o *RequeueObs) { o.NewAllocations = 1 }, "followed by a replacement"},
		"a replacement Instance":   {func(o *RequeueObs) { o.NewInstances = 1 }, "followed by a replacement"},
		"not released":             {func(o *RequeueObs) { o.AllocationReleased = false }, "release did not end the held job"},
		"the job not ended":        {func(o *RequeueObs) { o.JobEnded = false }, "release did not end the held job"},
		"not requeued":             {func(o *RequeueObs) { o.JobRequeued = false }, "does not show the job requeued"},
		"no drain first":           {func(o *RequeueObs) { o.DrainingSeen = false }, "not seen draining"},
	} {
		o := ok
		c.mutate(&o)
		if f := Summary(requeueChecks(o)); !strings.Contains(f, c.want) {
			t.Errorf("%s: failed checks %q lack %q", name, f, c.want)
		}
	}
	// A refused requeue judges only the window before it.
	if f := requeueChecks(RequeueObs{Attempted: true, DrainingSeen: true, Refusal: refusalDisabled}); len(f) != 0 {
		t.Errorf("a refused requeue with the drain seen fails: %v", f)
	}
}

// identityChecks against a record built from HBEP-3's rule, and one break per check.
func TestTheDeploymentIDUnderItsTagIsCheckedAsHBEP3Says(t *testing.T) {
	full := strings.Repeat("0123456789abcdef", 4)
	tag := HBEP3Tag("default", "alpha-x", "u")
	ok := IdentityObs{Read: true, Tag: tag, JobName: tag, ExternalName: tag, ObservedTag: tag, DeploymentID: full, DisplayLabel: full[:12],
		IdentityRecord: "id-" + full[:12], RecordDisplayLabel: full[:12], RecordKind: v1alpha1.RecordIdentity}
	if f := identityChecks(ok); len(f) != 0 {
		t.Fatalf("the healthy identity fails: %v", f)
	}
	for name, mutate := range map[string]func(*IdentityObs){
		"not read":              func(o *IdentityObs) { o.Read = false },
		"the job under a name":  func(o *IdentityObs) { o.JobName = "hb-other-000000000000" },
		"the external name":     func(o *IdentityObs) { o.ExternalName = "" },
		"the observed tag":      func(o *IdentityObs) { o.ObservedTag = "x" },
		"a short ID":            func(o *IdentityObs) { o.DeploymentID = full[:12] },
		"the label":             func(o *IdentityObs) { o.DisplayLabel = full[1:13] },
		"the record's name":     func(o *IdentityObs) { o.IdentityRecord = "id-x" },
		"the record missing":    func(o *IdentityObs) { o.RecordDisplayLabel, o.RecordKind = "", "" },
		"the record of another": func(o *IdentityObs) { o.RecordDisplayLabel = full[2:14] },
	} {
		o := ok
		mutate(&o)
		if len(identityChecks(o)) == 0 {
			t.Errorf("%s: the identity passes", name)
		}
	}
}

// The warm start is shown only by the agent's own lines (the ruling 1(b) on #376 and its conditions, relayed with #393's
// reason words): the whole log of the job's first execution, opening with the agent's start line (restartCount 0), its
// first staging the only attempt, its one image line naming the digest, and a file line naming each file the identity
// record lists less what the stager skips (a list from outside the log). The fixtures are written from the constants;
// warmpin_test.go pins those to the agent's and the stager's own.
func TestTheWarmStartIsShownOnlyByTheAgentsOwnLines(t *testing.T) {
	digest := "sha256:" + strings.Repeat("ab", 32)
	record := []agentapi.FileDigest{{Path: "config.json"}, {Path: "model.safetensors"}, {Path: "original/consolidated.pth"}}
	skipLast := func(files []agentapi.FileDigest, _ string) []bool {
		out := make([]bool, len(files))
		out[len(out)-1] = true
		return out
	}
	files, skipped := ExpectedWarmFiles(record, "", skipLast)
	if want := []string{"config.json", "model.safetensors"}; !slices.Equal(files, want) || !slices.Equal(skipped, []string{"original/consolidated.pth"}) {
		t.Fatalf("the expected files are %q, skipped %q; want %q and the last", files, skipped, want)
	}
	pre := `time=2026-11-11T10:00:00Z level=INFO msg="host-dialed: listening for the relays" control=1 serving=2`
	start := func(n int) string {
		return fmt.Sprintf("time=2026-11-11T10:00:01Z level=INFO msg=%q version=x mode=host-dialed cpuArch=x86_64 gpus=1 restartCount=%d", AgentStartMsg, n)
	}
	build := fmt.Sprintf("time=2026-11-11T10:00:02Z level=INFO msg=%q tmpDir=$TMPDIR", AgentBuildMsg)
	failed := fmt.Sprintf("time=2026-11-11T10:00:02Z level=WARN msg=%q uid=u generation=1 err=x", AgentStageFailMsg)
	worker := fmt.Sprintf("time=2026-11-11T10:00:09Z level=INFO msg=%q worker=worker-0 pid=1", AgentWorkerMsg)
	// The stager's own form (internal/agent/stage, PR #393).
	imageLine := func(reason, d string) string { return "stage: image " + d + ": " + reason }
	fileLine := func(f string) string { return "stage: checkpoint file " + f + ": " + StagedPresentMatching }
	shownIn := func(files []string, lines ...string) *WarmUnshownError {
		return warmStartShownByTheAgentsOwnLog(WarmObs{LogRead: true, ImageDigest: digest, Files: files, Log: ParseAgentLog([]byte(strings.Join(lines, "\n") + "\n"))})
	}
	shown := func(lines ...string) *WarmUnshownError { return shownIn(files, lines...) }
	warmLines := []string{imageLine(StagedAlreadyVerified, digest), fileLine("config.json"), fileLine("model.safetensors")}
	plain := append(append([]string{pre, start(0)}, warmLines...), worker)
	if e := shown(plain...); e != nil {
		t.Fatalf("the plain warm log, the skipped file with no line, is not shown: %+v", e)
	}
	if e := shown(start(0), imageLine(StagedByAnother, digest), fileLine("model.safetensors"), fileLine("config.json"), worker); e != nil {
		t.Errorf("an image staged by another Allocation is not shown warm: %+v", e)
	}
	if e := shown(start(0), "stage: image "+strings.TrimPrefix(digest, "sha256:")+": "+StagedAlreadyVerified, fileLine("config.json"), fileLine("model.safetensors"), worker); e != nil {
		t.Errorf("the image named by its hex alone is not shown warm: %+v", e)
	}
	// What comes after the first worker's start is not the first staging's: a rung-2 restage's lines, or a second
	// execution's, do not count, either way.
	if e := shown(append(slices.Clone(plain), imageLine(StagedAlreadyVerified, digest), build, start(1))...); e != nil {
		t.Errorf("lines after the first worker's start changed the verdict: %+v", e)
	}
	for name, c := range map[string]struct {
		lines []string
		files []string // nil for the expected set
		check func(*WarmUnshownError) bool
	}{
		"a cold build, then a retry's warm lines":    {[]string{start(0), build, imageLine(StagedAlreadyVerified, digest), fileLine("config.json"), fileLine("model.safetensors"), worker}, nil, func(e *WarmUnshownError) bool { return e.Retried }},
		"a failed attempt, then a retry's":           {[]string{start(0), failed, imageLine(StagedAlreadyVerified, digest), fileLine("config.json"), fileLine("model.safetensors"), worker}, nil, func(e *WarmUnshownError) bool { return e.Retried }},
		"a silent cancel, then a retry (two images)": {[]string{start(0), imageLine(StagedAlreadyVerified, digest), imageLine(StagedAlreadyVerified, digest), fileLine("config.json"), fileLine("model.safetensors"), worker}, nil, func(e *WarmUnshownError) bool { return e.Retried }},
		"no start line":                            {append([]string{pre}, append(warmLines, worker)...), nil, func(e *WarmUnshownError) bool { return e.NotWhole }},
		"a start line with restartCount 1":         {append([]string{start(1)}, append(warmLines, worker)...), nil, func(e *WarmUnshownError) bool { return e.NotWhole }},
		"a line nobody accounted for before it":    {append([]string{"slurmstepd: something", start(0)}, append(warmLines, worker)...), nil, func(e *WarmUnshownError) bool { return e.NotWhole }},
		"a read that ends before the first worker": {append([]string{start(0)}, warmLines...), nil, func(e *WarmUnshownError) bool { return e.NotWhole }},
		"no image line":                            {[]string{start(0), fileLine("config.json"), fileLine("model.safetensors"), worker}, nil, func(e *WarmUnshownError) bool { return !e.ImageShown && e.FilesShown == 2 }},
		"a checkpoint file missing":                {[]string{start(0), imageLine(StagedAlreadyVerified, digest), fileLine("config.json"), worker}, nil, func(e *WarmUnshownError) bool { return e.ImageShown && e.FilesShown == 1 && e.FilesOf == 2 }},
		"another digest":                           {[]string{start(0), imageLine(StagedAlreadyVerified, "sha256:"+strings.Repeat("cd", 32)), fileLine("config.json"), fileLine("model.safetensors"), worker}, nil, func(e *WarmUnshownError) bool { return !e.ImageShown }},
		"another file":                             {[]string{start(0), imageLine(StagedAlreadyVerified, digest), fileLine("config.json"), fileLine("model.safetensors.index.json"), worker}, nil, func(e *WarmUnshownError) bool { return e.FilesShown == 1 }},
		"a build, not a reuse":                     {[]string{start(0), build, fileLine("config.json"), fileLine("model.safetensors"), worker}, nil, func(e *WarmUnshownError) bool { return e.Retried && !e.ImageShown }},
		// "every" is the record's list less the skipped files, never the files the log mentions: a log that names only
		// config.json matches every file it mentions, and not the list.
		"every file from the log's own mentions": {[]string{start(0), imageLine(StagedAlreadyVerified, digest), fileLine("config.json"), worker}, nil, func(e *WarmUnshownError) bool { return e.FilesOf == 2 && e.FilesShown == 1 }},
		"a record that lists no file":            {plain, []string{}, func(e *WarmUnshownError) bool { return e.FilesOf == 0 }},
	} {
		fs := files
		if c.files != nil {
			fs = c.files
		}
		e := shownIn(fs, c.lines...)
		if e == nil {
			t.Errorf("%s: the warm start is shown", name)
			continue
		}
		if !c.check(e) {
			t.Errorf("%s: %+v, not the reason expected", name, e)
		}
	}
	if e := warmStartShownByTheAgentsOwnLog(WarmObs{ImageDigest: digest, Files: files}); e == nil || e.LogRead {
		t.Errorf("a log that was not read shows the warm start: %+v", e)
	}
	// Without the rule nothing is skipped, so the skipped file's missing line makes the plain log NOT RUN, never a pass.
	if all, _ := ExpectedWarmFiles(record, "", nil); shownIn(all, plain...) == nil {
		t.Error("with no skip rule, a file with no line still passes")
	}
	for _, e := range []*WarmUnshownError{{}, {LogRead: true, NotWhole: true}, {LogRead: true, Retried: true}, {LogRead: true, FilesShown: 1, FilesOf: 2}} {
		if c := e.counts(); regexp.MustCompile(`[/]|sha256|safetensors`).MatchString(c) {
			t.Errorf("the counts %q name a path, a digest or a file", c)
		}
	}
}

// A warm start the agent's log did not show is NOT RUN, never a pass (the ruling 1(b)): through Run, with every other
// check passing, each way of not showing it ends with ErrWarmUnshown after the record ("not run") and the text to post,
// which says NOT RUN with the counts and no name; with a failure beside it, the failure wins.
func TestAWarmStartTheAgentsLogDidNotShowIsNotRun(t *testing.T) {
	row := rowOf("alpha-athena-now-serve")
	for _, fault := range []string{"warm-no-image", "warm-file-missing", "warm-other-digest", "warm-other-file", "warm-unread", "warm-cold-then-retry",
		"warm-failed-then-retry", "warm-cancel-then-retry", "warm-no-start", "warm-restarted", "warm-unknown-first", "warm-truncated"} {
		t.Run(fault, func(t *testing.T) {
			cfg := Config{Row: row, Dry: true, DryFault: fault, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
			rec, err := NewRecorder(cfg.Out)
			if err != nil {
				t.Fatal(err)
			}
			failures, err := Run(context.Background(), cfg, newDrySteps(t, cfg, "nowserve"), rec, t.Logf)
			if !errors.Is(err, ErrWarmUnshown) || errors.Is(err, ErrRequeueRefused) || !NotRun(row, err) || len(failures) != 0 {
				t.Fatalf("failures %v, err %v; want the NOT RUN of a warm start not shown", failures, err)
			}
			posts, _ := os.ReadFile(filepath.Join(cfg.Out, PostsFile))
			if !strings.Contains(string(posts), "alpha-athena-now-serve: NOT RUN") || !strings.Contains(string(posts), warmUnshownWords) || strings.Contains(string(posts), ": PASS") {
				t.Errorf("the text to post does not say NOT RUN for the warm start:\n%s", posts)
			}
			if strings.Contains(string(posts), "safetensors") || strings.Contains(string(posts), "sha256:") {
				t.Errorf("the text to post names a file or a digest:\n%s", posts)
			}
			if note := NotRunNote(row, cfg.PendingMax, err); !strings.Contains(note, warmUnshownWords) {
				t.Errorf("the marker's note %q does not say the warm start was not shown", note)
			}
		})
	}
	t.Run("beside a failure", func(t *testing.T) {
		// requeue-refused-cut has the warm start shown; a cut stream with the image line gone is the failure that wins.
		cfg := Config{Row: row, Dry: true, DryFault: "cut", Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
		rec, err := NewRecorder(cfg.Out)
		if err != nil {
			t.Fatal(err)
		}
		st := newDrySteps(t, cfg, "nowserve")
		o, serr := st.NowServe(context.Background(), Plan{Row: row, Rec: rec, PendingMax: time.Minute, Logf: t.Logf})
		if serr != nil {
			t.Fatal(serr)
		}
		o.Warm.Log = nil
		failures, err := Run(context.Background(), cfg, fixedNowServe{drySteps: st, o: o}, rec, t.Logf)
		if err != nil || len(failures) == 0 || !strings.Contains(Summary(failures), "the long stream was cut") {
			t.Fatalf("failures %v, err %v; want the cut's FAIL to outrank the warm start's NOT RUN", failures, err)
		}
	})
}

// fixedNowServe is the dry steps with row A's scenario replaced by an observation made beforehand.
type fixedNowServe struct {
	*drySteps
	o NowServeObs
}

func (f fixedNowServe) NowServe(context.Context, Plan) (NowServeObs, error) { return f.o, nil }

// sprintf is fmt.Sprintf, for the log collectors here.
func sprintf(format string, a ...any) string { return fmt.Sprintf(format, a...) }
