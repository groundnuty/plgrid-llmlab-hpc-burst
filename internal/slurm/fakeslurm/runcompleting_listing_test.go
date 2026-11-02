package fakeslurm

// New. R5 of PR #161's review: a completing job (an end state while its
// processes live, run mode) stays listed by squeue and is not purged until its
// processes are gone, in both NotListed modes and however long ago it ended.
// The rehearsal runs the Helios mode, which hides every other ended job.
//
// Expected values are Slurm's: squeue reports pending, running and completing
// jobs by default (squeue(1), --states); the purge skips a completing job and
// ages a finished one from its end_time (_list_find_job_old, job_mgr.c, 24.11);
// scancel gets ESLURM_ALREADY_DONE, which real scancel does not print: it
// exits 0 with nothing on stderr (HBEP-4, a completing job; #207), and
// scontrol release gets ESLURM_JOB_FINISHED ("Job has already finished"), as
// for a completed one (slurm_errno.c). The clock is
// manual, so MinJobAge passes only when a test says so; every wait is on the
// job's own state, never a sleep.

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// completingMinAge is the MinJobAge of these tests, S7's 10 s.
const completingMinAge = 10 * time.Second

// endWay is one way to end a job whose processes ignore TERM.
type endWay struct {
	name, state, code string
	end               func(r *runRig, id int)
}

var endWays = []endWay{
	{"scancel", Cancelled, "CA", func(r *runRig, id int) { r.must("scancel " + itoa(id)) }},
	{"time limit", Timeout, "TO", func(r *runRig, id int) {
		if err := r.p.SetTimeLimit(id, 1); err != nil {
			r.t.Fatal(err)
		}
	}},
}

// newCompletingRig is a run-mode platform in the given NotListed mode whose
// KillWait is far off, so a job that ignores TERM keeps its processes until
// the test releases them.
func newCompletingRig(t *testing.T, mode NotListedMode) *runRig {
	t.Helper()
	return newRunRigCfg(t, Config{
		Run:       &RunMode{KillWait: 10 * time.Minute, Minute: 50 * time.Millisecond},
		NotListed: mode, MinJobAge: completingMinAge,
	}, true)
}

// in is the rig for a subtest: the same platform and clock, with t the
// subtest's, so that what the helpers report fails that subtest and not the
// test that made the rig.
func (r *runRig) in(t *testing.T) *runRig {
	c, base := *r, *r.rig
	base.t = t
	c.rig = &base
	return &c
}

// endStubborn submits a job that ignores TERM and waits for a file, ends it
// by way and returns once its state is the end state and the fake has sent the
// TERM. The job is then completing: release lets its processes go and waits
// until they are gone.
func (r *runRig) endStubborn(way endWay) (id int, release func()) {
	r.t.Helper()
	up, goFile := r.home+"/up", r.home+"/go"
	id = r.submit("trap \"\" TERM; echo up > "+up+"; "+waitLoop(goFile), "--time=6000")
	r.waitFile(up)
	way.end(r, id)
	waitFor(r.t, "job "+itoa(id)+" to be "+way.state, func() bool { return r.job(id).State == way.state })
	waitFor(r.t, "the TERM to the processes of job "+itoa(id), func() bool { return !r.job(id).TermAt.IsZero() })
	return id, func() {
		r.t.Helper()
		if err := os.WriteFile(goFile, nil, 0o644); err != nil {
			r.t.Fatal(err)
		}
		r.waitFinished(id)
	}
}

// squeueForm is one way to ask squeue about a job.
type squeueForm struct {
	line       string
	byID, text bool
}

// squeueForms are the listings of one job: the --me listing and --jobs=<id>,
// without --me and with it, as JSON, and the text table of both.
func squeueForms(id int) []squeueForm {
	n := strconv.Itoa(id)
	return []squeueForm{
		{"squeue --me --json", false, false},
		{"squeue --json --jobs=" + n, true, false},
		{"squeue --me --json --jobs=" + n, true, false},
		{"squeue --me", false, true},
		{"squeue --jobs=" + n, true, true},
	}
}

// listedRows are the jobs a squeue reply lists, each as "<id>:<state>", the
// state being job_state joined by "+" for JSON and the ST column for text.
func (r *runRig) listedRows(f squeueForm, stdout string) []string {
	r.t.Helper()
	var rows []string
	if !f.text {
		for _, j := range jobsOf(r.t, decode(r.t, stdout)) {
			var states []string
			for _, s := range j["job_state"].([]any) {
				states = append(states, s.(string))
			}
			rows = append(rows, fmt.Sprintf("%d:%s", int(j["job_id"].(float64)), strings.Join(states, "+")))
		}
		return rows
	}
	for _, l := range strings.Split(stdout, "\n") {
		if fs := strings.Fields(l); len(fs) > 4 {
			if _, err := strconv.Atoi(fs[0]); err == nil {
				rows = append(rows, fs[0]+":"+fs[4])
			}
		}
	}
	return rows
}

// wantSqueue checks every form's answer about job id. kind listed expects the
// job with job_state state (JSON) and ST code (text); emptyList exit 0 and no
// job; invalidID the error from the forms that name the job and no job from
// the --me listing.
func (r *runRig) wantSqueue(what string, id int, kind listKind, state []string, code string) {
	r.t.Helper()
	for _, f := range squeueForms(id) {
		stdout, stderr, exit := r.run(f.line)
		wantExit, wantErr := 0, ""
		if kind == invalidID && f.byID {
			wantExit, wantErr = 1, invalidJob+"\n"
		}
		if exit != wantExit || stderr != wantErr {
			r.t.Errorf("%s: %q: exit %d stderr %q, want exit %d stderr %q", what, f.line, exit, stderr, wantExit, wantErr)
			continue
		}
		if exit != 0 {
			continue
		}
		var want []string
		if kind == listed {
			st := strings.Join(state, "+")
			if f.text {
				st = code
			}
			want = []string{fmt.Sprintf("%d:%s", id, st)}
		}
		if got := r.listedRows(f, stdout); strings.Join(got, ",") != strings.Join(want, ",") {
			r.t.Errorf("%s: %q lists %v, want %v", what, f.line, got, want)
		}
	}
}

// wantFinishedAnswers checks that scancel and scontrol release answer for the
// completing job as they do for a finished one, and change nothing.
func (r *runRig) wantFinishedAnswers(what string, id int, state string) {
	r.t.Helper()
	before := r.job(id)
	_, stderr, exit := r.run("scancel " + itoa(id))
	if exit != 0 || stderr != "" {
		r.t.Errorf("%s: scancel: exit %d stderr %q, want exit 0 and nothing printed, as real scancel answers a completing job (#207)", what, exit, stderr)
	}
	_, stderr, exit = r.run("scontrol release " + itoa(id))
	want := "scontrol: error: Job has already finished\n"
	if exit != 1 || stderr != want {
		r.t.Errorf("%s: scontrol release: exit %d stderr %q, want exit 1 stderr %q", what, exit, stderr, want)
	}
	after := r.job(id)
	if after.State != state || !after.TermAt.Equal(before.TermAt) || !after.KillAt.IsZero() || after.Finished {
		r.t.Errorf("%s: after scancel and scontrol: state %s, TermAt %v (was %v), KillAt %v, Finished %v; want %s, TERM sent once, no KILL, processes alive",
			what, after.State, after.TermAt, before.TermAt, after.KillAt, after.Finished, state)
	}
}

// A completing job is listed with ["<end state>", "COMPLETING"] (ST CG) in
// every form, inside MinJobAge and long past it, in both modes, and scancel and
// scontrol answer as for a finished job rather than "Invalid job id". Once its
// processes are gone, long after its end, it is purged at once.
func TestCompletingJobStaysListedAndUnpurgedPastMinJobAge(t *testing.T) {
	for _, mode := range []NotListedMode{NotListedHelios, NotListedAthena} {
		for _, way := range endWays {
			t.Run(string(mode)+"/"+way.name, func(t *testing.T) {
				r := newCompletingRig(t, mode)
				id, release := r.endStubborn(way)
				completing := []string{way.state, Completing}
				t.Run("inside MinJobAge", func(t *testing.T) {
					r := r.in(t)
					r.clock.Advance(completingMinAge - time.Second)
					r.wantSqueue("completing, end + 9 s", id, listed, completing, "CG")
				})
				t.Run("past MinJobAge", func(t *testing.T) {
					r := r.in(t)
					r.clock.Advance(time.Second + 10*time.Minute)
					r.wantSqueue("completing, end + 10 min", id, listed, completing, "CG")
					r.wantFinishedAnswers("completing, end + 10 min", id, way.state)
				})
				release()
				t.Run("processes gone after MinJobAge", func(t *testing.T) {
					// MinJobAge counts from the end, as in Slurm, so a job
					// whose processes outlived it is purged when they go.
					r.in(t).wantSqueue("processes gone, end + 10 min", id, invalidID, nil, "")
				})
			})
		}
	}
}

// A job whose processes are gone before end + MinJobAge follows the mode's
// rules from then on: Helios drops it from --me and answers an empty list for
// it by --jobs, Athena lists it in its end state without the flag; both give
// the error from end + MinJobAge, counted from the end.
func TestCompletingJobEndedInsideMinJobAgeFollowsTheMode(t *testing.T) {
	for _, mode := range []NotListedMode{NotListedHelios, NotListedAthena} {
		for _, way := range endWays {
			t.Run(string(mode)+"/"+way.name, func(t *testing.T) {
				r := newCompletingRig(t, mode)
				id, release := r.endStubborn(way)
				r.clock.Advance(completingMinAge - time.Second)
				r.wantSqueue("completing, end + 9 s", id, listed, []string{way.state, Completing}, "CG")
				release()
				kind := emptyList
				if mode == NotListedAthena {
					kind = listed
				}
				r.wantSqueue("processes gone, end + 9 s", id, kind, []string{way.state}, way.code)
				r.clock.Advance(time.Second)
				r.wantSqueue("processes gone, end + 10 s", id, invalidID, nil, "")
			})
		}
	}
}
