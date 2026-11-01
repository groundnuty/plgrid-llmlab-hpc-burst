package fakeslurm

// New. Slurm's COMPLETING flag in run mode (science-agent ruling, 1 Nov 2026,
// on #153): squeue lists ["CANCELLED", "COMPLETING"] (state code CG) while a
// job whose end state is set still has processes, likewise for TIMEOUT and
// FAILED, and drops the flag once they are gone. Each wait is on the job's
// own state (a file it writes, its snapshot), never a sleep.

import (
	"os"
	"strings"
	"testing"
	"time"
)

// textState is the ST column of job id in squeue's text table.
func (r *runRig) textState(id int) string {
	r.t.Helper()
	for _, l := range strings.Split(r.must("squeue --me"), "\n") {
		f := strings.Fields(l)
		if len(f) > 4 && f[0] == itoa(id) {
			return f[4]
		}
	}
	r.t.Fatalf("squeue --me does not list job %d", id)
	return ""
}

func TestRunModeEndedJobCompletesUntilItsProcessesAreGone(t *testing.T) {
	for _, c := range []struct {
		name, state, code string
		end               func(r *runRig, id int)
	}{
		{"scancel", Cancelled, "CA", func(r *runRig, id int) { r.must("scancel " + itoa(id)) }},
		{"time limit", Timeout, "TO", func(r *runRig, id int) {
			if err := r.p.SetTimeLimit(id, 1); err != nil {
				r.t.Fatal(err)
			}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			// The job ignores TERM, and KillWait is far off, so after its end
			// state is set its processes live until it is told to exit.
			r := newRunRig(t, RunMode{KillWait: 10 * time.Minute, Minute: 50 * time.Millisecond})
			up, goFile := r.home+"/up", r.home+"/go"
			id := r.submit("trap \"\" TERM; echo up > "+up+"; "+waitLoop(goFile), "--time=6000")
			r.waitFile(up)
			c.end(r, id)
			waitFor(t, "job "+itoa(id)+" to be "+c.state, func() bool { return r.job(id).State == c.state })
			if got := r.squeueJob(id)["job_state"]; !equalStrings(got, c.state, Completing) {
				t.Errorf("squeue job_state %v while the processes live, want [%s COMPLETING]", got, c.state)
			}
			if got := r.textState(id); got != "CG" {
				t.Errorf("squeue's ST %q while the processes live, want CG", got)
			}
			if err := os.WriteFile(goFile, nil, 0o644); err != nil {
				t.Fatal(err)
			}
			r.waitFinished(id)
			if got := r.squeueJob(id)["job_state"]; !equalStrings(got, c.state) {
				t.Errorf("squeue job_state %v once the processes are gone, want [%s]", got, c.state)
			}
			if got := r.textState(id); got != c.code {
				t.Errorf("squeue's ST %q once the processes are gone, want %s", got, c.code)
			}
		})
	}
}

// jobState for each end state, with processes live, gone, and never started;
// a running job has no flag.
func TestJobStateCarriesCompletingOnlyWhileProcessesLive(t *testing.T) {
	for _, state := range []string{Cancelled, Timeout, Failed} {
		for _, c := range []struct {
			name string
			run  *jobRun
			want []string
		}{
			{"live", &jobRun{}, []string{state, Completing}},
			{"gone", &jobRun{done: true}, []string{state}},
			{"never started", nil, []string{state}},
		} {
			j := &job{execs: []*execution{{state: state, run: c.run}}}
			if got := jobState(j); strings.Join(got, ",") != strings.Join(c.want, ",") {
				t.Errorf("%s, processes %s: job_state %v, want %v", state, c.name, got, c.want)
			}
		}
	}
	j := &job{execs: []*execution{{state: Running, run: &jobRun{}}}}
	if got := jobState(j); strings.Join(got, ",") != Running {
		t.Errorf("a running job: job_state %v, want [RUNNING]", got)
	}
}
