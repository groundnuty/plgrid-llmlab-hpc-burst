package e2e

// Barrier and fake-state helpers. They use only the harness helpers that
// already existed at 9b3d066, so races_test.go can also run against that
// commit's provider to show the old logic fails (README, "Evidence against
// the old logic").

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// gate is FAKE_SLURM_SBATCH_GATE: sbatch blocks before registering a job
// until the file exists.
type gate struct{ path string }

func newGate(f *fakeSlurm) gate { return gate{path: filepath.Join(f.dir, "sbatch.gate")} }

func (g gate) env() string { return "FAKE_SLURM_SBATCH_GATE=" + g.path }

func (g gate) open(t *testing.T) {
	t.Helper()
	if err := os.WriteFile(g.path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
}

// events returns the fake's log entries of one kind.
func (f *fakeSlurm) events(t *testing.T, event string) []logEntry {
	var out []logEntry
	for _, e := range f.entries(t) {
		if e.Event == event {
			out = append(out, e)
		}
	}
	return out
}

// waitEvents waits until the fake has logged at least n events of a kind.
func (f *fakeSlurm) waitEvents(t *testing.T, event string, n int, d time.Duration) []logEntry {
	t.Helper()
	eventually(t, d, func() (bool, string) {
		got := len(f.events(t, event))
		return got >= n, fmt.Sprintf("%d %s events, want %d", got, event, n)
	})
	return f.events(t, event)
}

// releasedIDs are the distinct job IDs a provider released with exit 0. A
// job that is never released (or started by the test) never runs.
func (f *fakeSlurm) releasedIDs(t *testing.T) []int64 {
	seen := map[int64]bool{}
	var out []int64
	for _, e := range f.events(t, "scontrol_release") {
		if e.Exit == 0 && !seen[e.JobID] {
			seen[e.JobID] = true
			out = append(out, e.JobID)
		}
	}
	return out
}

// fakeJob is one job as `fakeslurmctl dump` prints it.
type fakeJob struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Held  bool   `json:"held"`
	Execs []struct {
		Start       time.Time `json:"start"`
		Transitions []struct {
			State string `json:"state"`
		} `json:"transitions"`
	} `json:"execs"`
}

// state is the job's latest state; started reports whether any execution
// ever started.
func (j fakeJob) state() string {
	e := j.Execs[len(j.Execs)-1]
	return e.Transitions[len(e.Transitions)-1].State
}

func (j fakeJob) started() bool {
	for _, e := range j.Execs {
		if !e.Start.IsZero() {
			return true
		}
	}
	return false
}

// jobs dumps the fake's jobs carrying a name ("" for all).
func (f *fakeSlurm) jobs(t *testing.T, name string) []fakeJob {
	t.Helper()
	var st struct {
		Jobs []fakeJob `json:"jobs"`
	}
	if err := json.Unmarshal([]byte(f.ctl(t, "dump")), &st); err != nil {
		t.Fatalf("dump: %v", err)
	}
	var out []fakeJob
	for _, j := range st.Jobs {
		if name == "" || j.Name == name {
			out = append(out, j)
		}
	}
	return out
}

// liveJobs are the jobs with a name that are PENDING or RUNNING in the fake.
func (f *fakeSlurm) liveJobs(t *testing.T, name string) []fakeJob {
	var out []fakeJob
	for _, j := range f.jobs(t, name) {
		if s := j.state(); s == "PENDING" || s == "RUNNING" {
			out = append(out, j)
		}
	}
	return out
}

// submitAs runs sbatch directly against the fake, as someone other than the
// provider (a duplicate from outside the protocol, or a pre-existing job).
func (f *fakeSlurm) submitAs(t *testing.T, caller string, args ...string) int64 {
	t.Helper()
	cmd := exec.Command(filepath.Join(h.slurmBin, "sbatch"), append([]string{"--parsable"}, args...)...)
	cmd.Env = f.cmdEnv(caller)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("sbatch %v: %v", args, err)
	}
	var id int64
	if _, err := fmt.Sscanf(string(out), "%d", &id); err != nil {
		t.Fatalf("sbatch output %q: %v", out, err)
	}
	return id
}

// k8sEvents lists the Kubernetes events with a reason in a namespace.
func k8sEvents(t *testing.T, ns, reason string) []corev1.Event {
	t.Helper()
	var l corev1.EventList
	if err := h.k8s.List(context.Background(), &l, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	var out []corev1.Event
	for _, e := range l.Items {
		if e.Reason == reason {
			out = append(out, e)
		}
	}
	return out
}

// callRate is the number of calls of one command by one caller per second
// over [from, to].
func (f *fakeSlurm) callRate(t *testing.T, cmd, caller string, from, to time.Time) float64 {
	n := 0
	for _, e := range f.entries(t) {
		if e.Event == cmd && e.Caller == caller && !e.TS.Before(from) && !e.TS.After(to) {
			n++
		}
	}
	return float64(n) / to.Sub(from).Seconds()
}
