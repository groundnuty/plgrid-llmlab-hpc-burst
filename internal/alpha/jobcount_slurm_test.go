package alpha

// Row B's live failure of 13 Nov 2026 (alpha-athena-now-nooverlap on the freeze commit of v0.1.0-alpha.1): the
// two-job guard stopped the run at its first job's start, because squeue listed that job's end_time a second past
// its start plus its time limit. Slurm does that. When a job's configuration ends (its prolog, its nodes made ready)
// one or more whole seconds after its start, slurmctld sets end_time to that moment plus the time limit and leaves
// start_time as it was (Slurm 23.11.7, src/slurmctld/job_mgr.c: job_config_fini calls _het_job_time_limit_incr,
// whose _job_time_limit_incr sets end_time = now + time_limit * 60 when now - start_time is not 0). The Provider
// reads a running job's end_time as its lease end (internal/slurm, parse.go, the hard end), and that is right: Slurm
// ends the job there. These tests take a reply of Athena's shape through the real parser into the guard.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Row B's figures: a 21-minute time limit, and a first end 14 minutes after the cluster's clock, so a 7-minute drain
// window (NowNoOverlapLease; the row's log of 13 Nov).
const (
	rowBLimitMinutes = 21
	rowBLimit        = rowBLimitMinutes * time.Minute
	rowBDrainWindow  = 7 * time.Minute
)

// squeueOnly is a login node that answers squeue --me --json with its reply and refuses any other command.
type squeueOnly struct{ reply []byte }

func (s squeueOnly) Run(_ context.Context, cmd slurm.Command) (slurm.Result, error) {
	if cmd.Line != "squeue --me --json" {
		return slurm.Result{ExitCode: 127, Stderr: []byte("not the queue listing")}, nil
	}
	return slurm.Result{Stdout: s.reply}, nil
}

func (squeueOnly) Generation() uint64 { return 1 }

// athenaJob is a running job of the run in an Athena reply: its ID and tag suffix, its start, and the end_time and
// time_limit listed. A nil one is listed unset, as Slurm lists it: set, the number 0. Every value is invented.
type athenaJob struct {
	id     int64
	suffix string
	start  time.Time
	end    *time.Time
	limit  *int64
}

// athenaRunning is squeue --me --json as Athena's Slurm 23.11.7 sends it (data_parser v0.0.40), cut to the keys the
// parser reads as internal/slurm/testdata/v0.0.40-athena-real/squeue-running.json is.
func athenaRunning(t *testing.T, jobs ...athenaJob) []byte {
	t.Helper()
	triple := func(n int64) map[string]any { return map[string]any{"set": true, "infinite": false, "number": n} }
	var out []any
	for _, j := range jobs {
		o := map[string]any{
			"job_id": j.id, "name": "hb-alpha-0a1b2c3d4e-" + j.suffix, "job_state": []string{"RUNNING"}, "state_reason": "None",
			"hold": false, "priority": triple(1000), "restart_cnt": 0, "partition": "example-now",
			"tres_req_str": "cpu=8,mem=64G,node=1,billing=8,gres/gpu=1",
			"submit_time":  triple(j.start.Add(-3 * time.Minute).Unix()), "start_time": triple(j.start.Unix()),
			"end_time": triple(0), "time_limit": triple(0),
		}
		if j.end != nil {
			o["end_time"] = triple(j.end.Unix())
		}
		if j.limit != nil {
			o["time_limit"] = triple(*j.limit)
		}
		out = append(out, o)
	}
	b, err := json.Marshal(map[string]any{
		"meta":   map[string]any{"plugin": map[string]any{"data_parser": "data_parser/v0.0.40"}, "slurm": map[string]any{"release": "23.11.7"}},
		"errors": []any{}, "warnings": []any{}, "jobs": out,
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// listed is the run's jobs as the live look reads them (liveRun.look): the queue listing of a real batch client, each
// job running.
func listed(t *testing.T, reply []byte, n int) []provider.Job {
	t.Helper()
	prof := provider.Profile{Name: "example", Spec: v1alpha1.PlatformProfileSpec{JSONDataParser: "v0.0.40"},
		Partition: v1alpha1.PartitionProfile{Name: "example-now"}}
	b := slurm.NewBatch(squeueOnly{reply}, prof, provider.Settings{Provider: "example", Account: "example-account", Partition: "example-now"}, slurm.Options{})
	js, err := b.Queue(context.Background())
	if err != nil {
		t.Fatalf("the listing: %v", err)
	}
	if len(js) != n {
		t.Fatalf("the listing has %d jobs, want %d", len(js), n)
	}
	for _, j := range js {
		if j.StartTime == nil {
			t.Fatalf("job %s of the listing has no start", j.ID)
		}
	}
	return js
}

func ptrTime(t time.Time) *time.Time        { return &t }
func ptrInt(n int64) *int64                 { return &n }
func ptrDur(d time.Duration) *time.Duration { return &d }

// The reproduction: the run's first job started three minutes after the deployment, and its configuration ended a
// second (or a minute and a half) after its start, so squeue lists its end_time that much past its start plus its time
// limit, with its own time limit the planned one. The guard must take that lease, not stop the run. The end it gave
// before the start is still no later than the job's drain start, and it reckons the job's drain start from its start
// (drainStartOf), the same whether its look came before the configuration ended or after. With a configuration that
// ended in the second it started (the shape of every Athena reply recorded before 13 Nov 2026), it works as before.
func TestTheTwoJobGuardTakesTheLeaseEndSlurmGivesAfterConfiguration(t *testing.T) {
	for _, config := range []time.Duration{0, time.Second, 90 * time.Second, MaxLeaseSlack} {
		g := &TwoJobGuard{TimeLimit: rowBLimit, DrainWindow: rowBDrainWindow}
		first, _, err := g.Next(Look{Clock: guardAt})
		if err != nil {
			t.Fatal(err)
		}
		start := guardAt.Add(3 * time.Minute)
		leaseEnd := start.Add(rowBLimit + config)
		jobs := listed(t, athenaRunning(t, athenaJob{4601, "p", start, &leaseEnd, ptrInt(rowBLimitMinutes)}), 1)
		if !jobs[0].HardEnd.Equal(leaseEnd) {
			t.Fatalf("configuration %s: the parser's lease end is %s, want squeue's end_time %s", config, jobs[0].HardEnd, leaseEnd)
		}
		at := start.Add(2 * time.Second)
		end, final, err := g.Next(Look{Clock: at, Jobs: jobs})
		if err != nil {
			t.Fatalf("configuration %s: the guard stopped the run: %v", config, err)
		}
		earliest := start.Add(rowBLimit - rowBDrainWindow)
		if final || !end.Equal(at.Add(rowBLimit-rowBDrainWindow)) || !g.predDrainStart.Equal(earliest) {
			t.Errorf("configuration %s: Next = %s %v with the predecessor's drain start %s, want %s false with %s",
				config, end, final, g.predDrainStart, at.Add(rowBLimit-rowBDrainWindow), earliest)
		}
		if first.After(earliest) {
			t.Errorf("configuration %s: the first end %s is after the job's earliest drain start %s", config, first, earliest)
		}
	}
}

// The successor's configuration ends a second (or a minute and a half) after its start, and the guard's look reads the
// lease end Slurm moved, while the Provider's last read may still hold the one before it, its start plus the time
// limit: the controller's drain start follows that read (internal/lifecycle, copyLeaseEnd), and it plans a replacement
// for a deployment that ends after it (replacements). So the final end is the successor's start plus the time limit
// less the drain window, at or before the drain start of either lease end, never the drain start of the later one: an
// end there would plan a third job until the Provider read the job again.
func TestTheTwoJobGuardEndsAtTheSuccessorsEarliestDrainStart(t *testing.T) {
	for _, config := range []time.Duration{0, time.Second, 90 * time.Second, MaxLeaseSlack} {
		g := &TwoJobGuard{TimeLimit: rowBLimit, DrainWindow: rowBDrainWindow}
		if _, _, err := g.Next(Look{Clock: guardAt}); err != nil {
			t.Fatal(err)
		}
		pred := guardAt.Add(3 * time.Minute)
		predEnd := pred.Add(rowBLimit + time.Second)
		if _, _, err := g.Next(Look{Clock: pred.Add(2 * time.Second), Jobs: listed(t, athenaRunning(t, athenaJob{4601, "p", pred, &predEnd, ptrInt(rowBLimitMinutes)}), 1)}); err != nil {
			t.Fatal(err)
		}
		// No overlap: the successor starts once the predecessor has left the slot, and the listing shows it alone.
		succ := predEnd.Add(4 * time.Minute)
		succEnd := succ.Add(rowBLimit + config)
		end, final, err := g.Next(Look{Clock: succ.Add(2 * time.Second), Jobs: listed(t, athenaRunning(t, athenaJob{4602, "s", succ, &succEnd, ptrInt(rowBLimitMinutes)}), 1)})
		if err != nil {
			t.Fatalf("configuration %s: the guard stopped the run: %v", config, err)
		}
		before := succ.Add(rowBLimit).Add(-rowBDrainWindow) // the drain start of the lease end before the configuration ended
		after := succEnd.Add(-rowBDrainWindow)              // and of the one after
		if !final || !end.Equal(before) {
			t.Errorf("configuration %s: Next = %s %v, want %s true, the successor's start plus the time limit less the drain window", config, end, final, before)
		}
		for _, ds := range []time.Time{before, after} {
			if end.After(ds) {
				t.Errorf("configuration %s: the final end %s is after the drain start %s, so the controller would plan a third job", config, end, ds)
			}
		}
	}
}

// The guard keeps its protection through the same parser: a lease end before the job's start plus the planned time
// limit (an earlier drain start than the ends the guard gave assume), one more than MaxLeaseSlack after it (the science
// agent's bound on #534; exactly MaxLeaseSlack is taken, above), a listed time limit that is not the planned one, a
// longer lease end with no time limit listed to say why, and no lease end at all each stop the run.
func TestTheTwoJobGuardStillStopsTheRunOnALeaseItDoesNotPlanWith(t *testing.T) {
	start := guardAt.Add(3 * time.Minute)
	for _, c := range []struct {
		name  string
		end   *time.Time
		limit *int64
		want  string
	}{
		{"a lease end a second short of the planned time limit", ptrTime(start.Add(rowBLimit - time.Second)), ptrInt(rowBLimitMinutes), "before its start plus the time limit"},
		{"a lease end a second past the slack the guard takes", ptrTime(start.Add(rowBLimit + MaxLeaseSlack + time.Second)), ptrInt(rowBLimitMinutes), "5m1s past its start plus the time limit 21m0s, more than the 5m0s the guard takes"},
		{"a listed time limit a minute longer than planned", ptrTime(start.Add(rowBLimit + time.Minute)), ptrInt(rowBLimitMinutes + 1), "listed with the time limit 22m0s"},
		{"a listed time limit a minute shorter than planned", ptrTime(start.Add(rowBLimit)), ptrInt(rowBLimitMinutes - 1), "listed with the time limit 20m0s"},
		{"a later lease end and no time limit listed", ptrTime(start.Add(rowBLimit + time.Second)), nil, "no time limit listed"},
		{"no lease end and no time limit", nil, nil, "no lease end"},
	} {
		g := &TwoJobGuard{TimeLimit: rowBLimit, DrainWindow: rowBDrainWindow}
		if _, _, err := g.Next(Look{Clock: guardAt}); err != nil {
			t.Fatal(err)
		}
		_, _, err := g.Next(Look{Clock: start.Add(2 * time.Second), Jobs: listed(t, athenaRunning(t, athenaJob{4601, "p", start, c.end, c.limit}), 1)})
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: Next's error is %v, want one that says %q", c.name, err, c.want)
		}
	}
}

// The slack the row posts: the most any look listed each job's lease end past its start plus the time limit. The
// predecessor's first look lands while it is still configuring (its end_time not moved yet), the next after Slurm moved
// it a second; the successor's look sees it moved a minute and a half. The predecessor's slack is kept once it has left
// the listing. Before any job ran there is none to post.
func TestTheTwoJobGuardKeepsEachJobsMostLeaseSlack(t *testing.T) {
	g := &TwoJobGuard{TimeLimit: rowBLimit, DrainWindow: rowBDrainWindow}
	if _, _, err := g.Next(Look{Clock: guardAt}); err != nil {
		t.Fatal(err)
	}
	if p, s := g.LeaseSlacks(); p != nil || s != nil {
		t.Fatalf("before any job ran, LeaseSlacks = %v, %v, want none", p, s)
	}
	pred := guardAt.Add(3 * time.Minute)
	look := func(at time.Time, j athenaJob) {
		t.Helper()
		if _, _, err := g.Next(Look{Clock: at, Jobs: listed(t, athenaRunning(t, j), 1)}); err != nil {
			t.Fatal(err)
		}
	}
	look(pred.Add(time.Second), athenaJob{4601, "p", pred, ptrTime(pred.Add(rowBLimit)), ptrInt(rowBLimitMinutes)})
	if p, s := g.LeaseSlacks(); p == nil || *p != 0 || s != nil {
		t.Errorf("mid-configuration, LeaseSlacks = %v, %v, want 0s and none", p, s)
	}
	look(pred.Add(3*time.Second), athenaJob{4601, "p", pred, ptrTime(pred.Add(rowBLimit + time.Second)), ptrInt(rowBLimitMinutes)})
	succ := pred.Add(rowBLimit + 5*time.Minute)
	look(succ.Add(2*time.Second), athenaJob{4602, "s", succ, ptrTime(succ.Add(rowBLimit + 90*time.Second)), ptrInt(rowBLimitMinutes)})
	p, s := g.LeaseSlacks()
	if p == nil || *p != time.Second || s == nil || *s != 90*time.Second {
		t.Fatalf("LeaseSlacks = %v, %v, want 1s and 1m30s", p, s)
	}
	if n, m := LeaseSlackSeconds(p), LeaseSlackSeconds(s); *n != 1 || *m != 90 || LeaseSlackSeconds(nil) != nil {
		t.Errorf("LeaseSlackSeconds = %d, %d, want 1 and 90", *n, *m)
	}
}
