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

// athenaRunning is squeue --me --json as Athena's Slurm 23.11.7 sends it (data_parser v0.0.40), cut to the keys the
// parser reads as internal/slurm/testdata/v0.0.40-athena-real/squeue-running.json is: the run's one job, running since
// start, with the end_time and time_limit given. A nil one is listed unset, as Slurm lists it: set, the number 0.
// Every value is invented.
func athenaRunning(t *testing.T, start time.Time, end *time.Time, limitMinutes *int64) []byte {
	t.Helper()
	triple := func(n int64) map[string]any { return map[string]any{"set": true, "infinite": false, "number": n} }
	job := map[string]any{
		"job_id": 4601, "name": "hb-alpha-0a1b2c3d4e-p", "job_state": []string{"RUNNING"}, "state_reason": "None",
		"hold": false, "priority": triple(1000), "restart_cnt": 0, "partition": "example-now",
		"tres_req_str": "cpu=8,mem=64G,node=1,billing=8,gres/gpu=1",
		"submit_time":  triple(start.Add(-3 * time.Minute).Unix()), "start_time": triple(start.Unix()),
		"end_time": triple(0), "time_limit": triple(0),
	}
	if end != nil {
		job["end_time"] = triple(end.Unix())
	}
	if limitMinutes != nil {
		job["time_limit"] = triple(*limitMinutes)
	}
	b, err := json.Marshal(map[string]any{
		"meta":   map[string]any{"plugin": map[string]any{"data_parser": "data_parser/v0.0.40"}, "slurm": map[string]any{"release": "23.11.7"}},
		"errors": []any{}, "warnings": []any{}, "jobs": []any{job},
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// listed is the run's jobs as the live look reads them (liveRun.look): the queue listing of a real batch client.
func listed(t *testing.T, reply []byte) []provider.Job {
	t.Helper()
	prof := provider.Profile{Name: "example", Spec: v1alpha1.PlatformProfileSpec{JSONDataParser: "v0.0.40"},
		Partition: v1alpha1.PartitionProfile{Name: "example-now"}}
	b := slurm.NewBatch(squeueOnly{reply}, prof, provider.Settings{Provider: "example", Account: "example-account", Partition: "example-now"}, slurm.Options{})
	js, err := b.Queue(context.Background())
	if err != nil {
		t.Fatalf("the listing: %v", err)
	}
	if len(js) != 1 || js[0].StartTime == nil {
		t.Fatalf("the listing has %d jobs, want the one running", len(js))
	}
	return js
}

func ptrTime(t time.Time) *time.Time { return &t }
func ptrInt(n int64) *int64          { return &n }

// The reproduction: the run's first job started three minutes after the deployment, and its configuration ended a
// second (or a minute and a half) after its start, so squeue lists its end_time that much past its start plus its time
// limit, with its own time limit the planned one. The guard must take that lease end, not stop the run: the end it
// gave before the start is still no later than the job's drain start, and it plans with Slurm's lease end from then on.
// With a configuration that ended in the second it started (the shape of every Athena reply recorded before 13 Nov
// 2026), it works as before.
func TestTheTwoJobGuardTakesTheLeaseEndSlurmGivesAfterConfiguration(t *testing.T) {
	for _, config := range []time.Duration{0, time.Second, 90 * time.Second} {
		g := &TwoJobGuard{TimeLimit: rowBLimit, DrainWindow: rowBDrainWindow}
		first, _, err := g.Next(Look{Clock: guardAt})
		if err != nil {
			t.Fatal(err)
		}
		start := guardAt.Add(3 * time.Minute)
		leaseEnd := start.Add(rowBLimit + config)
		jobs := listed(t, athenaRunning(t, start, &leaseEnd, ptrInt(rowBLimitMinutes)))
		if !jobs[0].HardEnd.Equal(leaseEnd) {
			t.Fatalf("configuration %s: the parser's lease end is %s, want squeue's end_time %s", config, jobs[0].HardEnd, leaseEnd)
		}
		at := start.Add(2 * time.Second)
		end, final, err := g.Next(Look{Clock: at, Jobs: jobs})
		if err != nil {
			t.Fatalf("configuration %s: the guard stopped the run: %v", config, err)
		}
		drainStart := leaseEnd.Add(-rowBDrainWindow)
		if final || !end.Equal(at.Add(rowBLimit-rowBDrainWindow)) || !g.predDrainStart.Equal(drainStart) {
			t.Errorf("configuration %s: Next = %s %v with the predecessor's drain start %s, want %s false with %s",
				config, end, final, g.predDrainStart, at.Add(rowBLimit-rowBDrainWindow), drainStart)
		}
		if first.After(drainStart) {
			t.Errorf("configuration %s: the first end %s is after the job's drain start %s", config, first, drainStart)
		}
	}
}

// The guard keeps its protection through the same parser: a lease end before the job's start plus the planned time
// limit (an earlier drain start than the ends the guard gave assume), a listed time limit that is not the planned one,
// a longer lease end with no time limit listed to say why, and no lease end at all each stop the run.
func TestTheTwoJobGuardStillStopsTheRunOnALeaseItDoesNotPlanWith(t *testing.T) {
	start := guardAt.Add(3 * time.Minute)
	for _, c := range []struct {
		name  string
		end   *time.Time
		limit *int64
		want  string
	}{
		{"a lease end a second short of the planned time limit", ptrTime(start.Add(rowBLimit - time.Second)), ptrInt(rowBLimitMinutes), "before its start plus the time limit"},
		{"a listed time limit a minute longer than planned", ptrTime(start.Add(rowBLimit + time.Minute)), ptrInt(rowBLimitMinutes + 1), "listed with the time limit 22m0s"},
		{"a listed time limit a minute shorter than planned", ptrTime(start.Add(rowBLimit)), ptrInt(rowBLimitMinutes - 1), "listed with the time limit 20m0s"},
		{"a later lease end and no time limit listed", ptrTime(start.Add(rowBLimit + time.Second)), nil, "no time limit listed"},
		{"no lease end and no time limit", nil, nil, "no lease end"},
	} {
		g := &TwoJobGuard{TimeLimit: rowBLimit, DrainWindow: rowBDrainWindow}
		if _, _, err := g.Next(Look{Clock: guardAt}); err != nil {
			t.Fatal(err)
		}
		_, _, err := g.Next(Look{Clock: start.Add(2 * time.Second), Jobs: listed(t, athenaRunning(t, start, c.end, c.limit))})
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: Next's error is %v, want one that says %q", c.name, err, c.want)
		}
	}
}
