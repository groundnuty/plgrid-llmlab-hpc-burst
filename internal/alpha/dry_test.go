package alpha

// The stand-in steps of the dry run: a scenario in place of the stack, over a
// fake Slurm in run mode in place of a cluster. Each job is a real local process
// that waits for a release file, so the order of events is the test's, never a
// sleep's; the clock is a manual one, so a one-hour run takes seconds and every
// time is exact. What the stand-in reports (the Instances, the streams) is
// derived from the fake cluster's own job times where a cluster has them. The
// judging path (Run, the verdicts, the recorder, the posts) is the live row's,
// untouched.
//
// A fault names one way the live run could go wrong; every fault must make the
// row fail with its label, or the dry run would pass with its checks gone.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const (
	dryOtherPartition = "dry-other-partition"
	dryWait           = 8 * time.Second
)

// dryFaults lists every fault the stand-in knows, per row family. An unknown
// name stops the run: a typo must not pass as "no fault".
var dryFaults = map[string][]string{
	"handover":  {"cut", "no-stream", "two-restarts", "no-restart", "no-handover", "late-successor", "wrong-partition", "untagged", "unattributed"},
	"nooverlap": {"nocap", "nogap", "noslot", "cut", "no-stream", "late-submit", "untagged", "wrong-partition"},
	"tunnel":    {"short", "cut", "drops", "no-rate", "short-rate", "wrong-direction", "untagged", "wrong-partition"},
}

type drySteps struct {
	t     *testing.T
	cfg   Config
	clock *fake.Clock
	plat  *fakeslurm.Platform
	sess  *fakeslurm.Session
	batch *slurm.Batch
	root  string
	jobs  int
	// tags in the order they were made, for the observation.
	tags []string
	// untaggedAt is the 1-based position of the tag the fault "untagged" does
	// not write down: the successor's, or the only one of a one-job run.
	untaggedAt int
}

func newDrySteps(t *testing.T, cfg Config, family string) *drySteps {
	t.Helper()
	known := false
	for _, f := range append([]string{""}, dryFaults[family]...) {
		known = known || f == cfg.DryFault
	}
	if !known {
		t.Fatalf("the dry fault %q is not one of %v", cfg.DryFault, dryFaults[family])
	}
	root := t.TempDir()
	home, scratch := filepath.Join(root, "home"), filepath.Join(root, "scratch")
	for _, d := range []string{home, scratch} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	clock := fake.NewClock(time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC))
	fcfg := fakeslurm.Config{
		Run:        &fakeslurm.RunMode{Root: root},
		Partitions: []fakeslurm.Partition{{Name: cfg.Row.Partition}, {Name: dryOtherPartition}},
	}
	if cfg.Row.QOS != "" {
		// The cap that makes no-overlap what it is (HBEP-7 Test plan: "A fixture
		// without a GPU cap proves nothing here").
		q := fakeslurm.QOS{Name: cfg.Row.QOS, MaxGPUsPerUser: fakeslurm.Cap(1)}
		if cfg.DryFault == "nocap" {
			q.MaxGPUsPerUser = fakeslurm.Cap(8)
		}
		fcfg.QOS = []fakeslurm.QOS{q}
	}
	plat := fakeslurm.New(clock, fcfg)
	d := &drySteps{t: t, cfg: cfg, clock: clock, plat: plat, root: root, untaggedAt: 2}
	if family == "tunnel" {
		d.untaggedAt = 1
	}
	d.sess = plat.Login(fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": home, "SCRATCH": scratch}})
	prof := provider.Profile{Name: "dry", Spec: v1alpha1.PlatformProfileSpec{JSONDataParser: "v0.0.42", StagingDir: scratch},
		Partition: v1alpha1.PartitionProfile{Name: cfg.Row.Partition}}
	set := provider.Settings{Provider: "dry", Account: "example-account", Partition: cfg.Row.Partition}
	// A listing is never older than a read: the tests move the clock and the jobs, not the cache.
	d.batch = slurm.NewBatch(d.sess, prof, set, slurm.Options{Clock: &tickClock{c: clock}, PollInterval: time.Nanosecond, BusyInterval: time.Nanosecond})
	t.Cleanup(func() {
		// End whatever is left, then close: a Platform in run mode must be closed.
		for i := 1; i <= d.jobs; i++ {
			_ = os.WriteFile(d.gate(i), nil, 0o600)
		}
		if err := plat.Close(); err != nil {
			t.Errorf("the fake cluster did not close cleanly: %v", err)
		}
	})
	return d
}

func (d *drySteps) gate(n int) string      { return filepath.Join(d.root, fmt.Sprintf("release-%d", n)) }
func (d *drySteps) now() time.Time         { return d.clock.Now() }
func (d *drySteps) tick(x time.Duration)   { d.clock.Advance(x) }
func (d *drySteps) fault(name string) bool { return d.cfg.DryFault == name }

// tag makes an Allocation's job tag the way the controller does and, unless
// the fault says otherwise, writes it down first.
func (d *drySteps) tag(p Plan, name string) string {
	tg := v1alpha1.JobTag("dry", name, types.UID("00000000-0000-4000-8000-"+fmt.Sprintf("%012d", len(d.tags)+1)))
	d.tags = append(d.tags, tg)
	if !d.fault("untagged") || len(d.tags) != d.untaggedAt {
		if err := p.Rec.Tag(tg); err != nil {
			d.t.Fatal(err)
		}
	}
	return tg
}

// submit sends a job that waits for its release file, as the Provider's
// controller would send an Allocation's job, and returns its sequence number and ID.
func (d *drySteps) submit(tag, partition, qos string) (n int, id string) {
	d.t.Helper()
	d.jobs++
	n = d.jobs
	wrap := fmt.Sprintf("while [ ! -e %s ]; do sleep 0.02; done", d.gate(n))
	line := fmt.Sprintf("sbatch --parsable --no-requeue --job-name=%s --partition=%s --account=example-account --time=00:30:00 --gres=gpu:1 --cpus-per-task=1 --mem=1024 --output=%s --error=%s", tag, partition, d.out(n), d.out(n))
	if qos != "" {
		line += " --qos=" + qos
	}
	line += " '--wrap=" + wrap + "'"
	res, err := d.sess.Run(context.Background(), slurm.Command{Line: line})
	if err != nil || res.ExitCode != 0 {
		d.t.Fatalf("sbatch %q: %v exit %d %s", tag, err, res.ExitCode, res.Stderr)
	}
	id = strings.TrimSpace(strings.SplitN(string(res.Stdout), ";", 2)[0])
	if _, err := strconv.Atoi(id); err != nil {
		d.t.Fatalf("sbatch printed %q", res.Stdout)
	}
	return n, id
}

func (d *drySteps) waitFor(what string, cond func(provider.Job) bool, id string) provider.Job {
	d.t.Helper()
	deadline := time.Now().Add(dryWait)
	for {
		j, found, err := d.batch.Observe(context.Background(), id)
		if err == nil && found && cond(j) {
			return j
		}
		if time.Now().After(deadline) {
			out, _ := os.ReadFile(d.out(1))
			d.t.Fatalf("timed out after %s waiting for %s (last read: found %t, err %v, state %v, phase %s, source %v; job 1 output %q)", dryWait, what, found, err, j.State, j.Phase, j.Source, out)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (d *drySteps) running(id string) provider.Job {
	return d.waitFor("a job to run", func(j provider.Job) bool { return j.StartTime != nil && j.EndTime == nil }, id)
}

// end releases a job's process and waits until the cluster says it ended.
func (d *drySteps) end(n int, id string) provider.Job {
	if err := os.WriteFile(d.gate(n), nil, 0o600); err != nil {
		d.t.Fatal(err)
	}
	return d.waitFor("a job to end", func(j provider.Job) bool { return j.EndTime != nil }, id)
}

func (d *drySteps) pending(id string) {
	d.waitFor("a job to pend", func(j provider.Job) bool { return j.StartTime == nil && j.EndTime == nil }, id)
}

func (d *drySteps) Ledger() Ledger              { return dryLedger{d} }
func (d *drySteps) Close(context.Context) error { return nil }

type dryLedger struct{ d *drySteps }

func (l dryLedger) Jobs(ctx context.Context, tags []string) ([]provider.Job, error) {
	var out []provider.Job
	since := l.d.now().Add(-100 * 24 * time.Hour)
	for _, tg := range tags {
		js, err := l.d.batch.FindByTag(ctx, tg, since)
		if err != nil {
			return nil, err
		}
		out = append(out, js...)
	}
	return out, nil
}

func (d *drySteps) partitionFor(second bool) string {
	if second && d.fault("wrong-partition") {
		return dryOtherPartition
	}
	return d.cfg.Row.Partition
}

func (d *drySteps) Handover(_ context.Context, p Plan) (HandoverObs, error) {
	t := d.t
	tag0 := d.tag(p, "pre")
	n0, id0 := d.submit(tag0, d.partitionFor(false), "")
	j0 := d.running(id0)
	opened := *j0.StartTime
	d.tick(10 * time.Minute)
	var tag1, id1 string
	var n1 int
	var ready, ended0 time.Time
	if d.fault("late-successor") {
		// Break before make: the predecessor ends, then the successor is made.
		d.tick(5 * time.Minute)
		j := d.end(n0, id0)
		ended0 = *j.EndTime
		d.tick(time.Minute)
		tag1 = d.tag(p, "suc")
		n1, id1 = d.submit(tag1, d.partitionFor(true), "")
		ready = *d.running(id1).StartTime
		_ = n1
	} else {
		tag1 = d.tag(p, "suc")
		n1, id1 = d.submit(tag1, d.partitionFor(true), "")
		j1 := d.running(id1)
		ready = *j1.StartTime
		_ = n1
	}
	drain := d.now().Add(time.Minute)
	d.tick(2 * time.Minute)
	streamEnd := d.now()
	if !d.fault("late-successor") {
		d.tick(time.Minute)
		j := d.end(n0, id0)
		ended0 = *j.EndTime
	}
	streams := []Stream{{ID: "s1", Opened: opened, Ended: streamEnd, Complete: true}, {ID: "s2", Opened: opened.Add(time.Minute), Ended: streamEnd, Complete: true}}
	if d.fault("no-stream") {
		streams = []Stream{{ID: "s1", Opened: opened, Ended: drain.Add(-time.Second), Complete: true}}
	}
	if d.fault("cut") {
		streams[0].Complete, streams[0].CutBy = false, "tunnel"
	}
	obs := HandoverObs{Tags: append([]string(nil), d.tags...), Streams: streams, DrainStart: drain, Handovers: 1, WorkerRestarts: 1, Responses: 20, Attributed: 20,
		Instances: []InstanceSpan{{Tag: tag0, ReadyAt: opened, EndedAt: ended0}, {Tag: tag1, ReadyAt: ready}}}
	switch {
	case d.fault("two-restarts"):
		obs.WorkerRestarts = 2
	case d.fault("no-restart"):
		obs.WorkerRestarts = 0
	case d.fault("no-handover"):
		obs.Handovers = 0
	case d.fault("unattributed"):
		obs.Attributed = 19
	}
	t.Logf("stand-in handover: %d streams, %d Allocations", len(streams), len(d.tags))
	return obs, nil
}

func (d *drySteps) NoOverlap(_ context.Context, p Plan) (NoOverlapObs, error) {
	tag0 := d.tag(p, "pre")
	n0, id0 := d.submit(tag0, d.partitionFor(false), d.cfg.Row.QOS)
	j0 := d.running(id0)
	opened := *j0.StartTime
	var id1 string
	submitSuccessor := func() {
		tag1 := d.tag(p, "suc")
		_, id1 = d.submit(tag1, d.partitionFor(true), d.cfg.Row.QOS)
	}
	if !d.fault("late-submit") {
		// Queued ahead: accepted at once, it pends on the QOS's GPU cap while the predecessor runs.
		d.tick(time.Minute)
		submitSuccessor()
		if !d.fault("nocap") {
			d.pending(id1)
		}
	}
	d.tick(5 * time.Minute)
	drain := d.now()
	d.tick(30 * time.Second)
	streamEnd := d.now()
	d.tick(10 * time.Second)
	if d.fault("nocap") {
		d.running(id1)
	}
	j0e := d.end(n0, id0)
	_ = j0e
	if d.fault("late-submit") {
		d.tick(time.Minute)
		submitSuccessor()
	}
	j1 := d.running(id1)
	d.tick(20 * time.Second)
	ready := d.now()
	streams := []Stream{{ID: "s1", Opened: opened, Ended: streamEnd, Complete: true}}
	if d.fault("no-stream") {
		streams = []Stream{{ID: "s1", Opened: opened, Ended: drain.Add(-time.Second), Complete: true}}
	}
	if d.fault("cut") {
		streams[0].Complete, streams[0].CutBy = false, "tunnel"
	}
	_ = j1
	obs := NoOverlapObs{Tags: append([]string(nil), d.tags...), Streams: streams, DrainStart: drain, SuccessorReady: ready, NoOverlapSlotSeen: true, NoReadyTarget: 3}
	switch {
	case d.fault("nogap"):
		obs.SuccessorReady = drain.Add(-time.Second)
	case d.fault("noslot"):
		obs.NoOverlapSlotSeen = false
	}
	return obs, nil
}

func (d *drySteps) TunnelHour(_ context.Context, p Plan) (TunnelHourObs, error) {
	tag := d.tag(p, "tunnel")
	// The run's only job is the one the wrong-partition fault sends elsewhere.
	n, id := d.submit(tag, d.partitionFor(true), d.cfg.Row.QOS)
	j := d.running(id)
	opened := *j.StartTime
	held := 61 * time.Minute
	if d.fault("short") {
		held = 59 * time.Minute
	}
	d.tick(held)
	ended := d.now()
	d.end(n, id)
	lane := Stream{ID: "lane1", Opened: opened, Ended: ended, Complete: true}
	if d.fault("cut") {
		lane.Complete, lane.CutBy = false, "tunnel"
	}
	obs := TunnelHourObs{Tags: append([]string(nil), d.tags...), Lanes: []Stream{lane, {ID: "lane2", Opened: opened, Ended: ended, Complete: true}},
		Rate: PathRate{Direction: "up", OK: true, AskedSeconds: 30, TimedSeconds: 30.2, Bytes: 31 << 20}}
	switch {
	case d.fault("drops"):
		obs.TunnelDrops = 1
	case d.fault("no-rate"):
		obs.Rate.OK = false
	case d.fault("short-rate"):
		obs.Rate.TimedSeconds = 20
	case d.fault("wrong-direction"):
		obs.Rate.Direction = "down"
	}
	return obs, nil
}

func (d *drySteps) out(n int) string { return filepath.Join(d.root, fmt.Sprintf("job-%d.out", n)) }

// tickClock is the manual clock plus one nanosecond for every reading, so that
// the Batch's cached listing is always due: the manual clock alone never moves
// between two reads, and the Batch would serve the first listing for ever.
type tickClock struct {
	c *fake.Clock
	n atomic.Int64
}

func (t *tickClock) Now() time.Time { return t.c.Now().Add(time.Duration(t.n.Add(1))) }
