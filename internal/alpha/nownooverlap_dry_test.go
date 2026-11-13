package alpha

// Row B over the fake cluster (issue #376; nownooverlap.go): the dry steps' NowNoOverlap, on the manual clock. What the
// stack would show is stood in for; the jobs are the fake's, so the one-slot order in the cluster's record is the
// fake's own times. Each fault departs from the healthy run in one way that must fail the row, or end it NOT RUN.

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// nowNoOverlapFaults are row B's dry faults (dryFaults["nownooverlap"]).
var nowNoOverlapFaults = []string{"cut", "no-stream", "noslot", "nogap", "early-submit", "created-early", "order-unseen", "marked",
	"not-ended", "not-ready", "successor-not-ready", "refused-no-owner", "no-pred-tag", "untagged", "wrong-partition", "adds-helios", "default-lease",
	"short-lease", "no-release-when-idle", "duplicate-tag", "extra-allocation", "pend-past-bound", "second-never-started",
	"second-never-started-cut", "slot-taken", "slot-taken-refused", "slot-taken-cut", "warm-pre-unshown", "warm-suc-unshown"}

func init() { dryFaults["nownooverlap"] = nowNoOverlapFaults }

// The dry run's figures, on the manual clock: a warm start of 8 minutes, the stream from a minute before the drain start
// to a minute after it, the idle limit, then the release; the successor submitted 10 s after the predecessor's end.
const (
	dryNowWarm     = 8 * time.Minute
	dryNowToSubmit = 10 * time.Second
)

// NowNoOverlap is row B over the fake cluster.
func (d *drySteps) NowNoOverlap(_ context.Context, p Plan) (NowNoOverlapObs, error) {
	if d.fault("pend-past-bound") {
		return NowNoOverlapObs{}, d.pendPastBound(p)
	}
	tm := p.Row.Clock()
	tag0 := d.tag(p, "pre")
	n0, id0 := d.submit(tag0, d.partitionFor(false), "")
	started := *d.running(id0).StartTime
	d.extras(p, tag0)
	d.tick(dryNowWarm)
	o := NowNoOverlapObs{Deployment: d.deployment(), PredecessorTag: tag0, PredecessorReady: d.now(), NoOverlapSlotSeen: !d.fault("noslot"), NoReadyTarget: 3,
		WarmPredecessor: d.warm(), OrderSeen: true, CreatedAfterLeft: true,
		// The dry run has no guard; the slacks are those its looks would list, one configuring a second, one in the second it started.
		PredecessorLeaseSlack: LeaseSlackSeconds(ptrDur(time.Second)), SuccessorLeaseSlack: LeaseSlackSeconds(ptrDur(0))}
	if d.fault("not-ready") {
		o.PredecessorReady = time.Time{}
	}
	if d.fault("no-pred-tag") {
		o.PredecessorTag = ""
	}
	if d.fault("warm-pre-unshown") {
		o.WarmPredecessor.LogRead = false
	}
	o.DrainStart = started.Add(d.deployment().LeaseLimit - DrainWindow(tm.MaxRequestDuration, tm.DrainMargin, 0))
	d.tick(o.DrainStart.Sub(d.now()) - time.Minute)
	opened := d.now()
	tag1 := ""
	var n1 int
	var id1 string
	if d.fault("early-submit") {
		// Queued ahead, as on the queued path: the one-slot path must never do it.
		tag1 = d.tag(p, "suc")
		n1, id1 = d.submit(tag1, d.partitionFor(true), "")
	}
	d.tick(2 * time.Minute)
	stream := Stream{ID: "s1", Opened: opened, Ended: d.now(), Complete: true}
	switch {
	case d.fault("no-stream"):
		stream.Ended = o.DrainStart.Add(-time.Second)
	case d.fault("cut"), d.fault("slot-taken-cut"), d.fault("second-never-started-cut"):
		stream.Complete, stream.CutBy = false, "tunnel"
	}
	o.Streams = []Stream{stream}
	// The idle limit after the stream, then the release ends the predecessor's job.
	d.tick(tm.IdleLimit)
	d.end(n0, id0)
	d.tick(dryNowToSubmit)
	if tag1 == "" {
		tag1 = d.tag(p, "suc")
	}
	done := func(err error) (NowNoOverlapObs, error) {
		o.Tags = append([]string(nil), d.tags...)
		return o, err
	}
	switch {
	case strings.HasPrefix(d.cfg.DryFault, "slot-taken"):
		// The owner's own job takes the slot as the predecessor's leaves it; the run reads it there, ends its deployment,
		// and never touches it. The successor's submission never makes a job.
		d.running(d.submitOwnerTo(p, d.cfg.Row.Partition))
		o.Slot = SlotObs{Reads: 2, ForeignSeen: true, Refused: d.fault("slot-taken-refused")}
		o.DeploymentEnded = true
		return done(ErrSlotTaken)
	case d.fault("refused-no-owner"):
		// Refused for the submit limit, with no job outside the run in the slot: the run's own count let it happen.
		o.Slot = SlotObs{Reads: 10, Refused: true}
		o.DeploymentEnded = true
		return done(nil)
	case strings.HasPrefix(d.cfg.DryFault, "second-never-started"):
		id := d.submitHeld(tag1, d.cfg.Row.Partition, "")
		d.pending(id)
		o.Slot = SlotObs{Reads: 600, Listed: true}
		d.tick(p.Row.StartWait + time.Second)
		if err := d.cancelTag(tag1); err != nil {
			return NowNoOverlapObs{}, err
		}
		return done(&SecondNotStartedError{ReplacementLate: LateNotRaised, StartWait: p.Row.StartWait})
	}
	if id1 == "" {
		n1, id1 = d.submit(tag1, d.partitionFor(true), "")
	}
	d.running(id1)
	o.Slot = SlotObs{Reads: 3, Listed: true}
	d.tick(dryNowWarm)
	o.SuccessorReady, o.WarmSuccessor = d.now(), d.warm()
	switch {
	case d.fault("successor-not-ready"):
		o.SuccessorReady = time.Time{}
	case d.fault("nogap"):
		o.SuccessorReady = o.DrainStart.Add(-time.Second)
	case d.fault("created-early"):
		o.CreatedAfterLeft = false
	case d.fault("order-unseen"):
		o.OrderSeen, o.CreatedAfterLeft = false, false
	case d.fault("marked"):
		o.SuccessorMarked = true
	case d.fault("warm-suc-unshown"):
		o.WarmSuccessor.LogRead = false
	}
	// The run ends its deployment: the successor's job ends.
	d.end(n1, id1)
	o.DeploymentEnded = !d.fault("not-ended")
	return done(nil)
}

// submitOwnerTo is the owner's own job on a partition with no QOS named (plgrid-now's own applies), from another project:
// it runs until its release file appears, which the cleanup writes, and the run never touches it.
func (d *drySteps) submitOwnerTo(p Plan, partition string) string {
	d.t.Helper()
	d.jobs++
	n := d.jobs
	line := fmt.Sprintf("sbatch --parsable --no-requeue --job-name=owner-other-project-job --partition=%s --account=example-account --time=%s --gres=gpu:1 --cpus-per-task=1 --mem=1024 --output=%s --error=%s '--wrap=%s'",
		partition, slurmTime(p.PendingMax+time.Hour), d.out(n), d.out(n), d.gateWrap(n))
	res, err := d.sess.Run(context.Background(), slurm.Command{Line: line})
	if err != nil || res.ExitCode != 0 {
		d.t.Fatalf("the owner's job was not accepted: %v exit %d %s", err, res.ExitCode, res.Stderr)
	}
	id := strings.TrimSpace(strings.SplitN(string(res.Stdout), ";", 2)[0])
	if _, err := strconv.Atoi(id); err != nil {
		d.t.Fatalf("sbatch printed %q", res.Stdout)
	}
	return id
}
