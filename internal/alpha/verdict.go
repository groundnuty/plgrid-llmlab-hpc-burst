package alpha

import (
	"fmt"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The pass criteria. Each function returns the failed checks as fixed labels
// (no job ID, host or account can be in one) and nothing when the run passes.
// Each check quotes the criterion it enforces, with the file and line at main's
// c7c41a4 (1 Nov 2026); docs/alpha-runbook.md lists the same quotes.

// Hour is S1's stream length: "a completed stream of at least 3600 s".
const Hour = time.Hour

// minRate is S1's flag line: "a rate under 629,146 B/s" is flagged, not failed
// (spikes/s1-frp/README.md:288, C6; branch spike/s1-frp-dial-home).
const minRate = 629146

// jobsByTag maps the cluster's record of the run's jobs by their tags.
func jobsByTag(jobs []provider.Job) map[string]provider.Job {
	m := make(map[string]provider.Job, len(jobs))
	for _, j := range jobs {
		m[j.Name] = j
	}
	return m
}

// streamChecks judges the streams of a handover or a no-overlap gap. Both
// criteria say "no request cut", and both tests warn that a run with nothing in
// flight proves nothing:
//
//	hbep/0007-lifecycle-controller/README.md:592: "A run with no stream in flight at the drain start proves nothing here"
//	hbep/0007-lifecycle-controller/README.md:620: "one deployment completes one handover end to end through the front with no request cut"
//	hbep/0006-instance-reachability/README.md:349: "A test that restarts the controller with no stream running proves nothing about cuts."
func streamChecks(streams []Stream, drainStart time.Time) []string {
	var f []string
	inFlight := 0
	for _, s := range streams {
		if s.InFlightAt(drainStart) {
			inFlight++
		}
		if s.CutBy != "" {
			f = append(f, "a stream was cut")
		} else if !s.Complete {
			f = append(f, "a stream did not finish by its completion rule")
		}
	}
	if inFlight == 0 {
		f = append(f, "no stream was in flight at the drain start")
	}
	return dedupe(f)
}

// ledgerChecks judges what the cluster itself recorded of the run's jobs: the
// partition each ran on, and that every tag the run wrote down is a job there.
func ledgerChecks(row Row, tags []string, jobs []provider.Job) ([]string, map[string]provider.Job) {
	var f []string
	by := jobsByTag(jobs)
	for _, tag := range tags {
		j, ok := by[tag]
		switch {
		case !ok:
			f = append(f, "the cluster has no record of a job the run made")
		case j.Partition != row.Partition:
			f = append(f, "a job ran on another partition than the row's")
		}
	}
	return f, by
}

// VerifyHandover judges alpha-athena-handover.
//
//	hbep/0007-lifecycle-controller/README.md:620: "on Athena `plgrid-gpu-a100`, one deployment completes one handover end to end through the front with no request cut;"
//	hbep/0007-lifecycle-controller/README.md:622: "on each alpha platform, Helios `plgrid-gpu-gh200` and Athena `plgrid-gpu-a100`, one restart at the worker rung;"
//	hbep/0005-llmlab-compatible-front/README.md:450: "on Athena `plgrid-gpu-a100`, through the front, one handover cuts no request (with HBEP-7)."
func VerifyHandover(row Row, o HandoverObs, jobs []provider.Job) []string {
	f := streamChecks(o.Streams, o.DrainStart)
	if len(o.Tags) < 2 {
		f = append(f, "fewer than two Allocations: a handover needs a predecessor and a successor")
	}
	lf, _ := ledgerChecks(row, o.Tags, jobs)
	f = append(f, lf...)
	if o.Handovers != 1 {
		f = append(f, fmt.Sprintf("%d handovers completed, want 1", o.Handovers))
	}
	if len(o.Instances) != 2 {
		f = append(f, "the run did not record both Instances")
	} else if p, s := o.Instances[0], o.Instances[1]; p.EndedAt.IsZero() || s.ReadyAt.IsZero() || s.ReadyAt.After(p.EndedAt) {
		// A handover is make-before-break: the successor is ready before the predecessor ends.
		f = append(f, "the successor was not ready before the predecessor ended")
	}
	if o.WorkerRestarts != 1 {
		f = append(f, fmt.Sprintf("%d restarts at the worker rung, want 1", o.WorkerRestarts))
	}
	if o.Responses > 0 && o.Attributed != o.Responses {
		f = append(f, "a response lacked its attribution headers")
	}
	return dedupe(f)
}

// VerifyNoOverlap judges alpha-helios-nooverlap.
//
//	hbep/0007-lifecycle-controller/README.md:621: "on Helios `plgrid-gpu-gh200` under QOS `now`, one deployment goes through no-overlap mode end to end through the front with no request cut, and the gap is measured"
//	hbep/0007-lifecycle-controller/README.md:594: "The replacement is created at its planned time, accepted, and pends on the fake's GPU cap while the predecessor runs; it never starts before the predecessor's job ends ... A fixture without a GPU cap proves nothing here, since its replacement would start beside the predecessor."
//	hbep/0007-lifecycle-controller/README.md:466: "the gap is the time from the primary's drain start until its successor is `ready`."
func VerifyNoOverlap(row Row, o NoOverlapObs, jobs []provider.Job) []string {
	f := streamChecks(o.Streams, o.DrainStart)
	if len(o.Tags) != 2 {
		f = append(f, "a no-overlap run makes exactly two Allocations: a predecessor and a successor")
	}
	lf, by := ledgerChecks(row, o.Tags, jobs)
	f = append(f, lf...)
	if o.Gap() <= 0 {
		f = append(f, "the gap was not measured: the drain start and the successor's ready are not both seen, in that order")
	}
	if !o.NoOverlapSlotSeen {
		f = append(f, "the deployment never showed NoOverlapSlot")
	}
	if len(o.Tags) == 2 {
		pre, okP := by[o.Tags[0]]
		suc, okS := by[o.Tags[1]]
		if okP && okS {
			f = append(f, queuedAheadChecks(pre, suc)...)
		}
	}
	return dedupe(f)
}

// queuedAheadChecks judges, from the cluster's own times, that the successor was
// queued ahead of its predecessor's end and never ran beside it.
func queuedAheadChecks(pre, suc provider.Job) []string {
	var f []string
	switch {
	case suc.SubmitTime.IsZero() || pre.EndTime == nil || suc.StartTime == nil:
		f = append(f, "the cluster's times for the two jobs are incomplete")
	default:
		if !suc.SubmitTime.Before(*pre.EndTime) {
			f = append(f, "the successor was not queued before the predecessor ended")
		}
		if suc.StartTime.Before(*pre.EndTime) {
			f = append(f, "the successor started before the predecessor ended")
		}
	}
	return f
}

// SuccessorQueueWait is the successor's queue wait once the GPU freed: its start
// minus the predecessor's end, which the Helios run records
// (hbep/0007-lifecycle-controller/README.md:600). ok is false when the cluster
// does not give both times.
func SuccessorQueueWait(pre, suc provider.Job) (d time.Duration, ok bool) {
	if pre.EndTime == nil || suc.StartTime == nil {
		return 0, false
	}
	return suc.StartTime.Sub(*pre.EndTime), true
}

// VerifyTunnelHour judges tunnel-hour-helios and tunnel-hour-athena.
//
//	hbep/0006-instance-reachability/README.md:354: "S1's one-hour runs pass with this wiring on Helios and on Athena, in the mode each uses, with the path rate measured."
//	spikes/s1-frp/README.md:284 and :285 (C2, C3; branch spike/s1-frp-dial-home, 01c2ecd): every lane has "a completed stream of at least 3600 s", and no stream is cut over that hour.
//	spikes/s1-frp/README.md:288 (C6): the path rate is "ok, at least 30 s asked, at least 29 s timed at the VM's receiver", and moved bytes.
func VerifyTunnelHour(row Row, o TunnelHourObs, jobs []provider.Job) []string {
	var f []string
	if len(o.Lanes) == 0 {
		f = append(f, "no stream was held for the hour")
	}
	for _, s := range o.Lanes {
		switch {
		case s.CutBy != "":
			f = append(f, "a stream was cut during the hour")
		case !s.Complete:
			f = append(f, "a stream did not complete")
		case s.Ended.Sub(s.Opened) < Hour:
			f = append(f, "a stream lasted less than an hour")
		}
	}
	if o.TunnelDrops != 0 {
		f = append(f, "the tunnel dropped during the hour")
	}
	r := o.Rate
	switch {
	case !r.OK:
		f = append(f, "the path rate was not measured")
	case r.Direction != "up":
		f = append(f, "the path rate is not in the token direction (node to front)")
	case r.AskedSeconds < 30 || r.TimedSeconds < 29 || r.Bytes <= 0:
		f = append(f, "the path rate was asked for under 30 s, timed under 29 s, or moved no bytes")
	}
	lf, by := ledgerChecks(row, o.Tags, jobs)
	f = append(f, lf...)
	if len(o.Tags) == 0 {
		f = append(f, "the run wrote down no job tag")
	}
	// The cluster's own times corroborate the lanes': a stream's clock is the
	// scenario's, the job's start and end are the scheduler's.
	for _, tag := range o.Tags {
		if j, ok := by[tag]; ok && j.StartTime != nil && j.EndTime != nil && j.EndTime.Sub(*j.StartTime) < Hour {
			f = append(f, "the cluster's record shows the job ran under an hour")
		}
	}
	return dedupe(f)
}

// TunnelHourFlags are the observations S1's verdict prints as FLAG lines: they
// are posted with the result and do not fail it.
func TunnelHourFlags(o TunnelHourObs) []string {
	if bps := o.Rate.BytesPerSecond(); o.Rate.OK && bps > 0 && bps < minRate {
		return []string{fmt.Sprintf("path rate %.0f B/s is under 629146 B/s (6 MiB window over 10 s)", bps)}
	}
	return nil
}

// VerifyAccounting judges gpu-accounting-*.
//
//	hbep/0008-observability-and-events/README.md:395: "the kept accounting record's GPU entry is checked once on a real GPU job on each alpha partition;"
//	hbep/0008-observability-and-events/README.md:331: the record holds "`allocatedGPUs` ... and `basis: allocated`".
//
// The record is built from the scheduler's accounting, so it is never checked
// against the scheduler's accounting: NodeGPUs is what the job itself saw.
func VerifyAccounting(row Row, o AccountingObs) []string {
	var f []string
	if o.Partition != row.Partition {
		f = append(f, "the record names another partition than the row's")
	}
	if o.Basis != "allocated" {
		f = append(f, "the record's basis is not allocated")
	}
	if o.RequestedGPUs <= 0 || o.NodeGPUs <= 0 {
		// A 0 = 0 agreement would prove nothing: a GPU job is the point.
		f = append(f, "the job asked for or saw no GPU")
	}
	switch {
	case o.RecordGPUs == nil:
		f = append(f, "the record marks its GPU entry unknown")
	case int(*o.RecordGPUs) != o.NodeGPUs:
		f = append(f, "the record's GPU entry differs from the GPUs the job saw")
	}
	if o.RequestedGPUs != o.NodeGPUs {
		f = append(f, "the GPUs the job saw differ from what it asked for")
	}
	return dedupe(f)
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// Summary joins failures for a log line.
func Summary(failures []string) string { return strings.Join(failures, "; ") }
