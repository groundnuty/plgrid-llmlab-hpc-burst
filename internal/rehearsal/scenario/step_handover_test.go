package scenario_test

// Step (c) of issue #146: a handover with a short lease. The replacement,
// created at its planned time (with no queue wait observed, the primary's
// start: HBEP-7, Handover), is ready; the primary drains at its drain start,
// status.leaseEnd minus the drain window; a stream still running then ends
// with its finish reason and [DONE]; and the primary ends Drained, its
// Allocation ends and its accounting record exists.

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

const (
	// handoverTokens is step (c)'s stream: 2,500 tokens at tokenDelay, about
	// 50 s, under maxRequestDuration (90 s), within max-model-len 4096.
	handoverTokens = 2500
	// streamLead is how long before the drain start step (c)'s stream
	// starts; the stream then runs about 30 s past it.
	streamLead = 20 * time.Second
	// annotationDrainStarted is the time the lifecycle controller recorded
	// for an Instance's drain (internal/lifecycle/instances.go).
	annotationDrainStarted = v1alpha1.Group + "/drain-started"
)

// stepTimer logs a step's elapsed time when the returned func runs.
func (r *rig) stepTimer(step string) func() {
	start := time.Now()
	return func() { r.t.Logf("step (%s) took %s", step, time.Since(start).Round(100*time.Millisecond)) }
}

// beforeDrain fails a step that would start too near the primary's drain
// start: steps (b), (d), (e) and (f) run on the primary before it, and step
// (c) needs streamLead and a key search before it.
func (r *rig) beforeDrain(step string, in *v1alpha1.Instance) {
	r.t.Helper()
	now, _ := r.instance(in.Name)
	ds, ok := drainStartOf(&now)
	if !ok {
		r.t.Fatalf("step (%s): Instance %s has no lease end", step, in.Name)
	}
	left := time.Until(ds)
	if left < streamLead+time.Minute {
		r.t.Fatalf("step (%s) would start %s before the primary's drain start %s: too late for it and step (c); lengthen the lease", step, left.Round(time.Second), ds.Format(time.RFC3339))
	}
	r.t.Logf("step (%s) starts %s before the primary's drain start", step, left.Round(time.Second))
}

// replacementReady waits for the primary's replacement to be ready and
// registered at the front, so the ring holds both before a key is chosen.
func (r *rig) replacementReady(p *v1alpha1.Instance) v1alpha1.Instance {
	r.t.Helper()
	var repl v1alpha1.Instance
	stack.WaitFor(r.t, stack.WaitDeadline, "the primary and its replacement to be ready and registered at the front", func() bool {
		if reg, known := stack.Registered(r.ports, p); !known || !reg {
			return false
		}
		for _, in := range r.instances() {
			if in.Annotations[v1alpha1.AnnotationPredecessor] == p.Name && in.Status.Phase == v1alpha1.InstanceReady {
				if reg, known := stack.Registered(r.ports, &in); known && reg {
					repl = in
					return true
				}
			}
		}
		return false
	})
	return repl
}

// strOrNil prints a record's optional string.
func strOrNil(p *string) string {
	if p == nil {
		return "null"
	}
	return *p
}

func parseRecordTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}

// checkStream holds a long stream to the issue: the client received all its
// tokens of content, its one finish reason, length, its usage and [DONE],
// never stream_cut (sseParse, streamProblems); its record names the
// Instance, has all its tokens, no cut, and took at least tokens times
// tokenDelay.
func (r *rig) checkStream(step string, s stack.Streamed, in *v1alpha1.Instance, tokens int) stack.UsageRow {
	r.t.Helper()
	if s.Err != nil || s.Status != 200 {
		r.t.Fatalf("step (%s) stream: %d %v: %.300s", step, s.Status, s.Err, s.Body)
	}
	got := sseParse(s.Body)
	if p := streamProblems(got, tokens); len(p) > 0 || bytes.Contains(s.Body, []byte("stream_cut")) {
		r.t.Errorf("step (%s) stream as the client received it: %s; stream_cut %v; its end: %.300s", step, strings.Join(p, "; "),
			bytes.Contains(s.Body, []byte("stream_cut")), s.Body[max(0, len(s.Body)-300):])
	}
	r.t.Logf("step (%s) stream: the client received %d tokens of content in %d events, finish %q, usage %d, [DONE] %v",
		step, got.words, got.chunks, got.finishes, got.usageTokens, got.done)
	row := r.fc.RowOf(s.ID)
	switch {
	case row.InstanceUID != string(in.UID):
		r.t.Errorf("step (%s) stream went to %s (%s), not %s", step, row.Instance, row.InstanceUID, in.Name)
	case !row.Stream || row.Finish == nil || *row.Finish != "length" || row.CutBy != nil:
		r.t.Errorf("step (%s) stream's record: stream %v, finish %s, cut by %s; want a stream finished by length, uncut", step, row.Stream, strOrNil(row.Finish), strOrNil(row.CutBy))
	case row.CompletionTokens == nil || *row.CompletionTokens != int64(tokens):
		r.t.Errorf("step (%s) stream's record has %v completion tokens, want %d", step, row.CompletionTokens, tokens)
	case time.Duration(row.DurationMs)*time.Millisecond < time.Duration(tokens)*r.tokenDelay:
		r.t.Errorf("step (%s) stream took %d ms, below %d tokens at %s", step, row.DurationMs, tokens, r.tokenDelay)
	}
	return row
}

// stepHandover is step (c), on the primary p, after the other steps.
func (r *rig) stepHandover(p *v1alpha1.Instance) {
	r.t.Helper()
	defer r.stepTimer("c")()
	pred, _ := r.instance(p.Name)
	ds, _ := drainStartOf(&pred)
	if left := time.Until(ds); left < streamLead+5*time.Second {
		r.t.Fatalf("step (c) reached %s before the drain start %s: too late to start its stream before it; lengthen the lease", left.Round(time.Second), ds.Format(time.RFC3339))
	}
	repl := r.replacementReady(&pred)
	key := r.fc.KeyFor(&pred) // the ring as it is now, with both Instances
	stack.WaitFor(r.t, time.Until(ds), "the drain start minus the stream's lead", func() bool { return !time.Now().Before(ds.Add(-streamLead)) })
	first, done := r.fc.StartStream(stack.ChatBody(stack.Model, handoverTokens, handoverTokens, true, ""), key)
	<-first
	var drainSeen time.Time
	stack.WaitFor(r.t, stack.WaitDeadline, "the primary to drain", func() bool {
		o, ok := r.watch.first(pred.UID, time.Time{}, func(o observation) bool { return o.phase == v1alpha1.InstanceDraining })
		drainSeen = o.at
		return ok
	})
	var s stack.Streamed
	select {
	case s = <-done:
	case <-time.After(r.fc.RequestTimeout()):
		r.t.Fatalf("step (c): the stream did not end within %s", r.fc.RequestTimeout())
	}
	row := r.checkStream("c", s, &pred, handoverTokens)
	rs, re := parseRecordTime(row.StartTime), parseRecordTime(row.EndTime)
	r.t.Logf("step (c): stream %s to %s; drain start %s; draining seen %s", rs.Format(time.RFC3339Nano), re.Format(time.RFC3339Nano), ds.Format(time.RFC3339), drainSeen.Format(time.RFC3339Nano))
	if !rs.Before(ds) || !ds.Before(re) {
		r.t.Errorf("step (c): no stream in flight at the drain start %s: its record runs %s to %s", ds.Format(time.RFC3339), row.StartTime, row.EndTime)
	}
	if !drainSeen.Before(re) {
		r.t.Errorf("step (c): the primary was first seen draining at %s, after the stream's end %s", drainSeen.Format(time.RFC3339Nano), row.EndTime)
	}
	r.checkDrained(&pred, &repl, ds, re)
}

// checkDrained: the controller recorded the drain at or after the drain start
// and before the stream ended; it handed over to the ready replacement, which
// was created no earlier than the primary's Allocation started; the primary
// ends Drained, its Allocation ends, and its acct- record exists.
func (r *rig) checkDrained(pred, repl *v1alpha1.Instance, ds, streamEnd time.Time) {
	r.t.Helper()
	// The ended Instance may already be gone with its Allocation: the
	// controller's recorded drain start is read from the watch.
	o, _ := r.watch.first(pred.UID, time.Time{}, func(o observation) bool { return o.drainBegan != "" })
	began, err := time.Parse(time.RFC3339, o.drainBegan)
	if err != nil || began.Before(ds) || !began.Before(streamEnd) {
		r.t.Errorf("step (c): the recorded drain start %q (%v) is not in [drain start %s, stream end %s)", o.drainBegan, err, ds.Format(time.RFC3339), streamEnd.Format(time.RFC3339Nano))
	}
	if countReason(r.eventReasons(pred.Name), "HandedOver") == 0 {
		r.t.Errorf("step (c): no HandedOver event on %s: the replacement was not ready at the drain start (events %v)", pred.Name, r.eventReasons(pred.Name))
	}
	var alloc v1alpha1.Allocation
	if !r.get(&alloc, pred.Spec.AllocationRef.Name, stack.Namespace) || alloc.Status.AtProvider.StartTime == nil {
		r.t.Fatalf("step (c): the primary's Allocation %s or its start is unknown", pred.Spec.AllocationRef.Name)
	}
	if repl.CreationTimestamp.Before(alloc.Status.AtProvider.StartTime) || repl.Status.ReadyTime == nil || !repl.Status.ReadyTime.Time.Before(ds) {
		r.t.Errorf("step (c): replacement %s created %s, ready %v; the primary's Allocation started %s and drains at %s",
			repl.Name, repl.CreationTimestamp.Format(time.RFC3339), repl.Status.ReadyTime, alloc.Status.AtProvider.StartTime.Format(time.RFC3339), ds.Format(time.RFC3339))
	}
	r.t.Logf("step (c): replacement %s created %s after the primary's Allocation started", repl.Name, repl.CreationTimestamp.Sub(alloc.Status.AtProvider.StartTime.Time))
	// A released Allocation is deleted (internal/lifecycle/retention.go), and
	// the ended Instance goes with it, so the watch shows how the Instance
	// ended; the Allocation ends gone or Ended, and its job leaves the fake
	// cluster's queue.
	jobID, _ := strconv.Atoi(pred.Status.JobID)
	stack.WaitState(r.t, stack.WaitDeadline, "the primary to end Drained, its Allocation to end and its job to finish", func() (bool, string) {
		o, ended := r.watch.first(pred.UID, time.Time{}, func(o observation) bool { return o.phase == v1alpha1.InstanceEnded })
		var a v1alpha1.Allocation
		gone := !r.get(&a, alloc.Name, stack.Namespace)
		j, _ := r.cl.Plat.Job(jobID)
		state := fmt.Sprintf("Instance %s seen ended %v (reason %q); Allocation gone %v phase %s; job %d %s finished %v",
			pred.Name, ended, o.endReason, gone, a.Status.AtProvider.Phase, jobID, j.State, j.Finished)
		return ended && o.endReason == v1alpha1.ReasonDrained &&
			(gone || a.Status.AtProvider.Phase == v1alpha1.AllocationEnded) &&
			(j.Finished || j.State != "PENDING" && j.State != "RUNNING"), state
	})
	name, err := v1alpha1.AccountingRecordName(alloc.UID)
	if err != nil {
		r.t.Fatal(err)
	}
	defer reportOrphanedTunnels(r.t, r.c, "c")
	r.proxyEnded(pred.UID)
	stack.WaitFor(r.t, stack.WaitDeadline, "the primary's Allocation's record "+name, func() bool {
		var l appsv1.ControllerRevisionList
		ctx, cancel := stack.CallCtx(r.t)
		defer cancel()
		if err := r.c.List(ctx, &l); err != nil {
			return false
		}
		for _, cr := range l.Items {
			if cr.Name == name {
				return true
			}
		}
		return false
	})
}
