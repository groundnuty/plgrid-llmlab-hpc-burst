package scenario_test

// Item 5 of issue #193 (the science agent's comment of 1 Nov 2026): HBEP-7's
// no-overlap handover end to end over the real tunnel, on the queued path and
// the one-slot path at once. A stream is in flight at each primary's drain
// start and runs to its end uncut; paced requests through each gap get 503
// no_ready_target, and the engine gate holds each successor until enough of
// them have been answered; NoOverlapSlot holds from the drain start until the
// successor is ready; the gap is measured; the one-slot path never has two
// live jobs, read at each sbatch the controller sent; and the queued
// successor pends on the GPU cap.

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

// depRun is one served deployment as the run goes.
type depRun struct {
	dep
	primary    *v1alpha1.Instance
	primaryJob int
	ds         time.Time // the primary's drain start, from its lease end and the spec
	// succ is the successor once ready; succAlloc and succJob its
	// Allocation and job.
	succ      *v1alpha1.Instance
	succAlloc string
	succJob   int
	stream    <-chan stack.Streamed // the stream in flight at the drain start
	probes    []probe
	// gateJob is the successor's job the engine gate held, from gateHeld
	// (its engine start reached the gate) until gateReleased, once
	// gateReplies requests had been answered in the gap.
	gateJob                int
	gateHeld, gateReleased time.Time
	gateReplies            int
}

// probe is one paced request of the gap prober, and when it was sent.
type probe struct {
	model string
	sent  time.Time
	rp    stack.Reply
}

// prober sends one short request for each model every probeEvery until it is
// stopped, and keeps every reply. It never fails the test itself: the replies
// are judged after (gapProblems).
type prober struct {
	stop   chan struct{}
	done   chan struct{}
	cancel context.CancelFunc
	once   sync.Once
	mu     sync.Mutex
	got    []probe
}

const probeEvery = time.Second

// startProber starts the prober; the test's clean-up stops it, so a test that
// fails part-way sends nothing after it.
func (p3 *part3) startProber(models ...string) *prober {
	ctx, cancel := context.WithCancel(context.Background())
	pr := &prober{stop: make(chan struct{}), done: make(chan struct{}), cancel: cancel}
	go func() {
		defer close(pr.done)
		for {
			for _, m := range models {
				sent := time.Now()
				rp := p3.fc.SendCtx(ctx, "/v1/chat/completions", stack.ChatBody(m, 1, 0, false, ""), "")
				pr.mu.Lock()
				pr.got = append(pr.got, probe{model: m, sent: sent, rp: rp})
				pr.mu.Unlock()
			}
			select {
			case <-pr.stop:
				return
			case <-time.After(probeEvery):
			}
		}
	}()
	p3.t.Cleanup(pr.halt)
	return pr
}

// halt stops the prober and cancels a request in flight; it is safe to call
// again.
func (pr *prober) halt() {
	pr.once.Do(func() {
		close(pr.stop)
		pr.cancel()
		<-pr.done
	})
}

// replies is a copy of the replies so far.
func (pr *prober) replies() []probe {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return slices.Clone(pr.got)
}

// end stops the prober once its round in flight is answered and returns
// every reply.
func (pr *prober) end() []probe {
	pr.once.Do(func() {
		close(pr.stop)
		<-pr.done
		pr.cancel()
	})
	return pr.replies()
}

// noOverlap is item 5: both deployments' handovers, from a stream started
// streamLead before each drain start to each successor's ready. The engine
// gate is armed throughout, so each successor's engine waits at its start
// until minGapProbes requests have been answered in its gap (waitSuccessors).
func (p3 *part3) noOverlap() {
	r := p3.rig
	r.t.Helper()
	order := slices.Clone(p3.deps)
	sort.Slice(order, func(i, j int) bool { return order[i].ds.Before(order[j].ds) })
	p3.atLeastBefore(order[0], "the no-overlap phase", streamLead+10*time.Second)
	var models []string
	for _, d := range p3.deps {
		models = append(models, d.Model)
		if d.oneSlot {
			// Before its drain start the one-slot path has no successor at
			// all: no Allocation is created while the slot is held.
			if n := len(p3.allocationsOf(d.Name)); n != 1 {
				r.t.Errorf("%s: %d Allocations before the drain start, want the primary's alone (HBEP-7, one-slot path)", d.Name, n)
			}
		}
	}
	gate := r.cl.gate
	if gate == nil {
		r.t.Fatal("the no-overlap phase needs the engine gate (rigSetup.engineGate)")
	}
	if err := gate.arm(); err != nil {
		r.t.Fatalf("arm the engine gate: %v", err)
	}
	pr := p3.startProber(models...)
	for _, d := range order {
		stack.WaitFor(r.t, time.Until(d.ds)+time.Minute, "the drain start of "+d.Name+" minus the stream's lead", func() bool {
			return !time.Now().Before(d.ds.Add(-streamLead))
		})
		first, done := r.fc.StartStream(stack.ChatBody(d.Model, longTokens, longTokens, true, ""), "rehearsal-part3-drain-"+d.Name)
		<-first
		d.stream = done
		r.t.Logf("%s: a %d-token stream started %s before the drain start", d.Name, longTokens, time.Until(d.ds).Round(100*time.Millisecond))
	}
	p3.waitSuccessors(pr)
	if err := gate.open(); err != nil {
		r.t.Fatalf("open the engine gate: %v", err)
	}
	probes := pr.end()
	for _, d := range p3.deps {
		for _, pb := range probes {
			if pb.model == d.Model {
				d.probes = append(d.probes, pb)
			}
		}
		p3.checkDrainStream(d)
		p3.checkGap(d)
		p3.checkNoOverlapSlot(d)
		if d.oneSlot {
			p3.checkOneSlot(d)
		} else {
			p3.checkQueuedHandover(d)
		}
	}
}

// waitSuccessors releases each successor from the engine gate once its
// engine start is held there and minGapProbes requests sent after its
// deployment's drain start (and gapSlack) have been answered with the gap's
// 503 (releaseGate); then waits until each successor is ready, registered at
// the front, and has served one of the prober's requests.
func (p3 *part3) waitSuccessors(pr *prober) {
	r := p3.rig
	r.t.Helper()
	stack.WaitState(r.t, stack.WaitDeadline+2*time.Minute, "each successor to be held at the engine gate through its gap, then to serve", func() (bool, string) {
		replies := pr.replies()
		var waiting []string
		for _, d := range p3.deps {
			if d.succ != nil {
				continue
			}
			if d.gateReleased.IsZero() {
				if w := p3.releaseGate(d, replies); w != "" {
					waiting = append(waiting, w)
					continue
				}
			}
			if w := p3.successorServes(d, replies); w != "" {
				waiting = append(waiting, w)
			}
		}
		return len(waiting) == 0, strings.Join(waiting, "; ")
	})
	for _, d := range p3.deps {
		if d.succJob != d.gateJob {
			r.t.Errorf("%s: the successor runs on job %d, but the engine gate held job %d", d.Name, d.succJob, d.gateJob)
		}
		r.t.Logf("%s: successor %s serves on job %d; the engine gate held it from %s to %s, released once %d requests had been answered in the gap",
			d.Name, d.succ.Name, d.succJob, ts(d.gateHeld), ts(d.gateReleased), d.gateReplies)
	}
}

// releaseGate releases the deployment's successor from the engine gate once
// its engine start is held there and enough of the gap has been probed; it
// returns what it still waits for, "" once released.
func (p3 *part3) releaseGate(d *depRun, replies []probe) string {
	r := p3.rig
	r.t.Helper()
	held, err := r.cl.gate.heldJobs()
	if err != nil {
		r.t.Fatalf("the engine gate: %v", err)
	}
	job := 0
	for _, id := range held {
		if j, ok := r.cl.Plat.Job(id); ok && id != d.primaryJob && j.Partition == d.partition {
			job = id
		}
	}
	if job == 0 {
		return d.Name + ": no successor's engine start is held at the gate yet"
	}
	n := gapReplies(replies, d.Model, d.ds.Add(gapSlack))
	if n < minGapProbes {
		return fmt.Sprintf("%s: the successor's job %d is held at the engine gate; %d of %d requests answered in the gap", d.Name, job, n, minGapProbes)
	}
	at, _ := r.cl.gate.held(job)
	if err := r.cl.gate.release(job); err != nil {
		r.t.Fatalf("release job %d from the engine gate: %v", job, err)
	}
	d.gateJob, d.gateHeld, d.gateReleased, d.gateReplies = job, at, time.Now(), n
	return ""
}

// gapReplies counts the model's requests sent at or after from that the front
// answered with the gap's 503: model_inactive, no_ready_target, Retry-After
// 30, from the front (HBEP-7, No-overlap mode; HBEP-5, Errors).
func gapReplies(replies []probe, model string, from time.Time) int {
	n := 0
	for _, pb := range replies {
		if pb.model != model || pb.sent.Before(from) || pb.rp.Err != nil || pb.rp.Status != http.StatusServiceUnavailable {
			continue
		}
		e := stack.ErrorOf(pb.rp.Body)
		if pb.rp.Header.Get(hdrEmitter) == "front" && e.Code == "model_inactive" && derefS(e.Detail) == "no_ready_target" &&
			pb.rp.Header.Get("Retry-After") == "30" {
			n++
		}
	}
	return n
}

// successorServes records the deployment's successor once it is ready,
// registered at the front, and has served one of the prober's requests; it
// returns what it still waits for, "" once it serves.
func (p3 *part3) successorServes(d *depRun, replies []probe) string {
	r := p3.rig
	r.t.Helper()
	var succ *v1alpha1.Instance
	for _, in := range p3.instancesOf(d.Name) {
		if in.Annotations[v1alpha1.AnnotationPredecessor] == d.primary.Name && in.Status.Phase == v1alpha1.InstanceReady {
			in := in
			succ = &in
		}
	}
	if succ == nil {
		return d.Name + ": its successor is not ready yet"
	}
	if reg, known := stack.Registered(r.ports, succ); !known || !reg {
		return d.Name + ": the successor " + succ.Name + " is ready but not registered at the front"
	}
	for _, pb := range replies {
		if pb.model == d.Model && pb.rp.Status == http.StatusOK && pb.rp.Header.Get("Hpcburst-Instance-Uid") == string(succ.UID) {
			d.succ, d.succAlloc = succ, succ.Spec.AllocationRef.Name
			d.succJob = jobIDOf(r.t, succ)
			return ""
		}
	}
	return d.Name + ": the successor " + succ.Name + " has served no probe yet"
}

// checkDrainStream: the stream in flight at the drain start ran to its end,
// with its finish reason and [DONE], never cut, across the drain start.
func (p3 *part3) checkDrainStream(d *depRun) {
	r := p3.rig
	r.t.Helper()
	var s stack.Streamed
	select {
	case s = <-d.stream:
	case <-time.After(r.fc.RequestTimeout()):
		r.t.Fatalf("%s: the stream in flight at the drain start did not end within %s", d.Name, r.fc.RequestTimeout())
	}
	row := r.checkStream("drain "+d.Name, s, d.primary, longTokens)
	p3.attribute("the stream in flight at "+d.Name+"'s drain start", s.Reply, row, wantInstance(d, d.primary))
	rs, re := parseRecordTime(row.StartTime), parseRecordTime(row.EndTime)
	if !rs.Before(d.ds) || !d.ds.Before(re) {
		r.t.Errorf("%s: no stream in flight at the drain start %s: its record runs %s to %s", d.Name, d.ds.UTC().Format(time.RFC3339), row.StartTime, row.EndTime)
	}
	r.t.Logf("%s: the drain-start stream ran %s to %s, %s past the drain start; finish %s, cut by %s", d.Name, row.StartTime, row.EndTime,
		re.Sub(d.ds).Round(100*time.Millisecond), derefS(row.Finish), derefS(row.CutBy))
}

// minGapProbes is the fewest requests that must fall inside a gap: a gap
// measured with no request sent during it proves nothing (issue #193).
const minGapProbes = 5

// checkGap judges every request the prober sent for the deployment
// (gapProblems) and reports the gap.
func (p3 *part3) checkGap(d *depRun) {
	r := p3.rig
	r.t.Helper()
	in := gapInput{drainStart: d.ds, primaryUID: string(d.primary.UID), succUID: string(d.succ.UID)}
	if d.succ.Status.ReadyTime != nil {
		in.succReady = d.succ.Status.ReadyTime.Time
	}
	if o, ok := r.watch.First(d.succ.UID, time.Time{}, func(o stack.Observation) bool { return o.Phase == v1alpha1.InstanceReady }); ok {
		in.succReadySeen = o.At
	}
	for _, pb := range d.probes {
		if pb.rp.Err != nil {
			in.probes = append(in.probes, gapProbe{err: pb.rp.Err.Error()})
			continue
		}
		row := r.fc.RowOf(pb.rp.ID)
		p3.attribute("a probe of "+d.Name, pb.rp, row, p3.wantForProbe(d, row))
		e := stack.ErrorOf(pb.rp.Body)
		in.probes = append(in.probes, gapProbe{start: parseRecordTime(row.StartTime), status: pb.rp.Status, emitter: row.Emitter,
			instanceUID: row.InstanceUID, code: e.Code, detail: derefS(e.Detail), retryAfter: pb.rp.Header.Get("Retry-After")})
	}
	rep, problems := gapProblems(in)
	for _, p := range problems {
		r.t.Errorf("%s: %s", d.Name, p)
	}
	o, _ := r.watch.First(d.primary.UID, time.Time{}, func(o stack.Observation) bool { return o.DrainBegan != "" })
	began, _ := time.Parse(time.RFC3339, o.DrainBegan)
	pj, _ := r.cl.Plat.Job(d.primaryJob)
	sj, _ := r.cl.Plat.Job(d.succJob)
	since := func(t time.Time) string {
		if t.IsZero() {
			return "never"
		}
		return t.Sub(d.ds).Round(100 * time.Millisecond).String()
	}
	r.t.Logf("GAP %s: %d requests in the gap, all 503 no_ready_target; first 503 %s after the drain start, first served by the successor %s after it; "+
		"the controller: drain recorded %s, successor ready %s (%s); the fake: predecessor's job ended %s, successor's started %s (%s queue wait after the GPU or slot freed); "+
		"the engine gate held the successor's engine start from %s to %s (%s after the drain start)",
		d.Name, rep.inGap, since(rep.first503), since(rep.firstSucc),
		began.UTC().Format(time.RFC3339), in.succReady.UTC().Format(time.RFC3339), in.succReady.Sub(began).Round(time.Second),
		pj.End.UTC().Format(time.RFC3339Nano), sj.Start.UTC().Format(time.RFC3339Nano), sj.Start.Sub(pj.End).Round(100*time.Millisecond),
		ts(d.gateHeld), ts(d.gateReleased), since(d.gateHeld))
}

// wantForProbe is what a probe's reply must carry: an Instance's attribution
// when one served it, else the front's error before a target.
func (p3 *part3) wantForProbe(d *depRun, row stack.UsageRow) wantAttr {
	switch row.InstanceUID {
	case string(d.primary.UID):
		return wantInstance(d, d.primary)
	case string(d.succ.UID):
		return wantInstance(d, d.succ)
	}
	return wantFront()
}

// gapInput is what the gap check reads: the drain start, the successor's
// ready time on the controller's clock and as the watch first saw it, and
// each probe's reply and usage record.
type gapInput struct {
	drainStart, succReady, succReadySeen time.Time
	primaryUID, succUID                  string
	probes                               []gapProbe
}

type gapProbe struct {
	err                      string // the client's error, "" when the request was answered
	start                    time.Time
	status                   int
	emitter, instanceUID     string
	code, detail, retryAfter string
}

type gapReport struct {
	inGap               int
	first503, firstSucc time.Time
}

// gapSlack is how far from the drain start a request's record may start and
// still be routed either way: the front judges the drain start when it
// chooses a target, a moment after the record's start time.
const gapSlack = time.Second

// gapProblems judges a gap from the prober's replies, and measures it.
//   - every request got an answer;
//   - before the drain start the primary serves; from it until the
//     successor serves, every reply is the front's 503 model_inactive with
//     detail no_ready_target and Retry-After 30 (HBEP-7, No-overlap mode;
//     HBEP-5, Errors); once the successor has served, it serves every one;
//   - at least minGapProbes requests started inside [drain start + slack,
//     the successor's ready time];
//   - the successor first served no earlier than its ready time.
func gapProblems(in gapInput) (gapReport, []string) {
	var rep gapReport
	var out []string
	for i, p := range in.probes {
		if p.err != "" {
			out = append(out, fmt.Sprintf("probe %d got no answer: %s", i, p.err))
			continue
		}
		if p.status == http.StatusOK && p.instanceUID == in.succUID && (rep.firstSucc.IsZero() || p.start.Before(rep.firstSucc)) {
			rep.firstSucc = p.start
		}
	}
	if rep.firstSucc.IsZero() {
		out = append(out, "the successor served no probe")
	}
	ready := in.succReady
	if in.succReadySeen.After(ready) {
		ready = in.succReadySeen
	}
	for i, p := range in.probes {
		if p.err != "" {
			continue
		}
		served := p.status == http.StatusOK && p.emitter == "instance"
		gapReply := p.status == http.StatusServiceUnavailable && p.emitter == "front" && p.code == "model_inactive" &&
			p.detail == "no_ready_target" && p.retryAfter == "30"
		switch {
		case p.start.Before(in.drainStart.Add(-gapSlack)):
			if !served || p.instanceUID != in.primaryUID {
				out = append(out, fmt.Sprintf("probe %d at %s, before the drain start: %d from %s (%s), want the primary's 200", i, ts(p.start), p.status, p.emitter, p.instanceUID))
			}
		case !rep.firstSucc.IsZero() && !p.start.Before(rep.firstSucc):
			if !served || p.instanceUID != in.succUID {
				out = append(out, fmt.Sprintf("probe %d at %s, after the successor first served: %d %s/%s from %s, want the successor's 200", i, ts(p.start), p.status, p.code, p.detail, p.emitter))
			}
		case p.start.Before(in.drainStart.Add(gapSlack)):
			if !served && !gapReply {
				out = append(out, fmt.Sprintf("probe %d at the drain start %s: %d %s/%s from %s, want the primary's 200 or the gap's 503", i, ts(p.start), p.status, p.code, p.detail, p.emitter))
			}
		default:
			if !gapReply {
				out = append(out, fmt.Sprintf("probe %d at %s, in the gap: %d code %q detail %q Retry-After %q from %s (%s), want 503 model_inactive no_ready_target, Retry-After 30, from the front",
					i, ts(p.start), p.status, p.code, p.detail, p.retryAfter, p.emitter, p.instanceUID))
				break
			}
			if rep.first503.IsZero() {
				rep.first503 = p.start
			}
			if !p.start.After(ready) {
				rep.inGap++
			}
		}
	}
	if rep.inGap < minGapProbes {
		out = append(out, fmt.Sprintf("%d requests got the gap's 503 between the drain start %s and the successor's ready %s, under %d: the gap was not probed",
			rep.inGap, ts(in.drainStart), ts(ready), minGapProbes))
	}
	if !rep.firstSucc.IsZero() && rep.firstSucc.Before(in.succReady) {
		out = append(out, fmt.Sprintf("the successor served a request at %s, before its ready time %s", ts(rep.firstSucc), ts(in.succReady)))
	}
	return rep, out
}

func ts(t time.Time) string { return t.UTC().Format("15:04:05.000") }

// checkNoOverlapSlot holds the deployment's Degraded condition, as the watch
// saw it, to HBEP-7 (noOverlapSlotProblems), then waits for it to clear.
func (p3 *part3) checkNoOverlapSlot(d *depRun) {
	r := p3.rig
	r.t.Helper()
	o, _ := r.watch.First(d.succ.UID, time.Time{}, func(o stack.Observation) bool { return o.Phase == v1alpha1.InstanceReady })
	for _, p := range noOverlapSlotProblems(r.deploymentObservations(d.Name), d.ds, o.At) {
		r.t.Errorf("%s: %s", d.Name, p)
	}
	stack.WaitFor(r.t, stack.WaitDeadline, d.Name+"'s NoOverlapSlot to clear once its successor is ready", func() bool {
		var md v1alpha1.ModelDeployment
		if !r.get(&md, d.Name, stack.Namespace) {
			return false
		}
		for _, c := range md.Status.Conditions {
			if c.Type == v1alpha1.ConditionDegraded && c.Reason == v1alpha1.ReasonNoOverlapSlot {
				return false
			}
		}
		return true
	})
}

// noOverlapSlotProblems: the deployment is never Degraded True with reason
// NoOverlapSlot before the drain start; it is before the successor is ready,
// and on every version from the first until the successor is first seen
// ready (a NoOverlapSlot reason on a Degraded that is not True does not
// count); and it
// never shows ReplacementLate, in its reason or its message, which lists every
// reason that applies (HBEP-7, No-overlap mode and Events and conditions).
func noOverlapSlotProblems(obs []mdObservation, drainStart, succReady time.Time) []string {
	var out []string
	first := -1
	for i, o := range obs {
		slot := o.degraded == metav1.ConditionTrue && o.degradedReason == v1alpha1.ReasonNoOverlapSlot
		if o.degradedReason == v1alpha1.ReasonReplacementLate || hasWord(o.degradedMessage, v1alpha1.ReasonReplacementLate) {
			out = append(out, fmt.Sprintf("Degraded at %s names ReplacementLate (%s: %.200s): the gap is planned in no-overlap mode", ts(o.at), o.degradedReason, o.degradedMessage))
		}
		if slot && first < 0 {
			first = i
			if o.at.Before(drainStart.Add(-gapSlack)) {
				out = append(out, fmt.Sprintf("NoOverlapSlot first seen at %s, before the drain start %s", ts(o.at), ts(drainStart)))
			}
		}
		if first >= 0 && !slot && o.at.Before(succReady) {
			out = append(out, fmt.Sprintf("at %s, before the successor was ready (%s), Degraded is %s %q, not NoOverlapSlot", ts(o.at), ts(succReady), o.degraded, o.degradedReason))
		}
	}
	switch {
	case first < 0:
		out = append(out, "Degraded was never True with reason NoOverlapSlot")
	case !obs[first].at.Before(succReady):
		out = append(out, fmt.Sprintf("NoOverlapSlot first seen at %s, not before the successor was ready at %s", ts(obs[first].at), ts(succReady)))
	}
	return out
}

// checkQueuedHandover is the queued path's own half: the successor, created
// before the drain start and marked, never started before the predecessor's
// job ended (the GPU cap), and its Allocation is the one checkQueuedSuccessor
// saw pend.
func (p3 *part3) checkQueuedHandover(d *depRun) {
	r := p3.rig
	r.t.Helper()
	pj, _ := r.cl.Plat.Job(d.primaryJob)
	sj, _ := r.cl.Plat.Job(d.succJob)
	if sj.Start.Before(pj.End) || pj.End.IsZero() {
		r.t.Errorf("%s: the successor's job %d started %s, before the predecessor's job %d ended (%s): it ran beside it", d.Name, d.succJob, ts(sj.Start), d.primaryJob, ts(pj.End))
	}
	if !sj.Submit.Before(d.ds) {
		r.t.Errorf("%s: the successor's job %d was submitted at %s, not before the drain start %s: it did not pend through the gap", d.Name, d.succJob, ts(sj.Submit), ts(d.ds))
	}
	var a v1alpha1.Allocation
	if !r.get(&a, d.succAlloc, stack.Namespace) || a.Annotations[v1alpha1.AnnotationQueuedSuccessor] == "" {
		r.t.Errorf("%s: the successor's Allocation %s carries no queued-successor mark (HBEP-7, No-overlap mode)", d.Name, d.succAlloc)
	}
}

// checkOneSlot is the one-slot path's own half, read from the login node's
// record of the controller's commands (cmdlog_test.go): never two live jobs
// in its partition, judged at each sbatch; every submission accepted (a
// refused one is a try while the slot was taken); the successor submitted
// and its Allocation first seen only after the predecessor's job had left the
// slot; and its Allocation not marked.
func (p3 *part3) checkOneSlot(d *depRun) {
	r := p3.rig
	r.t.Helper()
	atts := r.cl.cmds.submissions(d.partition)
	lastLive, cmds := r.cl.cmds.lastHeld(d.partition, d.primaryJob)
	for _, p := range oneSlotProblems(r.cl.Plat.Calls(), r.cl.Plat.Jobs(), atts, d.partition, d.succJob, lastLive) {
		r.t.Errorf("%s: %s", d.Name, p)
	}
	var a v1alpha1.Allocation
	if r.get(&a, d.succAlloc, stack.Namespace) && a.Annotations[v1alpha1.AnnotationQueuedSuccessor] != "" {
		r.t.Errorf("%s: the one-slot successor's Allocation %s carries the queued-successor mark %q", d.Name, d.succAlloc, a.Annotations[v1alpha1.AnnotationQueuedSuccessor])
	}
	if aw, ok := p3.allocFirstSeen(d.succAlloc); ok && !aw.After(lastLive) {
		r.t.Errorf("%s: the successor's Allocation %s was first seen at %s, while the predecessor's job %d still held the slot (at the command of %s)", d.Name, d.succAlloc, ts(aw), d.primaryJob, ts(lastLive))
	}
	held := 0
	for _, x := range atts {
		held = max(held, len(x.holders))
	}
	r.t.Logf("%s: %d sbatch attempts on the partition, each read with the slot's holders just before it ran (at most %d other job held it then); %d commands recorded; the predecessor's job %d last held the slot at the command of %s",
		d.Name, len(atts), held, cmds, d.primaryJob, ts(lastLive))
}

// allocFirstSeen is when the watch first saw the named Allocation.
func (p3 *part3) allocFirstSeen(name string) (time.Time, bool) {
	for _, a := range p3.watch.Allocations() {
		if a.Name == name {
			return a.At, true
		}
	}
	return time.Time{}, false
}

// oneSlotProblems holds the one-slot partition to HBEP-7 from the fake's
// call log and jobs and the login node's record of each sbatch attempt:
//   - every sbatch was accepted: each attempt exited 0 and printed a job ID,
//     and no name was submitted more times than it has jobs (a refused try
//     retried under the same tag);
//   - no job was submitted while another held the slot: just before each
//     accepted sbatch ran, no job of the partition was pending, running or
//     completing. Only an sbatch adds a slot holder, and a holder only ever
//     leaves (no job of the partition is requeued, which is checked), so the
//     state before each sbatch is the most jobs the partition ever held: two
//     live jobs at any moment would show at the sbatch that made the second;
//   - every job of the partition came from a recorded attempt;
//   - the successor's job was submitted after the predecessor's job was last
//     seen holding the slot.
func oneSlotProblems(calls []fakeslurm.Call, jobs []fakeslurm.Job, attempts []submitAttempt, partition string, succJob int, predLastLive time.Time) []string {
	var out []string
	jobsOf := map[string]int{}
	var mine []fakeslurm.Job
	for _, j := range jobs {
		if j.Partition == partition {
			jobsOf[j.Name]++
			mine = append(mine, j)
			if j.RestartCount > 0 {
				out = append(out, fmt.Sprintf("job %d was requeued (%d restarts): a requeue adds a slot holder no sbatch shows", j.ID, j.RestartCount))
			}
		}
	}
	callsOf := map[string]int{}
	var names []string
	for _, c := range calls {
		if len(c.Words) == 0 || c.Words[0] != "sbatch" || !slices.Contains(c.Words, "--partition="+partition) {
			continue
		}
		name := ""
		for _, w := range c.Words {
			if v, ok := strings.CutPrefix(w, "--job-name="); ok {
				name = v
			}
		}
		if callsOf[name] == 0 {
			names = append(names, name)
		}
		callsOf[name]++
	}
	for _, n := range names {
		if callsOf[n] > jobsOf[n] {
			out = append(out, fmt.Sprintf("%d sbatch calls for %q registered %d jobs: a submission was refused while the slot was taken", callsOf[n], n, jobsOf[n]))
		}
	}
	accepted := 0
	for _, a := range attempts {
		switch {
		case a.err != "" || a.exit != 0 || a.jobID == 0:
			out = append(out, fmt.Sprintf("the sbatch at %s for %q was refused: exit %d, job %d, %s", ts(a.at), a.name, a.exit, a.jobID, a.err))
		case len(a.holders) > 0:
			accepted++
			out = append(out, fmt.Sprintf("the sbatch at %s for %q registered job %d while job %v held the slot (pending, running or completing): two live jobs on the one-slot path",
				ts(a.at), a.name, a.jobID, a.holders))
		default:
			accepted++
		}
	}
	if accepted != len(mine) {
		out = append(out, fmt.Sprintf("the partition has %d jobs, and %d accepted sbatch attempts were recorded: a submission went unrecorded", len(mine), accepted))
	}
	for _, j := range mine {
		if j.ID == succJob && !j.Submit.After(predLastLive) {
			out = append(out, fmt.Sprintf("the successor's job %d was submitted at %s, while the predecessor's job still held the slot (seen at %s)", j.ID, ts(j.Submit), ts(predLastLive)))
		}
	}
	return out
}
