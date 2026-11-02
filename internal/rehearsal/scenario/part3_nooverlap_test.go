package scenario_test

// Item 5 of issue #193 (the science agent's comment of 1 Nov 2026): HBEP-7's
// no-overlap handover end to end over the real tunnel, on the queued path and
// the one-slot path at once. A stream is in flight at each primary's drain
// start and runs to its end uncut; paced requests through each gap get 503
// no_ready_target; NoOverlapSlot holds from the drain start until the
// successor is ready; the gap is measured; the one-slot path never has two
// live jobs; and the queued successor pends on the GPU cap.

import (
	"fmt"
	"net/http"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
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
	stream    <-chan streamed // the stream in flight at the drain start
	probes    []probe
}

// probe is one paced request of the gap prober.
type probe struct {
	model string
	rp    reply
}

// prober sends one short request for each model every probeEvery until it is
// stopped, and keeps every reply. It never fails the test itself: the replies
// are judged after (gapProblems).
type prober struct {
	stop chan struct{}
	done chan struct{}
	mu   sync.Mutex
	got  []probe
}

const probeEvery = time.Second

func (p3 *part3) startProber(models ...string) *prober {
	pr := &prober{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(pr.done)
		for {
			for _, m := range models {
				rp := p3.send("/v1/chat/completions", chatBodyFor(m, 1, 0, false), "")
				pr.mu.Lock()
				pr.got = append(pr.got, probe{model: m, rp: rp})
				pr.mu.Unlock()
			}
			select {
			case <-pr.stop:
				return
			case <-time.After(probeEvery):
			}
		}
	}()
	return pr
}

// replies is a copy of the replies so far.
func (pr *prober) replies() []probe {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return slices.Clone(pr.got)
}

// end stops the prober and returns every reply; a request in flight ends
// within requestTimeout.
func (pr *prober) end() []probe {
	close(pr.stop)
	<-pr.done
	return pr.replies()
}

// jobSampler records the jobs of one partition every 100 ms: the most live
// jobs seen at once, and for each job the last time it was seen holding its
// slot (pending, running, or ended and still completing). A sample is a lower
// bound on when a job left its slot, so a check against it never fails a
// correct controller.
type jobSampler struct {
	stop, done chan struct{}
	mu         sync.Mutex
	samples    int
	maxLive    int
	maxLiveAt  time.Time
	maxLiveIDs []int
	lastLive   map[int]time.Time
}

func (p3 *part3) startJobSampler(partition string) *jobSampler {
	s := &jobSampler{stop: make(chan struct{}), done: make(chan struct{}), lastLive: map[int]time.Time{}}
	go func() {
		defer close(s.done)
		for {
			at := time.Now()
			var live []int
			for _, j := range p3.cl.plat.Jobs() {
				if j.Partition == partition && holdsSlot(j) {
					live = append(live, j.ID)
				}
			}
			s.mu.Lock()
			s.samples++
			for _, id := range live {
				s.lastLive[id] = at
			}
			if len(live) > s.maxLive {
				s.maxLive, s.maxLiveAt, s.maxLiveIDs = len(live), at, live
			}
			s.mu.Unlock()
			select {
			case <-s.stop:
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	return s
}

func (s *jobSampler) end() {
	close(s.stop)
	<-s.done
}

// holdsSlot reports whether a job holds its partition's submit slot as HBEP-7
// counts it for the one-slot path: pending (held or not), running, or ended
// and still completing.
func holdsSlot(j fakeslurm.Job) bool {
	switch j.State {
	case "PENDING", "RUNNING":
		return true
	}
	return j.Completing || !j.Finished
}

// noOverlap is item 5: both deployments' handovers, from a stream started
// streamLead before each drain start to each successor's ready.
func (p3 *part3) noOverlap() {
	r := p3.rig
	r.t.Helper()
	order := slices.Clone(p3.deps)
	sort.Slice(order, func(i, j int) bool { return order[i].ds.Before(order[j].ds) })
	p3.atLeastBefore(order[0], "the no-overlap phase", streamLead+10*time.Second)
	var models []string
	for _, d := range p3.deps {
		models = append(models, d.model)
		if d.oneSlot {
			// Before its drain start the one-slot path has no successor at
			// all: no Allocation is created while the slot is held.
			if n := len(p3.allocationsOf(d.name)); n != 1 {
				r.t.Errorf("%s: %d Allocations before the drain start, want the primary's alone (HBEP-7, one-slot path)", d.name, n)
			}
		}
	}
	smp := p3.startJobSampler(oneSlotDep.partition)
	pr := p3.startProber(models...)
	for _, d := range order {
		waitFor(r.t, time.Until(d.ds)+time.Minute, "the drain start of "+d.name+" minus the stream's lead", func() bool {
			return !time.Now().Before(d.ds.Add(-streamLead))
		})
		first, done := r.startStream(chatBodyFor(d.model, longTokens, longTokens, true), "rehearsal-part3-drain-"+d.name)
		<-first
		d.stream = done
		r.t.Logf("%s: a %d-token stream started %s before the drain start", d.name, longTokens, time.Until(d.ds).Round(100*time.Millisecond))
	}
	for _, d := range p3.deps {
		p3.waitSuccessor(d, pr)
	}
	probes := pr.end()
	smp.end()
	for _, d := range p3.deps {
		for _, pb := range probes {
			if pb.model == d.model {
				d.probes = append(d.probes, pb)
			}
		}
		p3.checkDrainStream(d)
		p3.checkGap(d)
		p3.checkNoOverlapSlot(d)
		if d.oneSlot {
			p3.checkOneSlot(d, smp)
		} else {
			p3.checkQueuedHandover(d)
		}
	}
}

// waitSuccessor waits until the primary's successor is ready, registered at
// the front, and has served one of the prober's requests.
func (p3 *part3) waitSuccessor(d *depRun, pr *prober) {
	r := p3.rig
	r.t.Helper()
	waitState(r.t, waitDeadline+2*time.Minute, "the successor of "+d.primary.Name+" to serve through the front", func() (bool, string) {
		var succ *v1alpha1.Instance
		for _, in := range p3.instancesOf(d.name) {
			if in.Annotations[v1alpha1.AnnotationPredecessor] == d.primary.Name && in.Status.Phase == v1alpha1.InstanceReady {
				in := in
				succ = &in
			}
		}
		if succ == nil {
			return false, r.describe()
		}
		if reg, known := r.registered(succ); !known || !reg {
			return false, "the successor " + succ.Name + " is ready but not registered at the front"
		}
		for _, pb := range pr.replies() {
			if pb.model == d.model && pb.rp.status == http.StatusOK && pb.rp.header.Get("Hpcburst-Instance-Uid") == string(succ.UID) {
				d.succ, d.succAlloc = succ, succ.Spec.AllocationRef.Name
				d.succJob = jobIDOf(r.t, succ)
				return true, ""
			}
		}
		return false, "the successor " + succ.Name + " has served no probe yet"
	})
	r.t.Logf("%s: successor %s serves on job %d", d.name, d.succ.Name, d.succJob)
}

// checkDrainStream: the stream in flight at the drain start ran to its end,
// with its finish reason and [DONE], never cut, across the drain start.
func (p3 *part3) checkDrainStream(d *depRun) {
	r := p3.rig
	r.t.Helper()
	var s streamed
	select {
	case s = <-d.stream:
	case <-time.After(requestTimeout):
		r.t.Fatalf("%s: the stream in flight at the drain start did not end within %s", d.name, requestTimeout)
	}
	row := r.checkStream("drain "+d.name, s, d.primary, longTokens)
	p3.attribute("the stream in flight at "+d.name+"'s drain start", s.reply, row, wantInstance(d, d.primary))
	rs, re := parseRecordTime(row.StartTime), parseRecordTime(row.EndTime)
	if !rs.Before(d.ds) || !d.ds.Before(re) {
		r.t.Errorf("%s: no stream in flight at the drain start %s: its record runs %s to %s", d.name, d.ds.UTC().Format(time.RFC3339), row.StartTime, row.EndTime)
	}
	r.t.Logf("%s: the drain-start stream ran %s to %s, %s past the drain start; finish %s, cut by %s", d.name, row.StartTime, row.EndTime,
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
	if o, ok := r.watch.first(d.succ.UID, time.Time{}, func(o observation) bool { return o.phase == v1alpha1.InstanceReady }); ok {
		in.succReadySeen = o.at
	}
	for _, pb := range d.probes {
		if pb.rp.err != nil {
			in.probes = append(in.probes, gapProbe{err: pb.rp.err.Error()})
			continue
		}
		row := r.rowOf(pb.rp.id)
		p3.attribute("a probe of "+d.name, pb.rp, row, p3.wantForProbe(d, row))
		e := errorOf(pb.rp.body)
		in.probes = append(in.probes, gapProbe{start: parseRecordTime(row.StartTime), status: pb.rp.status, emitter: row.Emitter,
			instanceUID: row.InstanceUID, code: e.Code, detail: derefS(e.Detail), retryAfter: pb.rp.header.Get("Retry-After")})
	}
	rep, problems := gapProblems(in)
	for _, p := range problems {
		r.t.Errorf("%s: %s", d.name, p)
	}
	o, _ := r.watch.first(d.primary.UID, time.Time{}, func(o observation) bool { return o.drainBegan != "" })
	began, _ := time.Parse(time.RFC3339, o.drainBegan)
	pj, _ := r.cl.plat.Job(d.primaryJob)
	sj, _ := r.cl.plat.Job(d.succJob)
	r.t.Logf("GAP %s: %d requests in the gap, all 503 no_ready_target; first 503 %s after the drain start, first served by the successor %s after it; "+
		"the controller: drain recorded %s, successor ready %s (%s); the fake: predecessor's job ended %s, successor's started %s (%s queue wait after the GPU or slot freed)",
		d.name, rep.inGap, rep.first503.Sub(d.ds).Round(100*time.Millisecond), rep.firstSucc.Sub(d.ds).Round(100*time.Millisecond),
		began.UTC().Format(time.RFC3339), in.succReady.UTC().Format(time.RFC3339), in.succReady.Sub(began).Round(time.Second),
		pj.End.UTC().Format(time.RFC3339Nano), sj.Start.UTC().Format(time.RFC3339Nano), sj.Start.Sub(pj.End).Round(100*time.Millisecond))
}

// wantForProbe is what a probe's reply must carry: an Instance's attribution
// when one served it, else the front's error before a target.
func (p3 *part3) wantForProbe(d *depRun, row usageRow) wantAttr {
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
	o, _ := r.watch.first(d.succ.UID, time.Time{}, func(o observation) bool { return o.phase == v1alpha1.InstanceReady })
	for _, p := range noOverlapSlotProblems(r.watch.deployment(d.name), d.ds, o.at) {
		r.t.Errorf("%s: %s", d.name, p)
	}
	waitFor(r.t, waitDeadline, d.name+"'s NoOverlapSlot to clear once its successor is ready", func() bool {
		var md v1alpha1.ModelDeployment
		if !r.get(&md, d.name, namespace) {
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

// noOverlapSlotProblems: the deployment never shows NoOverlapSlot before the
// drain start; it shows it before the successor is ready, and on every
// version from its first until the successor is first seen ready; and it
// never shows ReplacementLate, in its reason or its message, which lists every
// reason that applies (HBEP-7, No-overlap mode and Events and conditions).
func noOverlapSlotProblems(obs []mdObservation, drainStart, succReady time.Time) []string {
	var out []string
	first := -1
	for i, o := range obs {
		slot := o.degradedReason == v1alpha1.ReasonNoOverlapSlot
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
		out = append(out, "Degraded never showed NoOverlapSlot")
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
	pj, _ := r.cl.plat.Job(d.primaryJob)
	sj, _ := r.cl.plat.Job(d.succJob)
	if sj.Start.Before(pj.End) || pj.End.IsZero() {
		r.t.Errorf("%s: the successor's job %d started %s, before the predecessor's job %d ended (%s): it ran beside it", d.name, d.succJob, ts(sj.Start), d.primaryJob, ts(pj.End))
	}
	if !sj.Submit.Before(d.ds) {
		r.t.Errorf("%s: the successor's job %d was submitted at %s, not before the drain start %s: it did not pend through the gap", d.name, d.succJob, ts(sj.Submit), ts(d.ds))
	}
	var a v1alpha1.Allocation
	if !r.get(&a, d.succAlloc, namespace) || a.Annotations[v1alpha1.AnnotationQueuedSuccessor] == "" {
		r.t.Errorf("%s: the successor's Allocation %s carries no queued-successor mark (HBEP-7, No-overlap mode)", d.name, d.succAlloc)
	}
}

// checkOneSlot is the one-slot path's own half: never two live jobs in its
// partition; the successor submitted only after the predecessor's job had
// left the slot, and every submission accepted (a refused one is a try while
// the slot was taken); and its Allocation not marked.
func (p3 *part3) checkOneSlot(d *depRun, smp *jobSampler) {
	r := p3.rig
	r.t.Helper()
	smp.mu.Lock()
	maxLive, at, ids, samples, lastLive := smp.maxLive, smp.maxLiveAt, smp.maxLiveIDs, smp.samples, smp.lastLive[d.primaryJob]
	smp.mu.Unlock()
	if maxLive > 1 {
		r.t.Errorf("%s: %d live jobs %v at once at %s on the one-slot path", d.name, maxLive, ids, ts(at))
	}
	for _, p := range oneSlotProblems(r.cl.plat.Calls(), r.cl.plat.Jobs(), d.partition, d.succJob, lastLive) {
		r.t.Errorf("%s: %s", d.name, p)
	}
	var a v1alpha1.Allocation
	if r.get(&a, d.succAlloc, namespace) && a.Annotations[v1alpha1.AnnotationQueuedSuccessor] != "" {
		r.t.Errorf("%s: the one-slot successor's Allocation %s carries the queued-successor mark %q", d.name, d.succAlloc, a.Annotations[v1alpha1.AnnotationQueuedSuccessor])
	}
	if aw, ok := p3.allocFirstSeen(d.succAlloc); ok && !aw.After(lastLive) {
		r.t.Errorf("%s: the successor's Allocation %s was first seen at %s, while the predecessor's job %d still held the slot (seen at %s)", d.name, d.succAlloc, ts(aw), d.primaryJob, ts(lastLive))
	}
	r.t.Logf("%s: %d samples of the partition's jobs, at most %d live at once; the predecessor's job %d last seen holding the slot at %s",
		d.name, samples, maxLive, d.primaryJob, ts(lastLive))
}

// allocFirstSeen is when the watch first saw the named Allocation.
func (p3 *part3) allocFirstSeen(name string) (time.Time, bool) {
	for _, a := range p3.watch.allocations() {
		if a.name == name {
			return a.at, true
		}
	}
	return time.Time{}, false
}

// oneSlotProblems reads the fake's call log and jobs: every sbatch to the
// partition registered a job (none was refused for the slot); jobs' lives,
// from submission to end, never overlap; and the successor's job was
// submitted after the predecessor's job was last seen holding the slot.
func oneSlotProblems(calls []fakeslurm.Call, jobs []fakeslurm.Job, partition string, succJob int, predLastLive time.Time) []string {
	var out []string
	names := map[string]bool{}
	var mine []fakeslurm.Job
	for _, j := range jobs {
		if j.Partition == partition {
			names[j.Name] = true
			mine = append(mine, j)
		}
	}
	for _, c := range calls {
		if len(c.Words) == 0 || c.Words[0] != "sbatch" || !slices.Contains(c.Words, "--partition="+partition) {
			continue
		}
		name := ""
		for _, w := range c.Words {
			if len(w) > len("--job-name=") && w[:len("--job-name=")] == "--job-name=" {
				name = w[len("--job-name="):]
			}
		}
		if !names[name] {
			out = append(out, fmt.Sprintf("an sbatch at %s for %q registered no job: refused while the slot was taken", ts(c.Time), name))
		}
	}
	sort.Slice(mine, func(i, j int) bool { return mine[i].Submit.Before(mine[j].Submit) })
	for i := 1; i < len(mine); i++ {
		prev, cur := mine[i-1], mine[i]
		if prev.End.IsZero() || cur.Submit.Before(prev.End) {
			out = append(out, fmt.Sprintf("job %d was submitted at %s while job %d (ended %s) was live", cur.ID, ts(cur.Submit), prev.ID, ts(prev.End)))
		}
	}
	for _, j := range mine {
		if j.ID == succJob && !j.Submit.After(predLastLive) {
			out = append(out, fmt.Sprintf("the successor's job %d was submitted at %s, while the predecessor's job still held the slot (seen at %s)", j.ID, ts(j.Submit), ts(predLastLive)))
		}
	}
	return out
}
