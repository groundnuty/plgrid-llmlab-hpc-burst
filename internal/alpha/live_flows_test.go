package alpha

// The live steps' flows (issue #245): the handover on Athena, no-overlap on
// Helios and the tunnel hour, each driving the stack liveRun started (in its
// world) and returning what it saw. Nothing here judges: Run does.

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/frpsplugin"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// srunTimeout bounds one srun step of the worker fault, the ssh call included.
const srunTimeout = 90 * time.Second

// stream is one streamed request the run sent, and its outcome once it ends.
type stream struct {
	first <-chan struct{}
	done  <-chan stack.Streamed
}

// openStream sends a streamed request of n tokens with the key, and pad as a
// further message when not empty.
func (r *liveRun) openStream(n int, key, pad string) stream {
	f, d := r.fc.StartStream(stack.ChatBody(r.fc.Model, n, n, true, pad), key)
	return stream{first: f, done: d}
}

// wait is a stream's end as the client read it.
func (r *liveRun) wait(s stream) stack.Streamed {
	select {
	case out := <-s.done:
		if out.Header != nil {
			r.keep(out.Header)
		}
		return out
	case <-time.After(r.fc.RequestTimeout() + time.Minute):
		r.t.Fatalf("a stream did not end within %s", r.fc.RequestTimeout()+time.Minute)
	}
	return stack.Streamed{}
}

// observe is a stream's end as an observation: the front's usage record and what the client read.
func (r *liveRun) observe(out stack.Streamed) Stream {
	if out.ID == "" {
		return Stream{Error: "no reply"}
	}
	done, finished := stack.SSEDone(out.Body)
	return StreamOf(r.fc.RowOf(out.ID), ClientEnd{Done: done, Finished: finished, ErrorCode: stack.StreamErrorCode(out.Body)})
}

// finish waits for a stream and observes it.
func (r *liveRun) finish(s stream) Stream { return r.observe(r.wait(s)) }

// rate is the engine's token rate through the front, from one stream, so that
// the run sizes a request to a length.
func (r *liveRun) rate(key string) float64 {
	const n = 200
	s := r.finish(r.openStream(n, key, ""))
	if !s.Complete || s.Tokens <= 0 || !s.Ended.After(s.Opened) {
		r.t.Fatalf("the rate stream did not complete: %+v", s)
	}
	return float64(s.Tokens) / s.Ended.Sub(s.Opened).Seconds()
}

// tokensFor is how many tokens make a stream last d at rate, within the
// deployment's max-model-len less a margin for the prompt.
func (r *liveRun) tokensFor(d time.Duration, rate float64) (int, error) {
	n := max(int(rate*d.Seconds()), 1)
	maxLen := 4096
	for _, f := range r.w.md.Spec.Source.Served.Flags {
		if f.Name == "max-model-len" {
			fmt.Sscan(f.Value, &maxLen)
		}
	}
	if n+512 > maxLen {
		return 0, fmt.Errorf("%w: %d tokens for %s at %.0f tokens/s, max-model-len %d", ErrRequestTooLong, n, d, rate, maxLen)
	}
	return n, nil
}

// current reads an Instance again from the API.
func (r *liveRun) current(in v1alpha1.Instance) v1alpha1.Instance {
	var x v1alpha1.Instance
	ctx, cancel := stack.CallCtx(r.t)
	defer cancel()
	if err := r.c.Get(ctx, client.ObjectKeyFromObject(&in), &x); err != nil {
		r.t.Fatalf("Instance %s: %v", in.Name, err)
	}
	return x
}

// ourJob is the job of the Instance, checked to be the run's own right now:
// its ID is our Allocation's (and the Instance's), the Allocation's tag is
// one the run wrote down, and the queue's --me listing shows that ID running
// under that tag. Anything else fails the row (the science agent's condition 1).
func (r *liveRun) ourJob(ctx context.Context, in v1alpha1.Instance) string {
	t := r.t
	t.Helper()
	var a v1alpha1.Allocation
	cctx, cancel := stack.CallCtx(t)
	err := r.c.Get(cctx, client.ObjectKey{Name: in.Spec.AllocationRef.Name, Namespace: in.Namespace}, &a)
	cancel()
	if err != nil {
		t.Fatalf("worker fault: the Instance's Allocation: %v", err)
	}
	id, tag := a.Status.AtProvider.JobID, v1alpha1.JobTag(a.Namespace, a.Name, a.UID)
	if id == "" || id != in.Status.JobID {
		t.Fatal("worker fault: the Instance's job is not its Allocation's")
	}
	if !r.tags.Tagged(tag) {
		t.Fatal("worker fault: the Allocation's tag is not one the run wrote down")
	}
	j, listed, err := r.batch.QueueJob(ctx, id)
	if err != nil || !listed || j.Name != tag || j.Phase != v1alpha1.AllocationRunning {
		t.Fatalf("worker fault: the queue's --me listing does not show the run's job running under its tag (listed %v, phase %s, read error %v)", listed, j.Phase, err != nil)
	}
	return id
}

// workerRestart is the worker-rung fault: it kills the engine of the
// Instance's job on its node, by PID, and waits for the stack to restart the
// worker and for the Instance to serve the key again (HBEP-7:637).
func (r *liveRun) workerRestart(ctx context.Context, p Plan, in v1alpha1.Instance, key string) RestartTimes {
	t := r.t
	t.Helper()
	before := len(r.current(in).Status.Restarts)
	jobID := r.ourJob(ctx, in)
	line, err := SrunLine(jobID)
	if err != nil {
		t.Fatalf("worker fault: %v", err)
	}
	res, err := r.tr.Run(ctx, slurm.Command{Line: line, Stdin: []byte(ListScript(jobID)), Timeout: srunTimeout})
	if err != nil || res.ExitCode != 0 {
		t.Fatalf("worker fault: the srun step that lists the job's processes failed (exit %d, transport error %v)", res.ExitCode, err != nil)
	}
	procs, err := ParseListing(res.Stdout)
	if err != nil {
		t.Fatalf("worker fault: %v", err)
	}
	eng, err := PickEngine(procs, in.Spec.Launch.Command)
	if err != nil {
		t.Fatalf("worker fault: %v", err)
	}
	r.ourJob(ctx, in) // again, right before the kill
	res, err = r.tr.Run(ctx, slurm.Command{Line: line, Stdin: []byte(KillScript(eng)), Timeout: srunTimeout})
	if err != nil || res.ExitCode != 0 || !Killed(res.Stdout, eng) {
		t.Fatalf("worker fault: the srun step that kills the engine did not (exit %d, transport error %v, said %q)", res.ExitCode, err != nil, res.Stdout)
	}
	times := RestartTimes{Killed: time.Now()}
	p.Logf("%s: killed the engine of the run's job by its PID (command digest %s)", p.Row.Name, CommandDigest(eng.Argv))
	stack.WaitFor(t, r.tm.StartTimeout+5*time.Minute, "the worker restart and the Instance serving the key again", func() bool {
		now := r.current(in)
		if len(now.Status.Restarts) <= before || now.Status.Phase != v1alpha1.InstanceReady {
			return false
		}
		rp := r.fc.Send("/v1/chat/completions", stack.ChatBody(r.fc.Model, 1, 0, false, ""), key)
		if rp.Header != nil {
			r.keep(rp.Header)
		}
		if rp.Err != nil || rp.Status != http.StatusOK || rp.Header.Get("Hpcburst-Instance-Uid") != string(in.UID) {
			return false
		}
		times.ReadyAgain = time.Now()
		times.Restarted = now.Status.Restarts[before].Time.Time
		return true
	})
	if o, ok := r.watch.First(in.UID, times.Killed.Add(-time.Second), func(o stack.Observation) bool { return healthyPartExited(o.Instance) }); ok {
		times.AgentNoticed = o.At
	}
	p.Logf("%s: %s", p.Row.Name, times.Line())
	return times
}

// streamLead is how long before the drain start the streams that cross it
// open: a third of the deadline, so that each runs a third past it and ends
// well within the deadline.
func (r *liveRun) streamLead() time.Duration { return r.tm.MaxRequestDuration / 3 }

// untilBefore waits until lead before t, failing when that has passed.
func (r *liveRun) untilBefore(t time.Time, lead time.Duration, what string) {
	at := t.Add(-lead)
	if time.Now().After(at) {
		r.t.Fatalf("%s: the time to open the streams, %s before the drain start, has passed: the steps before it took too long for the lease", what, lead)
	}
	stack.WaitFor(r.t, time.Until(at)+time.Minute, what, func() bool { return !time.Now().Before(at) })
}

// Handover is alpha-athena-handover: a worker-rung restart on the primary,
// then two streams in flight across its drain start while its replacement
// takes over.
func (r *liveRun) Handover(ctx context.Context, p Plan) (HandoverObs, error) {
	r.start(p)
	pre := r.firstReady(p)
	key := r.fc.KeyFor(&pre)
	rate := r.rate(key)
	times := r.workerRestart(ctx, p, pre, key)
	pre = r.current(pre)
	ds, ok := DrainStartOf(&pre, r.drainWindow())
	if !ok {
		r.t.Fatal("the primary has no lease end")
	}
	// HBEP-7, Handover: the replacement is created at its planned time and is ready before the drain start.
	succ, ok := r.successorOf(pre.Name, time.Until(ds))
	if !ok {
		p.Logf("%s: no replacement was ready by the drain start", p.Row.Name)
	}
	key = r.fc.KeyFor(&pre) // the ring as it is now, with both Instances
	lead := r.streamLead()
	n, err := r.tokensFor(2*lead, rate)
	if err != nil {
		return HandoverObs{}, err
	}
	r.untilBefore(ds, lead, "the streams across the drain start")
	s1 := r.openStream(n, key, "")
	<-s1.first
	s2 := r.openStream(n, key, "")
	streams := []Stream{r.finish(s1), r.finish(s2)}
	stack.WaitFor(r.t, r.drainWindow()+5*time.Minute, "the primary to end", func() bool {
		_, ended := r.watch.First(pre.UID, time.Time{}, func(o stack.Observation) bool { return o.Phase == v1alpha1.InstanceEnded })
		return ended
	})
	tags, err := r.runTags()
	if err != nil {
		return HandoverObs{}, err
	}
	responses, attributed := r.attribution()
	preLast, succLast := r.last(pre), r.last(succ)
	return HandoverObs{
		Tags: tags, Streams: streams,
		Instances:  []InstanceSpan{SpanOf(&preLast, r.tagOf(pre)), SpanOf(&succLast, r.tagOf(succ))},
		Deployment: r.observedDeployment(), DrainStart: ds,
		Handovers:      Handovers(stack.Events(r.t, r.c), []v1alpha1.Instance{preLast}),
		WorkerRestarts: WorkerRestarts(r.watch.Last()), Responses: responses, Attributed: attributed, WorkerRestart: times,
	}, nil
}

// NoOverlap is alpha-helios-nooverlap: a worker-rung restart on the primary;
// the replacement queued ahead, pending on the QOS's GPU cap; a stream in
// flight at the drain start; and the gap until the successor is ready, with
// paced requests through it.
func (r *liveRun) NoOverlap(ctx context.Context, p Plan) (NoOverlapObs, error) {
	r.start(p)
	pre := r.firstReady(p)
	key := r.fc.KeyFor(&pre)
	rate := r.rate(key)
	times := r.workerRestart(ctx, p, pre, key)
	pre = r.current(pre)
	ds, ok := DrainStartOf(&pre, r.drainWindow())
	if !ok {
		r.t.Fatal("the primary has no lease end")
	}
	lead := r.streamLead()
	n, err := r.tokensFor(2*lead, rate)
	if err != nil {
		return NoOverlapObs{}, err
	}
	r.untilBefore(ds, lead, "the stream across the drain start")
	s := r.openStream(n, key, "")
	<-s.first
	pr := r.startProber(ds)
	// The successor starts once the predecessor's job ends and frees the GPU, then becomes ready.
	succ, ok := r.successorOf(pre.Name, time.Until(pre.Status.LeaseEnd.Time)+p.PendingMax+r.tm.StartTimeout+5*time.Minute)
	noReady := pr.end()
	streams := []Stream{r.finish(s)}
	if !ok {
		p.Logf("%s: the successor was not ready in time", p.Row.Name)
	}
	tags, err := r.runTags()
	if err != nil {
		return NoOverlapObs{}, err
	}
	o := NoOverlapObs{Tags: tags, Streams: streams, Deployment: r.observedDeployment(), DrainStart: ds,
		WorkerRestarts: WorkerRestarts(r.watch.Last()), NoOverlapSlotSeen: SlotSeen(r.watch.Deployments()), NoReadyTarget: noReady, WorkerRestart: times}
	if ok && succ.Status.ReadyTime != nil {
		o.SuccessorReady = succ.Status.ReadyTime.Time
	}
	return o, nil
}

// prober sends one short request a second from a time on, and counts the
// replies the front gave with 503 no_ready_target (recorded, not judged).
type prober struct {
	stop, done chan struct{}
	noReady    int
}

func (r *liveRun) startProber(from time.Time) *prober {
	pr := &prober{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(pr.done)
		select {
		case <-pr.stop:
			return
		case <-time.After(time.Until(from)):
		}
		for {
			rp := r.fc.Send("/v1/chat/completions", stack.ChatBody(r.fc.Model, 1, 0, false, ""), "")
			if rp.Header != nil {
				r.keep(rp.Header)
			}
			if rp.Status == http.StatusServiceUnavailable && stack.ErrorOf(rp.Body).Code == "no_ready_target" {
				pr.noReady++
			}
			select {
			case <-pr.stop:
				return
			case <-time.After(time.Second):
			}
		}
	}()
	return pr
}

// end stops the prober and returns its count.
func (pr *prober) end() int {
	close(pr.stop)
	<-pr.done
	return pr.noReady
}

// tunnelBodyPad is the further message the hour's requests carry. On main it
// is none: issue #224 (PR #244) adds the large request bodies (MinLargeBody
// bytes, among the streams), their sizes (Stream.BodyBytes) and the upload
// direction's rate (TunnelHourObs.Rates); they are wired here, in TunnelHour's
// traffic and its rate, when that merges.
const tunnelBodyPad = ""

// sampler looks at the tunnel session of an Instance every interval: frps's
// proxy list for its lastStartTime and the API for its TunnelConnected. It
// never fails the test from its goroutine: a look it cannot take is a look
// with no lastStartTime, which the verdict fails.
type sampler struct {
	stop, done chan struct{}
	mu         sync.Mutex
	samples    []TunnelSample
}

func (r *liveRun) startSampler(in v1alpha1.Instance, every time.Duration) *sampler {
	rd, err := frpsplugin.NewHostReader(r.h.AgentHost())
	if err != nil {
		r.t.Fatalf("frps's proxy list: %v", err)
	}
	s := &sampler{stop: make(chan struct{}), done: make(chan struct{})}
	look := func() {
		at := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		list, _ := rd.List(ctx)
		var x v1alpha1.Instance
		if r.c.Get(ctx, client.ObjectKeyFromObject(&in), &x) != nil {
			x = v1alpha1.Instance{}
			x.UID = in.UID
		}
		smp := TunnelSampleOf(at, list, &x)
		s.mu.Lock()
		s.samples = append(s.samples, smp)
		s.mu.Unlock()
	}
	look()
	go func() {
		defer close(s.done)
		for {
			select {
			case <-s.stop:
				look()
				return
			case <-time.After(every):
				look()
			}
		}
	}()
	return s
}

// end takes a last look, stops the sampler and returns every look.
func (s *sampler) end() []TunnelSample {
	close(s.stop)
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]TunnelSample(nil), s.samples...)
}

// endDeployment deletes the run's deployment, and waits until the cluster's
// record shows every job of the run ended.
func (r *liveRun) endDeployment(ctx context.Context, p Plan) {
	t := r.t
	t.Helper()
	cctx, cancel := stack.CallCtx(t)
	err := r.c.Delete(cctx, &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Name: r.w.md.Name, Namespace: r.w.md.Namespace}})
	cancel()
	if err != nil {
		t.Fatalf("delete the run's deployment: %v", err)
	}
	led := r.Ledger()
	stack.WaitFor(t, r.drainWindow()+5*time.Minute, "every job of the run to end in the cluster's record", func() bool {
		tags, _ := r.runTags()
		jobs, err := led.Jobs(ctx, tags)
		if err != nil || len(jobs) == 0 {
			return false
		}
		for _, j := range jobs {
			if j.EndTime == nil {
				return false
			}
		}
		return true
	})
	p.Logf("%s: the deployment is deleted and its jobs ended", p.Row.Name)
}

// rateAsked is S1's path-rate measurement's length: at least 30 s asked
// (spikes/s1-frp/README.md:288, C6).
const rateAsked = 30 * time.Second

// TunnelHour is tunnel-hour-*: S1's hour over this wiring. With a look at the
// tunnel session every quarter of MaxSampleGap, it sends requests back to back
// near the deadline; a burst of ConcurrentStreams streams, inside which it
// measures the path rate in the token direction with one more stream asked for
// 30 s; an idle stretch longer than MinIdleGap; and requests back to back again
// until TunnelHour has run.
func (r *liveRun) TunnelHour(ctx context.Context, p Plan) (TunnelHourObs, error) {
	r.start(p)
	in := r.firstReady(p)
	key := r.fc.KeyFor(&in)
	rate := r.rate(key)
	tm := r.tm
	long := tm.NearDeadline + (tm.MaxRequestDuration-tm.NearDeadline)/2
	nLong, err := r.tokensFor(long, rate)
	if err != nil {
		return TunnelHourObs{}, err
	}
	burst := min(tm.MaxRequestDuration*6/10, 2*time.Minute)
	if burst < rateAsked+10*time.Second {
		return TunnelHourObs{}, fmt.Errorf("a burst of %s cannot hold the %s path-rate measurement: the deadline %s is too short", burst, rateAsked, tm.MaxRequestDuration)
	}
	nBurst, err := r.tokensFor(burst, rate)
	if err != nil {
		return TunnelHourObs{}, err
	}
	// Sized for 34 s: the measured rate includes the time to the first token,
	// and the receiver must time at least 29 s of the 30 asked.
	nRate, err := r.tokensFor(rateAsked+4*time.Second, rate)
	if err != nil {
		return TunnelHourObs{}, err
	}
	smp := r.startSampler(in, tm.MaxSampleGap/4)
	first := time.Now()
	var reqs []Stream
	for i := 0; i < tm.MinChain; i++ {
		reqs = append(reqs, r.finish(r.openStream(nLong, key, tunnelBodyPad)))
	}
	// The burst, and the path rate inside it.
	var bs []stream
	for i := 0; i < tm.ConcurrentStreams; i++ {
		bs = append(bs, r.openStream(nBurst, key, tunnelBodyPad))
	}
	for _, s := range bs {
		<-s.first
	}
	rs := r.wait(r.openStream(nRate, key, ""))
	var outs []stack.Streamed
	for _, s := range bs {
		outs = append(outs, r.wait(s))
	}
	under := true
	for _, o := range outs {
		under = under && !o.EndAt.Before(rs.EndAt)
		reqs = append(reqs, r.observe(o))
	}
	rateObs := r.observe(rs)
	reqs = append(reqs, rateObs)
	pr := PathRate{Direction: "up", OK: rateObs.Complete, AskedSeconds: rateAsked.Seconds(), Bytes: int64(len(rs.Body)), Tokens: int64(rateObs.Tokens), UnderLoad: under}
	if !rs.FirstAt.IsZero() {
		pr.TimedSeconds = rs.EndAt.Sub(rs.FirstAt).Seconds()
	}
	// Heartbeats only.
	idle := time.Now().Add(tm.MinIdleGap + 15*time.Second)
	stack.WaitFor(r.t, tm.MinIdleGap+time.Minute, "the idle stretch", func() bool { return !time.Now().Before(idle) })
	for time.Since(first) < tm.TunnelHour {
		reqs = append(reqs, r.finish(r.openStream(nLong, key, tunnelBodyPad)))
	}
	samples := smp.end()
	cur := r.current(in)
	ds, _ := DrainStartOf(&cur, r.drainWindow())
	// The hour is over: the deployment goes, and with it the job, so that the
	// cluster's record shows the job's end (the verdict needs both its times).
	r.endDeployment(ctx, p)
	tags, err := r.runTags()
	if err != nil {
		return TunnelHourObs{}, err
	}
	p.Logf("%s: %d requests over %s; path rate %.0f B/s, %.0f tokens/s", p.Row.Name, len(reqs), time.Since(first).Round(time.Second), pr.BytesPerSecond(), pr.TokensPerSecond())
	return TunnelHourObs{Tags: tags, Requests: reqs, Samples: samples, DrainStart: ds, Deployment: r.observedDeployment(), Rate: pr}, nil
}
