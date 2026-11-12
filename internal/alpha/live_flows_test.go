package alpha

// The live steps' flows (issue #245): the handover on Athena, no-overlap on
// Helios and the tunnel hour, each driving the stack liveRun started (in its
// world) and returning what it saw. Nothing here judges: Run does.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
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
	case <-r.guardFail:
		r.checkGuard()
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
	s := StreamOf(r.fc.RowOf(out.ID), ClientEnd{Done: done, Finished: finished, ErrorCode: stack.StreamErrorCode(out.Body)})
	s.Ticks = out.Ticks
	return s
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

// maxModelLenOrDefault is the deployment's max-model-len as tokensFor reads it: vLLM's 4096 when its flags set none (maxModelLen,
// below, is the strict reading the large bodies need).
func (r *liveRun) maxModelLenOrDefault() int {
	maxLen := 4096
	for _, f := range r.w.md.Spec.Source.Served.Flags {
		if f.Name == "max-model-len" {
			fmt.Sscan(f.Value, &maxLen)
		}
	}
	return maxLen
}

// tokensFor is how many tokens make a stream last d at rate, within the
// deployment's max-model-len less a margin for the prompt (PromptReserve).
func (r *liveRun) tokensFor(d time.Duration, rate float64) (int, error) {
	n := max(int(rate*d.Seconds()), 1)
	maxLen := r.maxModelLenOrDefault()
	if n+PromptReserve > maxLen {
		return 0, fmt.Errorf("%w: %d tokens for %s at %.0f tokens/s, max-model-len %d", ErrRequestTooLong, n, d, rate, maxLen)
	}
	return n, nil
}

// whyGone logs what the API recorded of an Instance that went away: its last
// version's conditions, the events about it and about the deployment, and the
// times that decide a drain, each from the kill.
func (r *liveRun) whyGone(ctx context.Context, in v1alpha1.Instance, killed time.Time) {
	t := r.t
	if err := r.syncWatch(ctx, stack.KindInstances|stack.KindDeployments, "what became of the Instance"); err != nil {
		t.Errorf("%v", err)
	}
	obs := r.watch.Of(in.UID)
	last := obs[len(obs)-1]
	x := last.Instance
	t.Logf("gone: last seen %s at kill%+s, end reason %q, restarts %d, endTime %v", last.Phase, last.At.Sub(killed).Round(time.Second), last.EndReason, last.Restarts, x.Status.EndTime)
	for _, c := range x.Status.Conditions {
		t.Logf("gone: condition %s=%s (%s): %.300s", c.Type, c.Status, c.Reason, c.Message)
	}
	for _, name := range []string{in.Name, r.w.md.Name} {
		for _, e := range stack.EventsFor(t, r.c, name) {
			t.Logf("gone: event on %s at kill%+s: %s: %.300s", name, e.EventTime.Sub(killed).Round(time.Second), e.Reason, e.Message)
		}
	}
	for _, d := range r.watch.Deployments() {
		if d.Spec.EndTime != nil {
			t.Logf("gone: the deployment's endTime is kill%+s", d.Spec.EndTime.Sub(killed).Round(time.Second))
			break
		}
	}
	if x.Status.LeaseEnd != nil {
		ds, _ := v1alpha1.DrainStart(x.Status.LeaseEnd.Time, r.w.profile, &v1alpha1.Routing{MaxRequestDuration: dur(r.tm.MaxRequestDuration)})
		t.Logf("gone: its lease end is kill%+s, its drain start kill%+s", x.Status.LeaseEnd.Sub(killed).Round(time.Second), ds.Sub(killed).Round(time.Second))
	}
}

// keyNow is a conversation key the front's ring maps to the Instance now, as
// stack's KeyFor finds one, but false rather than a failed test when none of
// its candidates does or a reply is not a 200: an Instance coming back from a
// restart, or paused, is not on the ring yet.
func (r *liveRun) keyNow(in *v1alpha1.Instance) (string, bool) {
	for i := 0; i < 64; i++ {
		key := fmt.Sprintf("rehearsal-conversation-%d", i)
		rp := r.fc.Send("/v1/chat/completions", stack.ChatBody(r.fc.Model, 1, 0, false, ""), key)
		if rp.Err != nil || rp.Status != http.StatusOK {
			return "", false
		}
		if rp.Header.Get("Hpcburst-Instance-Uid") == string(in.UID) {
			return key, true
		}
	}
	return "", false
}

// lookup reads an Instance again from the API; false when it is gone.
func (r *liveRun) lookup(in v1alpha1.Instance) (v1alpha1.Instance, bool) {
	var x v1alpha1.Instance
	ctx, cancel := stack.CallCtx(r.t)
	defer cancel()
	return x, r.c.Get(ctx, client.ObjectKeyFromObject(&in), &x) == nil
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
// It is ourJobFor (now_flows_test.go) for the worker fault.
func (r *liveRun) ourJob(ctx context.Context, in v1alpha1.Instance) string {
	r.t.Helper()
	return r.ourJobFor(ctx, in, "worker fault")
}

// workerRestart is the worker-rung fault: it kills the engine of the
// Instance's job on its node, by PID, and waits for the stack to restart the
// worker and for the Instance to serve the key again (HBEP-7:637).
func (r *liveRun) workerRestart(ctx context.Context, p Plan, in v1alpha1.Instance) RestartTimes {
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
	eng, err := PickEngine(procs, in.Spec.Launch.Command, r.w.matchEngine)
	if err != nil {
		t.Fatalf("worker fault: %v", err)
	}
	r.ourJob(ctx, in) // again, right before the kill
	for _, x := range r.instances() {
		if x.Annotations[v1alpha1.AnnotationPredecessor] == in.Name {
			reg, known := stack.Registered(r.h.Ports, &x)
			p.Logf("%s: at the kill, the replacement %s is %s, registered %v (known %v)", p.Row.Name, x.Name, x.Status.Phase, reg, known)
		}
	}
	res, err = r.tr.Run(ctx, slurm.Command{Line: line, Stdin: []byte(KillScript(eng)), Timeout: srunTimeout})
	if err != nil || res.ExitCode != 0 || !Killed(res.Stdout, eng) {
		t.Fatalf("worker fault: the srun step that kills the engine did not (exit %d, transport error %v, said %q)", res.ExitCode, err != nil, res.Stdout)
	}
	times := RestartTimes{Killed: time.Now()}
	p.Logf("%s: killed the engine of the run's job by its PID (command digest %s)", p.Row.Name, CommandDigest(eng.Argv))
	r.waitFor(r.tm.StartTimeout+5*time.Minute, "the worker restart and the Instance serving the key again", func() bool {
		now, ok := r.lookup(in)
		if !ok {
			r.whyGone(ctx, in, times.Killed)
			t.Fatalf("worker fault: Instance %s is gone", in.Name)
		}
		if len(now.Status.Restarts) <= before || now.Status.Phase != v1alpha1.InstanceReady {
			return false
		}
		// The key the run chose may map to a replacement that has since joined
		// the front's ring: serving is shown with a key that maps to this
		// Instance now (the rehearsal's step (d) chooses its key the same way).
		k, ok := r.keyNow(&now)
		if !ok {
			return false
		}
		rp := r.fc.Send("/v1/chat/completions", stack.ChatBody(r.fc.Model, 1, 0, false, ""), k)
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
	if err := r.syncWatch(ctx, stack.KindInstances, "the Instance's versions after the kill"); err != nil {
		t.Errorf("%v", err)
	}
	if o, ok := r.watch.First(in.UID, times.Killed.Add(-time.Second), func(o stack.Observation) bool { return healthyPartExited(o.Instance) }); ok {
		times.AgentNoticed = o.At
	}
	p.Logf("%s: %s", p.Row.Name, times.Line())
	return times
}

// streamLead is how long before the drain start the streams that cross it
// open: a third of the deadline, so that each runs a third past it and ends
// well within the deadline, or less when the deployment's max-model-len cannot
// hold a stream of two leads at the measured rate (FitLead, issue #447). Its
// error is ErrLeadTooShort when even the floor's lead does not fit: the row is
// NOT RUN. Call it once the rate is measured, before anything the lead times.
func (r *liveRun) streamLead(rate float64) (time.Duration, error) {
	return FitLead(r.tm.MaxRequestDuration, r.maxModelLenOrDefault(), rate)
}

// untilBefore waits until lead before t, failing when that has passed.
func (r *liveRun) untilBefore(t time.Time, lead time.Duration, what string) {
	at := t.Add(-lead)
	if time.Now().After(at) {
		r.t.Fatalf("%s: the time to open the streams, %s before the drain start, has passed: the steps before it took too long for the lease", what, lead)
	}
	// What the steps before it left to spare (the review of PR 317: on a handover whose successor is late, half the lead at most).
	r.t.Logf("%s: %s to spare before the time to open the streams", what, time.Until(at).Round(time.Millisecond))
	r.waitFor(time.Until(at)+time.Minute, what, func() bool { return !time.Now().Before(at) })
}

// Handover is alpha-athena-handover: a worker-rung restart on the primary,
// then two streams in flight across its drain start while its replacement
// takes over.
func (r *liveRun) Handover(ctx context.Context, p Plan) (HandoverObs, error) {
	r.start(p)
	pre, err := r.firstReady(p)
	if err != nil {
		return HandoverObs{}, err
	}
	key := r.fc.KeyFor(&pre)
	rate := r.rate(key)
	// The lead comes from the measured rate, before the restart: a deployment whose context cannot hold a stream that tells a
	// cut from an end is NOT RUN with nothing yet restarted.
	lead, err := r.streamLead(rate)
	if err != nil {
		return HandoverObs{}, err
	}
	times := r.workerRestart(ctx, p, pre)
	pre = r.current(pre)
	ds, ok := DrainStartOf(&pre, r.dw)
	if !ok {
		r.t.Fatal("the primary has no lease end")
	}
	// HBEP-7, Handover: the replacement is created at its planned time and is ready before the drain start. The
	// predecessor's streams open whether or not it is ready by then: they are the first job's part of the run, which stands
	// if the second never starts (the delta review of PR 284, L1). So this wait stops in time to open them, half the lead
	// before they open, and the rest of the replacement's time runs with them in flight.
	succ, ok := r.successorOf(pre.Name, time.Until(ds)+LeadTimesFor(lead).SuccessorWaitEnds, nil)
	key = r.fc.KeyFor(&pre) // the ring as it is now: with both Instances, once the replacement is ready
	n, err := r.tokensFor(2*lead, rate)
	if err != nil {
		return HandoverObs{}, err
	}
	r.untilBefore(ds, lead, "the streams across the drain start")
	s1 := r.openStream(n, key, "")
	<-s1.first
	s2 := r.openStream(n, key, "")
	if !ok {
		if succ, ok = r.successorOf(pre.Name, time.Until(ds), nil); !ok {
			p.Logf("%s: no replacement was ready by the drain start", p.Row.Name)
		}
	}
	streams := []Stream{r.finish(s1), r.finish(s2)}
	r.waitFor(r.dw+EndMargin, "the primary to end", func() bool {
		_, ended := r.watch.First(pre.UID, time.Time{}, func(o stack.Observation) bool { return o.Phase == v1alpha1.InstanceEnded })
		return ended
	})
	// The successor's drain start must be the end the API holds (review of PR 255, B6). A second job that never
	// started leaves the first job's part to judge (Run).
	second := r.guardResult()
	if second != nil && !errors.Is(second, ErrSecondNotStarted) {
		return HandoverObs{}, second
	}
	// The result is read from the watch, straight after the API showed the
	// primary ended (issue #281).
	if err := r.syncWatch(ctx, stack.KindInstances|stack.KindAllocations, "the handover's result"); err != nil {
		return HandoverObs{}, notKnownBeside(err, second)
	}
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
		Handovers:      r.handoversOf(second, preLast),
		WorkerRestarts: WorkerRestarts(r.watch.Last()), Responses: responses, Attributed: attributed, WorkerRestart: times,
	}, second
}

// handoversOf counts the handovers that completed. A run whose second job never started reads no events for it: that is not judged
// on such a run, and stack.Events fails the test when its list fails, which would decide the run's end on a read nothing needs
// (the delta review of PR 284, L4).
func (r *liveRun) handoversOf(second error, pre v1alpha1.Instance) int {
	if second != nil {
		return 0
	}
	return Handovers(stack.Events(r.t, r.c), []v1alpha1.Instance{pre})
}

// NoOverlap is alpha-helios-nooverlap: a worker-rung restart on the primary;
// the replacement queued ahead, pending on the QOS's GPU cap; a stream in
// flight at the drain start; and the gap until the successor is ready, with
// paced requests through it.
func (r *liveRun) NoOverlap(ctx context.Context, p Plan) (NoOverlapObs, error) {
	r.start(p)
	pre, err := r.firstReady(p)
	if err != nil {
		return NoOverlapObs{}, err
	}
	key := r.fc.KeyFor(&pre)
	rate := r.rate(key)
	lead, err := r.streamLead(rate) // before the restart, as in Handover
	if err != nil {
		return NoOverlapObs{}, err
	}
	times := r.workerRestart(ctx, p, pre)
	pre = r.current(pre)
	ds, ok := DrainStartOf(&pre, r.dw)
	if !ok {
		r.t.Fatal("the primary has no lease end")
	}
	n, err := r.tokensFor(2*lead, rate)
	if err != nil {
		return NoOverlapObs{}, err
	}
	r.untilBefore(ds, lead, "the stream across the drain start")
	s := r.openStream(n, key, "")
	<-s.first
	pr := r.startProber(ds)
	// The successor starts once the predecessor's job ends and frees the GPU, then becomes ready. Its bound is the wait
	// in the queue from that end; only a job that started needs the rest of the wait.
	leaseEnd := pre.Status.LeaseEnd.Time
	succ, ok := r.successorOf(pre.Name, time.Until(leaseEnd)+SuccessorWait(r.tm, p.PendingMax), r.secondPastItsBound(r.tagOf(pre), leaseEnd.Add(p.PendingMax)))
	noReady := pr.end()
	p.Logf("%s: the gap prober's replies by status and code: %v", p.Row.Name, pr.replies)
	streams := []Stream{r.finish(s)}
	if !ok {
		p.Logf("%s: the successor was not ready in time", p.Row.Name)
	}
	// The successor's drain start must be the end the API holds (review of PR 255, B6). A second job that never
	// started leaves the first job's part to judge (Run).
	second := r.guardResult()
	if second != nil && !errors.Is(second, ErrSecondNotStarted) {
		return NoOverlapObs{}, second
	}
	if err := r.syncWatch(ctx, stack.KindInstances|stack.KindAllocations|stack.KindDeployments, "the no-overlap row's result"); err != nil {
		return NoOverlapObs{}, notKnownBeside(err, second)
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
	return o, second
}

// prober sends one short request a second from a time on, and counts the
// replies the front gave with 503 no_ready_target (recorded, not judged).
type prober struct {
	stop, done chan struct{}
	noReady    int
	// replies counts every reply by its status and the front's error code, for the log.
	replies map[string]int
}

func (r *liveRun) startProber(from time.Time) *prober {
	pr := &prober{stop: make(chan struct{}), done: make(chan struct{}), replies: map[string]int{}}
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
			e := stack.ErrorOf(rp.Body)
			detail := ""
			if e.Detail != nil {
				detail = *e.Detail
			}
			pr.replies[fmt.Sprintf("%d %s/%s", rp.Status, e.Code, detail)]++
			// HBEP-5, Errors: attached, with no eligible target, is 503 model_inactive with detail no_ready_target.
			if rp.Status == http.StatusServiceUnavailable && e.Code == "model_inactive" && detail == "no_ready_target" {
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

// sentLarge is one large request of the hour as the client sent it: the reply,
// the client's window from its first byte sent to its reply read, and the
// body's bytes and its prompt's own input tokens.
type sentLarge struct {
	reply            stack.Reply
	start, sent, end time.Time
	bytes, expected  int
}

// arrivalBody is the hour's large request and its prompt's own input tokens
// (ArrivalSize), with the engine's count read from its completed responses'
// usage through the front, sized under the deployment's max-model-len.
func (r *liveRun) arrivalBody(key string) (body string, expected int, err error) {
	mml, err := r.maxModelLen()
	if err != nil {
		return "", 0, err
	}
	count := func(words int) (int, error) {
		rp := r.fc.Send("/v1/chat/completions", ArrivalPrompt(r.fc.Model, words, ArrivalMaxTokens), key)
		n, ok := PromptTokensOf(rp.Body)
		if !ok {
			return 0, fmt.Errorf("a prompt of %d words with max_tokens %d was answered %d, not completed with its usage.prompt_tokens: the large bodies' arrival cannot be shown", words, ArrivalMaxTokens, rp.Status)
		}
		r.t.Logf("%s: calibration: a prompt of %d words was answered with usage.prompt_tokens %d", r.cfg.Row.Name, words, n)
		return n, nil
	}
	body, expected, err = ArrivalSize(count, r.fc.Model, r.tm.UploadBody, mml)
	if err == nil {
		r.t.Logf("%s: the large prompt: %d bytes, %d input tokens expected, under the max-model-len of %d", r.cfg.Row.Name, len(body), expected, mml)
	}
	return body, expected, err
}

// largeRequests is the hour's upload direction (issue #224), with no
// sender-side timing on the live path in alpha (the science agent's ruling (4)
// on PR 255): tm.UploadStreams senders, each sending the large request body
// (arrivalBody), which the engine answers with one token, pausing
// tm.UploadPause after a reply, and sending the next until rateAsked has
// passed. It runs beside the burst's streams, from its own goroutines.
func (r *liveRun) largeRequests(key, body string, expected int) []sentLarge {
	tm := r.tm
	until := time.Now().Add(rateAsked)
	var (
		mu   sync.Mutex
		outs []sentLarge
		wg   sync.WaitGroup
	)
	for range tm.UploadStreams {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(until) {
				start := time.Now()
				rp, sent := r.fc.SendTimed("/v1/chat/completions", body, key)
				end := time.Now()
				mu.Lock()
				outs = append(outs, sentLarge{reply: rp, start: start, sent: sent, end: end, bytes: len(body), expected: expected})
				mu.Unlock()
				if tm.UploadPause > 0 {
					time.Sleep(tm.UploadPause)
				}
			}
		}()
	}
	wg.Wait()
	return outs
}

// observeLarge is a large request as an observation: the front's record of
// it, its body and its prompt's own input tokens, the input tokens the
// engine's usage counted, and the client's window. It is no stream: the
// client reads no [DONE], so it is never Complete and never stands in for the
// completed stream near the deadline (the delta round's B3).
func (r *liveRun) observeLarge(l sentLarge) Stream {
	if l.reply.ID == "" {
		return Stream{Error: "no reply", BodyBytes: l.bytes, ExpectedTokens: l.expected}
	}
	return WithArrival(StreamOf(r.fc.RowOf(l.reply.ID), ClientEnd{}), l.reply.Body, l.bytes, l.expected, l.start, l.sent, l.end)
}

// maxModelLen is the deployment's max-model-len flag; a deployment without
// one is an error, since the large bodies are sized under it.
func (r *liveRun) maxModelLen() (int, error) {
	for _, f := range r.w.md.Spec.Source.Served.Flags {
		if f.Name == "max-model-len" {
			n, err := strconv.Atoi(f.Value)
			if err != nil || n <= 0 {
				return 0, fmt.Errorf("the deployment's max-model-len %q is not a positive number", f.Value)
			}
			return n, nil
		}
	}
	return 0, errors.New("the deployment has no max-model-len flag, which the large bodies are sized under")
}

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
	// The run's tags are read from the watch while the jobs end: every Allocation
	// of the run must be in it (issue #281).
	if err := r.syncWatch(ctx, stack.KindAllocations, "the run's Allocations"); err != nil {
		t.Errorf("%v", err)
	}
	r.waitFor(r.dw+EndMargin, "every job of the run to end in the cluster's record", func() bool {
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
// measures the path rate in both directions for 30 s each (issue #224): the
// download, node to front, with one more stream, and the upload, front to
// node, with large request bodies sent back to back (uploads), whose upload
// intervals the front records; an idle stretch longer than MinIdleGap; and
// requests back to back again until TunnelHour has run.
func (r *liveRun) TunnelHour(ctx context.Context, p Plan) (TunnelHourObs, error) {
	r.start(p)
	in, err := r.firstReady(p)
	if err != nil {
		return TunnelHourObs{}, err
	}
	ds := r.oneEnd
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
	// The large request, measured against the engine's own count before the hour (issue #224; the ruling (4) on PR 255).
	largeBody, largeExpected, err := r.arrivalBody(key)
	if err != nil {
		return TunnelHourObs{}, err
	}
	smp := r.startSampler(in, tm.MaxSampleGap/4)
	first := time.Now()
	var reqs []Stream
	for i := 0; i < tm.MinChain; i++ {
		reqs = append(reqs, r.finish(r.openStream(nLong, key, "")))
	}
	// The burst, and inside it the download's path rate with one more stream,
	// and the large requests beside it once every stream of the burst runs.
	var bs []stream
	for i := 0; i < tm.ConcurrentStreams; i++ {
		bs = append(bs, r.openStream(nBurst, key, ""))
	}
	for _, s := range bs {
		<-s.first
	}
	largeDone := make(chan []sentLarge, 1)
	go func() { largeDone <- r.largeRequests(key, largeBody, largeExpected) }()
	rs := r.wait(r.openStream(nRate, key, ""))
	var larges []sentLarge
	select {
	case larges = <-largeDone:
	case <-r.guardFail:
		r.checkGuard()
	case <-time.After(rateAsked + r.fc.RequestTimeout() + 2*time.Minute):
		r.t.Fatal("the large requests' senders did not finish")
	}
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
	pr := PathRate{Direction: DirDownload, OK: rateObs.Complete, AskedSeconds: rateAsked.Seconds(), Bytes: int64(len(rs.Body)), Tokens: int64(rateObs.Tokens), UnderLoad: under}
	if !rs.FirstAt.IsZero() {
		pr.TimedSeconds = rs.EndAt.Sub(rs.FirstAt).Seconds()
	}
	var windows []LargeWindow
	for _, l := range larges {
		reqs = append(reqs, r.observeLarge(l))
		windows = append(windows, LargeWindow{Start: l.start, Sent: l.sent, End: l.end})
	}
	// Heartbeats only.
	idle := time.Now().Add(tm.MinIdleGap + 15*time.Second)
	r.waitFor(tm.MinIdleGap+time.Minute, "the idle stretch", func() bool { return !time.Now().Before(idle) })
	for time.Since(first) < tm.TunnelHour {
		if time.Now().Add(long).After(ds) {
			p.Logf("%s: the drain start comes before the hour is over", p.Row.Name)
			break
		}
		reqs = append(reqs, r.finish(r.openStream(nLong, key, "")))
	}
	samples := smp.end()
	dep := r.observedDeployment()
	// The hour is over: the deployment goes, and with it the job, so that the
	// cluster's record shows the job's end (the verdict needs both its times).
	r.endDeployment(ctx, p)
	tags, err := r.runTags()
	if err != nil {
		return TunnelHourObs{}, err
	}
	hol := HeadOfLineOf(reqs, windows)
	p.Logf("%s: %d requests over %s; path rate, download %.0f B/s and %.0f tokens/s; %d large requests of %d bytes; head-of-line %s in flight (%s send, %s prefill), %s elsewhere", p.Row.Name, len(reqs),
		time.Since(first).Round(time.Second), pr.BytesPerSecond(), pr.TokensPerSecond(), len(larges), tm.UploadBody, hol.InFlight.Round(time.Millisecond),
		hol.Send.Round(time.Millisecond), hol.Prefill.Round(time.Millisecond), hol.Elsewhere.Round(time.Millisecond))
	return TunnelHourObs{Tags: tags, Requests: reqs, Samples: samples, DrainStart: ds, Deployment: dep, Rates: []PathRate{pr},
		UploadNotMeasured: UploadNotMeasuredLive, HeadOfLine: &hol}, nil
}
