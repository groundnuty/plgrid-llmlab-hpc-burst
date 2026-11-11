package alpha

// Row A's live flow (issue #376; nowserve.go): alpha-athena-now-serve, driving
// the stack liveRun started (in its world) and returning what it saw. Nothing
// here judges: Run does (VerifyNowServe). The HBEP-8 checks run here, through
// internal/rehearsal/obscheck, which only test files may import, and their
// findings go into the observation.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/stage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/obscheck"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// NowServe is alpha-athena-now-serve: the Instance ready; the long stream; the deployment ID under its tag; the
// engine gauges; one worker-rung restart; the requeue of the run's own job last; HBEP-8's events once everything
// that raises them has happened; then the deployment goes and its job is seen ended.
func (r *liveRun) NowServe(ctx context.Context, p Plan) (NowServeObs, error) {
	r.start(p)
	in, err := r.firstReady(p)
	if err != nil {
		return NowServeObs{}, err
	}
	var o NowServeObs
	if in.Status.ReadyTime != nil {
		o.ReadyAt = in.Status.ReadyTime.Time
	}
	ds, ok := DrainStartOf(&in, r.dw)
	if !ok {
		r.t.Fatal("the Instance has no lease end")
	}
	o.DrainStart = ds
	o.Deployment = r.observedDeployment()
	var files []agentapi.FileDigest
	o.Identity, files = r.identityOf(ctx, in)
	// The warm start, from the agent's own log of the job, read while the job runs: an accepted requeue's release
	// removes the Allocation's files.
	o.Warm = r.warmOf(ctx, in, files)
	key := r.fc.KeyFor(&in)
	o.Rate = r.rate(key)
	o.Stream = r.serveStream(p, key, o.Rate)
	o.Gauges = r.nowGauges(in)
	o.WorkerRestart = r.workerRestart(ctx, p, in)
	in = r.current(in)
	var refused error
	o.Requeue, refused = r.requeueOwnJob(ctx, p, in, o.Rate)
	if refused != nil && !errors.Is(refused, ErrRequeueRefused) {
		return NowServeObs{}, refused
	}
	o.Events = r.nowEvents(in, o.Requeue.Accepted)
	// The run is over: the deployment goes, and the cluster's record shows its job ended (a refused requeue leaves the
	// job running until the drain ends it). On an accepted requeue the job has ended already, by the release.
	r.endDeployment(ctx, p)
	tags, err := r.runTags()
	if err != nil {
		return NowServeObs{}, err
	}
	o.Tags = tags
	o.WorkerRestarts = WorkerRestarts(r.watch.Last())
	o.Responses, o.Attributed = r.attribution()
	return o, refused
}

// identityOf reads the Instance's deployment ID under its Allocation's tag (HBEP-3; the rehearsal's part 3,
// checkIdentity): the tag computed from HBEP-3 here, the name the queue lists for the Allocation's job by its ID, the
// Allocation's external name and observed tag, and the Instance's deployment ID, its label and its identity record.
// A read that fails leaves its field empty, which the verdict fails.
// It returns, beside it, the checkpoint files the identity record lists, for the warm start (warmOf).
func (r *liveRun) identityOf(ctx context.Context, in v1alpha1.Instance) (IdentityObs, []agentapi.FileDigest) {
	var a v1alpha1.Allocation
	cctx, cancel := stack.CallCtx(r.t)
	err := r.c.Get(cctx, client.ObjectKey{Name: in.Spec.AllocationRef.Name, Namespace: in.Namespace}, &a)
	cancel()
	if err != nil {
		r.t.Logf("the Instance's Allocation could not be read for its identity: %v", err)
		return IdentityObs{}, nil
	}
	o := IdentityObs{Read: true, Tag: HBEP3Tag(a.Namespace, a.Name, string(a.UID)), ExternalName: a.Annotations[v1alpha1.AnnotationExternalName],
		ObservedTag: a.Status.AtProvider.Tag, DeploymentID: string(in.Spec.DeploymentID), DisplayLabel: in.Labels[v1alpha1.LabelDeploymentID],
		IdentityRecord: in.Spec.IdentityRecord}
	if id := a.Status.AtProvider.JobID; id != "" {
		if j, listed, err := r.batch.QueueJob(ctx, id); err == nil && listed {
			o.JobName = j.Name
		}
	}
	var rec appsv1.ControllerRevision
	cctx, cancel = stack.CallCtx(r.t)
	err = r.c.Get(cctx, client.ObjectKey{Name: in.Spec.IdentityRecord, Namespace: in.Namespace}, &rec)
	cancel()
	var files []agentapi.FileDigest
	if err == nil {
		o.RecordDisplayLabel, o.RecordKind = rec.Labels[v1alpha1.LabelDeploymentID], rec.Labels[v1alpha1.LabelRecord]
		if files, err = identityRecordFiles(rec.Data.Raw); err != nil {
			r.t.Logf("the identity record's file list: %v", err)
		}
	}
	return o, files
}

// identityRecordFiles is the checkpoint files an identity record lists, read from its data as HBEP-3's canonical object
// holds them (the record's "canonical" member, whose "files" are each a path and a SHA-256): the list the agent's
// stager is given (internal/lifecycle, desired.go, Checks.Verify.Files).
func identityRecordFiles(raw []byte) ([]agentapi.FileDigest, error) {
	var d struct {
		Canonical struct {
			Files []agentapi.FileDigest `json:"files"`
		} `json:"canonical"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	return d.Canonical.Files, nil
}

// stageSkipRule is the stage package's own rule of the files the engine does not load (nowserve.go, SkipRule), its
// export of PR #393: the files staging skips with no line.
var stageSkipRule SkipRule = stage.SkippedFiles

// warmOf reads the warm start's evidence (the ruling 1(b) on #376): the Instance's image digest, the files that must
// each show present and matching (ExpectedWarmFiles: the identity record's list less what the stager skips by the
// launch's load format, from outside the log), and the agent's own log of the job (ParseAgentLog). The verdict's seam
// (warmStartShownByTheAgentsOwnLog) judges them; a log that could not be read shows nothing.
func (r *liveRun) warmOf(ctx context.Context, in v1alpha1.Instance, files []agentapi.FileDigest) WarmObs {
	w := WarmObs{ImageDigest: in.Spec.Launch.ImageDigest}
	w.Files, w.Skipped = ExpectedWarmFiles(files, agent.LoadFormat(in.Spec.Launch.Command), stageSkipRule)
	b, err := r.agentLog(ctx, in)
	if err != nil {
		r.t.Logf("the agent's log of the job could not be read for the warm start: %v", err)
		return w
	}
	w.LogRead, w.Log = true, ParseAgentLog(b)
	r.t.Logf("%s: the agent's log of the job, read whole up to %d bytes: %d lines", r.cfg.Row.Name, agentLogMax, len(w.Log))
	return w
}

// agentLogMax is how much of the agent's log row A reads, from its start: the first staging of a warm start is in its
// first lines, and a read that does not reach that staging's end shows nothing (warmStartShownByTheAgentsOwnLog).
const agentLogMax = 4 << 20

// agentLogLine is the read of a job's agent log on the login node: the head of the job's standard error, where the agent
// and its stager write their log (cmd/hb-agent; agentapi.JobErrorName); exit 3 when there is no such file. path is
// checked by the caller.
func agentLogLine(path string) string {
	q := "'" + path + "'"
	return "if [ -r " + q + " ]; then head -c " + strconv.Itoa(agentLogMax) + " -- " + q + "; else exit 3; fi"
}

// safePath is the shape a path of the agent's log must have before it goes into a command line: absolute, and of
// characters that need no quoting.
var safePath = regexp.MustCompile(`^/[A-Za-z0-9._/-]+$`)

// agentLog reads the agent's log of the Instance's job over the run's own transport, or through the test's seam
// (agentLogFn). The path, which holds the login and the staging directory, goes to no log.
func (r *liveRun) agentLog(ctx context.Context, in v1alpha1.Instance) ([]byte, error) {
	path, err := r.agentLogPath(ctx, in)
	if err != nil {
		return nil, err
	}
	line := agentLogLine(path)
	if r.agentLogFn != nil {
		line = r.agentLogFn(path)
	}
	res, err := r.tr.Run(ctx, slurm.Command{Line: line, Timeout: time.Minute})
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("the read of the agent's log exited %d", res.ExitCode)
	}
	return res.Stdout, nil
}

// agentLogPath is the job's standard error on the cluster: the staging directory, resolved on the login node, the
// Allocation's directory under it (agentapi.AllocationDir) and agentapi.JobErrorName.
func (r *liveRun) agentLogPath(ctx context.Context, in v1alpha1.Instance) (string, error) {
	tag := r.tagOf(in)
	if !regexp.MustCompile(`^hb-[A-Za-z0-9._-]{1,60}$`).MatchString(tag) {
		return "", errors.New("the Instance's job tag is not known")
	}
	paths, err := r.batch.Paths(ctx)
	if err != nil {
		return "", fmt.Errorf("the staging directory: %w", err)
	}
	p := paths.Staging + "/" + agentapi.AllocationDir(tag) + "/" + agentapi.JobErrorName
	if !safePath.MatchString(p) || strings.Contains(p, "/../") {
		return "", errors.New("the agent's log path is not one a command line can carry unquoted")
	}
	return p, nil
}

// serveStream is HBEP-5's long stream: the row's tokens exactly (min_tokens and max_tokens both), at the engine's own
// rate, under the deployment's deadline. Its length is the row's, whatever the rate: a rate too slow for it under the
// deadline is the verdict's to fail, never a shorter stream's to hide.
func (r *liveRun) serveStream(p Plan, key string, rate float64) Stream {
	n := r.tm.ServeTokens
	mml, err := r.maxModelLen()
	if err != nil {
		r.t.Fatal(err)
	}
	if n+512 > mml {
		r.t.Fatalf("the long stream of %d tokens does not fit the deployment's max-model-len of %d", n, mml)
	}
	if rate > 0 {
		p.Logf("%s: the long stream of %d tokens should take %s at the measured %.0f tokens/s, under the %s deadline", p.Row.Name, n,
			(time.Duration(float64(n)/rate) * time.Second).Round(time.Second), rate, r.tm.MaxRequestDuration)
	}
	return r.finish(r.openStream(n, key, ""))
}

// servedTokens is what the front saw an Instance generate: the completion tokens of its usage records.
func (r *liveRun) servedTokens(uid string) float64 {
	var n float64
	for _, row := range r.fc.UsageRows() {
		if row.InstanceUID == uid && row.CompletionTokens != nil {
			n += float64(*row.CompletionTokens)
		}
	}
	return n
}

// nowGauges reads the lifecycle controller's /metrics (base + 12) once the Instance's generated-token counter has caught
// up with what the front saw it generate, so that the read is never before the run produced any, and holds the
// Instance's series to HBEP-8 through obscheck (the rehearsal's part 3, gauges): the engine gauges a real vLLM does not
// export are none (obscheck.VLLMNotReported), the stand-in's engine double's are its two prefix-cache counters.
func (r *liveRun) nowGauges(in v1alpha1.Instance) ObsCheck {
	uid := string(in.UID)
	min := r.servedTokens(uid)
	if min <= 0 {
		return ObsCheck{Checked: true, Problems: []string{"the front saw the Instance generate nothing: a read of its gauges now would prove nothing"}}
	}
	url := r.h.Ports.URL(12, "/metrics")
	var ss []obscheck.Sample
	r.waitFor(stack.WaitDeadline, "the controller's gauges to show the tokens the Instance generated", func() bool {
		var err error
		if ss, err = obscheck.Scrape(url); err != nil {
			return false
		}
		g := obscheck.Find(ss, obscheck.GaugeGenerated, map[string]string{"instance_uid": uid, "worker": "worker-0"})
		return len(g) == 1 && g[0].Value >= min
	})
	view := obscheck.GaugeView{UID: uid, Namespace: in.Namespace, Name: in.Name, Deployment: r.w.md.Name, Provider: r.w.provider,
		GPUModel: r.w.gpuModel, Reported: true, MinGenerated: min, NotReported: r.w.notReported}
	return ObsCheck{Checked: true, Problems: obscheck.GaugeProblems(ss, view)}
}

// nowEvents holds the run's events to HBEP-8 through obscheck. The expected set is written here from HBEP-8's Reasons
// table (hbep/0008-observability-and-events/README.md:164-177) and HBEP-7's rules, for this row's steps, never from what
// the run emitted (issue #193, Verifications that prove nothing): the agent registered (started); the Instance verified,
// qualified and warmed up (ready); its drain start under an hour away with no replacement ready, in a one-job row of a
// 48-minute lease, from its ready on (ending-soon, HBEP-7:547); the worker-rung restart (failing); and, when the
// scheduler took the requeue, its failure (failing, HBEP-3). Forbidden: a handover, since no replacement was ever
// ready. What else the run raised is not judged here beyond its form (obscheck.EventProblems holds every typed event
// to HBEP-8's form). It waits, bounded, for every expected event, then reads the forbidden ones and the form once.
func (r *liveRun) nowEvents(in v1alpha1.Instance, requeued bool) ObsCheck {
	inst := func(action, reason, why string) obscheck.WantEvent {
		return obscheck.WantEvent{Action: action, Reason: reason, Kind: "Instance", Namespace: in.Namespace, Name: in.Name, Controller: obscheck.LifecycleController, Why: why}
	}
	want := []obscheck.WantEvent{
		inst("started", v1alpha1.ReasonAgentRegistered, "the agent registers (Instance start)"),
		inst("ready", "Ready", "verified, qualified and warmed up (Instance start)"),
		inst("ending-soon", "NoReplacementReady", "ready with its drain start under an hour away and no replacement ready (HBEP-7:547)"),
		inst("failing", v1alpha1.ReasonWorkerRestart, "the worker-rung restart (HBEP-7, Health and restarts)"),
	}
	if requeued {
		want = append(want, inst("failing", v1alpha1.ReasonRequeued, "the requeue of the run's own job (HBEP-3)"))
	}
	forbid := []obscheck.WantEvent{{Action: "replaced", Reason: v1alpha1.EventReasonHandedOver, Kind: "Instance", Namespace: in.Namespace, Name: in.Name,
		Why: "no replacement was ever ready in a one-job row"}}
	var missing []obscheck.WantEvent
	var problems []string
	deadline, _ := stack.BoundedDeadline(r.t, stack.WaitDeadline)
	for {
		missing, problems = obscheck.EventProblems(stack.Events(r.t, r.c), want, forbid)
		if len(missing) == 0 || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(time.Second)
	}
	c := ObsCheck{Checked: true, Problems: problems}
	for _, m := range missing {
		c.Missing = append(c.Missing, m.String()+" ("+m.Why+")")
	}
	return c
}

// requeueOwnJob is the last step (Q5; nowserve.go, the order): a stream held open, the deployment's end set to now, the
// controller seen draining the Instance alone, the job checked to be the run's own by its ID and tag, then scontrol
// requeuehold of it by its ID. A refusal of the scheduler's ends the step with ErrRequeueRefused and the held stream
// left to finish; a call that never reached it fails the run. An accepted requeue is followed, bounded, until the
// Instance has ended, its Allocation is released and the cluster's record shows the job ended, each recorded as seen;
// then the watch is brought up to date and anything of the run it had not seen before the requeue is counted.
func (r *liveRun) requeueOwnJob(ctx context.Context, p Plan, in v1alpha1.Instance, rate float64) (RequeueObs, error) {
	t := r.t
	var ro RequeueObs
	holdFor := HoldStreamFor(r.tm)
	n, err := r.tokensFor(holdFor, rate)
	if err != nil {
		return ro, err
	}
	// The whole window must come before the deployment's own end (oneJob), which would drain the Instance first.
	if left := time.Until(r.oneEnd); left < holdFor+time.Minute {
		t.Fatalf("the requeue: %s is left before the deployment's planned end, under the %s its window needs: the steps before it took too long for the lease", left.Round(time.Second), holdFor+time.Minute)
	}
	knownA, knownI, err := r.requeueSnapshot(ctx)
	if err != nil {
		return ro, err
	}
	hold := r.openStream(n, r.fc.KeyFor(&in), "")
	<-hold.first
	if _, err := r.endAt(time.Now()); err != nil {
		t.Fatalf("the requeue: %v", err)
	}
	p.Logf("%s: the deployment's end is now, with a stream of %d tokens in flight", p.Row.Name, n)
	r.waitFor(stack.WaitDeadline, "the controller to drain the Instance alone at the deployment's end", func() bool { return r.drainingAlone(in) })
	ro.DrainingSeen = true
	jobID := r.ourJobFor(ctx, in, "the requeue")
	reply, err := r.requeue(ctx, jobID)
	if err != nil {
		t.Fatalf("the requeue: the scontrol call did not reach the scheduler, so nothing is known of a refusal: %v", err)
	}
	ro.Attempted = true
	if !reply.Accepted {
		ro.Refusal = reply.Refusal
		p.Logf("%s: the scheduler refused the requeue (%s); the row is NOT RUN unless another check fails", p.Row.Name, reply.Refusal)
		ro.HoldStream = r.finish(hold)
		return ro, &RequeueRefusedError{Reason: reply.Refusal}
	}
	ro.Accepted = true
	at := time.Now()
	p.Logf("%s: the scheduler took the requeue of the run's own job", p.Row.Name)
	r.requeueResult(ctx, in, jobID, &ro)
	p.Logf("%s: %s after the requeue: the Instance ended %q, its Allocation released %t, the job ended %t and requeued %t in the cluster's record",
		p.Row.Name, time.Since(at).Round(time.Second), ro.EndReason, ro.AllocationReleased, ro.JobEnded, ro.JobRequeued)
	ro.HoldStream = r.finish(hold)
	if err := r.syncWatch(ctx, stack.KindInstances|stack.KindAllocations, "the run's Instances and Allocations after the requeue"); err != nil {
		return ro, err
	}
	newA := map[types.UID]bool{}
	for _, o := range r.watch.Allocations() {
		if !knownA[o.UID] {
			newA[o.UID] = true
		}
	}
	ro.NewAllocations = len(newA)
	for _, u := range r.watch.UIDs() {
		if !knownI[u] {
			ro.NewInstances++
		}
	}
	return ro, nil
}

// requeueSnapshot is the run's Allocations and Instances before the requeue, once the watch has caught up with the API:
// what came after, the result is checked against.
func (r *liveRun) requeueSnapshot(ctx context.Context) (allocs, insts map[types.UID]bool, err error) {
	if err := r.syncWatch(ctx, stack.KindAllocations|stack.KindInstances, "the run's Allocations and Instances before the requeue"); err != nil {
		return nil, nil, err
	}
	allocs, insts = map[types.UID]bool{}, map[types.UID]bool{}
	for _, o := range r.watch.Allocations() {
		allocs[o.UID] = true
	}
	for _, u := range r.watch.UIDs() {
		insts[u] = true
	}
	return allocs, insts, nil
}

// drainingAlone reads, from the API, that the controller drains the Instance at the deployment's end and plans nothing
// beside it: the Instance draining, no other Instance of the run that has not ended, and no Allocation but its own.
func (r *liveRun) drainingAlone(in v1alpha1.Instance) bool {
	x, ok := r.lookup(in)
	if !ok || x.Status.Phase != v1alpha1.InstanceDraining {
		return false
	}
	for _, y := range r.instances() {
		if y.UID != in.UID && y.Status.Phase != v1alpha1.InstanceEnded {
			return false
		}
	}
	var al v1alpha1.AllocationList
	ctx, cancel := stack.CallCtx(r.t)
	defer cancel()
	if err := r.c.List(ctx, &al, client.InNamespace(r.w.md.Namespace)); err != nil {
		return false
	}
	for _, a := range al.Items {
		if a.Name != in.Spec.AllocationRef.Name {
			return false
		}
	}
	return true
}

// requeue sends the requeue: scontrol requeuehold of the job by its ID over the run's own transport (RequeueHoldLine),
// or the test's seam (requeueFn). Only an answer of the scheduler's is a reply; a call that did not reach it is an error.
// The scheduler's own text goes to the test's private log, never to the post: it can name the job's ID.
func (r *liveRun) requeue(ctx context.Context, jobID string) (RequeueReply, error) {
	if r.requeueFn != nil {
		return r.requeueFn(ctx, jobID)
	}
	line, err := RequeueHoldLine(jobID)
	if err != nil {
		return RequeueReply{}, err
	}
	res, err := r.tr.Run(ctx, slurm.Command{Line: line, Timeout: RequeueTimeout})
	if err != nil {
		return RequeueReply{}, err
	}
	reply := RequeueReplyOf(res.ExitCode, res.Stdout, res.Stderr)
	if !reply.Accepted {
		r.t.Logf("the requeue: scontrol exited %d: %.300s", res.ExitCode, strings.TrimSpace(string(res.Stderr)+" "+string(res.Stdout)))
	}
	return reply, nil
}

// requeueResult follows an accepted requeue, bounded by RequeueResultWait: the Instance's end and its reason, from the
// watch; its Allocation's deletion, from the watch; and, once both are seen, the job's end and its requeue in the
// cluster's own record, read by its tag. What it has seen when the bound runs out stays in the record; the verdict
// fails what is missing.
func (r *liveRun) requeueResult(ctx context.Context, in v1alpha1.Instance, jobID string, ro *RequeueObs) {
	tag := r.tagOf(in)
	led := r.Ledger()
	deadline, _ := stack.BoundedDeadline(r.t, RequeueResultWait)
	for {
		if o, ended := r.watch.First(in.UID, time.Time{}, func(o stack.Observation) bool { return o.Phase == v1alpha1.InstanceEnded }); ended {
			ro.EndReason = o.EndReason
		}
		ro.AllocationReleased = slices.ContainsFunc(r.watch.Allocations(), func(o stack.AllocObservation) bool {
			return o.Name == in.Spec.AllocationRef.Name && o.Deleted
		})
		if ro.EndReason != "" && ro.AllocationReleased && tag != "" {
			if jobs, err := led.Jobs(ctx, []string{tag}); err == nil {
				for _, j := range jobs {
					if j.ID == jobID {
						ro.JobEnded, ro.JobRequeued = j.EndTime != nil, provider.AccountingRequeued(j)
					}
				}
			}
		}
		if ro.EndReason != "" && ro.AllocationReleased && ro.JobEnded {
			return
		}
		if !time.Now().Before(deadline) {
			r.t.Logf("the requeue's result: not everything was seen within %s", RequeueResultWait)
			return
		}
		time.Sleep(time.Second)
	}
}

// ourJobFor is ourJob, its failures naming the step that needs it.
func (r *liveRun) ourJobFor(ctx context.Context, in v1alpha1.Instance, step string) string {
	t := r.t
	t.Helper()
	var a v1alpha1.Allocation
	cctx, cancel := stack.CallCtx(t)
	err := r.c.Get(cctx, client.ObjectKey{Name: in.Spec.AllocationRef.Name, Namespace: in.Namespace}, &a)
	cancel()
	if err != nil {
		t.Fatalf("%s: the Instance's Allocation: %v", step, err)
	}
	id, tag := a.Status.AtProvider.JobID, v1alpha1.JobTag(a.Namespace, a.Name, a.UID)
	if id == "" || id != in.Status.JobID {
		t.Fatalf("%s: the Instance's job is not its Allocation's", step)
	}
	if !r.tags.Tagged(tag) {
		t.Fatalf("%s: the Allocation's tag is not one the run wrote down", step)
	}
	j, listed, err := r.batch.QueueJob(ctx, id)
	if err != nil || !listed || j.Name != tag || j.Phase != v1alpha1.AllocationRunning {
		t.Fatalf("%s: the queue's --me listing does not show the run's job running under its tag (listed %v, phase %s, read error %v)", step, listed, j.Phase, err != nil)
	}
	return id
}
