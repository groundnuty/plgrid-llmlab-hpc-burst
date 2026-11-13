package alpha

// The router row's live flow (issue #521; betarouter.go): alpha's world with the router on, one worker. Nothing here judges: Run does
// (VerifyBetaRouter). The node's glibc is read as soon as the job runs and the Instance names it, before the Instance is ready, so that
// a node under 2.30 ends the run NOT RUN at once, and not after the restart ladder has spent its rungs on a router that cannot start.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/routerbytes/cases"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// BetaRouter is the router row: the Instance ready behind Envoy, what the node shows of Envoy and its glibc, one request through the
// front, S13's cases through it at the response level, one worker-rung restart with the router in front, then the deployment goes and
// its job is seen ended.
func (r *liveRun) BetaRouter(ctx context.Context, p Plan) (RouterObs, error) {
	r.start(p)
	var o RouterObs
	in, first, err := r.routerFirstReady(ctx, p, &o)
	if err != nil {
		return RouterObs{}, err
	}
	if in.Status.ReadyTime != nil {
		o.ReadyAt = in.Status.ReadyTime.Time
	}
	ds, ok := DrainStartOf(&in, r.dw)
	if !ok {
		r.t.Fatal("the Instance has no lease end")
	}
	o.DrainStart = ds
	o.Deployment = r.observedDeployment()
	o.LaunchRouter = string(in.Spec.Launch.Router)
	if w := in.Spec.Launch.Workers; w != nil {
		o.LaunchWorkers = *w
	}
	o.HealthyAtReady = healthyOf(r.current(in))
	r.envoyOf(ctx, p, first, &o)
	key := r.fc.KeyFor(&in)
	o.Rate = r.rate(key)
	o.Bytes = r.bytesReading(ctx, p, key)
	o.WorkerRestart = r.workerRestart(ctx, p, in)
	o.HealthyAfterRestart = r.healthyAgain(in)
	r.endDeployment(ctx, p)
	tags, err := r.runTags()
	if err != nil {
		return RouterObs{}, err
	}
	o.Tags = tags
	o.WorkerRestarts = WorkerRestarts(r.watch.Last())
	o.Ladder = LadderOf(r.watch.Last())
	o.Responses, o.Attributed = r.attribution()
	return o, nil
}

// healthyOf is status.healthyWorkers, -1 when the field is absent.
func healthyOf(in v1alpha1.Instance) int {
	if in.Status.HealthyWorkers == nil {
		return -1
	}
	return int(*in.Status.HealthyWorkers)
}

// healthyAgain is status.healthyWorkers once it is 1, or what it was when RouterHealthyWait ran out: the controller writes it from the
// agent's next report of Envoy's /clusters, a moment after the Instance serves again.
func (r *liveRun) healthyAgain(in v1alpha1.Instance) int {
	deadline := time.Now().Add(RouterHealthyWait)
	for {
		n := healthyOf(r.current(in))
		if n == 1 || time.Now().After(deadline) {
			return n
		}
		time.Sleep(2 * time.Second)
	}
}

// routerFirstReady is firstReady with the node's glibc read first. It returns the ready Instance and the one the controller made first
// (the same one, as the row has one job); an error is ErrNotStarted's family or ErrGlibcTooOld, which end the run NOT RUN.
func (r *liveRun) routerFirstReady(ctx context.Context, p Plan, o *RouterObs) (ready, first v1alpha1.Instance, err error) {
	if err := r.firstJobStarted(p); err != nil {
		return ready, first, err
	}
	r.waitFor(stack.WaitDeadline, "the first Instance's lease end", func() bool {
		for _, x := range r.instances() {
			if x.Annotations[v1alpha1.AnnotationPredecessor] == "" && x.Status.LeaseEnd != nil {
				first = x
				return true
			}
		}
		return false
	})
	r.oneEnd = r.oneJob(first)
	// The Instance names the job it runs on once its start instruction is given.
	r.waitFor(routerJobWait, "the first Instance to name its job", func() bool {
		if x, ok := r.lookup(first); ok && x.Status.JobID != "" {
			first = x
			return true
		}
		return false
	})
	jobID := r.ourJobFor(ctx, first, "the node's glibc")
	o.Glibc.Probe = r.glibcProbe(ctx, jobID)
	if v, ok := GlibcOf(o.Glibc); ok {
		p.Logf("%s: the compute node's glibc is %s, read on the node", p.Row.Name, v)
	}
	if err := o.Glibc.TooOld(); err != nil {
		p.Logf("%s: %v", p.Row.Name, err)
		return ready, first, err
	}
	var refused bool
	next := time.Now()
	r.waitFor(r.readyWait(), "the first Instance to be ready and registered at the front", func() bool {
		for _, x := range r.instances() {
			if x.Annotations[v1alpha1.AnnotationPredecessor] == "" && x.Status.Phase == v1alpha1.InstanceReady {
				if reg, known := stack.Registered(r.h.Ports, &x); known && reg {
					ready = x
					return true
				}
			}
		}
		// Envoy starts at the Instance's start instruction, which can come before the probe above ran: a refused start is in the agent's log.
		if time.Now().After(next) {
			next = time.Now().Add(15 * time.Second)
			if v, ok := r.agentGlibcRefusal(ctx, first); ok {
				o.Glibc.Agent = GlibcRead{OK: true, Version: v}
				refused = true
				return true
			}
		}
		return false
	})
	if refused {
		if err := o.Glibc.TooOld(); err != nil {
			p.Logf("%s: %v", p.Row.Name, err)
			return ready, first, err
		}
		r.t.Fatalf("the agent refused Envoy for a glibc of %s, which is not under the %s Envoy needs", o.Glibc.Agent.Version, MinEnvoyGlibc)
	}
	p.Logf("%s: the first Instance is ready behind the router", p.Row.Name)
	return ready, first, nil
}

// glibcProbe is one srun step in the run's job that asks the node for its glibc.
func (r *liveRun) glibcProbe(ctx context.Context, jobID string) GlibcRead {
	line, err := SrunLine(jobID)
	if err != nil {
		return GlibcRead{}
	}
	script := GlibcScript
	if r.glibcScript != "" {
		script = r.glibcScript // a stand-in's node with another glibc
	}
	res, err := r.tr.Run(ctx, slurm.Command{Line: line, Stdin: []byte(script), Timeout: srunTimeout})
	if err != nil || res.ExitCode != 0 {
		return GlibcRead{}
	}
	v, ok := ParseGetconfGlibc(res.Stdout)
	return GlibcRead{OK: ok, Version: v}
}

// agentGlibcRefusal is the node's glibc as the agent's own log of the job says it, when the log holds a refused router start.
func (r *liveRun) agentGlibcRefusal(ctx context.Context, in v1alpha1.Instance) (GlibcVersion, bool) {
	b, err := r.agentLog(ctx, in)
	if err != nil {
		return GlibcVersion{}, false
	}
	return ParseGlibcRefusal(string(b))
}

// envoyOf reads Envoy off the node and the agent's log: the job's Envoy process and whether it is bare, the digest of its executable
// against the pin, and the agent's own say that the router serves.
func (r *liveRun) envoyOf(ctx context.Context, p Plan, in v1alpha1.Instance, o *RouterObs) {
	jobID := r.ourJobFor(ctx, in, "Envoy on the node")
	if d, ok := pinnedEnvoyDigest(r.w.cpuArch); ok {
		o.Envoy.PinnedDigest = d
	}
	line, err := SrunLine(jobID)
	if err != nil {
		r.t.Fatalf("Envoy on the node: %v", err)
	}
	res, err := r.tr.Run(ctx, slurm.Command{Line: line, Stdin: []byte(ListScript(jobID)), Timeout: srunTimeout})
	if err != nil || res.ExitCode != 0 {
		r.t.Fatalf("Envoy on the node: the srun step that lists the job's processes failed (exit %d, transport error %v)", res.ExitCode, err != nil)
	}
	procs, err := ParseListing(res.Stdout)
	if err != nil {
		r.t.Fatalf("Envoy on the node: %v", err)
	}
	envoys, contained := PickEnvoy(procs)
	o.Envoy.Procs, o.Envoy.Contained = len(envoys), contained
	if len(envoys) == 1 {
		script, err := EnvoyDigestScript(envoys[0])
		if err != nil {
			r.t.Fatalf("Envoy on the node: %v", err)
		}
		r.ourJobFor(ctx, in, "Envoy's executable") // again, right before it is read
		res, err := r.tr.Run(ctx, slurm.Command{Line: line, Stdin: []byte(script), Timeout: srunTimeout})
		if err == nil && res.ExitCode == 0 {
			if d, ok := ParseEnvoyDigest(res.Stdout); ok {
				o.Envoy.RunningDigest = d
			}
		}
	}
	if b, err := r.agentLog(ctx, in); err == nil {
		o.Envoy.Serving = strings.Contains(string(b), "router is serving")
		o.Envoy.Refused = strings.Contains(string(b), "router did not start")
	}
	p.Logf("%s: Envoy on the node: %d process(es), bare %t, executable read %t, equal to the pin %t; the agent says it serves: %t",
		p.Row.Name, o.Envoy.Procs, !o.Envoy.Contained, o.Envoy.RunningDigest != "", o.Envoy.RunningDigest != "" && o.Envoy.RunningDigest == o.Envoy.PinnedDigest, o.Envoy.Serving)
}

// pinnedEnvoyDigest is the SHA-256 the controller stages Envoy by for a cpuArch: the pin of this build (internal/agent/envoypin).
func pinnedEnvoyDigest(cpuArch string) (string, bool) {
	ds, err := launcher.EnvoyDigests()
	if err != nil {
		return "", false
	}
	d, ok := ds[cpuArch]
	return d, ok
}

// routerJobWait is how long the run waits for the first Instance to name its job, once its lease end is known: the same bound as every
// read of the stack's own state (stack.WaitDeadline).
const routerJobWait = stack.WaitDeadline

// bytesWorkers is how many of S13's variants are in flight at once: the front's cap for the Instance is far above it.
const bytesWorkers = 8

// bytesReading sends every variant of S13's cases through the front once (AdaptBody), and reads each response (ClassifyReply). It is a
// reading of the exchange on a real engine and never of bytes. The whole reading is bounded by BytesBound; a variant not answered by then
// is one that did not finish.
func (r *liveRun) bytesReading(ctx context.Context, p Plan, key string) BytesObs {
	var vs []cases.Variant
	for _, c := range cases.All() {
		vs = append(vs, c.Variants()...)
	}
	obs := BytesObs{Expected: len(vs)}
	rctx, cancel := context.WithTimeout(ctx, BytesBound)
	defer cancel()
	model := r.w.md.Spec.Model
	work := make(chan cases.Variant)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range bytesWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for v := range work {
				rp := r.fc.SendCtx(rctx, string(v.Case.Endpoint), string(AdaptBody(v.Body, model)), key)
				if rp.Header != nil {
					r.keep(rp.Header)
				}
				mu.Lock()
				obs.Add(v.Case.Endpoint == cases.Chat, v.Stream, v.Valid(), v.Key(), rp.Status, rp.Body, rp.Err != nil)
				mu.Unlock()
			}
		}()
	}
	for _, v := range vs {
		select {
		case work <- v:
		case <-rctx.Done():
		}
		if rctx.Err() != nil {
			break
		}
	}
	close(work)
	wg.Wait()
	p.Logf("%s: S13's cases at the response level: %d of %d variants sent, %d answered completely (%d with 200, %d with 4xx, %d with 5xx), %d made by the router or the path",
		p.Row.Name, obs.Variants, obs.Expected, obs.Complete, obs.Status200, obs.Status4xx, obs.Status5xx, obs.RouterMade)
	if len(obs.Failed) > 0 {
		p.Logf("%s: variants that did not finish or that the path failed (first %d): %s", p.Row.Name, len(obs.Failed), fmt.Sprint(obs.Failed))
	}
	return obs
}
