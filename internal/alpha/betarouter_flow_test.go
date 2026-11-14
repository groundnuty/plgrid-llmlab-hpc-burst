package alpha

// The router row's live flow (issue #521; betarouter.go): alpha's world with the router on, one worker. Nothing here judges: Run does
// (VerifyBetaRouter). The node's glibc is read as soon as the job runs and the Instance names it, before the Instance is ready, so that
// a node under 2.30 ends the run NOT RUN at once, and not after the restart ladder has spent its rungs on a router that cannot start.

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/routerbytes/cases"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
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
	o.HealthyAtReady = r.healthyWithin(in)
	o.EngineVersion = r.engineVersionOf(in)
	r.envoyOf(ctx, p, first, &o)
	key := r.fc.KeyFor(&in)
	o.Rate = r.rate(key)
	var front []ControlVariant
	var reqs []ControlRequest
	o.Bytes, front, reqs = r.bytesReading(ctx, p, key)
	o.Control = r.directControl(ctx, p, first, in, front, reqs)
	o.WorkerRestart = r.workerRestart(ctx, p, in)
	o.HealthyAfterRestart = r.healthyWithin(in)
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

// healthyWithin is status.healthyWorkers once it is 1, or what it was when RouterHealthyWait ran out: the controller writes it from the
// agent's next report of Envoy's /clusters, a moment after the Instance is first ready, and again after it serves again.
func (r *liveRun) healthyWithin(in v1alpha1.Instance) int {
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

// glibcProbeTries is how many times the run asks the node for its glibc before it fails naming the step, and glibcProbePause the pause
// between two tries: a transient srun error must not become a verdict about the node 25 minutes later (#530's review).
const (
	glibcProbeTries = 3
	glibcProbePause = 5 * time.Second
)

// glibcProbe is the srun step in the run's job that asks the node for its glibc, tried glibcProbeTries times. A step that still fails
// fails the run at once, naming the step, its exit code and its error, as the listing of Envoy does.
func (r *liveRun) glibcProbe(ctx context.Context, jobID string) GlibcRead {
	line, err := SrunLine(jobID)
	if err != nil {
		r.t.Fatalf("the node's glibc: %v", err)
	}
	script := GlibcScript
	if r.glibcScript != "" {
		script = r.glibcScript // a stand-in's node with another glibc
	}
	v, err := probeGlibc(ctx, r.tr.Run, line, script, glibcProbeTries, glibcProbePause, r.t.Logf)
	if err != nil {
		r.t.Fatalf("the node's glibc: %v", err)
	}
	return GlibcRead{OK: true, Version: v}
}

// probeGlibc runs the glibc step up to tries times, pause apart, and returns the version of the first answer getconf's form parses. Each
// failed try is logged with its exit code and its error; when every try failed, the error names the step and the last try's failure.
func probeGlibc(ctx context.Context, run func(context.Context, slurm.Command) (slurm.Result, error), line, script string, tries int, pause time.Duration, logf func(string, ...any)) (GlibcVersion, error) {
	var why string
	for i := 1; i <= tries; i++ {
		if i > 1 {
			time.Sleep(pause)
		}
		res, err := run(ctx, slurm.Command{Line: line, Stdin: []byte(script), Timeout: srunTimeout})
		switch {
		case err != nil:
			why = fmt.Sprintf("the srun step that runs getconf failed: %v (exit %d)", err, res.ExitCode)
		case res.ExitCode != 0:
			why = fmt.Sprintf("the srun step that runs getconf exited %d", res.ExitCode)
		default:
			if v, ok := ParseGetconfGlibc(res.Stdout); ok {
				return v, nil
			}
			why = fmt.Sprintf("the srun step that runs getconf answered %q, not getconf's \"glibc <major>.<minor>\"", clip(res.Stdout, 80))
		}
		logf("the node's glibc, try %d of %d: %s", i, tries, why)
	}
	return GlibcVersion{}, fmt.Errorf("%s, in each of %d tries", why, tries)
}

// clip is at most n bytes of b, for a log line.
func clip(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "..."
	}
	return string(b)
}

// engineVersionOf is the engine's vLLM version as the Instance's verification record holds it (EngineVersionOf), "" when it was not read.
func (r *liveRun) engineVersionOf(in v1alpha1.Instance) string {
	x := r.current(in)
	if x.Status.VerificationRecord == "" {
		r.t.Logf("the engine's version: the Instance names no verification record")
		return ""
	}
	var rec appsv1.ControllerRevision
	cctx, cancel := stack.CallCtx(r.t)
	defer cancel()
	if err := r.c.Get(cctx, client.ObjectKey{Name: x.Status.VerificationRecord, Namespace: x.Namespace}, &rec); err != nil {
		r.t.Logf("the engine's version: the verification record: %v", err)
		return ""
	}
	return EngineVersionOf(rec.Data.Raw)
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
		r.envoyConfig = envoyConfigArg(envoys[0].Argv)
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
func (r *liveRun) bytesReading(ctx context.Context, p Plan, key string) (BytesObs, []ControlVariant, []ControlRequest) {
	var vs []cases.Variant
	for _, c := range cases.All() {
		vs = append(vs, c.Variants()...)
	}
	obs := BytesObs{Expected: len(vs)}
	rctx, cancel := context.WithTimeout(ctx, BytesBound)
	defer cancel()
	model := r.w.md.Spec.Model
	// Each variant's front answer is kept by its place, and so is what was sent, so that the control sends the same bodies straight to the
	// engine and compares the two answers.
	front := make([]ControlVariant, len(vs))
	reqs := make([]ControlRequest, len(vs))
	for i, v := range vs {
		front[i] = ControlVariant{Key: v.Key(), Stream: v.Stream}
		reqs[i] = ControlRequest{Endpoint: string(v.Case.Endpoint), Body: AdaptBody(v.Body, model)}
	}
	type job struct {
		i int
		v cases.Variant
	}
	work := make(chan job)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range bytesWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for jb := range work {
				v := jb.v
				rp := r.fc.SendCtx(rctx, string(v.Case.Endpoint), string(reqs[jb.i].Body), key)
				if rp.Header != nil {
					r.keep(rp.Header)
				}
				// A response still awaited when the reading's own bound ran out was cut by the harness, not by the router.
				past := rp.Err != nil && rctx.Err() != nil
				mu.Lock()
				obs.Add(Answer{Chat: v.Case.Endpoint == cases.Chat, Stream: v.Stream, Valid: v.Valid(), Key: v.Key(),
					Status: rp.Status, Body: rp.Body, Cut: rp.Err != nil, PastBound: past})
				front[jb.i].Front, front[jb.i].FrontPastBound = Reply{Status: rp.Status, Body: rp.Body, Cut: rp.Err != nil}, past
				mu.Unlock()
			}
		}()
	}
	for i, v := range vs {
		select {
		case work <- job{i, v}:
		case <-rctx.Done():
		}
		if rctx.Err() != nil {
			break
		}
	}
	close(work)
	wg.Wait()
	p.Logf("%s: S13's cases at the response level: %d of %d variants sent, %d answered completely (%d with 200, %d with 4xx, %d with 5xx, %d other), %d made by the router or the path, %d awaited at the harness's bound",
		p.Row.Name, obs.Variants, obs.Expected, obs.Complete, obs.Status200, obs.Status4xx, obs.Status5xx, obs.StatusOther, obs.RouterMade, obs.PastBound)
	if len(obs.Failed) > 0 {
		p.Logf("%s: variants that did not finish or that the path failed (first %d): %s", p.Row.Name, len(obs.Failed), fmt.Sprint(obs.Failed))
	}
	return obs, front, reqs
}

// envoyConfigArg is the configuration file on Envoy's command line, its -c argument; "" when there is none.
func envoyConfigArg(argv []string) string {
	for i, a := range argv {
		if a == "-c" && i+1 < len(argv) {
			return argv[i+1]
		}
	}
	return ""
}

// directControl is the router row's direct-to-engine control (routercontrol.go; the science agent's ruling (a) on the router row, #522):
// every variant the front's reading sent, sent again straight to the worker's engine socket from inside the run's own job, and the two
// answers compared. Its failures to be made are named in Why, never a pass: the step that reads Envoy's configuration, the worker's socket in
// it, the step that sends the variants (tried as the glibc probe is), a socket that answered nothing.
func (r *liveRun) directControl(ctx context.Context, p Plan, first, in v1alpha1.Instance, front []ControlVariant, reqs []ControlRequest) ControlObs {
	not := func(why string) ControlObs {
		p.Logf("%s: the direct-to-engine control was not made: %s", p.Row.Name, why)
		return ControlObs{Why: why}
	}
	if r.envoyConfig == "" || !filepath.IsAbs(r.envoyConfig) {
		return not("Envoy's configuration was not named on its own command line")
	}
	key, err := agentapi.ReadKeyFile(agentapi.InstanceKeyPath(agentapi.Host{StateDir: r.h.State}.CredentialsDir(), string(in.UID)))
	if err != nil {
		return not("the Instance's key could not be read on the host")
	}
	jobID := r.ourJobFor(ctx, first, "the direct-to-engine control")
	line, err := SrunLine(jobID)
	if err != nil {
		return not("the srun step: " + err.Error())
	}
	if !plainPath.MatchString(r.envoyConfig) {
		return not("Envoy's configuration path has characters a step may not quote")
	}
	res, err := r.tr.Run(ctx, slurm.Command{Line: line, Stdin: []byte("cat '" + r.envoyConfig + "'\n"), Timeout: srunTimeout})
	if err != nil || res.ExitCode != 0 {
		return not(fmt.Sprintf("the srun step that reads Envoy's configuration on the node failed (exit %d, transport error %t)", res.ExitCode, err != nil))
	}
	sock, err := WorkerSocketOf(res.Stdout)
	if err != nil {
		return not(err.Error())
	}
	script, err := ControlScript(sock, key, reqs)
	if err != nil {
		return not(err.Error())
	}
	if r.controlScript != nil {
		script = r.controlScript(script)
	}
	cline, err := ControlSrunLine(jobID)
	if err != nil {
		return not("the control's srun step: " + err.Error())
	}
	cctx, cancel := context.WithTimeout(ctx, ControlBound)
	defer cancel()
	var out []byte
	var why string
	for i := 1; i <= controlTries; i++ {
		if i > 1 {
			time.Sleep(controlPause)
		}
		res, err := r.tr.Run(cctx, slurm.Command{Line: cline, Stdin: []byte(script), Timeout: ControlBound})
		switch {
		case err != nil:
			why = fmt.Sprintf("the srun step that sends the variants straight to the engine failed: %v (exit %d)", err, res.ExitCode)
		case res.ExitCode != 0:
			why = fmt.Sprintf("the srun step that sends the variants straight to the engine exited %d: %s", res.ExitCode, clip(res.Stdout, 120))
		default:
			out, why = res.Stdout, ""
		}
		if why == "" || cctx.Err() != nil {
			break
		}
		p.Logf("%s: the direct-to-engine control, try %d of %d: %s", p.Row.Name, i, controlTries, why)
	}
	if why != "" {
		return not(why + fmt.Sprintf(", in %d tries or by its bound of %s", controlTries, ControlBound))
	}
	replies, connected := ParseControl(out, len(reqs))
	if !connected {
		return not("the worker's engine socket answered no variant (curl could not connect to it)")
	}
	for i := range front {
		front[i].Direct = replies[i]
	}
	c := ControlOf(front)
	p.Logf("%s: the direct-to-engine control: %d sent, %d answered completely, %d compared; %d answered differently through the router, %d other than 200 alike on both paths",
		p.Row.Name, c.Sent, c.Answered, c.Compared, len(c.Differ), len(c.Alike))
	return c
}

// The control's step is tried as the glibc probe is: three times, five seconds apart.
const (
	controlTries = 3
	controlPause = 5 * time.Second
)
