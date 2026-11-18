package alpha

// The baseline row's live world (baseline.go): the seam of RunBaseline over the stack liveRun started, the real cluster, LLMLab, and #768's load
// generator run by its binary. Nothing here orders the steps or judges: RunBaseline and the judge do.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/obscheck"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// engineLogName is a worker's engine log in the Allocation's directory (internal/agent/workers.go:411-414: "engine-"+worker+".log"; the agent appends the
// engine's output there and never removes it). The unexported function is the agent's, so the name is held here.
const engineLogName = "engine-worker-0.log"

// engineKVLine reads the last "KV cache size: N tokens" line of an engine log on the login node, reduced to N with its commas (ParseLoggedKV); exit 3
// when there is no such file. path is checked by the caller (safePath).
func engineKVLine(path string) string {
	q := "'" + path + "'"
	return "if [ -r " + q + " ]; then sed -n -E 's/.*KV cache size: ([0-9,]+) tokens.*/\\1/p' -- " + q + " | tail -n 1; else exit 3; fi"
}

// liveBaseline is RunBaseline's world over a liveRun.
type liveBaseline struct {
	r  *liveRun
	p  Plan
	in v1alpha1.Instance // the served Instance, once ready
	// keyFile is the generator's mode-600 key file in the record directory, "" before the first phase.
	keyFile string
}

func (r *liveRun) Baseline(ctx context.Context, p Plan) (BaselineObs, error) {
	b := &liveBaseline{r: r, p: p}
	o, err := RunBaseline(ctx, b, BaselineSettings{Phases: r.cfg.Phases, Cap: int(p.Row.InstanceCap), OmitMinTokens: r.cfg.OmitMinTokens, Logf: p.Logf})
	if err != nil {
		return BaselineObs{}, err
	}
	if o.Tags, err = r.runTags(); err != nil {
		return BaselineObs{}, err
	}
	return o, nil
}

func (b *liveBaseline) Now() time.Time { return time.Now() }

// front is a client of the front for the plain model; the served deployment's own is made when it is created (liveRun.createServed), after P0.
func (b *liveBaseline) front() *stack.Client {
	r := b.r
	return &stack.Client{T: r.t, H: r.h, Model: BaselineModel, MaxRequest: r.tm.MaxRequestDuration}
}

func (b *liveBaseline) AttachManaged(ctx context.Context) (ManagedMD, error) {
	r := b.r
	r.startStack(b.p)
	if r.w.managed == nil {
		return ManagedMD{}, errors.New("the world holds no managed ModelDeployment")
	}
	stack.Create(r.t, r.c, r.w.managed.DeepCopy())
	var md v1alpha1.ModelDeployment
	cctx, cancel := stack.CallCtx(r.t)
	defer cancel()
	if err := r.c.Get(cctx, client.ObjectKey{Name: r.w.managed.Name, Namespace: r.w.managed.Namespace}, &md); err != nil {
		return ManagedMD{}, fmt.Errorf("the managed ModelDeployment: %w", err)
	}
	m := ManagedMD{Model: md.Spec.Model, Providers: md.Spec.Providers.Names}
	if md.Spec.Source.Managed != nil {
		m.Platform = md.Spec.Source.Managed.Platform
	}
	return m, nil
}

// Probe is P0. The managed Provider lists LLMLab's catalog once a minute, so the managed target may not answer at first: the front answers 503 itself
// until then, and the request is sent again, for at most BaselineP0Wait. The row it returns is the last one.
func (b *liveBaseline) Probe(ctx context.Context) (ProbeObs, error) {
	fc := b.front()
	minTokens := P0Tokens
	if b.r.cfg.OmitMinTokens {
		minTokens = 0
	}
	body := stack.ChatBody(BaselineModel, P0Tokens, minTokens, true, "")
	deadline := time.Now().Add(BaselineP0Wait)
	for {
		rp := fc.Send("/v1/chat/completions", body, "baseline-p0")
		if rp.Err != nil || rp.ID == "" {
			return ProbeObs{}, errors.New("P0's request got no answer from the front")
		}
		row, err := FrontRowOf(fc.RowOf(rp.ID))
		if err != nil {
			return ProbeObs{}, err
		}
		if rp.Status == http.StatusOK || row.Emitter != "front" || !time.Now().Before(deadline) {
			return ProbeObs{Sent: true, Row: row}, nil
		}
		select {
		case <-ctx.Done():
			return ProbeObs{}, ctx.Err()
		case <-time.After(10 * time.Second):
		}
	}
}

func (b *liveBaseline) StartServed(ctx context.Context) (ServedReady, error) {
	r := b.r
	r.createServed(b.p)
	in, err := r.firstReady(b.p)
	if err != nil {
		return ServedReady{}, err
	}
	b.in = in
	var sr ServedReady
	if in.Status.ReadyTime != nil {
		sr.ReadyAt = in.Status.ReadyTime.Time
	}
	// The Instance drains at the deployment's end, which firstReady moved to a minute before its own drain start.
	sr.DrainStart = r.oneEnd
	sr.Deployment = r.observedDeployment()
	var md v1alpha1.ModelDeployment
	cctx, cancel := stack.CallCtx(r.t)
	defer cancel()
	if err := r.c.Get(cctx, client.ObjectKey{Name: r.w.md.Name, Namespace: r.w.md.Namespace}, &md); err != nil {
		return ServedReady{}, fmt.Errorf("the served ModelDeployment: %w", err)
	}
	sr.Served = ServedMD{Model: md.Spec.Model, Providers: md.Spec.Providers.Names, Instances: md.Spec.Instances, OverAllocation: md.Spec.OverAllocation, HedgeWidth: md.Spec.HedgeWidth,
		DeploymentID: in.Labels[v1alpha1.LabelDeploymentID]}
	if md.Spec.Routing != nil {
		sr.Served.InstanceCap = md.Spec.Routing.InstanceCap
	}
	return sr, nil
}

// LoggedKV reads the engine's logged KV from engine-worker-0.log in the Allocation's directory over the run's ssh, as row A reads the agent's log. The
// path, which holds the login and the staging directory, goes to no log.
func (b *liveBaseline) LoggedKV(ctx context.Context) (int64, bool) {
	r := b.r
	tag := r.tagOf(b.in)
	if !tagRE.MatchString(tag) {
		return 0, false
	}
	paths, err := r.batch.Paths(ctx)
	if err != nil {
		r.t.Logf("the staging directory could not be read for the engine's log: %v", err)
		return 0, false
	}
	path := paths.Staging + "/" + agentapi.AllocationDir(tag) + "/" + engineLogName
	if !safePath.MatchString(path) {
		return 0, false
	}
	res, err := r.tr.Run(ctx, slurm.Command{Line: engineKVLine(path), Timeout: time.Minute})
	if err != nil || res.ExitCode != 0 {
		r.t.Logf("the engine's log could not be read for its KV cache size")
		return 0, false
	}
	return ParseLoggedKV(res.Stdout)
}

func (b *liveBaseline) Gate(ctx context.Context) (GateObs, error) {
	fc := b.front()
	body := stack.ChatBody(BaselineModel+"@"+b.r.w.provider, 1, 0, false, "")
	deadline := time.Now().Add(stack.WaitDeadline)
	for {
		rp := fc.Send("/v1/chat/completions", body, "baseline-gate")
		if rp.Err != nil || rp.ID == "" {
			return GateObs{}, errors.New("the gate's request got no answer from the front")
		}
		row, err := FrontRowOf(fc.RowOf(rp.ID))
		if err != nil {
			return GateObs{}, err
		}
		if rp.Status == http.StatusOK || !time.Now().Before(deadline) {
			return GateObs{Sent: true, Row: row}, nil
		}
		select {
		case <-ctx.Done():
			return GateObs{}, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// LLMLabCount is the front's request metric for the managed Provider, summed over its codes. A counter has no series before its first increment, so a
// scrape that succeeds and shows none is 0; the judge reads this against the usage records, which a label that never matched cannot satisfy.
func (b *liveBaseline) LLMLabCount(ctx context.Context) (float64, bool) {
	ss, err := obscheck.Scrape(b.r.h.Ports.URL(4, "/metrics"))
	if err != nil {
		return 0, false
	}
	var n float64
	for _, s := range obscheck.Find(ss, "hpcburst_front_requests_total", map[string]string{"provider": LLMLabProviderName}) {
		n += s.Value
	}
	return n, true
}

// ServedEligible: the served Instance's phase is ready and its AgentUnreachable condition is not true.
func (b *liveBaseline) ServedEligible(ctx context.Context) bool {
	for _, x := range b.r.instances() {
		if x.UID != b.in.UID {
			continue
		}
		if x.Status.Phase != v1alpha1.InstanceReady {
			return false
		}
		c := meta.FindStatusCondition(x.Status.Conditions, v1alpha1.ConditionAgentUnreachable)
		return c == nil || c.Status != "True"
	}
	return false
}

// EngineSample reads the controller's /metrics for the served Instance's worker-0 gauges. A gauge that is absent makes the sample none: an unknown is never 0.
func (b *liveBaseline) EngineSample(ctx context.Context) (EngineSample, bool) {
	ss, err := obscheck.Scrape(b.r.h.Ports.URL(12, "/metrics"))
	if err != nil {
		return EngineSample{}, false
	}
	want := map[string]string{"instance_uid": string(b.in.UID), "worker": "worker-0"}
	var v [4]float64
	for i, g := range []string{obscheck.GaugeRunning, obscheck.GaugeWaiting, obscheck.GaugePreempt, obscheck.GaugeKV} {
		f := obscheck.Find(ss, g, want)
		if len(f) != 1 {
			return EngineSample{}, false
		}
		v[i] = f[0].Value
	}
	return EngineSample{At: time.Now(), Running: v[0], Waiting: v[1], Preemptions: v[2], KVUsage: v[3]}, true
}

// EndServed is the run's end: the served deployment goes, and the cluster's record shows its job ended (liveRun.endDeployment). The release removes
// the Allocation's directory (internal/providerctl/delete.go:110), so the engine's log is read before it (LoggedKV) and never after.
func (b *liveBaseline) EndServed(ctx context.Context) error {
	b.r.endDeployment(ctx, b.p)
	return nil
}

func (b *liveBaseline) DetachManaged(ctx context.Context) error {
	r := b.r
	cctx, cancel := stack.CallCtx(r.t)
	defer cancel()
	md := r.w.managed.DeepCopy()
	if err := r.c.Delete(cctx, md); err != nil {
		return fmt.Errorf("delete the managed ModelDeployment: %w", err)
	}
	return nil
}

// ManagedDetached asks the front for the managed Provider's level: with no managed ModelDeployment attached it answers 503 itself, and with one LLMLab
// would answer (one token of its own).
func (b *liveBaseline) ManagedDetached(ctx context.Context) bool {
	fc := b.front()
	rp := fc.Send("/v1/chat/completions", stack.ChatBody(BaselineModel+"@"+LLMLabProviderName, 1, 0, false, ""), "baseline-detach")
	return rp.Err == nil && rp.Status == http.StatusServiceUnavailable && rp.Header.Get("Hpcburst-Emitter") == "front"
}

func (b *liveBaseline) FrontRows(ctx context.Context, since time.Time, want []string) ([]FrontRow, error) {
	fc := b.front()
	var out []FrontRow
	read := func() (bool, error) {
		out = out[:0]
		have := map[string]bool{}
		for _, u := range fc.UsageRows() {
			row, err := FrontRowOf(u)
			if err != nil {
				return false, err
			}
			if row.Start.Before(since) {
				continue
			}
			have[row.RequestID] = true
			out = append(out, row)
		}
		for _, id := range want {
			if !have[id] {
				return false, nil
			}
		}
		return true, nil
	}
	deadline := time.Now().Add(stack.WaitDeadline)
	for {
		done, err := read()
		if err != nil {
			return nil, err
		}
		if done || !time.Now().Before(deadline) {
			return append([]FrontRow(nil), out...), nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// liveGenerator is one run of the generator's binary.
type liveGenerator struct {
	done    chan struct{}
	cancel  context.CancelFunc
	recPath string
	mu      sync.Mutex
	stopped bool
	err     error
}

func (g *liveGenerator) Done() <-chan struct{} { return g.done }

func (g *liveGenerator) Stop() {
	g.mu.Lock()
	g.stopped = true
	g.mu.Unlock()
	g.cancel()
}

// Result reads the records the run wrote. An exit status of 1 is a run some of whose requests failed (the front's records say which, never the
// generator's), and one the row stopped is interrupted by design; any other status, or records that cannot be read, is a run that broke.
func (g *liveGenerator) Result() ([]GenRow, error) {
	g.mu.Lock()
	err, stopped := g.err, g.stopped
	g.mu.Unlock()
	var ee *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &ee) && ee.ExitCode() == 1:
	case stopped:
	default:
		return nil, fmt.Errorf("the generator ended with an error (%v)", exitKind(err))
	}
	f, err := os.Open(g.recPath)
	if err != nil {
		return nil, errors.New("the generator's records could not be opened")
	}
	defer f.Close()
	return ParseGenRecords(io.Reader(f))
}

// exitKind names an error by its kind, never its text.
func exitKind(err error) string {
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return "exit status " + strconv.Itoa(ee.ExitCode())
	}
	return "it could not be run"
}

// StartGenerator writes the phase's config and the key file, both mode 600, into the run's private record directory, and runs the generator's binary
// on them: -config and -out are all it takes. The key is the stack's client key and goes to no log and no command line.
func (b *liveBaseline) StartGenerator(ctx context.Context, ph Phase) (Generator, error) {
	r, rec := b.r, b.p.Rec
	if b.keyFile == "" {
		if err := rec.SaveText("studyload.key", r.h.ClientKey+"\n"); err != nil {
			return nil, errors.New("the generator's key file could not be written")
		}
		b.keyFile = filepath.Join(r.cfg.Out, "studyload.key")
	}
	cfgJSON, err := StudyConfig(ph, StudyRun{BaseURL: "http://" + r.h.Ports.ClientAddr + "/v1", APIKeyFile: b.keyFile, RunID: PhaseRunID(r.cfg.RunID, ph.Name), ServedProvider: r.w.provider,
		OmitMinTokens: r.cfg.OmitMinTokens, RequestTimeout: r.tm.MaxRequestDuration})
	if err != nil {
		return nil, err
	}
	base := "studyload-" + ph.Name
	if err := rec.SaveText(base+".json", string(cfgJSON)); err != nil {
		return nil, errors.New("the generator's config could not be written")
	}
	g := &liveGenerator{done: make(chan struct{}), recPath: filepath.Join(r.cfg.Out, base+".jsonl")}
	gctx, cancel := context.WithCancel(ctx)
	g.cancel = cancel
	cmd := exec.CommandContext(gctx, r.cfg.StudyloadBin, "-config", filepath.Join(r.cfg.Out, base+".json"), "-out", g.recPath)
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	// Its summary and its errors stay in the private record directory: they hold the route names and may hold more.
	var outs []*os.File
	for _, name := range []string{base + ".out", base + ".err"} {
		f, ferr := os.OpenFile(filepath.Join(r.cfg.Out, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if ferr != nil {
			cancel()
			return nil, errors.New("the generator's output file could not be made")
		}
		outs = append(outs, f)
	}
	cmd.Stdout, cmd.Stderr = outs[0], outs[1]
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 30 * time.Second
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, errors.New("the generator's binary could not be started")
	}
	go func() {
		werr := cmd.Wait()
		for _, f := range outs {
			_ = f.Close()
		}
		g.mu.Lock()
		g.err = werr
		g.mu.Unlock()
		cancel()
		close(g.done)
	}()
	return g, nil
}
