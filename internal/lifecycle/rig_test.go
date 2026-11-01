package lifecycle

// The scenario rig: the lifecycle controller against the provider fake
// (pkg/provider/fake.Controller over fake.Batch), with one fake agent
// (pkg/agentapi/fake.Agent) per running job, talking through fake.Gate, which
// plays the agent API server's authentication step (issue #60). Time is a
// manual clock: a test moves it, and Step runs one round of the provider, the
// agents and the controller. RequeueAfter is ignored: every round reconciles
// every ModelDeployment.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const (
	rigNS        = "example-ns"
	rigProvider  = "helios-gpu"
	rigProfile   = "helios"
	rigPartition = "gpu-part"
	rigGPU       = "GH200"
	rigArch      = "aarch64"
	rigModel     = "example-model"
	rigDigest    = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	rigRevision  = "0123456789abcdef0123456789abcdef01234567"
)

// rigStart is the rig clock's start: a whole second, in UTC.
var rigStart = time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)

// rigAgentDigest is the agent build digest the rig's Config hands out.
var rigAgentDigest = strings.Repeat("a", 64)

// rigFiles is the checkpoint's per-file list the stub source returns.
var rigFiles = []deploymentid.File{
	{Path: "config.json", SHA256: strings.Repeat("1", 64)},
	{Path: "model.safetensors", SHA256: strings.Repeat("2", 64)},
}

// stubSource is a CheckpointSource with a fixed file list and no checkpoint
// template.
type stubSource struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (s *stubSource) Files(context.Context, v1alpha1.CheckpointSpec) ([]deploymentid.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return append([]deploymentid.File(nil), rigFiles...), nil
}

func (s *stubSource) TemplateSources(context.Context, v1alpha1.CheckpointSpec, []deploymentid.File) (deploymentid.TemplateSources, error) {
	return deploymentid.TemplateSources{}, nil
}

// switchBackend lets the rig replace the controller (a restart) under agents
// that keep their connection: the next call reaches the new Backend.
type switchBackend struct {
	mu sync.Mutex
	b  agentapi.Backend
}

func (s *switchBackend) get() agentapi.Backend  { s.mu.Lock(); defer s.mu.Unlock(); return s.b }
func (s *switchBackend) set(b agentapi.Backend) { s.mu.Lock(); defer s.mu.Unlock(); s.b = b }

func (s *switchBackend) Register(ctx context.Context, c agentapi.Caller, r agentapi.RegisterRequest) (agentapi.RegisterResponse, error) {
	return s.get().Register(ctx, c, r)
}

func (s *switchBackend) Desired(ctx context.Context, c agentapi.Caller) (agentapi.DesiredState, error) {
	return s.get().Desired(ctx, c)
}

func (s *switchBackend) Report(ctx context.Context, c agentapi.Caller, r agentapi.Report) error {
	return s.get().Report(ctx, c, r)
}

// rigAgent is one fake agent and the job it runs in.
type rigAgent struct {
	alloc string
	jobID string
	agent *afake.Agent
	// stopped: the job ended, so the agent no longer runs.
	stopped bool
}

type rig struct {
	t      *testing.T
	ctx    context.Context
	clock  *pfake.Clock
	batch  *pfake.Batch
	prov   *pfake.Controller
	c      client.Client
	source *stubSource
	rec    *MemoryRecorder
	creds  string

	r       *Reconciler
	backend *Backend
	sw      *switchBackend
	gate    *afake.Gate

	agents map[string]*rigAgent // by Allocation name
	// engine returns the engine double for a new agent on an Allocation;
	// nil gives the default engine, which passes every check.
	engine func(alloc string) afake.Engine
	// noAgents: jobs start but no agent registers (a dial-home outage).
	noAgents bool
	// onAgent, when set, runs for each new agent before its first Step.
	onAgent func(*rigAgent)
	// errs collects agent errors; a test asserts on them when it cares.
	errs []error
	// noTunnels: syncAgents does not play the path-state writer
	// (syncTunnels), so a test sets TunnelConnected itself, or leaves it
	// absent.
	noTunnels bool
}

type rigOption func(*rigConfig)

type rigConfig struct {
	profile      v1alpha1.PlatformProfileSpec
	maxTime      time.Duration
	holdRelease  bool
	front        bool
	interceptors *interceptor.Funcs
}

func withProfile(f func(*v1alpha1.PlatformProfileSpec)) rigOption {
	return func(c *rigConfig) { f(&c.profile) }
}

func withMaxTime(d time.Duration) rigOption { return func(c *rigConfig) { c.maxTime = d } }
func withoutFront() rigOption               { return func(c *rigConfig) { c.front = false } }
func withFallbackMode() rigOption           { return func(c *rigConfig) { c.holdRelease = false } }
func withInterceptors(f interceptor.Funcs) rigOption {
	return func(c *rigConfig) { c.interceptors = &f }
}

func ptrTo[T any](v T) *T { return &v }

func dur(d time.Duration) *v1alpha1.Duration { v := v1alpha1.Duration(d.String()); return &v }

// newUID returns a random lowercase UUID, the form the API server gives.
func newUID() types.UID {
	var b [16]byte
	_, _ = rand.Read(b[:])
	h := hex.EncodeToString(b[:])
	return types.UID(h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32])
}

func newRig(t *testing.T, opts ...rigOption) *rig {
	t.Helper()
	cfg := rigConfig{maxTime: 48 * time.Hour, holdRelease: true, front: true}
	for _, o := range opts {
		o(&cfg)
	}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	// The front's Lease, which the observer reads (HBEP-8, Observers).
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	clock := pfake.NewClock(rigStart)
	funcs := interceptor.Funcs{}
	if cfg.interceptors != nil {
		funcs = *cfg.interceptors
	}
	userCreate := funcs.Create
	funcs.Create = func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if obj.GetUID() == "" {
			obj.SetUID(newUID())
		}
		if ts := obj.GetCreationTimestamp(); ts.IsZero() {
			obj.SetCreationTimestamp(metav1.NewTime(clock.Now()))
		}
		if userCreate != nil {
			return userCreate(ctx, c, obj, opts...)
		}
		return c.Create(ctx, obj, opts...)
	}
	prof := &v1alpha1.PlatformProfile{
		ObjectMeta: metav1.ObjectMeta{Name: rigProfile},
		Spec:       cfg.profile,
	}
	prof.Spec.ProviderKind = v1alpha1.PlatformBatch
	prof.Spec.HoldRelease = cfg.holdRelease
	prof.Spec.Partitions = []v1alpha1.PartitionProfile{{
		Name: rigPartition, AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: rigGPU, CPUArch: rigArch},
		MaxTime: dur(cfg.maxTime), GPUsPerNode: ptrTo(int32(4)), CPUsPerGPU: ptrTo(int32(16)),
		MemPerGPU: ptrTo(resource.MustParse("120Gi")),
	}}
	provider := &v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: rigProvider, UID: newUID(), Generation: 1,
			Labels: map[string]string{v1alpha1.LabelPlatform: "helios"}},
		Spec: v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: rigProfile}, AllowedPartitions: []string{rigPartition}},
	}
	c := clientfake.NewClientBuilder().WithScheme(scheme).
		WithObjects(prof, provider).
		WithStatusSubresource(&v1alpha1.ModelDeployment{}, &v1alpha1.Instance{}, &v1alpha1.Allocation{}, &v1alpha1.Provider{}, &v1alpha1.PlatformProfile{}).
		WithInterceptorFuncs(funcs).
		Build()
	batch := pfake.NewBatch(clock, pfake.Config{Partitions: []pfake.Partition{{Name: rigPartition, GPUModel: rigGPU, GPUsPerNode: 4, MaxTime: cfg.maxTime}}})
	rg := &rig{
		t: t, ctx: context.Background(), clock: clock, batch: batch, c: c,
		prov:   &pfake.Controller{Client: c, Provider: rigProvider, Batch: batch, Clock: clock, HoldRelease: cfg.holdRelease},
		source: &stubSource{}, rec: &MemoryRecorder{}, creds: t.TempDir(),
		agents: map[string]*rigAgent{}, sw: &switchBackend{},
	}
	rg.gate = afake.NewGate(rg.sw)
	rg.startController(cfg.front)
	return rg
}

// startController builds a fresh Reconciler and Backend over the same store:
// a controller start or restart. Everything in memory is lost; agents
// register again on their next call (CodeNotRegistered).
func (rg *rig) startController(front bool) {
	rg.backend = &Backend{Reader: rg.c, Clock: rg.clock, CredentialsDir: rg.creds}
	rg.r = &Reconciler{
		Client: rg.c, Reader: rg.c, Clock: rg.clock, Backend: rg.backend, Checkpoints: rg.source, Recorder: rg.rec,
		Config: Config{CredentialsDir: rg.creds, FrontConfigured: front,
			AgentDigest: func(string) (string, error) { return rigAgentDigest, nil }},
	}
	rg.sw.set(rg.backend)
}

// restartController is a controller killed and started again.
func (rg *rig) restartController() { rg.startController(rg.r.Config.FrontConfigured) }

// mdSpec returns a served deployment of one instance on the rig's Provider.
func mdSpec(name string) *v1alpha1.ModelDeployment {
	return &v1alpha1.ModelDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: rigNS, Generation: 1},
		Spec: v1alpha1.ModelDeploymentSpec{
			Model: rigModel,
			Source: v1alpha1.ModelSource{Served: &v1alpha1.ServingSpec{
				Checkpoint: v1alpha1.CheckpointSpec{Repository: "example-org/example-model", Revision: rigRevision},
				Engine:     v1alpha1.EngineSpec{Name: "vllm", Version: "0.32.0", ImageDigest: rigDigest, Image: "docker.io/vllm/vllm-openai"},
				Flags:      []v1alpha1.EngineFlag{{Name: "max-model-len", Value: "32768"}},
			}},
			Instances: 1,
			Providers: v1alpha1.ProviderSelection{Names: []string{rigProvider}},
			Workload: &v1alpha1.Workload{
				ContextTokens:  v1alpha1.ContextTokens{Typical: 4096},
				OutputTokens:   v1alpha1.OutputTokens{Typical: 512},
				TargetSessions: 4,
			},
		},
	}
}

// create stores an object.
func (rg *rig) create(obj client.Object) {
	rg.t.Helper()
	if err := rg.c.Create(rg.ctx, obj); err != nil {
		rg.t.Fatalf("create %T %s: %v", obj, obj.GetName(), err)
	}
}

// mutateMD reads, changes and updates a ModelDeployment's spec.
func (rg *rig) mutateMD(name string, f func(*v1alpha1.ModelDeployment)) {
	rg.t.Helper()
	md := rg.md(name)
	f(md)
	md.Generation++
	if err := rg.c.Update(rg.ctx, md); err != nil {
		rg.t.Fatalf("update ModelDeployment %s: %v", name, err)
	}
}

func (rg *rig) md(name string) *v1alpha1.ModelDeployment {
	rg.t.Helper()
	var md v1alpha1.ModelDeployment
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Namespace: rigNS, Name: name}, &md); err != nil {
		rg.t.Fatalf("get ModelDeployment %s: %v", name, err)
	}
	return &md
}

// instances returns the Instances of a deployment, oldest first.
func (rg *rig) instances(md string) []v1alpha1.Instance {
	rg.t.Helper()
	var l v1alpha1.InstanceList
	if err := rg.c.List(rg.ctx, &l, client.InNamespace(rigNS), client.MatchingLabels{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(md)}); err != nil {
		rg.t.Fatal(err)
	}
	sort.Slice(l.Items, func(i, j int) bool {
		if !l.Items[i].CreationTimestamp.Equal(&l.Items[j].CreationTimestamp) {
			return l.Items[i].CreationTimestamp.Before(&l.Items[j].CreationTimestamp)
		}
		return l.Items[i].Name < l.Items[j].Name
	})
	return l.Items
}

// liveInstances returns the Instances that are not ended.
func (rg *rig) liveInstances(md string) []v1alpha1.Instance {
	var out []v1alpha1.Instance
	for _, in := range rg.instances(md) {
		if in.Status.Phase != v1alpha1.InstanceEnded {
			out = append(out, in)
		}
	}
	return out
}

// instance returns one Instance by name.
func (rg *rig) instance(name string) *v1alpha1.Instance {
	rg.t.Helper()
	var in v1alpha1.Instance
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Namespace: rigNS, Name: name}, &in); err != nil {
		rg.t.Fatalf("get Instance %s: %v", name, err)
	}
	return &in
}

// allocations returns every Allocation of a deployment, oldest first.
func (rg *rig) allocations(md string) []v1alpha1.Allocation {
	rg.t.Helper()
	var l v1alpha1.AllocationList
	if err := rg.c.List(rg.ctx, &l, client.InNamespace(rigNS), client.MatchingLabels{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(md)}); err != nil {
		rg.t.Fatal(err)
	}
	sort.Slice(l.Items, func(i, j int) bool {
		if !l.Items[i].CreationTimestamp.Equal(&l.Items[j].CreationTimestamp) {
			return l.Items[i].CreationTimestamp.Before(&l.Items[j].CreationTimestamp)
		}
		return l.Items[i].Name < l.Items[j].Name
	})
	return l.Items
}

// allocation returns one Allocation by name, and false once it is gone.
func (rg *rig) allocation(name string) (*v1alpha1.Allocation, bool) {
	var a v1alpha1.Allocation
	err := rg.c.Get(rg.ctx, client.ObjectKey{Namespace: rigNS, Name: name}, &a)
	if err != nil {
		return nil, false
	}
	return &a, true
}

// condition returns a ModelDeployment's condition of a type; Status "" when
// absent.
func (rg *rig) condition(md, typ string) metav1.Condition {
	for _, c := range rg.md(md).Status.Conditions {
		if c.Type == typ {
			return c
		}
	}
	return metav1.Condition{}
}

// Step runs one round: the provider's pass, the gate's table from the
// Allocations, one Step of every agent whose job runs, then a pass over every
// ModelDeployment.
func (rg *rig) Step() {
	rg.t.Helper()
	if err := rg.prov.Sync(rg.ctx); err != nil {
		rg.t.Fatalf("provider sync: %v", err)
	}
	rg.syncAgents()
	rg.reconcileAll()
	// The leader's observer judges the Providers' and the front's heartbeats
	// beside the reconcile loop (HBEP-8, Observers).
	if err := rg.r.checkHeartbeats(rg.ctx); err != nil {
		rg.t.Fatalf("checkHeartbeats: %v", err)
	}
}

// syncTunnels plays #90's path-state writer, which sets TunnelConnected from
// frps's proxy list (HBEP-6, Path state): the agent starts frpc when it runs
// the Instance, so every Instance that has not ended, on an Allocation whose
// agent runs, has its proxy online, True with reason ProxyOnline. It writes
// only a change, as the writer does, and a write that fails (a test's injected
// fault) is kept in rg.errs and tried again next round, as the writer retries.
// A test that sets the condition itself sets rg.noTunnels.
func (rg *rig) syncTunnels() {
	rg.t.Helper()
	var l v1alpha1.InstanceList
	if err := rg.c.List(rg.ctx, &l); err != nil {
		rg.t.Fatal(err)
	}
	for i := range l.Items {
		in := &l.Items[i]
		ra, ok := rg.agents[in.Spec.AllocationRef.Name]
		if ended(in) || !ok || ra.stopped || isTrue(in.Status.Conditions, v1alpha1.ConditionTunnelConnected) {
			continue
		}
		apimeta.SetStatusCondition(&in.Status.Conditions, metav1.Condition{
			Type: v1alpha1.ConditionTunnelConnected, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonProxyOnline,
			Message: "frps lists the Instance's proxy as online", LastTransitionTime: metav1.NewTime(rg.clock.Now()),
		})
		if err := rg.c.Status().Update(rg.ctx, in); err != nil {
			rg.errs = append(rg.errs, fmt.Errorf("path-state writer: set TunnelConnected on %s: %w", in.Name, err))
		}
	}
}

// syncAgents keeps the gate's table and the agents in line with the jobs: a
// job that runs gets an agent; one that ended stops its agent.
func (rg *rig) syncAgents() {
	rg.t.Helper()
	var l v1alpha1.AllocationList
	if err := rg.c.List(rg.ctx, &l); err != nil {
		rg.t.Fatal(err)
	}
	for i := range l.Items {
		a := &l.Items[i]
		tag := a.Annotations[v1alpha1.AnnotationExternalName]
		jobID := a.Annotations[v1alpha1.AnnotationJobID]
		cred, err := os.ReadFile(agentapi.AllocationCredentialPath(rg.creds, string(a.UID)))
		if tag == "" || err != nil {
			continue
		}
		rg.gate.Add(tag, afake.GateAllocation{Namespace: a.Namespace, Name: a.Name, UID: string(a.UID), Credential: string(cred), JobID: jobID})
		ra, ok := rg.agents[a.Name]
		if !ok && jobID != "" && !rg.noAgents {
			job, found := rg.batch.Job(jobID)
			if found && job.StartTime != nil && job.EndTime == nil {
				var e afake.Engine
				if rg.engine != nil {
					e = rg.engine(a.Name)
				}
				ra = &rigAgent{alloc: a.Name, jobID: jobID,
					agent: afake.NewAgent(rg.gate.As(string(cred), tag, jobID), agentapi.Caller{}, rg.clock.Now, e)}
				rg.agents[a.Name] = ra
				if rg.onAgent != nil {
					rg.onAgent(ra)
				}
			}
		}
	}
	for _, ra := range rg.agents {
		if ra.stopped {
			continue
		}
		if job, ok := rg.batch.Job(ra.jobID); !ok || job.EndTime != nil {
			ra.stopped = true
			continue
		}
		if err := ra.agent.Step(rg.ctx); err != nil {
			rg.errs = append(rg.errs, fmt.Errorf("agent of %s: %w", ra.alloc, err))
		}
	}
	// The tunnel follows the agent: frpc starts when the agent runs the
	// Instance (HBEP-6), so every path that moves the agents moves it too.
	if !rg.noTunnels {
		rg.syncTunnels()
	}
}

// reconcileAll runs one pass over every ModelDeployment, and over the
// deployments named by an Allocation or Instance whose ModelDeployment is gone.
func (rg *rig) reconcileAll() {
	rg.t.Helper()
	keys := map[types.NamespacedName]bool{}
	var mds v1alpha1.ModelDeploymentList
	if err := rg.c.List(rg.ctx, &mds); err != nil {
		rg.t.Fatal(err)
	}
	for _, md := range mds.Items {
		keys[types.NamespacedName{Namespace: md.Namespace, Name: md.Name}] = true
	}
	var al v1alpha1.AllocationList
	if err := rg.c.List(rg.ctx, &al); err != nil {
		rg.t.Fatal(err)
	}
	for _, a := range al.Items {
		if a.Spec.ModelDeploymentRef != nil {
			keys[types.NamespacedName{Namespace: a.Namespace, Name: a.Spec.ModelDeploymentRef.Name}] = true
		}
	}
	var il v1alpha1.InstanceList
	if err := rg.c.List(rg.ctx, &il); err != nil {
		rg.t.Fatal(err)
	}
	for _, in := range il.Items {
		for _, o := range in.OwnerReferences {
			keys[types.NamespacedName{Namespace: in.Namespace, Name: o.Name}] = true
		}
	}
	sorted := make([]types.NamespacedName, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].String() < sorted[j].String() })
	for _, k := range sorted {
		if _, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: k}); err != nil {
			rg.t.Fatalf("reconcile %s at %s: %v", k, rg.clock.Now().Format(time.RFC3339), err)
		}
	}
}

// run advances the clock by tick and steps, until total has passed.
func (rg *rig) run(total, tick time.Duration) {
	rg.t.Helper()
	for end := rg.clock.Now().Add(total); rg.clock.Now().Before(end); {
		rg.clock.Advance(tick)
		rg.Step()
	}
}

// until advances by tick and steps until cond holds, failing after limit.
func (rg *rig) until(limit, tick time.Duration, what string, cond func() bool) {
	rg.t.Helper()
	for end := rg.clock.Now().Add(limit); ; {
		if cond() {
			return
		}
		if !rg.clock.Now().Before(end) {
			rg.t.Fatalf("%s: not reached within %s (now %s)", what, limit, rg.clock.Now().Format(time.RFC3339))
		}
		rg.clock.Advance(tick)
		rg.Step()
	}
}

// readyCount returns how many of a deployment's Instances are ready.
func (rg *rig) readyCount(md string) int {
	n := 0
	for _, in := range rg.instances(md) {
		if in.Status.Phase == v1alpha1.InstanceReady {
			n++
		}
	}
	return n
}

// agentOf returns the fake agent of an Allocation.
func (rg *rig) agentOf(alloc string) *rigAgent {
	rg.t.Helper()
	ra, ok := rg.agents[alloc]
	if !ok {
		rg.t.Fatalf("no agent runs on Allocation %s", alloc)
	}
	return ra
}

// submissions counts the sbatch calls the fake Slurm received.
func (rg *rig) submissions() int { return rg.batch.Count("sbatch") }
