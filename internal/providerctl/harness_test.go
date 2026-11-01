package providerctl

// The test harness: crossplane-runtime's managed reconciler, built by New,
// driven by calling Reconcile directly over controller-runtime's fake client,
// against pkg/provider/fake.Batch (the protocol) through a small adapter that
// adds the Platform reads fake.Batch does not have. Every time comes from one
// manual clock. Submissions and lifts are counted from fake.Batch's call log,
// never from return values (HBEP-4, Test plan).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const (
	testProvider = "example-provider"
	testNS       = "example-ns"
	gpuPart      = "example-gpu"
	testArch     = "x86_64"
)

func at(h, m, s int) time.Time { return time.Date(2026, 10, 30, h, m, s, 0, time.UTC) }

// fakePlatform adapts fake.Batch to Platform.
type fakePlatform struct {
	*fake.Batch
	clock *fake.Clock

	mu          sync.Mutex
	busy        map[string]bool
	paths       slurm.Paths
	pathErr     error
	keysPresent bool
	pollErr     error
	lastListing time.Time
	keyChecks   int
}

var _ Platform = (*fakePlatform)(nil)

func (p *fakePlatform) ListByTag(ctx context.Context, tag string) ([]provider.Job, error) {
	jobs, err := p.FindByTag(ctx, tag, time.Time{})
	if err != nil {
		return nil, err
	}
	var out []provider.Job
	for _, j := range jobs {
		if j.Source == provider.SourceQueue {
			out = append(out, j)
		}
	}
	return out, nil
}

func (p *fakePlatform) Paths(context.Context) (slurm.Paths, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.paths, p.pathErr
}

func (p *fakePlatform) DialHomeKeysPresent(context.Context) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.keyChecks++
	return p.keysPresent, nil
}

func (p *fakePlatform) SetBusy(tag string, busy bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if busy {
		p.busy[tag] = true
	} else {
		delete(p.busy, tag)
	}
}

// Poll is fake.Batch's (it logs "squeue-me" and fails with FailReads), plus a
// scripted failure, and remembers the last successful poll for Health.
func (p *fakePlatform) Poll(ctx context.Context) error {
	err := p.Batch.Poll(ctx)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err == nil {
		err = p.pollErr
	}
	if err == nil {
		p.lastListing = p.clock.Now()
	}
	return err
}

func (p *fakePlatform) Health() slurm.Health {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := slurm.Health{LastListing: p.lastListing}
	if p.pathErr != nil {
		h.PathUnresolved = p.pathErr
	}
	return h
}

// staticProfiles is a ProfileSource that returns a fixed configuration, or a
// fixed error.
type staticProfiles struct {
	mu  sync.Mutex
	v   slurm.Validated
	err error
}

func (s *staticProfiles) Read(context.Context, string) (slurm.Validated, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.v, s.err
}

func (s *staticProfiles) set(f func(*slurm.Validated)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f(&s.v)
}

// recorder keeps the events the controller emits.
type recorder struct {
	mu     sync.Mutex
	events []event.Event
}

func (r *recorder) Event(_ runtime.Object, e event.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recorder) WithAnnotations(...string) event.Recorder { return r }

func (r *recorder) reasons() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, e := range r.events {
		out = append(out, string(e.Reason))
	}
	return out
}

func (r *recorder) has(reason string) bool {
	for _, x := range r.reasons() {
		if x == reason {
			return true
		}
	}
	return false
}

// env is one test's world.
type env struct {
	t        *testing.T
	ctx      context.Context
	clock    *fake.Clock
	batch    *fake.Batch
	platform *fakePlatform
	profiles *staticProfiles
	c        client.Client
	ctl      *Controller
	rec      *recorder
	credDir  string
	buildDir string
	digest   string
	opts     Options
}

type envOption func(*env)

// fallback runs fallback mode (holdRelease false).
func fallback(e *env) {
	e.profiles.set(func(v *slurm.Validated) { v.Profile.Spec.HoldRelease = false })
}

func newEnv(t *testing.T, mods ...envOption) *env {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	// The deleting pass writes the accounting record, a ControllerRevision.
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	clock := fake.NewClock(at(12, 0, 0))
	gpus := int32(4)
	maxTime := v1alpha1.Duration("48h")
	window := v1alpha1.Duration("5m")
	lost := v1alpha1.Duration("2m")
	bound := v1alpha1.Duration("10m")
	batch := fake.NewBatch(clock, fake.Config{Partitions: []fake.Partition{
		{Name: gpuPart, GPUModel: "A100", GPUsPerNode: gpus, MaxTime: 48 * time.Hour},
	}})
	e := &env{t: t, ctx: context.Background(), clock: clock, batch: batch, rec: &recorder{}}
	e.platform = &fakePlatform{Batch: batch, clock: clock, busy: map[string]bool{},
		paths: slurm.Paths{Home: "/home/example-user", Staging: "/scratch/example-user/stage"}, keysPresent: true}
	e.profiles = &staticProfiles{v: slurm.Validated{
		Profile: provider.Profile{Name: "example-profile",
			Spec: v1alpha1.PlatformProfileSpec{
				ProviderKind: v1alpha1.PlatformBatch, HoldRelease: true, JSONDataParser: "v0.0.42",
				InflightWindow: &window, LostGrace: &lost, UnknownBound: &bound, StagingDir: "$SCRATCH/stage",
			},
			Partition: v1alpha1.PartitionProfile{Name: gpuPart, AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "A100", CPUArch: testArch},
				GPUsPerNode: &gpus, MaxTime: &maxTime},
		},
		Settings: provider.Settings{Provider: testProvider, Account: "example-account", Partition: gpuPart},
		SSH:      slurm.SSHConfig{HostName: "login.example.org", User: "example-user", IdentityFile: "/nonexistent/key"},
	}}
	e.credDir = t.TempDir()
	e.buildDir = t.TempDir()
	agent := []byte("#!/bin/sh\n# an example agent build\n")
	sum := sha256.Sum256(agent)
	e.digest = hex.EncodeToString(sum[:])
	if err := os.WriteFile(filepath.Join(e.buildDir, agentapi.AgentBuildName(testArch)), agent, 0o700); err != nil {
		t.Fatal(err)
	}
	prov := &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: testProvider, Generation: 1}}
	e.c = clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(prov).
		WithStatusSubresource(&v1alpha1.Allocation{}, &v1alpha1.Provider{}).WithReturnManagedFields().Build()
	e.opts = Options{
		Provider: testProvider, Profiles: e.profiles,
		NewPlatform: func(slurm.Validated) (Platform, io.Closer, error) { return e.platform, io.NopCloser(nil), nil },
		Clock:       clock, CredentialsDir: e.credDir, AgentBuildsDir: e.buildDir,
		Front: &FrontSettings{Mode: agentapi.DialHomeDirect, Endpoint: "front.example.org:29443",
			CertPin: strings.Repeat("c", 64), FrpcDigests: map[string]string{testArch: strings.Repeat("d", 64)}},
		Recorder: e.rec, CreationGracePeriod: -1, Identity: "test-controller",
	}
	for _, m := range mods {
		m(e)
	}
	e.build()
	return e
}

// build makes the controller from e.opts; a test calls it again to model a
// controller restart (a new process with nothing in memory).
func (e *env) build() {
	e.t.Helper()
	ctl, err := New(e.c, e.c, e.c.Scheme(), e.opts)
	if err != nil {
		e.t.Fatal(err)
	}
	e.ctl = ctl
}

// newAlloc creates an Allocation and its credential file.
func (e *env) newAlloc(name string) *v1alpha1.Allocation {
	e.t.Helper()
	a := &v1alpha1.Allocation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, UID: types.UID("uid-" + name),
			CreationTimestamp: metav1.NewTime(e.clock.Now())},
		Spec: v1alpha1.AllocationSpec{
			ProviderConfigReference: &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: testProvider},
			ManagementPolicies:      v1alpha1.DefaultManagementPolicies(),
			ForProvider: v1alpha1.AllocationParameters{Batch: &v1alpha1.BatchParameters{
				Partition: gpuPart, TimeLimit: "30m", GPUs: 1, CPUsPerTask: 8,
				Memory: resource.MustParse("16Gi"), Nodes: 1, AgentDigest: e.digest,
			}},
		},
	}
	if err := e.c.Create(e.ctx, a); err != nil {
		e.t.Fatal(err)
	}
	e.writeCredential(a)
	return e.get(name)
}

func (e *env) writeCredential(a *v1alpha1.Allocation) {
	e.t.Helper()
	p := agentapi.AllocationCredentialPath(e.credDir, string(a.UID))
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		e.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(strings.Repeat("ab", 32)), 0o600); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) get(name string) *v1alpha1.Allocation {
	e.t.Helper()
	a := &v1alpha1.Allocation{}
	if err := e.c.Get(e.ctx, client.ObjectKey{Namespace: testNS, Name: name}, a); err != nil {
		e.t.Fatal(err)
	}
	return a
}

// reconcile runs one reconcile of the Allocation and returns its result.
func (e *env) reconcile(name string) (reconcile.Result, error) {
	e.t.Helper()
	return e.ctl.Reconciler.Reconcile(e.ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: name}})
}

// run reconciles n times, ignoring errors (crossplane returns errors it has
// recorded in status; tests judge from the objects and the call log).
func (e *env) run(name string, n int) {
	e.t.Helper()
	for i := 0; i < n; i++ {
		_, _ = e.reconcile(name)
	}
}

func (e *env) count(op string) int { return e.batch.Count(op) }

// lifts counts the lifts that took effect, from the call log.
func (e *env) lifts() int {
	n := 0
	for _, c := range e.batch.Calls() {
		if c.Op == "scontrol-release" && len(c.Args) > 1 && c.Args[1] == fake.LiftApplied {
			n++
		}
	}
	return n
}

// live returns the IDs of the jobs with the tag that have not ended, from the
// fake's own records.
func (e *env) live(tag string) []string {
	var out []string
	jobs, _ := e.batch.FindByTag(e.ctx, tag, time.Time{})
	for _, j := range jobs {
		if j.EndTime == nil {
			out = append(out, j.ID)
		}
	}
	return out
}

func tagOf(a *v1alpha1.Allocation) string { return v1alpha1.JobTag(a.Namespace, a.Name, a.UID) }

func clientKey(name string) client.ObjectKey { return client.ObjectKey{Namespace: testNS, Name: name} }
