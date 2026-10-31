package providerctl

// The controller over the real slurm.Batch and the in-process fake Slurm
// (internal/slurm/fakeslurm): the dialect, the cached listing and the file
// staging under the reconciler. fake.Batch tests the protocol (harness_test.go);
// these test what only the real Batch does. Counts come from the fake Slurm's
// call log.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

var varRef = regexp.MustCompile(`\$[A-Za-z_{]`)

// realEnv is one Provider's controller over a Batch on a fake Slurm session.
type realEnv struct {
	t        *testing.T
	ctx      context.Context
	clock    *fake.Clock
	slurm    *fakeslurm.Platform
	session  *fakeslurm.Session
	batch    *slurm.Batch
	profiles *staticProfiles
	c        client.Client
	ctl      *Controller
	opts     Options
	credDir  string
	digest   string
	provider string
	user     string
}

type realOption func(*realEnv)

// newSlurm returns a fake Slurm with one GPU partition.
func newSlurm(clock *fake.Clock, cfg fakeslurm.Config) *fakeslurm.Platform {
	if cfg.Partitions == nil {
		cfg.Partitions = []fakeslurm.Partition{{Name: gpuPart, GPUOnly: true}}
	}
	return fakeslurm.New(clock, cfg)
}

// newRealEnv makes a controller for provider, logged in to sl as user, with
// HOME and SCRATCH set for that user. c may be shared between envs.
func newRealEnv(t *testing.T, clock *fake.Clock, sl *fakeslurm.Platform, c client.Client, providerName, user string, mods ...realOption) *realEnv {
	t.Helper()
	e := &realEnv{t: t, ctx: context.Background(), clock: clock, slurm: sl, c: c, provider: providerName, user: user}
	env := map[string]string{"HOME": "/home/" + user, "SCRATCH": "/scratch/" + user}
	e.session = sl.Login(fakeslurm.Login{User: user, Env: env})
	gpus := int32(4)
	maxTime := v1alpha1.Duration("48h")
	window := v1alpha1.Duration("5m")
	e.profiles = &staticProfiles{v: slurm.Validated{
		Profile: provider.Profile{Name: "example-profile",
			Spec: v1alpha1.PlatformProfileSpec{ProviderKind: v1alpha1.PlatformBatch, HoldRelease: true, JSONDataParser: "v0.0.42",
				InflightWindow: &window, StagingDir: "$SCRATCH/stage"},
			Partition: v1alpha1.PartitionProfile{Name: gpuPart, AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "A100", CPUArch: testArch},
				GPUsPerNode: &gpus, MaxTime: &maxTime},
		},
		Settings: provider.Settings{Provider: providerName, Account: "account-" + user, Partition: gpuPart},
	}}
	e.credDir = t.TempDir()
	builds := t.TempDir()
	agent := []byte("#!/bin/sh\n# an example agent build\n")
	sum := sha256.Sum256(agent)
	e.digest = hex.EncodeToString(sum[:])
	if err := os.WriteFile(filepath.Join(builds, agentapi.AgentBuildName(testArch)), agent, 0o700); err != nil {
		t.Fatal(err)
	}
	e.opts = Options{
		Provider: providerName, Profiles: e.profiles, Clock: clock, CredentialsDir: e.credDir, AgentBuildsDir: builds,
		NewPlatform: func(v slurm.Validated) (Platform, io.Closer, error) {
			e.batch = slurm.NewBatch(e.session, v.Profile, v.Settings, slurm.Options{Clock: clock})
			return e.batch, io.NopCloser(nil), nil
		},
		Front:               &FrontSettings{Mode: agentapi.DialHomeDirect, Endpoint: "front.example.org:29443", CertPin: strings.Repeat("c", 64)},
		CreationGracePeriod: -1,
	}
	for _, m := range mods {
		m(e)
	}
	ctl, err := New(c, c, c.Scheme(), e.opts)
	if err != nil {
		t.Fatal(err)
	}
	e.ctl = ctl
	return e
}

func newFakeClient(t *testing.T, providers ...string) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	b := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.Allocation{}, &v1alpha1.Provider{})
	for _, p := range providers {
		b = b.WithObjects(&v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: p, Generation: 1}})
	}
	return b.Build()
}

func (e *realEnv) newAlloc(name string, gpus int32) *v1alpha1.Allocation {
	e.t.Helper()
	a := &v1alpha1.Allocation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, UID: types.UID("uid-" + e.provider + "-" + name),
			CreationTimestamp: metav1.NewTime(e.clock.Now())},
		Spec: v1alpha1.AllocationSpec{
			ProviderConfigReference: &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: e.provider},
			ManagementPolicies:      v1alpha1.DefaultManagementPolicies(),
			ForProvider: v1alpha1.AllocationParameters{Batch: &v1alpha1.BatchParameters{
				Partition: gpuPart, TimeLimit: "30m", GPUs: gpus, CPUsPerTask: 8, Memory: resource.MustParse("16Gi"), Nodes: 1, AgentDigest: e.digest,
			}},
		},
	}
	if err := e.c.Create(e.ctx, a); err != nil {
		e.t.Fatal(err)
	}
	p := agentapi.AllocationCredentialPath(e.credDir, string(a.UID))
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		e.t.Fatal(err)
	}
	cred := sha256.Sum256([]byte(a.UID))
	if err := os.WriteFile(p, []byte(hex.EncodeToString(cred[:])), 0o600); err != nil {
		e.t.Fatal(err)
	}
	return e.get(name)
}

func (e *realEnv) get(name string) *v1alpha1.Allocation {
	e.t.Helper()
	a := &v1alpha1.Allocation{}
	if err := e.c.Get(e.ctx, client.ObjectKey{Namespace: testNS, Name: name}, a); err != nil {
		e.t.Fatal(err)
	}
	return a
}

func (e *realEnv) run(name string, n int) {
	for i := 0; i < n; i++ {
		_, _ = e.ctl.Reconciler.Reconcile(e.ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: name}})
	}
}

// start starts the Allocation's recorded job on the fake Slurm.
func (e *realEnv) start(name string) {
	e.t.Helper()
	id, err := strconv.Atoi(e.get(name).Annotations[v1alpha1.AnnotationJobID])
	if err != nil {
		e.t.Fatalf("no recorded job: %v", err)
	}
	if err := e.slurm.Start(id); err != nil {
		e.t.Fatal(err)
	}
}

func (e *realEnv) provStatus() v1alpha1.ProviderStatus {
	e.t.Helper()
	var p v1alpha1.Provider
	if err := e.c.Get(e.ctx, client.ObjectKey{Name: e.provider}, &p); err != nil {
		e.t.Fatal(err)
	}
	return p.Status
}

// A spec change on a Running Allocation makes no Slurm call within the poll
// interval and leaves its state unchanged (#59, from the review of #48): the
// management policies exclude Update, and Observe reads the Provider's cached
// listing. Counted from the fake Slurm's log. What the test covers is exactly
// that state (review of PR 76: Opus N11): on an Allocation with no recorded
// job (waiting out an attempt, a Rejected backoff or the prerequisites), every
// reconcile, the one a spec change triggers included, looks the tag up and so
// reads accounting; it never writes.
func TestSpecChangeOnARunningAllocationMakesNoSlurmCallWithinThePollInterval(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 3)
	e.start("alloc-a")
	clock.Advance(40 * time.Second) // past the listing's 30 s, so the next read sees the start
	e.run("alloc-a", 2)
	before := e.get("alloc-a")
	if before.Status.AtProvider.Phase != v1alpha1.AllocationRunning {
		t.Fatalf("phase %s, want Running before the spec change", before.Status.AtProvider.Phase)
	}
	calls := len(sl.Calls())

	for _, change := range []struct {
		name string
		mod  func(a *v1alpha1.Allocation)
	}{
		{"modelDeploymentRef", func(a *v1alpha1.Allocation) {
			a.Spec.ModelDeploymentRef = &v1alpha1.ObjectReference{Name: "another-deployment"}
		}},
		// The API server refuses this one (CEL); the fake client lets it
		// through, as a CEL bypass would.
		{"forProvider", func(a *v1alpha1.Allocation) { a.Spec.ForProvider.Batch.TimeLimit = "45m" }},
	} {
		a := e.get("alloc-a")
		change.mod(a)
		if err := e.c.Update(e.ctx, a); err != nil {
			t.Fatal(err)
		}
		e.run("alloc-a", 1)
		after := e.get("alloc-a")
		if n := len(sl.Calls()); n != calls {
			t.Errorf("%s: %d Slurm calls after the spec change, want 0: %v", change.name, n-calls, sl.Calls()[calls:])
		}
		if !equality.Semantic.DeepEqual(before.Status.AtProvider, after.Status.AtProvider) {
			t.Errorf("%s: atProvider changed:\nbefore %+v\nafter  %+v", change.name, before.Status.AtProvider, after.Status.AtProvider)
		}
		for _, k := range []string{v1alpha1.AnnotationSubmission, v1alpha1.AnnotationJobID, v1alpha1.AnnotationHoldLifted, v1alpha1.AnnotationExternalName} {
			if before.Annotations[k] != after.Annotations[k] {
				t.Errorf("%s: annotation %s changed", change.name, k)
			}
		}
	}
	// Control: the same reconcile after the listing's interval does read
	// Slurm, so the count above would have seen a call.
	clock.Advance(40 * time.Second)
	e.run("alloc-a", 1)
	if n := sl.Count("squeue"); n == 0 || len(sl.Calls()) == calls {
		t.Error("control: a reconcile past the poll interval made no call; the count proves nothing")
	}
}

// Two providers on one platform, with separate accounts and users: their
// Allocations, files and credentials never mix (HBEP-4, Test plan). Their ssh
// masters are separate: internal/slurm/sshrun's TestSeparateMasters.
func TestTwoProvidersOnOneFakePlatform(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	c := newFakeClient(t, "prov-a", "prov-b")
	a := newRealEnv(t, clock, sl, c, "prov-a", "user-a")
	b := newRealEnv(t, clock, sl, c, "prov-b", "user-b")
	aa := a.newAlloc("alloc-pa", 1)
	bb := b.newAlloc("alloc-pb", 1)
	a.run("alloc-pa", 3)
	b.run("alloc-pb", 3)
	if n, m := sl.CountFor("user-a", "sbatch"), sl.CountFor("user-b", "sbatch"); n != 1 || m != 1 {
		t.Errorf("sbatch per user: %d and %d, want 1 and 1", n, m)
	}
	// Each controller acts only on its own Provider's Allocation.
	before := len(sl.Calls())
	a.run("alloc-pb", 2)
	if n := len(sl.Calls()); n != before {
		t.Errorf("prov-a's controller made %d calls for prov-b's Allocation, want 0", n-before)
	}
	jobs := sl.Jobs()
	if len(jobs) != 2 {
		t.Fatalf("%d jobs, want 2", len(jobs))
	}
	files := sl.Files()
	for _, x := range []struct {
		e   *realEnv
		a   *v1alpha1.Allocation
		dir string
	}{{a, aa, "/scratch/user-a/stage"}, {b, bb, "/scratch/user-b/stage"}} {
		tag := v1alpha1.JobTag(x.a.Namespace, x.a.Name, x.a.UID)
		f, ok := files[x.dir+"/"+agentapi.StagedCredentialPath(tag)]
		if !ok {
			t.Errorf("%s: no credential under its own staging directory", x.e.provider)
			continue
		}
		want, _ := os.ReadFile(agentapi.AllocationCredentialPath(x.e.credDir, string(x.a.UID)))
		if string(f.Content) != string(want) {
			t.Errorf("%s: the staged credential is not its own", x.e.provider)
		}
	}
	for p := range files {
		if strings.Contains(p, "/user-a/") && strings.Contains(p, string(bb.UID)) {
			t.Errorf("prov-b's file %s is under user-a's directory", p)
		}
	}
	for _, call := range sl.Calls() {
		if call.User == "user-a" && strings.Contains(strings.Join(call.Words, " "), "account-user-b") {
			t.Errorf("user-a ran a command with prov-b's account: %v", call.Words)
		}
	}
}

// The staging directory: $SCRATCH is resolved once per connection to the
// fake's absolute path, and only that path is in commands, the bootstrap and
// agent.json; an unset variable sets PathUnresolved and no file call is made;
// an unset HOME sets it and no sbatch runs (HBEP-4, Test plan).
func TestStagingDirectoryResolution(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	a := e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 3)
	if n := sl.Count("printenv"); n != 2 {
		t.Errorf("%d printenv calls, want 2 (HOME and SCRATCH, once for the connection)", n)
	}
	for _, call := range sl.Calls() {
		line := strings.Join(call.Words, " ")
		// The bootstrap's $( ) is a command substitution; a variable is $NAME
		// or ${NAME}.
		if varRef.MatchString(line) || strings.Contains(line, "~") {
			t.Errorf("a command holds a variable or ~: %s", line)
		}
	}
	tag := v1alpha1.JobTag(a.Namespace, a.Name, a.UID)
	js := sl.Files()["/scratch/user-a/stage/"+agentapi.StagedConfigPath(tag)]
	cfg, err := agentapi.DecodeAgentConfig(js.Content)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StagingDir != "/scratch/user-a/stage" || cfg.DialHomeDir != "/home/user-a/.hpcburst/dial-home" || cfg.Tag != tag {
		t.Errorf("agent.json %+v", cfg)
	}
	if js.Mode.Perm() != 0o600 {
		t.Errorf("agent.json mode %o, want 600", js.Mode.Perm())
	}
	// A reconnect resolves the paths again.
	e.session.Reconnect()
	b := e.newAlloc("alloc-b", 1)
	_ = b
	e.run("alloc-b", 3)
	if n := sl.Count("printenv"); n != 4 {
		t.Errorf("%d printenv calls after a reconnect, want 4", n)
	}

	for _, unset := range []string{"SCRATCH", "HOME"} {
		t.Run("unset "+unset, func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := newSlurm(clock, fakeslurm.Config{})
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
			env := map[string]string{"HOME": "/home/user-a", "SCRATCH": "/scratch/user-a"}
			delete(env, unset)
			e.session = sl.Login(fakeslurm.Login{User: "user-a", Env: env})
			e.newAlloc("alloc-a", 1)
			e.run("alloc-a", 3)
			if err := e.ctl.SyncStatus(e.ctx); err != nil {
				t.Fatal(err)
			}
			if n := sl.Count("sbatch"); n != 0 {
				t.Errorf("%d sbatch calls with %s unset, want 0", n, unset)
			}
			if n := sl.Count("cat"); n != 0 {
				t.Errorf("%d file writes with %s unset, want 0", n, unset)
			}
			c := apimeta.FindStatusCondition(e.provStatus().Conditions, v1alpha1.ConditionPathUnresolved)
			if c == nil || c.Status != metav1.ConditionTrue {
				t.Errorf("PathUnresolved is %+v, want True", c)
			}
		})
	}
}

// In ssh dial-home mode a missing key file sets DialHomeKeyMissing and no
// sbatch runs; once the operator places both files, the submission goes on
// (HBEP-4, Test plan, The dial-home key). Create writes the condition itself
// (HBEP-4, Two controllers), so no status-loop pass runs here (review of PR 76:
// Opus N2).
func TestDialHomeKeyMissing(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", func(e *realEnv) {
		e.opts.Front = &FrontSettings{Mode: agentapi.DialHomeSSH, Endpoint: "front.example.org:22", CertPin: strings.Repeat("c", 64),
			SSHUser: "example-user", AgentPort: 29270}
	})
	if err := sl.MkdirAll("/home/user-a/.hpcburst/dial-home", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := sl.WriteFile("/home/user-a/.hpcburst/dial-home/key", []byte("example"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 3)
	if n := sl.Count("sbatch"); n != 0 {
		t.Errorf("%d sbatch calls with known_hosts missing, want 0", n)
	}
	c := apimeta.FindStatusCondition(e.provStatus().Conditions, v1alpha1.ConditionDialHomeKeyMissing)
	if c == nil || c.Status != metav1.ConditionTrue {
		t.Fatalf("DialHomeKeyMissing is %+v, want True", c)
	}
	if err := sl.WriteFile("/home/user-a/.hpcburst/dial-home/known_hosts", []byte("example"), 0o600); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Minute)
	e.run("alloc-a", 3)
	if n := sl.Count("sbatch"); n != 1 {
		t.Errorf("%d sbatch calls once both files exist, want 1", n)
	}
	if c := apimeta.FindStatusCondition(e.provStatus().Conditions, v1alpha1.ConditionDialHomeKeyMissing); c == nil || c.Status != metav1.ConditionFalse {
		t.Errorf("DialHomeKeyMissing is %+v, want False", c)
	}
}

// A reply whose data_parser differs from the profile's is not interpreted: the
// read is unknown, nothing is submitted, and the Provider gets DialectMismatch
// (HBEP-4, The Slurm dialect).
func TestDialectMismatch(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{DataParser: "v0.0.42", ReplyDataParser: "v0.0.43"})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 3)
	if err := e.ctl.SyncStatus(e.ctx); err != nil {
		t.Fatal(err)
	}
	if n := sl.Count("sbatch"); n != 0 {
		t.Errorf("%d sbatch calls while every reply is in another dialect, want 0", n)
	}
	st := e.provStatus()
	if c := apimeta.FindStatusCondition(st.Conditions, v1alpha1.ConditionDialectMismatch); c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("DialectMismatch is %+v, want True", c)
	}
	if st.LastSuccessfulStatusRead != nil {
		t.Error("a reply that was not interpreted counted as a successful read")
	}
}

// A job purged from the queue listing is reported from accounting as ended,
// never as missing, and is never submitted again (HBEP-4, Test plan; S4
// scenario 1), in both of S7's "not listed" shapes.
func TestPurgedJobEndsFromAccounting(t *testing.T) {
	for _, mode := range []fakeslurm.NotListedMode{fakeslurm.NotListedHelios, fakeslurm.NotListedAthena} {
		t.Run(string(mode), func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := newSlurm(clock, fakeslurm.Config{NotListed: mode, DataParser: map[fakeslurm.NotListedMode]string{
				fakeslurm.NotListedHelios: "v0.0.42", fakeslurm.NotListedAthena: "v0.0.40"}[mode]})
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", func(e *realEnv) {
				if mode == fakeslurm.NotListedAthena {
					e.profiles.set(func(v *slurm.Validated) { v.Profile.Spec.JSONDataParser = "v0.0.40" })
				}
			})
			e.newAlloc("alloc-a", 1)
			e.run("alloc-a", 3)
			e.start("alloc-a")
			id, _ := strconv.Atoi(e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID])
			clock.Advance(time.Minute)
			if err := sl.End(id, "COMPLETED"); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 6; i++ {
				clock.Advance(20 * time.Second)
				e.run("alloc-a", 1)
			}
			a := e.get("alloc-a")
			if a.Status.AtProvider.Phase != v1alpha1.AllocationEnded || a.Status.AtProvider.EndTime == nil {
				t.Errorf("phase %s end %v, want Ended with an end time", a.Status.AtProvider.Phase, a.Status.AtProvider.EndTime)
			}
			if n := sl.Count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls, want 1", n)
			}
		})
	}
}

// With every read failing, an Allocation is never reported Lost, however long
// the failure lasts (HBEP-4, Test plan).
func TestFailedReadsAreNotAbsenceOverTheDialect(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 3)
	e.start("alloc-a")
	clock.Advance(40 * time.Second)
	e.run("alloc-a", 1)
	sl.FailNext(10000, slurm.ErrTransport)
	for i := 0; i < 60; i++ {
		clock.Advance(time.Minute)
		e.run("alloc-a", 1)
		if p := e.get("alloc-a").Status.AtProvider.Phase; p == v1alpha1.AllocationLost {
			t.Fatalf("Lost after %d min of failed reads", i+1)
		}
	}
	if n := sl.Count("sbatch"); n != 1 {
		t.Errorf("%d sbatch calls, want 1", n)
	}
}

// The Provider's queue listing is read once per poll interval and shared by
// its Allocations (HBEP-4, Transport, Poll rate).
func TestOneListingPerPollInterval(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	names := []string{"alloc-a", "alloc-b", "alloc-c"}
	for _, n := range names {
		e.newAlloc(n, 1)
		e.run(n, 3)
		e.start(n)
	}
	clock.Advance(40 * time.Second)
	for _, n := range names {
		e.run(n, 1)
	}
	before := sl.Count("squeue")
	clock.Advance(40 * time.Second)
	for _, n := range names {
		e.run(n, 2)
	}
	if got := sl.Count("squeue") - before; got != 1 {
		t.Errorf("%d queue listings for 3 Allocations in one interval, want 1", got)
	}
}

// A submission without GPUs to a GPU partition is refused before any call
// (HBEP-4, Test plan, Final state and GPU rules).
func TestNoGPUOnAGPUPartitionIsRefusedBeforeAnyCall(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 0)
	if calls, err := e.createDirect("alloc-a"); calls != 0 || err == nil {
		t.Errorf("Create made %d calls and returned %v, want no call at all and a refusal", calls, err)
	}
	a := e.get("alloc-a")
	rec, err := readRecord(a)
	if err != nil || rec.sub == nil || rec.sub.State != v1alpha1.SubmissionRejected || !strings.Contains(rec.sub.Reason, "GPU partition") {
		t.Errorf("record %+v (%v), want Rejected naming the GPU rule", rec.sub, err)
	}
	e.run("alloc-a", 3)
	if n := sl.Count("sbatch"); n != 0 {
		t.Errorf("%d sbatch calls, want 0", n)
	}
}

// createDirect calls Create on the Allocation, as the reconciler does after an
// Observe that reports the job missing, and returns how many commands it
// ran on the fake Slurm: the Observe's own reads are not in the count. The tag
// initializer's write is done first, as a reconcile does it.
func (e *realEnv) createDirect(name string) (int, error) {
	e.t.Helper()
	a := e.get(name)
	meta.SetExternalName(a, tagOf(a))
	if err := e.c.Update(e.ctx, a); err != nil {
		e.t.Fatal(err)
	}
	a = e.get(name)
	ext, err := (&connector{s: e.ctl.shared}).Connect(e.ctx, a)
	if err != nil {
		e.t.Fatal(err)
	}
	mark := len(e.slurm.Calls())
	_, err = ext.Create(e.ctx, a)
	return len(e.slurm.Calls()) - mark, err
}

// A one-job-per-user partition refuses the second job: Rejected, then backoff,
// then a new attempt (HBEP-4, Test plan, Batch fake behaviours).
func TestOneJobPerUserRejectsThenBacksOff(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{Partitions: []fakeslurm.Partition{{Name: gpuPart, GPUOnly: true, OneJobPerUser: true}}})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 3)
	e.newAlloc("alloc-b", 1)
	e.run("alloc-b", 3)
	b := e.get("alloc-b")
	rec, _ := readRecord(b)
	if rec.sub == nil || rec.sub.State != v1alpha1.SubmissionRejected {
		t.Fatalf("second Allocation's record %+v, want Rejected", rec.sub)
	}
	if b.Status.AtProvider.Phase != v1alpha1.AllocationSubmitting {
		t.Errorf("phase %s, want Submitting during the backoff", b.Status.AtProvider.Phase)
	}
	n := sl.Count("sbatch")
	e.run("alloc-b", 3)
	if m := sl.Count("sbatch"); m != n {
		t.Errorf("%d sbatch calls during the backoff, want none", m-n)
	}
	clock.Advance(2 * time.Minute)
	e.run("alloc-b", 2)
	if m := sl.Count("sbatch"); m != n+1 {
		t.Errorf("%d sbatch calls after the backoff, want 1", m-n)
	}
	rec, _ = readRecord(e.get("alloc-b"))
	if rec.sub == nil || rec.sub.Attempt != 2 {
		t.Errorf("record %+v, want attempt 2", rec.sub)
	}
}

// Start estimates swing and flip to unknown; the status follows each read and
// never plans on one (HBEP-4, Test plan, Batch fake behaviours: swinging
// estimates). Freed slots go to another user at once, so the provider never
// assumes a start (freed slots taken at once).
func TestSwingingEstimatesAndFreedSlots(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{Partitions: []fakeslurm.Partition{{Name: gpuPart, GPUOnly: true, Slots: 1}}})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 3)
	e.start("alloc-a")
	e.newAlloc("alloc-b", 1)
	e.run("alloc-b", 3)
	idB, _ := strconv.Atoi(e.get("alloc-b").Annotations[v1alpha1.AnnotationJobID])
	for _, est := range []*time.Time{ptrTime(at(14, 0, 0)), ptrTime(at(18, 30, 0)), nil, ptrTime(at(13, 0, 0))} {
		if err := sl.SetEstimate(idB, est); err != nil {
			t.Fatal(err)
		}
		clock.Advance(40 * time.Second)
		e.run("alloc-b", 1)
		o := e.get("alloc-b").Status.AtProvider
		switch {
		case est == nil && o.StartEstimate != nil:
			t.Errorf("the estimate went unknown but the status shows %v", o.StartEstimate)
		case est != nil && (o.StartEstimate == nil || !o.StartEstimate.Time.Equal(*est)):
			t.Errorf("estimate %v, status shows %v", est, o.StartEstimate)
		}
		if o.Phase != v1alpha1.AllocationPending || o.StartTime != nil {
			t.Errorf("phase %s start %v: an estimate is never a start", o.Phase, o.StartTime)
		}
	}
	// alloc-a ends; its slot goes to another user at once, so alloc-b stays
	// pending and the provider shows it pending.
	idA, _ := strconv.Atoi(e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID])
	if err := sl.End(idA, "COMPLETED"); err != nil {
		t.Fatal(err)
	}
	if err := sl.Start(idB); err == nil {
		t.Fatal("the fake started alloc-b in a slot another user took")
	}
	clock.Advance(40 * time.Second)
	e.run("alloc-b", 2)
	if o := e.get("alloc-b").Status.AtProvider; o.Phase != v1alpha1.AllocationPending || o.StartTime != nil {
		t.Errorf("phase %s start %v after a slot was freed; want Pending with no start", o.Phase, o.StartTime)
	}
}

func ptrTime(t time.Time) *time.Time { return &t }

// A requeue reruns the job under the same ID: the Allocation is Failed with
// reason Requeued, and stays so after accounting forgets the earlier
// execution (HBEP-4, Phases; Test plan: a requeue that reruns a job under the
// same job ID), on both data_parser versions.
func TestRequeueUnderTheSameJobID(t *testing.T) {
	for _, parser := range []string{"v0.0.42", "v0.0.40"} {
		t.Run(parser, func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := newSlurm(clock, fakeslurm.Config{DataParser: parser})
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", func(e *realEnv) {
				e.profiles.set(func(v *slurm.Validated) { v.Profile.Spec.JSONDataParser = parser })
			})
			e.newAlloc("alloc-a", 1)
			e.run("alloc-a", 3)
			e.start("alloc-a")
			id, _ := strconv.Atoi(e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID])
			if err := sl.Requeue(id); err != nil {
				t.Fatal(err)
			}
			clock.Advance(40 * time.Second)
			e.run("alloc-a", 1)
			o := e.get("alloc-a").Status.AtProvider
			if o.Phase != v1alpha1.AllocationFailed || o.Reason != v1alpha1.ReasonRequeued || !o.Requeued {
				t.Fatalf("phase %s reason %q requeued %v, want Failed, Requeued", o.Phase, o.Reason, o.Requeued)
			}
			if err := sl.ForgetRequeue(id); err != nil {
				t.Fatal(err)
			}
			clock.Advance(40 * time.Second)
			e.run("alloc-a", 1)
			if o := e.get("alloc-a").Status.AtProvider; o.Phase != v1alpha1.AllocationFailed || !o.Requeued {
				t.Errorf("after the evidence is gone: phase %s requeued %v; the verdict is kept", o.Phase, o.Requeued)
			}
			if n := sl.Count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls, want 1", n)
			}
		})
	}
}

// Only JSON is read: no call ever asks for text output, so the local-time text
// across a daylight-saving change is never parsed (HBEP-4, Test plan:
// local-time text output, which no code reads).
func TestOnlyJSONIsRead(t *testing.T) {
	clock := fake.NewClock(time.Date(2026, 10, 25, 0, 30, 0, 0, time.UTC)) // 30 min before the Europe/Warsaw change
	sl := newSlurm(clock, fakeslurm.Config{TimeZone: "Europe/Warsaw"})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 3)
	e.start("alloc-a")
	for i := 0; i < 4; i++ {
		clock.Advance(20 * time.Minute)
		e.run("alloc-a", 1)
	}
	a := e.get("alloc-a")
	if err := e.c.Delete(e.ctx, a); err != nil {
		t.Fatal(err)
	}
	e.run("alloc-a", 3)
	n := 0
	for _, call := range sl.Calls() {
		if w := call.Words[0]; w == "squeue" || w == "sacct" {
			n++
			if !strings.Contains(strings.Join(call.Words, " "), "--json") {
				t.Errorf("a call asks for text output: %v", call.Words)
			}
		}
	}
	if n == 0 {
		t.Fatal("no squeue or sacct call at all; the check proves nothing")
	}
	o := a.Status.AtProvider
	if o.StartTime == nil || o.HardEnd == nil || !o.HardEnd.Time.Equal(o.StartTime.Add(30*time.Minute)) {
		t.Errorf("start %v hard end %v: the hard end is start plus 30 min across the change", o.StartTime, o.HardEnd)
	}
}
