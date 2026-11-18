package e2e

// The test world: a control plane that writes the audit log, the identities that act on it, and the lifecycle
// controller running as its own ServiceAccount. Everything that makes a request has its own credential, and each
// identity's user name is asked of the API server (SelfSubjectReview), never typed in, so the log's names can be
// compared with what the API server itself says.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/config/audit"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/lifecycle"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv/crdinstall"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

const (
	// portBase is this package's testenv block, 28560-28563, inside the second range (28560-28579).
	portBase = "28560"

	// The deployment's world, synthetic values of the shapes the code reads.
	exampleNS       = "example-ns"
	exampleProfile  = "helios"
	exampleProvider = "helios-gpu"
	examplePart     = "gpu-part"
	exampleGPU      = "GH200"
	exampleArch     = "aarch64"
	exampleModel    = "example-model"
	exampleDigest   = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	exampleRevision = "0123456789abcdef0123456789abcdef01234567"

	// rotationMB is the log's size at rotation for the whole run.
	rotationMB = 1
)

// exampleFiles is the checkpoint's per-file list the stub source returns.
var exampleFiles = []deploymentid.File{
	{Path: "config.json", SHA256: strings.Repeat("1", 64)},
	{Path: "model.safetensors", SHA256: strings.Repeat("2", 64)},
}

// stubCheckpoints is a lifecycle.CheckpointSource with a fixed file list and no chat template.
type stubCheckpoints struct{}

func (stubCheckpoints) Files(context.Context, v1alpha1.CheckpointSpec) ([]deploymentid.File, error) {
	return append([]deploymentid.File(nil), exampleFiles...), nil
}

func (stubCheckpoints) TemplateSources(context.Context, v1alpha1.CheckpointSpec, []deploymentid.File) (deploymentid.TemplateSources, error) {
	return deploymentid.TemplateSources{}, nil
}

// logBuffer keeps what the controllers log through controller-runtime's logger, bounded, for a failing wait to print.
type logBuffer struct {
	mu    sync.Mutex
	lines []string
}

func (b *logBuffer) line(s string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.lines) >= 2000 {
		b.lines = b.lines[1:]
	}
	if len(s) > 400 {
		s = s[:400]
	}
	b.lines = append(b.lines, s)
}

// tail is the last n lines.
func (b *logBuffer) tail(n int) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	from := len(b.lines) - n
	if from < 0 {
		from = 0
	}
	return strings.Join(b.lines[from:], "\n")
}

// shiftClock is the real clock plus an offset a test advances: the lifecycle controller reads it for every decision, so a
// jump of 7 days is one call, and in between it runs at the speed of the wall clock, as the objects' own timestamps do.
type shiftClock struct {
	mu  sync.Mutex
	off time.Duration
}

func (c *shiftClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Now().UTC().Add(c.off)
}

// Advance moves the clock forward.
func (c *shiftClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.off += d
}

// who is one identity of the run: its user name and groups as the API server reports them, and a client that acts
// as it.
type who struct {
	name   string
	user   string
	groups []string
	cfg    *rest.Config
	c      client.Client
}

// world is the running control plane and the identities.
type world struct {
	t   *testing.T
	ctx context.Context
	cp  *testenv.ControlPlane

	// logs is what the controllers logged.
	logs *logBuffer

	// clock is the controller's clock (startLifecycle); the Provider's readiness is stamped by it.
	clock *shiftClock

	// logPath is the audit log's file; its directory also holds the rotated files.
	logPath string
	// state is the launcher's state directory of this run, holding the controller identities' kubeconfigs.
	state string

	admin  who // the person: the control plane's administrator
	a, b   who // agents A and B
	lc, pv who // the lifecycle controller's and the Provider managers' identities
}

func installCRDs(ctx context.Context, cfg *rest.Config) error {
	crds, err := crdinstall.ReadCRDs(filepath.Join("..", "..", "..", "config", "crd"))
	if err != nil {
		return err
	}
	cl, err := crdinstall.NewClient(cfg)
	if err != nil {
		return err
	}
	// The kinds are served only once each CRD is Established: the first create must not meet "no matches for kind".
	return crdinstall.InstallAndWait(ctx, cl, crds)
}

func eventually(t *testing.T, limit time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(limit)
	last := ""
	for {
		ok, msg := cond()
		if ok {
			return
		}
		last = msg
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %s; last: %s", what, limit, last)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// identityOf asks the API server who cfg's credential is (SelfSubjectReview) and returns the identity with a client.
func identityOf(ctx context.Context, t *testing.T, name string, cfg *rest.Config) who {
	t.Helper()
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r, err := cs.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("SelfSubjectReview of %s: %v", name, err)
	}
	s, err := lifecycle.Scheme()
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	return who{name: name, user: r.Status.UserInfo.Username, groups: r.Status.UserInfo.Groups, cfg: cfg, c: c}
}

// kubeconfigOf reads a kubeconfig file the launcher wrote.
func kubeconfigOf(t *testing.T, path string) *rest.Config {
	t.Helper()
	cfg, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		t.Fatalf("kubeconfig %s: %v", path, err)
	}
	cfg.QPS, cfg.Burst = 200, 400
	return cfg
}

// newWorld starts the control plane with the audit log on, makes the agents' access, the person's ApprovalPolicy
// and the three controller identities ready exactly as hpcburst up does (the launcher's own functions), and opens
// the identities of the run.
func newWorld(t *testing.T) *world {
	t.Helper()
	if os.Getenv("HPCBURST_TESTENV_PORT_BASE") == "" {
		t.Setenv("HPCBURST_TESTENV_PORT_BASE", portBase)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Minute)
	t.Cleanup(cancel)
	dir := t.TempDir()
	policy := filepath.Join(dir, "audit-policy.yaml")
	if err := os.WriteFile(policy, audit.Policy, 0o600); err != nil {
		t.Fatal(err)
	}
	w := &world{t: t, ctx: ctx, clock: &shiftClock{}, logs: &logBuffer{}, logPath: filepath.Join(dir, "audit", "audit.log"), state: filepath.Join(dir, "state")}
	ctrl.SetLogger(funcr.New(func(prefix, args string) { w.logs.line(prefix + " " + args) }, funcr.Options{}))
	cp, err := testenv.Start(ctx, filepath.Join(dir, "cp"), testenv.Options{
		BeforeControllerManager: installCRDs,
		Audit:                   &testenv.AuditOptions{PolicyFile: policy, LogPath: w.logPath, MaxSizeMB: rotationMB},
	})
	if err != nil {
		t.Fatalf("start the control plane: %v", err)
	}
	w.cp = cp
	t.Cleanup(func() {
		if err := cp.Stop(); err != nil {
			t.Errorf("stop the control plane: %v", err)
		}
		if err := cp.AssertAllDead(); err != nil {
			t.Errorf("leftover process: %v", err)
		}
	})
	w.admin = identityOf(ctx, t, "the person", cp.Config)
	if err := w.admin.c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: exampleNS}}); err != nil {
		t.Fatal(err)
	}

	// The agents' access and the person's policy, with the launcher's own functions: the narrow role, the three
	// admission policies, the wait until each refuses its probe, and the wide role.
	if writes, err := launcher.InstallAgentAccess(ctx, cp.Config, io.Discard, launcher.AgentAccessOptions{}); err != nil || !writes {
		t.Fatalf("install the agents' access: writes %v, %v", writes, err)
	}
	if outcome, err := launcher.EnsureGlobalApprovalPolicy(ctx, cp.Config, io.Discard); err != nil || outcome != launcher.GlobalPolicyCreated {
		t.Fatalf("the person's global ApprovalPolicy: %q, %v", outcome, err)
	}
	ids, err := launcher.PrepareControllerIdentities(ctx, cp.Config, w.state, io.Discard)
	if err != nil || len(ids) != 3 {
		t.Fatalf("the controller identities: %d, %v", len(ids), err)
	}
	for _, id := range ids {
		switch id.Name {
		case launcher.ControllerLifecycleIdentity:
			w.lc = identityOf(ctx, t, id.Name, kubeconfigOf(t, id.Kubeconfig))
		case launcher.ControllerProviderIdentity:
			w.pv = identityOf(ctx, t, id.Name, kubeconfigOf(t, id.Kubeconfig))
		}
	}
	if w.lc.user != "system:serviceaccount:hpcburst-system:hpcburst-lifecycle" || w.pv.user != "system:serviceaccount:hpcburst-system:hpcburst-provider" {
		t.Fatalf("the controller kubeconfigs authenticate as %q and %q", w.lc.user, w.pv.user)
	}
	w.a, w.b = w.agent(t, filepath.Join(dir, "identities"), "agent-a"), w.agent(t, filepath.Join(dir, "identities"), "agent-b")
	if w.a.user == w.b.user || w.a.user == w.admin.user {
		t.Fatalf("the identities are not three: %q, %q, %q", w.a.user, w.b.user, w.admin.user)
	}
	return w
}

// agent makes an agent identity as hpcburst identity add does, and waits until RBAC lets it create Operations.
func (w *world) agent(t *testing.T, identitiesDir, name string) who {
	t.Helper()
	res, err := launcher.IssueIdentity(w.ctx, w.cp.Config, identitiesDir, name, false, io.Discard)
	if err != nil {
		t.Fatalf("identity add %s: %v", name, err)
	}
	id := identityOf(w.ctx, t, name, kubeconfigOf(t, res.Kubeconfig))
	if want := "system:serviceaccount:hpcburst-agents:" + name; id.user != want {
		t.Fatalf("the client of %s is %q, want %q", name, id.user, want)
	}
	cs, err := kubernetes.NewForConfig(id.cfg)
	if err != nil {
		t.Fatal(err)
	}
	// RBAC reaches the API server's authorizer a moment after the binding: wait until the agent may create.
	eventually(t, 30*time.Second, name+" may create Operations", func() (bool, string) {
		r, err := cs.AuthorizationV1().SelfSubjectAccessReviews().Create(w.ctx, &authorizationv1.SelfSubjectAccessReview{Spec: authorizationv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authorizationv1.ResourceAttributes{Namespace: exampleNS, Verb: "create", Group: v1alpha1.Group, Resource: "operations"}}}, metav1.CreateOptions{})
		return err == nil && r.Status.Allowed, fmt.Sprintf("%v %+v", err, r)
	})
	return id
}

// deployment is a served ModelDeployment of one Instance on the example Provider. Its over-allocation is off, set explicitly:
// absent is 1, the API server's default, which gives the Provider one more Instance and Allocation than the run's waits
// count (an Instance and its Allocation, each one), and the extra capacity is no part of what the run shows.
func deployment(name string) *v1alpha1.ModelDeployment {
	noOverAllocation := int32(0)
	return &v1alpha1.ModelDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: exampleNS},
		Spec: v1alpha1.ModelDeploymentSpec{
			Model: exampleModel,
			Source: v1alpha1.ModelSource{Served: &v1alpha1.ServingSpec{
				Checkpoint: v1alpha1.CheckpointSpec{Repository: "example-org/example-model", Revision: exampleRevision},
				Engine:     v1alpha1.EngineSpec{Name: "vllm", Version: "0.32.0", ImageDigest: exampleDigest, Image: "docker.io/vllm/vllm-openai"},
				Flags:      []v1alpha1.EngineFlag{{Name: "max-model-len", Value: "32768"}},
			}},
			Instances:      1,
			OverAllocation: &noOverAllocation,
			Providers:      v1alpha1.ProviderSelection{Names: []string{exampleProvider}},
			Workload: &v1alpha1.DeploymentWorkload{
				ContextTokens:  v1alpha1.ContextTokens{Typical: 4096},
				OutputTokens:   v1alpha1.OutputTokens{Typical: 512},
				TargetSessions: 4,
			},
		},
	}
}

// makeProvider creates the PlatformProfile and the Provider the deployment names, as the person.
func (w *world) makeProvider() *v1alpha1.Provider {
	t := w.t
	t.Helper()
	gpus, cpusPerGPU, mem := int32(4), int32(16), resource.MustParse("120Gi")
	maxTime := v1alpha1.Duration((48 * time.Hour).String())
	prof := &v1alpha1.PlatformProfile{
		ObjectMeta: metav1.ObjectMeta{Name: exampleProfile},
		Spec: v1alpha1.PlatformProfileSpec{ProviderKind: v1alpha1.PlatformBatch, HoldRelease: true, Partitions: []v1alpha1.PartitionProfile{{
			Name: examplePart, AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: exampleGPU, CPUArch: exampleArch},
			MaxTime: &maxTime, GPUsPerNode: &gpus, CPUsPerGPU: &cpusPerGPU, MemPerGPU: &mem,
		}}},
	}
	if err := w.admin.c.Create(w.ctx, prof); err != nil {
		t.Fatal(err)
	}
	prov := &v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: exampleProvider},
		Spec:       v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: exampleProfile}, AllowedPartitions: []string{examplePart}},
	}
	if err := w.admin.c.Create(w.ctx, prov); err != nil {
		t.Fatal(err)
	}
	w.markProviderReady(prov)
	return prov
}

// markProviderReady writes the status the Provider's controller would: Ready, with a read as recent as the controller's clock says.
func (w *world) markProviderReady(prov *v1alpha1.Provider) {
	t := w.t
	t.Helper()
	if err := w.refreshProvider(prov); err != nil {
		t.Fatalf("mark the Provider ready: %v", err)
	}
}

// refreshProvider reads the Provider again and stamps its readiness with the controller's clock.
func (w *world) refreshProvider(prov *v1alpha1.Provider) error {
	var cur v1alpha1.Provider
	if err := w.admin.c.Get(w.ctx, client.ObjectKeyFromObject(prov), &cur); err != nil {
		return err
	}
	apimeta.SetStatusCondition(&cur.Status.Conditions, metav1.Condition{Type: v1alpha1.ConditionProviderReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPolled, LastTransitionTime: metav1.NewTime(w.clock.Now())})
	readAt := metav1.NewTime(w.clock.Now().Truncate(time.Second))
	cur.Status.LastSuccessfulStatusRead = &readAt
	return w.admin.c.Status().Update(w.ctx, &cur, client.FieldOwner(v1alpha1.FieldManagerProvider))
}

// startLifecycle runs the real lifecycle controller's manager as the lifecycle identity, with its leader election on,
// and returns when it has stopped the manager. The controller reads a ServiceAccount's token, never the
// administrator's credential, so a verb its role lacks is a 403 in the audit log.
func (w *world) startLifecycle(clock lifecycle.Clock) (stop func()) {
	t := w.t
	t.Helper()
	builds := t.TempDir()
	if err := os.WriteFile(filepath.Join(builds, "hb-agent-"+exampleArch), []byte("a stand-in for the agent build"), 0o600); err != nil {
		t.Fatal(err)
	}
	mgr, _, err := lifecycle.NewManager(lifecycle.Options{
		Config: w.lc.cfg, CredentialsDir: t.TempDir(), AgentBuildsDir: builds, FrontConfigured: true,
		HealthAddr: "0", MetricsAddr: "0", LeaderElection: true, Checkpoints: stubCheckpoints{}, Clock: clock,
	})
	if err != nil {
		t.Fatalf("the lifecycle manager: %v", err)
	}
	mctx, cancel := context.WithCancel(w.ctx)
	done := make(chan error, 1)
	go func() { done <- mgr.Start(mctx) }()
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("the lifecycle manager stopped with an error: %v", err)
			}
		case <-time.After(90 * time.Second):
			t.Errorf("the lifecycle manager did not stop within 90 s")
		}
	}
	t.Cleanup(stop)
	return stop
}

// jsonUnmarshal is encoding/json's, named here so that the run's helpers read as one block.
func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
