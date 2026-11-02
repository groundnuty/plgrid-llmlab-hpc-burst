package quickstart

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/lifecycle"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// The quickstart applies profiles/helios.yaml, examples/provider-helios-gh200.yaml and
// examples/modeldeployment-qwen25-instruct.yaml. This file builds the request that the lifecycle
// controller makes from exactly those three files, and runs it through the check that the Slurm
// provider runs before it submits (PR 206 review, B1): the example Provider asks for QOS now, whose
// maxWall is 12 h, and a request sized from the partition's maxTime alone (48 h) would be refused
// before it reached Slurm, with the quickstart's deployment never starting. Nothing is compared
// with a value derived from the code under test; the expected values are worked out from
// profiles/helios.yaml in the comments below.
//
// The pieces are the real ones: the lifecycle Reconciler (internal/lifecycle) over a fake API
// server, slurm.ProfileReader (which also checks that the profile lists the Provider's qos) and
// slurm.CheckRequest, called with the request fields that internal/providerctl/create.go gives it.

// stubCheckpoints answers with a fixed file list: the quickstart's deployment pins a Hub revision
// that a test does not fetch.
type stubCheckpoints struct{}

func (stubCheckpoints) Files(context.Context, v1alpha1.CheckpointSpec) ([]deploymentid.File, error) {
	return []deploymentid.File{
		{Path: "config.json", SHA256: strings.Repeat("1", 64)},
		{Path: "model.safetensors", SHA256: strings.Repeat("2", 64)},
	}, nil
}

func (stubCheckpoints) TemplateSources(context.Context, v1alpha1.CheckpointSpec, []deploymentid.File) (deploymentid.TemplateSources, error) {
	return deploymentid.TemplateSources{}, nil
}

// quickstartPlan is what one round of the lifecycle controller makes of the quickstart's objects.
type quickstartPlan struct {
	allocs    []v1alpha1.Allocation
	degraded  *metav1.Condition // the deployment's Degraded condition, nil when absent
	req       provider.SubmitRequest
	profile   provider.Profile
	settings  provider.Settings
	checkErr  error // slurm.CheckRequest on req, nil when there is no Allocation
	profileOK error // slurm.ProfileReader.Read
}

func mustYAML(t *testing.T, path string, into any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		t.Fatal(err)
	}
	// The placeholders the guide tells a user to replace, with values of the right shape.
	text := strings.NewReplacer("<account>", "exampleaccount", "<grant>", "examplegrant", "<user>", "exampleuser").Replace(string(raw))
	if err := yaml.UnmarshalStrict([]byte(text), into); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

// planQuickstart loads the three files, lets the caller edit the two objects a user would edit,
// runs the lifecycle controller until it makes an Allocation (or three rounds pass), and checks
// the Allocation's request as the provider does.
func planQuickstart(t *testing.T, edit func(*v1alpha1.Provider, *v1alpha1.ModelDeployment)) quickstartPlan {
	t.Helper()
	var prof v1alpha1.PlatformProfile
	var prov v1alpha1.Provider
	var md v1alpha1.ModelDeployment
	mustYAML(t, "profiles/helios.yaml", &prof)
	mustYAML(t, "examples/provider-helios-gh200.yaml", &prov)
	mustYAML(t, "examples/modeldeployment-qwen25-instruct.yaml", &md)
	if edit != nil {
		edit(&prov, &md)
	}

	// The credentials file and the key it names, as the guide describes them: mode 600, owned by
	// the controller's user.
	dir := t.TempDir()
	key := filepath.Join(dir, "key")
	cfg := filepath.Join(dir, "helios.ssh")
	if err := os.WriteFile(key, []byte("not a real key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg, []byte("Host helios\nHostName login.example\nUser exampleuser\nIdentityFile "+key+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	prov.Spec.CredentialsRef = &v1alpha1.CredentialsReference{File: cfg}

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{v1alpha1.AddToScheme, appsv1.AddToScheme, coordinationv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)
	clock := fake.NewClock(start)
	uid := 0
	funcs := interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if obj.GetUID() == "" {
			uid++
			obj.SetUID(types.UID(fmt.Sprintf("00000000-0000-0000-0000-%012d", uid)))
		}
		if ts := obj.GetCreationTimestamp(); ts.IsZero() {
			obj.SetCreationTimestamp(metav1.NewTime(clock.Now()))
		}
		return c.Create(ctx, obj, opts...)
	}}
	// The Provider reads Ready: its last successful read is now. Its status is not a subresource
	// in this fake, so Create keeps it.
	now := metav1.NewTime(start)
	prov.Status.LastSuccessfulStatusRead = &now
	apimeta.SetStatusCondition(&prov.Status.Conditions, metav1.Condition{Type: v1alpha1.ConditionProviderReady, Status: metav1.ConditionTrue, Reason: "Read", LastTransitionTime: now})
	md.Generation = 1
	c := clientfake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&prof, &prov).
		WithStatusSubresource(&v1alpha1.ModelDeployment{}, &v1alpha1.Instance{}, &v1alpha1.Allocation{}, &v1alpha1.PlatformProfile{}).
		WithInterceptorFuncs(funcs).Build()
	ctx := context.Background()
	if err := c.Create(ctx, &md); err != nil {
		t.Fatal(err)
	}
	clock.Advance(10 * time.Second)
	r := &lifecycle.Reconciler{
		Client: c, Reader: c, Clock: clock,
		Backend:     &lifecycle.Backend{Reader: c, Clock: clock, CredentialsDir: t.TempDir()},
		Checkpoints: stubCheckpoints{}, Recorder: &lifecycle.MemoryRecorder{},
		Config: lifecycle.Config{CredentialsDir: t.TempDir(), FrontConfigured: true,
			AgentDigest: func(string) (string, error) { return strings.Repeat("a", 64), nil }},
	}
	key2 := types.NamespacedName{Namespace: md.Namespace, Name: md.Name}
	var plan quickstartPlan
	for round := 0; round < 3 && len(plan.allocs) == 0; round++ {
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key2}); err != nil {
			t.Fatalf("the lifecycle controller fails on the quickstart's objects: %v", err)
		}
		var l v1alpha1.AllocationList
		if err := c.List(ctx, &l, client.InNamespace(md.Namespace)); err != nil {
			t.Fatal(err)
		}
		plan.allocs = l.Items
	}
	var got v1alpha1.ModelDeployment
	if err := c.Get(ctx, key2, &got); err != nil {
		t.Fatal(err)
	}
	plan.degraded = apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionDegraded)
	if len(plan.allocs) != 1 || plan.allocs[0].Spec.ForProvider.Batch == nil {
		return plan
	}

	// The provider's side: the profile and settings as ReadProfile gives them, and the request as
	// providerctl's Create builds it (internal/providerctl/create.go: the tag, the profile's
	// holdRelease, the Provider's account and qos, and the Allocation's batch parameters).
	v, err := (&slurm.ProfileReader{Reader: c, Home: dir}).Read(ctx, prov.Name)
	plan.profileOK = err
	plan.profile, plan.settings = v.Profile, v.Settings
	a := plan.allocs[0]
	plan.req = provider.SubmitRequest{
		Tag: v1alpha1.JobTag(a.Namespace, a.Name, a.UID), Hold: v.Profile.Spec.HoldRelease,
		Account: v.Settings.Account, QOS: v.Settings.QOS, Params: *a.Spec.ForProvider.Batch,
	}
	if err == nil {
		plan.checkErr = slurm.CheckRequest(plan.req, plan.profile, plan.settings)
	}
	return plan
}

// The quickstart's Helios deployment on the quickstart's Helios Provider gets one Allocation whose
// request the Slurm provider accepts. Worked out from profiles/helios.yaml, not from the code:
//   - partition plgrid-gpu-gh200: maxTime 48h, cpusPerGpu 72, memPerGpu 122375Mi;
//   - QOS now: maxWall 12h, maxGpusPerUser 1, maxCpusPerUser 72, maxMemPerUser 122400Mi; the
//     partition's own QOS, plgrid, sets none of these four, so now's apply;
//   - the deployment needs one GPU (no tensor or pipeline parallel flag), so the request is 1 GPU,
//     72 CPUs and 122375Mi (below now's 122400Mi), and its time limit is the smaller of 48h and
//     12h: 12h;
//   - the lead an Instance needs is the profile's drainWindow 2h10m + handoverMargin 30m +
//     health.startTimeout 1h = 3h40m, below 12h, so the partition is not too short.
func TestQuickstartHeliosRequestIsAcceptedUnderNow(t *testing.T) {
	plan := planQuickstart(t, nil)
	if plan.degraded != nil && plan.degraded.Status == metav1.ConditionTrue {
		t.Logf("Degraded: %s: %s", plan.degraded.Reason, plan.degraded.Message)
	}
	if len(plan.allocs) != 1 {
		t.Fatalf("the lifecycle controller made %d Allocations from the quickstart's objects, want 1 (Degraded: %v)", len(plan.allocs), plan.degraded)
	}
	b := plan.allocs[0].Spec.ForProvider.Batch
	if b == nil {
		t.Fatal("the Allocation has no batch parameters")
	}
	want := resource.MustParse("122375Mi")
	if b.Partition != "plgrid-gpu-gh200" || b.GPUs != 1 || b.CPUsPerTask != 72 || b.Nodes != 1 || b.Memory.Cmp(want) != 0 {
		t.Errorf("the request is %+v; want partition plgrid-gpu-gh200, 1 GPU, 72 CPUs, 1 node, 122375Mi", *b)
	}
	if b.TimeLimit != "12h0m0s" {
		t.Errorf("the time limit is %q; want 12h0m0s, the maxWall of QOS now (the partition's maxTime is 48h)", b.TimeLimit)
	}
	if lim, err := b.TimeLimit.AsDuration(); err != nil || lim <= 3*time.Hour+40*time.Minute {
		t.Errorf("the time limit %q is not above the lead of 3h40m an Instance needs (%v)", b.TimeLimit, err)
	}
	if plan.profileOK != nil {
		t.Fatalf("the Slurm provider cannot read the example Provider with the Helios profile: %v", plan.profileOK)
	}
	if plan.req.QOS != "now" {
		t.Errorf("the request names QOS %q; the example Provider asks for now", plan.req.QOS)
	}
	if plan.checkErr != nil {
		t.Errorf("the Slurm provider refuses the quickstart's request before it submits: %v", plan.checkErr)
	}
}

// Controls: each shows the test above can see what it claims to.
func TestQuickstartHeliosRequestControls(t *testing.T) {
	t.Run("without qos the limit is the partition's 48h, which the check accepts", func(t *testing.T) {
		plan := planQuickstart(t, func(p *v1alpha1.Provider, _ *v1alpha1.ModelDeployment) { p.Spec.QOS = "" })
		if len(plan.allocs) != 1 || plan.allocs[0].Spec.ForProvider.Batch.TimeLimit != "48h0m0s" {
			t.Fatalf("want one Allocation with a 48h0m0s limit, got %d (%v)", len(plan.allocs), plan.degraded)
		}
		if plan.profileOK != nil || plan.checkErr != nil {
			t.Errorf("the request without qos is refused: %v %v", plan.profileOK, plan.checkErr)
		}
	})
	t.Run("a 48h request under now is refused by the check", func(t *testing.T) {
		// What the lifecycle made before QOS-aware sizing: the partition's maxTime alone.
		plan := planQuickstart(t, nil)
		if len(plan.allocs) != 1 || plan.profileOK != nil {
			t.Fatalf("no request to alter: %d Allocations, %v", len(plan.allocs), plan.profileOK)
		}
		req := plan.req
		req.Params.TimeLimit = "48h0m0s"
		err := slurm.CheckRequest(req, plan.profile, plan.settings)
		if err == nil || !strings.Contains(err.Error(), "maxWall") {
			t.Errorf("a 48h request under QOS now: %v; want a refusal that names maxWall", err)
		}
	})
	t.Run("a lease under the lead gives PartitionTooShort and no Allocation", func(t *testing.T) {
		plan := planQuickstart(t, func(_ *v1alpha1.Provider, md *v1alpha1.ModelDeployment) {
			d := v1alpha1.Duration("3h0m0s")
			md.Spec.LeaseLimit = &d
		})
		if len(plan.allocs) != 0 {
			t.Fatalf("a 3h lease made %d Allocations; the lead is 3h40m", len(plan.allocs))
		}
		if plan.degraded == nil || plan.degraded.Reason != v1alpha1.ReasonPartitionTooShort {
			t.Errorf("Degraded is %v; want PartitionTooShort", plan.degraded)
		}
	})
}
