package agentcli

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// hpcburst inventory (issue #662): the command around internal/inventory's lister and printer, which have tests of
// their own. These test what the command adds: the flags, the credential, the clock read once, and that nothing is
// written. The world is written by hand, and testdata/inventory.json is written by hand from docs/agent-cli.md.

// The owner-hash label values of agents A and B, worked out in the shell with sha256sum: the first 12 hex characters
// of the SHA-256 of the user name, behind "u-".
const (
	invHashA = "u-77d125ed9ec1"
	invHashB = "u-494c5a26e66f"
)

func invMeta(ns, name, user, hash, purpose string) metav1.ObjectMeta {
	m := metav1.ObjectMeta{Namespace: ns, Name: name}
	if user != "" {
		m.Annotations = map[string]string{v1alpha1.AnnotationOwner: user, v1alpha1.AnnotationPurpose: purpose}
		m.Labels = map[string]string{v1alpha1.LabelOwnerHash: hash}
	}
	return m
}

func invAlloc(m metav1.ObjectMeta, md, provider string, hardEnd time.Time, pol *v1alpha1.RetentionPolicy) *v1alpha1.Allocation {
	a := &v1alpha1.Allocation{ObjectMeta: m}
	a.Spec.ModelDeploymentRef = &v1alpha1.ObjectReference{Name: md}
	a.Spec.ProviderConfigReference = &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: provider}
	a.Spec.RetentionPolicy = pol
	a.Status.AtProvider = v1alpha1.AllocationObservation{Phase: v1alpha1.AllocationRunning, HardEnd: mt(hardEnd)}
	return a
}

func invInst(m metav1.ObjectMeta, allocation string) *v1alpha1.Instance {
	in := &v1alpha1.Instance{ObjectMeta: m}
	in.Spec.AllocationRef = v1alpha1.ObjectReference{Name: allocation}
	in.Status.Phase = v1alpha1.InstanceReady
	return in
}

func invOp(ns, name, md, user string, phase v1alpha1.OperationPhase, plan string) *v1alpha1.Operation {
	o := &v1alpha1.Operation{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	o.Spec = v1alpha1.OperationSpec{Action: v1alpha1.OperationActionScale, ModelDeploymentRef: v1alpha1.ObjectReference{Name: md}, Owner: user}
	o.Status.Phase, o.Status.Plan = phase, &v1alpha1.OperationPlan{ID: plan}
	return o
}

func invMD(m metav1.ObjectMeta, instances int32, end *time.Time, ready *int32) *v1alpha1.ModelDeployment {
	md := &v1alpha1.ModelDeployment{ObjectMeta: m}
	md.Spec.Instances = instances
	if end != nil {
		md.Spec.EndTime = mt(*end)
	}
	if ready != nil {
		md.Status.LastReconcileTime, md.Status.ReadyInstances = mt(t0), *ready
	}
	return md
}

// invWorld is the control plane of these tests, in namespace team (the caller's), with one deployment of agent B's in
// namespace other. alloc-a2 is a kept Allocation with no Instance; md-old and its children predate the owner.
func invWorld() []client.Object {
	one, h48, h24, h3, h2, h1 := int32(1), t0.Add(48*time.Hour), t0.Add(24*time.Hour), t0.Add(3*time.Hour), t0.Add(2*time.Hour), t0.Add(time.Hour)
	read := t0
	sano := &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: "sano"}, Spec: v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: "batch"}}}
	sano.Status.LastSuccessfulStatusRead = mt(read)
	maxIdle := v1alpha1.Duration("1h")
	kept := &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep, Until: mt(h24)}
	a2 := invMeta("team", "alloc-a2", userA, invHashA, "nightly run")
	a2.Annotations[v1alpha1.AnnotationIdleSince] = "2026-11-20T09:30:00Z"
	return []client.Object{
		sano, &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: "helios"}, Spec: v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: "batch-nocap"}}},
		&v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: "batch"}, Spec: v1alpha1.PlatformProfileSpec{MaxIdle: &maxIdle}},
		&v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: "batch-nocap"}},

		invMD(invMeta("team", "md-alice", userA, invHashA, "nightly run"), 2, &h48, &one),
		invMD(invMeta("team", "md-old", "", "", ""), 1, nil, nil),
		invMD(invMeta("other", "md-other", userB, invHashB, "elsewhere"), 1, nil, &one),

		invAlloc(invMeta("team", "alloc-a1", userA, invHashA, "nightly run"), "md-alice", "sano", h3, nil),
		invAlloc(a2, "md-alice", "sano", h2, kept),
		invAlloc(invMeta("team", "alloc-old", "", "", ""), "md-old", "helios", h1, nil),
		invAlloc(invMeta("other", "alloc-o1", userB, invHashB, "elsewhere"), "md-other", "sano", h1, nil),

		invInst(invMeta("team", "inst-a1", userA, invHashA, "nightly run"), "alloc-a1"),
		invInst(invMeta("team", "inst-old", "", "", ""), "alloc-old"),

		invOp("team", "op-wait", "md-alice", userA, v1alpha1.OperationPhaseAwaitingApproval, "plan-1"),
		invOp("team", "op-planned", "md-alice", userA, v1alpha1.OperationPhasePlanned, "plan-0"),
		invOp("other", "op-other", "md-other", userB, v1alpha1.OperationPhaseAwaitingApproval, "plan-2"),
	}
}

// invRun runs a command at the clock 90 s after Provider sano's last read.
func invRun(t *testing.T, args ...string) (*world, int) {
	t.Helper()
	w := newWorld(t, invWorld())
	w.now = t0.Add(90 * time.Second)
	return w, w.run("", args...)
}

func TestInventoryJSONIsTheHandWrittenGolden(t *testing.T) {
	w, code := invRun(t, "inventory", "--json")
	if code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	if strings.Contains(w.out.String(), "null") {
		t.Errorf("the JSON holds a null:\n%s", w.out.String())
	}
	sameJSON(t, w.out.Bytes(), readGolden(t, "inventory.json"))
}

// The command is a read: nothing goes through the recorder as a write. The recorder is shown to catch one by the same
// run's scale, which creates an Operation.
func TestInventoryWritesNothing(t *testing.T) {
	w := newWorld(t, invWorld())
	w.now = t0.Add(90 * time.Second)
	for _, args := range [][]string{
		{"inventory"}, {"inventory", "--json"}, {"inventory", "-A"}, {"inventory", "-A", "--json"}, {"inventory", "-n", "other"},
		{"inventory", "--owner", userA}, {"inventory", "-A", "--owner", userB, "--json"},
	} {
		if code := w.run("", args...); code != exitOK {
			t.Fatalf("%v: exit %d, stderr %q", args, code, w.errs.String())
		}
		if w.out.Len() == 0 {
			t.Errorf("%v: printed nothing, so the run read nothing to show", args)
		}
		if got := w.writes(); len(got) != 0 {
			t.Fatalf("%v: wrote %v; an inventory writes nothing", args, got)
		}
	}
	w.names = []string{"op-1111111111"}
	w.onPause = func() {}
	_ = w.run("", "scale", "md-alice", "--instances", "1", "--timeout", "1s")
	if got := w.writes(); !reflect.DeepEqual(got, []string{"create *v1alpha1.Operation"}) {
		t.Errorf("the recorder caught %v from a scale, want the one create: it would not have caught a write of the inventory", got)
	}
}

func TestInventoryOwnerSelectsOneOwner(t *testing.T) {
	w, code := invRun(t, "inventory", "--owner", userA, "-A")
	if code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	out := w.out.String()
	contains(t, "--owner A", out, "selected by owner "+userA, "team/md-alice:", "team/alloc-a1:", "team/alloc-a2:", "team/inst-a1:", "team/op-wait:")
	lacks(t, "--owner A", out, "md-old", "alloc-old", "inst-old", "md-other", "alloc-o1", "op-other", "op-planned")

	w, code = invRun(t, "inventory", "--owner", userB, "-A")
	if code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	out = w.out.String()
	contains(t, "--owner B", out, "other/md-other:", "other/alloc-o1:", "other/op-other:")
	lacks(t, "--owner B", out, "md-alice", "alloc-a", "inst-a1", "md-old", "op-wait")
}

// A deployment with no owner annotation is in the full view with its owner unknown, and only --owner leaves it out.
func TestInventoryListsADeploymentWithNoOwnerAsUnknown(t *testing.T) {
	w, code := invRun(t, "inventory")
	if code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	contains(t, "the full view", w.out.String(), "team/md-old: owner unknown (no owner annotation)", "team/alloc-old: ModelDeployment md-old, owner unknown (no owner annotation)",
		"team/inst-old: owner unknown (no owner annotation)", "kept, no Instance")
}

func TestInventoryScope(t *testing.T) {
	w, code := invRun(t, "inventory", "--json")
	if code != exitOK || !strings.Contains(w.out.String(), `"namespace": "team"`) || strings.Contains(w.out.String(), "md-other") {
		t.Errorf("the default namespace is the kubeconfig's: exit %d\n%s", code, w.out.String())
	}
	w, code = invRun(t, "inventory", "-n", "other")
	if code != exitOK {
		t.Fatal(w.errs.String())
	}
	contains(t, "-n other", w.out.String(), "namespace other", "other/md-other:")
	lacks(t, "-n other", w.out.String(), "team/")
	w, code = invRun(t, "inventory", "-A")
	if code != exitOK {
		t.Fatal(w.errs.String())
	}
	contains(t, "-A", w.out.String(), "every namespace", "team/md-alice:", "other/md-other:")
}

func TestInventoryUsageErrorsExitTwoBeforeAnythingIsRead(t *testing.T) {
	for _, args := range [][]string{
		{"inventory", "extra"},
		{"inventory", "-A", "-n", "team"},
		{"inventory", "--all-namespaces", "--namespace", "team"},
		{"inventory", "--owner", ""},
		{"inventory", "--owner"},
		{"inventory", "--bogus"},
	} {
		w := newWorld(t, invWorld())
		connected := false
		inner := w.cli.connect
		w.cli.connect = func(ctx context.Context, kc string, stderr io.Writer) (*caller, error) {
			connected = true
			return inner(ctx, kc, stderr)
		}
		if code := w.run("", args...); code != exitUsage {
			t.Errorf("%v: exit %d, want 2; stderr %q", args, code, w.errs.String())
		}
		if connected {
			t.Errorf("%v: the command asked the API server who the caller is before it checked its flags", args)
		}
		if w.out.Len() != 0 {
			t.Errorf("%v: printed on stdout: %q", args, w.out.String())
		}
	}
	w := newWorld(t, invWorld())
	if code := w.run("", "inventory", "-h"); code != exitOK {
		t.Errorf("-h: exit %d, want 0", code)
	}
	contains(t, "-h", w.errs.String(), "-A", "-owner", "-json", "-kubeconfig")
}

// The credential is every command's: a kubeconfig that is missing is a usage error, and a refused identity a failure,
// and nothing is listed either way.
func TestInventoryTakesTheCallersCredentialOrFails(t *testing.T) {
	for _, c := range []struct {
		err  error
		code int
	}{{usageError{errNoKubeconfig}, exitUsage}, {errController, exitFail}} {
		w := newWorld(t, invWorld())
		w.cli.connect = func(context.Context, string, io.Writer) (*caller, error) { return nil, c.err }
		if code := w.run("", "inventory"); code != c.code {
			t.Errorf("%v: exit %d, want %d", c.err, code, c.code)
		}
		if w.out.Len() != 0 {
			t.Errorf("%v: printed %q", c.err, w.out.String())
		}
		contains(t, "stderr", w.errs.String(), c.err.Error())
	}
}

// The clock is read once, and every age counts from that reading.
func TestInventoryReadsTheClockOnce(t *testing.T) {
	w := newWorld(t, invWorld())
	reads := 0
	w.cli.now = func() time.Time { reads++; return t0.Add(90 * time.Second) }
	if code := w.run("", "inventory", "-A", "--json"); code != exitOK {
		t.Fatal(w.errs.String())
	}
	if reads != 1 {
		t.Errorf("the command read the clock %d times, want once", reads)
	}
}

// A failure to read is a failure that names the kind, never a partial listing.
func TestInventoryAFailedListIsAFailureAndPrintsNothing(t *testing.T) {
	w := newWorld(t, invWorld())
	base := w.rec.Client
	w.rec.Client = failingLists{Client: base, kind: "AllocationList"}
	if code := w.run("", "inventory"); code != exitFail {
		t.Errorf("exit %d, want 1", code)
	}
	contains(t, "stderr", w.errs.String(), "list Allocations", "boom")
	if w.out.Len() != 0 {
		t.Errorf("printed %q after a failed list", w.out.String())
	}
}

type failingLists struct {
	client.Client
	kind string
}

func (f failingLists) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if strings.HasSuffix(reflect.TypeOf(list).Elem().Name(), f.kind) {
		return errors.New("boom")
	}
	return f.Client.List(ctx, list, opts...)
}
