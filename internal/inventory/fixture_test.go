package inventory_test

import (
	"context"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The two owners of the fixture, and their owner-hash label values, worked out in the shell with sha256sum (the
// first 12 hex characters of the SHA-256 of the name, behind "u-"), never by OwnerHash.
const (
	alice     = "example-alice"
	bob       = "example-bob"
	aliceHash = "u-2dd7f9cfae4d"
	bobHash   = "u-601f67a26a92"
)

// The clock of the tests: the command reads it once. sano's last read is 90 s before it.
var (
	t0  = time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	now = t0.Add(90 * time.Second)
)

func mt(t time.Time) *metav1.Time { m := metav1.NewTime(t); return &m }

func newReader(t *testing.T, objs ...client.Object) client.Reader {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
}

// owned is the metadata of an object the lifecycle controller stamped: the annotations and the owner-hash label.
func owned(ns, name, user, hash, purpose string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: ns, Name: name,
		Annotations: map[string]string{v1alpha1.AnnotationOwner: user, v1alpha1.AnnotationPurpose: purpose},
		Labels:      map[string]string{v1alpha1.LabelOwnerHash: hash}}
}

// bare is the metadata of an object that predates the copy of the owner: none of its own.
func bare(ns, name string) metav1.ObjectMeta { return metav1.ObjectMeta{Namespace: ns, Name: name} }

func withIdleSince(m metav1.ObjectMeta, v string) metav1.ObjectMeta {
	a := map[string]string{}
	for k, x := range m.Annotations {
		a[k] = x
	}
	a[v1alpha1.AnnotationIdleSince] = v
	m.Annotations = a
	return m
}

func alloc(m metav1.ObjectMeta, md, provider string, phase v1alpha1.AllocationPhase, hardEnd *metav1.Time, pol *v1alpha1.RetentionPolicy) *v1alpha1.Allocation {
	a := &v1alpha1.Allocation{ObjectMeta: m}
	if md != "" {
		a.Spec.ModelDeploymentRef = &v1alpha1.ObjectReference{Name: md}
	}
	if provider != "" {
		a.Spec.ProviderConfigReference = &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: provider}
	}
	a.Spec.RetentionPolicy = pol
	a.Status.AtProvider = v1alpha1.AllocationObservation{Phase: phase, HardEnd: hardEnd}
	return a
}

func inst(m metav1.ObjectMeta, allocation string, phase v1alpha1.InstancePhase) *v1alpha1.Instance {
	in := &v1alpha1.Instance{ObjectMeta: m}
	in.Spec.AllocationRef = v1alpha1.ObjectReference{Name: allocation}
	in.Status.Phase = phase
	return in
}

func op(ns, name, md, user string, action v1alpha1.OperationAction, phase v1alpha1.OperationPhase, planID string) *v1alpha1.Operation {
	o := &v1alpha1.Operation{ObjectMeta: bare(ns, name)}
	o.Spec = v1alpha1.OperationSpec{Action: action, ModelDeploymentRef: v1alpha1.ObjectReference{Name: md}, Owner: user}
	o.Status.Phase = phase
	if planID != "" {
		o.Status.Plan = &v1alpha1.OperationPlan{ID: planID}
	}
	return o
}

func deployment(m metav1.ObjectMeta, instances int32, end *metav1.Time, reconciled bool, ready int32) *v1alpha1.ModelDeployment {
	md := &v1alpha1.ModelDeployment{ObjectMeta: m}
	md.Spec.Instances, md.Spec.EndTime = instances, end
	if reconciled {
		md.Status.LastReconcileTime, md.Status.ReadyInstances = mt(t0), ready
	}
	return md
}

func provider(name, profile string, read *time.Time) *v1alpha1.Provider {
	p := &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: name}}
	p.Spec.PlatformProfileRef = v1alpha1.ObjectReference{Name: profile}
	if read != nil {
		p.Status.LastSuccessfulStatusRead = mt(*read)
	}
	return p
}

func profile(name string, maxIdle *v1alpha1.Duration) *v1alpha1.PlatformProfile {
	return &v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: v1alpha1.PlatformProfileSpec{MaxIdle: maxIdle}}
}

func dur(s string) *v1alpha1.Duration { d := v1alpha1.Duration(s); return &d }

// world is the fixture of the lister's tests: two owners, and objects that predate the owner. Namespace
// team holds most of it; namespace other holds one deployment of alice's.
//
//	md-alice  alice  sano   alloc-a1 (hosts inst-a1), alloc-a2 (kept, idle, no Instance), alloc-a3 (pending, hosts inst-a3),
//	                        alloc-a4 (no owner of its own, hosts inst-a4-old, which has none either), alloc-a5 (alice's, with an idle
//	                        anchor, and hosts the unlabelled inst-a5-old)
//	md-bob    bob    helios alloc-b1 (no anchor), alloc-b2 (an anchor that is no time, only an ended Instance on it), alloc-b3
//	                        (Ended, on ghost), alloc-b4 (ReleaseWhenIdle, idle, inside its limit), alloc-b5 (a stale anchor, and inst-b5 is on it)
//	md-old    none   helios alloc-old (hosts inst-old)
//	alloc-bare names nothing and has no status.
func world() []client.Object {
	return []client.Object{
		provider("sano", "batch", &t0), provider("helios", "batch-nocap", nil), provider("ghost", "missing", &t0),
		profile("batch", dur("1h")), profile("batch-nocap", nil),

		deployment(owned("team", "md-alice", alice, aliceHash, "nightly run"), 2, mt(t0.Add(48*time.Hour)), true, 1),
		deployment(owned("team", "md-bob", bob, bobHash, "ablation"), 1, nil, false, 0),
		deployment(bare("team", "md-old"), 1, nil, true, 0),
		deployment(owned("other", "md-other", alice, aliceHash, "elsewhere"), 1, nil, true, 1),

		alloc(owned("team", "alloc-a1", alice, aliceHash, "nightly run"), "md-alice", "sano", v1alpha1.AllocationRunning, mt(t0.Add(3*time.Hour)), nil),
		alloc(withIdleSince(owned("team", "alloc-a2", alice, aliceHash, "nightly run"), "2026-11-20T09:30:00Z"), "md-alice", "sano", v1alpha1.AllocationRunning,
			mt(t0.Add(2*time.Hour)), &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep, Until: mt(t0.Add(24 * time.Hour))}),
		alloc(owned("team", "alloc-a3", alice, aliceHash, "nightly run"), "md-alice", "sano", v1alpha1.AllocationPending, nil,
			&v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur("15m")}),
		alloc(bare("team", "alloc-a4"), "md-alice", "sano", v1alpha1.AllocationRunning, mt(t0.Add(3*time.Hour)), nil),
		alloc(withIdleSince(owned("team", "alloc-a5", alice, aliceHash, "nightly run"), "2026-11-20T09:40:00Z"), "md-alice", "sano", v1alpha1.AllocationRunning, mt(t0.Add(3*time.Hour)), nil),
		alloc(owned("team", "alloc-b1", bob, bobHash, "ablation"), "md-bob", "helios", v1alpha1.AllocationRunning, mt(t0.Add(time.Hour)), nil),
		alloc(withIdleSince(owned("team", "alloc-b2", bob, bobHash, "ablation"), "not a time"), "md-bob", "helios", v1alpha1.AllocationRunning, mt(t0.Add(time.Hour)), nil),
		alloc(withIdleSince(owned("team", "alloc-b3", bob, bobHash, "ablation"), "2026-11-20T08:00:00Z"), "md-bob", "ghost", v1alpha1.AllocationEnded, mt(t0.Add(-time.Hour)), nil),
		alloc(withIdleSince(owned("team", "alloc-b5", bob, bobHash, "ablation"), "2026-11-20T09:00:00Z"), "md-bob", "helios", v1alpha1.AllocationRunning, mt(t0.Add(time.Hour)), nil),
		alloc(withIdleSince(owned("team", "alloc-b4", bob, bobHash, "ablation"), "2026-11-20T09:50:00Z"), "md-bob", "helios", v1alpha1.AllocationRunning,
			mt(t0.Add(time.Hour)), &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur("30m")}),
		alloc(bare("team", "alloc-old"), "md-old", "helios", v1alpha1.AllocationRunning, mt(t0.Add(time.Hour)), nil),
		alloc(bare("team", "alloc-bare"), "", "", "", nil, nil),
		alloc(owned("other", "alloc-o1", alice, aliceHash, "elsewhere"), "md-other", "sano", v1alpha1.AllocationRunning, mt(t0.Add(time.Hour)), nil),

		inst(owned("team", "inst-a1", alice, aliceHash, "nightly run"), "alloc-a1", v1alpha1.InstanceReady),
		inst(owned("team", "inst-a3", alice, aliceHash, "nightly run"), "alloc-a3", v1alpha1.InstancePending),
		inst(bare("team", "inst-a4-old"), "alloc-a4", v1alpha1.InstanceReady),
		inst(bare("team", "inst-a5-old"), "alloc-a5", v1alpha1.InstanceReady),
		inst(owned("team", "inst-b2-ended", bob, bobHash, "ablation"), "alloc-b2", v1alpha1.InstanceEnded),
		inst(owned("team", "inst-b5", bob, bobHash, "ablation"), "alloc-b5", v1alpha1.InstanceReady),
		inst(bare("team", "inst-old"), "alloc-old", ""),

		op("team", "op-wait", "md-alice", alice, v1alpha1.OperationActionScale, v1alpha1.OperationPhaseAwaitingApproval, "plan-1"),
		op("team", "op-planned", "md-alice", alice, v1alpha1.OperationActionEnd, v1alpha1.OperationPhasePlanned, "plan-0"),
		op("team", "op-bob", "md-bob", bob, v1alpha1.OperationActionEnd, v1alpha1.OperationPhaseAwaitingApproval, "plan-2"),
		op("other", "op-other", "md-other", alice, v1alpha1.OperationActionRelease, v1alpha1.OperationPhaseAwaitingApproval, "plan-3"),
	}
}

// listCall is one List the lister made: the kind, the namespace and the label selector it sent.
type listCall struct{ kind, namespace, selector string }

// recordingReader keeps what the lister asked the API server for.
type recordingReader struct {
	client.Reader
	calls []listCall
}

func (r *recordingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	lo := &client.ListOptions{}
	for _, o := range opts {
		o.ApplyToList(lo)
	}
	c := listCall{kind: kindOf(list), namespace: lo.Namespace}
	if lo.LabelSelector != nil {
		c.selector = lo.LabelSelector.String()
	}
	r.calls = append(r.calls, c)
	return r.Reader.List(ctx, list, opts...)
}

func kindOf(l client.ObjectList) string {
	switch l.(type) {
	case *v1alpha1.ModelDeploymentList:
		return "ModelDeployments"
	case *v1alpha1.AllocationList:
		return "Allocations"
	case *v1alpha1.InstanceList:
		return "Instances"
	case *v1alpha1.OperationList:
		return "Operations"
	case *v1alpha1.ProviderList:
		return "Providers"
	case *v1alpha1.PlatformProfileList:
		return "PlatformProfiles"
	}
	return "?"
}
