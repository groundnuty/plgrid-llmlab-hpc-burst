package e2e

// The capacity guard's bypass matrix for a ModelDeployment (issue #589, PR 3; plan section 10, HBEP-10, The
// capacity guard), as agents A and B with real tokens and as the person. Each refused case names the
// policy that refused it. Each accepted case is shown to have taken effect, since "accepted" would pass for
// a write that did nothing. Each case is made through the transport named: the object (update), a merge
// patch, server-side apply, and the scale subresource through the scale client that kubectl scale uses.
//
// The mapping of cases to the guard's validations, for the mutation runs (a mutant makes one validation
// true and one of these must fail): delete -> the delete rows in admission_depth_test.go; status -> the
// status row there; create ownerReferences -> "a create with ownerReferences"; scale -> "scale"; instances
// -> "a lower count"; endTime -> "an end time"; source -> "spec.source"; rest of spec -> "the rest of the
// spec"; labels, finalizers, ownerReferences, annotations -> the metadata rows.

import (
	"fmt"
	"strings"
	"testing"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/scale"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// writer is whoever sends a write: an agent with its real token, or the person (the admin) as a dry run,
// which runs admission and stores nothing.
type writer struct {
	name   string
	c      client.Client
	dyn    dynamic.Interface
	scales scale.ScalesGetter
	dry    bool
}

func agentWriter(a *agent) writer {
	return writer{name: a.name, c: a.c, dyn: a.dyn, scales: a.scales}
}

func personWriter(t *testing.T) writer {
	t.Helper()
	dyn, err := dynamic.NewForConfig(cfg)
	must(t, err, "the admin's dynamic client")
	return writer{name: "the person", c: c, dyn: dyn, scales: scaleClient(t, cfg), dry: true}
}

func (w writer) dryRun() []string {
	if w.dry {
		return []string{metav1.DryRunAll}
	}
	return nil
}

// update reads md as the writer, applies edit and sends the whole object.
func (w writer) update(md *v1alpha1.ModelDeployment, edit func(*v1alpha1.ModelDeployment)) error {
	cur := &v1alpha1.ModelDeployment{}
	if err := w.c.Get(bg(), client.ObjectKeyFromObject(md), cur); err != nil {
		return fmt.Errorf("%s's read: %w", w.name, err)
	}
	edit(cur)
	if w.dry {
		return w.c.Update(bg(), cur, client.DryRunAll)
	}
	return w.c.Update(bg(), cur)
}

// mergePatch sends a JSON merge patch.
func (w writer) mergePatch(md *v1alpha1.ModelDeployment, body string) error {
	patch := client.RawPatch(types.MergePatchType, []byte(body))
	if w.dry {
		return w.c.Patch(bg(), md.DeepCopy(), patch, client.DryRunAll)
	}
	return w.c.Patch(bg(), md.DeepCopy(), patch)
}

// apply sends a server-side apply of spec fields, taking ownership of a conflict.
func (w writer) apply(md *v1alpha1.ModelDeployment, spec map[string]any) error {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": v1alpha1.GroupVersion.String(), "kind": "ModelDeployment",
		"metadata": map[string]any{"name": md.Name, "namespace": md.Namespace},
		"spec":     spec,
	}}
	_, err := w.dyn.Resource(mdResource).Namespace(md.Namespace).Apply(bg(), md.Name, u,
		metav1.ApplyOptions{FieldManager: "test-" + w.name, Force: true, DryRun: w.dryRun()})
	return err
}

// scaleTo sets the scale subresource's replicas, as kubectl scale does.
func (w writer) scaleTo(md *v1alpha1.ModelDeployment, n int32) error {
	_, err := w.scales.Scales(md.Namespace).Update(bg(), mdGroupRes, &autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{Name: md.Name, Namespace: md.Namespace},
		Spec:       autoscalingv1.ScaleSpec{Replicas: n},
	}, metav1.UpdateOptions{DryRun: w.dryRun()})
	return err
}

// ownerRefToAConfigMap is a reference to an object that need not exist; the garbage collector would
// delete the ModelDeployment when it is gone.
func ownerRefToAConfigMap() metav1.OwnerReference {
	return metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: "short-lived", UID: "00000000-0000-0000-0000-000000000001"}
}

type guardCase struct {
	name string
	// base changes the object the case starts from (agentMD's: 3 instances, no end time).
	base func(*v1alpha1.ModelDeployment)
	run  func(w writer, md *v1alpha1.ModelDeployment) error
	// policies and msg: the refusal is by one of the policies (more than one when two apply to the case),
	// with msg in it. Empty policies means the case is accepted, and check says what it did.
	policies []string
	msg      string
	check    func(t *testing.T, got *v1alpha1.ModelDeployment)
}

func refusedMDCases() []guardCase {
	setEnd := func(h int) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.EndTime = inHours(h) }
	}
	capacity := []string{capacityGuard}
	const rest = "may change only spec.instances"
	return []guardCase{
		{name: "a lower count through the object", policies: capacity, msg: "cannot lower spec.instances",
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
			}},
		{name: "a lower count through a merge patch", policies: capacity, msg: "cannot lower spec.instances",
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.mergePatch(md, `{"spec":{"instances":1}}`)
			}},
		{name: "a lower count through server-side apply", policies: capacity, msg: "cannot lower spec.instances",
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.apply(md, map[string]any{"instances": 1})
			}},
		{name: "a lower count through the scale subresource", policies: capacity, msg: "cannot lower a ModelDeployment's count with scale",
			run: func(w writer, md *v1alpha1.ModelDeployment) error { return w.scaleTo(md, 1) }},
		{name: "a count of zero through the scale subresource", policies: capacity, msg: "cannot lower a ModelDeployment's count with scale",
			run: func(w writer, md *v1alpha1.ModelDeployment) error { return w.scaleTo(md, 0) }},
		{name: "an end time set where there was none", policies: capacity, msg: "set spec.endTime where there was none or move it earlier",
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) { m.Spec.EndTime = inHours(5) })
			}},
		{name: "an end time moved earlier", base: setEnd(8), policies: capacity, msg: "set spec.endTime where there was none or move it earlier",
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) { m.Spec.EndTime = inHours(4) })
			}},
		{name: "spec.source switched to managed", policies: capacity, msg: "edit spec.source.served only",
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) {
					m.Spec.Source = v1alpha1.ModelSource{Managed: &v1alpha1.ManagedCatalogEntry{Platform: "example-platform", CatalogName: "example-model"}}
				})
			}},
		{name: "spec.routing.maxRequestDuration raised", policies: capacity, msg: rest,
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) { m.Spec.Routing.MaxRequestDuration = ptr.To(v1alpha1.Duration("2h")) })
			}},
		{name: "spec.routing.maxRequestDuration removed", policies: capacity, msg: rest,
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) { m.Spec.Routing = nil })
			}},
		{name: "an owner reference added", policies: capacity, msg: "cannot change a ModelDeployment's ownerReferences",
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) { m.OwnerReferences = []metav1.OwnerReference{ownerRefToAConfigMap()} })
			}},
		{name: "a finalizer added", policies: capacity, msg: "cannot change a ModelDeployment's finalizers",
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) { m.Finalizers = []string{"example.invalid/hold"} })
			}},
		{name: "spec.retentionPolicy changed", policies: capacity, msg: rest,
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) {
					m.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: ptr.To(v1alpha1.Duration("1h"))}
				})
			}},
		{name: "spec.providers changed", policies: capacity, msg: rest,
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) { m.Spec.Providers.Names = []string{"helios-a", "helios-b"} })
			}},
		{name: "a label added", policies: capacity, msg: "cannot change a ModelDeployment's labels",
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) { m.Labels = map[string]string{"example": "label"} })
			}},
		{name: "another annotation added", policies: capacity, msg: "may edit only the purpose annotation",
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) { m.Annotations["example.invalid/other"] = "x" })
			}},
	}
}

func acceptedMDCases() []guardCase {
	setEnd := func(h int) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.EndTime = inHours(h) }
	}
	instances := func(n int32) func(t *testing.T, got *v1alpha1.ModelDeployment) {
		return func(t *testing.T, got *v1alpha1.ModelDeployment) {
			t.Helper()
			if got.Spec.Instances != n {
				t.Errorf("spec.instances = %d after the write, want %d: it was accepted and did nothing", got.Spec.Instances, n)
			}
		}
	}
	hedgeWidth := func(n int32) func(t *testing.T, got *v1alpha1.ModelDeployment) {
		return func(t *testing.T, got *v1alpha1.ModelDeployment) {
			t.Helper()
			if got.Spec.HedgeWidth == nil || *got.Spec.HedgeWidth != n {
				t.Errorf("spec.hedgeWidth = %v after the write, want %d: it was accepted and did nothing", got.Spec.HedgeWidth, n)
			}
		}
	}
	return []guardCase{
		{name: "a higher count through the object", check: instances(5),
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 5 })
			}},
		{name: "a higher count through a merge patch", check: instances(5),
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.mergePatch(md, `{"spec":{"instances":5}}`)
			}},
		{name: "a higher count through server-side apply", check: instances(5),
			run: func(w writer, md *v1alpha1.ModelDeployment) error { return w.apply(md, map[string]any{"instances": 5}) }},
		{name: "a higher count through the scale subresource", check: instances(5),
			run: func(w writer, md *v1alpha1.ModelDeployment) error { return w.scaleTo(md, 5) }},
		{name: "the same count through the scale subresource", check: instances(3),
			run: func(w writer, md *v1alpha1.ModelDeployment) error { return w.scaleTo(md, 3) }},
		{name: "a later end time", base: setEnd(4),
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) { m.Spec.EndTime = inHours(9) })
			},
			check: func(t *testing.T, got *v1alpha1.ModelDeployment) {
				if got.Spec.EndTime == nil || got.Spec.EndTime.Time.Before(inHours(8).Time) {
					t.Errorf("spec.endTime = %v after the write, want about 9 hours on", got.Spec.EndTime)
				}
			}},
		{name: "the removal of the end time", base: setEnd(4),
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) { m.Spec.EndTime = nil })
			},
			check: func(t *testing.T, got *v1alpha1.ModelDeployment) {
				if got.Spec.EndTime != nil {
					t.Errorf("spec.endTime = %v after its removal", got.Spec.EndTime)
				}
			}},
		{name: "an edit of spec.source.served",
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) {
					m.Spec.Source.Served.Flags = []v1alpha1.EngineFlag{{Name: "dtype", Value: "float16"}, {Name: "max-model-len", Value: "16384"}}
				})
			},
			check: func(t *testing.T, got *v1alpha1.ModelDeployment) {
				if got.Spec.Source.Served == nil || got.Spec.Source.Served.Flags[0].Value != "float16" {
					t.Errorf("the serving spec after the edit: %+v, want dtype float16", got.Spec.Source.Served)
				}
			}},
		{name: "an edit of the purpose annotation",
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) { m.Annotations[v1alpha1.AnnotationPurpose] = "another reason" })
			},
			check: func(t *testing.T, got *v1alpha1.ModelDeployment) {
				if got.Annotations[v1alpha1.AnnotationPurpose] != "another reason" {
					t.Errorf("the purpose is %q after the edit", got.Annotations[v1alpha1.AnnotationPurpose])
				}
			}},
		{name: "a change of kubectl's last-applied-configuration beside a raised count (Q4)", check: instances(4),
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) {
					m.Spec.Instances = 4
					m.Annotations["kubectl.kubernetes.io/last-applied-configuration"] = `{"spec":{"instances":4}}`
				})
			}},
		{name: "spec.hedgeWidth raised, 2 to 3 (HBEP-10, as amended for #606)", check: hedgeWidth(3),
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) { m.Spec.HedgeWidth = ptr.To[int32](3) })
			}},
		{name: "spec.hedgeWidth lowered, 2 to 1 (HBEP-10, as amended for #606)", check: hedgeWidth(1),
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(m *v1alpha1.ModelDeployment) { m.Spec.HedgeWidth = ptr.To[int32](1) })
			}},
		{name: "a no-op update", check: instances(3),
			run: func(w writer, md *v1alpha1.ModelDeployment) error {
				return w.update(md, func(*v1alpha1.ModelDeployment) {})
			}},
	}
}

// Every write that lowers capacity, or that the controllers would act on, is refused for an agent with
// the capacity guard's own message, and the stored object is untouched. The same write is accepted from the
// person (a dry run: the guard's match condition is the agents' group, so the person is outside it), which
// shows the write itself is valid and the refusal is the guard's.
func TestCapacityGuardRefusesAgentsWritesThatLowerCapacity(t *testing.T) {
	agents := newAgents(t, "agent-a", "agent-b")
	a := agents[0]
	requireEnforced(t)
	ns := newNamespace(t)
	person := personWriter(t)
	for _, tc := range refusedMDCases() {
		t.Run(tc.name, func(t *testing.T) {
			md := agentMD(a, ns, uniq("md"))
			if tc.base != nil {
				tc.base(md)
			}
			stored := createMD(t, a, md)
			requireAgentIdentity(t, a)
			err := tc.run(agentWriter(a), stored)
			if !anyDenied(err, tc.policies, tc.msg) {
				t.Errorf("an agent's write: %v, want a refusal by %v containing %q", err, tc.policies, tc.msg)
			}
			requireUntouched(t, stored, tc.name)
			requireAccepted(t, "the person's same write (a dry run)", tc.run(person, stored))
		})
	}
}

func anyDenied(err error, policies []string, msg string) bool {
	for _, p := range policies {
		if deniedBy(err, p, msg) {
			return true
		}
	}
	return false
}

// What an allowlisted edit does, for an agent: it is accepted, and takes effect.
func TestCapacityGuardAcceptsTheAllowlistedEdits(t *testing.T) {
	agents := newAgents(t, "agent-a", "agent-b")
	a := agents[0]
	requireEnforced(t)
	ns := newNamespace(t)
	for _, tc := range acceptedMDCases() {
		t.Run(tc.name, func(t *testing.T) {
			md := agentMD(a, ns, uniq("md"))
			if tc.base != nil {
				tc.base(md)
			}
			stored := createMD(t, a, md)
			requireAgentIdentity(t, a)
			requireAccepted(t, "an agent's write", tc.run(agentWriter(a), stored))
			if tc.check != nil {
				tc.check(t, storedMD(t, stored))
			}
		})
	}
}

// An agent creates a ModelDeployment with any fields, its own owner annotation and no owner references.
// It does so by create and by server-side apply, and what it made is stored.
func TestAnAgentMayCreateAModelDeploymentWithAnyFields(t *testing.T) {
	a := newAgents(t, "agent-a", "agent-b")[0]
	requireEnforced(t)
	ns := newNamespace(t)
	requireAgentIdentity(t, a)
	md := agentMD(a, ns, uniq("md"))
	md.Spec.WorkersPerInstance = ptr.To[int32](2)
	md.Spec.EndTime = inHours(3)
	md.Labels = map[string]string{"example": "label"}
	md.Finalizers = []string{"example.invalid/hold"}
	md.Annotations["example.invalid/other"] = "x"
	must(t, a.c.Create(bg(), md), "an agent's create with fields of every kind")
	got := storedMD(t, md)
	if got.Spec.WorkersPerInstance == nil || got.Labels["example"] != "label" || got.Annotations["example.invalid/other"] != "x" {
		t.Errorf("the stored deployment lost fields the agent created it with: %+v", got.ObjectMeta)
	}
	// Server-side apply that creates: the same rules, the CREATE operation.
	applied := agentMD(a, ns, uniq("md"))
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(applied)
	must(t, err, "to unstructured")
	u := &unstructured.Unstructured{Object: m}
	u.SetAPIVersion(v1alpha1.GroupVersion.String())
	u.SetKind("ModelDeployment")
	_, err = a.dyn.Resource(mdResource).Namespace(ns).Apply(bg(), applied.Name, u, metav1.ApplyOptions{FieldManager: "test-agent", Force: true})
	must(t, err, "an agent's server-side apply that creates a ModelDeployment")
	if got := storedMD(t, applied); got.Annotations[v1alpha1.AnnotationOwner] != a.username() {
		t.Errorf("the applied deployment has the owner %q, want %q", got.Annotations[v1alpha1.AnnotationOwner], a.username())
	}
}

// The create rules: an owner reference (Q3), and the owner annotation (Q6), for an agent; the person may
// do both (the person may leave the owner out, and is outside the capacity guard).
func TestCapacityGuardAndOwnerPinRefuseWhatAnAgentCreates(t *testing.T) {
	agents := newAgents(t, "agent-a", "agent-b")
	a, b := agents[0], agents[1]
	requireEnforced(t)
	ns := newNamespace(t)
	person := thePerson(t)
	for _, tc := range []struct {
		name   string
		edit   func(*v1alpha1.ModelDeployment)
		policy string
		msg    string
	}{
		{"an owner reference", func(m *v1alpha1.ModelDeployment) { m.OwnerReferences = []metav1.OwnerReference{ownerRefToAConfigMap()} },
			capacityGuard, "cannot create a ModelDeployment with ownerReferences"},
		{"no owner annotation", func(m *v1alpha1.ModelDeployment) { delete(m.Annotations, v1alpha1.AnnotationOwner) }, ownerPin, "carries the annotation"},
		{"no annotations at all", func(m *v1alpha1.ModelDeployment) { m.Annotations = nil }, ownerPin, "carries the annotation"},
		{"another identity's owner annotation", func(m *v1alpha1.ModelDeployment) { m.Annotations[v1alpha1.AnnotationOwner] = b.username() }, ownerPin, "carries the annotation"},
		{"the person as owner", func(m *v1alpha1.ModelDeployment) { m.Annotations[v1alpha1.AnnotationOwner] = person }, ownerPin, "carries the annotation"},
		{"an empty owner annotation", func(m *v1alpha1.ModelDeployment) { m.Annotations[v1alpha1.AnnotationOwner] = "" }, ownerPin, "carries the annotation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireAgentIdentity(t, a)
			md := agentMD(a, ns, uniq("md"))
			tc.edit(md)
			err := a.c.Create(bg(), md)
			requireDenied(t, "an agent's create", err, tc.policy, tc.msg)
			if gerr := c.Get(bg(), client.ObjectKeyFromObject(md), &v1alpha1.ModelDeployment{}); !apierrors.IsNotFound(gerr) {
				t.Errorf("after the refused create the ModelDeployment exists: %v", gerr)
			}
			// The person's same create passes both policies (a dry run).
			requireAccepted(t, "the person's same create (a dry run)", c.Create(bg(), md.DeepCopy(), client.DryRunAll))
		})
	}
}

// The owner annotation is fixed at create for every identity, the person included; a purpose is at most 256
// characters, for every identity. Absent stays absent: adding the annotation, and setting it empty, are
// changes too.
func TestOwnerPinFixesTheOwnerAndBoundsThePurposeForEveryone(t *testing.T) {
	agents := newAgents(t, "agent-a", "agent-b")
	a, b := agents[0], agents[1]
	requireEnforced(t)
	ns := newNamespace(t)
	person := personWriter(t)
	personName := thePerson(t)
	stored := createMD(t, a, agentMD(a, ns, uniq("md")))
	const fixed = "is fixed at create"
	// The person adds nothing, changes nothing, removes nothing.
	for _, tc := range []struct {
		name string
		edit func(*v1alpha1.ModelDeployment)
	}{
		{"changed to another identity", func(m *v1alpha1.ModelDeployment) { m.Annotations[v1alpha1.AnnotationOwner] = b.username() }},
		{"emptied", func(m *v1alpha1.ModelDeployment) { m.Annotations[v1alpha1.AnnotationOwner] = "" }},
		{"removed", func(m *v1alpha1.ModelDeployment) { delete(m.Annotations, v1alpha1.AnnotationOwner) }},
	} {
		requireDenied(t, "the person's owner annotation "+tc.name, person.update(stored, tc.edit), ownerPin, fixed)
	}
	// The agent's change of the owner annotation is refused by the owner pin and by the capacity guard,
	// whichever the API server reports first.
	requireAgentIdentity(t, a)
	err := agentWriter(a).update(stored, func(m *v1alpha1.ModelDeployment) { m.Annotations[v1alpha1.AnnotationOwner] = b.username() })
	if !anyDenied(err, []string{ownerPin, capacityGuard}, "") {
		t.Errorf("an agent's change of the owner annotation: %v, want a refusal by the owner pin or the capacity guard", err)
	}
	requireUntouched(t, stored, "the owner annotation")

	// Absent stays absent: a deployment the person made without an owner gets none from anyone.
	bare := newModelDeployment(ns, uniq("md-bare"))
	must(t, c.Create(bg(), bare), "the person's create without an owner")
	for _, owner := range []string{personName, "", b.username()} {
		requireDenied(t, fmt.Sprintf("the person's addition of the owner annotation %q to a deployment that has none", owner),
			person.update(bare, func(m *v1alpha1.ModelDeployment) {
				m.Annotations = map[string]string{v1alpha1.AnnotationOwner: owner}
			}), ownerPin, fixed)
	}
	// The purpose: 256 characters pass, 257 do not, from the person and from an agent, on update and on create.
	long := strings.Repeat("p", v1alpha1.PurposeMaxLength+1)
	exact := strings.Repeat("p", v1alpha1.PurposeMaxLength)
	const tooLong = "at most 256 characters"
	setPurpose := func(p string) func(*v1alpha1.ModelDeployment) {
		return func(m *v1alpha1.ModelDeployment) {
			if m.Annotations == nil {
				m.Annotations = map[string]string{}
			}
			m.Annotations[v1alpha1.AnnotationPurpose] = p
		}
	}
	requireDenied(t, "the person's 257-character purpose", person.update(stored, setPurpose(long)), ownerPin, tooLong)
	requireAccepted(t, "the person's 256-character purpose", person.update(stored, setPurpose(exact)))
	requireDenied(t, "an agent's 257-character purpose", agentWriter(a).update(stored, setPurpose(long)), ownerPin, tooLong)
	requireAccepted(t, "an agent's 256-character purpose", agentWriter(a).update(stored, setPurpose(exact)))
	tooLongMD := agentMD(a, ns, uniq("md"))
	tooLongMD.Annotations[v1alpha1.AnnotationPurpose] = long
	requireDenied(t, "an agent's create with a 257-character purpose", a.c.Create(bg(), tooLongMD), ownerPin, tooLong)
	requireDenied(t, "the person's create with a 257-character purpose", c.Create(bg(), tooLongMD.DeepCopy(), client.DryRunAll), ownerPin, tooLong)
}
