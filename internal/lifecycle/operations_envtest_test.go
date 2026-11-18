package lifecycle

// Operations against a real API server, with two agent identities and the person (#589, PR 4, ACs 6 and 7).
// It starts internal/testenv's control plane on #61's block, 29204-29207, so run it under envtest.lock. The
// three admission policies and the agents' wide role are applied from the embedded manifests of config/,
// as up applies them, and the test waits until each policy refuses its dry-run probe before the first case
// (the activation window, plan section 1). Agent A and agent B hold ServiceAccount tokens of the agents'
// namespace, from TokenRequest; the person is the control plane's admin. Every judgement of admission is the
// API server's: a refusal must name the policy (a bare Forbidden is RBAC's too). The pass runs the
// Operations step over a hand-made state of the ModelDeployment, as the fake-client tests do, and writes the
// Operation's status through the API server, so the status subresource, the resourceVersion and the
// owner pin's reading of the written plan are real.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/discovery"
	discmem "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	cfgadmission "github.com/groundnuty/plgrid-llmlab-hpc-burst/config/admission"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/config/rbac"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

// Written out: the test states what the issue says, never reading the launcher's constants.
const (
	envAgentsNamespace = "hpcburst-agents"
	envAgentsGroup     = "system:serviceaccounts:hpcburst-agents"
	envOwnerPin        = "ValidatingAdmissionPolicy 'hpcburst-owner-pin'"
	envApproverPin     = "ValidatingAdmissionPolicy 'hpcburst-approver-pin'"
)

// envIdentity is one identity of the test: its name and groups as the API server reports them, and a
// client that acts as it.
type envIdentity struct {
	user   string
	groups []string
	cfg    *rest.Config
	c      client.Client
}

// opsEnv is the control plane and its three identities.
type opsEnv struct {
	ctx    context.Context
	admin  *envIdentity // the person
	a, b   *envIdentity // agents A and B
	person v1alpha1.Approver
}

func envClient(t *testing.T, cfg *rest.Config) client.Client {
	t.Helper()
	s, err := Scheme()
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// whoAmI asks the API server who the configuration's credential is (SelfSubjectReview).
func whoAmI(ctx context.Context, t *testing.T, cfg *rest.Config) *envIdentity {
	t.Helper()
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r, err := cs.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("SelfSubjectReview: %v", err)
	}
	return &envIdentity{user: r.Status.UserInfo.Username, groups: r.Status.UserInfo.Groups, cfg: cfg, c: envClient(t, cfg)}
}

// createManifests creates every object of the multi-document YAML through a dynamic client over discovery.
func createManifests(ctx context.Context, t *testing.T, cfg *rest.Config, manifests []byte) {
	t.Helper()
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(discmem.NewMemCacheClient(dc))
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	dec := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(manifests), 4096)
	for {
		u := &unstructured.Unstructured{}
		if err := dec.Decode(&u.Object); err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			t.Fatal(err)
		}
		if len(u.Object) == 0 {
			continue
		}
		m, err := mapper.RESTMapping(u.GroupVersionKind().GroupKind(), u.GroupVersionKind().Version)
		if err != nil {
			t.Fatalf("map %s: %v", u.GroupVersionKind(), err)
		}
		var ri dynamic.ResourceInterface = dyn.Resource(m.Resource)
		if m.Scope.Name() == meta.RESTScopeNameNamespace {
			ri = dyn.Resource(m.Resource).Namespace(u.GetNamespace())
		}
		if _, err := ri.Create(ctx, u, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatalf("create %s %s: %v", u.GetKind(), u.GetName(), err)
		}
	}
}

// agentIdentity makes a ServiceAccount of the agents' namespace and a client with its TokenRequest token,
// and checks by SelfSubjectReview that the client is that account in the agents' group, so that no case of
// the test passes by running as someone outside the policies.
func agentIdentity(ctx context.Context, t *testing.T, admin *envIdentity, name string) *envIdentity {
	t.Helper()
	cs, err := kubernetes.NewForConfig(admin.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.CoreV1().ServiceAccounts(envAgentsNamespace).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	secs := int64(3600)
	tok, err := cs.CoreV1().ServiceAccounts(envAgentsNamespace).CreateToken(ctx, name, &authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: &secs}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("TokenRequest for %s: %v", name, err)
	}
	cfg := rest.AnonymousClientConfig(admin.cfg)
	cfg.BearerToken = tok.Status.Token
	cfg.QPS, cfg.Burst = 200, 400
	id := whoAmI(ctx, t, cfg)
	if want := "system:serviceaccount:" + envAgentsNamespace + ":" + name; id.user != want {
		t.Fatalf("the client of %s is %q, want %q", name, id.user, want)
	}
	in := false
	for _, g := range id.groups {
		in = in || g == envAgentsGroup
	}
	if !in {
		t.Fatalf("the client of %s is not in the group %s (groups %v): the policies do not apply to it", name, envAgentsGroup, id.groups)
	}
	return id
}

// refusedBy reports whether err is a refusal that names the policy.
func refusedBy(err error, policy string) bool {
	return err != nil && apierrors.IsForbidden(err) && strings.Contains(err.Error(), policy)
}

// waitEnforced waits until the API server refuses the policy's dry-run probe with the policy's own message.
func waitEnforced(ctx context.Context, t *testing.T, policy string, probe func() error) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		if last = probe(); refusedBy(last, policy) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s was applied but did not refuse its dry-run probe in 60 s; the last answer: %v", policy, last)
}

func dryRun(ctx context.Context, c client.Client, obj client.Object) error {
	return c.Create(ctx, obj, client.DryRunAll)
}

// newOpsEnv starts the control plane and opens the agents' access, as up does: the wide role, the three
// policies, and the wait until each refuses its probe.
func newOpsEnv(t *testing.T) *opsEnv {
	t.Helper()
	t.Setenv("HPCBURST_TESTENV_PORT_BASE", managerPortBase)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	cp, err := testenv.Start(ctx, t.TempDir(), testenv.Options{BeforeControllerManager: installTestCRDs})
	if err != nil {
		t.Fatalf("start the control plane: %v", err)
	}
	t.Cleanup(func() {
		if err := cp.Stop(); err != nil {
			t.Errorf("stop the control plane: %v", err)
		}
		if err := cp.AssertAllDead(); err != nil {
			t.Errorf("leftover process: %v", err)
		}
	})
	admin := whoAmI(ctx, t, cp.Config)
	if err := admin.c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: rigNS}}); err != nil {
		t.Fatal(err)
	}
	createManifests(ctx, t, cp.Config, rbac.Agents)
	for _, m := range [][]byte{cfgadmission.CapacityGuard, cfgadmission.OwnerPin, cfgadmission.ApproverPin} {
		createManifests(ctx, t, cp.Config, m)
	}
	e := &opsEnv{ctx: ctx, admin: admin, person: v1alpha1.Approver{User: admin.user}}
	e.a = agentIdentity(ctx, t, admin, "agent-a")
	e.b = agentIdentity(ctx, t, admin, "agent-b")

	// The activation window: each policy is enforced once it refuses its probe, and only then does a case start.
	waitEnforced(ctx, t, "ValidatingAdmissionPolicy 'hpcburst-owner-pin'", func() error {
		return dryRun(ctx, admin.c, &v1alpha1.Operation{
			ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: rigNS},
			Spec: v1alpha1.OperationSpec{Action: v1alpha1.OperationActionScale, ModelDeploymentRef: v1alpha1.ObjectReference{Name: opMD},
				Scale: &v1alpha1.ScaleParameters{Instances: 0}, Owner: "somebody-else"},
		})
	})
	waitEnforced(ctx, t, envApproverPin, func() error {
		return dryRun(ctx, admin.c, &v1alpha1.Approval{
			ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: rigNS},
			Spec: v1alpha1.ApprovalSpec{Operation: v1alpha1.OperationReference{Name: "probe", UID: "uid-probe"}, PlanID: "0123456789abcdef",
				Approver: v1alpha1.ApproverIdentity{Username: "somebody-else"}},
		})
	})
	waitEnforced(ctx, t, "ValidatingAdmissionPolicy 'hpcburst-capacity-guard'", func() error {
		// An agent's create of a ModelDeployment that carries an ownerReference: the guard refuses it.
		md := mdSpec("probe")
		md.Generation = 0
		md.Annotations = map[string]string{v1alpha1.AnnotationOwner: e.a.user}
		md.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: "probe", UID: "00000000-0000-0000-0000-000000000000"}}
		return dryRun(ctx, e.a.c, md)
	})
	// RBAC reaches the API server's authorizer a moment after the binding: wait until the agents may create.
	for _, a := range []*envIdentity{e.a, e.b} {
		cs, _ := kubernetes.NewForConfig(a.cfg)
		deadline := time.Now().Add(30 * time.Second)
		for {
			r, err := cs.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, &authorizationv1.SelfSubjectAccessReview{Spec: authorizationv1.SelfSubjectAccessReviewSpec{
				ResourceAttributes: &authorizationv1.ResourceAttributes{Namespace: rigNS, Verb: "create", Group: v1alpha1.Group, Resource: "operations"}}}, metav1.CreateOptions{})
			if err == nil && r.Status.Allowed {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s may not create Operations after 30 s: %v %+v", a.user, err, r)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return e
}

// request creates an Operation as the identity, its owner the identity itself, as the owner pin requires.
func (e *opsEnv) request(t *testing.T, as *envIdentity, name string, op *v1alpha1.Operation) *v1alpha1.Operation {
	t.Helper()
	op.Name, op.Namespace, op.Spec.Owner = name, rigNS, as.user
	if err := as.c.Create(e.ctx, op); err != nil {
		t.Fatalf("%s creates Operation %s: %v", as.user, name, err)
	}
	return op
}

func (e *opsEnv) get(t *testing.T, name string) *v1alpha1.Operation {
	t.Helper()
	var op v1alpha1.Operation
	if err := e.admin.c.Get(e.ctx, types.NamespacedName{Namespace: rigNS, Name: name}, &op); err != nil {
		t.Fatal(err)
	}
	return &op
}

// step runs the Operations step as the controller, which is the admin here, over the fixture's state.
func (e *opsEnv) step(t *testing.T, f *opFixture, now time.Time) {
	t.Helper()
	p := f.pass()
	p.key.Namespace = rigNS // the Operations of this test live in the rig's namespace
	p.r, p.ctx, p.now = &Reconciler{Client: e.admin.c, Reader: e.admin.c, Clock: SystemClock{}, Backend: &Backend{}, Recorder: &MemoryRecorder{}}, e.ctx, now
	p.mem = p.r.memory()
	if err := p.operations(); err != nil {
		t.Fatalf("the Operations step: %v", err)
	}
}

// stepErr is step for a pass whose Operations step is expected to fail. This test's ModelDeployment is a hand-made state
// the control plane does not hold, so an Operation that reaches its commit commits (the status write is real) and then
// fails at the spec write; the execution itself is the rig's (opexec_rig_test.go).
func (e *opsEnv) stepErr(t *testing.T, f *opFixture, now time.Time) error {
	t.Helper()
	_, err := e.stepRec(t, f, now)
	return err
}

// stepRec is stepErr that also returns the events the step raised.
func (e *opsEnv) stepRec(t *testing.T, f *opFixture, now time.Time) (*MemoryRecorder, error) {
	t.Helper()
	p := f.pass()
	p.key.Namespace = rigNS
	rec := &MemoryRecorder{}
	p.r, p.ctx, p.now = &Reconciler{Client: e.admin.c, Reader: e.admin.c, Clock: SystemClock{}, Backend: &Backend{}, Recorder: rec}, e.ctx, now
	p.mem = p.r.memory()
	return rec, p.operations()
}

// committedTo asserts that every approval is in and the Operation was committed to its whole plan: the approvals are
// counted by the API server's objects, and the commit's status write is the control plane's own. It then finishes the
// Operation as the executor would, with a status write by the admin: this test's deployment is a hand-made state that the
// control plane does not hold, so the Operation's spec write fails on every pass, and an Operation left Executing would
// fail the steps of the cases after it. The execution itself is the rig's (opexec_rig_test.go).
func (e *opsEnv) committedTo(t *testing.T, name string, err error) {
	t.Helper()
	op := e.get(t, name)
	if op.Status.Phase != v1alpha1.OperationPhaseExecuting || op.Status.Plan == nil || len(op.Status.Committed) != len(op.Status.Plan.Targets) || len(op.Status.Committed) == 0 {
		t.Errorf("every approval in: phase %q, %d committed of %v planned, want Executing with the whole plan committed", op.Status.Phase, len(op.Status.Committed), op.Status.Plan)
	}
	if err == nil || !strings.Contains(err.Error(), "set spec.instances to 1") {
		t.Errorf("the pass failed with %v, want the spec write of a deployment this control plane does not hold", err)
	}
	now := metav1.NewTime(time.Now())
	op.Status.Phase, op.Status.FinishedAt = v1alpha1.OperationPhaseDone, &now
	if uerr := e.admin.c.Status().Update(e.ctx, op); uerr != nil {
		t.Fatalf("finish %s: %v", name, uerr)
	}
}

func (e *opsEnv) approval(t *testing.T, as *envIdentity, name string, op *v1alpha1.Operation, planID string, approver *envIdentity) error {
	t.Helper()
	return as.c.Create(e.ctx, &v1alpha1.Approval{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: rigNS},
		Spec: v1alpha1.ApprovalSpec{Operation: v1alpha1.OperationReference{Name: op.Name, UID: op.UID}, PlanID: planID,
			Approver: v1alpha1.ApproverIdentity{Username: approver.user, Groups: approver.groups}},
	})
}

func (e *opsEnv) setPolicies(t *testing.T, ps ...v1alpha1.ApprovalPolicy) {
	t.Helper()
	var old v1alpha1.ApprovalPolicyList
	if err := e.admin.c.List(e.ctx, &old); err != nil {
		t.Fatal(err)
	}
	for i := range old.Items {
		if err := e.admin.c.Delete(e.ctx, &old.Items[i]); err != nil {
			t.Fatal(err)
		}
	}
	for i := range ps {
		ps[i].UID, ps[i].Generation, ps[i].ResourceVersion = "", 0, ""
		if err := e.admin.c.Create(e.ctx, &ps[i]); err != nil {
			t.Fatalf("create ApprovalPolicy %s: %v", ps[i].Name, err)
		}
	}
}

// opsFixture is a deployment of three Instances: r serves on sano, p waits on sano, p2 waits on helios.
// Scale(1) cancels p and p2, whose Allocations are pending: both need approval.
func opsFixture(t *testing.T) *opFixture {
	f := newOpFixture(t, 3)
	f.md.Namespace = rigNS
	f.addProvider("helios")
	f.addInstance(instOpts{name: "r", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd)})
	f.addInstance(instOpts{name: "p", phase: v1alpha1.InstancePending, created: time.Minute, submit: tp(opT0.Add(10 * time.Minute))})
	f.addInstance(instOpts{name: "p2", phase: v1alpha1.InstancePending, created: 2 * time.Minute, provider: "helios", submit: tp(opT0.Add(15 * time.Minute))})
	return f
}

func globalFor(a ...v1alpha1.Approver) v1alpha1.ApprovalPolicy { return globalPolicy(a...) }

func TestOperationsOnAControlPlaneWithTwoAgents(t *testing.T) {
	e := newOpsEnv(t)
	agentA := v1alpha1.Approver{ServiceAccount: &v1alpha1.ServiceAccountRef{Namespace: envAgentsNamespace, Name: "agent-a"}}

	t.Run("the confirm: a conflict after a re-plan, a retry that keeps the ID, another agent refused, never confirmed, the backstop", func(t *testing.T) {
		e.setPolicies(t, globalFor(e.person))
		f := opsFixture(t)
		e.request(t, e.a, "scale-1", scaleOp(1))
		e.step(t, f, opAt)
		planned := e.get(t, "scale-1")
		if planned.Status.Phase != v1alpha1.OperationPhasePlanned || planned.Status.Plan == nil {
			t.Fatalf("phase %q: want Planned with a plan", planned.Status.Phase)
		}
		id := planned.Status.Plan.ID

		// A reads the Operation with its plan. The controller then writes the status again for a new queue wait,
		// which moves the resourceVersion and keeps the ID.
		readByA := &v1alpha1.Operation{}
		if err := e.a.c.Get(e.ctx, client.ObjectKeyFromObject(planned), readByA); err != nil {
			t.Fatal(err)
		}
		f.pvs["sano"].prov.Status.QueueWaits = waitsOf(6, 9)
		e.step(t, f, opAt.Add(time.Minute))
		replanned := e.get(t, "scale-1")
		if replanned.ResourceVersion == readByA.ResourceVersion || replanned.Status.Plan.ID != id {
			t.Fatalf("the re-plan moved the resourceVersion %s -> %s with ID %s -> %s; it should move the first and keep the second", readByA.ResourceVersion, replanned.ResourceVersion, id, replanned.Status.Plan.ID)
		}
		// A's confirm with the resourceVersion it read before the re-plan is a Conflict.
		readByA.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: id}
		if err := e.a.c.Update(e.ctx, readByA); !apierrors.IsConflict(err) {
			t.Fatalf("A's confirm with a stale resourceVersion: %v, want a Conflict", err)
		}
		// After a write that kept the ID, A reads again and confirms: accepted.
		fresh := &v1alpha1.Operation{}
		if err := e.a.c.Get(e.ctx, client.ObjectKeyFromObject(planned), fresh); err != nil {
			t.Fatal(err)
		}
		fresh.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: fresh.Status.Plan.ID}
		if err := e.a.c.Update(e.ctx, fresh); err != nil {
			t.Fatalf("A's confirm after re-reading: %v", err)
		}
		e.step(t, f, opAt.Add(2*time.Minute))
		if got := e.get(t, "scale-1"); got.Status.Phase != v1alpha1.OperationPhaseAwaitingApproval {
			t.Errorf("a confirmed Operation with approvals to get is %q, want AwaitingApproval", got.Status.Phase)
		}

		// B's confirm of A's Operation is refused by the owner pin, and by nothing else.
		e.request(t, e.a, "scale-2", scaleOp(1))
		e.step(t, f, opAt.Add(3*time.Minute))
		other := &v1alpha1.Operation{}
		if err := e.b.c.Get(e.ctx, types.NamespacedName{Namespace: rigNS, Name: "scale-2"}, other); err != nil {
			t.Fatal(err)
		}
		other.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: other.Status.Plan.ID}
		if err := e.b.c.Update(e.ctx, other); !refusedBy(err, envOwnerPin) {
			t.Fatalf("B's confirm of A's Operation: %v, want a refusal by the owner pin", err)
		}
		// An Operation that is never confirmed never leaves Planned, whatever else is written beside it.
		if err := e.approval(t, e.admin, "ap-unconfirmed", other, other.Status.Plan.ID, e.admin); err != nil {
			t.Fatal(err)
		}
		for i := 4; i < 9; i++ {
			e.step(t, f, opAt.Add(time.Duration(i)*time.Minute))
		}
		if got := e.get(t, "scale-2"); got.Status.Phase != v1alpha1.OperationPhasePlanned || got.Spec.Confirm != nil {
			t.Errorf("never confirmed: phase %q, confirm %v, want Planned and none", got.Status.Phase, got.Spec.Confirm)
		}

		// The backstop. The admin writes a status carrying a plan ID the controller never wrote, A confirms that ID
		// (the owner pin sees a written plan and the same ID), and the controller refuses it.
		op3 := e.request(t, e.a, "scale-3", scaleOp(1))
		forged := e.get(t, "scale-3")
		forged.Status.Phase = v1alpha1.OperationPhasePlanned
		forged.Status.Plan = &v1alpha1.OperationPlan{
			ID: "0123456789abcdef", ObservedAt: metav1.NewTime(opT0), ModelDeployment: v1alpha1.PlannedDeployment{Name: opMD, UID: "uid-md", Generation: 3},
			Targets: []v1alpha1.PlanTarget{{Kind: tInst, Name: "p", UID: "uid-p", Effect: v1alpha1.PlanEffectCancelPending}},
		}
		if err := e.admin.c.Status().Update(e.ctx, forged); err != nil {
			t.Fatal(err)
		}
		toConfirm := &v1alpha1.Operation{}
		if err := e.a.c.Get(e.ctx, client.ObjectKeyFromObject(op3), toConfirm); err != nil {
			t.Fatal(err)
		}
		toConfirm.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: "0123456789abcdef"}
		if err := e.a.c.Update(e.ctx, toConfirm); err != nil {
			t.Fatalf("A's confirm of the forged plan's ID: %v (admission sees a written plan and the same ID)", err)
		}
		e.step(t, f, opAt.Add(10*time.Minute))
		got := e.get(t, "scale-3")
		if got.Status.Phase != v1alpha1.OperationPhaseRefused || !strings.Contains(condOf(got, v1alpha1.ConditionExecutable).Message, "confirmed before a plan was written, or with an ID this controller never wrote") {
			t.Errorf("the backstop: phase %q, Executable %+v, want Refused", got.Status.Phase, condOf(got, v1alpha1.ConditionExecutable))
		}
	})

	t.Run("approvals: two identities and the person, A's own under a Provider policy only on that Provider", func(t *testing.T) {
		// Global names the person; the Provider policy of sano names agent A. Targets on sano are under A's policy
		// alone; the targets on helios are under global.
		e.setPolicies(t, globalFor(e.person), providerPolicy("sano", agentA))
		f := opsFixture(t)
		op := e.request(t, e.a, "scale-4", scaleOp(1))
		e.step(t, f, opAt)
		id := e.get(t, "scale-4").Status.Plan.ID
		e.confirmAs(t, e.a, "scale-4", id)
		step := func(min int) *v1alpha1.Operation {
			e.step(t, f, opAt.Add(time.Duration(min)*time.Minute))
			return e.get(t, "scale-4")
		}
		op = step(1)
		for _, want := range []string{"Allocation alloc-p: needs an approval under provider.sano", "Instance p2: needs an approval under global"} {
			if !strings.Contains(condOf(op, v1alpha1.ConditionExecutable).Message, want) {
				t.Fatalf("Executable %q lacks %q", condOf(op, v1alpha1.ConditionExecutable).Message, want)
			}
		}
		// B is outside every policy: its Approval is admitted (it is B's own identity) and counts for nothing.
		if err := e.approval(t, e.b, "ap-b", op, id, e.b); err != nil {
			t.Fatalf("B's own Approval: %v", err)
		}
		if msg := condOf(step(2), v1alpha1.ConditionExecutable).Message; !strings.Contains(msg, "Instance p: needs an approval under provider.sano") {
			t.Errorf("B's Approval counted: %q", msg)
		}
		// A's Approval naming the person is refused at admission, by the approver pin.
		if err := e.approval(t, e.a, "ap-a-as-person", op, id, e.admin); !refusedBy(err, envApproverPin) {
			t.Fatalf("A's Approval naming the person: %v, want a refusal by the approver pin", err)
		}
		// A's own Approval counts under the Provider policy that names A: on sano, and not on helios.
		if err := e.approval(t, e.a, "ap-a", op, id, e.a); err != nil {
			t.Fatalf("A's own Approval: %v", err)
		}
		msg := condOf(step(3), v1alpha1.ConditionExecutable).Message
		if strings.Contains(msg, "provider.sano") || !strings.Contains(msg, "Instance p2: needs an approval under global") || !strings.Contains(msg, "Allocation alloc-p2: needs an approval under global") {
			t.Errorf("after A's Approval the helios targets alone should wait, under global: %q", msg)
		}
		// The person's Approval counts under global: every target is approved.
		if err := e.approval(t, e.admin, "ap-person", op, id, e.admin); err != nil {
			t.Fatalf("the person's Approval: %v", err)
		}
		err := e.stepErr(t, f, opAt.Add(4*time.Minute))
		e.committedTo(t, "scale-4", err)
	})

	t.Run("a plan that needs the person's approval alone is not approved by an agent", func(t *testing.T) {
		e.setPolicies(t, globalFor(e.person))
		f := opsFixture(t)
		op := e.request(t, e.a, "scale-5", scaleOp(1))
		e.step(t, f, opAt)
		id := e.get(t, "scale-5").Status.Plan.ID
		e.confirmAs(t, e.a, "scale-5", id)
		e.step(t, f, opAt.Add(time.Minute))
		if err := e.approval(t, e.a, "ap5-a", op, id, e.a); err != nil {
			t.Fatal(err)
		}
		if err := e.approval(t, e.b, "ap5-b", op, id, e.b); err != nil {
			t.Fatal(err)
		}
		e.step(t, f, opAt.Add(2*time.Minute))
		if got := e.get(t, "scale-5"); condOf(got, v1alpha1.ConditionExecutable).Reason != v1alpha1.ReasonAwaitingApproval {
			t.Fatalf("A's and B's Approvals satisfied a global policy that names the person: %+v", condOf(got, v1alpha1.ConditionExecutable))
		}
		if err := e.approval(t, e.admin, "ap5-person", op, id, e.admin); err != nil {
			t.Fatal(err)
		}
		err := e.stepErr(t, f, opAt.Add(3*time.Minute))
		e.committedTo(t, "scale-5", err)
	})

	// Stale plans (HBEP-10, Approval and execution): between the approval and the execution, each change the plan covers turns
	// the Operation Stale: nothing is committed, a PlanStale event goes out, and an approval added afterwards changes
	// nothing. The policy edit is the API server's own: it moves the policy's generation, which the plan's ID covers.
	for _, tc := range []struct {
		name   string
		slug   string
		change func(t *testing.T, f *opFixture)
	}{
		{"the pending Allocation started", "started", func(t *testing.T, f *opFixture) {
			a := f.allocs["alloc-p"]
			a.Status.AtProvider.Phase, a.Status.AtProvider.StartTime, a.Status.AtProvider.HardEnd = v1alpha1.AllocationRunning, opTime(opT0), opTime(opHardEnd)
		}},
		{"another Instance became ready", "ready", func(t *testing.T, f *opFixture) {
			f.insts[1].Status.Phase = v1alpha1.InstanceReady // p, which the Scale was to cancel
		}},
		{"the policy that applies was edited", "policy", func(t *testing.T, f *opFixture) {
			var pol v1alpha1.ApprovalPolicy
			if err := e.admin.c.Get(e.ctx, types.NamespacedName{Name: "global"}, &pol); err != nil {
				t.Fatal(err)
			}
			gen := pol.Generation
			pol.Spec.Approvers = append(pol.Spec.Approvers, v1alpha1.Approver{Group: "operators"}) // the person stays an approver
			if err := e.admin.c.Update(e.ctx, &pol); err != nil {
				t.Fatal(err)
			}
			if pol.Generation <= gen {
				t.Fatalf("the API server left the policy's generation at %d: the edit does not move what the plan covers", pol.Generation)
			}
		}},
	} {
		t.Run("stale: "+tc.name, func(t *testing.T) {
			e.setPolicies(t, globalFor(e.person))
			f := opsFixture(t)
			name := "stale-" + tc.slug
			op := e.request(t, e.a, name, scaleOp(1))
			e.step(t, f, opAt)
			id := e.get(t, name).Status.Plan.ID
			e.confirmAs(t, e.a, name, id)
			e.step(t, f, opAt.Add(time.Minute))
			if err := e.approval(t, e.admin, "ap-"+name, op, id, e.admin); err != nil {
				t.Fatal(err)
			}
			tc.change(t, f)
			rec, err := e.stepRec(t, f, opAt.Add(2*time.Minute))
			if err != nil {
				t.Fatalf("the step: %v", err)
			}
			got := e.get(t, name)
			if got.Status.Phase != v1alpha1.OperationPhaseStale || len(got.Status.Committed) != 0 || got.Status.FinishedAt == nil {
				t.Fatalf("phase %q, %d committed, finishedAt %v: want Stale, nothing committed, finished", got.Status.Phase, len(got.Status.Committed), got.Status.FinishedAt)
			}
			if c := condOf(got, v1alpha1.ConditionExecutable); c == nil || c.Reason != v1alpha1.ReasonPlanStale {
				t.Errorf("Executable %+v, want PlanStale", c)
			}
			stale := 0
			for _, ev := range rec.Events() {
				if ev.Code == v1alpha1.ReasonPlanStale && ev.Name == name {
					stale++
				}
			}
			if stale != 1 {
				t.Errorf("%d PlanStale events, want one", stale)
			}
			// An approval added afterwards changes nothing.
			rv := got.ResourceVersion
			if err := e.approval(t, e.admin, "ap-late-"+name, op, id, e.admin); err != nil {
				t.Fatal(err)
			}
			if err := e.stepErr(t, f, opAt.Add(3*time.Minute)); err != nil {
				t.Fatalf("the step after a late approval: %v", err)
			}
			if later := e.get(t, name); later.Status.Phase != v1alpha1.OperationPhaseStale || later.ResourceVersion != rv {
				t.Errorf("after a late approval: phase %q, resourceVersion %s -> %s: a Stale Operation is never written again", later.Status.Phase, rv, later.ResourceVersion)
			}
		})
	}
}

// confirmAs confirms an Operation as the identity, from a fresh read.
func (e *opsEnv) confirmAs(t *testing.T, as *envIdentity, name, planID string) {
	t.Helper()
	op := &v1alpha1.Operation{}
	if err := as.c.Get(e.ctx, types.NamespacedName{Namespace: rigNS, Name: name}, op); err != nil {
		t.Fatal(err)
	}
	op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: planID}
	if err := as.c.Update(e.ctx, op); err != nil {
		t.Fatalf("%s confirms %s: %v", as.user, name, err)
	}
}
