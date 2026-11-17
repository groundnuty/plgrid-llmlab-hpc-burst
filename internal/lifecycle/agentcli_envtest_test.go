package lifecycle

// The Operation and approval commands (hpcburst scale, end, release, confirm, approvals, approve and operation;
// internal/agentcli; #659) against a real API server, as two agent identities and the person. It starts ONE control plane on
// #61's block, 29204-29207, so run it under envtest.lock; every row is a subtest of it, so the whole file costs one start. The
// world, the stepper and the helpers are agentcli_world_test.go's, and the identities, the policies from config/ and the
// activation window are opsEnv's.
//
// Rows 1 to 8 run as agents A and B, never as the admin, which is outside the capacity guard and the pins and so proves nothing
// about them. Every expected value is written by hand from HBEP-7 and HBEP-10, never from the planner: a plan that needs approval
// for a cancelled pending Allocation (HBEP-10, What needs approval, row 1) under the one Provider policy that applies, which names
// agent A and not agent B. The rows run in the order below, since the first of them to change the world is the last.

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agentcli"
)

// planned waits until the stepper has written the Operation's plan and returns it.
func (w *cliWorld) planned(t *testing.T, name string) *v1alpha1.Operation {
	t.Helper()
	return w.operationIs(t, name, "has a plan", func(o *v1alpha1.Operation) bool { return o.Status.Plan != nil })
}

// operationNames are the names of the Operations of the namespace, read as the person.
func (w *cliWorld) operationNames(t *testing.T) []string {
	t.Helper()
	var l v1alpha1.OperationList
	if err := w.e.admin.c.List(w.e.ctx, &l, client.InNamespace(rigNS)); err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, o := range l.Items {
		out = append(out, o.Name)
	}
	return out
}

func (w *cliWorld) approvalCount(t *testing.T) int {
	t.Helper()
	var l v1alpha1.ApprovalList
	if err := w.e.admin.c.List(w.e.ctx, &l, client.InNamespace(rigNS)); err != nil {
		t.Fatal(err)
	}
	return len(l.Items)
}

func sameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestAgentCLIOnAControlPlane(t *testing.T) {
	e := newOpsEnv(t)
	agentA := v1alpha1.Approver{ServiceAccount: &v1alpha1.ServiceAccountRef{Namespace: envAgentsNamespace, Name: "agent-a"}}
	w := &cliWorld{e: e}
	w.storeWorld(t, agentA)
	w.start(t)
	dir := t.TempDir()
	kcA, kcB := cliKubeconfig(t, dir, "agent-a", e.a.cfg, rigNS), cliKubeconfig(t, dir, "agent-b", e.b.cfg, rigNS)
	asA, asB := agentcli.Env{Kubeconfig: kcA}, agentcli.Env{Kubeconfig: kcB}

	t.Run("row 5: scale without a terminal prints the plan and the confirm line, and confirms nothing", func(t *testing.T) {
		before := w.worldVersions(t)
		r := w.cli(t, asA, false, "y\n", "scale", opMD, "--instances", "0", "--key", "row5")
		if r.code != 0 {
			t.Fatalf("exit %d, stderr %q", r.code, r.errs)
		}
		op := w.planned(t, "row5")
		cliHas(t, "stdout", r.out, "plan "+op.Status.Plan.ID)
		if !regexp.MustCompile(`(?m)^confirm with: hpcburst confirm row5 --plan ` + op.Status.Plan.ID + ` -n ` + rigNS + `$`).MatchString(r.out) {
			t.Errorf("stdout has no line that starts with the confirm command:\n%s", r.out)
		}
		cliLacks(t, "stdout", r.out, "[y/N]", "not confirmed")
		w.afterSteps(t, 2, "the stepper passes the Operation again")
		got := w.operationIs(t, "row5", "is still Planned", hasPhase(v1alpha1.OperationPhasePlanned))
		if got.Spec.Confirm != nil {
			t.Errorf("spec.confirm is %v: nothing was confirmed", got.Spec.Confirm)
		}
		if got.Spec.Owner != e.a.user {
			t.Errorf("owner %q, want %q", got.Spec.Owner, e.a.user)
		}
		if after := w.worldVersions(t); !sameMap(before, after) {
			t.Errorf("the world changed: %v -> %v", before, after)
		}
	})

	t.Run("row 6: one key, one Operation, and another request under it is refused", func(t *testing.T) {
		for i := 1; i <= 2; i++ {
			if r := w.cli(t, asA, false, "", "scale", opMD, "--instances", "1", "--key", "row6"); r.code != 0 {
				t.Fatalf("call %d: exit %d, stderr %q", i, r.code, r.errs)
			}
		}
		r := w.cli(t, asA, false, "", "scale", opMD, "--instances", "0", "--key", "row6")
		if r.code != 1 {
			t.Fatalf("a different request under the key: exit %d, want 1; stdout %q", r.code, r.out)
		}
		cliHas(t, "stderr", r.errs, "Operation row6 exists with a different request")
		n := 0
		for _, name := range w.operationNames(t) {
			if name == "row6" {
				n++
			}
		}
		if n != 1 {
			t.Errorf("%d Operations named row6, want 1", n)
		}
		if got := w.operationIs(t, "row6", "exists", func(*v1alpha1.Operation) bool { return true }); got.Spec.Scale == nil || got.Spec.Scale.Instances != 1 {
			t.Errorf("row6 asks for %+v, want the first request's 1 Instance", got.Spec.Scale)
		}
	})

	t.Run("row 7: a confirm that meets a conflict while the plan holds is retried and accepted; after a re-plan it is refused", func(t *testing.T) {
		// 7a. Between the CLI's read and its update the person writes the Operation's status, which keeps the plan's ID.
		w.cli(t, asA, false, "", "scale", opMD, "--instances", "1", "--key", "row7a")
		id := w.planned(t, "row7a").Status.Plan.ID
		hook := newPutHook("row7a", func() { w.personWritesStatus(t, "row7a") })
		r := w.cli(t, agentcli.Env{Kubeconfig: kcA, WrapTransport: hook.wrap}, false, "", "confirm", "row7a", "--plan", id)
		seq := hook.sequence()
		if conflicts(seq) == 0 {
			t.Fatalf("the server never answered a PUT with a Conflict (%v): the row proves nothing", seq)
		}
		// The command read the Operation, its first update conflicted, and it read again and updated again until one landed
		// (it then reads the Operation while it waits for the controller to move it on).
		if len(seq) < 4 || seq[0] != "GET 200" || seq[1] != "PUT 409" || seq[2] != "GET 200" || !strings.Contains(strings.Join(seq[3:], ","), "PUT 200") {
			t.Errorf("the command's requests about the Operation were %v, want a read, a PUT that conflicts, then a read and a PUT that lands", seq)
		}
		if r.code != 0 {
			t.Fatalf("exit %d, stderr %q", r.code, r.errs)
		}
		if got := w.operationIs(t, "row7a", "is confirmed", func(o *v1alpha1.Operation) bool { return o.Spec.Confirm != nil }); got.Spec.Confirm.PlanID != id {
			t.Errorf("confirmed %q, want the plan %q", got.Spec.Confirm.PlanID, id)
		}

		// 7b. Between the read and the update the person edits the deployment, so the stepper plans again under another ID.
		w.cli(t, asA, false, "", "scale", opMD, "--instances", "1", "--key", "row7b")
		id = w.planned(t, "row7b").Status.Plan.ID
		hook = newPutHook("row7b", func() { w.personReplans(t, "row7b", id) })
		r = w.cli(t, agentcli.Env{Kubeconfig: kcA, WrapTransport: hook.wrap}, false, "", "confirm", "row7b", "--plan", id)
		seq = hook.sequence()
		if conflicts(seq) == 0 {
			t.Fatalf("the server never answered a PUT with a Conflict (%v): the row proves nothing", seq)
		}
		if r.code != 1 {
			t.Fatalf("a confirm after a re-plan: exit %d, want 1; stderr %q", r.code, r.errs)
		}
		fresh := w.operationIs(t, "row7b", "holds the new plan", func(o *v1alpha1.Operation) bool { return o.Status.Plan != nil && o.Status.Plan.ID != id })
		cliHas(t, "stderr", r.errs, "the current plan is "+fresh.Status.Plan.ID)
		cliHas(t, "stdout", r.out, "plan "+fresh.Status.Plan.ID)
		if fresh.Spec.Confirm != nil {
			t.Errorf("confirm is %v after a re-plan: a plan the caller had not seen was confirmed", fresh.Spec.Confirm)
		}
	})

	t.Run("row 8: a release makes one Operation per deployment, by names and by selector; a selector that matches nothing makes none", func(t *testing.T) {
		w.storeKept(t, "md-b", "alloc-b1")
		w.storeKept(t, "md-c", "alloc-c1")
		// The suffixes are the first 8 hex of the SHA-256 of the deployment's name, worked out in the shell.
		const bName, cName = "-8c769230", "-60c1fc8b"
		if r := w.cli(t, asA, false, "", "release", "alloc-b1", "alloc-c1", "--key", "row8a", "--timeout", "30s"); r.code != 0 {
			t.Fatalf("by names: exit %d, stderr %q", r.code, r.errs)
		}
		for name, want := range map[string][2]string{"row8a" + bName: {"md-b", "alloc-b1"}, "row8a" + cName: {"md-c", "alloc-c1"}} {
			op := w.operationIs(t, name, "exists", func(*v1alpha1.Operation) bool { return true })
			rel := op.Spec.Release
			if op.Spec.Action != v1alpha1.OperationActionRelease || op.Spec.ModelDeploymentRef.Name != want[0] || rel == nil || !sameStrings(rel.Allocations, []string{want[1]}) || rel.Selector != nil {
				t.Errorf("%s: %+v, want a Release of %s naming %s", name, op.Spec, want[0], want[1])
			}
			if op.Spec.Owner != e.a.user || op.Spec.Confirm != nil {
				t.Errorf("%s: owner %q, confirm %v, want A's and none", name, op.Spec.Owner, op.Spec.Confirm)
			}
		}
		if r := w.cli(t, asA, false, "", "release", "-l", "tier=kept", "--key", "row8b", "--timeout", "30s"); r.code != 0 {
			t.Fatalf("by selector: exit %d, stderr %q", r.code, r.errs)
		}
		for _, name := range []string{"row8b" + bName, "row8b" + cName} {
			op := w.operationIs(t, name, "exists", func(*v1alpha1.Operation) bool { return true })
			rel := op.Spec.Release
			if rel == nil || len(rel.Allocations) != 0 || rel.Selector == nil || rel.Selector.MatchLabels["tier"] != "kept" {
				t.Errorf("%s: release %+v, want the selector tier=kept and no names", name, rel)
			}
		}
		before := w.operationNames(t)
		r := w.cli(t, asA, false, "", "release", "-l", "tier=nothing", "--key", "row8c")
		if r.code != 1 {
			t.Errorf("a selector that matches nothing: exit %d, want 1", r.code)
		}
		if after := w.operationNames(t); !sameStrings(before, after) {
			t.Errorf("Operations %v -> %v: a release that matched nothing created one", before, after)
		}
	})

	t.Run("row 9a: with no kubeconfig given, every command exits 2 and reads no default or launcher file", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("a process of root reads a file of mode 000, so there is nothing to refuse")
		}
		home, xdg := t.TempDir(), t.TempDir()
		state := filepath.Join(xdg, "hpcburst")
		plantAdminKubeconfigs(t, w, home, state, 0o000)
		t.Setenv("HOME", home)
		t.Setenv("XDG_STATE_HOME", xdg)
		t.Setenv("KUBECONFIG", "")
		env := agentcli.Env{Kubeconfig: os.Getenv("KUBECONFIG"), Home: os.Getenv("HOME"), XDGStateHome: os.Getenv("XDG_STATE_HOME")}
		before := len(w.operationNames(t))
		for _, args := range allCommands() {
			r := w.cli(t, env, false, "", args...)
			if r.code != 2 {
				t.Errorf("%v: exit %d, want 2; stderr %q", args, r.code, r.errs)
			}
			cliHas(t, strings.Join(args, " ")+" stderr", r.errs, "KUBECONFIG", "--kubeconfig")
			cliLacks(t, strings.Join(args, " ")+" output", r.out+r.errs, "permission denied", state)
		}
		if after := len(w.operationNames(t)); after != before {
			t.Errorf("%d Operations before and %d after: a command with no credential created one", before, after)
		}
	})

	t.Run("row 9b: the admin kubeconfig at the default paths is not used when another is given", func(t *testing.T) {
		home, xdg := t.TempDir(), t.TempDir()
		plantAdminKubeconfigs(t, w, home, filepath.Join(xdg, "hpcburst"), 0o600)
		t.Setenv("HOME", home)
		t.Setenv("XDG_STATE_HOME", xdg)
		t.Setenv("KUBECONFIG", kcA)
		env := agentcli.Env{Kubeconfig: os.Getenv("KUBECONFIG"), Home: os.Getenv("HOME"), XDGStateHome: os.Getenv("XDG_STATE_HOME")}
		r := w.cli(t, env, false, "", "end", opMD, "--key", "row9b")
		if r.code != 0 {
			t.Fatalf("exit %d, stderr %q", r.code, r.errs)
		}
		if got := w.operationIs(t, "row9b", "exists", func(*v1alpha1.Operation) bool { return true }); got.Spec.Owner != e.a.user {
			t.Errorf("the stored owner is %q, want agent A's %q: another kubeconfig was used", got.Spec.Owner, e.a.user)
		}
		cliLacks(t, "stderr", r.errs, "acting as")
	})

	t.Run("row 10: a controller's credential is refused by every command, and nothing is created", func(t *testing.T) {
		cs, err := kubernetes.NewForConfig(e.admin.cfg)
		if err != nil {
			t.Fatal(err)
		}
		ns, sa := v1alpha1.ControllerNamespace, "cli-test-controller"
		if _, err := cs.CoreV1().Namespaces().Create(e.ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatal(err)
		}
		if _, err := cs.CoreV1().ServiceAccounts(ns).Create(e.ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: sa}}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		secs := int64(3600)
		tok, err := cs.CoreV1().ServiceAccounts(ns).CreateToken(e.ctx, sa, &authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: &secs}}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		cfg := rest.AnonymousClientConfig(e.admin.cfg)
		cfg.BearerToken = tok.Status.Token
		kc := cliKubeconfig(t, dir, "controller", cfg, rigNS)
		who := whoAmI(e.ctx, t, cfg)
		if who.user != "system:serviceaccount:"+ns+":"+sa {
			t.Fatalf("the kubeconfig is %q, want the controller's ServiceAccount", who.user)
		}
		ops, aps := len(w.operationNames(t)), w.approvalCount(t)
		for _, args := range allCommands() {
			r := w.cli(t, agentcli.Env{Kubeconfig: kc}, false, "", args...)
			if r.code != 1 {
				t.Errorf("%v: exit %d, want 1; stderr %q", args, r.code, r.errs)
			}
			cliHas(t, strings.Join(args, " ")+" stderr", r.errs, "a controller's credential is outside the capacity guard; use your own identity")
		}
		if got := len(w.operationNames(t)); got != ops {
			t.Errorf("%d Operations before and %d after", ops, got)
		}
		if got := w.approvalCount(t); got != aps {
			t.Errorf("%d Approvals before and %d after", aps, got)
		}
	})

	var rowID string
	t.Run("row 1: scale at a terminal, answered y, ends at AwaitingApproval with the pending Allocation untouched", func(t *testing.T) {
		r := w.cli(t, asA, true, "y\n", "scale", opMD, "--instances", "1", "--key", "row1", "--purpose", "free a slot for another run")
		if r.code != 0 {
			t.Fatalf("exit %d, stderr %q", r.code, r.errs)
		}
		op := w.operationIs(t, "row1", "waits for approval", hasPhase(v1alpha1.OperationPhaseAwaitingApproval))
		rowID = op.Status.Plan.ID
		if op.Spec.Owner != e.a.user {
			t.Errorf("the stored owner is %q, want agent A's %q", op.Spec.Owner, e.a.user)
		}
		if op.Spec.Confirm == nil || op.Spec.Confirm.PlanID != rowID {
			t.Errorf("confirm is %v, want the plan the caller saw, %s", op.Spec.Confirm, rowID)
		}
		cancel := cancelPendingTarget(op.Status.Plan, "alloc-p")
		if cancel == nil {
			t.Fatalf("the plan has no CancelPending target for alloc-p: %+v", op.Status.Plan.Targets)
		}
		if !cancel.NeedsApproval || len(cancel.Policies) != 1 || cancel.Policies[0].Name != "provider."+cliProvider {
			t.Errorf("the target needs approval %v under %+v, want true under the one policy provider.%s", cancel.NeedsApproval, cancel.Policies, cliProvider)
		}
		cliHas(t, "stdout", r.out, "confirm plan "+rowID+"? [y/N]", "phase: AwaitingApproval", "must approve, policy provider."+cliProvider+": serviceaccount "+envAgentsNamespace+"/agent-a")
		var pending v1alpha1.Allocation
		if err := e.admin.c.Get(e.ctx, types.NamespacedName{Namespace: rigNS, Name: "alloc-p"}, &pending); err != nil {
			t.Errorf("the pending Allocation: %v, want it still there: nothing runs before the approval", err)
		}
	})

	t.Run("row 2: an approval by an identity outside the policy is refused by the CLI, naming the policy and its approver", func(t *testing.T) {
		before := w.approvalCount(t)
		r := w.cli(t, asB, false, "", "approve", "row1", "--plan", rowID)
		if r.code != 1 {
			t.Fatalf("exit %d, want 1; stdout %q", r.code, r.out)
		}
		cliHas(t, "stderr", r.errs, "provider."+cliProvider, "serviceaccount "+envAgentsNamespace+"/agent-a")
		if after := w.approvalCount(t); after != before {
			t.Errorf("%d Approvals before and %d after: the refused approve created one", before, after)
		}
	})

	t.Run("row 3: an Approval that B creates directly is admitted and counts for nothing", func(t *testing.T) {
		op := w.operationIs(t, "row1", "is read", func(*v1alpha1.Operation) bool { return true })
		if err := e.approval(t, e.b, "row3-by-b", op, rowID, e.b); err != nil {
			t.Fatalf("B's own Approval: %v, want it admitted by the approver pin", err)
		}
		w.afterSteps(t, 2, "the stepper counts the Approvals again")
		got := w.operationIs(t, "row1", "still waits", hasPhase(v1alpha1.OperationPhaseAwaitingApproval))
		if c := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionExecutable); c == nil || c.Reason != v1alpha1.ReasonAwaitingApproval || !strings.Contains(c.Message, "needs an approval under provider."+cliProvider) {
			t.Errorf("Executable is %+v, want it still waiting for an approval under provider.%s: B's Approval counted", c, cliProvider)
		}
	})

	// Row 11 reads the world as rows 1 to 3 left it: row1 waiting for its approval, and the pending Allocation still there. Row 4
	// executes that plan and changes the world, so it runs last.
	t.Run("row 11: inventory shows the world, selects by owner, shows an object with no owner as unknown, and only reads", func(t *testing.T) {
		w.inventoryRow(t, asA, asB)
	})

	t.Run("row 4: A's approval, through the CLI, runs the plan to Done: the pending Allocation goes, the started one stays", func(t *testing.T) {
		r := w.cli(t, asA, false, "", "approve", "row1", "--plan", rowID)
		if r.code != 0 {
			t.Fatalf("exit %d, stderr %q", r.code, r.errs)
		}
		cliHas(t, "stdout", r.out, "approved plan "+rowID+" of Operation row1 as "+e.a.user)
		done := w.operationIs(t, "row1", "is Done", hasPhase(v1alpha1.OperationPhaseDone))
		var gone v1alpha1.Allocation
		if err := e.admin.c.Get(e.ctx, types.NamespacedName{Namespace: rigNS, Name: "alloc-p"}, &gone); !apierrors.IsNotFound(err) {
			t.Errorf("the pending Allocation: %v, want NotFound", err)
		}
		var kept v1alpha1.Allocation
		if err := e.admin.c.Get(e.ctx, types.NamespacedName{Namespace: rigNS, Name: "alloc-r"}, &kept); err != nil {
			t.Errorf("the started Allocation: %v, want it kept", err)
		}
		var ready v1alpha1.Instance
		if err := e.admin.c.Get(e.ctx, types.NamespacedName{Namespace: rigNS, Name: "inst-r"}, &ready); err != nil || ready.Status.Phase != v1alpha1.InstanceReady {
			t.Errorf("the ready Instance: %v, phase %q, want it untouched and Ready", err, ready.Status.Phase)
		}
		for _, o := range done.Status.Targets {
			if o.State != v1alpha1.TargetStateDone {
				t.Errorf("target %s %s is %s %s, want Done", o.Kind, o.Name, o.State, o.Reason)
			}
		}
		// The report as the CLI shows it, read by agent A.
		show := w.cli(t, asA, false, "", "operation", "row1", "--json")
		if show.code != 0 {
			t.Fatalf("operation --json: exit %d, stderr %q", show.code, show.errs)
		}
		var doc struct {
			SchemaVersion int `json:"schemaVersion"`
			Operation     struct {
				Phase   string `json:"phase"`
				Targets []struct {
					Kind, Name, State string
				} `json:"targets"`
			} `json:"operation"`
		}
		if err := json.Unmarshal([]byte(show.out), &doc); err != nil {
			t.Fatalf("operation --json is not JSON: %v\n%s", err, show.out)
		}
		if doc.SchemaVersion != 1 || doc.Operation.Phase != "Done" || len(doc.Operation.Targets) == 0 {
			t.Fatalf("operation --json: %+v, want schema 1, phase Done and a report", doc)
		}
		sawAlloc := false
		for _, tg := range doc.Operation.Targets {
			sawAlloc = sawAlloc || (tg.Kind == "Allocation" && tg.Name == "alloc-p")
			if tg.State != "Done" {
				t.Errorf("the report shows %s %s as %s, want Done", tg.Kind, tg.Name, tg.State)
			}
		}
		if !sawAlloc {
			t.Errorf("the report names no target alloc-p: %+v", doc.Operation.Targets)
		}
	})

	if errs := w.unexpectedStepErrors(); len(errs) > 0 {
		t.Errorf("the Operations step failed %d times: %v", len(errs), errors.Join(errs...))
	}
}

// cancelPendingTarget is the plan's CancelPending target of the named object.
func cancelPendingTarget(p *v1alpha1.OperationPlan, name string) *v1alpha1.PlanTarget {
	for i := range p.Targets {
		if p.Targets[i].Name == name && p.Targets[i].Effect == v1alpha1.PlanEffectCancelPending {
			return &p.Targets[i]
		}
	}
	return nil
}

// allCommands is one command line of each command, with nothing the command needs beyond its arguments.
func allCommands() [][]string {
	return [][]string{
		{"scale", opMD, "--instances", "1"}, {"end", opMD}, {"release", "alloc-r"}, {"confirm", "row1", "--plan", "p"},
		{"approvals"}, {"approve", "row1", "--plan", "p"}, {"operation", "row1"}, {"inventory"},
	}
}

func conflicts(seq []string) int {
	n := 0
	for _, s := range seq {
		if s == "PUT 409" {
			n++
		}
	}
	return n
}
