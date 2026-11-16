package main

// The global ApprovalPolicy that up writes, and the refusal to start a controller without the definitions it
// watches, on a real k3s (HBEP-10, Approval policy; issue #589, launcher track). The unit tests in
// internal/launcher judge the logic against a fake API server; this is what a real one must agree with:
//
//	(a) up with a controller and no --manifests fails, says --manifests config/crd, and leaves nothing running;
//	(b) the same up with --manifests config/crd starts, and ApprovalPolicy/global exists with ONE approver, the
//	    user the API server reports for the admin kubeconfig, which the test asks for itself;
//	(c) a person's edit of the approvers survives down and up: the same UID and resource version, both approvers;
//	(d) status shows the approvers.
//
// The launcher block is 28460 (#589's, 28460-28479), as admission_k3s_test.go's.

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

var apolGVR = v1alpha1.GroupVersion.WithResource("approvalpolicies")

// controllerCRDKinds are the definitions an up that starts the controller needs besides the ones its
// other steps use: the kinds the controller watches.
var controllerCRDKinds = []string{"operations", "approvals", "approvalpolicies"}

// addControllerCRDs copies the definitions of Operation, Approval and ApprovalPolicy from config/crd into dir,
// for a test whose own manifests directory holds none of them and whose up starts a controller.
func addControllerCRDs(t testing.TB, dir string) {
	t.Helper()
	for _, kind := range controllerCRDKinds {
		b, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "hpcburst.groundnuty.github.io_"+kind+".yaml"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "15-crd-"+kind+".yaml"), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// manifestsWithControllerCRDs is a copy of the manifests directory src with the three definitions added.
func manifestsWithControllerCRDs(t testing.TB, src string) string {
	t.Helper()
	dir := shortTmp(t, false)
	ents, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		b, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, e.Name()), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	addControllerCRDs(t, dir)
	return dir
}

// policyStatus is the part of status --json this test reads.
type policyStatus struct {
	Health               string `json:"health"`
	GlobalApprovalPolicy *struct {
		Present   bool     `json:"present"`
		Approvers []string `json:"approvers"`
		Detail    string   `json:"detail"`
	} `json:"global_approval_policy"`
}

func TestUpWritesTheGlobalApprovalPolicyOnARealK3s(t *testing.T) {
	const base = 28460
	h := newHost(t)
	ctl, _ := fakeController(t)
	crds := filepath.Join("..", "..", "config", "crd")
	upWithController := func(extra ...string) cliResult {
		return cli(t, h.upArgs(base, append([]string{"--json", "--controller-bin", ctl}, extra...)...)...)
	}

	// ---- (a) a controller and no definitions: up refuses, says how to fix it, and stops what it started ----
	r := upWithController()
	if r.code == exitOK {
		t.Fatalf("up with a controller and no --manifests exited 0: %s", r.out)
	}
	if !strings.Contains(r.err, "--manifests config/crd") {
		t.Errorf("the refusal does not say how to fix it (--manifests config/crd): %s", r.err)
	}
	for _, crd := range []string{"operations.", "approvals.", "approvalpolicies."} {
		if !strings.Contains(r.err, crd+"hpcburst.groundnuty.github.io") {
			t.Errorf("the refusal does not name the missing definition %s: %s", crd, r.err)
		}
	}
	if !strings.Contains(r.err, "the host was stopped again") {
		t.Errorf("the refusal does not say the host was stopped: %s", r.err)
	}
	if _, err := os.Stat(filepath.Join(h.dir, "state.json")); err == nil {
		t.Error("state.json remains after the refused up")
	}
	if pids := k3sProcessesUsing(h.dir); len(pids) != 0 {
		t.Errorf("processes still run after the refused up: %v", pids)
	}
	if st := cli(t, h.args("status")...); st.code != exitStopped {
		t.Errorf("status after the refused up: exit %d, want %d", st.code, exitStopped)
	}

	// ---- (b) the same up with the definitions: the controller starts, and the policy is written ----
	r = upWithController("--manifests", crds)
	if r.code != exitOK {
		t.Fatalf("up with --manifests config/crd exited %d: %s", r.code, r.err)
	}
	var up struct {
		GlobalApprovalPolicy string `json:"global_approval_policy"`
		SupervisorPID        int    `json:"supervisor_pid"`
	}
	parseJSON(t, r.out, &up)
	if up.GlobalApprovalPolicy != "created" || up.SupervisorPID == 0 {
		t.Errorf("up reported %+v, want the policy created and the controller supervised", up)
	}
	// The admin's user name, asked for by the test itself with the same credential the launcher holds.
	admin := whoIs(t, filepath.Join(h.dir, "kubeconfig")).Username
	if admin == "" {
		t.Fatal("the API server reports no user name for the admin kubeconfig")
	}
	ctx, cancel := testCtx()
	defer cancel()
	dyn := h.clients().dyn
	pol, err := dyn.Resource(apolGVR).Get(ctx, "global", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("ApprovalPolicy global after up: %v", err)
	}
	approvers, _, _ := unstructured.NestedSlice(pol.Object, "spec", "approvers")
	global, _, _ := unstructured.NestedBool(pol.Object, "spec", "scope", "global")
	if len(approvers) != 1 || !global {
		t.Fatalf("the policy is %v, want the global scope and one approver", pol.Object["spec"])
	}
	if a, _ := approvers[0].(map[string]any); len(a) != 1 || a["user"] != admin {
		t.Errorf("the approver is %v, want only user %q", approvers[0], admin)
	}

	// ---- (c) a person's edit survives down and up: no overwrite, no new object ----
	if err := unstructured.SetNestedSlice(pol.Object, append(approvers, map[string]any{"group": "example-approvers"}), "spec", "approvers"); err != nil {
		t.Fatal(err)
	}
	edited, err := dyn.Resource(apolGVR).Update(ctx, pol, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("the admin's edit of the policy: %v", err)
	}
	uid, rv := edited.GetUID(), edited.GetResourceVersion() // after the edit, so a later write shows
	if d := cli(t, h.args("down")...); d.code != exitOK {
		t.Fatalf("down exited %d: %s", d.code, d.err)
	}
	r = upWithController("--manifests", crds)
	if r.code != exitOK {
		t.Fatalf("the second up exited %d: %s", r.code, r.err)
	}
	up.GlobalApprovalPolicy = ""
	parseJSON(t, r.out, &up)
	if up.GlobalApprovalPolicy != "exists" {
		t.Errorf("the second up reported %q for the policy, want exists", up.GlobalApprovalPolicy)
	}
	dyn = h.clients().dyn
	again, err := dyn.Resource(apolGVR).Get(ctx, "global", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("ApprovalPolicy global after the second up: %v", err)
	}
	if again.GetUID() != uid || again.GetResourceVersion() != rv {
		t.Errorf("the policy was written again: uid %s -> %s, resource version %s -> %s", uid, again.GetUID(), rv, again.GetResourceVersion())
	}
	got, _, _ := unstructured.NestedSlice(again.Object, "spec", "approvers")
	if len(got) != 2 {
		t.Fatalf("the approvers after the second up are %v, want both", got)
	}
	if a, _ := got[0].(map[string]any); a["user"] != admin {
		t.Errorf("the first approver is %v, want user %q", got[0], admin)
	}
	if a, _ := got[1].(map[string]any); a["group"] != "example-approvers" {
		t.Errorf("the second approver is %v, want the group the admin added", got[1])
	}

	// ---- (d) status shows the approvers, in the text and in the JSON ----
	var js policyStatus
	st := cli(t, h.args("status", "--json", "--no-gc-probe")...)
	parseJSON(t, st.out, &js)
	want := []string{"user " + admin, "group example-approvers"}
	if st.code != exitOK || js.Health != "healthy" || js.GlobalApprovalPolicy == nil || !js.GlobalApprovalPolicy.Present || !slices.Equal(js.GlobalApprovalPolicy.Approvers, want) {
		t.Errorf("status --json: exit %d, health %q, policy %+v, want healthy with approvers %v", st.code, js.Health, js.GlobalApprovalPolicy, want)
	}
	if text := cli(t, h.args("status", "--no-gc-probe")...).out; !strings.Contains(text, "global ApprovalPolicy: approvers "+strings.Join(want, ", ")+"\n") {
		t.Errorf("status text lacks the approvers:\n%s", text)
	}

	if d := cli(t, h.args("down")...); d.code != exitOK {
		t.Fatalf("down exited %d: %s", d.code, d.err)
	}
	if l := listeningInRange(t, base, base+19); len(l) != 0 {
		t.Errorf("after down something still listens in 28460-28479: %v", l)
	}
}
