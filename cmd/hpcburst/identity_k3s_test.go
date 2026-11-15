package main

// The agents' identities and roles on a real k3s (HBEP-10, Identities and roles; issue #589, PR 2).
// up installs the namespace, the roles and the bindings; hpcburst identity add and renew make two
// agents' kubeconfigs; each is judged by the API server's own answers made with that kubeconfig, never
// by looking at an object: a SelfSubjectReview that names the agent, a real update of an Allocation that
// RBAC refuses, and a real read that it allows. The roles' full verb by resource matrix is judged on
// envtest (api/v1alpha1/e2e); this is the subset a real k3s must agree with, which is also where the
// token's 30 days are shown to be honoured, and where the admin's user name is read, never assumed.
//
// The launcher block is 28460 (#589's, 28460-28479).

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const (
	agentsGroupName = "system:serviceaccounts:hpcburst-agents"
	agentsNamespace = "hpcburst-agents"
	month           = 30 * 24 * time.Hour
)

var roleGVR = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}

// identityJSON is what identity add and renew print with --json.
type identityJSON struct {
	Name       string    `json:"name"`
	Username   string    `json:"username"`
	Kubeconfig string    `json:"kubeconfig"`
	Expires    time.Time `json:"expires"`
	Renewed    bool      `json:"renewed"`
}

// agentStatus is the part of status --json this test reads.
type agentStatus struct {
	Health    string `json:"health"`
	AgentRBAC *struct {
		Present bool   `json:"present"`
		Detail  string `json:"detail"`
	} `json:"agent_rbac"`
	Identities []struct {
		Name       string     `json:"name"`
		Kubeconfig string     `json:"kubeconfig"`
		Expires    *time.Time `json:"expires"`
		Valid      bool       `json:"valid"`
		Detail     string     `json:"detail"`
	} `json:"identities"`
}

// kubeClients are the clients of one kubeconfig file.
func kubeClients(t *testing.T, path string) (kubernetes.Interface, dynamic.Interface) {
	t.Helper()
	cfg, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Timeout = 30 * time.Second
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return cs, dyn
}

// whoIs asks the API server who the kubeconfig's credential is.
func whoIs(t *testing.T, path string) authenticationv1.UserInfo {
	t.Helper()
	cs, _ := kubeClients(t, path)
	ctx, cancel := testCtx()
	defer cancel()
	r, err := cs.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("SelfSubjectReview with %s: %v", path, err)
	}
	return r.Status.UserInfo
}

// tokenOf reads the token out of a kubeconfig file, and its exp claim with the test's own decoder: a
// service account token is a JWT, and the API server set its exp from its TokenRequest answer.
func tokenOf(t *testing.T, path string) (token string, exp time.Time) {
	t.Helper()
	cfg, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(cfg.BearerToken, ".")
	if len(parts) != 3 {
		t.Fatalf("the token in %s is not a JWT", path)
	}
	b, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(b, &claims); err != nil || claims.Exp == 0 {
		t.Fatalf("the token in %s has no exp claim: %v", path, err)
	}
	return cfg.BearerToken, time.Unix(claims.Exp, 0).UTC()
}

func TestIdentitiesOnARealK3s(t *testing.T) {
	const base = 28460
	h := newHost(t)
	crds := filepath.Join("..", "..", "config", "crd")

	// ---- up installs the roles ----
	var up struct {
		AgentRBAC bool `json:"agent_rbac"`
	}
	r := cli(t, h.upArgs(base, "--manifests", crds, "--json")...)
	if r.code != exitOK {
		t.Fatalf("up exited %d: %s", r.code, r.err)
	}
	parseJSON(t, r.out, &up)
	if !up.AgentRBAC {
		t.Errorf("up did not say it installed the agents' roles: %s", r.out)
	}
	adminKubeconfig := filepath.Join(h.dir, "kubeconfig")
	// PR 4's global policy names the admin by this user name, so it is logged, never assumed.
	admin := whoIs(t, adminKubeconfig)
	t.Logf("the admin kubeconfig's SelfSubjectReview: user %q, groups %v", admin.Username, admin.Groups)

	// ---- add two identities ----
	idDir := filepath.Join(h.dir, "identities")
	add := func(verb, name string) (identityJSON, time.Time, time.Time) {
		t.Helper()
		before := time.Now()
		res := cli(t, "identity", verb, name, "--json", "--state-dir", h.dir)
		after := time.Now()
		if res.code != exitOK {
			t.Fatalf("identity %s %s exited %d: %s", verb, name, res.code, res.err)
		}
		var out identityJSON
		parseJSON(t, res.out, &out)
		return out, before, after
	}
	a, aBefore, aAfter := add("add", "agent-a")
	b, _, _ := add("add", "agent-b")
	for _, id := range []identityJSON{a, b} {
		if want := filepath.Join(idDir, id.Name+".kubeconfig"); id.Kubeconfig != want || id.Username != "system:serviceaccount:hpcburst-agents:"+id.Name || id.Renewed {
			t.Errorf("identity add %s reported %+v, want the file %s", id.Name, id, want)
		}
		// The file is mode 600 and its directory 700.
		if fi, err := os.Stat(id.Kubeconfig); err != nil || fi.Mode().Perm() != 0o600 {
			t.Errorf("%s: %v, %v, want mode 600", id.Kubeconfig, fi, err)
		}
		if fi, err := os.Stat(idDir); err != nil || fi.Mode().Perm() != 0o700 {
			t.Errorf("%s: %v, %v, want mode 700", idDir, fi, err)
		}
		// Each kubeconfig authenticates as its own agent, in the agents' group, and not as the admin.
		u := whoIs(t, id.Kubeconfig)
		if u.Username != id.Username || u.Username == admin.Username || !slices.Contains(u.Groups, agentsGroupName) {
			t.Errorf("the kubeconfig of %s authenticates as %q in %v, want %q in %q", id.Name, u.Username, u.Groups, id.Username, agentsGroupName)
		}
		if raw := fileOf(id.Kubeconfig); strings.Contains(raw, "client-certificate") || strings.Contains(raw, "client-key") {
			t.Errorf("the kubeconfig of %s holds a client certificate or key, which is the admin's kind of credential", id.Name)
		}
	}
	// The token is 30 days, as the API server answered: k3s honours it. The reported expiry is the
	// token's own exp claim, read by the test's decoder, and 30 days from the request.
	if _, exp := tokenOf(t, a.Kubeconfig); !exp.Equal(a.Expires) ||
		a.Expires.Before(aBefore.Add(month).Add(-2*time.Second)) || a.Expires.After(aAfter.Add(month).Add(2*time.Second)) {
		t.Errorf("agent-a's token carries exp %s, the command reported %s, want both 30 days from a request made between %s and %s", exp, a.Expires, aBefore, aAfter)
	}

	// ---- the roles, by real requests as agent-a ----
	h.runningAllocation("alloc-a")
	cl := h.clients() // the admin's
	allocs := v1alpha1.GroupVersion.WithResource("allocations")
	ctx, cancel := testCtx()
	defer cancel()
	_, agentDyn := kubeClients(t, a.Kubeconfig)
	// An allowed read.
	got, err := agentDyn.Resource(allocs).Namespace("default").Get(ctx, "alloc-a", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("agent-a's read of an Allocation: %v, want it allowed", err)
	}
	// A refused update, by a real request: RBAC's "cannot", naming the agent, and the object is untouched.
	got.SetLabels(map[string]string{"changed-by": "agent"})
	_, err = agentDyn.Resource(allocs).Namespace("default").Update(ctx, got, metav1.UpdateOptions{})
	want := `User "system:serviceaccount:hpcburst-agents:agent-a" cannot update resource "allocations"`
	if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), want) {
		t.Errorf("agent-a's update of an Allocation: %v, want a Forbidden RBAC refusal containing %s", err, want)
	}
	if cur, err := cl.dyn.Resource(allocs).Namespace("default").Get(ctx, "alloc-a", metav1.GetOptions{}); err != nil || cur.GetLabels()["changed-by"] != "" {
		t.Errorf("after the refused update, the Allocation: %v, labels %v", err, cur.GetLabels())
	}
	// Beside it the admin's same update (a dry run) passes, so the refusal is the agent's role.
	adminGot, err := cl.dyn.Resource(allocs).Namespace("default").Get(ctx, "alloc-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	adminGot.SetLabels(map[string]string{"changed-by": "admin"})
	if _, err := cl.dyn.Resource(allocs).Namespace("default").Update(ctx, adminGot, metav1.UpdateOptions{DryRun: []string{metav1.DryRunAll}}); err != nil {
		t.Errorf("the admin's dry-run update of the same Allocation: %v, want it allowed", err)
	}

	// ---- the refusals of the commands ----
	before := fileOf(a.Kubeconfig)
	beforeInfo, _ := os.Stat(a.Kubeconfig)
	if r := cli(t, "identity", "add", "agent-a", "--state-dir", h.dir); r.code != exitFail || !strings.Contains(r.err, "renew") {
		t.Errorf("add of an existing name: exit %d, stderr %q, want exit 1 and 'renew'", r.code, r.err)
	}
	if afterInfo, _ := os.Stat(a.Kubeconfig); fileOf(a.Kubeconfig) != before || !afterInfo.ModTime().Equal(beforeInfo.ModTime()) {
		t.Errorf("the refused add changed agent-a's kubeconfig (bytes or modification time)")
	}
	if r := cli(t, "identity", "renew", "agent-ghost", "--state-dir", h.dir); r.code != exitFail {
		t.Errorf("renew of a name with no ServiceAccount: exit %d, stderr %q, want exit 1", r.code, r.err)
	}
	if _, err := os.Stat(filepath.Join(idDir, "agent-ghost.kubeconfig")); err == nil {
		t.Errorf("the refused renew wrote a kubeconfig")
	}
	for _, name := range []string{"Agent-A", "agent_a", "../x"} {
		if r := cli(t, "identity", "add", name, "--state-dir", h.dir); r.code != exitUsage {
			t.Errorf("add of %q: exit %d, want the usage exit %d", name, r.code, exitUsage)
		}
	}

	// ---- renew: a different token, 30 days from the renewal, the old one valid until it expires ----
	oldToken, _ := tokenOf(t, a.Kubeconfig)
	a2, rBefore, rAfter := add("renew", "agent-a")
	newToken, exp := tokenOf(t, a.Kubeconfig)
	if newToken == oldToken || !a2.Renewed || !exp.Equal(a2.Expires) ||
		a2.Expires.Before(rBefore.Add(month).Add(-2*time.Second)) || a2.Expires.After(rAfter.Add(month).Add(2*time.Second)) {
		t.Errorf("renew: %+v, token changed %v, token exp %s, want a new token that expires 30 days from a request between %s and %s", a2, newToken != oldToken, exp, rBefore, rAfter)
	}
	if u := whoIs(t, a.Kubeconfig); u.Username != a.Username {
		t.Errorf("after renew the kubeconfig authenticates as %q, want %q", u.Username, a.Username)
	}
	if fi, err := os.Stat(a.Kubeconfig); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("after renew %s: %v, %v, want mode 600", a.Kubeconfig, fi, err)
	}

	// ---- status lists both identities with their expiry, and the roles ----
	// statusJSON runs status --json, which is judged the same way every time: exit 0 and healthy.
	statusJSON := func() agentStatus {
		t.Helper()
		var js agentStatus
		sr := cli(t, h.args("status", "--json", "--no-gc-probe")...)
		parseJSON(t, sr.out, &js)
		if sr.code != exitOK || js.Health != "healthy" {
			t.Errorf("status: exit %d, health %q, want exit 0 and healthy", sr.code, js.Health)
		}
		return js
	}
	status := func() (agentStatus, string) {
		t.Helper()
		return statusJSON(), cli(t, h.args("status", "--no-gc-probe")...).out
	}
	js, text := status()
	if js.AgentRBAC == nil || !js.AgentRBAC.Present || len(js.Identities) != 2 {
		t.Fatalf("status --json: agent_rbac %+v, %d identities, want the roles present and 2 identities", js.AgentRBAC, len(js.Identities))
	}
	// Exactly the two identities that were added: the ServiceAccount called default that Kubernetes
	// makes in every namespace is not one of them.
	for i, id := range js.Identities {
		wantName := []string{"agent-a", "agent-b"}[i]
		wantExp := map[string]time.Time{"agent-a": a2.Expires, "agent-b": b.Expires}[wantName]
		if id.Name != wantName || !id.Valid || id.Detail != "" || id.Expires == nil || !id.Expires.Equal(wantExp) {
			t.Errorf("status identity %d = %+v, want %s valid with expiry %s", i, id, wantName, wantExp)
		}
		if line := "agent identity " + wantName + ": valid, expires " + wantExp.Format(time.RFC3339); !strings.Contains(text, line) {
			t.Errorf("status text lacks %q:\n%s", line, text)
		}
	}
	// --no-api asks nothing, so it says nothing about the agents.
	var noAPI agentStatus
	parseJSON(t, cli(t, h.args("status", "--json", "--no-api")...).out, &noAPI)
	if noAPI.AgentRBAC != nil || len(noAPI.Identities) != 0 {
		t.Errorf("status --no-api reports %+v and %d identities, want nothing: it did not ask", noAPI.AgentRBAC, len(noAPI.Identities))
	}

	// ---- a rejected identity and a role edited by hand are reported states: the host stays healthy and the exit code 0 ----
	if err := cl.cs.CoreV1().ServiceAccounts(agentsNamespace).Delete(ctx, "agent-b", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.dyn.Resource(roleGVR).Patch(ctx, "hpcburst-agent", types.JSONPatchType,
		[]byte(`[{"op":"add","path":"/rules/-","value":{"apiGroups":["hpcburst.groundnuty.github.io"],"resources":["allocations"],"verbs":["delete"]}}]`), metav1.PatchOptions{}); err != nil {
		t.Fatalf("add a rule to the agent role by hand: %v", err)
	}
	// The API server stops accepting a deleted ServiceAccount's token a moment after the delete.
	end := time.Now().Add(2 * time.Minute)
	for {
		js = statusJSON()
		rejected := false
		for _, id := range js.Identities {
			rejected = rejected || (id.Name == "agent-b" && !id.Valid && strings.Contains(id.Detail, "rejected the token (401)"))
		}
		if rejected {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("status never reported agent-b rejected: %+v", js.Identities)
		}
		time.Sleep(time.Second)
	}
	if js.AgentRBAC == nil || js.AgentRBAC.Present || !strings.Contains(js.AgentRBAC.Detail, "ClusterRole hpcburst-agent has rules that differ") {
		t.Errorf("status of a hand-edited role: %+v, want not the manifest's, naming ClusterRole hpcburst-agent", js.AgentRBAC)
	}
	for _, id := range js.Identities {
		if id.Name == "agent-a" && !id.Valid {
			t.Errorf("agent-a is not valid after agent-b's ServiceAccount was deleted: %+v", id)
		}
	}
	text = cli(t, h.args("status", "--no-gc-probe")...).out
	for _, w := range []string{"agent roles: DIFFER FROM THE MANIFEST", "agent identity agent-b: NOT VALID"} {
		if !strings.Contains(text, w) {
			t.Errorf("status text lacks %q:\n%s", w, text)
		}
	}

	// ---- a restart: the identities stay, their tokens stay valid, and the roles are the manifest's again ----
	if d := cli(t, h.args("down")...); d.code != exitOK {
		t.Fatalf("down exited %d: %s", d.code, d.err)
	}
	if _, err := os.Stat(a.Kubeconfig); err != nil {
		t.Errorf("down removed agent-a's kubeconfig: %v", err)
	}
	if u := cli(t, h.upArgs(base, "--json")...); u.code != exitOK {
		t.Fatalf("second up exited %d: %s", u.code, u.err)
	}
	if u := whoIs(t, a.Kubeconfig); u.Username != a.Username {
		t.Errorf("after a restart agent-a's old kubeconfig authenticates as %q, want %q", u.Username, a.Username)
	}
	js, _ = status()
	if js.AgentRBAC == nil || !js.AgentRBAC.Present {
		t.Errorf("after a restart the roles are %+v, want the manifest's: up resets them on every start", js.AgentRBAC)
	}
	// agent-a still cannot delete what the hand edit had let it delete: the reset took the rule away.
	waitFor(t, 60*time.Second, "agent-a's delete of an Allocation to be refused again", func() bool {
		_, dyn := kubeClients(t, a.Kubeconfig)
		err := dyn.Resource(allocs).Namespace("default").Delete(ctx, "alloc-a", metav1.DeleteOptions{DryRun: []string{metav1.DryRunAll}})
		return apierrors.IsForbidden(err)
	})
	if d := cli(t, h.args("down")...); d.code != exitOK {
		t.Fatalf("down exited %d: %s", d.code, d.err)
	}
	if l := listeningInRange(t, base, base+19); len(l) != 0 {
		t.Errorf("after down something still listens in 28460-28479: %v", l)
	}
}
