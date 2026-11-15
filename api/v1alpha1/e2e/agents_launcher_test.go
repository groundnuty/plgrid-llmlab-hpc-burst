package e2e

// The launcher's side of the agents' identities and roles (internal/launcher/agents.go, identity.go,
// identity_status.go; issue #589, PR 2) against a real API server with RBAC: the install, what a hand
// edit does and the reset after it, the check, and issuing, renewing and checking identities. Every
// judgement is the API server's decision for an agent's own credential, or its answer to a request:
// that a role object holds the manifest's rules proves nothing about what an agent may do.
//
// Each edit is shown to take effect first (the agent is allowed what the edit grants, or denied what it
// removes): a reset that is judged against an edit that never reached the authorizer proves nothing.

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

func TestLauncherInstallsResetsAndChecksTheAgentRoles(t *testing.T) {
	a := newAgents(t, "agent-a")[0]
	requireAgentIdentity(t, a)
	adminCS, _ := clientsFromConfig(t, cfg)
	const G = frameworkGroup
	deleteAllocation := accessRow{G, "allocations", "", "default", "delete", false}
	getProviders := accessRow{G, "providers", "", "", "get", true}
	createMD := accessRow{G, "modeldeployments", "", "default", "create", true}
	deleteApprovalPolicy := accessRow{G, "approvalpolicies", "", "", "delete", false}
	waitForAgentMatrix(t, a.cs, expectedAgentMatrix())

	check := func(wantPresent bool, detail string) {
		t.Helper()
		st, err := launcher.CheckAgentRBAC(bg(), cfg)
		if err != nil || st.Present != wantPresent || !strings.Contains(st.Detail, detail) {
			t.Fatalf("CheckAgentRBAC = %+v, %v, want present %v with a detail containing %q", st, err, wantPresent, detail)
		}
	}
	install := func() {
		t.Helper()
		must(t, launcher.InstallAgentRBAC(bg(), cfg, os.Stderr), "install the agents' roles")
	}
	updateRole := func(name string, edit func(r *rbacv1.ClusterRole)) {
		t.Helper()
		var r rbacv1.ClusterRole
		must(t, c.Get(bg(), client.ObjectKey{Name: name}, &r), "read "+name)
		edit(&r)
		must(t, c.Update(bg(), &r), "edit "+name+" by hand")
	}

	// Installed: it is the manifest's, and installing again changes nothing and fails nothing.
	check(true, "")
	install()
	check(true, "")

	// An extra rule: the agent may now delete an Allocation (the edit took effect). The check names
	// the role, and the install takes the rule away: the agent is denied again.
	if ok, err := ask(a.cs, deleteAllocation); ok || err != nil {
		t.Fatalf("before the edit an agent may delete an Allocation: %v, %v", ok, err)
	}
	updateRole("hpcburst-agent", func(r *rbacv1.ClusterRole) {
		r.Rules = append(r.Rules, rbacv1.PolicyRule{APIGroups: []string{G}, Resources: []string{"allocations"}, Verbs: []string{"delete"}})
	})
	waitUntilAllowed(t, a.cs, deleteAllocation)
	check(false, "ClusterRole hpcburst-agent has rules that differ")
	install()
	waitUntilDenied(t, a.cs, deleteAllocation)
	check(true, "")

	// A rule removed: the agent may no longer read Providers. The install gives the rule back.
	updateRole("hpcburst-agent-read", func(r *rbacv1.ClusterRole) { r.Rules = nil })
	waitUntilDenied(t, a.cs, getProviders)
	check(false, "ClusterRole hpcburst-agent-read has rules that differ")
	install()
	waitUntilAllowed(t, a.cs, getProviders)
	check(true, "")

	// An aggregationRule, which a controller would use to rewrite the rules after the reset: reported,
	// and removed by the install. (This host runs no aggregation controller, so the rules stay the
	// manifest's here; the check and the reset are what is judged.)
	updateRole("hpcburst-agent", func(r *rbacv1.ClusterRole) {
		r.AggregationRule = &rbacv1.AggregationRule{ClusterRoleSelectors: []metav1.LabelSelector{{MatchLabels: map[string]string{"example": "never"}}}}
	})
	check(false, "ClusterRole hpcburst-agent has an aggregationRule")
	install()
	var role rbacv1.ClusterRole
	must(t, c.Get(bg(), client.ObjectKey{Name: "hpcburst-agent"}, &role), "read the agent role")
	if role.AggregationRule != nil {
		t.Errorf("the install left an aggregationRule on the agent role: %+v", role.AggregationRule)
	}
	check(true, "")

	// A role deleted: the agent loses what it gave, and the install makes it again.
	must(t, c.Delete(bg(), &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "hpcburst-agent-read"}}), "delete the read role by hand")
	waitUntilDenied(t, a.cs, getProviders)
	check(false, "ClusterRole hpcburst-agent-read is not there")
	install()
	waitUntilAllowed(t, a.cs, getProviders)
	check(true, "")

	// An extra subject on the agent binding: a user that is no agent now may create a
	// ModelDeployment (a SubjectAccessReview by the admin for that user shows it), and after the install
	// may not.
	bob := func() bool {
		t.Helper()
		r, err := adminCS.AuthorizationV1().SubjectAccessReviews().Create(bg(), &authorizationv1.SubjectAccessReview{Spec: authorizationv1.SubjectAccessReviewSpec{
			User: "example-user", ResourceAttributes: &authorizationv1.ResourceAttributes{Namespace: "default", Verb: "create", Group: G, Resource: "modeldeployments"},
		}}, metav1.CreateOptions{})
		must(t, err, "SubjectAccessReview")
		return r.Status.Allowed
	}
	if bob() {
		t.Fatal("before the edit a user outside the agents' group may create a ModelDeployment")
	}
	var crb rbacv1.ClusterRoleBinding
	must(t, c.Get(bg(), client.ObjectKey{Name: "hpcburst-agent"}, &crb), "read the agent binding")
	crb.Subjects = append(crb.Subjects, rbacv1.Subject{APIGroup: "rbac.authorization.k8s.io", Kind: "User", Name: "example-user"})
	must(t, c.Update(bg(), &crb), "add a subject to the agent binding by hand")
	eventually(t, 60*time.Second, "the extra subject to be granted the agent role", func() (bool, error) { return bob(), nil })
	check(false, "ClusterRoleBinding hpcburst-agent has subjects that differ")
	install()
	eventually(t, 60*time.Second, "the extra subject to lose the agent role", func() (bool, error) { return !bob(), nil })
	check(true, "")

	// A binding whose roleRef differs, which cannot be edited: the install deletes it and makes it again
	// with the manifest's. Here the agents' group is bound to the operator role instead of the agent role.
	must(t, c.Delete(bg(), &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "hpcburst-agent"}}), "delete the agent binding")
	must(t, c.Create(bg(), &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "hpcburst-agent"},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "hpcburst-operator"},
		Subjects:   []rbacv1.Subject{{APIGroup: "rbac.authorization.k8s.io", Kind: "Group", Name: agentsGroup}},
	}), "bind the agents' group to the operator role by hand")
	waitUntilAllowed(t, a.cs, deleteApprovalPolicy) // the operator role's delete
	waitUntilDenied(t, a.cs, createMD)              // and the agent role's create is gone
	check(false, "ClusterRoleBinding hpcburst-agent has a roleRef that differs")
	install()
	waitUntilAllowed(t, a.cs, createMD)
	waitUntilDenied(t, a.cs, deleteApprovalPolicy)
	check(true, "")
	// Whatever the edits did, every row of the matrix is the expected one again.
	judge(t, "agent "+a.name+" after the resets", a.cs, expectedAgentMatrix(), func(r accessRow) bool { return r.agentMay })
}

func TestIdentityIssueAndRenew(t *testing.T) {
	installAgentRoles(t)
	adminCS, _ := clientsFromConfig(t, cfg)
	// The directory does not exist yet: issuing makes it, mode 700.
	dir := filepath.Join(t.TempDir(), "identities")
	name := uniq("agent-x")
	t.Cleanup(func() {
		_ = adminCS.CoreV1().ServiceAccounts(agentsNamespace).Delete(bg(), name, metav1.DeleteOptions{})
	})

	var log bytes.Buffer
	before := time.Now()
	res, err := launcher.IssueIdentity(bg(), cfg, dir, name, false, &log)
	after := time.Now()
	must(t, err, "add")
	path := filepath.Join(dir, name+".kubeconfig")
	if res.Kubeconfig != path || res.Name != name || res.Username != "system:serviceaccount:"+agentsNamespace+":"+name || res.Renewed {
		t.Errorf("the result is %+v, want the file %s for %s, not renewed", res, path, name)
	}
	for p, want := range map[string]os.FileMode{dir: 0o700, path: 0o600} {
		if fi, err := os.Stat(p); err != nil || fi.Mode().Perm() != want {
			t.Errorf("%s: %v, %v, want mode %v", p, fi, err, want)
		}
	}
	// The expiry is the API server's answer, 30 days from the request (it is cut to whole seconds).
	const month = 30 * 24 * time.Hour
	if res.Expires.Before(before.Add(month).Add(-2*time.Second)) || res.Expires.After(after.Add(month).Add(2*time.Second)) {
		t.Errorf("the token expires %s, want 30 days from the request, made between %s and %s", res.Expires, before, after)
	}

	// The kubeconfig authenticates as the new identity in the agents' group, and holds the server and
	// the token and none of the admin's credential.
	kc, err := clientcmd.BuildConfigFromFlags("", path)
	must(t, err, "read the kubeconfig")
	cs, _ := clientsFromConfig(t, kc)
	u := whoAmI(t, cs)
	if u.Username != res.Username {
		t.Errorf("the kubeconfig authenticates as %q, want %q", u.Username, res.Username)
	}
	raw, err := os.ReadFile(path)
	must(t, err, "read the file")
	if kc.Host != cfg.Host || (cfg.BearerToken != "" && bytes.Contains(raw, []byte(cfg.BearerToken))) || bytes.Contains(raw, []byte("system:masters")) {
		t.Errorf("the kubeconfig is not just the server and the identity's token:\n%s", sanitised(raw, kc.BearerToken))
	}
	// Neither the result nor the log holds the token.
	enc, _ := json.Marshal(res)
	if kc.BearerToken == "" || bytes.Contains(enc, []byte(kc.BearerToken)) || bytes.Contains(log.Bytes(), []byte(kc.BearerToken)) {
		t.Errorf("the token is missing from the file, or is in the result or the log")
	}

	// A second add of the name is refused, and the file is as it was: bytes and modification time.
	fi1, err := os.Stat(path)
	must(t, err, "stat")
	_, err = launcher.IssueIdentity(bg(), cfg, dir, name, false, &log)
	if !errors.Is(err, launcher.ErrIdentityExists) || !strings.Contains(err.Error(), "renew") {
		t.Errorf("a second add: %v, want a refusal that says to use renew", err)
	}
	raw2, _ := os.ReadFile(path)
	fi2, _ := os.Stat(path)
	if !bytes.Equal(raw, raw2) || !fi1.ModTime().Equal(fi2.ModTime()) {
		t.Errorf("a refused add changed the kubeconfig: same bytes %v, modification times %s and %s", bytes.Equal(raw, raw2), fi1.ModTime(), fi2.ModTime())
	}

	// Renew: a different token for the same identity, a new expiry from the API's answer, the same
	// modes. The old token stays valid until it expires, as the command says.
	res2, err := launcher.IssueIdentity(bg(), cfg, dir, name, true, &log)
	must(t, err, "renew")
	kc2, err := clientcmd.BuildConfigFromFlags("", path)
	must(t, err, "read the renewed kubeconfig")
	if kc2.BearerToken == kc.BearerToken || kc2.BearerToken == "" || !res2.Renewed {
		t.Errorf("renew wrote the same token, or none, or did not say it renewed: %+v", res2)
	}
	if res2.Expires.Before(res.Expires) || res2.Expires.Before(time.Now().Add(month).Add(-time.Minute)) {
		t.Errorf("the renewed expiry %s is not 30 days from the renewal (first %s)", res2.Expires, res.Expires)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("after renew the file: %v, %v, want mode 600", fi, err)
	}
	for label, token := range map[string]string{"new": kc2.BearerToken, "old": kc.BearerToken} {
		cc := *kc
		cc.BearerToken = token
		cs, _ := clientsFromConfig(t, &cc)
		if got := whoAmI(t, cs).Username; got != res.Username {
			t.Errorf("the %s token authenticates as %q, want %q", label, got, res.Username)
		}
	}
	if bytes.Contains(log.Bytes(), []byte(kc2.BearerToken)) {
		t.Errorf("the log holds the renewed token")
	}

	// Renew of a name with no ServiceAccount is refused and writes nothing.
	ghost := uniq("agent-ghost")
	if _, err := launcher.IssueIdentity(bg(), cfg, dir, ghost, true, &log); !errors.Is(err, launcher.ErrNoIdentity) {
		t.Errorf("renew of %s: %v, want a refusal", ghost, err)
	}
	if _, err := os.Stat(filepath.Join(dir, ghost+".kubeconfig")); err == nil {
		t.Errorf("a refused renew wrote a kubeconfig")
	}
	// A name that is not a DNS-1123 label is refused before anything is asked of the API server.
	for _, bad := range []string{"", "Agent", "a_b", "a.b", "../x", strings.Repeat("a", 64)} {
		if _, err := launcher.IssueIdentity(bg(), cfg, dir, bad, false, &log); !errors.Is(err, launcher.ErrIdentityName) {
			t.Errorf("add of %q: %v, want ErrIdentityName", bad, err)
		}
	}

	// An add that fails after it made the ServiceAccount does not leave it behind: the kubeconfig's
	// directory cannot be made under a file, so the write fails after the token was issued.
	blocker := filepath.Join(t.TempDir(), "blocker")
	must(t, os.WriteFile(blocker, nil, 0o600), "write the blocker")
	failing := uniq("agent-fail")
	if _, err := launcher.IssueIdentity(bg(), cfg, filepath.Join(blocker, "identities"), failing, false, &log); err == nil {
		t.Errorf("an add into a directory that cannot be made succeeded")
	}
	if _, err := adminCS.CoreV1().ServiceAccounts(agentsNamespace).Get(bg(), failing, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("after the failed add the ServiceAccount %s: %v, want it gone, so that a retry is an add", failing, err)
	}
}

// sanitised hides the token in a kubeconfig that a failure message prints.
func sanitised(raw []byte, token string) string {
	if token == "" {
		return string(raw)
	}
	return strings.ReplaceAll(string(raw), token, "<token>")
}

// Status's side: each identity's expiry (from the token) and validity (from the API server), where a
// rejected token is a state, a file or a ServiceAccount that is missing is a state, a kubeconfig that
// names another server is never used, and a failure to reach the API server is an error.
func TestIdentityStatus(t *testing.T) {
	installAgentRoles(t)
	adminCS, _ := clientsFromConfig(t, cfg)
	dir := t.TempDir()
	ports := launcher.Ports{HTTPS: httpsPort(t, cfg)}
	var all []string
	issue := func(prefix string) (string, *launcher.IdentityResult) {
		name := uniq(prefix)
		res, err := launcher.IssueIdentity(bg(), cfg, dir, name, false, bytes.NewBuffer(nil))
		must(t, err, "issue "+name)
		all = append(all, name)
		return name, res
	}
	t.Cleanup(func() {
		for _, n := range all {
			_ = adminCS.CoreV1().ServiceAccounts(agentsNamespace).Delete(bg(), n, metav1.DeleteOptions{})
		}
	})
	good, goodRes := issue("agent-good")
	gone, _ := issue("agent-gone")
	wrong, _ := issue("agent-wrong")
	nofile := uniq("agent-nofile")
	all = append(all, nofile)
	_, err := adminCS.CoreV1().ServiceAccounts(agentsNamespace).Create(bg(), serviceAccount(nofile), metav1.CreateOptions{})
	must(t, err, "create a ServiceAccount no file belongs to")
	// agent-gone: its ServiceAccount is deleted, so the API server rejects its token.
	must(t, adminCS.CoreV1().ServiceAccounts(agentsNamespace).Delete(bg(), gone, metav1.DeleteOptions{}), "delete a ServiceAccount")
	// agent-wrong: its kubeconfig names a server that is not this host's.
	wrongPath := filepath.Join(dir, wrong+".kubeconfig")
	kc, err := clientcmd.LoadFromFile(wrongPath)
	must(t, err, "read the kubeconfig to point elsewhere")
	for _, cl := range kc.Clusters {
		cl.Server = "https://127.0.0.1:1"
	}
	must(t, clientcmd.WriteToFile(*kc, wrongPath), "point the kubeconfig elsewhere")

	var states []launcher.IdentityState
	eventually(t, 60*time.Second, "the deleted ServiceAccount's token to be rejected", func() (bool, error) {
		var err error
		if states, err = launcher.CheckIdentities(bg(), cfg, dir, ports); err != nil {
			return false, err
		}
		for _, s := range states {
			if s.Name == gone {
				return s.Detail != "" && strings.Contains(s.Detail, "rejected"), nil
			}
		}
		return false, nil
	})
	byName := map[string]launcher.IdentityState{}
	for _, s := range states {
		byName[s.Name] = s
	}
	if g := byName[good]; !g.Valid || g.Detail != "" || g.Expires == nil || !g.Expires.Equal(goodRes.Expires) {
		t.Errorf("the good identity: %+v, want valid with the expiry %s that issuing reported", g, goodRes.Expires)
	}
	if g := byName[gone]; g.Valid || !strings.Contains(g.Detail, "no ServiceAccount "+gone) || !strings.Contains(g.Detail, "rejected the token (401)") || g.Expires == nil {
		t.Errorf("the identity whose ServiceAccount is gone: %+v, want not valid, no ServiceAccount, a 401 and an expiry", g)
	}
	if g := byName[wrong]; g.Valid || !strings.Contains(g.Detail, "not at this launcher's") {
		t.Errorf("the identity whose kubeconfig names another server: %+v, want not valid and refused as another server", g)
	}
	if g := byName[nofile]; g.Valid || g.Kubeconfig != "" || !strings.Contains(g.Detail, "no kubeconfig file") {
		t.Errorf("the ServiceAccount with no file: %+v, want not valid, no kubeconfig and a detail saying so", g)
	}

	// A failure to reach the API server is an error, not a list of identities that are not valid.
	dead := *cfg
	dead.Host = "https://127.0.0.1:1"
	if states, err := launcher.CheckIdentities(bg(), &dead, dir, ports); err == nil {
		t.Errorf("CheckIdentities with no API server: %v, want an error", states)
	}
}
