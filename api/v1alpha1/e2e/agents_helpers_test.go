package e2e

// What the agents' RBAC tests share (issue #589, PR 2; HBEP-10, Identities and roles): the agent
// identities, produced by the launcher's own identity function and never by hand, a client for each,
// the question asked of the API server about one access, and the expected matrix.
//
// The expected matrix is written here by hand from the issue's role description and its "agents never
// get" list. It is never read from config/rbac/agents.yaml: a test that took its expectation from the
// manifest under test would agree with it whatever it held.

import (
	"io"
	"net/url"
	"slices"
	"strconv"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

const (
	// Written out, not taken from the launcher's constants: the test states what the brief says.
	agentsNamespace = "hpcburst-agents"
	agentsGroup     = "system:serviceaccounts:hpcburst-agents"
	frameworkGroup  = "hpcburst.groundnuty.github.io"
)

// agent is one identity, made by launcher.IssueIdentity, with the clients its own kubeconfig gives.
type agent struct {
	name       string
	kubeconfig string
	cfg        *rest.Config
	cs         kubernetes.Interface
	c          client.Client
}

func (a *agent) username() string { return "system:serviceaccount:" + agentsNamespace + ":" + a.name }

// clientsFromConfig makes the clientset and the controller-runtime client for cfg.
func clientsFromConfig(t *testing.T, cfg *rest.Config) (kubernetes.Interface, client.Client) {
	t.Helper()
	cfg = rest.CopyConfig(cfg)
	cfg.QPS, cfg.Burst = 200, 400
	cs, err := kubernetes.NewForConfig(cfg)
	must(t, err, "clientset")
	cl, err := client.New(cfg, client.Options{Scheme: scheme()})
	must(t, err, "client")
	return cs, cl
}

// installAgentRoles installs the namespace, roles and bindings through the launcher's own function,
// as up does, and sets them back to the manifest when the test ends.
func installAgentRoles(t *testing.T) {
	t.Helper()
	must(t, launcher.InstallAgentRBAC(bg(), cfg, io.Discard), "install the agents' roles")
	t.Cleanup(func() {
		if err := launcher.InstallAgentRBAC(bg(), cfg, io.Discard); err != nil {
			t.Errorf("clean-up: install the agents' roles again: %v", err)
		}
	})
}

// newAgents installs the roles and makes one identity per name with the launcher's identity function
// (the kubeconfigs are written under a directory of the test), and removes the ServiceAccounts at the
// end. Names get a unique suffix, so a host that other tests used has no clash.
func newAgents(t *testing.T, prefixes ...string) []*agent {
	t.Helper()
	installAgentRoles(t)
	dir := t.TempDir()
	adminCS, _ := clientsFromConfig(t, cfg)
	var out []*agent
	for _, p := range prefixes {
		name := uniq(p)
		res, err := launcher.IssueIdentity(bg(), cfg, dir, name, false, io.Discard)
		must(t, err, "issue the identity "+name)
		t.Cleanup(func() {
			_ = adminCS.CoreV1().ServiceAccounts(agentsNamespace).Delete(bg(), name, metav1.DeleteOptions{})
		})
		kc, err := clientcmd.BuildConfigFromFlags("", res.Kubeconfig)
		must(t, err, "read the kubeconfig of "+name)
		a := &agent{name: name, kubeconfig: res.Kubeconfig, cfg: kc}
		a.cs, a.c = clientsFromConfig(t, kc)
		out = append(out, a)
	}
	return out
}

// whoAmI asks the API server who the clientset's credential is (SelfSubjectReview).
func whoAmI(t *testing.T, cs kubernetes.Interface) authenticationv1.UserInfo {
	t.Helper()
	r, err := cs.AuthenticationV1().SelfSubjectReviews().Create(bg(), &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	must(t, err, "SelfSubjectReview")
	return r.Status.UserInfo
}

// requireAgentIdentity fails unless the API server reports the clientset as the agent's
// ServiceAccount, in the agents' group: no row below is then judged as the admin or an outsider.
func requireAgentIdentity(t *testing.T, a *agent) {
	t.Helper()
	u := whoAmI(t, a.cs)
	if u.Username != a.username() || !slices.Contains(u.Groups, agentsGroup) {
		t.Fatalf("the kubeconfig of %s authenticates as %q in groups %v, want %q in %q", a.name, u.Username, u.Groups, a.username(), agentsGroup)
	}
}

// accessRow is one question to the API server: may this credential do verb on the resource?
type accessRow struct {
	group, resource, subresource, namespace, verb string
	agentMay                                      bool
}

func (r accessRow) String() string {
	res := r.resource
	if r.subresource != "" {
		res += "/" + r.subresource
	}
	g := r.group
	if g == "" {
		g = "core"
	}
	return r.verb + " " + res + " (" + g + ", namespace '" + r.namespace + "')"
}

// ask makes a SelfSubjectAccessReview from cs's own credential.
func ask(cs kubernetes.Interface, r accessRow) (bool, error) {
	res, err := cs.AuthorizationV1().SelfSubjectAccessReviews().Create(bg(), &authorizationv1.SelfSubjectAccessReview{
		Spec: authorizationv1.SelfSubjectAccessReviewSpec{ResourceAttributes: &authorizationv1.ResourceAttributes{
			Namespace: r.namespace, Verb: r.verb, Group: r.group, Resource: r.resource, Subresource: r.subresource,
		}},
	}, metav1.CreateOptions{})
	if err != nil {
		return false, err
	}
	return res.Status.Allowed, nil
}

var allVerbs = []string{"get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"}

// expectedAgentMatrix is the matrix of what an agent may do, by hand from the issue. Every resource
// is asked about with every verb of allVerbs; the verbs listed are the only ones allowed.
func expectedAgentMatrix() []accessRow {
	var rows []accessRow
	add := func(group, resource, sub, ns string, allowed ...string) {
		for _, v := range allVerbs {
			rows = append(rows, accessRow{group, resource, sub, ns, v, slices.Contains(allowed, v)})
		}
	}
	read := []string{"get", "list", "watch"}
	const G = frameworkGroup

	// hpcburst-agent-read: the framework's cluster-scoped kinds, read only. approvalpolicies is
	// written by the operator role, which no agent has.
	for _, r := range []string{"providers", "platformprofiles", "approvalpolicies"} {
		add(G, r, "", "", read...)
	}
	// hpcburst-agent: read the namespaced kinds, create ModelDeployments, and nothing else. No update,
	// patch, scale or write of an Operation or an Approval: those come with PR 3's admission policies.
	add(G, "modeldeployments", "", "default", "get", "list", "watch", "create")
	for _, r := range []string{"allocations", "instances", "operations", "approvals"} {
		add(G, r, "", "default", read...)
	}
	for _, r := range []string{"modeldeployments", "allocations", "instances", "operations", "approvals"} {
		add(G, r, "status", "default") // never any */status
	}
	add(G, "modeldeployments", "scale", "default") // the scale subresource lowers capacity
	add("apps", "controllerrevisions", "", "default", read...)
	add("", "events", "", "default", read...)
	add("events.k8s.io", "events", "", "default", read...)
	add("coordination.k8s.io", "leases", "", "default", read...)

	// The role binds cluster-wide, so another namespace and all of them are covered too.
	rows = append(rows,
		accessRow{G, "modeldeployments", "", "", "list", true},
		accessRow{G, "allocations", "", "kube-system", "get", true},
		accessRow{G, "modeldeployments", "", "kube-system", "create", true},
		accessRow{G, "allocations", "", "kube-system", "delete", false},
	)

	// What agents never get.
	add("", "serviceaccounts", "", agentsNamespace)
	add("", "serviceaccounts", "", "default")
	add("", "serviceaccounts", "token", agentsNamespace)
	add("", "serviceaccounts", "token", "default")
	add("", "secrets", "", "default")
	add("", "secrets", "", agentsNamespace)
	add("certificates.k8s.io", "certificatesigningrequests", "", "")
	add("", "namespaces", "", "")
	for _, r := range []string{"clusterroles", "clusterrolebindings"} {
		add("rbac.authorization.k8s.io", r, "", "")
	}
	for _, r := range []string{"roles", "rolebindings"} {
		add("rbac.authorization.k8s.io", r, "", agentsNamespace)
	}
	for _, r := range []string{"validatingadmissionpolicies", "validatingadmissionpolicybindings", "validatingwebhookconfigurations", "mutatingwebhookconfigurations"} {
		add("admissionregistration.k8s.io", r, "", "")
	}
	add("apiextensions.k8s.io", "customresourcedefinitions", "", "")
	for _, r := range []struct{ group, resource string }{{"", "users"}, {"", "groups"}, {"", "serviceaccounts"}, {"authentication.k8s.io", "userextras"}} {
		rows = append(rows, accessRow{r.group, r.resource, "", "", "impersonate", false})
	}
	return rows
}

// judge asks every row of a credential and fails each row whose answer is not want(row). An error from
// the API server is a failure of its own, never a "no".
func judge(t *testing.T, label string, cs kubernetes.Interface, rows []accessRow, want func(accessRow) bool) {
	t.Helper()
	bad := 0
	for _, r := range rows {
		got, err := ask(cs, r)
		switch {
		case err != nil:
			t.Errorf("%s: %s: the access review failed: %v", label, r, err)
			bad++
		case got != want(r):
			t.Errorf("%s: %s: the API server says allowed=%v, want %v", label, r, got, want(r))
			bad++
		}
		if bad >= 20 {
			t.Fatalf("%s: stopping after %d wrong rows", label, bad)
		}
	}
	t.Logf("%s: %d rows judged by the API server, %d wrong", label, len(rows), bad)
}

// waitUntilAllowed polls one row until the API server allows it. The RBAC authorizer learns of a
// role or a binding after the write returns, so an "allowed" is the evidence that it has learned, and a
// "denied" judged before it would pass for the wrong reason.
func waitUntilAllowed(t *testing.T, cs kubernetes.Interface, r accessRow) {
	t.Helper()
	eventually(t, 60*time.Second, "the authorizer to allow "+r.String(), func() (bool, error) { return ask(cs, r) })
}

// waitUntilDenied polls one row until the API server denies it, after a role or a binding was made
// narrower.
func waitUntilDenied(t *testing.T, cs kubernetes.Interface, r accessRow) {
	t.Helper()
	eventually(t, 60*time.Second, "the authorizer to deny "+r.String(), func() (bool, error) {
		ok, err := ask(cs, r)
		return !ok && err == nil, err
	})
}

// waitForAgentMatrix waits until every row an agent may do is allowed, which proves that both agent
// roles and their bindings are visible to the authorizer, so a denied row is judged after that.
func waitForAgentMatrix(t *testing.T, cs kubernetes.Interface, rows []accessRow) {
	t.Helper()
	for _, r := range rows {
		if r.agentMay {
			waitUntilAllowed(t, cs, r)
		}
	}
}

// httpsPort is the port of cfg's server.
func httpsPort(t *testing.T, c *rest.Config) int {
	t.Helper()
	u, err := url.Parse(c.Host)
	must(t, err, "parse the server address")
	p, err := strconv.Atoi(u.Port())
	must(t, err, "the server's port")
	return p
}

// tokenFor asks the API server, as the admin, for a short token of a ServiceAccount, and returns a
// clientset that uses it. It is the control's credential, made without the launcher's identity function.
func tokenClient(t *testing.T, ns, sa string) (kubernetes.Interface, client.Client) {
	t.Helper()
	adminCS, _ := clientsFromConfig(t, cfg)
	_, err := adminCS.CoreV1().ServiceAccounts(ns).Create(bg(), serviceAccount(sa), metav1.CreateOptions{})
	must(t, err, "create the ServiceAccount "+ns+"/"+sa)
	tr, err := adminCS.CoreV1().ServiceAccounts(ns).CreateToken(bg(), sa, tokenRequest(3600), metav1.CreateOptions{})
	must(t, err, "request a token for "+ns+"/"+sa)
	tc := rest.AnonymousClientConfig(cfg) // the server and its TLS settings, with none of the admin's credential
	tc.BearerToken = tr.Status.Token
	return clientsFromConfig(t, tc)
}

func tokenRequest(seconds int64) *authenticationv1.TokenRequest {
	return &authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: &seconds}}
}

func serviceAccount(name string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name}}
}
