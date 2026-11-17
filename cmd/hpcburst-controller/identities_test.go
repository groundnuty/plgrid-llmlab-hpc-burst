package main

// The controllers run under their own identities in every suite of this package (HBEP-10, Audit; issue #660,
// PR A). The harness (run_test.go) installs the roles of config/rbac/controllers.yaml and mints a token for
// each ServiceAccount with launcher.PrepareControllerIdentities, the function hpcburst up runs, and starts the
// binary on --kubeconfig and --provider-kubeconfig. This file holds what makes that a PROOF and not a run on the
// admin credential, which RBAC never checks:
//   - each identity's kubeconfig is asked who it is (a SelfSubjectReview through that very kubeconfig);
//   - the API server writes an audit log of the three identities' requests, and a request answered 403 fails
//     the test that made it, even when the controller retried or logged it and went on: the suites passing
//     says the roles are enough, the log saying no 403 says no verb was missing on a path the suite did not
//     wait for;
//   - the negative rows: an identity is refused a verb outside its role (so the roles are not wildcards in
//     effect), with the control that an identity whose role has the verb is not.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	authenticationv1 "k8s.io/api/authentication/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

// negativeRowAgent is the user agent of the requests that the negative rows make ON PURPOSE to be refused: the
// audit check lets those through, and no other.
const negativeRowAgent = "hpcburst-rbac-negative-row"

// auditPolicy logs, at Metadata level, the requests of the three controller identities, and nothing else.
const controllerAuditPolicy = `apiVersion: audit.k8s.io/v1
kind: Policy
omitStages: ["RequestReceived"]
rules:
  - level: Metadata
    users:
      - system:serviceaccount:hpcburst-system:hpcburst-lifecycle
      - system:serviceaccount:hpcburst-system:hpcburst-provider
      - system:serviceaccount:hpcburst-system:hpcburst-front
  - level: None
`

// configureControllerAudit makes env's API server write the audit log of the controller identities under dir,
// and returns the log's path.
func configureControllerAudit(t testing.TB, env *envtest.Environment, dir string) string {
	t.Helper()
	policy, log := filepath.Join(dir, "audit-policy.yaml"), filepath.Join(dir, "audit.log")
	if err := os.WriteFile(policy, []byte(controllerAuditPolicy), 0o600); err != nil {
		t.Fatal(err)
	}
	env.ControlPlane.GetAPIServer().Configure().Set("audit-policy-file", policy).Set("audit-log-path", log)
	return log
}

// auditEvent is the part of an audit.k8s.io/v1 event these tests read.
type auditEvent struct {
	Stage     string `json:"stage"`
	Verb      string `json:"verb"`
	UserAgent string `json:"userAgent"`
	User      struct {
		Username string `json:"username"`
	} `json:"user"`
	ObjectRef struct {
		APIGroup    string `json:"apiGroup"`
		Resource    string `json:"resource"`
		Subresource string `json:"subresource"`
		Namespace   string `json:"namespace"`
		Name        string `json:"name"`
	} `json:"objectRef"`
	ResponseStatus struct {
		Code int `json:"code"`
	} `json:"responseStatus"`
}

func (e auditEvent) String() string {
	r := e.ObjectRef.Resource
	if e.ObjectRef.Subresource != "" {
		r += "/" + e.ObjectRef.Subresource
	}
	return fmt.Sprintf("%s %s.%s %s/%s by %s (%s)", e.Verb, r, e.ObjectRef.APIGroup, e.ObjectRef.Namespace, e.ObjectRef.Name, e.User.Username, e.UserAgent)
}

// readAuditEvents reads the audit log file; a file that is not there yet is empty, since the API server opens
// it at its first event. A line that is not a JSON event fails the test.
func readAuditEvents(t testing.TB, path string) []auditEvent {
	t.Helper()
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("audit log: %v", err)
	}
	var out []auditEvent
	sc := bufio.NewScanner(bytes.NewReader(b))
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for n := 1; sc.Scan(); n++ {
		var e auditEvent
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("audit log line %d is not a JSON event: %v", n, err)
		}
		out = append(out, e)
	}
	return out
}

// refusedEvents are the requests of the controller identities that were answered 403, except the negative rows'.
func refusedEvents(evs []auditEvent) []auditEvent {
	var out []auditEvent
	for _, e := range evs {
		if e.ResponseStatus.Code == 403 && strings.HasPrefix(e.User.Username, "system:serviceaccount:hpcburst-system:") && e.UserAgent != negativeRowAgent {
			out = append(out, e)
		}
	}
	return out
}

// assertNoControllerRequestRefused fails the test for each request a controller identity made that the API server
// refused: a verb the role lacks, whatever the controller did with the answer.
func assertNoControllerRequestRefused(t testing.TB, auditLog string) {
	t.Helper()
	evs := readAuditEvents(t, auditLog)
	recordRoleCoverage(t, evs)
	refused := refusedEvents(evs)
	for i, e := range refused {
		if i == 20 {
			t.Errorf("... and %d more refused requests", len(refused)-i)
			break
		}
		t.Errorf("a controller identity was REFUSED (403), so its role lacks what the code calls: %s", e)
	}
}

// controllerIdentities makes the controller identities ready under state exactly as hpcburst up does, and asks the
// API server who each kubeconfig is, through that kubeconfig. It returns the lifecycle's and the Provider
// managers' kubeconfigs, the two the controller binary reads. The roles are installed and the tokens issued once
// per API server, by the launcher's own function; a later state directory of the same API server gets the same
// three files (the same tokens, which are valid for the whole server), so that a suite that prepares many
// state directories does not repeat fifteen applies and three token requests in each.
func (api *apiServer) controllerIdentities(t testing.TB, state string) (lifecycle, provider string) {
	t.Helper()
	api.idMu.Lock()
	defer api.idMu.Unlock()
	if api.idFiles == nil {
		ids, err := launcher.PrepareControllerIdentities(context.Background(), api.cfg, state, io.Discard)
		if err != nil {
			t.Fatalf("make the controller identities ready: %v", err)
		}
		if len(ids) != 3 {
			t.Fatalf("%d controller identities were issued, want 3", len(ids))
		}
		api.idFiles = map[string][]byte{}
		for _, id := range ids {
			if got := whoAmI(t, id.Kubeconfig, ""); got != id.Username || got != controllerSA+id.Name {
				t.Fatalf("the kubeconfig of %s authenticates as %q, want %q", id.Name, got, id.Username)
			}
			b, err := os.ReadFile(id.Kubeconfig)
			if err != nil {
				t.Fatal(err)
			}
			api.idFiles[id.Name] = b
		}
	} else {
		dir := filepath.Dir(launcher.ControllerKubeconfig(state, launcher.ControllerLifecycleIdentity))
		for _, d := range []string{filepath.Dir(dir), dir} {
			if err := os.MkdirAll(d, 0o700); err != nil {
				t.Fatal(err)
			}
		}
		for name, b := range api.idFiles {
			if err := os.WriteFile(launcher.ControllerKubeconfig(state, name), b, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	return launcher.ControllerKubeconfig(state, launcher.ControllerLifecycleIdentity), launcher.ControllerKubeconfig(state, launcher.ControllerProviderIdentity)
}

// restFor reads a kubeconfig file into a client configuration, under userAgent when one is given.
func restFor(t testing.TB, path, userAgent string) *rest.Config {
	t.Helper()
	cfg, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		t.Fatal(err)
	}
	if userAgent != "" {
		cfg.UserAgent = userAgent
	}
	return cfg
}

// whoAmI is the user name the API server reports for the kubeconfig at path: a SelfSubjectReview made with it,
// never with the harness's own client, which is the administrator.
func whoAmI(t testing.TB, path, userAgent string) string {
	t.Helper()
	cs, err := kubernetes.NewForConfig(restFor(t, path, userAgent))
	if err != nil {
		t.Fatal(err)
	}
	review, err := cs.AuthenticationV1().SelfSubjectReviews().Create(context.Background(), &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("a SelfSubjectReview with %s: %v", path, err)
	}
	return review.Status.UserInfo.Username
}

// TestControllerIdentitiesAreWhoTheyClaimAndNothingElseCan: each of the three kubeconfigs authenticates as its own
// ServiceAccount of hpcburst-system, none as the administrator, and the admin client is not any of them. The
// negative rows follow: a verb outside a role is refused (HTTP 403, Forbidden by RBAC), for a verb that ANOTHER
// controller's role grants, so the refusal is the role's and not the verb's being one no role could hold; the
// control is that the identity whose role has the verb does it. The API server's audit log file records the
// refusals under the refused identity's name, which also shows that the audit check reads something.
func TestControllerIdentitiesAreWhoTheyClaimAndNothingElseCan(t *testing.T) {
	api := startAPIServer(t)
	ctx := context.Background()
	state := t.TempDir()
	lifecycleKC, providerKC := api.controllerIdentities(t, state)
	frontKC := launcher.ControllerKubeconfig(state, launcher.ControllerFrontIdentity)
	for name, kc := range map[string]string{"hpcburst-lifecycle": lifecycleKC, "hpcburst-provider": providerKC, "hpcburst-front": frontKC} {
		if got, want := whoAmI(t, kc, ""), "system:serviceaccount:hpcburst-system:"+name; got != want {
			t.Errorf("%s: the kubeconfig authenticates as %q, want %q", name, got, want)
		}
	}
	adminReview, err := kubernetes.NewForConfig(api.cfg)
	if err != nil {
		t.Fatal(err)
	}
	rv, err := adminReview.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil || strings.HasPrefix(rv.Status.UserInfo.Username, "system:serviceaccount:") {
		t.Fatalf("the harness's own client is %v (%v), want the administrator: the check that the identities differ from it proves nothing otherwise", rv, err)
	}

	// An Allocation to refuse a delete of, made by the administrator.
	alloc := batchAllocation("rbac-row", "default", "rbac-row-provider", partX86, strings.Repeat("a", 64))
	if err := api.client.Create(ctx, alloc); err != nil {
		t.Fatalf("create the Allocation the rows act on: %v", err)
	}
	as := func(path string) client.Client {
		c, err := client.New(restFor(t, path, negativeRowAgent), client.Options{Scheme: api.client.Scheme()})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	frontC, providerC, lifecycleC := as(frontKC), as(providerKC), as(lifecycleKC)

	// hpcburst-front deleting an Allocation: Forbidden by RBAC; the lifecycle role grants that very verb.
	err = frontC.Delete(ctx, alloc.DeepCopy())
	if !apierrors.IsForbidden(err) {
		t.Fatalf("hpcburst-front deleted an Allocation, or was refused otherwise than by RBAC: %v", err)
	}
	if !strings.Contains(err.Error(), "hpcburst-front") {
		t.Errorf("the refusal does not name the identity: %v", err)
	}
	if err := api.client.Get(ctx, client.ObjectKeyFromObject(alloc), &v1alpha1.Allocation{}); err != nil {
		t.Fatalf("the Allocation is gone after the front's refused delete: %v", err)
	}
	// hpcburst-provider deleting one: the same, though it may update one.
	if err := providerC.Delete(ctx, alloc.DeepCopy()); !apierrors.IsForbidden(err) {
		t.Errorf("hpcburst-provider deleting an Allocation: %v, want Forbidden", err)
	}
	// hpcburst-provider creating an Allocation: the lifecycle role grants create, the provider's does not.
	extra := batchAllocation("rbac-row-created-by-provider", "default", "rbac-row-provider", partX86, strings.Repeat("a", 64))
	if err := providerC.Create(ctx, extra); !apierrors.IsForbidden(err) {
		t.Errorf("hpcburst-provider creating an Allocation: %v, want Forbidden", err)
	}
	// hpcburst-lifecycle patching an Allocation's status: the provider role grants that, the lifecycle's does not.
	if err := lifecycleC.Status().Patch(ctx, alloc.DeepCopy(), client.RawPatch("application/merge-patch+json", []byte(`{"status":{}}`))); !apierrors.IsForbidden(err) {
		t.Errorf("hpcburst-lifecycle patching an Allocation's status: %v, want Forbidden", err)
	}
	// hpcburst-front reading a Secret, and writing a Provider: nothing grants either.
	if err := frontC.List(ctx, &corev1.SecretList{}); !apierrors.IsForbidden(err) {
		t.Errorf("hpcburst-front listing Secrets: %v, want Forbidden", err)
	}
	if err := frontC.Create(ctx, &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: "rbac-row-provider"}}); !apierrors.IsForbidden(err) {
		t.Errorf("hpcburst-front creating a Provider: %v, want Forbidden", err)
	}

	// A Lease write in ANOTHER namespace: every controller's Lease verbs are a Role of hpcburst-system, so the same
	// create in default, and an update of a Lease that exists there, are Forbidden; the control is the create
	// in hpcburst-system, which each identity is allowed.
	existing := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "rbac-row-existing", Namespace: "default"}}
	if err := api.client.Create(ctx, existing); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]client.Client{"hpcburst-lifecycle": lifecycleC, "hpcburst-provider": providerC, "hpcburst-front": frontC} {
		elsewhere := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "rbac-row-" + name, Namespace: "default"}}
		if err := c.Create(ctx, elsewhere); !apierrors.IsForbidden(err) {
			t.Errorf("%s creating a Lease in default: %v, want Forbidden", name, err)
		}
		if err := c.Update(ctx, existing.DeepCopy()); !apierrors.IsForbidden(err) {
			t.Errorf("%s updating a Lease in default: %v, want Forbidden", name, err)
		}
		if err := c.Get(ctx, client.ObjectKeyFromObject(existing), &coordinationv1.Lease{}); !apierrors.IsForbidden(err) {
			t.Errorf("%s reading a Lease in default: %v, want Forbidden", name, err)
		}
		own := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "rbac-row-" + name, Namespace: v1alpha1.ControllerNamespace}}
		if err := c.Create(ctx, own); err != nil {
			t.Errorf("%s creating a Lease in hpcburst-system, where its Role grants it: %v", name, err)
		}
	}

	// By NAME, in hpcburst-system: the lifecycle's and the front's Leases have fixed names, and their roles name them,
	// so the front's token cannot take the lifecycle Lease, nor the lifecycle's the front's, which it only reads. The
	// controls: each identity does what its role says to its own Lease. (The Provider managers' Leases are one per
	// Provider, made at run time, so that role is not narrowed by name: nothing is asserted of it here.)
	for _, n := range []string{v1alpha1.LeaseLifecycle, v1alpha1.LeaseFront} {
		if err := api.client.Create(ctx, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: n, Namespace: v1alpha1.ControllerNamespace}}); err != nil {
			t.Fatal(err)
		}
	}
	leaseKey := func(n string) client.ObjectKey {
		return client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: n}
	}
	var lifecycleLease, frontLease coordinationv1.Lease
	if err := api.client.Get(ctx, leaseKey(v1alpha1.LeaseLifecycle), &lifecycleLease); err != nil {
		t.Fatal(err)
	}
	if err := api.client.Get(ctx, leaseKey(v1alpha1.LeaseFront), &frontLease); err != nil {
		t.Fatal(err)
	}
	if err := frontC.Update(ctx, lifecycleLease.DeepCopy()); !apierrors.IsForbidden(err) {
		t.Errorf("hpcburst-front updating the lifecycle Lease: %v, want Forbidden", err)
	}
	if err := frontC.Get(ctx, leaseKey(v1alpha1.LeaseLifecycle), &coordinationv1.Lease{}); !apierrors.IsForbidden(err) {
		t.Errorf("hpcburst-front reading the lifecycle Lease: %v, want Forbidden", err)
	}
	if err := lifecycleC.Update(ctx, frontLease.DeepCopy()); !apierrors.IsForbidden(err) {
		t.Errorf("hpcburst-lifecycle updating the front's Lease: %v, want Forbidden", err)
	}
	var got coordinationv1.Lease
	if err := frontC.Get(ctx, leaseKey(v1alpha1.LeaseFront), &got); err != nil {
		t.Errorf("hpcburst-front reading its own Lease, the control: %v", err)
	} else if err := frontC.Update(ctx, got.DeepCopy()); err != nil {
		t.Errorf("hpcburst-front updating its own Lease, the control: %v", err)
	}
	if err := lifecycleC.Get(ctx, leaseKey(v1alpha1.LeaseFront), &coordinationv1.Lease{}); err != nil {
		t.Errorf("hpcburst-lifecycle reading the front's Lease, which its observer does: %v", err)
	}
	if err := lifecycleC.Get(ctx, leaseKey(v1alpha1.LeaseLifecycle), &got); err != nil {
		t.Errorf("hpcburst-lifecycle reading its own Lease, the control: %v", err)
	} else if err := lifecycleC.Update(ctx, got.DeepCopy()); err != nil {
		t.Errorf("hpcburst-lifecycle updating its own Lease, the control: %v", err)
	}

	// The control: the lifecycle identity, whose role grants delete, deletes it. A refusal above that came from a
	// missing object or an admission rule, and not from RBAC, would have shown here as a different answer.
	if err := lifecycleC.Delete(ctx, alloc); err != nil {
		t.Fatalf("hpcburst-lifecycle, whose role grants delete on Allocations, was refused: %v", err)
	}

	// The audit log file records the refusals under the refused identity, and the one delete that was allowed
	// under the lifecycle's: so the audit check that runs at the end of every test reads real events, and
	// the user names it sees are the ServiceAccounts', never the administrator's.
	var front403, lifecycleOK bool
	waitUntil(t, 30*time.Second, "the audit log to hold the rows' requests", func() bool {
		front403, lifecycleOK = false, false
		for _, e := range readAuditEvents(t, api.auditLog) {
			if e.Verb == "delete" && e.ObjectRef.Resource == "allocations" && e.ObjectRef.Name == "rbac-row" {
				front403 = front403 || (e.User.Username == "system:serviceaccount:hpcburst-system:hpcburst-front" && e.ResponseStatus.Code == 403)
				lifecycleOK = lifecycleOK || (e.User.Username == "system:serviceaccount:hpcburst-system:hpcburst-lifecycle" && e.ResponseStatus.Code == 200)
			}
		}
		return front403 && lifecycleOK
	})
	for _, e := range readAuditEvents(t, api.auditLog) {
		if strings.Contains(e.User.Username, "admin") || e.User.Username == "" {
			t.Errorf("the audit log holds a request by %q: the policy logs the three identities only", e.User.Username)
		}
	}
}

// TestTheAuditCheckCountsARefusalByAnIdentity: the check that ends every test is not vacuous. A 403 for a controller
// identity is counted, one for another user or with the negative rows' agent is not, and a 200 is not.
func TestTheAuditCheckCountsARefusalByAnIdentity(t *testing.T) {
	mk := func(user, agent string, code int) auditEvent {
		var e auditEvent
		e.User.Username, e.UserAgent, e.ResponseStatus.Code = user, agent, code
		return e
	}
	sa := "system:serviceaccount:hpcburst-system:hpcburst-provider"
	got := refusedEvents([]auditEvent{
		mk(sa, "hpcburst-controller", 403),
		mk(sa, "hpcburst-controller", 200),
		mk(sa, negativeRowAgent, 403),
		mk("system:serviceaccount:hpcburst-agents:agent-a", "kubectl", 403),
		mk("kubernetes-admin", "kubectl", 403),
	})
	if len(got) != 1 || got[0].User.Username != sa || got[0].ResponseStatus.Code != 403 {
		t.Fatalf("the check counted %v, want only the provider's 403", got)
	}
	// A log file: lines are read as events, and a line that is not one is an error of the check.
	path := filepath.Join(t.TempDir(), "audit.log")
	line, _ := json.Marshal(mk(sa, "x", 403))
	if err := os.WriteFile(path, append(line, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if evs := readAuditEvents(t, path); len(evs) != 1 || !slices.Equal([]int{evs[0].ResponseStatus.Code}, []int{403}) {
		t.Fatalf("the file was read as %v", evs)
	}
}

// A binary started by hand keeps its defaults: --kubeconfig is <state-dir>/kubeconfig and the Provider managers
// share the lifecycle manager's configuration; with --provider-kubeconfig they have their own.
func TestProviderKubeconfigFlagAndTheManagersConfig(t *testing.T) {
	base := []string{"--state-dir", "/state", "--port-base", "29220", "--health-addr", "127.0.0.1:29226"}
	f, err := parse(base, io.Discard)
	if err != nil || f.kubeconfig != "/state/kubeconfig" || f.providerKubeconfig != "" {
		t.Fatalf("by hand: %+v, %v; want the default kubeconfig and no provider kubeconfig", f, err)
	}
	f, err = parse(append(slices.Clone(base), "--kubeconfig", "/state/identities/system/hpcburst-lifecycle.kubeconfig", "--provider-kubeconfig", "/state/identities/system/hpcburst-provider.kubeconfig"), io.Discard)
	if err != nil || f.kubeconfig != "/state/identities/system/hpcburst-lifecycle.kubeconfig" || f.providerKubeconfig != "/state/identities/system/hpcburst-provider.kubeconfig" {
		t.Fatalf("with the identities: %+v, %v", f, err)
	}
	lifecycle, provider := &rest.Config{Host: "https://127.0.0.1:1"}, &rest.Config{Host: "https://127.0.0.1:2"}
	if got := (wiring{cfg: lifecycle}).providerConfig(); got != lifecycle {
		t.Error("without --provider-kubeconfig the Provider managers do not share the lifecycle manager's configuration")
	}
	if got := (wiring{cfg: lifecycle, providerCfg: provider}).providerConfig(); got != provider {
		t.Error("with --provider-kubeconfig the Provider managers do not use it")
	}
}

// TestEachManagerOfTheBinaryRunsUnderItsOwnIdentity: the real binary, on the roles of controllers.yaml, with a
// batch Provider on the fake Slurm. The lifecycle manager leads its Lease as hpcburst-lifecycle, the Provider
// manager takes provider-<name> as hpcburst-provider, and neither writes the other's: the API server's audit log
// file, not the test's own client, says who. The end-of-test check adds that no request of either was refused.
func TestEachManagerOfTheBinaryRunsUnderItsOwnIdentity(t *testing.T) {
	api := startAPIServer(t)
	ctx := context.Background()
	c := api.client
	profile := batchProfile("identity-slurm")
	prov := batchProvider("identity-batch", profile.Name, partX86, sshCredentials(t))
	for _, o := range []client.Object{profile, prov} {
		if err := c.Create(ctx, o); err != nil {
			t.Fatal(err)
		}
	}
	provLease := v1alpha1.ProviderLeaseName(prov.Name)
	_, session := newFakeSlurm()
	f, _, _ := prepareState(t, api, "direct", exampleFront)
	var served sync.Map
	b := startBinary(t, &f, deps{providerScheme: providerScheme, newPlatform: recordingPlatforms(session, &served), registry: prometheus.NewRegistry()})
	b.eventually(t, "the lifecycle Lease and "+provLease+" are held and the Provider is served", 90*time.Second, func() bool {
		_, ok := served.Load(prov.Name)
		return ok && held(ctx, c, v1alpha1.LeaseLifecycle) && held(ctx, c, provLease)
	})
	lifecycle, provider := controllerSA+"hpcburst-lifecycle", controllerSA+"hpcburst-provider"
	waitUntil(t, 30*time.Second, "the audit log to show both Leases taken", func() bool {
		evs := readAuditEvents(t, api.auditLog)
		return leaseWrittenBy(evs, v1alpha1.LeaseLifecycle, lifecycle) && leaseWrittenBy(evs, provLease, provider)
	})
	for _, e := range readAuditEvents(t, api.auditLog) {
		if e.ObjectRef.Resource != "leases" {
			continue
		}
		if e.ObjectRef.Name == v1alpha1.LeaseLifecycle && e.User.Username != lifecycle || e.ObjectRef.Name == provLease && e.User.Username != provider {
			t.Errorf("the Lease %s is touched by %q: %s", e.ObjectRef.Name, e.User.Username, e)
		}
	}
	// Both stop with the binary, and the check that no request was refused runs at the end of the test.
	if err := b.stop(t); err != nil {
		t.Errorf("run returned %v", err)
	}
}

// controllerSA is the user-name prefix of a controller identity.
const controllerSA = "system:serviceaccount:hpcburst-system:"

// leaseWrittenBy reports whether the log holds a create or an update of the Lease by user.
func leaseWrittenBy(evs []auditEvent, lease, user string) bool {
	for _, e := range evs {
		if e.ObjectRef.Resource == "leases" && e.ObjectRef.Name == lease && e.User.Username == user && (e.Verb == "create" || e.Verb == "update") && e.ResponseStatus.Code/100 == 2 {
			return true
		}
	}
	return false
}

// roleCoverageEnv names a directory: when it is set, each test that started an API server writes there, as
// <test name>.jsonl, which requests each controller identity made that the API server ANSWERED (2xx), counted by
// user, verb, group, resource and subresource. Run over the whole package it is the evidence of which rule of
// each role some suite really exercises under that identity, and which none does (the PR's coverage table).
// Unset, nothing is written.
const roleCoverageEnv = "HPCBURST_ROLE_COVERAGE_DIR"

func recordRoleCoverage(t testing.TB, evs []auditEvent) {
	dir := os.Getenv(roleCoverageEnv)
	if dir == "" {
		return
	}
	type key struct{ user, verb, group, resource, sub string }
	counts := map[key]int{}
	for _, e := range evs {
		if e.ResponseStatus.Code/100 == 2 && strings.HasPrefix(e.User.Username, controllerSA) {
			counts[key{strings.TrimPrefix(e.User.Username, controllerSA), e.Verb, e.ObjectRef.APIGroup, e.ObjectRef.Resource, e.ObjectRef.Subresource}]++
		}
	}
	f, err := os.OpenFile(filepath.Join(dir, strings.ReplaceAll(t.Name(), "/", "_")+".jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Errorf("role coverage: %v", err)
		return
	}
	defer f.Close()
	for k, n := range counts {
		b, _ := json.Marshal(map[string]any{"test": t.Name(), "user": k.user, "verb": k.verb, "group": k.group, "resource": k.resource, "subresource": k.sub, "n": n})
		fmt.Fprintf(f, "%s\n", b)
	}
}
