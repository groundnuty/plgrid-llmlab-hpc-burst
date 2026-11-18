package main

// The controllers' own identities on a real k3s (HBEP-10, Audit; issue #660, PR A), run by CI's launcher-k3s
// and never locally. The test's ports are #660's block, 28520-28539: the launcher's base 28520 (28520-28532),
// and the front's client listener at 28533.
//
// What it shows, each by its own evidence:
//   - up issues a kubeconfig per controller identity, mode 600 in a directory of mode 700, and each
//     authenticates, through a SelfSubjectReview made WITH that file, as its own ServiceAccount of
//     hpcburst-system, never as the administrator;
//   - the token's life is a year, from the token's own exp claim;
//   - the API server's audit log FILE holds a write made with the lifecycle
//     identity under user.username = the lifecycle ServiceAccount, and the real front's Lease writes under the
//     front's, so the real front process runs on its identity; the administrator's name is on neither;
//   - up writes the embedded audit policy, owner only, and the running k3s logs by it: an agent's Operation create is in the audit log FILE at RequestResponse, under the
//     agent's user name (issue #660, PR B);
//   - the real front's Lease renewTime moves, so its heartbeat is written under its own role (rule 1 of the audit policy leaves
//     no line for a Lease renewal, so no log scan can show it);
//   - down leaves the files, and the next up issues new tokens, with new expiries, to the same paths;
//   - status lists each identity, valid, with its expiry and no warning.

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/config/audit"
)

const (
	controllerIdentitiesBase      = 28520
	controllerIdentitiesFrontPort = 28533
	controllerSA                  = "system:serviceaccount:hpcburst-system:"
)

type controllerIdentityJSON struct {
	Name       string    `json:"name"`
	Username   string    `json:"username"`
	Kubeconfig string    `json:"kubeconfig"`
	Expires    time.Time `json:"expires"`
}

// findAuditBy is the first audit event of verb on the resource's object name made by user.
func findAuditBy(evs []auditEvent, verb, resource, name, user string) *auditEvent {
	for i := range evs {
		if e := &evs[i]; e.Verb == verb && e.ObjectRef.Resource == resource && e.ObjectRef.Name == name && e.User.Username == user {
			return e
		}
	}
	return nil
}

func TestControllerIdentitiesOnARealK3s(t *testing.T) {
	const base = controllerIdentitiesBase
	h := newHost(t)
	ctl, _ := fakeController(t)
	fb := frontBinary(t)
	work := shortTmp(t, false)
	clientKey := "ctl-identities-client-key-" + strings.Repeat("5", 20)
	keys := filepath.Join(work, "client-keys.json")
	if err := os.WriteFile(keys, []byte(fmt.Sprintf(`{"keys":[{"id":"e2e","key":%q}]}`, clientKey)), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := filepath.Join(work, "front.json")
	cfgBody := fmt.Sprintf(`{"clientListen":"127.0.0.1:%d","clientKeysFile":%q,"usageDir":%q}`, controllerIdentitiesFrontPort, keys, filepath.Join(work, "usage"))
	if err := os.WriteFile(cfg, []byte(cfgBody), 0o600); err != nil {
		t.Fatal(err)
	}
	sshKey := filepath.Join(work, "restricted-key")
	if err := os.WriteFile(sshKey, []byte("not read by the launcher"), 0o600); err != nil {
		t.Fatal(err)
	}
	up := func() (ids []controllerIdentityJSON) {
		t.Helper()
		before := time.Now()
		r := cli(t, h.upArgs(base, append(frpsArgs(t), "--json", "--controller-bin", ctl, "--manifests", filepath.Join("..", "..", "config", "crd"),
			"--front-config", cfg, "--front-bin", fb, "--front-mode", "ssh", "--front-addr", "front.example.test", "--front-key", sshKey)...)...)
		if r.code != exitOK {
			t.Fatalf("up exited %d: %s", r.code, r.err)
		}
		var rep struct {
			ControllerIdentities []controllerIdentityJSON `json:"controller_identities"`
		}
		parseJSON(t, r.out, &rep)
		// A token's life is a year: the exp claim the API server set, read by the test's own decoder.
		for _, id := range rep.ControllerIdentities {
			if _, exp := tokenOf(t, id.Kubeconfig); !exp.Equal(id.Expires) ||
				id.Expires.Before(before.Add(364*24*time.Hour)) || id.Expires.After(time.Now().Add(366*24*time.Hour)) {
				t.Errorf("%s: the token carries exp %s and up reported %s, want both about a year after %s", id.Name, exp, id.Expires, before)
			}
		}
		return rep.ControllerIdentities
	}

	ids := up()
	idDir := filepath.Join(h.dir, "identities")
	sysDir := filepath.Join(idDir, "system")
	adminKubeconfig := filepath.Join(h.dir, "kubeconfig")
	admin := whoIs(t, adminKubeconfig)
	if len(ids) != 3 {
		t.Fatalf("up issued %d controller identities (%+v), want hpcburst-lifecycle, hpcburst-provider and hpcburst-front", len(ids), ids)
	}
	check := func(ids []controllerIdentityJSON) {
		t.Helper()
		for i, name := range []string{"hpcburst-lifecycle", "hpcburst-provider", "hpcburst-front"} {
			id := ids[i]
			if want := filepath.Join(sysDir, name+".kubeconfig"); id.Name != name || id.Kubeconfig != want || id.Username != controllerSA+name {
				t.Errorf("identity %d is %+v, want %s in %s as %s", i, id, name, want, controllerSA+name)
			}
			if fi, err := os.Stat(id.Kubeconfig); err != nil || fi.Mode().Perm() != 0o600 {
				t.Errorf("%s: %v, %v, want mode 600", id.Kubeconfig, fi, err)
			}
			for _, d := range []string{idDir, sysDir} {
				if fi, err := os.Stat(d); err != nil || fi.Mode().Perm() != 0o700 {
					t.Errorf("%s: %v, %v, want mode 700", d, fi, err)
				}
			}
			// The SelfSubjectReview is made WITH the identity's own file: it names the ServiceAccount, not the admin.
			if u := whoIs(t, id.Kubeconfig); u.Username != controllerSA+name || u.Username == admin.Username {
				t.Errorf("the kubeconfig of %s authenticates as %q (the admin: %q), want %q", name, u.Username, admin.Username, controllerSA+name)
			}
			if raw := fileOf(id.Kubeconfig); strings.Contains(raw, "client-certificate") || strings.Contains(raw, "client-key") {
				t.Errorf("the kubeconfig of %s holds a client certificate or key: the admin's kind of credential", name)
			}
		}
	}
	check(ids)

	// ---- the audit log file: a write by the lifecycle identity, and the real front's own Lease ----
	lifecycle := controllerSA + "hpcburst-lifecycle"
	a := &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Name: "lifecycle-write", Namespace: "default"}}
	a.APIVersion, a.Kind = v1alpha1.GroupVersion.String(), "Allocation"
	a.Spec.ProviderConfigReference = &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: "example-a"}
	a.Spec.ForProvider = v1alpha1.AllocationParameters{Batch: &v1alpha1.BatchParameters{
		Partition: "example-gpu", TimeLimit: "1h", GPUs: 1, CPUsPerTask: 16,
		Memory: resource.MustParse("120Gi"), AgentDigest: strings.Repeat("3", 64)}}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(a)
	if err != nil {
		t.Fatal(err)
	}
	_, lifecycleDyn := kubeClients(t, ids[0].Kubeconfig)
	ctx, cancel := testCtx()
	defer cancel()
	res := lifecycleDyn.Resource(v1alpha1.GroupVersion.WithResource("allocations")).Namespace("default")
	got, err := res.Create(ctx, &unstructured.Unstructured{Object: obj}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("the lifecycle identity's create of an Allocation: %v", err)
	}
	got.SetAnnotations(map[string]string{"example.test/written-by": "the lifecycle identity"})
	if _, err := res.Update(ctx, got, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("the lifecycle identity's update of an Allocation: %v", err)
	}
	auditPath := filepath.Join(h.dir, "logs", "audit.log")
	frontUser := controllerSA + "hpcburst-front"
	waitFor(t, 30*time.Second, "the lifecycle identity's writes and the front's Lease in the audit log", func() bool {
		if _, err := os.Stat(auditPath); err != nil {
			return false
		}
		_, evs := readAudit(t, auditPath, true)
		return findAuditBy(evs, "create", "allocations", "lifecycle-write", lifecycle) != nil &&
			findAuditBy(evs, "update", "allocations", "lifecycle-write", lifecycle) != nil &&
			(findAuditBy(evs, "update", "leases", "front", frontUser) != nil || findAuditBy(evs, "create", "leases", "front", frontUser) != nil)
	})
	_, evs := readAudit(t, auditPath, true)
	// Rule 3 of the policy, in the log FILE: the lifecycle identity's UPDATE of an Allocation is at Request level, with its
	// request body (the release cause is logged before the delete that follows). The levels of every rule are judged by the
	// API server's own evaluator in config/audit; this is what the real API server wrote.
	if e := findAuditBy(evs, "update", "allocations", "lifecycle-write", lifecycle); e == nil || e.Level != "Request" || !e.hasRequest {
		t.Errorf("the lifecycle identity's update of an Allocation is logged as %+v, want level Request with its request body", e)
	}
	for _, e := range evs {
		if e.ObjectRef.Name == "lifecycle-write" && e.ObjectRef.Resource == "allocations" && e.User.Username != lifecycle {
			t.Errorf("the Allocation the lifecycle identity wrote is logged under %q (%s), never the admin's or another's", e.User.Username, e.Verb)
		}
		if e.ObjectRef.Resource == "leases" && e.ObjectRef.Name == "front" && e.User.Username == admin.Username {
			t.Errorf("the front's Lease is logged under the admin %q", admin.Username)
		}
	}

	// No request of a controller identity was refused: the real front ran under its role, and a verb it
	// lacked would be a 403 here, whatever the front did with the answer.
	for _, e := range evs {
		if e.ResponseStatus.Code == 403 && strings.HasPrefix(e.User.Username, controllerSA) {
			t.Errorf("a controller identity was refused (403): %s %s %q by %s", e.Verb, e.ObjectRef.Resource, e.ObjectRef.Name, e.User.Username)
		}
	}

	// ---- the front's heartbeat moves, on state (rule 1 of the audit policy leaves no line for a Lease read or renewal in
	// hpcburst-system, refused or not, so the log's 403 scan cannot see a Lease verb the front's role lacks: drop its update and
	// the heartbeat is refused every 10 s while the front runs on, and the Lease's create alone met the wait above) ----
	leases, _ := kubeClients(t, adminKubeconfig)
	frontLease := func() (time.Time, bool) {
		l, err := leases.CoordinationV1().Leases("hpcburst-system").Get(ctx, "front", metav1.GetOptions{})
		if err != nil || l.Spec.RenewTime == nil {
			return time.Time{}, false
		}
		return l.Spec.RenewTime.Time, true
	}
	var firstRenew time.Time
	waitFor(t, 60*time.Second, "the front's Lease has a renewTime", func() bool {
		rt, ok := frontLease()
		firstRenew = rt
		return ok
	})
	waitFor(t, 90*time.Second, "the front's Lease renewTime moves: its heartbeat is written, under its own role", func() bool {
		rt, ok := frontLease()
		return ok && rt.After(firstRenew)
	})

	// ---- the audit policy and its rotation (HBEP-10, Audit; issue #660, PR B) ----
	// up wrote the embedded policy, owner only; the launcher passed k3s the flags that keep every rotated file, gzipped
	// (as it recorded them); and an agent's Operation create is in the log FILE at
	// RequestResponse, with its body, under the agent's user name.
	policyPath := filepath.Join(h.dir, "audit-policy.yaml")
	if got, err := os.ReadFile(policyPath); err != nil || !bytes.Equal(got, audit.Policy) {
		t.Errorf("up did not write the embedded audit policy to %s: %v", policyPath, err)
	}
	if fi, err := os.Stat(policyPath); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("the audit policy file: %v, %v, want mode 600", fi, err)
	}
	// The flags k3s is started with are pinned where they are built (internal/launcher's TestK3sArgs, with literals). They are
	// not read from the running process: k3s rewrites its own command line after it starts. What the RUNNING k3s shows is its
	// behaviour: the policy it reads is the one up wrote, because only that policy logs an Operation's create at RequestResponse,
	// with its body (below), and the log is where the flags put it.
	for _, p := range []string{filepath.Join(h.dir, "logs"), auditPath} {
		if fi, err := os.Stat(p); err != nil || fi.Mode().Perm()&0o077 != 0 {
			t.Errorf("%s: %v, %v, want owner only", p, fi, err)
		}
	}
	agent := cli(t, "identity", "add", "agent-a", "--json", "--state-dir", h.dir)
	if agent.code != exitOK {
		t.Fatalf("identity add agent-a exited %d: %s", agent.code, agent.err)
	}
	var agentID identityJSON
	parseJSON(t, agent.out, &agentID)
	_, agentDyn := kubeClients(t, agentID.Kubeconfig)
	const opName = "audit-agent-op"
	waitFor(t, 60*time.Second, "agent-a's Operation create, accepted", func() bool {
		_, err := agentDyn.Resource(opGVR).Namespace("default").Create(ctx, toUnstructured(t, operationFor(agentID.Username, opName), "Operation"), metav1.CreateOptions{})
		return err == nil || apierrors.IsAlreadyExists(err) // the agent's role is the wide one a moment after up returns
	})
	waitFor(t, 30*time.Second, "the agent's Operation create in the audit log file", func() bool {
		_, evs := readAudit(t, auditPath, true)
		return findAuditBy(evs, "create", "operations", opName, agentID.Username) != nil
	})
	_, evs = readAudit(t, auditPath, true)
	if e := findAuditBy(evs, "create", "operations", opName, agentID.Username); e == nil || e.Level != "RequestResponse" || !e.hasBody {
		t.Errorf("the agent's Operation create is logged as %+v, want level RequestResponse with a body, under %s", e, agentID.Username)
	}

	// ---- status: each identity valid, with its expiry and no warning ----
	r := cli(t, h.args("status", "--json", "--no-gc-probe")...)
	var st struct {
		Controllers []struct {
			Name    string     `json:"name"`
			Valid   bool       `json:"valid"`
			Expires *time.Time `json:"expires"`
			Warning string     `json:"warning"`
		} `json:"controller_identities"`
	}
	parseJSON(t, r.out, &st)
	if len(st.Controllers) != 3 {
		t.Fatalf("status lists %d controller identities: %s", len(st.Controllers), r.out)
	}
	for _, s := range st.Controllers {
		if !s.Valid || s.Expires == nil || s.Warning != "" {
			t.Errorf("status says %+v of %s, want valid, with an expiry, and no warning", s, s.Name)
		}
	}
	if r.code != exitOK || !strings.Contains(cli(t, h.args("status", "--no-gc-probe")...).out, "controller identity hpcburst-front: valid, expires ") {
		t.Errorf("status exited %d, or its text lacks the front's identity line", r.code)
	}

	// ---- down leaves the files; the next up issues new tokens, to the same paths, with new expiries ----
	oldTokens := map[string]string{}
	for _, id := range ids {
		tok, _ := tokenOf(t, id.Kubeconfig)
		oldTokens[id.Name] = tok
	}
	if r := cli(t, "down", "--state-dir", h.dir); r.code != exitOK {
		t.Fatalf("down exited %d: %s", r.code, r.err)
	}
	for _, id := range ids {
		if fi, err := os.Stat(id.Kubeconfig); err != nil || fi.Mode().Perm() != 0o600 {
			t.Errorf("down did not leave %s in place at mode 600: %v, %v", id.Kubeconfig, fi, err)
		}
	}
	ids2 := up()
	check(ids2)
	for i, id := range ids2 {
		tok, _ := tokenOf(t, id.Kubeconfig)
		if tok == oldTokens[id.Name] {
			t.Errorf("the second up left the old token of %s in %s", id.Name, id.Kubeconfig)
		}
		if !id.Expires.After(ids[i].Expires) {
			t.Errorf("the second up's expiry for %s is %s, not after the first's %s", id.Name, id.Expires, ids[i].Expires)
		}
	}
}
