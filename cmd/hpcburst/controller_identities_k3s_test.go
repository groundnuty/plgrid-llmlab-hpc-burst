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
//   - the API server's audit log FILE, which is today's Metadata policy, holds a write made with the lifecycle
//     identity under user.username = the lifecycle ServiceAccount, and the real front's Lease writes under the
//     front's, so the real front process runs on its identity; the administrator's name is on neither;
//   - down leaves the files, and the next up issues new tokens, with new expiries, to the same paths;
//   - status lists each identity, valid, with its expiry and no warning.

import (
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

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
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
	for _, e := range evs {
		if e.ObjectRef.Name == "lifecycle-write" && e.ObjectRef.Resource == "allocations" && e.User.Username != lifecycle {
			t.Errorf("the Allocation the lifecycle identity wrote is logged under %q (%s), never the admin's or another's", e.User.Username, e.Verb)
		}
		if e.ObjectRef.Resource == "leases" && e.ObjectRef.Name == "front" && e.User.Username == admin.Username {
			t.Errorf("the front's Lease is logged under the admin %q", admin.Username)
		}
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
