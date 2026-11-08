package main

// The CRD deletion guard on a real k3s (HBEP-11, Risks and Admission; issue #242): hpcburst up
// installs it, a delete of a framework CRD is refused, status reports it, and hpcburst guard remove
// is the one step that lets a delete through.
//
// Every judgement is made with client-go and a delete the API server answers, not with the
// launcher's own code and not by looking for the policy object: a policy that exists and is never
// tried against a CRD delete proves nothing, since a binding that does not name it, or a policy the
// API server does not enforce, would exist all the same.
//
// The framework's CRDs are the ones of config/crd, applied by up --manifests. A refused delete is a
// real one; the API server's answer is the evidence. An allowed delete is a real one too, last,
// on a host that is thrown away.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

var (
	crdGVR     = schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	bindingGVR = schema.GroupVersionResource{Group: "admissionregistration.k8s.io", Version: "v1", Resource: "validatingadmissionpolicybindings"}
)

const (
	guardObject = "hpcburst-protect-crds"
	// providers.hpcburst.groundnuty.github.io is one of the five CRDs of config/crd.
	frameworkCRD = "providers.hpcburst.groundnuty.github.io"
)

// guardStatus is the part of status --json this test reads.
type guardStatus struct {
	Health   string `json:"health"`
	CRDGuard *struct {
		Present bool   `json:"present"`
		Detail  string `json:"detail"`
	} `json:"crd_guard"`
}

func TestUpInstallsTheCRDGuardAndGuardRemoveRemovesIt(t *testing.T) {
	const base = 29220
	h := newHost(t)
	crds := filepath.Join("..", "..", "config", "crd")

	// ---- up installs the guard, with --manifests naming only the CRDs ----
	var up struct {
		CRDGuard bool `json:"crd_guard"`
		Applied  int  `json:"manifests_applied"`
	}
	r := cli(t, h.upArgs(base, "--manifests", crds, "--json")...)
	if r.code != exitOK {
		t.Fatalf("up exited %d: %s", r.code, r.err)
	}
	parseJSON(t, r.out, &up)
	if !up.CRDGuard {
		t.Errorf("up did not say it installed the CRD deletion guard: %s", r.out)
	}
	if up.Applied != 5 {
		t.Errorf("up applied %d manifests, want the 5 CRDs of %s: the guard is not one of --manifests's", up.Applied, crds)
	}

	cl := h.clients()
	ctx, cancel := testCtx()
	defer cancel()

	// up returns only once the guard is enforced (WaitGuardEnforced), so a delete RIGHT after it is
	// refused: no polling here, since a wait of the test's own would hide a window in which up had
	// returned and the API server still accepted the delete (review of PR 250: B2).
	dryRun := func(name string) error {
		return cl.dyn.Resource(crdGVR).Delete(ctx, name, metav1.DeleteOptions{DryRun: []string{metav1.DryRunAll}})
	}
	if err := dryRun(frameworkCRD); !apierrors.IsForbidden(err) {
		t.Fatalf("a dry-run delete of %s right after up returned: %v, want Forbidden: up returned before the guard was enforced", frameworkCRD, err)
	}

	// A real delete of every framework CRD is refused with the guard's own reason, and each CRD is
	// still there, with no deletion under way.
	for _, name := range []string{"providers", "platformprofiles", "modeldeployments", "allocations", "instances"} {
		crd := name + ".hpcburst.groundnuty.github.io"
		err := cl.dyn.Resource(crdGVR).Delete(ctx, crd, metav1.DeleteOptions{})
		if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), "refusing to delete the custom resource definition "+crd) ||
			!strings.Contains(err.Error(), "hpcburst guard remove") {
			t.Errorf("delete of %s: %v, want the guard's refusal naming the CRD and hpcburst guard remove", crd, err)
		}
		o, gerr := cl.dyn.Resource(crdGVR).Get(ctx, crd, metav1.GetOptions{})
		if gerr != nil || o.GetDeletionTimestamp() != nil {
			t.Errorf("after the refused delete, %s: %v, deletionTimestamp %v", crd, gerr, o.GetDeletionTimestamp())
		}
	}

	// ---- status reports it present ----
	wantGuard := func(present bool, detail string) {
		t.Helper()
		var js guardStatus
		sr := cli(t, h.args("status", "--json", "--no-gc-probe")...)
		parseJSON(t, sr.out, &js)
		if js.CRDGuard == nil || js.CRDGuard.Present != present || !strings.Contains(js.CRDGuard.Detail, detail) {
			t.Errorf("status --json crd_guard = %+v, want present %v with detail containing %q", js.CRDGuard, present, detail)
		}
		// An absent guard is a state, not a fault: the host stays healthy and status exits 0 (review of PR 250, nit).
		if sr.code != exitOK || js.Health != "healthy" {
			t.Errorf("status with the guard present=%v: exit %d, health %q, want exit 0 and healthy", present, sr.code, js.Health)
		}
		text := cli(t, h.args("status", "--no-gc-probe")...)
		word := "CRD deletion guard: present"
		if !present {
			word = "CRD deletion guard: ABSENT"
		}
		if !strings.Contains(text.out, word) || text.code != exitOK {
			t.Errorf("status text (exit %d) lacks %q:\n%s", text.code, word, text.out)
		}
	}
	wantGuard(true, "")
	// With --no-api the API server is not asked: the guard is "not checked", never "absent".
	var noAPI guardStatus
	parseJSON(t, cli(t, h.args("status", "--json", "--no-api")...).out, &noAPI)
	if noAPI.CRDGuard != nil {
		t.Errorf("status --no-api reports the guard as %+v, want nothing: it did not ask", noAPI.CRDGuard)
	}

	// ---- a match filter added by hand leaves a binding that exists and matches nothing (review of PR 250: B1) ----
	// The delete really goes through, status says absent, and a restart sets the binding back to the
	// manifest's spec: a delete right after up is refused.
	_, err := cl.dyn.Resource(bindingGVR).Patch(ctx, guardObject, types.MergePatchType,
		[]byte(`{"spec":{"matchResources":{"objectSelector":{"matchLabels":{"example":"never"}},"namespaceSelector":{"matchLabels":{"example":"never"}}}}}`), metav1.PatchOptions{})
	if err != nil {
		t.Fatalf("add a selector to the binding by hand: %v", err)
	}
	wantGuard(false, "spec.matchResources")
	waitFor(t, 90*time.Second, "a dry-run delete of a framework CRD to be allowed with a selector on the binding", func() bool {
		return dryRun(frameworkCRD) == nil
	})
	if d := cli(t, h.args("down")...); d.code != exitOK {
		t.Fatalf("down exited %d: %s", d.code, d.err)
	}
	if u := cli(t, h.upArgs(base, "--json")...); u.code != exitOK {
		t.Fatalf("up over the filtered binding exited %d: %s", u.code, u.err)
	}
	cl = h.clients()
	if err := dryRun(frameworkCRD); !apierrors.IsForbidden(err) {
		t.Fatalf("a dry-run delete right after up over a filtered binding: %v, want Forbidden", err)
	}
	wantGuard(true, "")

	// ---- the old by-hand removal deletes only the binding; status must see that ----
	if err := cl.dyn.Resource(bindingGVR).Delete(ctx, guardObject, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete the binding by hand: %v", err)
	}
	wantGuard(false, "ValidatingAdmissionPolicyBinding "+guardObject+" is not there")
	waitFor(t, 90*time.Second, "a dry-run delete of a framework CRD to be allowed with the binding gone", func() bool {
		return dryRun(frameworkCRD) == nil
	})

	// ---- a restart installs it again: up is the way back, down never took it away ----
	if d := cli(t, h.args("down")...); d.code != exitOK {
		t.Fatalf("down exited %d: %s", d.code, d.err)
	}
	if u := cli(t, h.upArgs(base, "--json")...); u.code != exitOK { // no --manifests this time
		t.Fatalf("second up exited %d: %s", u.code, u.err)
	}
	cl = h.clients()
	if err := dryRun(frameworkCRD); !apierrors.IsForbidden(err) {
		t.Fatalf("a dry-run delete right after the second up: %v, want Forbidden", err)
	}
	wantGuard(true, "")

	// ---- guard remove: the explicit step that lets a delete through ----
	var rm struct {
		Removed bool `json:"removed"`
	}
	rr := cli(t, h.args("guard", "remove", "--json")...)
	if rr.code != exitOK {
		t.Fatalf("guard remove exited %d: %s", rr.code, rr.err)
	}
	parseJSON(t, rr.out, &rm)
	if !rm.Removed {
		t.Errorf("guard remove said it removed nothing: %s", rr.out)
	}
	wantGuard(false, "is not there")
	waitFor(t, 90*time.Second, "a dry-run delete of a framework CRD to be allowed after guard remove", func() bool {
		return dryRun(frameworkCRD) == nil
	})
	// Removing it again is a success that says there was nothing.
	again := cli(t, h.args("guard", "remove")...)
	if again.code != exitOK || !strings.Contains(again.out, "already absent") {
		t.Errorf("a second guard remove: exit %d, %q, want 0 and 'already absent'", again.code, again.out)
	}
	// The delete itself, for real: the CRD goes (its deletion is asynchronous).
	if err := cl.dyn.Resource(crdGVR).Delete(ctx, frameworkCRD, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete of %s after guard remove: %v, want it allowed", frameworkCRD, err)
	}
	waitFor(t, 90*time.Second, frameworkCRD+" to be gone", func() bool {
		_, err := cl.dyn.Resource(crdGVR).Get(ctx, frameworkCRD, metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	})
}

// guard remove on a state directory that records no host is a refusal, not a success that did
// nothing.
func TestGuardRemoveRefusesWithoutAHost(t *testing.T) {
	dir := filepath.Join(shortTmp(t, false), "st")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	r := cli(t, "guard", "remove", "--state-dir", dir)
	if r.code != exitFail || !strings.Contains(r.err, "no host is recorded") {
		t.Errorf("guard remove with no host: exit %d, stderr %q, want exit %d and 'no host is recorded'", r.code, r.err, exitFail)
	}
	if r := cli(t, "guard"); r.code != exitUsage || !strings.Contains(r.err, "hpcburst guard remove") {
		t.Errorf("guard with no subcommand: exit %d, stderr %q, want the usage", r.code, r.err)
	}
	// The help says what the removal exposes (science agent, 2 Nov 2026).
	for _, args := range [][]string{{"guard", "-h"}, {"guard", "remove", "-h"}, {"guard"}} {
		r := cli(t, args...)
		want := "Framework CRDs can then be\ndeleted, which deletes every resource of those kinds"
		if !strings.Contains(r.err+r.out, want) {
			t.Errorf("hpcburst %s: the help does not say what the removal exposes (%q):\n%s%s", strings.Join(args, " "), want, r.out, r.err)
		}
	}
	if r := cli(t, "guard", "install"); r.code != exitUsage {
		t.Errorf("guard install: exit %d, want a usage error: up installs the guard, and there is no other way in", r.code)
	}
}
