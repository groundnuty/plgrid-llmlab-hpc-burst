package lifecycle

// The ModelDeployment's own owner-hash label (owner.go syncOwnerLabel; `@hpc-burst-science-agent`, 17 Nov 2026, #662,
// Q4): the controller sets it from the owner annotation, corrects a wrong one, removes it when there is no owner
// annotation, and writes nothing when it is right. The writes are read from the client (ownerLog), the merge patch's
// body written out by hand; a fake client that never records writes would prove nothing about "no write".

import (
	"errors"
	"testing"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

// wantOnePatch checks the deployment's one label patch: the merge patch's body, written out by hand, and the field
// manager that sent it.
func wantOnePatch(t *testing.T, log *ownerLog, md, body string) {
	t.Helper()
	ps := log.patchesOf(md)
	if len(ps) != 1 {
		t.Fatalf("%d patches of %s: %+v, want one", len(ps), md, ps)
	}
	if ps[0].body != body {
		t.Errorf("the patch of %s is %s, want %s", md, ps[0].body, body)
	}
	if ps[0].manager != "hpcburst-lifecycle" {
		t.Errorf("the patch of %s names the field manager %q, want hpcburst-lifecycle", md, ps[0].manager)
	}
}

// TestTheDeploymentsLabelIsSetCorrectedAndRemoved covers Q4's three writes and the labels-only rule.
func TestTheDeploymentsLabelIsSetCorrectedAndRemoved(t *testing.T) {
	const (
		setBody     = `{"metadata":{"labels":{"hpcburst.groundnuty.github.io/owner-hash":"u-77d125ed9ec1"}}}`
		removeBody  = `{"metadata":{"labels":{"hpcburst.groundnuty.github.io/owner-hash":null}}}`
		teamLabel   = "example.org/team"
		noteAnnot   = "example.org/note"
		ownerNoteIn = "an annotation of someone else's"
	)
	for _, tc := range []struct {
		name       string
		owner      string
		label      string // the label the deployment is created with; "" for none
		wantLabels map[string]string
		wantBody   string
	}{
		{"set when absent", ownerA, "", map[string]string{teamLabel: "a", keyOwnerHash: hashA}, setBody},
		{"corrected when an agent set another value at create", ownerA, wrongHash, map[string]string{teamLabel: "a", keyOwnerHash: hashA}, setBody},
		{"removed when there is no owner annotation", "", hashA, map[string]string{teamLabel: "a"}, removeBody},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := &ownerLog{}
			rg := newRig(t, withInterceptors(log.funcs()))
			md := ownedMD("md-a", tc.owner, "example purpose of a")
			md.Labels = map[string]string{teamLabel: "a"}
			if tc.label != "" {
				md.Labels[keyOwnerHash] = tc.label
			}
			md.Annotations[noteAnnot] = ownerNoteIn
			wantAnn := map[string]string{keyPurpose: "example purpose of a", noteAnnot: ownerNoteIn}
			if tc.owner != "" {
				wantAnn[keyOwner] = tc.owner
			}
			spec := *md.Spec.DeepCopy()
			rg.create(md)

			rg.Step()

			wantOnePatch(t, log, "md-a", tc.wantBody)
			lb, ann := rg.mdMeta("md-a")
			wantMap(t, "the deployment's labels", lb, tc.wantLabels)
			wantMap(t, "the deployment's annotations", ann, wantAnn)
			if got := rg.md("md-a").Spec; !apiequality.Semantic.DeepEqual(got, spec) {
				t.Errorf("the deployment's spec changed: %+v, want %+v", got, spec)
			}
			if n := log.mdUpdates; n != 0 {
				t.Errorf("%d whole-object updates of the deployment, want none: the label is a patch", n)
			}
			// The pass went on: the label write did not stop it, and its status write did not conflict on the
			// resourceVersion the patch advanced (the rig fails the test on a pass that returns an error).
			if c := rg.condition("md-a", "Ready"); c.Type == "" {
				t.Error("the deployment has no Ready condition: the pass's status write did not land after the patch")
			}
		})
	}
}

// TestTheDeploymentsLabelIsNotWrittenWhenItIsRight: once the label is right the controller writes none, pass after
// pass. The first pass patches once, so the count could have grown; two more passes at the same time and at later
// times leave it at one, and the stored label is the same.
func TestTheDeploymentsLabelIsNotWrittenWhenItIsRight(t *testing.T) {
	log := &ownerLog{}
	rg := newRig(t, withInterceptors(log.funcs()))
	rg.create(ownedMD("md-a", ownerA, "example purpose of a"))
	rg.Step()
	if n := len(log.patchesOf("md-a")); n != 1 {
		t.Fatalf("%d patches after the first pass, want one", n)
	}
	writes := log.mdWrites()
	// The same pass again at the same instant, then passes while the rest of the scenario runs.
	rg.reconcileAll()
	rg.until(2*time.Minute, 10*time.Second, "a ready Instance", func() bool { return rg.readyCount("md-a") == 1 })
	if n := log.mdWrites(); n != writes {
		t.Errorf("%d writes to ModelDeployments after the label was right, want the %d of the first pass: %+v", n, writes, log.mdPatches)
	}
	if lb, _ := rg.mdMeta("md-a"); lb[keyOwnerHash] != hashA {
		t.Errorf("the label is %q, want %s", lb[keyOwnerHash], hashA)
	}
	// A deployment that never had an owner or a label is written no more than that.
	rg.create(ownedMD("md-none", "", ""))
	before := log.mdWrites()
	rg.Step()
	rg.Step()
	if n := log.mdWrites(); n != before {
		t.Errorf("%d writes to ModelDeployments for a deployment with no owner and no label, want none", n-before)
	}
}

// TestADeploymentBeingDeletedIsNotLabelled: a deployment with a deletion timestamp (held by a finalizer) gets no label
// write, whether its label is absent, wrong or stale. A live deployment of the same shape is patched, in the same rig,
// which shows the log sees such a patch.
func TestADeploymentBeingDeletedIsNotLabelled(t *testing.T) {
	log := &ownerLog{}
	rg := newRig(t, withInterceptors(log.funcs()))
	for name, lb := range map[string]map[string]string{
		"md-gone-absent": nil,
		"md-gone-wrong":  {keyOwnerHash: wrongHash},
		"md-gone-stale":  {keyOwnerHash: hashA},
	} {
		md := ownedMD(name, ownerA, "example purpose of a")
		if name == "md-gone-stale" {
			md.Annotations = nil // no owner: the label is stale, and would be removed on a live deployment
		}
		md.Labels = lb
		md.Finalizers = []string{"example.org/hold"}
		rg.create(md)
		// A delete of an object with a finalizer sets its deletion timestamp and keeps it.
		if err := rg.c.Delete(rg.ctx, md); err != nil {
			t.Fatal(err)
		}
		if rg.md(name).DeletionTimestamp.IsZero() {
			t.Fatalf("%s has no deletion timestamp after its delete", name)
		}
	}
	rg.Step()
	rg.Step()
	if ps := log.mdPatches; len(ps) != 0 {
		t.Errorf("patches of deployments being deleted: %+v, want none", ps)
	}
	for name, want := range map[string]map[string]string{
		"md-gone-absent": nil, "md-gone-wrong": {keyOwnerHash: wrongHash}, "md-gone-stale": {keyOwnerHash: hashA},
	} {
		lb, _ := rg.mdMeta(name)
		wantMap(t, name+" labels", lb, want)
	}
	// The control: the same deployment, not being deleted.
	rg.create(ownedMD("md-live", ownerA, "example purpose of a"))
	rg.Step()
	if n := len(log.patchesOf("md-live")); n != 1 {
		t.Errorf("%d patches of the live deployment, want one", n)
	}
}

// TestAFailedLabelWriteDoesNotHoldBackTheRestOfThePass: when the label's patch fails (a controller with no right to
// patch ModelDeployments, say), the pass still provisions, so an Instance exists after it, and still returns the error,
// so the key is retried with backoff and the label is written once the patch works.
func TestAFailedLabelWriteDoesNotHoldBackTheRestOfThePass(t *testing.T) {
	boom := errors.New("example refusal of the patch")
	log := &ownerLog{}
	rg := newRig(t, withInterceptors(log.funcs()))
	rg.Step() // the Providers' first status, before the deployment exists
	log.failPatches(boom)
	rg.create(ownedMD("md-a", ownerA, "example purpose of a"))
	rg.clock.Advance(10 * time.Second)
	_, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: "md-a"}})
	if !errors.Is(err, boom) {
		t.Fatalf("the pass returned %v, want the failed patch's error", err)
	}
	if n := len(rg.instances("md-a")); n == 0 {
		t.Error("no Instance exists after the pass: a failed label write held back provisioning")
	}
	if lb, _ := rg.mdMeta("md-a"); lb[keyOwnerHash] != "" {
		t.Errorf("the label is %q although every patch failed", lb[keyOwnerHash])
	}
	log.failPatches(nil)
	rg.Step()
	if lb, _ := rg.mdMeta("md-a"); lb[keyOwnerHash] != hashA {
		t.Errorf("the label is %q after the patch worked, want %s", lb[keyOwnerHash], hashA)
	}
}
