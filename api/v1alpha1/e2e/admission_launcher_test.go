package e2e

// The launcher's install, check and reset of the three admission policies (internal/launcher/admission.go;
// issue #589, PR 3), and the owner pin's second layer, against a real API server that evaluates
// ValidatingAdmissionPolicies. Every judgement is the API server's answer to a dry-run probe: that the
// policy object exists proves nothing about whether it stops a write.

import (
	"io"
	"strings"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

func bindingObject(name string) *admissionregistrationv1.ValidatingAdmissionPolicyBinding {
	return &admissionregistrationv1.ValidatingAdmissionPolicyBinding{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func probeRefused(t *testing.T, name string) bool {
	t.Helper()
	refused, err := launcher.ProbeAdmission(bg(), cfg, name)
	if !refused && err != nil {
		t.Logf("probe of %s: %v", name, err)
	}
	return refused
}

func TestLauncherInstallsChecksAndResetsTheAdmissionPolicies(t *testing.T) {
	if states, err := launcher.CheckAdmission(bg(), cfg); err != nil {
		t.Fatal(err)
	} else {
		for _, s := range states {
			if s.Present {
				t.Skipf("%s is installed on this host already: this test starts from none", s.Name)
			}
		}
	}
	cleanUpPolicies(t)
	dyn, err := dynamic.NewForConfig(cfg)
	must(t, err, "dynamic client")

	// Before: a probe is accepted (so a refusal below is the policy's doing), and every policy is absent.
	for _, name := range launcher.AdmissionPolicyNames() {
		refused, err := launcher.ProbeAdmission(bg(), cfg, name)
		if refused || err != nil {
			t.Fatalf("before the install the probe of %s: refused %v, %v, want it accepted", name, refused, err)
		}
	}
	states, err := launcher.CheckAdmission(bg(), cfg)
	must(t, err, "check before")
	for _, s := range states {
		if s.Present || s.Detail == "" {
			t.Errorf("CheckAdmission before the install: %+v, want absent with a detail", s)
		}
	}

	// Install twice: server-side apply repeats safely. The wait returns once every probe is refused, and
	// then one probe each, with no polling, is refused (up returns only after it).
	must(t, launcher.InstallAdmission(bg(), cfg, io.Discard), "install")
	must(t, launcher.InstallAdmission(bg(), cfg, io.Discard), "install again")
	unprobed, err := launcher.WaitAdmissionEnforced(bg(), cfg, 60*time.Second, io.Discard)
	if err != nil || len(unprobed) != 0 {
		t.Fatalf("WaitAdmissionEnforced = %v, %v, want every policy probed and enforced", unprobed, err)
	}
	requireEnforced(t)
	states, err = launcher.CheckAdmission(bg(), cfg)
	must(t, err, "check after")
	for _, s := range states {
		if !s.Present {
			t.Errorf("CheckAdmission after the install: %+v, want present", s)
		}
	}

	// What is added to a policy by hand does not survive an install, and CheckAdmission names it. Each
	// weakening really lets the probe through (the control), then the install sets the spec to the
	// manifest's whole and the policy is enforced again.
	for _, tc := range []struct {
		name, policy string
		res          string // "binding" or "policy"
		patch        string
	}{
		{"a binding that only warns", capacityGuard, "binding", `{"spec":{"validationActions":["Warn"]}}`},
		{"a binding with an objectSelector no object satisfies", ownerPin, "binding",
			`{"spec":{"matchResources":{"objectSelector":{"matchLabels":{"example":"never"}},"namespaceSelector":{"matchLabels":{"example":"never"}}}}}`},
		{"a policy with a match condition that is never true", approverPin, "policy", `{"spec":{"matchConditions":[{"name":"never","expression":"false"}]}}`},
		{"a policy with a match condition that is never true (the capacity guard)", capacityGuard, "policy",
			`{"spec":{"matchConditions":[{"name":"never","expression":"false"}]}}`},
	} {
		gvr := bindingGVR
		if tc.res == "policy" {
			gvr = policyGVR
		}
		_, err := dyn.Resource(gvr).Patch(bg(), tc.policy, types.MergePatchType, []byte(tc.patch), metav1.PatchOptions{})
		must(t, err, "weaken "+tc.policy+": "+tc.name)
		// A merge patch cannot remove the match conditions the manifest has, so the capacity guard's
		// "never true" replaces them: it matches no request at all.
		states, err := launcher.CheckAdmission(bg(), cfg)
		must(t, err, "check after "+tc.name)
		for _, s := range states {
			if s.Name == tc.policy && (s.Present || s.Detail == "") && tc.res == "binding" {
				t.Errorf("%s: CheckAdmission = %+v, want absent with a detail", tc.name, s)
			}
		}
		eventually(t, 60*time.Second, tc.name+" to let the probe through", func() (bool, error) {
			refused, perr := launcher.ProbeAdmission(bg(), cfg, tc.policy)
			return !refused && perr == nil, nil
		})
		must(t, launcher.InstallAdmission(bg(), cfg, io.Discard), "install over "+tc.name)
		unprobed, err := launcher.WaitAdmissionEnforced(bg(), cfg, 60*time.Second, io.Discard)
		if err != nil || len(unprobed) != 0 {
			t.Fatalf("%s: WaitAdmissionEnforced after the reinstall = %v, %v", tc.name, unprobed, err)
		}
		if !probeRefused(t, tc.policy) {
			t.Errorf("%s: the probe right after the reinstall was not refused", tc.name)
		}
		states, err = launcher.CheckAdmission(bg(), cfg)
		must(t, err, "check after the reinstall")
		for _, s := range states {
			if !s.Present {
				t.Errorf("%s: CheckAdmission after the reinstall: %+v, want present", tc.name, s)
			}
		}
	}

	// A binding deleted by hand: the policy is there and does nothing; CheckAdmission names the binding of
	// that policy only, and the next install brings it back.
	must(t, c.Delete(bg(), bindingObject(ownerPin)), "delete the owner pin's binding by hand")
	states, err = launcher.CheckAdmission(bg(), cfg)
	must(t, err, "check with a binding gone")
	for _, s := range states {
		want := s.Name != ownerPin
		if s.Present != want || (!want && !strings.Contains(s.Detail, "ValidatingAdmissionPolicyBinding "+ownerPin)) {
			t.Errorf("with only the owner pin's binding gone, %+v: want present=%v", s, want)
		}
	}
	waitPolicyNotEnforced(t, ownerPin)
	must(t, launcher.InstallAdmission(bg(), cfg, io.Discard), "install after the by-hand removal")
	eventually(t, 60*time.Second, "the reinstalled owner pin to refuse again", func() (bool, error) { return probeRefused(t, ownerPin), nil })

	// The explicit unit of the others: removing the capacity guard leaves the two pins enforced.
	removePolicy(t, capacityGuard)
	waitPolicyNotEnforced(t, capacityGuard)
	if !probeRefused(t, ownerPin) || !probeRefused(t, approverPin) {
		t.Error("removing the capacity guard also stopped a pin from refusing its probe")
	}
}

// The owner pin refuses a create that carries spec.confirm, for an agent and for the person (#593's
// review: a confirm exists only by an update that follows a written plan). The CRD refuses it too, by an
// optionalOldSelf rule, so the pin's own refusal is shown against the Operation CRD with that one rule
// taken out, and the CRD is put back after.
func TestOwnerPinRefusesAConfirmAtCreateAsTheSecondLayer(t *testing.T) {
	agents := newAgents(t, "agent-a", "agent-b")
	a := agents[0]
	requireEnforced(t)
	ns := newNamespace(t)
	person := thePerson(t)
	const crdMessage = "spec.confirm cannot be set when an Operation is created"
	withConfirm := func(owner string) *v1alpha1.Operation {
		op := agentOperation(ns, uniq("op"), owner)
		op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: opPlanID}
		return op
	}

	// With the CRD as it is, the CRD's rule is what refuses (an Invalid), for the agent and the person.
	requireAgentIdentity(t, a)
	for who, err := range map[string]error{
		"the agent":  a.c.Create(bg(), withConfirm(a.username())),
		"the person": c.Create(bg(), withConfirm(person), client.DryRunAll),
	} {
		if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), crdMessage) {
			t.Errorf("a create with spec.confirm from %s, CRD as it is: %v, want the CRD's Invalid refusal", who, err)
		}
	}

	// Take the CRD's create rule out, and wait until the API server serves the schema without it.
	const crdName = "operations." + frameworkGroup
	crd := &apiextensionsv1.CustomResourceDefinition{}
	must(t, c.Get(bg(), client.ObjectKey{Name: crdName}, crd), "read the Operation CRD")
	original := crd.Spec.DeepCopy()
	t.Cleanup(func() {
		cur := &apiextensionsv1.CustomResourceDefinition{}
		if err := c.Get(bg(), client.ObjectKey{Name: crdName}, cur); err != nil {
			t.Errorf("clean-up: read the Operation CRD: %v", err)
			return
		}
		cur.Spec = *original
		if err := c.Update(bg(), cur); err != nil {
			t.Errorf("clean-up: put the Operation CRD back: %v", err)
			return
		}
		eventually(t, 60*time.Second, "the CRD's create rule to be back", func() (bool, error) {
			err := c.Create(bg(), withConfirm(person), client.DryRunAll)
			return apierrors.IsInvalid(err) && strings.Contains(err.Error(), crdMessage), nil
		})
	})
	removed := 0
	for vi := range crd.Spec.Versions {
		specSchema := crd.Spec.Versions[vi].Schema.OpenAPIV3Schema.Properties["spec"]
		var kept []apiextensionsv1.ValidationRule
		for _, r := range specSchema.XValidations {
			if strings.Contains(r.Message, crdMessage) {
				removed++
				continue
			}
			kept = append(kept, r)
		}
		specSchema.XValidations = kept
		crd.Spec.Versions[vi].Schema.OpenAPIV3Schema.Properties["spec"] = specSchema
	}
	if removed != 1 {
		t.Fatalf("removed %d rules from the Operation CRD, want exactly the create rule", removed)
	}
	must(t, c.Update(bg(), crd), "update the Operation CRD without its create rule")
	eventually(t, 60*time.Second, "the Operation CRD to be served without the create rule", func() (bool, error) {
		err := c.Create(bg(), withConfirm(person), client.DryRunAll)
		return deniedBy(err, ownerPin, "without spec.confirm"), nil
	})

	// Now the refusal is the pin's, for the agent and for the person, and nothing is stored.
	requireAgentIdentity(t, a)
	mine := withConfirm(a.username())
	requireDenied(t, "the agent's create with spec.confirm", a.c.Create(bg(), mine), ownerPin, "created without spec.confirm")
	requireDenied(t, "the person's create with spec.confirm", c.Create(bg(), withConfirm(person)), ownerPin, "created without spec.confirm")
	if err := c.Get(bg(), client.ObjectKeyFromObject(mine), &v1alpha1.Operation{}); !apierrors.IsNotFound(err) {
		t.Errorf("the refused Operation exists: %v", err)
	}
	// The control: the same Operation without a confirm is accepted by the pin.
	ok := agentOperation(ns, uniq("op"), a.username())
	must(t, a.c.Create(bg(), ok), "the agent's Operation without a confirm")
}
