package e2e

// The launcher's install, check and removal of the CRD deletion guard (internal/launcher/guard.go;
// issue #242), against a real API server that evaluates ValidatingAdmissionPolicies. Every judgement
// is the API server's answer to a delete of a framework CRD: that the policy object exists proves
// nothing about whether it stops one.
//
// The deletes are dry runs: admission runs, nothing is removed, and a delete that slipped past a
// guard that is not yet active does not remove a CRD of this suite. One real delete follows, only
// after the dry run has been refused.

import (
	"io"
	"strings"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

var bindingGVR = schema.GroupVersionResource{Group: "admissionregistration.k8s.io", Version: "v1", Resource: "validatingadmissionpolicybindings"}

func TestLauncherInstallsChecksAndRemovesTheGuard(t *testing.T) {
	const ours = "providers.hpcburst.groundnuty.github.io"
	if state, err := launcher.CheckGuard(bg(), cfg); err != nil || state.Present {
		t.Skipf("a guard is installed on this host already (%+v, %v): this test starts from none", state, err)
	}
	// However the test ends, no guard stays behind, and the host allows the delete again before the
	// next test (the API server learns of a removed policy asynchronously, too).
	t.Cleanup(func() {
		if _, err := launcher.RemoveGuard(bg(), cfg, io.Discard); err != nil {
			t.Errorf("clean-up: %v", err)
		}
		eventually(t, 60*time.Second, "the clean-up's removal to take effect", func() (bool, error) {
			return dryRunDelete(ours) == nil, nil
		})
	})

	dyn, err := dynamic.NewForConfig(cfg)
	must(t, err, "dynamic client")
	bindings := dyn.Resource(bindingGVR)

	// Before: a delete is accepted, so the refusal below is the guard's doing.
	must(t, dryRunDelete(ours), "dry-run delete of a framework CRD before the guard")
	if state, err := launcher.CheckGuard(bg(), cfg); err != nil || state.Present || state.Detail == "" {
		t.Fatalf("CheckGuard before the install: %+v, %v, want absent with a detail", state, err)
	}

	// Install, twice: server-side apply repeats safely. Then the API server refuses the delete, with
	// the reason that names the way out, and CheckGuard agrees.
	must(t, launcher.InstallGuard(bg(), cfg, io.Discard), "install the guard")
	must(t, launcher.InstallGuard(bg(), cfg, io.Discard), "install the guard again")
	var refusal error
	eventually(t, 60*time.Second, "the installed guard to refuse deleting a framework CRD", func() (bool, error) {
		refusal = dryRunDelete(ours)
		return refusal != nil, nil
	})
	if !apierrors.IsForbidden(refusal) {
		t.Fatalf("want a Forbidden refusal, got %T: %v", refusal, refusal)
	}
	for _, want := range []string{"refusing to delete the custom resource definition " + ours, "hpcburst guard remove"} {
		if !strings.Contains(refusal.Error(), want) {
			t.Errorf("the refusal %q does not contain %q", refusal, want)
		}
	}
	err = c.Delete(bg(), &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: ours}})
	if !apierrors.IsForbidden(err) {
		t.Fatalf("a real delete of a framework CRD: %v, want Forbidden", err)
	}
	if state, err := launcher.CheckGuard(bg(), cfg); err != nil || !state.Present {
		t.Fatalf("CheckGuard after the install: %+v, %v, want present", state, err)
	}

	// A binding that does not deny is not a guard: CheckGuard says absent, and the API server
	// agrees by accepting the delete.
	_, err = bindings.Patch(bg(), launcher.GuardName, types.MergePatchType, []byte(`{"spec":{"validationActions":["Warn"]}}`), metav1.PatchOptions{})
	must(t, err, "weaken the binding to Warn")
	if state, err := launcher.CheckGuard(bg(), cfg); err != nil || state.Present || !strings.Contains(state.Detail, "Deny") {
		t.Errorf("CheckGuard with a Warn binding: %+v, %v, want absent naming Deny", state, err)
	}
	eventually(t, 60*time.Second, "a Warn binding to let the delete through", func() (bool, error) {
		return dryRunDelete(ours) == nil, nil
	})
	// Installing again puts Deny back.
	must(t, launcher.InstallGuard(bg(), cfg, io.Discard), "install over the weakened binding")
	eventually(t, 60*time.Second, "the reinstalled guard to refuse again", func() (bool, error) {
		return apierrors.IsForbidden(dryRunDelete(ours)), nil
	})

	// The old by-hand step deleted only the binding: the policy is still there, and CheckGuard,
	// which would be fooled by a look at the policy alone, says absent.
	must(t, bindings.Delete(bg(), launcher.GuardName, metav1.DeleteOptions{}), "delete the binding by hand")
	if state, err := launcher.CheckGuard(bg(), cfg); err != nil || state.Present || !strings.Contains(state.Detail, "ValidatingAdmissionPolicyBinding") {
		t.Errorf("CheckGuard with the binding gone: %+v, %v, want absent naming the binding", state, err)
	}
	eventually(t, 60*time.Second, "the delete to be allowed with the binding gone", func() (bool, error) {
		return dryRunDelete(ours) == nil, nil
	})
	must(t, launcher.InstallGuard(bg(), cfg, io.Discard), "install after the by-hand removal")
	eventually(t, 60*time.Second, "the reinstalled guard to refuse once more", func() (bool, error) {
		return apierrors.IsForbidden(dryRunDelete(ours)), nil
	})

	// The explicit removal: the delete is allowed, CheckGuard says absent, and a second removal is a
	// success that removes nothing.
	removed, err := launcher.RemoveGuard(bg(), cfg, io.Discard)
	if err != nil || !removed {
		t.Fatalf("RemoveGuard: removed %v, %v, want it to remove the guard", removed, err)
	}
	if state, err := launcher.CheckGuard(bg(), cfg); err != nil || state.Present {
		t.Errorf("CheckGuard after the removal: %+v, %v, want absent", state, err)
	}
	eventually(t, 60*time.Second, "the delete to be allowed after the removal", func() (bool, error) {
		return dryRunDelete(ours) == nil, nil
	})
	removed, err = launcher.RemoveGuard(bg(), cfg, io.Discard)
	if err != nil || removed {
		t.Errorf("a second RemoveGuard: removed %v, %v, want a success that removed nothing", removed, err)
	}
}
