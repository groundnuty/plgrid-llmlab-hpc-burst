package launcher

// The CRD deletion guard (HBEP-11, Risks and Admission; issue #242). Deleting a framework custom
// resource definition deletes every resource of that kind, and the Allocations' finalizers would
// cancel every provider job. config/admission/crd-deletion-guard.yaml holds a
// ValidatingAdmissionPolicy that refuses that, with the binding that makes it Deny. up installs
// both on every start, whatever --manifests names; removing them is an explicit step
// (hpcburst guard remove), never a side effect of down, upgrade or a restart; status reports
// whether they are there.
//
// What counts as "present" is what stops a delete: the policy, and a binding that names it and
// denies. The policy alone does nothing, and the documented by-hand removal deletes only the
// binding, so a check of the policy would report a guard that is not there.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/config/admission"
)

// GuardName is the name of both guard objects, the policy and its binding.
const GuardName = "hpcburst-protect-crds"

var (
	guardPolicyGVR  = schemaGVR("admissionregistration.k8s.io", "v1", "validatingadmissionpolicies")
	guardBindingGVR = schemaGVR("admissionregistration.k8s.io", "v1", "validatingadmissionpolicybindings")
)

// guardObjects decodes the embedded manifest and returns its policy and its binding. It refuses a
// manifest that holds anything else, so that a change to the file that the launcher does not
// understand stops here and not at a start.
func guardObjects() (policy, binding *unstructured.Unstructured, err error) {
	objs, err := decodeManifests(bytes.NewReader(admission.CRDDeletionGuard), "config/admission/crd-deletion-guard.yaml")
	if err != nil {
		return nil, nil, err
	}
	for _, o := range objs {
		switch o.GetKind() {
		case "ValidatingAdmissionPolicy":
			policy = o
		case "ValidatingAdmissionPolicyBinding":
			binding = o
		default:
			return nil, nil, fmt.Errorf("the guard manifest holds a %s %s, want only the policy and its binding", o.GetKind(), o.GetName())
		}
	}
	if len(objs) != 2 || policy == nil || binding == nil || policy.GetName() != GuardName || binding.GetName() != GuardName {
		return nil, nil, fmt.Errorf("the guard manifest holds %d objects, want one ValidatingAdmissionPolicy and one ValidatingAdmissionPolicyBinding, both named %s", len(objs), GuardName)
	}
	return policy, binding, nil
}

// InstallGuard applies the guard with server-side apply, policy first, so repeating it is safe.
func InstallGuard(ctx context.Context, cfg *rest.Config, log io.Writer) error {
	policy, binding, err := guardObjects()
	if err != nil {
		return err
	}
	dyn, mapper, err := newApplier(cfg)
	if err != nil {
		return err
	}
	if err := normalizeBinding(ctx, dyn, binding); err != nil {
		return err
	}
	for _, o := range []*unstructured.Unstructured{policy, binding} {
		if err := applyOne(ctx, dyn, mapper, o); err != nil {
			return err
		}
		logf(log, "applied %s %s", o.GetKind(), o.GetName())
	}
	return nil
}

// normalizeBinding sets the policy name and the actions of a binding that exists to what the
// manifest says, before the apply. validationActions is a set: a server-side apply adds Deny to a
// Warn that someone put there by hand (a field of another manager), and the API server refuses the
// pair ("must not contain both Deny and Warn"), so up would fail on a host whose binding was
// weakened. The binding is replaced in place, never deleted, so no moment is left without it.
func normalizeBinding(ctx context.Context, dyn dynamic.Interface, want *unstructured.Unstructured) error {
	cur, err := dyn.Resource(guardBindingGVR).Get(ctx, want.GetName(), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read the ValidatingAdmissionPolicyBinding %s: %w", want.GetName(), err)
	}
	wantName, _, _ := unstructured.NestedString(want.Object, "spec", "policyName")
	wantActions, _, _ := unstructured.NestedStringSlice(want.Object, "spec", "validationActions")
	gotName, _, _ := unstructured.NestedString(cur.Object, "spec", "policyName")
	gotActions, _, _ := unstructured.NestedStringSlice(cur.Object, "spec", "validationActions")
	if gotName == wantName && slices.Equal(gotActions, wantActions) {
		return nil
	}
	if err := unstructured.SetNestedField(cur.Object, wantName, "spec", "policyName"); err != nil {
		return err
	}
	if err := unstructured.SetNestedStringSlice(cur.Object, wantActions, "spec", "validationActions"); err != nil {
		return err
	}
	if _, err := dyn.Resource(guardBindingGVR).Update(ctx, cur, metav1.UpdateOptions{FieldManager: fieldManager}); err != nil {
		return fmt.Errorf("reset the ValidatingAdmissionPolicyBinding %s to the manifest's: %w", want.GetName(), err)
	}
	return nil
}

// RemoveGuard deletes the binding, then the policy. An object that is already gone is not an
// error, so the removal can be repeated. It returns whether it deleted anything.
func RemoveGuard(ctx context.Context, cfg *rest.Config, log io.Writer) (removed bool, err error) {
	dyn, _, err := newApplier(cfg)
	if err != nil {
		return false, err
	}
	return removeGuard(ctx, dyn, log)
}

func removeGuard(ctx context.Context, dyn dynamic.Interface, log io.Writer) (removed bool, err error) {
	for _, step := range []struct {
		kind string
		gvr  schema.GroupVersionResource
	}{
		{"ValidatingAdmissionPolicyBinding", guardBindingGVR},
		{"ValidatingAdmissionPolicy", guardPolicyGVR},
	} {
		switch err := dyn.Resource(step.gvr).Delete(ctx, GuardName, metav1.DeleteOptions{}); {
		case err == nil:
			removed = true
			logf(log, "deleted %s %s", step.kind, GuardName)
		case apierrors.IsNotFound(err):
		default:
			return removed, fmt.Errorf("delete %s %s: %w", step.kind, GuardName, err)
		}
	}
	return removed, nil
}

// GuardState is what the API server says about the guard.
type GuardState struct {
	// Present is true when the policy exists and a binding that names it denies.
	Present bool `json:"present"`
	// Detail says what is missing when Present is false.
	Detail string `json:"detail,omitempty"`
}

// CheckGuard asks the API server for the policy and the binding. An error is a failure to ask,
// never an absent guard.
func CheckGuard(ctx context.Context, cfg *rest.Config) (*GuardState, error) {
	dyn, _, err := newApplier(cfg)
	if err != nil {
		return nil, err
	}
	return checkGuard(ctx, dyn)
}

func checkGuard(ctx context.Context, dyn dynamic.Interface) (*GuardState, error) {
	var missing []string
	if _, err := dyn.Resource(guardPolicyGVR).Get(ctx, GuardName, metav1.GetOptions{}); apierrors.IsNotFound(err) {
		missing = append(missing, "the ValidatingAdmissionPolicy "+GuardName+" is not there")
	} else if err != nil {
		return nil, fmt.Errorf("read the ValidatingAdmissionPolicy %s: %w", GuardName, err)
	}
	b, err := dyn.Resource(guardBindingGVR).Get(ctx, GuardName, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		missing = append(missing, "the ValidatingAdmissionPolicyBinding "+GuardName+" is not there")
	case err != nil:
		return nil, fmt.Errorf("read the ValidatingAdmissionPolicyBinding %s: %w", GuardName, err)
	default:
		if name, _, _ := unstructured.NestedString(b.Object, "spec", "policyName"); name != GuardName {
			missing = append(missing, fmt.Sprintf("the binding names the policy %q, want %q", name, GuardName))
		}
		if actions, _, _ := unstructured.NestedStringSlice(b.Object, "spec", "validationActions"); !slices.Contains(actions, "Deny") {
			missing = append(missing, fmt.Sprintf("the binding's validationActions %v do not include Deny", actions))
		}
	}
	if len(missing) > 0 {
		return &GuardState{Detail: strings.Join(missing, "; ")}, nil
	}
	return &GuardState{Present: true}, nil
}

// GuardOptions are the inputs of RemoveCRDGuard.
type GuardOptions struct {
	StateDir string
	Log      io.Writer
}

// RemoveCRDGuard is the explicit removal: hpcburst guard remove. It needs the host to run, because
// the guard lives in its API server. It refuses when the state directory records no host.
func RemoveCRDGuard(ctx context.Context, o GuardOptions) (removed bool, err error) {
	l, err := newLayout(o.StateDir)
	if err != nil {
		return false, err
	}
	st, err := l.loadState()
	if err != nil {
		return false, err
	}
	if st == nil {
		return false, errNotRunning
	}
	cfg, err := restConfig(l.kubeconfig(), st.Ports, 30*time.Second)
	if err != nil {
		return false, err
	}
	return RemoveGuard(ctx, cfg, o.Log)
}
