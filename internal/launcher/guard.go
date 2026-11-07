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
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"

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

// InstallGuard makes the guard exist and be the manifest's, and applies it with server-side apply,
// policy first, so repeating it is safe. Objects that exist already are set to the manifest's spec
// first (resetSpecs); only then does the apply create what is missing.
func InstallGuard(ctx context.Context, cfg *rest.Config, log io.Writer) error {
	policy, binding, err := guardObjects()
	if err != nil {
		return err
	}
	dyn, mapper, err := newApplier(cfg)
	if err != nil {
		return err
	}
	if err := resetSpecs(ctx, dyn, policy, binding); err != nil {
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

// resetSpecs replaces the spec of each guard object that exists with the manifest's, whole, before the
// apply, so that what was added by hand does not survive a start. Server-side apply alone keeps a field
// the manifest does not mention when another manager owns it: a Warn in validationActions, a set that
// then holds Deny beside it and is refused ("must not contain both Deny and Warn"), or a
// matchResources.objectSelector that the CRDs do not satisfy, which leaves a binding that up reports and
// status finds, and that matches nothing. The objects are replaced in place, never deleted, so no moment
// is left without them. A conflicting write (the policy's status controller writes too) is retried.
func resetSpecs(ctx context.Context, dyn dynamic.Interface, objs ...*unstructured.Unstructured) error {
	for _, want := range objs {
		gvr := guardBindingGVR
		if want.GetKind() == "ValidatingAdmissionPolicy" {
			gvr = guardPolicyGVR
		}
		spec, _, err := unstructured.NestedMap(want.Object, "spec")
		if err != nil {
			return fmt.Errorf("the manifest's %s %s: %w", want.GetKind(), want.GetName(), err)
		}
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			cur, err := dyn.Resource(gvr).Get(ctx, want.GetName(), metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return nil // the apply creates it
			}
			if err != nil {
				return err
			}
			if got, _, _ := unstructured.NestedMap(cur.Object, "spec"); reflect.DeepEqual(got, spec) {
				return nil
			}
			if err := unstructured.SetNestedMap(cur.Object, runtime.DeepCopyJSON(spec), "spec"); err != nil {
				return err
			}
			_, err = dyn.Resource(gvr).Update(ctx, cur, metav1.UpdateOptions{FieldManager: fieldManager})
			return err
		})
		if err != nil {
			return fmt.Errorf("set the %s %s to the manifest's: %w", want.GetKind(), want.GetName(), err)
		}
	}
	return nil
}

// FrameworkGroup is the API group of the framework's CRDs, which the guard protects: the group the
// manifest's rule names. A test compares the two.
const FrameworkGroup = "hpcburst.groundnuty.github.io"

// guardEnforcementTimeout bounds WaitGuardEnforced.
const guardEnforcementTimeout = 60 * time.Second

var crdGVR = schemaGVR("apiextensions.k8s.io", "v1", "customresourcedefinitions")

// WaitGuardEnforced returns once the API server refuses a delete of a framework CRD, so that a delete
// right after up is refused. Admission learns of a policy and its binding after the objects exist, and
// the apply returns before that. The proof is a server-side dry-run delete of one framework CRD, which
// runs admission and removes nothing, and its refusal must be the guard's own. It is bounded, with an
// error naming the CRD if the API server never refuses. With no framework CRD yet there is nothing to
// delete and nothing to probe, and it says so and returns.
func WaitGuardEnforced(ctx context.Context, cfg *rest.Config, log io.Writer) error {
	dyn, _, err := newApplier(cfg)
	if err != nil {
		return err
	}
	return waitGuardEnforced(ctx, dyn, guardEnforcementTimeout, 100*time.Millisecond, log)
}

func waitGuardEnforced(ctx context.Context, dyn dynamic.Interface, timeout, interval time.Duration, log io.Writer) error {
	list, err := dyn.Resource(crdGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list the CustomResourceDefinitions to probe the guard with: %w", err)
	}
	var names []string
	for _, o := range list.Items {
		if g, _, _ := unstructured.NestedString(o.Object, "spec", "group"); g == FrameworkGroup {
			names = append(names, o.GetName())
		}
	}
	if len(names) == 0 {
		logf(log, "no CustomResourceDefinition of %s exists yet, so there is nothing to probe the guard with; it protects them once they exist", FrameworkGroup)
		return nil
	}
	slices.Sort(names)
	name := names[0]
	deadline := time.Now().Add(timeout)
	var last error
	for {
		err := dyn.Resource(crdGVR).Delete(ctx, name, metav1.DeleteOptions{DryRun: []string{metav1.DryRunAll}})
		if apierrors.IsForbidden(err) && strings.Contains(err.Error(), "refusing to delete the custom resource definition "+name) {
			logf(log, "the API server refuses deleting %s: the guard is enforced", name)
			return nil
		}
		last = err
		if time.Now().After(deadline) || ctx.Err() != nil {
			if last == nil {
				last = errors.New("the dry-run delete was accepted")
			}
			return fmt.Errorf("the guard was applied, but the API server still did not refuse a delete of the custom resource definition %s after %s (%v)", name, timeout, last)
		}
		select {
		case <-ctx.Done():
		case <-time.After(interval):
		}
	}
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
	wantPolicy, wantBinding, err := guardObjects()
	if err != nil {
		return nil, err
	}
	var missing []string
	p, err := dyn.Resource(guardPolicyGVR).Get(ctx, GuardName, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		missing = append(missing, "the ValidatingAdmissionPolicy "+GuardName+" is not there")
	case err != nil:
		return nil, fmt.Errorf("read the ValidatingAdmissionPolicy %s: %w", GuardName, err)
	default:
		// The server fills defaults in the nested parts of a policy, so only its top-level fields are
		// compared: one the manifest does not set (matchConditions, paramKind, variables) can switch the
		// policy off.
		if extra := extraKeys(p, wantPolicy); len(extra) > 0 {
			missing = append(missing, fmt.Sprintf("the policy has spec fields the manifest does not set (%s), which can switch it off", strings.Join(extra, ", ")))
		}
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
		if actions, _, _ := unstructured.NestedStringSlice(b.Object, "spec", "validationActions"); !slices.Equal(actions, []string{"Deny"}) {
			missing = append(missing, fmt.Sprintf("the binding's validationActions %v are not just Deny", actions))
		}
		// The binding's spec is the manifest's, whole: a match filter or a parameter reference narrows it,
		// and a filter the CRDs do not satisfy leaves a binding that exists and matches nothing.
		if extra := extraKeys(b, wantBinding); len(extra) > 0 {
			missing = append(missing, fmt.Sprintf("the binding has spec fields the manifest does not set (%s), which can narrow it to nothing", strings.Join(extra, ", ")))
		}
	}
	if len(missing) > 0 {
		return &GuardState{Detail: strings.Join(missing, "; ")}, nil
	}
	return &GuardState{Present: true}, nil
}

// extraKeys names the top-level spec fields of got that want does not have, sorted.
func extraKeys(got, want *unstructured.Unstructured) []string {
	g, _, _ := unstructured.NestedMap(got.Object, "spec")
	w, _, _ := unstructured.NestedMap(want.Object, "spec")
	var extra []string
	for k := range g {
		if _, ok := w[k]; !ok {
			extra = append(extra, "spec."+k)
		}
	}
	slices.Sort(extra)
	return extra
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
