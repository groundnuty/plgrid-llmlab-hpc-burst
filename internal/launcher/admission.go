package launcher

// The three admission policies of HBEP-10 (issue #589, PR 3) and the order in which up opens the agents'
// writes. config/admission holds the policies: the capacity guard (what an agent may change in a
// ModelDeployment, and nothing the controllers act on), the owner pin (who owns a ModelDeployment or
// an Operation, and who confirms a plan) and the approver pin (an Approval only from its approver's own
// identity). up installs them on every start, as it does the CRD deletion guard, resets each to the
// manifest (guard.go works on a list of guards), and checks them in status.
//
// The agent role gains its writes only once the policy that bounds them is ENFORCED, and "installed" is
// not "enforced": the API server learns of a policy after its create returns (a refused edit went through
// in one probe of the plan, 15 Nov 2026). So the window is closed, not shortened. InstallAgentAccess
// does, in this order, what up needs between the CRD guard and the controllers:
//
//  1. the three policies, installed or reset;
//  2. the agent role at its NARROW rules (config/rbac/agents-narrow.yaml), which are read-only. On a
//     restart this narrows a role an earlier up widened, for the length of the wait, the safe direction;
//  3. a bounded wait until each policy refuses its dry-run probe;
//  4. only then, the agent role at its WIDE rules (config/rbac/agents.yaml).
//
// A probe that never refuses fails the step with an error naming the policy, and the role stays narrow.
// A policy that cannot be probed, because the CRD of its probe's kind is absent, leaves the role narrow
// too, with a warning and no failure: a later up that finds the CRD probes it and widens the role.

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/config/admission"
)

// The names of the three policies, each also the name of its binding.
const (
	CapacityGuardName = "hpcburst-capacity-guard"
	OwnerPinName      = "hpcburst-owner-pin"
	ApproverPinName   = "hpcburst-approver-pin"
)

// admissionProbeNamespace is where the probes' dry-run creates are made. It always exists.
const admissionProbeNamespace = "default"

// The probes' identities. None of them is a real user: the owner pin's probe names an owner and the
// approver pin's an approver that is not the admin who sends it, and the capacity guard's is sent by
// the admin impersonating the agent below.
const (
	probeGuardUser       = "system:serviceaccount:" + AgentsNamespace + ":hpcburst-guard-probe"
	probeForeignOwner    = "hpcburst-probe-owner"
	probeForeignApprover = "hpcburst-probe-approver"
)

// agentWritesOffWarning is the warning of an up that left the agent role narrow because a policy could not
// be probed.
const agentWritesOffWarning = "warning: the agents' writes stay off: an admission policy could not be probed because its custom resource definition does not exist, so the role " + AgentRoleName +
	" is the read-only one; a later hpcburst up that finds the ModelDeployment, Operation and Approval definitions probes the policies and widens it"

// admissionPolicy is one of the three policies, with the dry run that shows it enforced.
type admissionPolicy struct {
	guardDef
	// crd is the CRD whose kind the probe creates; without it there is nothing to probe with.
	crd string
	// attempt makes the probe: a server-side dry-run create that the policy must refuse. cfg is the
	// admin's.
	attempt func(ctx context.Context, cfg *rest.Config) error
}

var admissionPolicies = []admissionPolicy{
	{
		guardDef: guardDef{name: CapacityGuardName, file: "config/admission/capacity-guard.yaml", manifest: admission.CapacityGuard},
		crd:      "modeldeployments." + FrameworkGroup,
		attempt:  probeCapacityGuard,
	},
	{
		guardDef: guardDef{name: OwnerPinName, file: "config/admission/owner-pin.yaml", manifest: admission.OwnerPin},
		crd:      "operations." + FrameworkGroup,
		attempt:  probeOwnerPin,
	},
	{
		guardDef: guardDef{name: ApproverPinName, file: "config/admission/approver-pin.yaml", manifest: admission.ApproverPin},
		crd:      "approvals." + FrameworkGroup,
		attempt:  probeApproverPin,
	},
}

// AdmissionPolicyNames returns the names of the three policies, in the order up installs them.
func AdmissionPolicyNames() []string {
	var out []string
	for _, p := range admissionPolicies {
		out = append(out, p.name)
	}
	return out
}

func admissionDefs() []guardDef {
	var out []guardDef
	for _, p := range admissionPolicies {
		out = append(out, p.guardDef)
	}
	return out
}

// InstallAdmission makes the three policies and their bindings exist and be the manifests', with
// server-side apply, policy first, resetting each existing object's spec to the manifest's first (as
// InstallGuard does). It waits for nothing: WaitAdmissionEnforced does.
func InstallAdmission(ctx context.Context, cfg *rest.Config, log io.Writer) error {
	return installGuards(ctx, cfg, log, admissionDefs()...)
}

// AdmissionState is what the API server says about one of the three policies: Present is true when the
// policy exists and a binding that names it denies.
type AdmissionState struct {
	Name string `json:"name"`
	GuardState
}

// CheckAdmission asks the API server for each policy and its binding, in InstallAdmission's order. An
// error is a failure to ask, never an absent policy.
func CheckAdmission(ctx context.Context, cfg *rest.Config) ([]AdmissionState, error) {
	dyn, _, err := newApplier(cfg)
	if err != nil {
		return nil, err
	}
	var out []AdmissionState
	for _, p := range admissionPolicies {
		gs, err := p.check(ctx, dyn)
		if err != nil {
			return nil, err
		}
		out = append(out, AdmissionState{Name: p.name, GuardState: *gs})
	}
	return out, nil
}

// ProbeAdmission makes the dry-run probe of one policy (by name) once. refused is true when the API
// server refused it with the policy's own message. Otherwise err says why not: nil when the API server
// accepted the probe (the policy is not enforced), or whatever else it answered, a refusal by another
// policy or by the CRD's schema included. A policy of another name is an error.
func ProbeAdmission(ctx context.Context, cfg *rest.Config, name string) (refused bool, err error) {
	for _, p := range admissionPolicies {
		if p.name == name {
			return p.probe(ctx, cfg)
		}
	}
	return false, fmt.Errorf("%q is not one of the admission policies %v", name, AdmissionPolicyNames())
}

func (p admissionPolicy) probe(ctx context.Context, cfg *rest.Config) (bool, error) {
	err := p.attempt(ctx, cfg)
	if apierrors.IsForbidden(err) && strings.Contains(err.Error(), "ValidatingAdmissionPolicy '"+p.name+"'") {
		return true, nil
	}
	return false, err
}

// probeCapacityGuard is the admin's dry-run create of a schema-valid ModelDeployment that carries an
// ownerReference, made as an agent: the admin impersonates a ServiceAccount of the agents' namespace
// with its groups and also system:masters. The narrow agent role is read-only, so RBAC would refuse the
// create before admission; system:masters lets it past RBAC, and the agents' group still puts it under
// the guard. Only an identity that may impersonate can send it, and the admin is the only one on the
// host. The object carries the owner annotation set to the impersonated name and a short purpose, so the
// owner pin passes it and the refusal is the capacity guard's alone.
func probeCapacityGuard(ctx context.Context, cfg *rest.Config) error {
	icfg := rest.CopyConfig(cfg)
	icfg.Impersonate = rest.ImpersonationConfig{
		UserName: probeGuardUser,
		Groups:   []string{"system:serviceaccounts", AgentsGroup, "system:authenticated", "system:masters"},
	}
	dyn, err := dynamic.NewForConfig(icfg)
	if err != nil {
		return err
	}
	md := &v1alpha1.ModelDeployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "hpcburst-guard-probe", Namespace: admissionProbeNamespace,
			Annotations: map[string]string{
				v1alpha1.AnnotationOwner:   probeGuardUser,
				v1alpha1.AnnotationPurpose: "the launcher's dry-run probe of the capacity guard; it is never stored",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1", Kind: "ConfigMap", Name: "hpcburst-guard-probe",
				UID: types.UID("00000000-0000-0000-0000-000000000000"),
			}},
		},
		Spec: v1alpha1.ModelDeploymentSpec{
			Model: "probe-model",
			Source: v1alpha1.ModelSource{Served: &v1alpha1.ServingSpec{
				Checkpoint: v1alpha1.CheckpointSpec{Repository: "example-org/example-1b", Revision: "0123456789abcdef0123456789abcdef01234567"},
				Engine: v1alpha1.EngineSpec{
					Name: "vllm", Version: "0.32.0", Image: "docker.io/vllm/vllm-openai",
					ImageDigest: "sha256:2222222222222222222222222222222222222222222222222222222222222222",
				},
			}},
			Instances: 1,
			Providers: v1alpha1.ProviderSelection{Names: []string{"probe"}},
			Workload: &v1alpha1.Workload{
				ContextTokens: v1alpha1.ContextTokens{Typical: 1024}, OutputTokens: v1alpha1.OutputTokens{Typical: 128}, TargetSessions: 1,
			},
		},
	}
	return dryRunCreate(ctx, dyn, md, "modeldeployments")
}

// probeOwnerPin is the admin's dry-run create of an Operation whose spec.owner is not the admin. The
// pin applies to every identity, so no impersonation is needed.
func probeOwnerPin(ctx context.Context, cfg *rest.Config) error {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return err
	}
	op := &v1alpha1.Operation{
		ObjectMeta: metav1.ObjectMeta{Name: "hpcburst-owner-pin-probe", Namespace: admissionProbeNamespace},
		Spec: v1alpha1.OperationSpec{
			Action:             v1alpha1.OperationActionScale,
			ModelDeploymentRef: v1alpha1.ObjectReference{Name: "hpcburst-probe"},
			Scale:              &v1alpha1.ScaleParameters{Instances: 0},
			Owner:              probeForeignOwner,
		},
	}
	return dryRunCreate(ctx, dyn, op, "operations")
}

// probeApproverPin is the admin's dry-run create of an Approval that names another approver.
func probeApproverPin(ctx context.Context, cfg *rest.Config) error {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return err
	}
	ap := &v1alpha1.Approval{
		ObjectMeta: metav1.ObjectMeta{Name: "hpcburst-approver-pin-probe", Namespace: admissionProbeNamespace},
		Spec: v1alpha1.ApprovalSpec{
			Operation: v1alpha1.OperationReference{Name: "hpcburst-probe", UID: types.UID("00000000-0000-0000-0000-000000000000")},
			PlanID:    "0123456789abcdef",
			Approver:  v1alpha1.ApproverIdentity{Username: probeForeignApprover},
		},
	}
	return dryRunCreate(ctx, dyn, ap, "approvals")
}

// dryRunCreate sends obj as a server-side dry-run create, which runs admission and stores nothing.
func dryRunCreate(ctx context.Context, dyn dynamic.Interface, obj runtime.Object, resource string) error {
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return err
	}
	u := &unstructured.Unstructured{Object: m}
	kind := map[string]string{"modeldeployments": "ModelDeployment", "operations": "Operation", "approvals": "Approval"}[resource]
	u.SetAPIVersion(v1alpha1.GroupVersion.String())
	u.SetKind(kind)
	gvr := v1alpha1.GroupVersion.WithResource(resource)
	_, err = dyn.Resource(gvr).Namespace(admissionProbeNamespace).Create(ctx, u, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	return err
}

// WaitAdmissionEnforced returns once each of the three policies has refused its dry-run probe with its
// own message, so that a write right after is bounded. It is bounded (timeout, one budget for the three),
// with an error naming the policy that never refused. A policy whose probe needs a CRD that does not exist
// is not probed: its name is returned in unprobed, with a line in the log saying there is nothing to
// probe with, as WaitGuardEnforced says for the CRD guard.
func WaitAdmissionEnforced(ctx context.Context, cfg *rest.Config, timeout time.Duration, log io.Writer) (unprobed []string, err error) {
	dyn, _, err := newApplier(cfg)
	if err != nil {
		return nil, err
	}
	return waitAdmissionEnforced(ctx, cfg, dyn, admissionPolicies, timeout, 100*time.Millisecond, log)
}

func waitAdmissionEnforced(ctx context.Context, cfg *rest.Config, dyn dynamic.Interface, policies []admissionPolicy, timeout, interval time.Duration, log io.Writer) (unprobed []string, err error) {
	deadline := time.Now().Add(timeout)
	for _, p := range policies {
		_, err := dyn.Resource(crdGVR).Get(ctx, p.crd, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			logf(log, "the custom resource definition %s does not exist yet, so there is nothing to probe %s with; it is not known to be enforced", p.crd, p.name)
			unprobed = append(unprobed, p.name)
			continue
		case err != nil:
			return unprobed, fmt.Errorf("read the custom resource definition %s to probe %s with: %w", p.crd, p.name, err)
		}
		var last error
		for {
			refused, perr := p.probe(ctx, cfg)
			if refused {
				logf(log, "the API server refuses the probe of %s: the policy is enforced", p.name)
				break
			}
			last = perr
			if time.Now().After(deadline) || ctx.Err() != nil {
				if last == nil {
					last = errors.New("the dry-run probe was accepted")
				}
				return unprobed, fmt.Errorf("the policy %s was applied, but the API server still did not refuse its dry-run probe after %s (%v)", p.name, timeout, last)
			}
			select {
			case <-ctx.Done():
			case <-time.After(interval):
			}
		}
	}
	return unprobed, nil
}

// AgentAccessOptions are the inputs of InstallAgentAccess. The zero value is what up uses.
type AgentAccessOptions struct {
	// EnforcementTimeout bounds the wait for the policies; zero means guardEnforcementTimeout (60 s).
	EnforcementTimeout time.Duration
	// BeforeWait, set by tests only, runs after the narrow role is applied and before the wait begins,
	// and the wait does not begin until it returns: a test holds the window open in it, and looks at what
	// an agent may do while the policies are not yet known to be enforced.
	BeforeWait func(ctx context.Context) error
}

// InstallAgentAccess does what up needs between the CRD deletion guard and the controllers, in the order
// that closes the window (see the top of this file): the policies, the narrow agent role, the bounded
// wait, and only then the wide agent role. writes reports whether the wide role was applied. It is false
// without an error when a policy could not be probed (a CRD of its kind is absent), and the role is then
// still the narrow one; a probe that never refuses is an error naming the policy, with the role still
// the narrow one.
func InstallAgentAccess(ctx context.Context, cfg *rest.Config, log io.Writer, o AgentAccessOptions) (writes bool, err error) {
	return installAgentAccess(ctx, agentAccessSteps{
		policies: func(ctx context.Context) error { return InstallAdmission(ctx, cfg, log) },
		narrow:   func(ctx context.Context) error { return InstallAgentRBACNarrow(ctx, cfg, log) },
		wait: func(ctx context.Context, timeout time.Duration) ([]string, error) {
			return WaitAdmissionEnforced(ctx, cfg, timeout, log)
		},
		wide: func(ctx context.Context) error { return InstallAgentRBAC(ctx, cfg, log) },
		log:  log,
	}, o)
}

// agentAccessSteps are the four steps of InstallAgentAccess, so that a test can record their order.
type agentAccessSteps struct {
	policies func(ctx context.Context) error
	narrow   func(ctx context.Context) error
	wait     func(ctx context.Context, timeout time.Duration) (unprobed []string, err error)
	wide     func(ctx context.Context) error
	log      io.Writer
}

func installAgentAccess(ctx context.Context, st agentAccessSteps, o AgentAccessOptions) (writes bool, err error) {
	if err := st.policies(ctx); err != nil {
		return false, err
	}
	if err := st.narrow(ctx); err != nil {
		return false, err
	}
	if o.BeforeWait != nil {
		if err := o.BeforeWait(ctx); err != nil {
			return false, err
		}
	}
	unprobed, err := st.wait(ctx, cmp.Or(o.EnforcementTimeout, guardEnforcementTimeout))
	if err != nil {
		return false, err
	}
	if len(unprobed) > 0 {
		logf(st.log, "WARNING: %s could not be probed, so the agents' writes stay off: the role %s is the narrow, read-only one until a later up finds the custom resource definitions and probes the policies",
			strings.Join(unprobed, ", "), AgentRoleName)
		return false, nil
	}
	if err := st.wide(ctx); err != nil {
		return false, err
	}
	return true, nil
}
