package e2e

// What the admission tests share (issue #589, PR 3; HBEP-10, The capacity guard, Owner and purpose,
// Confirm, Approvals). Every case is made with a REAL agent token (newAgents) and judged by the API server's
// own answer, which must name the policy: a bare Forbidden is RBAC's as well, and proves nothing about a
// policy. Before each case the test asks the API server who the client is (requireAgentIdentity), so a
// case that passes cannot have run as the admin or a controller, which are outside the capacity guard.
// The person (the host's admin) is the control beside the agents, never the subject.
//
// The activation window (plan section 1) is closed before the first case: newAgents opens the agents'
// access through the launcher's ordered step, whose bounded wait ends only when each policy has refused
// its dry-run probe. openAgentAccess is the single place that does it.

import (
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

// The policies' names, written here and not read from the launcher: the test states what HBEP-10 says.
const (
	capacityGuard = "hpcburst-capacity-guard"
	ownerPin      = "hpcburst-owner-pin"
	approverPin   = "hpcburst-approver-pin"
)

var (
	mdResource = v1alpha1.GroupVersion.WithResource("modeldeployments")
	mdGroupRes = schema.GroupResource{Group: frameworkGroup, Resource: "modeldeployments"}
)

// denial is what a refusal by a ValidatingAdmissionPolicy looks like to the client: Forbidden, naming the
// policy and its binding, then the policy's message.
func policyDenial(policy string) string {
	return "ValidatingAdmissionPolicy '" + policy + "' with binding '" + policy + "' denied request: "
}

// deniedBy reports whether err is the named policy's refusal and, when msg is not empty, carries msg.
func deniedBy(err error, policy, msg string) bool {
	return err != nil && apierrors.IsForbidden(err) && strings.Contains(err.Error(), policyDenial(policy)) && strings.Contains(err.Error(), msg)
}

// requireDenied fails unless err is the named policy's refusal with msg in it. A bare Forbidden, which
// RBAC returns as well, is not enough: it would pass on a host where the policy is not installed.
func requireDenied(t *testing.T, what string, err error, policy, msg string) {
	t.Helper()
	if !deniedBy(err, policy, msg) {
		t.Errorf("%s: %v, want a Forbidden that names the policy %s and contains %q", what, err, policy, msg)
	}
}

// requireAccepted fails when err is not nil.
func requireAccepted(t *testing.T, what string, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("%s: %v, want it accepted", what, err)
	}
}

// the person is the host's admin: the identity the API server reports for the admin kubeconfig.
func thePerson(t *testing.T) string {
	t.Helper()
	cs, _ := clientsFromConfig(t, cfg)
	return whoAmI(t, cs).Username
}

// agentMD returns a ModelDeployment that agent a may create: its own owner annotation, a purpose, no
// ownerReferences, and every optional field the allowlist treats alike populated, so that a change to any
// of them is a change and not an addition.
func agentMD(a *agent, ns, name string) *v1alpha1.ModelDeployment {
	md := newModelDeployment(ns, name)
	md.Annotations = map[string]string{v1alpha1.AnnotationOwner: a.username(), v1alpha1.AnnotationPurpose: "a test deployment"}
	md.Spec.Instances = 3
	md.Spec.Routing = &v1alpha1.Routing{MaxRequestDuration: ptr.To(v1alpha1.Duration("30m"))}
	md.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep}
	md.Spec.LeaseLimit = ptr.To(v1alpha1.Duration("8h"))
	md.Spec.HedgeWidth = ptr.To[int32](2)
	return md
}

// createMD makes agent a create a ModelDeployment as itself (its real token), and returns what is stored.
func createMD(t *testing.T, a *agent, md *v1alpha1.ModelDeployment) *v1alpha1.ModelDeployment {
	t.Helper()
	requireAgentIdentity(t, a)
	must(t, a.c.Create(bg(), md), a.name+"'s create of "+md.Name)
	return storedMD(t, md)
}

// storedMD reads the ModelDeployment as the admin: what the API server holds, whoever asks.
func storedMD(t *testing.T, md *v1alpha1.ModelDeployment) *v1alpha1.ModelDeployment {
	t.Helper()
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), client.ObjectKeyFromObject(md), got), "read "+md.Name+" as the admin")
	return got
}

// mdAs reads md as agent a, so the edit a test then makes carries the stored resourceVersion.
func mdAs(t *testing.T, a *agent, md *v1alpha1.ModelDeployment) *v1alpha1.ModelDeployment {
	t.Helper()
	got := &v1alpha1.ModelDeployment{}
	must(t, a.c.Get(bg(), client.ObjectKeyFromObject(md), got), a.name+"'s read of "+md.Name)
	return got
}

// requireUntouched fails unless the stored object still has the resourceVersion it had: a refused write
// stored nothing.
func requireUntouched(t *testing.T, before *v1alpha1.ModelDeployment, what string) {
	t.Helper()
	if after := storedMD(t, before); after.ResourceVersion != before.ResourceVersion {
		t.Errorf("%s: %s changed after a refused write (resourceVersion %s, then %s)", what, before.Name, before.ResourceVersion, after.ResourceVersion)
	}
}

// requireEnforced shows, for each of the three policies, that the API server refuses its dry-run probe
// NOW, with no polling: the activation window is closed before the first case (plan section 1). A case
// run inside the window can store an edit the policy would have refused.
func requireEnforced(t *testing.T) {
	t.Helper()
	for _, name := range launcher.AdmissionPolicyNames() {
		if refused, err := launcher.ProbeAdmission(bg(), cfg, name); !refused {
			t.Fatalf("%s does not refuse its probe right after the agents' access was opened (%v): the cases would run inside the activation window", name, err)
		}
	}
}

// inHours returns a time h hours from now, rounded to the second as the API stores it.
func inHours(h int) *metav1.Time {
	return &metav1.Time{Time: time.Now().UTC().Add(time.Duration(h) * time.Hour).Truncate(time.Second)}
}

// dynamicOf is the admin's dynamic client.
func dynamicOf(t *testing.T) dynamic.Interface {
	t.Helper()
	dyn, err := dynamic.NewForConfig(cfg)
	must(t, err, "the admin's dynamic client")
	return dyn
}
