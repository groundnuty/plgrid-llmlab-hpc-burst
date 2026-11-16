package main

// The three admission policies and the window-closing order of up on a real k3s (HBEP-10; issue #589, PR 3).
// The full matrix of refusals is judged on envtest (api/v1alpha1/e2e); this is the subset a real k3s must
// agree with, and the part that only the launcher's own up can show: up returns only when the policies are
// ENFORCED (a probe right after it is refused, with no polling of the test's own), the agent role is
// the wide one only then, a host with no CRDs leaves it narrow and says so, and a binding deleted by hand is
// back after the next up.
//
// Every judgement is the API server's answer to a real request made with client-go and an agent's own
// kubeconfig from hpcburst identity add, not the launcher's code and not a look at an object. The probes are
// the test's own: they are written here and not read from the launcher.
//
// The launcher block is 28460 (#589's, 28460-28479), as identity_k3s_test.go's.

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/scale"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const (
	capacityGuardName = "hpcburst-capacity-guard"
	ownerPinName      = "hpcburst-owner-pin"
	approverPinName   = "hpcburst-approver-pin"
)

var (
	mdGVR = v1alpha1.GroupVersion.WithResource("modeldeployments")
	opGVR = v1alpha1.GroupVersion.WithResource("operations")
	apGVR = v1alpha1.GroupVersion.WithResource("approvals")
)

// admissionStatus is the part of status --json this test reads.
type admissionStatus struct {
	Health            string `json:"health"`
	AdmissionPolicies []struct {
		Name    string `json:"name"`
		Present bool   `json:"present"`
		Detail  string `json:"detail"`
	} `json:"admission_policies"`
	AgentRBAC *struct {
		Present bool   `json:"present"`
		Writes  bool   `json:"writes"`
		Detail  string `json:"detail"`
	} `json:"agent_rbac"`
	Identities []struct {
		Name   string `json:"name"`
		Valid  bool   `json:"valid"`
		Detail string `json:"detail"`
	} `json:"identities"`
}

// upReport is the part of up --json this test reads.
type admissionUp struct {
	Admission   bool     `json:"admission_policies"`
	AgentRBAC   bool     `json:"agent_rbac"`
	AgentWrites bool     `json:"agent_writes"`
	Warnings    []string `json:"warnings"`
}

func toUnstructured(t *testing.T, obj runtime.Object, kind string) *unstructured.Unstructured {
	t.Helper()
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatal(err)
	}
	u := &unstructured.Unstructured{Object: m}
	u.SetAPIVersion(v1alpha1.GroupVersion.String())
	u.SetKind(kind)
	return u
}

// deploymentFor returns a ModelDeployment that the CRD accepts and the owner pin accepts from owner.
func deploymentFor(owner, name string, instances int32) *v1alpha1.ModelDeployment {
	return &v1alpha1.ModelDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Annotations: map[string]string{v1alpha1.AnnotationOwner: owner}},
		Spec: v1alpha1.ModelDeploymentSpec{
			Model: "example-27b",
			Source: v1alpha1.ModelSource{Served: &v1alpha1.ServingSpec{
				Checkpoint: v1alpha1.CheckpointSpec{Repository: "example-org/example-1b", Revision: "0123456789abcdef0123456789abcdef01234567"},
				Engine: v1alpha1.EngineSpec{Name: "vllm", Version: "0.32.0", Image: "docker.io/vllm/vllm-openai",
					ImageDigest: "sha256:2222222222222222222222222222222222222222222222222222222222222222"},
				Flags: []v1alpha1.EngineFlag{{Name: "dtype", Value: "bfloat16"}, {Name: "max-model-len", Value: "32768"}},
			}},
			Instances: instances,
			Providers: v1alpha1.ProviderSelection{Names: []string{"helios-a"}},
			Workload: &v1alpha1.Workload{
				ContextTokens: v1alpha1.ContextTokens{Typical: 8192}, OutputTokens: v1alpha1.OutputTokens{Typical: 1024}, TargetSessions: 8,
			},
		},
	}
}

func operationFor(owner, name string) *v1alpha1.Operation {
	return &v1alpha1.Operation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: v1alpha1.OperationSpec{
			Action: v1alpha1.OperationActionScale, ModelDeploymentRef: v1alpha1.ObjectReference{Name: "example-md"},
			Scale: &v1alpha1.ScaleParameters{Instances: 0}, Owner: owner,
		},
	}
}

func approvalFor(approver, name string) *v1alpha1.Approval {
	return &v1alpha1.Approval{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: v1alpha1.ApprovalSpec{
			Operation: v1alpha1.OperationReference{Name: "example-op", UID: "00000000-0000-0000-0000-000000000007"},
			PlanID:    "0123456789abcdef", Approver: v1alpha1.ApproverIdentity{Username: approver},
		},
	}
}

// denial is how the API server words a refusal by a ValidatingAdmissionPolicy.
func denial(policy string) string {
	return "ValidatingAdmissionPolicy '" + policy + "' with binding '" + policy + "' denied request"
}

func refusedBy(err error, policy string) bool {
	return err != nil && apierrors.IsForbidden(err) && strings.Contains(err.Error(), denial(policy))
}

// probesRefusedNow asks each of the three policies, ONCE and with no polling, with a dry-run create the
// API server must refuse by that policy: the admin makes the Operation's and the Approval's, and the
// capacity guard's is the admin impersonating an agent (with system:masters, to get past RBAC, and
// the agents' group, which puts it under the guard). This is the check that up returned only after
// enforcement: a wait of the test's own would hide a window.
func probesRefusedNow(t *testing.T, h *host) {
	t.Helper()
	ctx, cancel := testCtx()
	defer cancel()
	cl := h.clients()
	dry := metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}}
	_, err := cl.dyn.Resource(opGVR).Namespace("default").Create(ctx, toUnstructured(t, operationFor("someone-else", "probe-op"), "Operation"), dry)
	if !refusedBy(err, ownerPinName) {
		t.Errorf("the owner pin's probe right after up: %v, want its refusal", err)
	}
	_, err = cl.dyn.Resource(apGVR).Namespace("default").Create(ctx, toUnstructured(t, approvalFor("someone-else", "probe-ap"), "Approval"), dry)
	if !refusedBy(err, approverPinName) {
		t.Errorf("the approver pin's probe right after up: %v, want its refusal", err)
	}
	cfg, kerr := clientcmd.BuildConfigFromFlags("", filepath.Join(h.dir, "kubeconfig"))
	if kerr != nil {
		t.Fatal(kerr)
	}
	user := "system:serviceaccount:hpcburst-agents:example-probe"
	cfg.Impersonate = rest.ImpersonationConfig{UserName: user, Groups: []string{"system:serviceaccounts", agentsGroupName, "system:authenticated", "system:masters"}}
	icl, kerr := dynamic.NewForConfig(cfg)
	if kerr != nil {
		t.Fatal(kerr)
	}
	md := deploymentFor(user, "probe-md", 1)
	md.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: "short-lived", UID: "00000000-0000-0000-0000-000000000001"}}
	_, err = icl.Resource(mdGVR).Namespace("default").Create(ctx, toUnstructured(t, md, "ModelDeployment"), dry)
	if !refusedBy(err, capacityGuardName) {
		t.Errorf("the capacity guard's probe right after up: %v, want its refusal", err)
	}
}

func scaleClientFor(t *testing.T, kubeconfig string) scale.ScalesGetter {
	t.Helper()
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sc, err := scale.NewForConfig(cfg, restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc)), dynamic.LegacyAPIPathResolverFunc, scale.NewDiscoveryScaleKindResolver(dc))
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

func admissionStatusOf(t *testing.T, h *host) (admissionStatus, int) {
	t.Helper()
	var js admissionStatus
	r := cli(t, h.args("status", "--json", "--no-gc-probe")...)
	parseJSON(t, r.out, &js)
	return js, r.code
}

func TestUpClosesTheWindowAndThePoliciesBoundTheAgentsOnARealK3s(t *testing.T) {
	const base = 28460
	h := newHost(t)
	crds := filepath.Join("..", "..", "config", "crd")

	// ---- a host with no CRDs: the policies cannot be probed, so the agents' writes stay off, and up says so ----
	var up admissionUp
	r := cli(t, h.upArgs(base, "--json")...)
	if r.code != exitOK {
		t.Fatalf("up with no CRDs exited %d: %s", r.code, r.err)
	}
	parseJSON(t, r.out, &up)
	if !up.Admission || !up.AgentRBAC || up.AgentWrites || !strings.Contains(strings.Join(up.Warnings, "\n"), "writes stay off") {
		t.Errorf("up with no CRDs reported %+v, want the policies and roles installed, the writes off, with a warning", up)
	}
	js, code := admissionStatusOf(t, h)
	if code != exitOK || js.Health != "healthy" || js.AgentRBAC == nil || !js.AgentRBAC.Present || js.AgentRBAC.Writes || !strings.Contains(js.AgentRBAC.Detail, "narrow") {
		t.Errorf("status of a host with no CRDs: exit %d, %+v, want healthy with the agent role present, writes off, and why", code, js.AgentRBAC)
	}
	if d := cli(t, h.args("down")...); d.code != exitOK {
		t.Fatalf("down exited %d: %s", d.code, d.err)
	}

	// ---- up with the CRDs returns with every policy enforced and the wide role applied ----
	r = cli(t, h.upArgs(base, "--manifests", crds, "--json")...)
	if r.code != exitOK {
		t.Fatalf("up exited %d: %s", r.code, r.err)
	}
	up = admissionUp{}
	parseJSON(t, r.out, &up)
	if !up.Admission || !up.AgentRBAC || !up.AgentWrites {
		t.Errorf("up reported %+v, want the policies installed and the agents' writes on", up)
	}
	// A probe right after up is refused, with no polling: up returned only after enforcement.
	probesRefusedNow(t, h)

	// ---- status shows the three present, and the role with its writes ----
	js, code = admissionStatusOf(t, h)
	if code != exitOK || js.Health != "healthy" || len(js.AdmissionPolicies) != 3 {
		t.Fatalf("status: exit %d, health %q, %d policies, want 0, healthy and 3", code, js.Health, len(js.AdmissionPolicies))
	}
	for i, want := range []string{capacityGuardName, ownerPinName, approverPinName} {
		if p := js.AdmissionPolicies[i]; p.Name != want || !p.Present || p.Detail != "" {
			t.Errorf("status policy %d = %+v, want %s present", i, p, want)
		}
	}
	if js.AgentRBAC == nil || !js.AgentRBAC.Present || !js.AgentRBAC.Writes {
		t.Errorf("status agent_rbac = %+v, want present with the writes on", js.AgentRBAC)
	}
	text := cli(t, h.args("status", "--no-gc-probe")...).out
	for _, w := range []string{"admission policy " + capacityGuardName + ": present", "admission policy " + ownerPinName + ": present", "admission policy " + approverPinName + ": present"} {
		if !strings.Contains(text, w) {
			t.Errorf("status text lacks %q:\n%s", w, text)
		}
	}

	// ---- an agent of identity add: a lower count via scale is refused by the guard, a raise is accepted ----
	add := func(name string) identityJSON {
		t.Helper()
		res := cli(t, "identity", "add", name, "--json", "--state-dir", h.dir)
		if res.code != exitOK {
			t.Fatalf("identity add %s exited %d: %s", name, res.code, res.err)
		}
		var out identityJSON
		parseJSON(t, res.out, &out)
		return out
	}
	a := add("agent-a")
	_, aDyn := kubeClients(t, a.Kubeconfig)
	scales := scaleClientFor(t, a.Kubeconfig)
	ctx, cancel := testCtx()
	defer cancel()
	// The agent's role is the wide one (up applied it after the wait), so a create is allowed by RBAC and
	// the policies bound it: its own owner annotation, and no owner reference.
	if u := whoIs(t, a.Kubeconfig); u.Username != a.Username || !strings.Contains(strings.Join(u.Groups, ","), agentsGroupName) {
		t.Fatalf("agent-a's kubeconfig authenticates as %q in %v", u.Username, u.Groups)
	}
	waitFor(t, 60*time.Second, "agent-a's create of a ModelDeployment to be allowed by RBAC", func() bool {
		_, err := aDyn.Resource(mdGVR).Namespace("default").Create(ctx, toUnstructured(t, deploymentFor(a.Username, "md-a", 3), "ModelDeployment"), metav1.CreateOptions{})
		return err == nil || !apierrors.IsForbidden(err) || refusedBy(err, ownerPinName)
	})
	md := deploymentFor(a.Username, "md-a", 3)
	gr := schema.GroupResource{Group: "hpcburst.groundnuty.github.io", Resource: "modeldeployments"}
	_, err := scales.Scales("default").Update(ctx, gr, &autoscalingv1.Scale{ObjectMeta: metav1.ObjectMeta{Name: md.Name, Namespace: "default"}, Spec: autoscalingv1.ScaleSpec{Replicas: 1}}, metav1.UpdateOptions{})
	if !refusedBy(err, capacityGuardName) || !strings.Contains(err.Error(), "a lower count is an Operation of action Scale") {
		t.Errorf("agent-a's lower count through scale: %v, want the capacity guard's refusal", err)
	}
	if _, err := scales.Scales("default").Update(ctx, gr, &autoscalingv1.Scale{ObjectMeta: metav1.ObjectMeta{Name: md.Name, Namespace: "default"}, Spec: autoscalingv1.ScaleSpec{Replicas: 5}}, metav1.UpdateOptions{}); err != nil {
		t.Errorf("agent-a's raise through scale: %v, want it accepted", err)
	}
	cl := h.clients()
	if got, err := cl.dyn.Resource(mdGVR).Namespace("default").Get(ctx, md.Name, metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	} else if n, _, _ := unstructured.NestedInt64(got.Object, "spec", "instances"); n != 5 {
		t.Errorf("spec.instances = %d after the refused lower count and the accepted raise, want 5", n)
	}
	// A delete is refused. RBAC refuses it first (the agent role has no delete), so the message is RBAC's;
	// the guard's delete rule is judged behind a wider role on envtest.
	if err := aDyn.Resource(mdGVR).Namespace("default").Delete(ctx, md.Name, metav1.DeleteOptions{}); !apierrors.IsForbidden(err) {
		t.Errorf("agent-a's delete of a ModelDeployment: %v, want it refused", err)
	}
	if _, err := cl.dyn.Resource(mdGVR).Namespace("default").Get(ctx, md.Name, metav1.GetOptions{}); err != nil {
		t.Errorf("the ModelDeployment is gone after the refused delete: %v", err)
	}
	// An Operation with a foreign owner is refused by the owner pin; with its own owner, accepted.
	_, err = aDyn.Resource(opGVR).Namespace("default").Create(ctx, toUnstructured(t, operationFor("system:serviceaccount:hpcburst-agents:agent-b", "op-foreign"), "Operation"), metav1.CreateOptions{})
	if !refusedBy(err, ownerPinName) {
		t.Errorf("agent-a's Operation owned by another identity: %v, want the owner pin's refusal", err)
	}
	if _, err := aDyn.Resource(opGVR).Namespace("default").Create(ctx, toUnstructured(t, operationFor(a.Username, "op-own"), "Operation"), metav1.CreateOptions{}); err != nil {
		t.Errorf("agent-a's Operation with its own owner: %v, want it accepted", err)
	}

	// ---- an identity whose kubeconfig cannot be used is that identity's state, not the host's failure ----
	b := add("agent-b")
	kc, err := clientcmd.LoadFromFile(b.Kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range kc.Clusters {
		c.CertificateAuthority = ""
		c.CertificateAuthorityData = []byte("-----BEGIN CERTIFICATE-----\nnot a certificate\n-----END CERTIFICATE-----\n")
	}
	if err := clientcmd.WriteToFile(*kc, b.Kubeconfig); err != nil {
		t.Fatal(err)
	}
	js, code = admissionStatusOf(t, h)
	if code != exitOK || js.Health != "healthy" {
		t.Errorf("status with an identity whose CA is corrupt: exit %d, health %q, want 0 and healthy", code, js.Health)
	}
	states := map[string]struct {
		valid  bool
		detail string
	}{}
	for _, id := range js.Identities {
		states[id.Name] = struct {
			valid  bool
			detail string
		}{id.Valid, id.Detail}
	}
	if s := states["agent-a"]; !s.valid {
		t.Errorf("agent-a is %+v in status, want valid", s)
	}
	if s := states["agent-b"]; s.valid || !strings.HasPrefix(s.detail, "unreadable: ") {
		t.Errorf("agent-b (corrupt CA) is %+v in status, want not valid, 'unreadable: <why>'", s)
	}

	// ---- a binding deleted by hand is back after the next up, and up returns with it enforced ----
	bindingRes := bindingGVR
	if err := cl.dyn.Resource(bindingRes).Delete(ctx, ownerPinName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete the owner pin's binding by hand: %v", err)
	}
	js, _ = admissionStatusOf(t, h)
	for _, p := range js.AdmissionPolicies {
		if want := p.Name != ownerPinName; p.Present != want {
			t.Errorf("status with the owner pin's binding gone: %+v, want present=%v", p, want)
		}
	}
	if d := cli(t, h.args("down")...); d.code != exitOK {
		t.Fatalf("down exited %d: %s", d.code, d.err)
	}
	if u := cli(t, h.upArgs(base, "--json")...); u.code != exitOK {
		t.Fatalf("up over the missing binding exited %d: %s", u.code, u.err)
	}
	probesRefusedNow(t, h)
	js, code = admissionStatusOf(t, h)
	for _, p := range js.AdmissionPolicies {
		if !p.Present {
			t.Errorf("status after the restart: %+v, want present", p)
		}
	}
	if code != exitOK {
		t.Errorf("status after the restart exited %d", code)
	}
	if d := cli(t, h.args("down")...); d.code != exitOK {
		t.Fatalf("down exited %d: %s", d.code, d.err)
	}
	if l := listeningInRange(t, base, base+19); len(l) != 0 {
		t.Errorf("after down something still listens in 28460-28479: %v", l)
	}
}
