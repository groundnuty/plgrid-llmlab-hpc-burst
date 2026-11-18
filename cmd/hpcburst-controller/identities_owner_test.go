package main

// The ModelDeployment's owner-hash label, under the lifecycle identity (#660, PR A, with #662's owner label). The
// lifecycle controller keeps the label hpcburst.groundnuty.github.io/owner-hash true to the owner annotation by a JSON merge
// PATCH of that one key (internal/lifecycle/owner.go, syncOwnerLabel). A refused patch only joins the pass's error and
// provisioning goes on, so without a row that looks for the label itself, a role that lacks the verb passes every
// other test. The expected value is written out by hand: the first 12 hex characters of the SHA-256 of the owner's
// user name, as `printf '%s' <owner> | sha256sum | cut -c1-12` prints them, never computed by the code under test.

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func TestTheLifecycleIdentityStampsTheOwnerHashLabel(t *testing.T) {
	const (
		owner      = "system:serviceaccount:hpcburst-agents:agent-a"
		ownerKey   = "hpcburst.groundnuty.github.io/owner"
		labelKey   = "hpcburst.groundnuty.github.io/owner-hash"
		wantLabel  = "u-77d125ed9ec1" // printf '%s' 'system:serviceaccount:hpcburst-agents:agent-a' | sha256sum | cut -c1-12
		deployment = "owner-row"
	)
	api := startAPIServer(t)
	ctx := t.Context()
	c := api.client
	md := &v1alpha1.ModelDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: deployment, Namespace: "default", Annotations: map[string]string{ownerKey: owner}},
		Spec: v1alpha1.ModelDeploymentSpec{
			Model: "example-model",
			Source: v1alpha1.ModelSource{Served: &v1alpha1.ServingSpec{
				Checkpoint: v1alpha1.CheckpointSpec{Repository: "example-org/example-model", Revision: "0123456789abcdef0123456789abcdef01234567"},
				Engine: v1alpha1.EngineSpec{Name: "vllm", Version: "0.32.0", Image: "docker.io/vllm/vllm-openai",
					ImageDigest: "sha256:1111111111111111111111111111111111111111111111111111111111111111"},
				Flags: []v1alpha1.EngineFlag{{Name: "max-model-len", Value: "32768"}},
			}},
			Instances: 1,
			// No Provider by that name: nothing is provisioned, and the label sync runs all the same, first in every pass.
			Providers: v1alpha1.ProviderSelection{Names: []string{"absent-provider"}},
			Workload: &v1alpha1.DeploymentWorkload{
				ContextTokens: v1alpha1.ContextTokens{Typical: 4096}, OutputTokens: v1alpha1.OutputTokens{Typical: 512}, TargetSessions: 4,
			},
		},
	}
	if err := c.Create(ctx, md); err != nil {
		t.Fatal(err)
	}
	if _, has := md.Labels[labelKey]; has {
		t.Fatalf("the deployment was created with the label %s: the row would prove nothing", labelKey)
	}
	f, _, _ := prepareState(t, api, "direct", exampleFront)
	b := startBinary(t, &f, deps{providerScheme: providerScheme, registry: prometheus.NewRegistry()})
	var got v1alpha1.ModelDeployment
	b.eventually(t, "the lifecycle controller to stamp the owner-hash label", 60*time.Second, func() bool {
		return c.Get(ctx, client.ObjectKeyFromObject(md), &got) == nil && got.Labels[labelKey] != ""
	})
	if got.Labels[labelKey] != wantLabel {
		t.Errorf("the label %s is %q, want %q", labelKey, got.Labels[labelKey], wantLabel)
	}
	if got.Annotations[ownerKey] != owner {
		t.Errorf("the owner annotation is %q after the patch, want it untouched (%q)", got.Annotations[ownerKey], owner)
	}
	// The API server's own account: the patch is the lifecycle identity's, and it was answered 2xx. (Every test of the
	// package also ends by failing on a 403 for a controller identity.)
	lifecycle := controllerSA + "hpcburst-lifecycle"
	waitUntil(t, 30*time.Second, "the audit log to hold the lifecycle identity's patch of the deployment", func() bool {
		for _, e := range readAuditEvents(t, api.auditLog) {
			if e.User.Username == lifecycle && e.Verb == "patch" && e.ObjectRef.Resource == "modeldeployments" && e.ObjectRef.Name == deployment && e.ResponseStatus.Code/100 == 2 {
				return true
			}
		}
		return false
	})
	if err := b.stop(t); err != nil {
		t.Errorf("run returned %v", err)
	}
}
