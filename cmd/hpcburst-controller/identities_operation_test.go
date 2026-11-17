package main

// An approved Operation executed by the real lifecycle controller, under the lifecycle identity (#660, PR A, with
// #664's Operation execution). The row exists for what no other suite of this package reaches: the lifecycle's
// update and delete of a ModelDeployment (an End that deletes: the kept-warm finalizer's add and removal, the spec
// write, the delete) and the uncached read of an Operation that recovers a commit whose status write returned an error
// (opexec.go afterCommitError), each of which a missing verb would refuse. The commit's reply is lost on purpose: the
// write is stored and the client is told it failed, through a transport on the lifecycle manager's configuration
// (deps.wrapConfig). Everything is judged from the API server's audit file, which is the API server's word on who
// asked and how it answered, and every test of the package also ends by failing on any 403 for a controller identity.

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// loseTheCommitReply puts a transport on cfg that stores the first write of an Operation's status that sets phase
// Executing, and then answers the client with an error: the commit landed, and the controller is not told so. lost is set
// once it has happened.
func loseTheCommitReply(lost *atomic.Bool) func(*rest.Config) {
	return func(cfg *rest.Config) {
		cfg.Wrap(func(rt http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodPut && strings.Contains(req.URL.Path, "/operations/") && strings.HasSuffix(req.URL.Path, "/status") && req.GetBody != nil && !lost.Load() {
					rc, err := req.GetBody()
					if err != nil {
						return nil, err
					}
					body, err := io.ReadAll(rc)
					rc.Close()
					if err != nil {
						return nil, err
					}
					if bytes.Contains(body, []byte(`"phase":"Executing"`)) && lost.CompareAndSwap(false, true) {
						resp, err := rt.RoundTrip(req)
						if err == nil {
							resp.Body.Close()
						}
						return nil, errors.New("injected: the reply to the commit never came")
					}
				}
				return rt.RoundTrip(req)
			})
		})
	}
}

func TestAnOperationsCommitIsRecoveredAndExecutedUnderTheLifecycleIdentity(t *testing.T) {
	api := startAPIServer(t)
	ctx := t.Context()
	c := api.client
	global := true
	person := "person"
	md := &v1alpha1.ModelDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "op-row", Namespace: "default"},
		Spec: v1alpha1.ModelDeploymentSpec{
			Model: "example-model",
			Source: v1alpha1.ModelSource{Served: &v1alpha1.ServingSpec{
				Checkpoint: v1alpha1.CheckpointSpec{Repository: "example-org/example-model", Revision: "0123456789abcdef0123456789abcdef01234567"},
				Engine: v1alpha1.EngineSpec{Name: "vllm", Version: "0.32.0", Image: "docker.io/vllm/vllm-openai",
					ImageDigest: "sha256:1111111111111111111111111111111111111111111111111111111111111111"},
				Flags: []v1alpha1.EngineFlag{{Name: "max-model-len", Value: "32768"}},
			}},
			Instances: 1,
			// No Provider by that name: the deployment provisions nothing and asks the Hub nothing, and the Operations step
			// runs all the same.
			Providers: v1alpha1.ProviderSelection{Names: []string{"absent-provider"}},
			Workload: &v1alpha1.Workload{
				ContextTokens: v1alpha1.ContextTokens{Typical: 4096}, OutputTokens: v1alpha1.OutputTokens{Typical: 512}, TargetSessions: 4,
			},
		},
	}
	policy := &v1alpha1.ApprovalPolicy{ObjectMeta: metav1.ObjectMeta{Name: "global"},
		Spec: v1alpha1.ApprovalPolicySpec{Scope: v1alpha1.ApprovalScope{Global: &global}, Approvers: []v1alpha1.Approver{{User: person}}}}
	for _, o := range []client.Object{policy, md} {
		if err := c.Create(ctx, o); err != nil {
			t.Fatal(err)
		}
	}
	f, _, _ := prepareState(t, api, "direct", exampleFront)
	var lost atomic.Bool
	b := startBinary(t, &f, deps{providerScheme: providerScheme, registry: prometheus.NewRegistry(), wrapConfig: loseTheCommitReply(&lost)})

	op := &v1alpha1.Operation{ObjectMeta: metav1.ObjectMeta{Name: "end-op", Namespace: "default"}, Spec: v1alpha1.OperationSpec{
		Action: v1alpha1.OperationActionEnd, Owner: "agent-a", ModelDeploymentRef: v1alpha1.ObjectReference{Name: md.Name},
		End: &v1alpha1.EndParameters{Delete: true},
	}}
	if err := c.Create(ctx, op); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(op)
	b.eventually(t, "the controller to plan the Operation", 60*time.Second, func() bool {
		return c.Get(ctx, key, op) == nil && op.Status.Phase == v1alpha1.OperationPhasePlanned && op.Status.Plan != nil
	})
	op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: op.Status.Plan.ID}
	if err := c.Update(ctx, op); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	approval := &v1alpha1.Approval{ObjectMeta: metav1.ObjectMeta{Name: "end-op-approval", Namespace: "default"}, Spec: v1alpha1.ApprovalSpec{
		Operation: v1alpha1.OperationReference{Name: op.Name, UID: op.UID}, PlanID: op.Status.Plan.ID, Approver: v1alpha1.ApproverIdentity{Username: person},
	}}
	if err := c.Create(ctx, approval); err != nil {
		t.Fatalf("approve: %v", err)
	}
	b.eventually(t, "the Operation to be committed, executed and done", 90*time.Second, func() bool {
		return c.Get(ctx, key, op) == nil && op.Status.Phase == v1alpha1.OperationPhaseDone
	})
	if !lost.Load() {
		t.Fatal("the commit's reply was never lost: the row did not arise")
	}
	// The API server's own account of who asked what, and how it answered.
	lifecycle := controllerSA + "hpcburst-lifecycle"
	has := func(verb, resource, name string) bool {
		for _, e := range readAuditEvents(t, api.auditLog) {
			if e.User.Username == lifecycle && e.Verb == verb && e.ObjectRef.Resource == resource && e.ObjectRef.Name == name && e.ResponseStatus.Code/100 == 2 {
				return true
			}
		}
		return false
	}
	waitUntil(t, 30*time.Second, "the audit log to hold the Operation's reads and writes", func() bool {
		return has("get", "operations", op.Name) && has("update", "modeldeployments", md.Name) && has("delete", "modeldeployments", md.Name)
	})
}
