package lifecycle

// The world of the Operation and approval commands' end-to-end test (agentcli_envtest_test.go): what the person stores in
// the control plane, and the stepper that carries Operations on it. The admission policies, the agents' role, the identities
// and the policies are opsEnv's (operations_envtest_test.go), never declared again here.
//
// The world is stored in the control plane by the person: a Provider with status, a ModelDeployment of two Instances (a
// started Allocation with a ready Instance, and a pending Allocation with its pending Instance, which has no finalizer
// because the Allocation finalizer is the Provider's controller's and none runs here) and the ApprovalPolicies. A stepper
// goroutine runs the same Operations step as opsEnv's step every 200 ms. There is no full manager: with no agent and no
// Provider controller, a full pass's other steps would act on the world (retention.go cancels a pending Allocation that
// hosts no live Instance, and an Instance with no heartbeat times out). The stepper reads the world from the API server on
// every tick, so the spec write and the deletes of an execution land on what is stored.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agentcli"
)

const (
	cliProvider = "sano"
	cliCommit   = "0123456789abcdef0123456789abcdef01234567"
	cliFullID   = "0e1dd69efe8a57c3eadd5e0293fd8c0f0cc185629e79297b6530de361b887852"
)

var (
	cliImageDigest = "sha256:" + strings.Repeat("2", 64)
	cliAgentDigest = strings.Repeat("3", 64)
)

// cliWorld is the stored world and the stepper that carries Operations on it.
type cliWorld struct {
	e *opsEnv

	mu    sync.Mutex
	errs  []error // what the Operations step returned, tick by tick
	steps int
}

// cliBatch is a batch job's submission parameters that admission accepts.
func cliBatch() *v1alpha1.BatchParameters {
	return &v1alpha1.BatchParameters{Partition: rigPartition, TimeLimit: "4h", GPUs: 4, CPUsPerTask: 16, Memory: resource.MustParse("120Gi"), AgentDigest: cliAgentDigest}
}

// cliAllocation is an Allocation of deployment md on the Provider, with its status.
func (w *cliWorld) cliAllocation(t *testing.T, name, md string, status v1alpha1.AllocationStatus) {
	t.Helper()
	a := &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: rigNS,
		Labels: map[string]string{v1alpha1.LabelProvider: cliProvider, v1alpha1.LabelModelDeployment: md}}}
	a.Spec.ProviderConfigReference = &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: cliProvider}
	a.Spec.ForProvider = v1alpha1.AllocationParameters{Batch: cliBatch()}
	a.Spec.ModelDeploymentRef = &v1alpha1.ObjectReference{Name: md}
	if err := w.e.admin.c.Create(w.e.ctx, a); err != nil {
		t.Fatalf("create Allocation %s: %v", name, err)
	}
	w.retryConflict(t, "write the status of Allocation "+name, func() error {
		var cur v1alpha1.Allocation
		if err := w.e.admin.c.Get(w.e.ctx, types.NamespacedName{Namespace: rigNS, Name: name}, &cur); err != nil {
			return err
		}
		cur.Status = status
		return w.e.admin.c.Status().Update(w.e.ctx, &cur)
	})
}

// cliInstance is an Instance on an Allocation, with its status.
func (w *cliWorld) cliInstance(t *testing.T, name, alloc string, status v1alpha1.InstanceStatus) {
	t.Helper()
	in := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: rigNS, Labels: map[string]string{v1alpha1.LabelProvider: cliProvider}}}
	in.Spec = v1alpha1.InstanceSpec{
		DeploymentID: cliFullID, IdentityRecord: "id-" + cliFullID[:12], AllocationRef: v1alpha1.ObjectReference{Name: alloc},
		Launch: v1alpha1.Launch{
			Image: "docker.io/vllm/vllm-openai", ImageDigest: cliImageDigest,
			Command:    []string{"vllm", "serve", "/hpcburst/model", "--uds", "/hpcburst/run/worker-0.sock", "--dtype=bfloat16", "--max-num-seqs=256"},
			Checkpoint: v1alpha1.LaunchCheckpoint{Repository: "example-org/example-1b", Revision: cliCommit}, GPUs: 4, MaxNumSeqs: 256,
		},
	}
	if err := w.e.admin.c.Create(w.e.ctx, in); err != nil {
		t.Fatalf("create Instance %s: %v", name, err)
	}
	w.retryConflict(t, "write the status of Instance "+name, func() error {
		var cur v1alpha1.Instance
		if err := w.e.admin.c.Get(w.e.ctx, types.NamespacedName{Namespace: rigNS, Name: name}, &cur); err != nil {
			return err
		}
		cur.Status = status
		return w.e.admin.c.Status().Update(w.e.ctx, &cur)
	})
}

// storeWorld makes the person's world: the Provider with its status, the deployment md of two Instances, and the two
// policies, a Provider policy that names agent A beside the global one that names the person.
func (w *cliWorld) storeWorld(t *testing.T, agentA v1alpha1.Approver) {
	t.Helper()
	ctx, c := w.e.ctx, w.e.admin.c
	profile := &v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: rigProfile}, Spec: v1alpha1.PlatformProfileSpec{
		ProviderKind: v1alpha1.PlatformBatch, HoldRelease: true, Partitions: []v1alpha1.PartitionProfile{{
			Name: rigPartition, AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: rigGPU, CPUArch: rigArch},
			MaxTime: dur(48 * time.Hour), GPUsPerNode: ptrTo(int32(4)), CPUsPerGPU: ptrTo(int32(16)), MemPerGPU: ptrTo(resource.MustParse("120Gi")),
		}}}}
	if err := c.Create(ctx, profile); err != nil {
		t.Fatalf("create the PlatformProfile: %v", err)
	}
	prov := &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: cliProvider},
		Spec: v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: rigProfile}, AllowedPartitions: []string{rigPartition}}}
	if err := c.Create(ctx, prov); err != nil {
		t.Fatalf("create the Provider: %v", err)
	}
	read := metav1.NewTime(time.Now().UTC().Truncate(time.Second))
	prov.Status.LastSuccessfulStatusRead = &read
	prov.Status.QueueWaits = waitsOf(60, 90)
	if err := c.Status().Update(ctx, prov, client.FieldOwner(v1alpha1.FieldManagerProvider)); err != nil {
		t.Fatalf("write the Provider's status: %v", err)
	}
	w.e.setPolicies(t, globalFor(w.e.person), providerPolicy(cliProvider, agentA))
	w.storeDeployment(t, opMD, "r", "p")
}

// storeDeployment makes a deployment of two Instances, a ready one on a started Allocation and a pending one on a pending
// Allocation, named <prefix>-<tag> after the tag. Its over-allocation is 0: the CRD defaults it to 1, which would add an
// Allocation and an Instance of its own.
func (w *cliWorld) storeDeployment(t *testing.T, name, readyTag, pendingTag string) {
	t.Helper()
	md := mdSpec(name)
	md.Generation = 0
	md.Spec.Instances = 2
	md.Spec.OverAllocation = ptrTo(int32(0))
	md.Spec.Providers = v1alpha1.ProviderSelection{Names: []string{cliProvider}}
	if err := w.e.admin.c.Create(w.e.ctx, md); err != nil {
		t.Fatalf("create ModelDeployment %s: %v", name, err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	started, hardEnd, submitted := metav1.NewTime(now.Add(-time.Hour)), metav1.NewTime(now.Add(3*time.Hour)), metav1.NewTime(now.Add(-2*time.Hour))
	w.cliAllocation(t, "alloc-"+readyTag, name, v1alpha1.AllocationStatus{AtProvider: v1alpha1.AllocationObservation{
		Phase: v1alpha1.AllocationRunning, StartTime: &started, HardEnd: &hardEnd}})
	w.cliInstance(t, "inst-"+readyTag, "alloc-"+readyTag, v1alpha1.InstanceStatus{Phase: v1alpha1.InstanceReady, ReadyTime: &started, LeaseEnd: &hardEnd})
	w.cliAllocation(t, "alloc-"+pendingTag, name, v1alpha1.AllocationStatus{AtProvider: v1alpha1.AllocationObservation{
		Phase: v1alpha1.AllocationPending, SubmitTime: &submitted}})
	w.cliInstance(t, "inst-"+pendingTag, "alloc-"+pendingTag, v1alpha1.InstanceStatus{Phase: v1alpha1.InstancePending})
}

// cliReconciler is the controller of the stepper: the admin, since the lifecycle controller's own identity is the person's
// here, with no agent and no Provider. An execution needs the clock, a recorder and the pass's memory too.
func cliReconciler(c client.Client) *Reconciler {
	return &Reconciler{Client: c, Reader: c, Clock: SystemClock{}, Backend: &Backend{}, Recorder: &MemoryRecorder{}}
}

// fixture reads one deployment's world from the API server into the pass's fields, as the lifecycle controller's
// observe step does: the deployment, the Instances that run on its Allocations, those Allocations and the Provider.
func (w *cliWorld) fixture(ctx context.Context, md *v1alpha1.ModelDeployment) (*opFixture, error) {
	c := w.e.admin.c
	var allocs v1alpha1.AllocationList
	var insts v1alpha1.InstanceList
	prov := &v1alpha1.Provider{}
	if err := c.List(ctx, &allocs, client.InNamespace(rigNS)); err != nil {
		return nil, err
	}
	if err := c.List(ctx, &insts, client.InNamespace(rigNS)); err != nil {
		return nil, err
	}
	if err := c.Get(ctx, types.NamespacedName{Name: cliProvider}, prov); err != nil {
		return nil, err
	}
	f := newOpFixture(nil, 0)
	f.md, f.allocs = md, map[string]*v1alpha1.Allocation{}
	f.pvs[cliProvider].prov = prov
	for i := range allocs.Items {
		if ref := allocs.Items[i].Spec.ModelDeploymentRef; ref != nil && ref.Name == md.Name {
			f.allocs[allocs.Items[i].Name] = &allocs.Items[i]
		}
	}
	for i := range insts.Items {
		if f.allocs[insts.Items[i].Spec.AllocationRef.Name] != nil {
			f.insts = append(f.insts, &insts.Items[i])
		}
	}
	return f, nil
}

// step runs the Operations step once for every deployment of the namespace, as the controller would in each deployment's
// pass, at a time read once. Its errors are kept: a conflict with an agent's write is expected and retried by the next tick,
// and anything else fails the test (check).
func (w *cliWorld) step() {
	ctx, now := w.e.ctx, time.Now().UTC()
	var mds v1alpha1.ModelDeploymentList
	if err := w.e.admin.c.List(ctx, &mds, client.InNamespace(rigNS)); err != nil {
		w.record(err)
		return
	}
	for i := range mds.Items {
		f, err := w.fixture(ctx, &mds.Items[i])
		if err != nil {
			w.record(err)
			continue
		}
		p := f.pass()
		p.key = types.NamespacedName{Namespace: rigNS, Name: mds.Items[i].Name}
		p.r, p.ctx, p.now = cliReconciler(w.e.admin.c), ctx, now
		p.mem = p.r.memory()
		w.record(p.operations())
	}
	w.mu.Lock()
	w.steps++
	w.mu.Unlock()
}

func (w *cliWorld) record(err error) {
	if err == nil {
		return
	}
	w.mu.Lock()
	w.errs = append(w.errs, err)
	w.mu.Unlock()
}

// start runs the stepper every 200 ms until the test ends, or its deadline.
func (w *cliWorld) start(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(w.e.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				w.step()
			}
		}
	}()
	t.Cleanup(func() { cancel(); <-done })
}

// unexpectedStepErrors are the errors of the stepper that are not a conflict between its status write and an agent's
// write to the same Operation, which the next tick reads again.
func (w *cliWorld) unexpectedStepErrors() []error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []error
	for _, err := range w.errs {
		if !onlyConflicts(err) {
			out = append(out, err)
		}
	}
	return out
}

// onlyConflicts reports whether every error joined in err is a conflict.
func onlyConflicts(err error) bool {
	if j, ok := err.(interface{ Unwrap() []error }); ok {
		for _, e := range j.Unwrap() {
			if !onlyConflicts(e) {
				return false
			}
		}
		return true
	}
	if u := errors.Unwrap(err); u != nil && !apierrors.IsConflict(err) {
		return onlyConflicts(u)
	}
	return apierrors.IsConflict(err)
}

// operationIs waits, on the Operation's observed state, until it satisfies cond.
func (w *cliWorld) operationIs(t *testing.T, name, what string, cond func(*v1alpha1.Operation) bool) *v1alpha1.Operation {
	t.Helper()
	var last v1alpha1.Operation
	eventually(t, 60*time.Second, "Operation "+name+" "+what, func() (bool, string) {
		if err := w.e.admin.c.Get(w.e.ctx, types.NamespacedName{Namespace: rigNS, Name: name}, &last); err != nil {
			return false, err.Error()
		}
		return cond(&last), "phase " + string(last.Status.Phase)
	})
	return &last
}

// confirmAs confirms the plan as the identity, from a fresh read, again after a conflict: the stepper writes the Operation's
// status between the read and the update now and then, which is what the optimistic lock is for.
func (w *cliWorld) confirmAs(t *testing.T, as *envIdentity, name, planID string) {
	t.Helper()
	eventually(t, 30*time.Second, as.user+" confirms "+name, func() (bool, string) {
		op := &v1alpha1.Operation{}
		if err := as.c.Get(w.e.ctx, types.NamespacedName{Namespace: rigNS, Name: name}, op); err != nil {
			t.Fatal(err)
		}
		op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: planID}
		switch err := as.c.Update(w.e.ctx, op); {
		case err == nil:
			return true, ""
		case apierrors.IsConflict(err):
			return false, err.Error()
		default:
			t.Fatalf("%s confirms %s: %v", as.user, name, err)
		}
		return false, ""
	})
}

func hasPhase(p v1alpha1.OperationPhase) func(*v1alpha1.Operation) bool {
	return func(op *v1alpha1.Operation) bool { return op.Status.Phase == p }
}

// storeKept makes a deployment that holds one started Allocation and no Instance on it, a candidate of a Release. Its
// Allocation carries the label tier=kept, which a selector picks it by.
func (w *cliWorld) storeKept(t *testing.T, name, alloc string) {
	t.Helper()
	md := mdSpec(name)
	md.Generation = 0
	md.Spec.OverAllocation = ptrTo(int32(0))
	md.Spec.Providers = v1alpha1.ProviderSelection{Names: []string{cliProvider}}
	if err := w.e.admin.c.Create(w.e.ctx, md); err != nil {
		t.Fatalf("create ModelDeployment %s: %v", name, err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	started, hardEnd := metav1.NewTime(now.Add(-time.Hour)), metav1.NewTime(now.Add(3*time.Hour))
	w.cliAllocation(t, alloc, name, v1alpha1.AllocationStatus{AtProvider: v1alpha1.AllocationObservation{
		Phase: v1alpha1.AllocationRunning, StartTime: &started, HardEnd: &hardEnd}})
	w.retryConflict(t, "label Allocation "+alloc, func() error {
		var a v1alpha1.Allocation
		if err := w.e.admin.c.Get(w.e.ctx, types.NamespacedName{Namespace: rigNS, Name: alloc}, &a); err != nil {
			return err
		}
		a.Labels["tier"] = "kept"
		return w.e.admin.c.Update(w.e.ctx, &a)
	})
}

// retryConflict runs a person-side write until it lands: a Conflict means the stepper (or the API server) wrote the same object
// between the write's read and its update, which is no fault of the test, so the write reads again and tries again, with a
// generous bound, and fails the test for any other error. fn reads the object afresh on every call, so what it writes is built on
// what the stepper wrote.
func (w *cliWorld) retryConflict(t *testing.T, what string, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := fn()
		switch {
		case err == nil:
			return
		case !apierrors.IsConflict(err):
			t.Errorf("%s: %v", what, err)
			return
		case time.Now().After(deadline):
			t.Errorf("%s: still a conflict after 30 s: %v", what, err)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// afterSteps waits until the stepper has finished n more ticks: on observed state, with no sleep of a fixed length. Two ticks
// mean that one tick began after the call and read what the test had written.
func (w *cliWorld) afterSteps(t *testing.T, n int, what string) {
	t.Helper()
	w.mu.Lock()
	start := w.steps
	w.mu.Unlock()
	eventually(t, 30*time.Second, what, func() (bool, string) {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.steps >= start+n, "the stepper has not ticked"
	})
}

// worldVersions are the resourceVersions of everything the world holds but the Operations and Approvals: the Provider, the
// deployments, the Allocations and the Instances. Two reads that agree show that nothing of the world was written between.
func (w *cliWorld) worldVersions(t *testing.T) map[string]string {
	t.Helper()
	ctx, c, out := w.e.ctx, w.e.admin.c, map[string]string{}
	var mds v1alpha1.ModelDeploymentList
	var allocs v1alpha1.AllocationList
	var insts v1alpha1.InstanceList
	var provs v1alpha1.ProviderList
	for _, l := range []client.ObjectList{&mds, &allocs, &insts, &provs} {
		if err := c.List(ctx, l); err != nil {
			t.Fatal(err)
		}
	}
	for _, o := range mds.Items {
		out["ModelDeployment/"+o.Name] = o.ResourceVersion
	}
	for _, o := range allocs.Items {
		out["Allocation/"+o.Name] = o.ResourceVersion
	}
	for _, o := range insts.Items {
		out["Instance/"+o.Name] = o.ResourceVersion
	}
	for _, o := range provs.Items {
		out["Provider/"+o.Name] = o.ResourceVersion
	}
	return out
}

// cliKubeconfig writes a kubeconfig of mode 600 that reaches the control plane as the identity of cfg (a bearer token), with
// the namespace as its context's.
func cliKubeconfig(t *testing.T, dir, name string, cfg *rest.Config, namespace string) string {
	t.Helper()
	kc := clientcmdapi.NewConfig()
	kc.Clusters["c"] = &clientcmdapi.Cluster{Server: cfg.Host, InsecureSkipTLSVerify: cfg.Insecure, CertificateAuthorityData: cfg.CAData}
	kc.AuthInfos["u"] = &clientcmdapi.AuthInfo{Token: cfg.BearerToken}
	kc.Contexts["x"] = &clientcmdapi.Context{Cluster: "c", AuthInfo: "u", Namespace: namespace}
	kc.CurrentContext = "x"
	path := filepath.Join(dir, name+".kubeconfig")
	if err := clientcmd.WriteToFile(*kc, path); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// cliResult is what one command line printed and exited with.
type cliResult struct {
	code      int
	out, errs string
}

// cli runs agentcli.Run with the kubeconfig, as a person at a terminal or not.
func (w *cliWorld) cli(t *testing.T, env agentcli.Env, tty bool, stdin string, args ...string) cliResult {
	t.Helper()
	var out, errs bytes.Buffer
	env.IsTerminal = func() bool { return tty }
	code := agentcli.Run(w.e.ctx, args, env, strings.NewReader(stdin), &out, &errs)
	return cliResult{code: code, out: out.String(), errs: errs.String()}
}

// putHook wraps a command's transport so that something happens between the command's read of an Operation and its update
// of it: before the FIRST PUT of the named Operation is sent, before runs, and every request of the command about that
// Operation is recorded with the status it got. The retry goes through unwrapped, which is what a conflict is for.
type putHook struct {
	suffix string // /operations/<name>
	before func()

	mu    sync.Mutex
	fired bool
	seq   []string // "GET 200", "PUT 409", ...
}

func newPutHook(operation string, before func()) *putHook {
	return &putHook{suffix: "/operations/" + operation, before: before}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func (h *putHook) wrap(next http.RoundTripper) http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		about := strings.HasSuffix(r.URL.Path, h.suffix)
		if about && r.Method == http.MethodPut {
			h.mu.Lock()
			first := !h.fired
			h.fired = true
			h.mu.Unlock()
			if first {
				h.before()
			}
		}
		resp, err := next.RoundTrip(r)
		if about && err == nil {
			h.mu.Lock()
			h.seq = append(h.seq, fmt.Sprintf("%s %d", r.Method, resp.StatusCode))
			h.mu.Unlock()
		}
		return resp, err
	})
}

func (h *putHook) sequence() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.seq...)
}

// personWritesStatus is a write by the person to the Operation's status that keeps the plan's ID: a condition of its own, which
// the lifecycle controller leaves as it is. It moves the Operation's resourceVersion, so an update that carries the old one
// meets a Conflict.
func (w *cliWorld) personWritesStatus(t *testing.T, name string) {
	t.Helper()
	w.retryConflict(t, "write the status of Operation "+name, func() error {
		op := &v1alpha1.Operation{}
		if err := w.e.admin.c.Get(w.e.ctx, types.NamespacedName{Namespace: rigNS, Name: name}, op); err != nil {
			return err
		}
		apimeta.SetStatusCondition(&op.Status.Conditions, metav1.Condition{Type: "PersonNote", Status: metav1.ConditionTrue, Reason: "Noted",
			Message: "written between a command's read and its update", LastTransitionTime: metav1.Now()})
		return w.e.admin.c.Status().Update(w.e.ctx, op)
	})
}

// personReplans edits the deployment's spec, which moves its generation and so the plan's ID, and waits until the stepper has
// written the Operation's new plan: the Operation's resourceVersion has moved and its plan is another.
func (w *cliWorld) personReplans(t *testing.T, name, oldID string) {
	t.Helper()
	w.retryConflict(t, "edit the deployment", func() error {
		var md v1alpha1.ModelDeployment
		if err := w.e.admin.c.Get(w.e.ctx, types.NamespacedName{Namespace: rigNS, Name: opMD}, &md); err != nil {
			return err
		}
		md.Spec.Workload.TargetSessions++
		return w.e.admin.c.Update(w.e.ctx, &md)
	})
	w.operationIs(t, name, "is planned again", func(o *v1alpha1.Operation) bool { return o.Status.Plan != nil && o.Status.Plan.ID != oldID })
}

// plantAdminKubeconfigs writes a kubeconfig that works as the person (the admin) at the default path under home and in the
// launcher's state directory, with the mode: readable, a fallback to either would succeed silently; at mode 000 it would print
// the permission error.
func plantAdminKubeconfigs(t *testing.T, w *cliWorld, home, state string, mode os.FileMode) {
	t.Helper()
	for _, dir := range []string{filepath.Join(home, ".kube"), state} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for path, name := range map[string]string{filepath.Join(home, ".kube", "config"): "default", filepath.Join(state, "kubeconfig"): "state"} {
		src := cliKubeconfig(t, filepath.Dir(path), "planted-"+name, w.e.admin.cfg, rigNS)
		if err := os.Rename(src, path); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
	}
}

// cliHas fails the test for each of wants that got lacks.
func cliHas(t *testing.T, what, got string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(got, want) {
			t.Errorf("%s lacks %q:\n%s", what, want, got)
		}
	}
}

// cliLacks fails the test for each of nots that got holds.
func cliLacks(t *testing.T, what, got string, nots ...string) {
	t.Helper()
	for _, not := range nots {
		if strings.Contains(got, not) {
			t.Errorf("%s holds %q:\n%s", what, not, got)
		}
	}
}
