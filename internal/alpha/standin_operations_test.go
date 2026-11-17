package alpha

// The stand-in of HBEP-10's beta step 1 (#589, PR 5): the Operations, the identities and the three admission policies over
// the REAL stack on this host (hpcburst up: k3s, the controllers, frps and the front, the real hb-agent in each job with the
// engine double as its engine), against the stand-in cluster: the fake Slurm in run mode behind the rehearsal's sshd, reached
// by the system ssh. It runs the owner's example end to end:
//
//   - hpcburst up, and two agents made by hpcburst identity add, A and B, each acting through its own kubeconfig;
//   - a Provider policy that names agent A for Provider P, beside the global policy that names the person (up writes it);
//   - A's Scale of a deployment on P: planned, confirmed by A, and run on A's own approval;
//   - A's Scale of a deployment on Q: A's approval and B's count under no policy that applies to Q, so it waits, and the
//     person's approval runs it;
//   - the person's End of the deployment on P, approved by the person: its Instances drain and its Allocations are released,
//     and the fake Slurm's own record shows every job of the deployment ended.
//
// It proves the wiring on one host, not a cluster: a real cluster's queue, nodes and network are a live run's to show. It runs
// only with HPCBURST_ALPHA_STANDIN=1, as the other stand-ins do (standin_test.go), and needs what they need: the four
// binaries in HPCBURST_REHEARSAL_BIN, k3s in HPCBURST_BIN_DIR, the pinned frp (HPCBURST_FRP_DIR) and OpenSSH. Run it only
// on a hosted throwaway, never on the development VM, under launcher.lock and a memory cap otherwise (docs/development.md).
// Ports: #589's block, 28460-28479: the launcher's 28460-28472 (hpcburst up's block), the front's client listener 28473 and
// its dial-home listener 28474; nothing else here binds.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

// opsStandInPorts are #589's block: hpcburst up's 28460-28472, the front's client listener and its dial-home listener.
var opsStandInPorts = stack.Ports{Base: 28460, ClientAddr: "127.0.0.1:28473", DialHomeListen: "127.0.0.1:28474"}

// opsStandInDirect is the stand-in's dial-home: direct mode, the front's dial-home listener on loopback.
var opsStandInDirect = stack.DialHome{Mode: "direct", Addr: opsStandInPorts.DialHomeListen}

const (
	// opsProviderQ is the second Provider, over the same login node as the first (stack.ProviderName), so that a policy can
	// name one and not the other.
	opsProviderQ   = "rehearsal-q"
	opsMDP, opsMDQ = "md-p", "md-q"
	opsAgentsNS    = "hpcburst-agents"
)

// opsWho is one identity of the example: the client that acts as it, its user name and its groups as the API server reports
// them, which an Approval records.
type opsWho struct {
	name   string
	c      client.Client
	user   string
	groups []string
}

// whoIs asks the API server who the client is (SelfSubjectReview).
func opsWhoIs(t *testing.T, name string, cl client.Client, cs *kubernetes.Clientset) opsWho {
	t.Helper()
	ctx, cancel := stack.CallCtx(t)
	defer cancel()
	r, err := cs.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("SelfSubjectReview of %s: %v", name, err)
	}
	return opsWho{name: name, c: cl, user: r.Status.UserInfo.Username, groups: r.Status.UserInfo.Groups}
}

// opsStand is the running stack and the identities of the example.
type opsStand struct {
	t      *testing.T
	h      *stack.Host
	cl     *stack.Cluster
	person opsWho
	a, b   opsWho
}

// addIdentity runs hpcburst identity add NAME against the running host and returns the identity's client, made from the
// kubeconfig the command wrote (it holds the agent's own token and never the admin's).
func (s *opsStand) addIdentity(name string) opsWho {
	t := s.t
	t.Helper()
	out, err := stack.Run(t, 2*time.Minute, s.h.Env, s.h.B.HPCBurst, "identity", "add", name, "--state-dir", s.h.State, "--json")
	if err != nil {
		t.Fatalf("hpcburst identity add %s: %v\n%s", name, err, out)
	}
	// The command prints one JSON object; anything the run's output carries around it is not part of it.
	from, to := strings.Index(out, "{"), strings.LastIndex(out, "}")
	if from < 0 || to < from {
		t.Fatalf("hpcburst identity add %s printed no JSON object: %q", name, out)
	}
	var res struct{ Name, Username, Kubeconfig string }
	if err := json.Unmarshal([]byte(out[from:to+1]), &res); err != nil {
		t.Fatalf("hpcburst identity add %s: %v\n%s", name, err, out)
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", res.Kubeconfig)
	if err != nil {
		t.Fatalf("the kubeconfig of identity %s: %v", name, err)
	}
	cfg.QPS, cfg.Burst = 50, 100
	cfg.Timeout = stack.APICallTimeout
	c, err := client.New(cfg, client.Options{Scheme: stack.APIScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	who := opsWhoIs(t, name, c, cs)
	if want := "system:serviceaccount:" + opsAgentsNS + ":" + name; who.user != want {
		t.Fatalf("identity %s acts as %q, want %q", name, who.user, want)
	}
	return who
}

// get reads an object with the identity's client.
func (w opsWho) get(t *testing.T, key client.ObjectKey, obj client.Object) error {
	ctx, cancel := stack.CallCtx(t)
	defer cancel()
	return w.c.Get(ctx, key, obj)
}

// request creates an Operation as the identity, with the identity as its owner (the owner pin requires it), and waits until
// the controller has written its plan.
func (s *opsStand) request(as opsWho, name, md string, spec v1alpha1.OperationSpec) *v1alpha1.Operation {
	t := s.t
	t.Helper()
	spec.Owner, spec.ModelDeploymentRef = as.user, v1alpha1.ObjectReference{Name: md}
	op := &v1alpha1.Operation{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: stack.Namespace}, Spec: spec}
	ctx, cancel := stack.CallCtx(t)
	err := as.c.Create(ctx, op)
	cancel()
	if err != nil {
		t.Fatalf("%s creates Operation %s: %v", as.name, name, err)
	}
	s.waitOp(name, "its plan", func(o *v1alpha1.Operation) bool {
		return o.Status.Phase == v1alpha1.OperationPhasePlanned && o.Status.Plan != nil
	})
	return s.op(name)
}

func (s *opsStand) op(name string) *v1alpha1.Operation {
	var op v1alpha1.Operation
	if err := s.person.get(s.t, client.ObjectKey{Namespace: stack.Namespace, Name: name}, &op); err != nil {
		s.t.Fatalf("get Operation %s: %v", name, err)
	}
	return &op
}

// waitOp waits until cond holds of the Operation, failing with what it saw last.
func (s *opsStand) waitOp(name, what string, cond func(*v1alpha1.Operation) bool) {
	s.t.Helper()
	stack.WaitState(s.t, 10*time.Minute, fmt.Sprintf("Operation %s: %s", name, what), func() (bool, string) {
		var op v1alpha1.Operation
		if err := s.person.get(s.t, client.ObjectKey{Namespace: stack.Namespace, Name: name}, &op); err != nil {
			return false, err.Error()
		}
		return cond(&op), opsDescribe(&op)
	})
}

// opsDescribe is a one-line view of an Operation, for a wait's failure message.
func opsDescribe(op *v1alpha1.Operation) string {
	s := fmt.Sprintf("phase %q", op.Status.Phase)
	for _, c := range op.Status.Conditions {
		s += fmt.Sprintf(" %s=%s(%s: %.200s)", c.Type, c.Status, c.Reason, c.Message)
	}
	for _, o := range op.Status.Targets {
		s += fmt.Sprintf(" target %s %s=%s", o.Kind, o.Name, o.State)
	}
	return s
}

// confirm sets the owner's confirm of the plan in status, as an update that carries the resourceVersion it read.
func (s *opsStand) confirm(as opsWho, name string) {
	t := s.t
	t.Helper()
	var op v1alpha1.Operation
	if err := as.get(t, client.ObjectKey{Namespace: stack.Namespace, Name: name}, &op); err != nil {
		t.Fatalf("get Operation %s: %v", name, err)
	}
	op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: op.Status.Plan.ID}
	ctx, cancel := stack.CallCtx(t)
	defer cancel()
	if err := as.c.Update(ctx, &op); err != nil {
		t.Fatalf("%s confirms Operation %s: %v", as.name, name, err)
	}
}

// approve creates an Approval of the Operation's plan: the approver is the identity itself, as the approver pin requires.
func (s *opsStand) approve(as opsWho, name string) {
	t := s.t
	t.Helper()
	op := s.op(name)
	ap := &v1alpha1.Approval{
		ObjectMeta: metav1.ObjectMeta{Name: "ap-" + name + "-" + as.name, Namespace: stack.Namespace},
		Spec: v1alpha1.ApprovalSpec{
			Operation: v1alpha1.OperationReference{Name: name, UID: op.UID}, PlanID: op.Status.Plan.ID,
			Approver: v1alpha1.ApproverIdentity{Username: as.user, Groups: as.groups},
		},
	}
	ctx, cancel := stack.CallCtx(t)
	defer cancel()
	if err := as.c.Create(ctx, ap); err != nil {
		t.Fatalf("%s approves Operation %s: %v", as.name, name, err)
	}
}

// instancesOf lists the live Instances of a deployment, and how many of them are ready.
func (s *opsStand) instancesOf(md string) (live, ready []v1alpha1.Instance) {
	t := s.t
	t.Helper()
	var l v1alpha1.InstanceList
	ctx, cancel := stack.CallCtx(t)
	defer cancel()
	if err := s.person.c.List(ctx, &l, client.InNamespace(stack.Namespace), client.MatchingLabels{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(md)}); err != nil {
		t.Fatalf("list the Instances of %s: %v", md, err)
	}
	for _, in := range l.Items {
		if in.Status.Phase == v1alpha1.InstanceEnded {
			continue
		}
		live = append(live, in)
		if in.Status.Phase == v1alpha1.InstanceReady {
			ready = append(ready, in)
		}
	}
	return live, ready
}

// allocationsOf lists the deployment's Allocations.
func (s *opsStand) allocationsOf(md string) []v1alpha1.Allocation {
	t := s.t
	t.Helper()
	var l v1alpha1.AllocationList
	ctx, cancel := stack.CallCtx(t)
	defer cancel()
	if err := s.person.c.List(ctx, &l, client.InNamespace(stack.Namespace), client.MatchingLabels{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(md)}); err != nil {
		t.Fatalf("list the Allocations of %s: %v", md, err)
	}
	return l.Items
}

func (s *opsStand) deployment(name string) *v1alpha1.ModelDeployment {
	var md v1alpha1.ModelDeployment
	if err := s.person.get(s.t, client.ObjectKey{Namespace: stack.Namespace, Name: name}, &md); err != nil {
		s.t.Fatalf("get ModelDeployment %s: %v", name, err)
	}
	return &md
}

// policiesOf are the names of the policies the plan recorded for its targets that need approval, by Provider.
func opsPolicies(op *v1alpha1.Operation) map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for _, t := range op.Status.Plan.Targets {
		if !t.NeedsApproval {
			continue
		}
		if out[t.Provider] == nil {
			out[t.Provider] = map[string]bool{}
		}
		for _, p := range t.Policies {
			out[t.Provider][p.Name] = true
		}
	}
	return out
}

// dump logs the state the stand-in's waits and checks read, for the reading of a failure: every Instance of the two deployments
// (ended ones too), every Operation's status, each deployment's spec and counts, and the fake Slurm's jobs by tag. It prints no
// value of a live system: the cluster is the fake Slurm, and no job ID, path or credential is logged, only names, UIDs of this
// run's objects, phases, times, reasons and job states.
func (s *opsStand) dump(label string) {
	t := s.t
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var b strings.Builder
	fmt.Fprintf(&b, "stand-in state, %s (at %s)\n", label, time.Now().UTC().Format(time.RFC3339))
	for _, name := range []string{opsMDP, opsMDQ} {
		var md v1alpha1.ModelDeployment
		if err := s.person.c.Get(ctx, client.ObjectKey{Namespace: stack.Namespace, Name: name}, &md); err != nil {
			fmt.Fprintf(&b, "ModelDeployment %s: %v\n", name, err)
			continue
		}
		over := "unset"
		if md.Spec.OverAllocation != nil {
			over = fmt.Sprint(*md.Spec.OverAllocation)
		}
		fmt.Fprintf(&b, "ModelDeployment %s: spec.instances %d, overAllocation %s, generation %d; status instances %d, ready %d, desired %d\n",
			name, md.Spec.Instances, over, md.Generation, md.Status.Instances, md.Status.ReadyInstances, md.Status.DesiredInstances)
		for _, c := range md.Status.Conditions {
			fmt.Fprintf(&b, "  condition %s=%s (%s): %.300s\n", c.Type, c.Status, c.Reason, c.Message)
		}
		var il v1alpha1.InstanceList
		if err := s.person.c.List(ctx, &il, client.InNamespace(stack.Namespace), client.MatchingLabels{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(name)}); err != nil {
			fmt.Fprintf(&b, "  Instances: %v\n", err)
			continue
		}
		sort.Slice(il.Items, func(i, j int) bool { return il.Items[i].CreationTimestamp.Before(&il.Items[j].CreationTimestamp) })
		for _, in := range il.Items {
			ready, lease, end := "-", "-", "-"
			if in.Status.ReadyTime != nil {
				ready = in.Status.ReadyTime.UTC().Format(time.TimeOnly)
			}
			if in.Status.LeaseEnd != nil {
				lease = in.Status.LeaseEnd.UTC().Format(time.TimeOnly)
			}
			if in.Status.EndTime != nil {
				end = in.Status.EndTime.UTC().Format(time.TimeOnly)
			}
			fmt.Fprintf(&b, "  Instance %s uid %s: phase %q, created %s, ready %s, leaseEnd %s, ended %s (%q), allocation %s, predecessor %q, deleting %v\n",
				in.Name, in.UID, in.Status.Phase, in.CreationTimestamp.UTC().Format(time.TimeOnly), ready, lease, end, in.Status.EndReason,
				in.Spec.AllocationRef.Name, in.Annotations[v1alpha1.AnnotationPredecessor], !in.DeletionTimestamp.IsZero())
		}
		var al v1alpha1.AllocationList
		if err := s.person.c.List(ctx, &al, client.InNamespace(stack.Namespace), client.MatchingLabels{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(name)}); err != nil {
			fmt.Fprintf(&b, "  Allocations: %v\n", err)
			continue
		}
		jobs := map[string]string{}
		for _, j := range s.cl.Plat.Jobs() {
			jobs[j.Name] = j.State
		}
		for _, a := range al.Items {
			tag := v1alpha1.JobTag(a.Namespace, a.Name, a.UID)
			fmt.Fprintf(&b, "  Allocation %s uid %s: deleting %v, release cause %q; fake Slurm job under its tag %s: %q\n",
				a.Name, a.UID, !a.DeletionTimestamp.IsZero(), a.Annotations[v1alpha1.AnnotationReleaseCause], tag, jobs[tag])
		}
	}
	var ol v1alpha1.OperationList
	if err := s.person.c.List(ctx, &ol, client.InNamespace(stack.Namespace)); err != nil {
		fmt.Fprintf(&b, "Operations: %v\n", err)
	}
	for _, op := range ol.Items {
		fmt.Fprintf(&b, "Operation %s: %s of %s, phase %q, finishedAt %v\n", op.Name, op.Spec.Action, op.Spec.ModelDeploymentRef.Name, op.Status.Phase, op.Status.FinishedAt)
		if op.Status.Plan != nil {
			for _, pt := range op.Status.Plan.Targets {
				fmt.Fprintf(&b, "  plan target %s %s: %s, provider %s, needsApproval %v\n", pt.Kind, pt.Name, pt.Effect, pt.Provider, pt.NeedsApproval)
			}
		}
		for _, c := range op.Status.Committed {
			fmt.Fprintf(&b, "  committed %s %s: %s\n", c.Kind, c.Name, c.Effect)
		}
		for _, o := range op.Status.Targets {
			fmt.Fprintf(&b, "  outcome %s %s: %s %s %.300s\n", o.Kind, o.Name, o.State, o.Reason, o.Message)
		}
		for _, c := range op.Status.Conditions {
			fmt.Fprintf(&b, "  condition %s=%s (%s): %.300s\n", c.Type, c.Status, c.Reason, c.Message)
		}
	}
	t.Log(b.String())
}

// instancesLine is the one-line state of a deployment's Instances for a wait's failure message.
func (s *opsStand) instancesLine(md string) string {
	var l v1alpha1.InstanceList
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.person.c.List(ctx, &l, client.InNamespace(stack.Namespace), client.MatchingLabels{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(md)}); err != nil {
		return err.Error()
	}
	sort.Slice(l.Items, func(i, j int) bool { return l.Items[i].CreationTimestamp.Before(&l.Items[j].CreationTimestamp) })
	var parts []string
	for _, in := range l.Items {
		parts = append(parts, fmt.Sprintf("%s %s (predecessor %q, ended %q)", in.Name, in.Status.Phase, in.Annotations[v1alpha1.AnnotationPredecessor], in.Status.EndReason))
	}
	return strings.Join(parts, "; ")
}

// TestStandInOperations is the owner's Sano example and an approved End, end to end on the stand-in cluster, as the file's
// comment says.
//
//livecheck:none a stand-in on this host, no cluster: run by hand with HPCBURST_ALPHA_STANDIN=1
func TestStandInOperations(t *testing.T) {
	if os.Getenv(envStandIn) != "1" {
		t.Skipf("the stand-in proof of the Operations runs only with %s=1 (internal/alpha, standin_operations_test.go)", envStandIn)
	}
	b := standInBinaries(t)
	rehearsal.RequireTools(t)
	stm := stack.Timing{Lease: "20m", MaxRequest: "60s", DrainMargin: "30s", DrainWindow: "10m", HandoverMargin: "30s", StopGrace: "5s", AgentTimeout: "20s", StartTimeout: "4m"}
	arch := stack.CPUArch(t)

	// The stand-in cluster: job IDs of this process's own, one login node, and a Hub double for the checkpoint.
	cl := stack.StartCluster(t, stack.ShortDir(t), stack.ClusterShape{FirstJobID: 10_000_000 + 8*os.Getpid()}, enginedouble.Install, nil)
	hostDir := stack.ShortDir(t)
	creds := hostDir + "/provider-ssh"
	if err := cl.Login.WriteCredentials(creds); err != nil {
		t.Fatal(err)
	}
	hub := stack.StartHub(t)

	// hpcburst up: k3s, the controllers (the guard, the roles and the three policies are installed and enforced before up
	// returns), frps and the front.
	h := stack.StartHost(t, b, hostDir, opsStandInDirect, stack.CleanEnv("HF_ENDPOINT="+hub.URL), opsStandInPorts, "", stack.NameDownBound)
	h.PlaceAgentBuild(arch)
	admin := stack.KubeClient(t, h.State)
	adminCS, err := kubernetes.NewForConfig(stack.RestConfig(t, h.State))
	if err != nil {
		t.Fatal(err)
	}
	s := &opsStand{t: t, h: h, cl: cl, person: opsWhoIs(t, "person", admin, adminCS)}
	// Whatever wait times out, or check fails, the state it read is logged once, before the stack is stopped (a cleanup runs
	// ahead of those StartCluster and StartHost registered): the run keeps no artifacts, so the log is all there is.
	t.Cleanup(func() {
		if t.Failed() {
			s.dump("at the failure")
		}
	})

	// The Providers P and Q over the one login node, and a deployment of two Instances on each.
	provP, provQ := stack.BatchProvider(creds), stack.BatchProvider(creds)
	provQ.Name = opsProviderQ
	stack.Create(t, admin, stack.BatchProfile(arch, stm), provP, provQ)
	for _, name := range []string{stack.ProviderName, opsProviderQ} {
		var p v1alpha1.Provider
		stack.WaitFor(t, stack.WaitDeadline, "Provider "+name+" to be Ready over the login node", func() bool {
			return s.person.get(t, client.ObjectKey{Name: name}, &p) == nil && v1alpha1.ProviderReady(&p, time.Now())
		})
	}
	engine := v1alpha1.EnvVar{Name: "HB_ENGINE_DOUBLE", Value: "token-delay=" + standInTokenDelay.String()}
	zero := int32(0)
	mdP := stack.ServedDeploymentAs(stack.Served{Name: opsMDP, Model: "example-org/model-p", Provider: stack.ProviderName, MaxModelLen: "4096"}, stm, engine)
	mdQ := stack.ServedDeploymentAs(stack.Served{Name: opsMDQ, Model: "example-org/model-q", Provider: opsProviderQ, MaxModelLen: "2048"}, stm, engine)
	for _, md := range []*v1alpha1.ModelDeployment{mdP, mdQ} {
		md.Spec.Instances, md.Spec.OverAllocation = 2, &zero
	}
	stack.Create(t, admin, mdP, mdQ)
	for _, name := range []string{opsMDP, opsMDQ} {
		stack.WaitState(t, 10*time.Minute, "two ready Instances of "+name, func() (bool, string) {
			live, ready := s.instancesOf(name)
			return len(ready) == 2, fmt.Sprintf("%d live, %d ready", len(live), len(ready))
		})
	}

	// The identities: two agents, each with a kubeconfig of its own.
	s.a, s.b = s.addIdentity("agent-a"), s.addIdentity("agent-b")
	// up wrote the global policy, which names the person: the example relies on it.
	var global v1alpha1.ApprovalPolicy
	if err := s.person.get(t, client.ObjectKey{Name: "global"}, &global); err != nil {
		t.Fatalf("the global ApprovalPolicy that up writes: %v", err)
	}
	named := false
	for _, ap := range global.Spec.Approvers {
		named = named || ap.User == s.person.user
	}
	if !named {
		t.Fatalf("the global policy's approvers %+v do not name the person, %s", global.Spec.Approvers, s.person.user)
	}
	// The Provider policy: agent A may approve what happens on Provider P.
	policyP := &v1alpha1.ApprovalPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "provider." + stack.ProviderName},
		Spec: v1alpha1.ApprovalPolicySpec{
			Scope:     v1alpha1.ApprovalScope{Provider: stack.ProviderName},
			Approvers: []v1alpha1.Approver{{ServiceAccount: &v1alpha1.ServiceAccountRef{Namespace: opsAgentsNS, Name: "agent-a"}}},
		},
	}
	stack.Create(t, admin, policyP)

	s.dump("before A's Scale of the deployment on P")

	// A's Scale of the deployment on P, to one Instance: its targets are on Provider P, whose policy names A, so A's own
	// approval runs it.
	planP := s.request(s.a, "scale-p", opsMDP, v1alpha1.OperationSpec{Action: v1alpha1.OperationActionScale, Scale: &v1alpha1.ScaleParameters{Instances: 1}})
	if got := opsPolicies(planP)[stack.ProviderName]; !got["provider."+stack.ProviderName] || len(got) != 1 || len(opsPolicies(planP)) != 1 {
		t.Fatalf("the plan of A's Scale on P records the policies %v, want the one Provider policy of P alone", opsPolicies(planP))
	}
	s.confirm(s.a, "scale-p")
	s.waitOp("scale-p", "AwaitingApproval", func(o *v1alpha1.Operation) bool { return o.Status.Phase == v1alpha1.OperationPhaseAwaitingApproval })
	if got := s.deployment(opsMDP).Spec.Instances; got != 2 {
		t.Fatalf("spec.instances of P's deployment is %d before the approval, want 2", got)
	}
	s.approve(s.a, "scale-p")
	s.waitOp("scale-p", "Done", func(o *v1alpha1.Operation) bool { return o.Status.Phase == v1alpha1.OperationPhaseDone })
	if got := s.deployment(opsMDP).Spec.Instances; got != 1 {
		t.Errorf("after A's own approval, spec.instances of P's deployment is %d, want 1", got)
	}
	s.dump("A's Scale of the deployment on P is Done")
	stack.WaitState(t, 5*time.Minute, "one ready Instance of "+opsMDP, func() (bool, string) {
		live, ready := s.instancesOf(opsMDP)
		return len(live) == 1 && len(ready) == 1, fmt.Sprintf("%d live, %d ready: %s", len(live), len(ready), s.instancesLine(opsMDP))
	})

	// A's Scale of the deployment on Q: Provider Q has no policy of its own, so the global policy applies, which names the
	// person alone. A's approval and B's count under none; the person's runs it.
	planQ := s.request(s.a, "scale-q", opsMDQ, v1alpha1.OperationSpec{Action: v1alpha1.OperationActionScale, Scale: &v1alpha1.ScaleParameters{Instances: 1}})
	if got := opsPolicies(planQ)[opsProviderQ]; !got["global"] || len(got) != 1 {
		t.Fatalf("the plan of A's Scale on Q records the policies %v, want the global policy alone", opsPolicies(planQ))
	}
	s.confirm(s.a, "scale-q")
	s.waitOp("scale-q", "AwaitingApproval", func(o *v1alpha1.Operation) bool { return o.Status.Phase == v1alpha1.OperationPhaseAwaitingApproval })
	s.approve(s.a, "scale-q")
	s.approve(s.b, "scale-q")
	// A and B's Approvals count for nothing under Q's policies, and an Approval that counts for nothing makes the controller
	// write nothing, so the Scale's own state cannot show that a pass has seen them. A sentinel does: a fresh Operation on Q
	// (an End, only ever planned: it is never confirmed, run, or read by a later check) reads Planned once a pass of Q's key
	// has run after both Approvals exist. Only then is the Scale read: still waiting for the person, with nothing committed.
	s.request(s.a, "sentinel-q", opsMDQ, v1alpha1.OperationSpec{Action: v1alpha1.OperationActionEnd})
	if op := s.op("scale-q"); op.Status.Phase != v1alpha1.OperationPhaseAwaitingApproval || len(op.Status.Committed) != 0 {
		t.Fatalf("with A's and B's approvals only, Operation scale-q is %s with %d committed targets, want it waiting for the person", opsDescribe(op), len(op.Status.Committed))
	}
	// Its targets still lack the approval: the condition names each target and the policy it waits for, which here is the
	// global policy that names the person alone.
	var waiting string
	for _, c := range s.op("scale-q").Status.Conditions {
		if c.Type == "Executable" {
			waiting = c.Reason + ": " + c.Message
		}
	}
	if !strings.HasPrefix(waiting, v1alpha1.ReasonAwaitingApproval+": ") || !strings.Contains(waiting, "needs an approval under global") {
		t.Fatalf("Operation scale-q's Executable condition is %q after A's and B's approvals, want it still awaiting approval under the global policy", waiting)
	}
	if got := s.deployment(opsMDQ).Spec.Instances; got != 2 {
		t.Fatalf("spec.instances of Q's deployment is %d while the Operation waits for the person, want 2", got)
	}
	s.approve(s.person, "scale-q")
	s.waitOp("scale-q", "Done", func(o *v1alpha1.Operation) bool { return o.Status.Phase == v1alpha1.OperationPhaseDone })
	if got := s.deployment(opsMDQ).Spec.Instances; got != 1 {
		t.Errorf("after the person's approval, spec.instances of Q's deployment is %d, want 1", got)
	}

	// The person's End of the deployment on P. The tags of its Allocations' jobs are read first: the fake Slurm's own record
	// of those jobs is the end of the example.
	var tags []string
	for _, a := range s.allocationsOf(opsMDP) {
		if a.DeletionTimestamp.IsZero() {
			tags = append(tags, v1alpha1.JobTag(a.Namespace, a.Name, a.UID))
		}
	}
	if len(tags) == 0 {
		t.Fatal("the deployment on P has no Allocation to end")
	}
	// jobsOf is the fake Slurm's own record of the jobs under the tags, by tag.
	jobsOf := func() map[string]string {
		out := map[string]string{}
		for _, j := range cl.Plat.Jobs() {
			for _, tag := range tags {
				if j.Name == tag {
					out[tag] = j.State
				}
			}
		}
		return out
	}
	// Before the approval every job of the deployment RUNS: the End is then what ends them, and a job that had ended by its
	// own time limit before it could not pass the check below.
	stack.WaitState(t, 3*time.Minute, "every job of the deployment on P to be running before the End is approved", func() (bool, string) {
		got := jobsOf()
		for _, tag := range tags {
			if got[tag] != fakeslurm.Running {
				return false, fmt.Sprintf("jobs %v", got)
			}
		}
		return true, ""
	})
	s.request(s.person, "end-p", opsMDP, v1alpha1.OperationSpec{Action: v1alpha1.OperationActionEnd})
	s.confirm(s.person, "end-p")
	s.waitOp("end-p", "AwaitingApproval", func(o *v1alpha1.Operation) bool { return o.Status.Phase == v1alpha1.OperationPhaseAwaitingApproval })
	// Before the approval nothing is released: a sentinel (an End of the same deployment, only ever planned) reads Planned
	// once a pass of its key has run after the End waits, and only then is the End read, still waiting, with nothing committed,
	// no end time, every Allocation alive and every job running.
	s.request(s.person, "sentinel-p", opsMDP, v1alpha1.OperationSpec{Action: v1alpha1.OperationActionEnd})
	if op := s.op("end-p"); op.Status.Phase != v1alpha1.OperationPhaseAwaitingApproval || len(op.Status.Committed) != 0 {
		t.Fatalf("Operation end-p is %s with %d committed targets before the approval, want it waiting with none", opsDescribe(op), len(op.Status.Committed))
	}
	if md := s.deployment(opsMDP); md.Spec.EndTime != nil {
		t.Errorf("the deployment on P has the end time %v before the End is approved", md.Spec.EndTime)
	}
	if live, _ := s.instancesOf(opsMDP); len(live) == 0 {
		t.Error("the deployment on P has no live Instance before the End is approved")
	}
	for _, a := range s.allocationsOf(opsMDP) {
		if !a.DeletionTimestamp.IsZero() {
			t.Errorf("Allocation %s is being deleted before the End is approved", a.Name)
		}
	}
	for tag, state := range jobsOf() {
		if state != fakeslurm.Running {
			t.Errorf("the job %s is %s before the End is approved, want RUNNING", tag, state)
		}
	}
	s.approve(s.person, "end-p")
	// The End is Done once every target is. The Allocations are read while it runs: the release cause each carries, which is
	// written before the delete, is the End's own proof that it, and not the end of a time limit, released the job.
	causes := map[string]string{}
	stack.WaitState(t, 10*time.Minute, "Operation end-p: Done", func() (bool, string) {
		for _, a := range s.allocationsOf(opsMDP) {
			if c := a.Annotations[v1alpha1.AnnotationReleaseCause]; c != "" {
				causes[v1alpha1.JobTag(a.Namespace, a.Name, a.UID)] = c
			}
		}
		op := s.op("end-p")
		return op.Status.Phase == v1alpha1.OperationPhaseDone, opsDescribe(op)
	})
	end := s.op("end-p")
	for _, o := range end.Status.Targets {
		if o.State != v1alpha1.TargetStateDone {
			t.Errorf("target %s %s of the End is %s, want Done", o.Kind, o.Name, o.State)
		}
	}
	if md := s.deployment(opsMDP); md.Spec.EndTime == nil {
		t.Error("the deployment on P has no end time after its End")
	}
	if live, _ := s.instancesOf(opsMDP); len(live) != 0 {
		t.Errorf("the deployment on P still has %d live Instances after its End is Done", len(live))
	}
	// The fake Slurm's own record: every job of the deployment's Allocations has ended, none is left running or pending.
	stack.WaitState(t, 3*time.Minute, "the fake Slurm to show every job of the deployment on P ended", func() (bool, string) {
		got := jobsOf()
		for _, tag := range tags {
			if !fakeslurm.IsTerminal(got[tag]) {
				return false, fmt.Sprintf("jobs %v", got)
			}
		}
		return true, ""
	})
	// Each job ended because the End released it: the fake Slurm says CANCELLED (the Provider's finalizer cancelled it), or
	// the release cause its Allocation carried names this End by namespace, name and UID. A job that ran out its time limit
	// (TIMEOUT), or failed, is neither: it fails the check, whatever else holds.
	got := jobsOf()
	for _, tag := range tags {
		state, seen := got[tag]
		if !seen {
			t.Errorf("the fake Slurm holds no job under the tag %s: the check read nothing", tag)
			continue
		}
		ns, name, uid, ok := v1alpha1.ParseReleaseCauseOperation(causes[tag])
		byThisEnd := ok && ns == end.Namespace && name == end.Name && uid == end.UID
		switch {
		case state == fakeslurm.Timeout || state == fakeslurm.NodeFail || state == fakeslurm.Failed:
			t.Errorf("the job %s ended %s: it ran out its time limit or failed, the End did not end it", tag, state)
		case state != fakeslurm.Cancelled && !byThisEnd:
			t.Errorf("the job %s ended %s, and its Allocation's release cause was %q: want CANCELLED or the cause of Operation %s/%s %s", tag, state, causes[tag], end.Namespace, end.Name, end.UID)
		}
	}
	// The stack's own stop is down's, registered by StartHost; the cluster's clean-up kills what is left.
}
