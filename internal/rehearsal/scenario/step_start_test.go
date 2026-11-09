package scenario_test

// Step (a) of issue #146: an Allocation submitted held and released, the agent
// in the job, staging from the Hub double, verification, qualification and
// warm-up, and an Instance that enters ready only once its tunnel is
// connected (HBEP-6, Path state and the inventory; HBEP-7, Instance start and
// ready). Asserted from the API, the watch and the fake cluster's call log.

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// stepStart is step (a). The primary is the Instance on the first Allocation;
// its replacement is created at its start when no queue wait is observed
// (HBEP-7), so two Instances start, and the replacement's tunnel is left
// alone. The primary's tunnel is held down until its checks are done
// (holdTunnel), so only the first-entry gate keeps it out of ready meanwhile.
func (r *rig) stepStart() *v1alpha1.Instance {
	r.t.Helper()
	r.providerReady()
	stack.Create(r.t, r.c, stack.ServedDeployment(rehearsalTiming, r.engineEnv...))
	var alloc v1alpha1.Allocation
	stack.WaitFor(r.t, stack.WaitDeadline, "an Allocation with a job ID", func() bool {
		for _, a := range r.allocations() {
			if a.Status.AtProvider.JobID != "" {
				alloc = a
				return true
			}
		}
		r.t.Logf("waiting:%s", r.describe())
		return false
	})
	id, err := strconv.Atoi(alloc.Status.AtProvider.JobID)
	if err != nil {
		r.t.Fatalf("job ID %q: %v", alloc.Status.AtProvider.JobID, err)
	}
	stack.WaitFor(r.t, stack.WaitDeadline, "the job to start in the fake cluster", func() bool {
		j, ok := r.cl.Plat.Job(id)
		return ok && j.PID > 0
	})
	r.checkReleased(id)
	uid := r.holdTunnel(alloc.Name, id)
	var ready *v1alpha1.Instance
	stack.WaitFor(r.t, stack.WaitDeadline, "the primary Instance to be ready", func() bool {
		for _, in := range r.instances() {
			if in.UID == uid && in.Status.Phase == v1alpha1.InstanceReady {
				in := in
				ready = &in
				return true
			}
		}
		return false
	})
	if p := ready.Annotations[v1alpha1.AnnotationPredecessor]; p != "" {
		r.t.Errorf("step (a): the Instance on the first Allocation %s has a predecessor %s", alloc.Name, p)
	}
	r.checkFirstEntry(ready)
	r.checkStartRecords(ready)
	r.checkAgentStaged()
	return ready
}

// checkReleased: the job was submitted held, and started only after a
// scontrol release of its own ID (the fake cluster's call log).
func (r *rig) checkReleased(id int) {
	r.t.Helper()
	j, _ := r.cl.Plat.Job(id)
	want := strconv.Itoa(id)
	var seen []string
	for _, c := range r.cl.Plat.Calls() {
		if len(c.Words) == 0 || c.Words[0] != "scontrol" {
			continue
		}
		seen = append(seen, strings.Join(c.Words, " "))
		if slices.Contains(c.Words, "release") && slices.Contains(c.Words, want) && !c.Time.After(j.Start) {
			return
		}
	}
	r.t.Errorf("step (a): job %d started at %s with no scontrol release of it before (scontrol calls %q): it was not submitted held", id, j.Start.Format(time.RFC3339Nano), seen)
}

// holdTunnel keeps the tunnel of the Instance on the Allocation down until the
// controller has judged its warm-up, and returns the Instance's UID.
//
// The agent starts frpc at the Instance's first run instruction, long before
// its checks end (internal/agent/converge.go, serveTunnel), so without a hold
// the tunnel is always up first and the gate is never what keeps an Instance
// out of ready. Here every frpc the agent starts is killed as soon as it shows
// among the agent's children; the agent starts it again after a backoff that
// doubles from 1 s (internal/agent/frpc.go). The hold lasts until the watch
// has seen WarmedUp True and, a lower bound, one agent report interval more,
// so the controller reconciles the Instance again with its checks done and
// its tunnel down. Then frpc is left alone, connects, and the condition turns
// True at the controller's next read of frps's list.
func (r *rig) holdTunnel(allocName string, jobID int) types.UID {
	r.t.Helper()
	deadline, _ := stack.BoundedDeadline(r.t, stack.WaitDeadline)
	start := time.Now()
	agent, uid := 0, types.UID("")
	killed := map[int]bool{}
	var warmAt, lastScan time.Time
	for {
		if agent == 0 && time.Since(lastScan) >= 100*time.Millisecond {
			agent, lastScan = r.jobAgent(jobID), time.Now()
		}
		if agent != 0 {
			for _, c := range stack.ChildrenOf(agent) {
				if killed[c] || !isStagedFrpc(stack.ProcessExe(c)) {
					continue
				}
				if err := syscall.Kill(c, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
					r.t.Fatalf("step (a): kill frpc %d: %v", c, err)
				}
				killed[c] = true
			}
		}
		if uid == "" {
			uid, _ = r.watch.OnAllocation(allocName)
		}
		if uid != "" && warmAt.IsZero() {
			if o, ok := r.watch.First(uid, time.Time{}, func(o stack.Observation) bool { return o.WarmedUp }); ok {
				warmAt = o.At
			}
		}
		if !warmAt.IsZero() && time.Since(warmAt) >= agentapi.ReportInterval {
			break
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("step (a): no WarmedUp True for the Instance on %s while its tunnel was held (%d frpc kills, agent %d):%s", allocName, len(killed), agent, r.describe())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(killed) == 0 {
		r.t.Errorf("step (a): the agent of job %d started no frpc before the warm-up was judged: nothing was held, so the check below says nothing of the gate", jobID)
	}
	r.t.Logf("step (a): the tunnel was held %s with %d frpc kills; WarmedUp True was seen %s into the hold",
		time.Since(start).Round(100*time.Millisecond), len(killed), warmAt.Sub(start).Round(100*time.Millisecond))
	return uid
}

// jobAgent is the PID of the job's agent, the staged hb-agent
// (agentapi.StagedAgentPath), 0 while none runs.
func (r *rig) jobAgent(jobID int) int {
	pids, err := r.cl.Plat.Processes(jobID)
	if err != nil {
		return 0
	}
	for _, p := range pids {
		exe := stack.ProcessExe(p)
		if filepath.Base(exe) == agentapi.AgentBinaryName && filepath.Base(filepath.Dir(filepath.Dir(exe))) == "agent" {
			return p
		}
	}
	return 0
}

// isStagedFrpc reports whether exe is a staged frpc
// (<staging dir>/frpc/<digest>/frpc, agentapi.StagedFrpcPath).
func isStagedFrpc(exe string) bool {
	return filepath.Base(exe) == agentapi.FrpcBinaryName && filepath.Base(filepath.Dir(filepath.Dir(exe))) == "frpc"
}

// checkFirstEntry holds the Instance's versions, as the watch saw them, to the
// first-entry gate (internal/lifecycle/verify.go): before its first ready it
// was pending, starting or verifying; while its warm-up was judged and its
// tunnel not connected it stayed verifying; TunnelConnected turned True after
// that and before it was first ready; and it was first ready with
// TunnelConnected True, Verified, Qualified and WarmedUp True and its ready
// time set, the checks judged in their order.
func (r *rig) checkFirstEntry(in *v1alpha1.Instance) {
	r.t.Helper()
	obs := r.watch.Of(in.UID)
	firstReady := slices.IndexFunc(obs, func(o stack.Observation) bool { return o.Phase == v1alpha1.InstanceReady })
	if firstReady < 0 {
		r.t.Fatalf("step (a): the watch never saw Instance %s ready", in.Name)
	}
	held, tunnelUp := -1, -1
	for i, o := range obs[:firstReady] {
		switch o.Phase {
		case "", v1alpha1.InstancePending, v1alpha1.InstanceStarting, v1alpha1.InstanceVerifying:
		default:
			r.t.Errorf("step (a): before its first ready, Instance %s was seen %s at %s", in.Name, o.Phase, o.At.Format(time.RFC3339Nano))
		}
		if o.WarmedUp && o.Tunnel != metav1.ConditionTrue {
			if o.Phase != v1alpha1.InstanceVerifying {
				r.t.Errorf("step (a): with its warm-up judged and TunnelConnected %q, Instance %s was %s, not verifying", o.Tunnel, in.Name, o.Phase)
			}
			if held < 0 {
				held = i
			}
		}
		if held >= 0 && tunnelUp < 0 && o.Tunnel == metav1.ConditionTrue {
			tunnelUp = i
		}
	}
	f := obs[firstReady]
	switch {
	case held < 0:
		r.t.Errorf("step (a): Instance %s was never seen with WarmedUp True and TunnelConnected not True: the gate was not what kept it out of ready", in.Name)
	case tunnelUp < 0:
		r.t.Errorf("step (a): Instance %s was first seen ready at %s with no TunnelConnected True seen after its warm-up: it entered ready with its tunnel down",
			in.Name, f.At.Format(time.RFC3339Nano))
	default:
		r.t.Logf("step (a): Instance %s verifying with its checks done and its tunnel down from %s, TunnelConnected True %s, ready %s", in.Name,
			obs[held].At.Format(time.RFC3339Nano), obs[tunnelUp].At.Format(time.RFC3339Nano), f.At.Format(time.RFC3339Nano))
	}
	if f.Tunnel != metav1.ConditionTrue || !f.Verified || !f.Qualified || !f.WarmedUp || !f.ReadyTime {
		r.t.Errorf("step (a): Instance %s's first ready version: TunnelConnected %q, Verified %v, Qualified %v, WarmedUp %v, readyTime set %v; want all", in.Name,
			f.Tunnel, f.Verified, f.Qualified, f.WarmedUp, f.ReadyTime)
	}
	iv := slices.IndexFunc(obs, func(o stack.Observation) bool { return o.Verified })
	iq := slices.IndexFunc(obs, func(o stack.Observation) bool { return o.Qualified })
	iw := slices.IndexFunc(obs, func(o stack.Observation) bool { return o.WarmedUp })
	if iv < 0 || iq < iv || iw < iq {
		r.t.Errorf("step (a): Instance %s's checks were first seen True at versions %d (Verified), %d (Qualified), %d (WarmedUp): not in the order the controller judges them", in.Name, iv, iq, iw)
	}
}

// checkStartRecords reads what the controller persisted for the Instance's
// start: the verification record its status names, which passed, and a ready
// time no earlier than its tunnel's connection, both on the controller's own
// clock.
func (r *rig) checkStartRecords(in *v1alpha1.Instance) {
	r.t.Helper()
	now, ok := r.instance(in.Name)
	if !ok {
		r.t.Fatalf("step (a): Instance %s is gone", in.Name)
	}
	name := now.Status.VerificationRecord
	uid, gen, err := v1alpha1.ParseVerificationRecordName(name)
	if err != nil || uid != in.UID || now.Status.Verdict != v1alpha1.VerdictPass {
		r.t.Errorf("step (a): Instance %s names verification record %q (%v) with verdict %q; want one of its own that passed", in.Name, name, err, now.Status.Verdict)
	}
	var rec appsv1.ControllerRevision
	if !r.get(&rec, name, in.Namespace) {
		r.t.Errorf("step (a): the verification record %s is not in the API", name)
	} else {
		var d struct {
			Instance   string           `json:"instance"`
			Generation int64            `json:"generation"`
			Verdict    v1alpha1.Verdict `json:"verdict"`
		}
		if err := json.Unmarshal(rec.Data.Raw, &d); err != nil || d.Instance != in.Name || d.Generation != gen || d.Verdict != v1alpha1.VerdictPass {
			r.t.Errorf("step (a): verification record %s holds %+v (%v); want Instance %s, generation %d, verdict pass", name, d, err, in.Name, gen)
		}
	}
	tc := meta.FindStatusCondition(now.Status.Conditions, v1alpha1.ConditionTunnelConnected)
	if now.Status.ReadyTime == nil || tc == nil || tc.Status != metav1.ConditionTrue || now.Status.ReadyTime.Before(&tc.LastTransitionTime) {
		r.t.Errorf("step (a): Instance %s's ready time %v and TunnelConnected %+v: not ready after its tunnel connected", in.Name, now.Status.ReadyTime, tc)
	}
}

// checkAgentStaged: the agent staged from the Hub double, whose address
// reached the job only through agent.json. The LFS file shows it: the
// controller downloads only the files outside LFS, to hash them, and takes an
// LFS file's SHA-256 from the listing, so only the agent fetches the weights.
func (r *rig) checkAgentStaged() {
	r.t.Helper()
	for _, p := range r.hub.Resolved() {
		if stack.HubFileIsLFS(p) {
			return
		}
	}
	r.t.Errorf("the agent never downloaded the LFS weights from the Hub double (downloads %v): agent.json's hubEndpoint did not reach it", r.hub.Resolved())
}
