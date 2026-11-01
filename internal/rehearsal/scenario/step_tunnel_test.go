package scenario_test

// Step (e) of issue #146: a tunnel loss. frpc in the primary's job is killed;
// TunnelConnected turns False, the front's probe drops the Instance's
// transport so it leaves routing, and the agent starts frpc again, which
// brings both back (HBEP-6, frpc in the job; Path state and the inventory;
// The front's half).

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// stagedFrpc is the frpc file the controller staged on the cluster
// (<staging dir>/frpc/<digest>/frpc, agentapi.StagedFrpcPath), resolved. The
// agent runs it as /proc/self/fd/3, so a frpc process's exe link names it.
func (r *rig) stagedFrpc() string {
	r.t.Helper()
	var found []string
	_ = filepath.WalkDir(r.cl.scratch, func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() && d.Name() == agentapi.FrpcBinaryName &&
			filepath.Base(filepath.Dir(filepath.Dir(p))) == "frpc" {
			found = append(found, p)
		}
		return nil
	})
	if len(found) != 1 {
		r.t.Fatalf("step (e): %d staged frpc files under %s (%v), want 1", len(found), r.cl.scratch, found)
	}
	p, err := filepath.EvalSymlinks(found[0])
	if err != nil {
		r.t.Fatal(err)
	}
	return p
}

// frpcProcesses lists the job's live processes whose executable is the
// staged frpc.
func (r *rig) frpcProcesses(jobID int, staged string) []int {
	r.t.Helper()
	pids, err := r.cl.plat.Processes(jobID)
	if err != nil {
		r.t.Fatalf("job %d's processes: %v", jobID, err)
	}
	var out []int
	for _, p := range pids {
		if processExe(p) == staged && alive(p) {
			out = append(out, p)
		}
	}
	return out
}

// parentOf is a live process's parent PID, 0 when it is gone.
func parentOf(pid int) int {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0
	}
	f := strings.Fields(string(b[bytes.LastIndexByte(b, ')')+1:]))
	if len(f) < 2 {
		return 0
	}
	n, _ := strconv.Atoi(f[1])
	return n
}

// stepTunnelLoss is step (e). The agent starts a killed frpc again after a
// second, and the front's probe needs two failures 2 s apart, so one kill
// may heal before either the controller's read or the probe sees it: every
// frpc the agent starts is killed until both have seen the loss, and the
// number of kills is reported.
func (r *rig) stepTunnelLoss(in *v1alpha1.Instance, key string) {
	r.t.Helper()
	defer r.stepTimer("e")()
	r.beforeDrain("e", in)
	before, ok := r.instance(in.Name)
	if reg, known := registered(in); !ok || before.Status.Phase != v1alpha1.InstanceReady ||
		!conditionTrue(before.Status.Conditions, v1alpha1.ConditionTunnelConnected) || !known || !reg {
		r.t.Fatalf("step (e) needs a ready, connected and registered Instance %s: %+v, registered %v (known %v)", in.Name, before.Status, reg, known)
	}
	jobID, _ := strconv.Atoi(before.Status.JobID)
	staged := r.stagedFrpc()
	cur := r.frpcProcesses(jobID, staged)
	if len(cur) != 1 {
		r.t.Fatalf("step (e): job %d has %d frpc processes %v running %s, want 1", jobID, len(cur), cur, staged)
	}
	parent := parentOf(cur[0])
	start := time.Now()
	killed := map[int]bool{}
	var falseAt, goneAt time.Time
	waitFor(r.t, waitDeadline, "TunnelConnected False and the front's registration dropped", func() bool {
		if o, ok := r.watch.first(in.UID, start, func(o observation) bool { return o.tunnel == metav1.ConditionFalse }); ok && falseAt.IsZero() {
			falseAt = o.at
		}
		if reg, known := registered(in); known && !reg && goneAt.IsZero() {
			goneAt = time.Now()
		}
		if !falseAt.IsZero() && !goneAt.IsZero() {
			return true
		}
		for _, p := range r.frpcProcesses(jobID, staged) {
			if !killed[p] {
				if err := syscall.Kill(p, syscall.SIGKILL); err != nil {
					r.t.Fatalf("step (e): kill frpc %d: %v", p, err)
				}
				killed[p] = true
			}
		}
		return false
	})
	r.t.Logf("step (e): %d frpc kills; TunnelConnected False seen %s and the front's registration gone %s after the first",
		len(killed), falseAt.Sub(start).Round(100*time.Millisecond), goneAt.Sub(start).Round(100*time.Millisecond))
	r.checkTunnelBack(in, key, jobID, staged, killed, parent, start)
}

// checkTunnelBack: the agent starts frpc again (a new process, child of the
// same parent as the killed one), TunnelConnected is True again, the front
// registers the transport and routes the key to the Instance, and the agent
// was never judged unreachable: the loss was the tunnel's.
func (r *rig) checkTunnelBack(in *v1alpha1.Instance, key string, jobID int, staged string, killed map[int]bool, parent int, start time.Time) {
	r.t.Helper()
	fresh := 0
	waitFor(r.t, waitDeadline, "the agent to start frpc again", func() bool {
		for _, p := range r.frpcProcesses(jobID, staged) {
			if !killed[p] {
				fresh = p
				return true
			}
		}
		return false
	})
	if pp := parentOf(fresh); pp != parent {
		r.t.Errorf("step (e): the new frpc %d has parent %d; the killed one had %d, the agent", fresh, pp, parent)
	}
	lost := time.Now()
	waitFor(r.t, waitDeadline, "TunnelConnected True and the front's registration back", func() bool {
		now, ok := r.instance(in.Name)
		reg, known := registered(in)
		return ok && conditionTrue(now.Status.Conditions, v1alpha1.ConditionTunnelConnected) && known && reg
	})
	r.t.Logf("step (e): the tunnel and the registration came back %s after the agent's new frpc was seen", time.Since(lost).Round(100*time.Millisecond))
	rp := r.send("/v1/chat/completions", chatBody(4, 0, false), key)
	if rp.err != nil || rp.status != 200 {
		r.t.Fatalf("step (e): a request after the tunnel came back: %d %v: %.300s", rp.status, rp.err, rp.body)
	}
	if row := r.rowOf(rp.id); row.InstanceUID != string(in.UID) {
		r.t.Errorf("step (e): the key's request after the tunnel came back went to %s, not %s", row.Instance, in.Name)
	}
	phases := map[v1alpha1.InstancePhase]bool{}
	for _, o := range r.watch.between(in.UID, start, time.Now()) {
		phases[o.phase] = true
		if o.unreachable {
			r.t.Errorf("step (e): Instance %s was judged AgentUnreachable at %s: the agent's reports do not use frpc", in.Name, o.at.Format(time.RFC3339Nano))
		}
	}
	r.t.Logf("step (e): the Instance's phases during the loss: %v", phases)
	r.checkNoGap(r.usageRows())
}
