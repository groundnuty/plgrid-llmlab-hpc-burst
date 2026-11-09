package scenario_test

// Step (f) of issue #146's scenario: a controller restart while the Instance
// serves, asserted from the API, the fake cluster's job snapshots, frps's
// proxy list and the launcher's own status, never from log lines.

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// statusPID is the PID that hpcburst status reports for a supervised entry,
// 0 when it reports none running.
func (r *rig) statusPID(name string) int {
	r.t.Helper()
	out, err := stack.Run(r.t, stack.Within(r.t, stack.StatusTimeout), r.h.env, r.b.hpcburst, "status", "--state-dir", r.h.state, "--json", "--no-api")
	var doc any
	if jerr := json.Unmarshal([]byte(out), &doc); jerr != nil {
		r.t.Logf("hpcburst status --json (%v): %v: %.300s", err, jerr, out)
		return 0
	}
	pid := 0
	var walk func(any)
	walk = func(o any) {
		switch v := o.(type) {
		case map[string]any:
			if v["name"] == name && v["running"] == true {
				if p, ok := v["pid"].(float64); ok {
					pid = int(p)
				}
			}
			for _, x := range v {
				walk(x)
			}
		case []any:
			for _, x := range v {
				walk(x)
			}
		}
	}
	walk(doc)
	return pid
}

// leaseHolder is the lifecycle controller's Lease holder, "" when unknown.
func (r *rig) leaseHolder() string {
	var l coordinationv1.Lease
	ctx, cancel := stack.CallCtx(r.t)
	defer cancel()
	if err := r.c.Get(ctx, types.NamespacedName{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle}, &l); err != nil || l.Spec.HolderIdentity == nil {
		return ""
	}
	return *l.Spec.HolderIdentity
}

func (r *rig) instance(name string) (v1alpha1.Instance, bool) {
	var in v1alpha1.Instance
	ok := r.get(&in, name, stack.Namespace)
	return in, ok
}

// stepControllerRestart is step (f): the controller is killed while the
// Instance serves, and the supervisor starts it again. The new controller
// leads the Lease, and for one agentTimeout after that (the profile's, scaled
// to rehearsalAgentTimeout) the agent keeps serving: the Instance stays ready
// with the same agentInstanceID and no new restart, its job keeps its process,
// and no Allocation is added. That is HBEP-7's "the controller restarts while
// agents serve". The tunnel half (HBEP-6): frps's proxy for the Instance,
// online before the kill, stays online throughout, and a stream through the
// front, in flight at the kill and sized to outlast the new controller's
// takeover, ends with its finish reason and [DONE].
func (r *rig) stepControllerRestart(in *v1alpha1.Instance, key string) {
	r.t.Helper()
	r.controllerRestart(in, key, chatBody(restartTokens, restartTokens, true), restartTokens)
}

// controllerRestart is step (f) with a streamed request body whose reply is
// tokens long, which must outlast the new controller's takeover of the Lease.
// It returns the stream and its usage record.
func (r *rig) controllerRestart(in *v1alpha1.Instance, key, body string, tokens int) (streamed, usageRow) {
	r.t.Helper()
	defer r.stepTimer("f")()
	r.beforeDrain("f", in)
	before, ok := r.instance(in.Name)
	if !ok || before.Status.Phase != v1alpha1.InstanceReady || before.Status.Agent == nil {
		r.t.Fatalf("step (f) needs a ready, registered Instance %s: %+v", in.Name, before.Status)
	}
	jobID, _ := strconv.Atoi(before.Status.JobID)
	job, _ := r.cl.plat.Job(jobID)
	allocs := len(r.allocations())
	online := r.frpsProxies()
	if !online[string(in.UID)] {
		r.t.Fatalf("step (f): frps lists no online proxy for Instance %s (%s) before the kill (online: %v); the check would be vacuous", in.Name, in.UID, online)
	}
	oldPID, oldHolder := r.statusPID("controller"), r.leaseHolder()
	if oldPID == 0 || oldHolder == "" {
		r.t.Fatalf("step (f): the controller's PID %d or Lease holder %q is unknown", oldPID, oldHolder)
	}
	first, done := r.startStream(body, key)
	<-first
	if err := syscall.Kill(oldPID, syscall.SIGKILL); err != nil {
		r.t.Fatalf("kill the controller %d: %v", oldPID, err)
	}
	killAt := time.Now()
	stack.WaitFor(r.t, stack.WaitDeadline, "the supervisor to start a new controller that leads the Lease", func() bool {
		p := r.statusPID("controller")
		h := r.leaseHolder()
		return p != 0 && p != oldPID && h != "" && h != oldHolder &&
			stack.HTTPStatus(r.ports.url(6, "/readyz")) == http.StatusOK
	})
	ledAt := time.Now()
	r.t.Logf("step (f): a new controller led the Lease %s after the kill", ledAt.Sub(killAt).Round(100*time.Millisecond))
	if r.onLed != nil {
		r.onLed()
	}
	// For one agentTimeout the new controller would have judged a silent
	// agent unreachable; sample until then that nothing gave way.
	until := time.Now().Add(rehearsalAgentTimeout + 5*time.Second)
	for time.Now().Before(until) {
		now, ok := r.instance(in.Name)
		switch {
		case !ok:
			r.t.Fatalf("step (f): Instance %s is gone after the controller restart", in.Name)
		case now.Status.Phase != v1alpha1.InstanceReady:
			r.t.Fatalf("step (f): Instance %s left ready (%s, %s) after the controller restart", in.Name, now.Status.Phase, now.Status.EndReason)
		case conditionTrue(now.Status.Conditions, v1alpha1.ConditionAgentUnreachable):
			r.t.Fatalf("step (f): the new controller judged the agent unreachable: %v", now.Status.Conditions)
		case len(now.Status.Restarts) != len(before.Status.Restarts):
			r.t.Fatalf("step (f): a restart was recorded after the controller restart: %+v", now.Status.Restarts)
		case now.Status.Agent == nil || now.Status.Agent.AgentInstanceID != before.Status.Agent.AgentInstanceID:
			r.t.Fatalf("step (f): the agent registration changed: %+v, was %+v", now.Status.Agent, before.Status.Agent)
		}
		if j, _ := r.cl.plat.Job(jobID); j.PID != job.PID || j.State != "RUNNING" {
			r.t.Fatalf("step (f): job %d is %s with PID %d, was RUNNING with %d", jobID, j.State, j.PID, job.PID)
		}
		for name := range online {
			if !r.frpsProxies()[name] {
				r.t.Fatalf("step (f): frps's proxy %s went offline across the controller restart", name)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	if n := len(r.allocations()); n != allocs {
		r.t.Errorf("step (f): %d Allocations after the controller restart, %d before", n, allocs)
	}
	return r.checkRestartStream(in, done, killAt, ledAt, tokens)
}

// restartTokens is step (f)'s stream: 2,250 tokens at tokenDelay, about 45 s,
// longer than a new controller's takeover of the Lease.
const restartTokens = 2250

// checkRestartStream: the stream was in flight at the controller's kill and
// after the new controller led the Lease, and ended uncut with [DONE].
func (r *rig) checkRestartStream(in *v1alpha1.Instance, done <-chan streamed, killAt, ledAt time.Time, tokens int) (streamed, usageRow) {
	r.t.Helper()
	var s streamed
	select {
	case s = <-done:
	case <-time.After(requestTimeout):
		r.t.Fatalf("step (f): the stream did not end within %s", requestTimeout)
	}
	row := r.checkStream("f", s, in, tokens)
	rs, re := parseRecordTime(row.StartTime), parseRecordTime(row.EndTime)
	if !rs.Before(killAt) || !re.After(ledAt) {
		r.t.Errorf("step (f): the stream ran %s to %s, not across the kill %s and the new leader %s", row.StartTime, row.EndTime,
			killAt.UTC().Format(time.RFC3339Nano), ledAt.UTC().Format(time.RFC3339Nano))
	}
	return s, row
}

// frpsProxies lists the tcpmux proxies frps reports online, by name, from its
// web API (base+11, user hpcburst, the password in <state>/frps/web-password).
func (r *rig) frpsProxies() map[string]bool {
	r.t.Helper()
	pw, err := os.ReadFile(filepath.Join(r.h.state, "frps", "web-password"))
	if err != nil {
		r.t.Fatalf("frps's web password: %v", err)
	}
	req, _ := http.NewRequest(http.MethodGet, r.ports.url(11, "/api/proxy/tcpmux"), nil)
	req.SetBasicAuth("hpcburst", strings.TrimSpace(string(pw)))
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		r.t.Fatalf("frps's proxy list: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		r.t.Fatalf("frps's proxy list: %d %.200s", resp.StatusCode, body)
	}
	var list struct {
		Proxies []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		} `json:"proxies"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		r.t.Fatalf("frps's proxy list: %v: %.200s", err, body)
	}
	out := map[string]bool{}
	for _, p := range list.Proxies {
		if p.Status == "online" {
			out[p.Name] = true
		}
	}
	return out
}
