package scenario_test

// Steps (f) and (g) of issue #146's scenario: a controller restart while the
// Instance serves, and the scale to 0 with hpcburst down. Each is asserted
// from the API, the fake cluster's job snapshots, frps's proxy list and the
// launcher's own status, never from log lines.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// statusPID is the PID that hpcburst status reports for a supervised entry,
// 0 when it reports none running.
func (r *rig) statusPID(name string) int {
	r.t.Helper()
	out, err := run(r.t, r.h.env, r.b.hpcburst, "status", "--state-dir", r.h.state, "--json", "--no-api")
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
	if err := r.c.Get(context.Background(), types.NamespacedName{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle}, &l); err != nil || l.Spec.HolderIdentity == nil {
		return ""
	}
	return *l.Spec.HolderIdentity
}

func (r *rig) instance(name string) (v1alpha1.Instance, bool) {
	var in v1alpha1.Instance
	ok := r.get(&in, name, namespace)
	return in, ok
}

// stepControllerRestart is step (f): the controller is killed while the
// Instance serves, and the supervisor starts it again. The new controller
// leads the Lease, and for one agentTimeout after that (the profile's, scaled
// to rehearsalAgentTimeout) the agent keeps serving: the Instance stays ready
// with the same agentInstanceID and no new restart, its job keeps its process,
// and no Allocation is added. That is HBEP-7's "the controller restarts while
// agents serve". The tunnel half, frps's proxy staying online and no stream
// cut (HBEP-6), needs the final tunnel PR: frpsProxies checks it once there
// are proxies at all.
func (r *rig) stepControllerRestart(in *v1alpha1.Instance) {
	r.t.Helper()
	before, ok := r.instance(in.Name)
	if !ok || before.Status.Phase != v1alpha1.InstanceReady || before.Status.Agent == nil {
		r.t.Fatalf("step (f) needs a ready, registered Instance %s: %+v", in.Name, before.Status)
	}
	jobID, _ := strconv.Atoi(before.Status.JobID)
	job, _ := r.cl.plat.Job(jobID)
	allocs := len(r.allocations())
	online := r.frpsProxies()
	oldPID, oldHolder := r.statusPID("controller"), r.leaseHolder()
	if oldPID == 0 || oldHolder == "" {
		r.t.Fatalf("step (f): the controller's PID %d or Lease holder %q is unknown", oldPID, oldHolder)
	}
	if err := syscall.Kill(oldPID, syscall.SIGKILL); err != nil {
		r.t.Fatalf("kill the controller %d: %v", oldPID, err)
	}
	waitFor(r.t, waitDeadline, "the supervisor to start a new controller that leads the Lease", func() bool {
		p := r.statusPID("controller")
		h := r.leaseHolder()
		return p != 0 && p != oldPID && h != "" && h != oldHolder &&
			httpStatus(fmt.Sprintf("http://127.0.0.1:%d/readyz", portBase+6)) == http.StatusOK
	})
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
}

// frpsProxies lists the tcpmux proxies frps reports online, by name, from its
// web API (base+11, user hpcburst, the password in <state>/frps/web-password).
func (r *rig) frpsProxies() map[string]bool {
	r.t.Helper()
	pw, err := os.ReadFile(filepath.Join(r.h.state, "frps", "web-password"))
	if err != nil {
		r.t.Fatalf("frps's web password: %v", err)
	}
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/api/proxy/tcpmux", portBase+11), nil)
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

// stepScaleToZero is step (g): scale to 0. The Instances drain and end, the
// Allocations end, and each has its acct-<UID> record; every job's processes
// are gone, frps lists no online proxy for an ended Instance, and hpcburst
// down stops everything within its grace.
func (r *rig) stepScaleToZero() {
	r.t.Helper()
	uids := map[types.UID]string{}
	for _, a := range r.allocations() {
		uids[a.UID] = a.Name
	}
	var insts []string
	for _, in := range r.instances() {
		insts = append(insts, string(in.UID))
	}
	var md v1alpha1.ModelDeployment
	if !r.get(&md, mdName, namespace) {
		r.t.Fatal("step (g): no ModelDeployment")
	}
	patch := client.MergeFrom(md.DeepCopy())
	md.Spec.Instances = 0
	if err := r.c.Patch(context.Background(), &md, patch); err != nil {
		r.t.Fatalf("scale to 0: %v", err)
	}
	last := time.Now()
	waitFor(r.t, waitDeadline, "every Instance to end and every Allocation to go", func() bool {
		if time.Since(last) > 10*time.Second {
			r.t.Logf("step (g) waiting:%s", r.describe())
			last = time.Now()
		}
		for _, in := range r.instances() {
			if in.Status.Phase != v1alpha1.InstanceEnded {
				return false
			}
		}
		for _, a := range r.allocations() {
			if a.Status.AtProvider.Phase != v1alpha1.AllocationEnded && a.Status.AtProvider.Phase != v1alpha1.AllocationFailed {
				return false
			}
		}
		return true
	})
	waitFor(r.t, waitDeadline, "an acct- record for every Allocation", func() bool {
		var l appsv1.ControllerRevisionList
		if err := r.c.List(context.Background(), &l); err != nil {
			return false
		}
		have := map[string]bool{}
		for _, cr := range l.Items {
			have[cr.Name] = true
		}
		for uid := range uids {
			name, err := v1alpha1.AccountingRecordName(uid)
			if err != nil || !have[name] {
				return false
			}
		}
		return true
	})
	for _, j := range r.cl.plat.Jobs() {
		jj := j
		waitFor(r.t, waitDeadline, fmt.Sprintf("job %d's processes to be gone", jj.ID), func() bool {
			s, _ := r.cl.plat.Job(jj.ID)
			return s.Finished || s.PID == 0 && s.State != "PENDING" && s.State != "RUNNING"
		})
	}
	online := r.frpsProxies()
	for _, uid := range insts {
		if online[uid] {
			r.t.Errorf("step (g): frps still lists the ended Instance %s online", uid)
		}
	}
	start := time.Now()
	r.h.down()
	if d := time.Since(start); d > 40*time.Second {
		r.t.Errorf("step (g): hpcburst down took %s, above its default grace of 40 s", d)
	}
	if p := r.statusPID("controller"); p != 0 {
		r.t.Errorf("step (g): status still reports a controller (pid %d) after down", p)
	}
}
