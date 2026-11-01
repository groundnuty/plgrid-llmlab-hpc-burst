package scenario_test

// A recorder of every Instance version the API server sends on a watch. Steps
// (c), (d) and (e) ask it whether a state was ever observed (TunnelConnected
// False, a phase), which a poll could miss when the state lasts a second.

import (
	"context"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// observation is one Instance version as the watch delivered it, with the
// time this test received it.
type observation struct {
	at          time.Time
	name        string
	uid         types.UID
	phase       v1alpha1.InstancePhase
	endReason   string
	drainBegan  string                 // the drain-started annotation (annotationDrainStarted)
	tunnel      metav1.ConditionStatus // "" when the condition is absent
	unreachable bool                   // AgentUnreachable True
	restarts    int
}

type recorder struct {
	mu  sync.Mutex
	obs []observation
}

// startWatch records every version of the namespace's Instances until the
// test ends. A watch the server closes is opened again; its first events
// repeat the current versions, which are observations too.
func (r *rig) startWatch() {
	r.t.Helper()
	wc, err := client.NewWithWatch(restConfig(r.t, r.h.state), client.Options{Scheme: apiScheme(r.t)})
	if err != nil {
		r.t.Fatalf("a watching client: %v", err)
	}
	rec := &recorder{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	r.t.Cleanup(func() { cancel(); <-done })
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			w, err := wc.Watch(ctx, &v1alpha1.InstanceList{}, client.InNamespace(namespace))
			if err != nil {
				select {
				case <-ctx.Done():
				case <-time.After(time.Second):
				}
				continue
			}
			for ev := range w.ResultChan() {
				if in, ok := ev.Object.(*v1alpha1.Instance); ok {
					rec.add(in)
				}
			}
			w.Stop()
		}
	}()
	r.watch = rec
}

func (rec *recorder) add(in *v1alpha1.Instance) {
	o := observation{at: time.Now(), name: in.Name, uid: in.UID, phase: in.Status.Phase, endReason: in.Status.EndReason, restarts: len(in.Status.Restarts),
		drainBegan:  in.Annotations[annotationDrainStarted],
		unreachable: meta.IsStatusConditionTrue(in.Status.Conditions, v1alpha1.ConditionAgentUnreachable)}
	if c := meta.FindStatusCondition(in.Status.Conditions, v1alpha1.ConditionTunnelConnected); c != nil {
		o.tunnel = c.Status
	}
	rec.mu.Lock()
	rec.obs = append(rec.obs, o)
	rec.mu.Unlock()
}

// first returns the earliest observation of the Instance received at or after
// since that matches, and whether there is one.
func (rec *recorder) first(uid types.UID, since time.Time, match func(observation) bool) (observation, bool) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, o := range rec.obs {
		if o.uid == uid && !o.at.Before(since) && match(o) {
			return o, true
		}
	}
	return observation{}, false
}

// between returns the Instance's observations received in [from, to].
func (rec *recorder) between(uid types.UID, from, to time.Time) []observation {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var out []observation
	for _, o := range rec.obs {
		if o.uid == uid && !o.at.Before(from) && !o.at.After(to) {
			out = append(out, o)
		}
	}
	return out
}

// The controller raises TunnelOrphaned for a proxy only once every frps read
// for TunnelOrphanGrace has listed it online with no current Instance
// (@hpc-burst-science-agent, 1 Nov 2026, for #167); it reads frps's list
// every 10 s (HBEP-6, Path state and the inventory). A check that no such
// event fired proves something only that long after a proxy ended.
const (
	tunnelOrphanGrace = 2 * time.Minute
	tunnelReadPeriod  = 10 * time.Second
)

// proxyEnded waits until frps no longer lists the Instance's proxy online and
// records when that was first seen: a bound the proxy's end lies before.
func (r *rig) proxyEnded(uid types.UID) {
	r.t.Helper()
	waitFor(r.t, waitDeadline, "frps to list the proxy "+string(uid)+" offline", func() bool {
		return !r.frpsProxies()[string(uid)]
	})
	r.markProxyEnd(uid, time.Now())
}

func (r *rig) markProxyEnd(uid types.UID, at time.Time) {
	if r.proxyEnds == nil {
		r.proxyEnds = map[types.UID]time.Time{}
	}
	if _, ok := r.proxyEnds[uid]; !ok {
		r.proxyEnds[uid] = at
	}
}

// assertNoOrphans waits until TunnelOrphanGrace and one frps read have passed
// since the last proxy end recorded, then fails on any TunnelOrphaned event:
// a real teardown (steps c and g, real frps and frpc) must raise none. It
// logs, for each proxy, how long before the event read it ended.
func (r *rig) assertNoOrphans() {
	r.t.Helper()
	var last time.Time
	for _, end := range r.proxyEnds {
		if end.After(last) {
			last = end
		}
	}
	if last.IsZero() {
		r.t.Fatal("no proxy end was recorded: the TunnelOrphaned check would be vacuous")
	}
	until := last.Add(tunnelOrphanGrace + tunnelReadPeriod + 5*time.Second)
	waitFor(r.t, time.Until(until)+time.Minute, "TunnelOrphaned's window after the last proxy end", func() bool { return !time.Now().Before(until) })
	read := time.Now()
	for uid, end := range r.proxyEnds {
		r.t.Logf("TunnelOrphaned window: proxy %s ended by %s, %s before the event read", uid, end.UTC().Format(time.RFC3339), read.Sub(end).Round(time.Second))
	}
	if n := reportOrphanedTunnels(r.t, r.c, "c, g"); n > 0 {
		r.t.Errorf("%d TunnelOrphaned events across steps (c) and (g): a real teardown left a proxy online with no current Instance for %s", n, tunnelOrphanGrace)
	}
}
