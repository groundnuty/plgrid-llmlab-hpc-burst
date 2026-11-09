package scenario_test

// A recorder of every Instance and Allocation version the API server sends on
// a watch. The steps ask it whether a state was ever observed and in which
// order (step (a)'s first entry to ready, step (g)'s drain), which a poll could
// miss when the state lasts a second.

import (
	"context"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// observation is one Instance version as the watch delivered it, with the
// time this test received it.
type observation struct {
	at          time.Time
	name        string
	uid         types.UID
	created     time.Time // metadata.creationTimestamp, cut to the second
	alloc       string    // spec.allocationRef.name
	phase       v1alpha1.InstancePhase
	endReason   string
	drainBegan  string                 // the drain-started annotation (annotationDrainStarted)
	tunnel      metav1.ConditionStatus // "" when the condition is absent
	verified    bool                   // Verified True
	qualified   bool                   // Qualified True
	warmedUp    bool                   // WarmedUp True
	readyTime   bool                   // status.readyTime is set
	unreachable bool                   // AgentUnreachable True
	restarts    int
	deleted     bool // the watch's DELETED event: the object's last version
}

// allocObservation is one Allocation version as the watch delivered it.
type allocObservation struct {
	at      time.Time
	name    string
	uid     types.UID
	phase   v1alpha1.AllocationPhase
	deleted bool
	// deletion is the version's deletionTimestamp, zero for none: when the
	// API server took the delete request, cut to the second.
	deletion time.Time
}

// mdObservation is one ModelDeployment version as the watch delivered it:
// its Degraded and Ready conditions, "" when absent.
type mdObservation struct {
	at              time.Time
	name            string
	degraded        metav1.ConditionStatus
	degradedReason  string
	degradedMessage string
	ready           metav1.ConditionStatus
	readyReason     string
}

type recorder struct {
	mu     sync.Mutex
	obs    []observation
	allocs []allocObservation
	mds    []mdObservation
	// firstSpec is the spec of the first Instance the watch delivered: the
	// tunnel witness's (orphans_test.go).
	firstSpec *v1alpha1.InstanceSpec
}

// startWatch records every version of the namespace's Instances and
// Allocations until the test ends. A watch the server closes is opened again
// from the last resourceVersion it delivered, so no version in between is
// lost; only when that version has expired does it start again from the
// current versions.
func (r *rig) startWatch() {
	r.t.Helper()
	// No client timeout here: a watch is long, and its context ends it.
	wc, err := client.NewWithWatch(stack.RestConfig(r.t, r.h.State), client.Options{Scheme: stack.APIScheme(r.t)})
	if err != nil {
		r.t.Fatalf("a watching client: %v", err)
	}
	rec := &recorder{}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	r.t.Cleanup(func() { cancel(); wg.Wait() })
	wg.Add(3)
	go func() {
		defer wg.Done()
		follow(ctx, wc, &v1alpha1.InstanceList{}, func(t watch.EventType, o client.Object) {
			if in, ok := o.(*v1alpha1.Instance); ok {
				rec.add(in, t == watch.Deleted)
			}
		})
	}()
	go func() {
		defer wg.Done()
		follow(ctx, wc, &v1alpha1.AllocationList{}, func(t watch.EventType, o client.Object) {
			if a, ok := o.(*v1alpha1.Allocation); ok {
				rec.addAlloc(a, t == watch.Deleted)
			}
		})
	}()
	go func() {
		defer wg.Done()
		follow(ctx, wc, &v1alpha1.ModelDeploymentList{}, func(_ watch.EventType, o client.Object) {
			if md, ok := o.(*v1alpha1.ModelDeployment); ok {
				rec.addMD(md)
			}
		})
	}()
	r.watch = rec
}

// follow watches list in the rehearsal's namespace until ctx ends, passing on
// every added, modified and deleted object.
func follow(ctx context.Context, wc client.WithWatch, list client.ObjectList, add func(watch.EventType, client.Object)) {
	rv := ""
	for ctx.Err() == nil {
		opts := []client.ListOption{client.InNamespace(stack.Namespace)}
		if rv != "" {
			opts = append(opts, &client.ListOptions{Raw: &metav1.ListOptions{ResourceVersion: rv, AllowWatchBookmarks: true}})
		}
		w, err := wc.Watch(ctx, list, opts...)
		if err != nil {
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
			continue
		}
		for ev := range w.ResultChan() {
			switch ev.Type {
			case watch.Error:
				rv = "" // the version expired (410): start from the current versions
			case watch.Bookmark:
				if o, ok := ev.Object.(client.Object); ok {
					rv = o.GetResourceVersion()
				}
			default:
				if o, ok := ev.Object.(client.Object); ok {
					rv = o.GetResourceVersion()
					add(ev.Type, o)
				}
			}
		}
		w.Stop()
	}
}

func (rec *recorder) add(in *v1alpha1.Instance, deleted bool) {
	c := in.Status.Conditions
	o := observation{at: time.Now(), name: in.Name, uid: in.UID, created: in.CreationTimestamp.Time, alloc: in.Spec.AllocationRef.Name, phase: in.Status.Phase,
		endReason: in.Status.EndReason, restarts: len(in.Status.Restarts), drainBegan: in.Annotations[annotationDrainStarted],
		verified:    meta.IsStatusConditionTrue(c, v1alpha1.ConditionVerified),
		qualified:   meta.IsStatusConditionTrue(c, v1alpha1.ConditionQualified),
		warmedUp:    meta.IsStatusConditionTrue(c, v1alpha1.ConditionWarmedUp),
		readyTime:   in.Status.ReadyTime != nil,
		unreachable: meta.IsStatusConditionTrue(c, v1alpha1.ConditionAgentUnreachable), deleted: deleted}
	if tc := meta.FindStatusCondition(c, v1alpha1.ConditionTunnelConnected); tc != nil {
		o.tunnel = tc.Status
	}
	rec.mu.Lock()
	rec.obs = append(rec.obs, o)
	if rec.firstSpec == nil {
		rec.firstSpec = in.Spec.DeepCopy()
	}
	rec.mu.Unlock()
}

func (rec *recorder) addMD(md *v1alpha1.ModelDeployment) {
	o := mdObservation{at: time.Now(), name: md.Name}
	if c := meta.FindStatusCondition(md.Status.Conditions, v1alpha1.ConditionDegraded); c != nil {
		o.degraded, o.degradedReason, o.degradedMessage = c.Status, c.Reason, c.Message
	}
	if c := meta.FindStatusCondition(md.Status.Conditions, v1alpha1.ConditionReady); c != nil {
		o.ready, o.readyReason = c.Status, c.Reason
	}
	rec.mu.Lock()
	rec.mds = append(rec.mds, o)
	rec.mu.Unlock()
}

// deployment returns every observation of the named ModelDeployment, in the
// order received.
func (rec *recorder) deployment(name string) []mdObservation {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var out []mdObservation
	for _, o := range rec.mds {
		if o.name == name {
			out = append(out, o)
		}
	}
	return out
}

func (rec *recorder) addAlloc(a *v1alpha1.Allocation, deleted bool) {
	o := allocObservation{at: time.Now(), name: a.Name, uid: a.UID, phase: a.Status.AtProvider.Phase, deleted: deleted}
	if a.DeletionTimestamp != nil {
		o.deletion = a.DeletionTimestamp.Time
	}
	rec.mu.Lock()
	rec.allocs = append(rec.allocs, o)
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

// createdAt returns the Instance's creationTimestamp from the watch, and
// whether the watch saw the Instance with one.
func (rec *recorder) createdAt(uid types.UID) (time.Time, bool) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, o := range rec.obs {
		if o.uid == uid && !o.created.IsZero() {
			return o.created, true
		}
	}
	return time.Time{}, false
}

// of returns every observation of the Instance, in the order received.
func (rec *recorder) of(uid types.UID) []observation {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var out []observation
	for _, o := range rec.obs {
		if o.uid == uid {
			out = append(out, o)
		}
	}
	return out
}

// onAllocation returns the UID of the first Instance seen placed on the
// Allocation, and whether there is one.
func (rec *recorder) onAllocation(alloc string) (types.UID, bool) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, o := range rec.obs {
		if o.alloc == alloc {
			return o.uid, true
		}
	}
	return "", false
}

// uids returns every Instance UID the watch saw, in the order first seen;
// none for a rig without a watch.
func (rec *recorder) uids() []types.UID {
	if rec == nil {
		return nil
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	seen := map[types.UID]bool{}
	var out []types.UID
	for _, o := range rec.obs {
		if !seen[o.uid] {
			seen[o.uid] = true
			out = append(out, o.uid)
		}
	}
	return out
}

// allocations returns every Allocation observation, in the order received.
func (rec *recorder) allocations() []allocObservation {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]allocObservation(nil), rec.allocs...)
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

// proxyEnded waits until frps no longer lists the Instance's proxy online and
// records when that was first seen: a bound the proxy's end lies before.
func (r *rig) proxyEnded(uid types.UID) {
	r.t.Helper()
	stack.WaitFor(r.t, stack.WaitDeadline, "frps to list the proxy "+string(uid)+" offline", func() bool {
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
