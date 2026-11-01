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
	o := observation{at: time.Now(), name: in.Name, uid: in.UID, phase: in.Status.Phase, restarts: len(in.Status.Restarts),
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
