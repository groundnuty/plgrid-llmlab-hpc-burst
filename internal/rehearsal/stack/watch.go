package stack

// The watch: every version of a namespace's Instances, Allocations and
// ModelDeployments the API sends, with the time it arrived and the version's
// object. A hook on each Allocation version lets the alpha rows write a job tag
// down as soon as its Allocation exists.

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
)

// AnnotationDrainStarted is the time the lifecycle controller recorded for an
// Instance's drain (internal/lifecycle/instances.go).
const AnnotationDrainStarted = v1alpha1.Group + "/drain-started"

// Observation is one Instance version as the watch delivered it, with the
// time this process received it.
type Observation struct {
	At          time.Time
	Name        string
	UID         types.UID
	Alloc       string // spec.allocationRef.name
	Phase       v1alpha1.InstancePhase
	EndReason   string
	DrainBegan  string                 // the drain-started annotation (AnnotationDrainStarted)
	Tunnel      metav1.ConditionStatus // "" when the condition is absent
	Verified    bool                   // Verified True
	Qualified   bool                   // Qualified True
	WarmedUp    bool                   // WarmedUp True
	ReadyTime   bool                   // status.readyTime is set
	Unreachable bool                   // AgentUnreachable True
	Restarts    int
	Deleted     bool // the watch's DELETED event: the object's last version
	// Instance is the version itself.
	Instance *v1alpha1.Instance
}

// AllocObservation is one Allocation version as the watch delivered it.
type AllocObservation struct {
	At         time.Time
	Name       string
	UID        types.UID
	Phase      v1alpha1.AllocationPhase
	Deleted    bool
	Allocation *v1alpha1.Allocation
}

// DeploymentObservation is one ModelDeployment version as the watch delivered it.
type DeploymentObservation struct {
	At         time.Time
	Deployment *v1alpha1.ModelDeployment
}

// Recorder holds what the watch delivered.
type Recorder struct {
	mu     sync.Mutex
	obs    []Observation
	allocs []AllocObservation
	mds    []DeploymentObservation
	// lost holds the objects a catch-up found an event lost on, each as it was
	// reported: later catch-ups do not wait for them or report them again, and
	// say they are lost (catchup.go).
	lost map[types.UID]Behind
	// streams holds how far each kind's stream has come (catchup.go).
	streams map[Kinds]*streamState
	// resetsDecideNothing: see ResetsDecideNothing.
	resetsDecideNothing bool
	// afterLook is a test's: it runs right after a catch-up looks at the
	// Recorder, so that the test orders the stream and the look. nil otherwise.
	afterLook func()
}

// StartWatch records every version of the namespace's Instances, Allocations
// and ModelDeployments until the test ends, those that exist already as well. A
// connection that ends is opened again from the last resourceVersion its stream
// reached, so no version in between is lost; only when that version has expired
// does it start again from the current versions, and then what came between can
// be gone for good: the Recorder keeps that as a reset, a catch-up on the kind
// reports it as not known, and the test fails at its end. onAlloc, when not nil,
// is called with every Allocation version, in the watch's goroutine, before the
// version is recorded.
func StartWatch(t T, state, namespace string, onAlloc func(*v1alpha1.Allocation)) *Recorder {
	t.Helper()
	// No client timeout here: a watch is long, and its context ends it.
	wc, err := client.NewWithWatch(RestConfig(t, state), client.Options{Scheme: APIScheme(t)})
	if err != nil {
		t.Fatalf("a watching client: %v", err)
	}
	return startWatch(t, wc, namespace, onAlloc)
}

// startWatch is StartWatch with its client given.
func startWatch(t T, wc client.WithWatch, namespace string, onAlloc func(*v1alpha1.Allocation)) *Recorder {
	t.Helper()
	rec := &Recorder{}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		// Every read of the watch assumes an unbroken stream, those a catch-up
		// does not cover as well.
		why := rec.resets(KindInstances | KindAllocations | KindDeployments)
		switch {
		case why == "":
		case rec.decidesNothing():
			t.Logf("the watch was reset, which decided nothing this test ended on: %s", why)
		default:
			t.Errorf("what the test read from the watch is not known to be complete: %s", why)
		}
	})
	wg.Add(3)
	go func() {
		defer wg.Done()
		Follow(ctx, wc, namespace, &v1alpha1.InstanceList{}, rec.stream(KindInstances, func(t watch.EventType, o client.Object) {
			if in, ok := o.(*v1alpha1.Instance); ok {
				rec.add(in, t == watch.Deleted)
			}
		}))
	}()
	go func() {
		defer wg.Done()
		Follow(ctx, wc, namespace, &v1alpha1.AllocationList{}, rec.stream(KindAllocations, func(t watch.EventType, o client.Object) {
			if a, ok := o.(*v1alpha1.Allocation); ok {
				if onAlloc != nil {
					onAlloc(a)
				}
				rec.addAlloc(a, t == watch.Deleted)
			}
		}))
	}()
	go func() {
		defer wg.Done()
		Follow(ctx, wc, namespace, &v1alpha1.ModelDeploymentList{}, rec.stream(KindDeployments, func(_ watch.EventType, o client.Object) {
			if md, ok := o.(*v1alpha1.ModelDeployment); ok {
				rec.addDeployment(md)
			}
		}))
	}()
	return rec
}

func (rec *Recorder) add(in *v1alpha1.Instance, deleted bool) {
	c := in.Status.Conditions
	o := Observation{At: time.Now(), Name: in.Name, UID: in.UID, Alloc: in.Spec.AllocationRef.Name, Phase: in.Status.Phase,
		EndReason: in.Status.EndReason, Restarts: len(in.Status.Restarts), DrainBegan: in.Annotations[AnnotationDrainStarted],
		Verified:    meta.IsStatusConditionTrue(c, v1alpha1.ConditionVerified),
		Qualified:   meta.IsStatusConditionTrue(c, v1alpha1.ConditionQualified),
		WarmedUp:    meta.IsStatusConditionTrue(c, v1alpha1.ConditionWarmedUp),
		ReadyTime:   in.Status.ReadyTime != nil,
		Unreachable: meta.IsStatusConditionTrue(c, v1alpha1.ConditionAgentUnreachable), Deleted: deleted,
		Instance: in.DeepCopy()}
	if tc := meta.FindStatusCondition(c, v1alpha1.ConditionTunnelConnected); tc != nil {
		o.Tunnel = tc.Status
	}
	rec.mu.Lock()
	rec.obs = append(rec.obs, o)
	rec.mu.Unlock()
}

func (rec *Recorder) addAlloc(a *v1alpha1.Allocation, deleted bool) {
	rec.mu.Lock()
	rec.allocs = append(rec.allocs, AllocObservation{At: time.Now(), Name: a.Name, UID: a.UID, Phase: a.Status.AtProvider.Phase, Deleted: deleted, Allocation: a.DeepCopy()})
	rec.mu.Unlock()
}

func (rec *Recorder) addDeployment(md *v1alpha1.ModelDeployment) {
	o := DeploymentObservation{At: time.Now(), Deployment: md.DeepCopy()}
	rec.mu.Lock()
	rec.mds = append(rec.mds, o)
	rec.mu.Unlock()
}

// First returns the earliest observation of the Instance received at or after
// since that matches, and whether there is one.
func (rec *Recorder) First(uid types.UID, since time.Time, match func(Observation) bool) (Observation, bool) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.noteRead(KindInstances, "First", since, time.Time{})
	for _, o := range rec.obs {
		if o.UID == uid && !o.At.Before(since) && match(o) {
			return o, true
		}
	}
	return Observation{}, false
}

// Of returns every observation of the Instance, in the order received.
func (rec *Recorder) Of(uid types.UID) []Observation {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.noteRead(KindInstances, "Of", time.Time{}, time.Time{})
	var out []Observation
	for _, o := range rec.obs {
		if o.UID == uid {
			out = append(out, o)
		}
	}
	return out
}

// Last returns the last version of each Instance the watch saw, in the order
// first seen.
func (rec *Recorder) Last() []v1alpha1.Instance {
	var out []v1alpha1.Instance
	for _, uid := range rec.UIDs() {
		obs := rec.Of(uid)
		out = append(out, *obs[len(obs)-1].Instance)
	}
	return out
}

// OnAllocation returns the UID of the first Instance seen placed on the
// Allocation, and whether there is one.
func (rec *Recorder) OnAllocation(alloc string) (types.UID, bool) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.noteRead(KindInstances, "OnAllocation", time.Time{}, time.Time{})
	for _, o := range rec.obs {
		if o.Alloc == alloc {
			return o.UID, true
		}
	}
	return "", false
}

// UIDs returns every Instance UID the watch saw, in the order first seen;
// none for no watch.
func (rec *Recorder) UIDs() []types.UID {
	if rec == nil {
		return nil
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.noteRead(KindInstances, "UIDs", time.Time{}, time.Time{})
	seen := map[types.UID]bool{}
	var out []types.UID
	for _, o := range rec.obs {
		if !seen[o.UID] {
			seen[o.UID] = true
			out = append(out, o.UID)
		}
	}
	return out
}

// Allocations returns every Allocation observation, in the order received.
func (rec *Recorder) Allocations() []AllocObservation {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.noteRead(KindAllocations, "Allocations", time.Time{}, time.Time{})
	return append([]AllocObservation(nil), rec.allocs...)
}

// Deployments returns every ModelDeployment version, in the order received.
func (rec *Recorder) Deployments() []*v1alpha1.ModelDeployment {
	var out []*v1alpha1.ModelDeployment
	for _, o := range rec.DeploymentVersions() {
		out = append(out, o.Deployment)
	}
	return out
}

// DeploymentVersions is Deployments with the time each version arrived.
func (rec *Recorder) DeploymentVersions() []DeploymentObservation {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.noteRead(KindDeployments, "DeploymentVersions", time.Time{}, time.Time{})
	return append([]DeploymentObservation(nil), rec.mds...)
}

// Between returns the Instance's observations received in [from, to].
func (rec *Recorder) Between(uid types.UID, from, to time.Time) []Observation {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.noteRead(KindInstances, "Between", from, to)
	var out []Observation
	for _, o := range rec.obs {
		if o.UID == uid && !o.At.Before(from) && !o.At.After(to) {
			out = append(out, o)
		}
	}
	return out
}
