package lifecycle

// Helpers of the rows of the extra's checks and its admission headroom (extra_retry_rig_test.go, headroom_rig_test.go;
// #616 PR 4): a pass that tolerates a cut-short Allocation create, and the fake clock's step.

import (
	"context"
	"errors"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

func tick10(rg *rig) {
	rg.t.Helper()
	rg.clock.Advance(10 * time.Second)
	rg.Step()
}

// extraMD is a deployment with k = 1 and an end time, 14:00, before its primary's expected drain start (08:00:10 + 12 h
// - 2 h 10 min = 17:50:10 and later): it holds no slot for a replacement, so an extra's slot is the one beyond what is
// listed and expected (1 + 0 + 1), and four Allocations fill the limit of 4.
func extraMD(name string) *v1alpha1.ModelDeployment {
	md := mdSpec(name)
	md.Spec.OverAllocation = ptrTo(int32(1))
	end := metav1.NewTime(hoT(14, 0, 0))
	md.Spec.EndTime = &end
	return md
}

func deploymentOfInstance(in v1alpha1.Instance) string {
	return in.Labels[v1alpha1.LabelModelDeployment]
}

func pending10h(n int) []pfake.Script {
	out := make([]pfake.Script, n)
	for i := range out {
		out[i] = pfake.Script{QueueWait: 10 * time.Hour}
	}
	return out
}

// instOrNil is the Instance of the store by name, whatever its deployment, or nil once it is gone.
func instOrNil(rg *rig, name string) *v1alpha1.Instance {
	rg.t.Helper()
	var in v1alpha1.Instance
	switch err := rg.c.Get(rg.ctx, types.NamespacedName{Namespace: rigNS, Name: name}, &in); {
	case apierrors.IsNotFound(err):
		return nil
	case err != nil:
		rg.t.Fatal(err)
	}
	return &in
}

// allocCutter cuts an Allocation's creation short, as a pass killed between the Instance's create and the
// Allocation's is: while armed for a deployment, its Allocation creates from the skip+1-th on fail with errHOKilled.
type allocCutter struct {
	mu     sync.Mutex
	md     string
	skip   int
	seen   int
	failed int
}

func (c *allocCutter) arm(md string, skip int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.md, c.skip, c.seen = md, skip, 0
}

func (c *allocCutter) disarm() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.md = ""
}

func (c *allocCutter) failures() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failed
}

// funcs is base with a Create that fails the armed deployment's Allocation creates.
func (c *allocCutter) funcs(base interceptor.Funcs) interceptor.Funcs {
	base.Create = func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if a, ok := obj.(*v1alpha1.Allocation); ok {
			c.mu.Lock()
			cut := false
			if c.md != "" && a.Labels[v1alpha1.LabelModelDeployment] == c.md {
				c.seen++
				if c.seen > c.skip {
					c.failed++
					cut = true
				}
			}
			c.mu.Unlock()
			if cut {
				return errHOKilled
			}
		}
		return cl.Create(ctx, obj, opts...)
	}
	return base
}

// stepTolerantAll is rg.Step that tolerates the cut-short creates' errors: every deployment but those named in skip is
// reconciled in name order (skip: a deployment whose pass the processing order delays), and an error is fatal unless it
// is errHOKilled.
func stepTolerantAll(rg *rig, skip ...string) {
	rg.t.Helper()
	if err := rg.prov.Sync(rg.ctx); err != nil {
		rg.t.Fatalf("provider sync: %v", err)
	}
	for _, x := range rg.extra {
		if err := x.prov.Sync(rg.ctx); err != nil {
			rg.t.Fatalf("provider %s sync: %v", x.name, err)
		}
	}
	rg.syncAgents()
	var mds v1alpha1.ModelDeploymentList
	if err := rg.c.List(rg.ctx, &mds); err != nil {
		rg.t.Fatal(err)
	}
	names := make([]string, 0, len(mds.Items))
	for _, md := range mds.Items {
		names = append(names, md.Name)
	}
	sort.Strings(names)
	for _, n := range names {
		if slices.Contains(skip, n) {
			continue
		}
		_, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: n}})
		if err != nil && !errors.Is(err, errHOKilled) {
			rg.t.Fatalf("reconcile %s at %s: %v", n, rg.clock.Now().Format(time.RFC3339), err)
		}
	}
}

// cancelsOf is how many Allocations the controller has deleted.
func cancelsOf(log *causeLog) int {
	log.mu.Lock()
	defer log.mu.Unlock()
	return len(log.cause)
}

// syncProviders is the first half of rg.Step: the Providers' passes and the agents.
func syncProviders(rg *rig) {
	rg.t.Helper()
	if err := rg.prov.Sync(rg.ctx); err != nil {
		rg.t.Fatalf("provider sync: %v", err)
	}
	for _, x := range rg.extra {
		if err := x.prov.Sync(rg.ctx); err != nil {
			rg.t.Fatalf("provider %s sync: %v", x.name, err)
		}
	}
	rg.syncAgents()
}

// TestShortageIsCountedByAllocationPresence (Y1): shortageOf is the desired count less the units, one per hedge, that have
// an Allocation. An Instance with no Allocation adds nothing, whichever the selection chooses.
func TestShortageIsCountedByAllocationPresence(t *testing.T) {
	inst := func(name, uid string) *v1alpha1.Instance {
		return &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(uid), CreationTimestamp: metav1.NewTime(hoT(8, 0, 0))}}
	}
	alloc := &v1alpha1.Allocation{}
	p, x := inst("p", "bbbbbbbb"), inst("x", "aaaaaaaa") // x has the lower UID and wins the selection's tie
	for _, c := range []struct {
		name    string
		insts   []*v1alpha1.Instance
		has     map[string]bool
		desired int
		want    int
	}{
		{"nothing made", nil, nil, 1, 1},
		{"a cut-short primary alone: missing", []*v1alpha1.Instance{p}, nil, 1, 1},
		{"the primary has its Allocation, the extra with the lower UID is cut short: none missing", []*v1alpha1.Instance{p, x}, map[string]bool{"p": true}, 1, 0},
		{"the extra with the lower UID has its Allocation, the primary is cut short: none missing", []*v1alpha1.Instance{p, x}, map[string]bool{"x": true}, 1, 0},
		{"two wanted, one with its Allocation, one cut short: one missing", []*v1alpha1.Instance{p, x}, map[string]bool{"p": true}, 2, 1},
		{"both have theirs and two wanted: none", []*v1alpha1.Instance{p, x}, map[string]bool{"p": true, "x": true}, 2, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			sel := selection{insts: c.insts, allocOf: func(in *v1alpha1.Instance) *v1alpha1.Allocation {
				if c.has[in.Name] {
					return alloc
				}
				return nil
			}}
			if got := shortageOf(sel, c.desired); got != c.want {
				t.Fatalf("shortage = %d, want %d", got, c.want)
			}
		})
	}
}
