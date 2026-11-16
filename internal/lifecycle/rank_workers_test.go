package lifecycle

// The ranking point of an Instance of several workers (HBEP-7, Several workers; Over-allocation and qualification
// (beta), The ranking point; #616, PR 3): the hook sits in startingWorkers as it does in starting, after the tunnel's
// and the router's gates, so a surplus Instance of two workers is ranked and not made ready as today. The measurement
// is the Instance's status.throughput, the mean of its workers' means; both workers measure the same here, so it is
// 20 ms, 18.0 ms or 17.9 ms exactly. The rows wait on state, not on a second: the worker path's timeline is the
// package's own (workers_start_test.go).

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

func TestASurplusInstanceOfTwoWorkersIsRankedLikeAnyOther(t *testing.T) {
	for _, c := range []struct {
		name     string
		x        time.Duration
		outranks bool
	}{{"18.0 ms against 20 ms", rk180, false}, {"17.9 ms against 20 ms", rk179, true}} {
		t.Run(c.name, func(t *testing.T) {
			h := newRkRig(t, false)
			h.r.Config.InstanceRouter = v1alpha1.InstanceRouterEnvoy
			h.defaultDelay = rkNoDelay
			p, x := h.start(func(md *v1alpha1.ModelDeployment) { withWorkers(md, 2) },
				pfake.Script{QueueWait: 5 * time.Minute}, pfake.Script{QueueWait: 10 * time.Minute})
			h.engineOf(p, rk200, rkNoDelay)
			h.engineOf(x, c.x, rkNoDelay)
			if !several(h.mustInstance(x)) {
				t.Fatal("X is not an Instance of several workers: the row would exercise the wrong path")
			}
			h.until(30*time.Minute, 10*time.Second, "P ready", func() bool { return h.mustInstance(p).Status.Phase == v1alpha1.InstanceReady })
			if h.mustInstance(x).Status.ReadyTime != nil {
				t.Fatal("X was ready before P: it is surplus and waits")
			}
			if !c.outranks {
				h.until(30*time.Minute, 10*time.Second, "X ranked out", func() bool { return ended(h.mustInstance(x)) })
				xi := h.mustInstance(x)
				if xi.Status.EndReason != v1alpha1.ReasonRankedOut || xi.Status.ReadyTime != nil || !h.kept(x) {
					t.Fatalf("X ended for %q, ready %v, Allocation kept %v: want RankedOut, never ready, kept", xi.Status.EndReason, xi.Status.ReadyTime, h.kept(x))
				}
				return
			}
			h.until(30*time.Minute, 10*time.Second, "X ready", func() bool { return h.mustInstance(x).Status.Phase == v1alpha1.InstanceReady })
			if got := h.mustInstance(p).Annotations[annotationRankedOut]; got != x {
				t.Fatalf("P's mark %q, want %q", got, x)
			}
			h.until(10*time.Minute, 10*time.Second, "P ranked out", func() bool { return ended(h.mustInstance(p)) })
			if pi := h.mustInstance(p); pi.Status.EndReason != v1alpha1.ReasonRankedOut || !h.kept(p) {
				t.Fatalf("P ended for %q, Allocation kept %v: want RankedOut, kept", pi.Status.EndReason, h.kept(p))
			}
		})
	}
}
