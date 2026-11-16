package lifecycle

// The release cause (HBEP-7, Release, The release cause; issue #609): before every delete of an Allocation, the
// controller writes the annotation release-cause on it, and deletes only once that write succeeded. Each case reads
// the annotation as the store held it at the moment of the delete, through an interceptor on the delete, never the
// text the code passed, so a delete made before the write, or with no write, shows an empty cause. Every expected
// cause is written out here by hand.

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// causeLog records, for each Allocation deleted, the release cause the store held when its delete was made.
type causeLog struct {
	mu    sync.Mutex
	cause map[string]string
	n     map[string]int
	// failWrites, while above 0, fails that many writes of a release cause.
	failWrites int
}

func (l *causeLog) funcs() interceptor.Funcs {
	return interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if a, ok := obj.(*v1alpha1.Allocation); ok {
				var stored v1alpha1.Allocation
				if err := cl.Get(ctx, client.ObjectKeyFromObject(a), &stored); err == nil {
					l.mu.Lock()
					l.cause[a.Name] = stored.Annotations[v1alpha1.AnnotationReleaseCause]
					l.n[a.Name]++
					l.mu.Unlock()
				}
			}
			return cl.Delete(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if a, ok := obj.(*v1alpha1.Allocation); ok && a.Annotations[v1alpha1.AnnotationReleaseCause] != "" {
				l.mu.Lock()
				fail := l.failWrites > 0
				if fail {
					l.failWrites--
				}
				l.mu.Unlock()
				if fail {
					return apierrors.NewServiceUnavailable("the release cause's write fails")
				}
			}
			return cl.Update(ctx, obj, opts...)
		},
	}
}

func newCauseLog() *causeLog { return &causeLog{cause: map[string]string{}, n: map[string]int{}} }

// deletedWith reports the cause the store held when the Allocation was deleted, and whether it was.
func (l *causeLog) deletedWith(name string) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	c, ok := l.cause[name]
	return c, ok
}

// TestEveryDeletePathWritesItsCause: each of the lifecycle controller's delete paths writes its cause before its
// delete: release on failure, scale-down, shutdown, and retention's releases of an empty Allocation.
func TestEveryDeletePathWritesItsCause(t *testing.T) {
	fail := 30 * time.Second
	for _, c := range []struct {
		name    string
		scripts []pfake.Script
		edit    func(*v1alpha1.ModelDeployment)
		// act brings the delete about; target names the Allocation it deletes, and want is its cause, given the
		// Instance on it (written out per case).
		act  func(t *testing.T, rg *rig) (target string, want string)
		wait time.Duration
	}{
		{"release on failure", []pfake.Script{{FailAfter: &fail}}, nil,
			func(t *testing.T, rg *rig) (string, string) {
				in := rg.instances("md")[0]
				return in.Spec.AllocationRef.Name, "Instance " + in.Name + " failed: AllocationFailed"
			}, 2 * time.Minute},
		{"scale-down of a pending Instance", []pfake.Script{{}, {QueueWait: 10 * time.Hour}}, func(md *v1alpha1.ModelDeployment) { noOverAllocation(md).Spec.Instances = 2 },
			func(t *testing.T, rg *rig) (string, string) {
				rg.until(5*time.Minute, 10*time.Second, "one ready", func() bool { return rg.readyCount("md") == 1 })
				var pending v1alpha1.Instance
				for _, in := range rg.liveInstances("md") {
					if in.Status.Phase != v1alpha1.InstanceReady {
						pending = in
					}
				}
				rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
				return pending.Spec.AllocationRef.Name, "Instance " + pending.Name + " ended: scaled down"
			}, time.Minute},
		{"shutdown of a pending Instance", []pfake.Script{{QueueWait: 10 * time.Hour}}, nil,
			func(t *testing.T, rg *rig) (string, string) {
				in := rg.instances("md")[0]
				rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { now := metav1.NewTime(rg.clock.Now()); m.Spec.EndTime = &now })
				return in.Spec.AllocationRef.Name, "Instance " + in.Name + " ended: the deployment is shutting down"
			}, time.Minute},
		{"retention: released when idle", []pfake.Script{{}}, func(md *v1alpha1.ModelDeployment) {
			md.Spec.Instances = 2
			md.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(15 * time.Minute)}
		},
			func(t *testing.T, rg *rig) (string, string) {
				rg.until(5*time.Minute, 10*time.Second, "two ready", func() bool { return rg.readyCount("md") == 2 })
				rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
				rg.until(5*time.Minute, 10*time.Second, "one live", func() bool { return len(rg.liveInstances("md")) == 1 })
				live := rg.liveInstances("md")[0].Spec.AllocationRef.Name
				for _, a := range rg.allocations("md") {
					if a.Name != live {
						return a.Name, "released when idle, after 15m0s"
					}
				}
				t.Fatal("no emptied Allocation")
				return "", ""
			}, 20 * time.Minute},
		{"retention: its deployment was deleted", []pfake.Script{{}}, nil,
			func(t *testing.T, rg *rig) (string, string) {
				rg.until(5*time.Minute, 10*time.Second, "one ready", func() bool { return rg.readyCount("md") == 1 })
				a := rg.allocations("md")[0].Name
				if err := rg.c.Delete(rg.ctx, rg.md("md")); err != nil {
					t.Fatal(err)
				}
				return a, "its deployment was deleted"
			}, 10 * time.Minute},
	} {
		t.Run(c.name, func(t *testing.T) {
			log := newCauseLog()
			rg := newRig(t, withInterceptors(log.funcs()))
			rg.batch.Enqueue(c.scripts...)
			rg.Step()
			md := mdSpec("md")
			if c.edit != nil {
				c.edit(md)
			}
			rg.create(md)
			rg.Step()
			target, want := c.act(t, rg)
			rg.until(c.wait, 10*time.Second, "the Allocation deleted", func() bool { _, ok := log.deletedWith(target); return ok })
			if got, _ := log.deletedWith(target); got != want {
				t.Fatalf("Allocation %s was deleted with the cause %q in the store, want %q", target, got, want)
			}
		})
	}
}

// TestAFailedCauseWriteDeletesNothing: while no release cause can be written, no Allocation is deleted, by any rule.
// A shutdown's end persists the Instance as ended before it releases, so when that release's cause fails to write,
// retention, later in the same pass, finds the Allocation empty and tries its own release; that cause fails too, so
// nothing goes, pass after pass, each failing with the write's error. Once writes succeed, the delete follows, and
// it carries the cause the store held. Worked by hand: the Instance has ended for Drained, so its end no longer asks
// for a release (releaseRequired), and retention releases the empty, pending Allocation with "a pending
// Allocation with no Instance is cancelled".
func TestAFailedCauseWriteDeletesNothing(t *testing.T) {
	log := newCauseLog()
	rg := newRig(t, withInterceptors(log.funcs()))
	rg.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour})
	rg.Step()
	rg.create(mdSpec("md"))
	rg.Step()
	in := rg.instances("md")[0]
	alloc := in.Spec.AllocationRef.Name
	rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { now := metav1.NewTime(rg.clock.Now()); m.Spec.EndTime = &now })
	log.mu.Lock()
	log.failWrites = 1 << 20 // every cause write fails
	log.mu.Unlock()
	for pass := range 3 {
		rg.clock.Advance(10 * time.Second)
		if err := rg.reconcileOf("md"); err == nil || !strings.Contains(err.Error(), "write the release cause") {
			t.Fatalf("pass %d, with every cause write failing, returned %v, want the write's error", pass, err)
		}
		if _, deleted := log.deletedWith(alloc); deleted {
			t.Fatalf("pass %d deleted the Allocation though no release cause could be written", pass)
		}
		a, ok := rg.allocation(alloc)
		if !ok || !a.DeletionTimestamp.IsZero() || a.Annotations[v1alpha1.AnnotationReleaseCause] != "" {
			t.Fatalf("pass %d left the Allocation as %+v, want it in place, with no cause", pass, a)
		}
	}
	log.mu.Lock()
	log.failWrites = 0
	log.mu.Unlock()
	rg.clock.Advance(10 * time.Second)
	rg.Step()
	got, ok := log.deletedWith(alloc)
	if !ok {
		t.Fatal("the Allocation was not deleted once its cause could be written")
	}
	if got != "a pending Allocation with no Instance is cancelled" {
		t.Fatalf("the Allocation was deleted with the stored cause %q, want retention's, %q", got, "a pending Allocation with no Instance is cancelled")
	}
}

// TestTheReleaseCauseIsOneBoundedLine: a cause is one line of at most 256 bytes, cut at a character's boundary; a
// failure's cause names its reason alone, never its message, which can carry text from outside the controller.
func TestTheReleaseCauseIsOneBoundedLine(t *testing.T) {
	if v1alpha1.AnnotationReleaseCause != "hpcburst.groundnuty.github.io/release-cause" {
		t.Errorf("the release cause's key is %q", v1alpha1.AnnotationReleaseCause)
	}
	if got := releaseCause("released when idle,\n  after 15m0s"); got != "released when idle, after 15m0s" {
		t.Errorf("a two-line cause is %q", got)
	}
	long := strings.Repeat("ż", 200) // 400 bytes
	got := releaseCause(long)
	if len(got) > 256 || !strings.HasSuffix(got, "…") || !strings.HasPrefix(long, strings.TrimSuffix(got, "…")) {
		t.Errorf("a 400-byte cause is %d bytes %q", len(got), got)
	}
	in := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "md-x"}}
	if got := endCause(in, v1alpha1.ReasonVerificationFailed, "rehash failed: /example/scratch/model"); got != "Instance md-x failed: VerificationFailed" {
		t.Errorf("a failure's cause is %q, want its reason alone", got)
	}
	if got := endCause(in, v1alpha1.ReasonDrained, "scaled down"); got != "Instance md-x ended: scaled down" {
		t.Errorf("a drain's cause is %q", got)
	}
}
