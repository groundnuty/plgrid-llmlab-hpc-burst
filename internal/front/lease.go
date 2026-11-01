package front

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// The front's heartbeat Lease (HBEP-8, Observers; HBEP-11, Controllers): named
// "front", in v1alpha1.ControllerNamespace, renewed every 10 s with a
// leaseDurationSeconds of 30. It is a heartbeat, not an election: the
// lifecycle controller judges it stale once its renewTime is older than the
// duration. It gives the front's own events (UsageWriteFailed,
// ObservationGap) a namespaced object to regard. Its name is v1alpha1.LeaseFront.
const (
	leaseName            = v1alpha1.LeaseFront
	leaseRenewEvery      = 10 * time.Second
	leaseDurationSeconds = 30
	// fieldManagerFront names the front in the managedFields of what it
	// writes, and as the reportingController of its events.
	fieldManagerFront = "hpcburst-front"
	// leaseLogEvery bounds how often the same failure to renew is logged.
	leaseLogEvery = time.Hour
)

// reasonObservationGap: the front restarted, and what it should have seen
// since its previous heartbeat it did not (HBEP-8, Gaps after a restart).
const reasonObservationGap = "ObservationGap"

// leaseRef is the front's Lease as the subject of an event. The keeper sets
// it when it holds the Lease; the router reads it.
type leaseRef struct {
	mu   sync.Mutex
	have bool
	uid  types.UID
}

// held reports whether the front has taken its Lease.
func (l *leaseRef) held() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.have
}

func (l *leaseRef) set(held bool, uid types.UID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.have = held
	if uid != "" {
		l.uid = uid
	}
}

// object is a Lease with just the identity an event reference needs. It
// carries no resourceVersion: the recorder keys isomorphic events on the
// reference, and the Lease changes every 10 s.
func (l *leaseRef) object() *coordinationv1.Lease {
	l.mu.Lock()
	defer l.mu.Unlock()
	return &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: v1alpha1.ControllerNamespace, Name: leaseName, UID: l.uid}}
}

// leaseKeeper takes the front's Lease and renews it. A failure to read or
// write it is logged and tried again at the next beat: the front keeps
// serving without it, since serving depends on no metric or event (HBEP-8,
// Operational readiness 7), and the launcher creates the namespace, which may
// not exist yet.
type leaseKeeper struct {
	api    kubernetes.Interface
	clk    clock.Clock
	rec    Recorder
	ref    *leaseRef
	log    *slog.Logger
	holder string

	// onHeld, when set, runs after each beat that leaves the front holding
	// the Lease: the event that waited for a Lease to regard goes out then.
	onHeld func()

	cur *coordinationv1.Lease // the Lease as last written
	// observed, previous: the start's observation. The first read of the
	// Lease in this run, before any write of ours, gives the previous
	// renewTime (nil: no Lease, or none renewed). A write that commits and
	// then reports an error is retried, and the retry reads this run's own
	// heartbeat; the gap is judged from the observation kept here, never
	// from what a later read finds.
	observed bool
	previous *metav1.MicroTime
	gapDone  bool // the start's ObservationGap has been decided
	lastErr  string
	lastErrT time.Time
}

func newLeaseKeeper(api kubernetes.Interface, clk clock.Clock, rec Recorder, ref *leaseRef, log *slog.Logger) *leaseKeeper {
	return &leaseKeeper{api: api, clk: clk, rec: rec, ref: ref, log: log, holder: fmt.Sprintf("%s/%d", fieldManagerFront, os.Getpid())}
}

// run takes the Lease at once and renews it every leaseRenewEvery on the
// front's clock, until ctx ends. The Lease is left as it is at the end: its
// renewTime is what the next start names as the gap's start.
func (k *leaseKeeper) run(ctx context.Context) {
	for {
		k.beat(ctx)
		tick := make(chan struct{})
		t := k.clk.AfterFunc(leaseRenewEvery, func() { close(tick) })
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-tick:
		}
	}
}

// beat takes the Lease when the front does not hold it, and renews it when it
// does.
func (k *leaseKeeper) beat(ctx context.Context) {
	var err error
	if k.cur == nil {
		err = k.take(ctx)
	} else {
		err = k.renew(ctx)
	}
	if err != nil {
		k.ref.set(false, "")
		k.fail(err)
		return
	}
	k.lastErr = ""
	if k.onHeld != nil {
		k.onHeld()
	}
}

// take reads the Lease, then creates it or updates it with the resourceVersion
// it read (a conflict is an error, to be read again at the next beat, never
// overwritten). The first read of a run is the start's observation: the Lease's
// previous renewTime, kept across retries (see leaseKeeper.previous). Once the
// front holds the Lease, the first take of this run raises one ObservationGap
// naming that renewTime; no previous Lease, or one with no renewTime, raises
// none.
func (k *leaseKeeper) take(ctx context.Context) error {
	leases := k.api.CoordinationV1().Leases(v1alpha1.ControllerNamespace)
	now := metav1.NewMicroTime(k.clk.Now())
	prev, err := leases.Get(ctx, leaseName, metav1.GetOptions{})
	var held *coordinationv1.Lease
	switch {
	case apierrors.IsNotFound(err):
		k.observe(nil)
		held, err = leases.Create(ctx, &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Namespace: v1alpha1.ControllerNamespace, Name: leaseName},
			Spec:       k.spec(now),
		}, metav1.CreateOptions{FieldManager: fieldManagerFront})
	case err != nil:
		return fmt.Errorf("read the Lease: %w", err)
	default:
		k.observe(prev.Spec.RenewTime)
		next := prev.DeepCopy()
		next.Spec = k.spec(now)
		held, err = leases.Update(ctx, next, metav1.UpdateOptions{FieldManager: fieldManagerFront})
	}
	if err != nil {
		return fmt.Errorf("take the Lease: %w", err)
	}
	k.cur = held
	k.ref.set(true, held.UID)
	if !k.gapDone {
		k.gapDone = true
		if k.previous != nil {
			k.observationGap(k.previous.Time)
		}
	}
	return nil
}

// observe keeps the start's observation: the first call of a run sets it, and
// the later ones, which read what this run itself wrote, do not.
func (k *leaseKeeper) observe(renewTime *metav1.MicroTime) {
	if k.observed {
		return
	}
	k.observed, k.previous = true, renewTime
}

// spec is the Lease's content for a take or a renewal at now.
func (k *leaseKeeper) spec(now metav1.MicroTime) coordinationv1.LeaseSpec {
	return coordinationv1.LeaseSpec{
		HolderIdentity:       ptr.To(k.holder),
		LeaseDurationSeconds: ptr.To(int32(leaseDurationSeconds)),
		AcquireTime:          &now,
		RenewTime:            &now,
	}
}

// renew moves renewTime on. The Lease gone, or changed under the front,
// makes the next beat take it again; that is no restart, so it raises no gap.
func (k *leaseKeeper) renew(ctx context.Context) error {
	next := k.cur.DeepCopy()
	now := metav1.NewMicroTime(k.clk.Now())
	next.Spec.HolderIdentity = ptr.To(k.holder)
	next.Spec.LeaseDurationSeconds = ptr.To(int32(leaseDurationSeconds))
	next.Spec.RenewTime = &now
	got, err := k.api.CoordinationV1().Leases(v1alpha1.ControllerNamespace).Update(ctx, next, metav1.UpdateOptions{FieldManager: fieldManagerFront})
	if err != nil {
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			k.cur = nil
		}
		return fmt.Errorf("renew the Lease: %w", err)
	}
	k.cur = got
	// A renewal that follows a failure holds the Lease again.
	k.ref.set(true, got.UID)
	return nil
}

// observationGap raises the event for a restart: degraded, regarding the
// Lease, its message naming the previous renewTime, the gap's start.
func (k *leaseKeeper) observationGap(since time.Time) {
	k.rec.Event(k.ref.object(), true, eventDegraded, reasonObservationGap,
		fmt.Sprintf("the front restarted; its previous heartbeat was at %s, and what it should have observed since then was not observed", usage.Time(since)))
}

// fail logs a failure to take or renew, once for each distinct error and then
// at most hourly, so a namespace that does not exist yet does not fill the log
// every 10 s.
func (k *leaseKeeper) fail(err error) {
	now := k.clk.Now()
	if msg := err.Error(); msg == k.lastErr && now.Sub(k.lastErrT) < leaseLogEvery {
		return
	}
	k.lastErr, k.lastErrT = err.Error(), now
	k.log.Error("the front's Lease is not held; serving goes on", "lease", leaseName, "namespace", v1alpha1.ControllerNamespace, "error", err.Error())
}
