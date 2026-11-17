package lifecycle

import (
	"context"
	"fmt"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/leaderlock"
)

// gapLock is the leader election's lock with the start's observation of HBEP-8,
// Gaps after a restart: the lifecycle controller reads its Lease's previous
// renewTime before it takes the Lease. The lock is leaderlock's, which the
// Provider managers use too on their provider-<name> Leases.
type gapLock = leaderlock.GapLock

// newLeaseLock builds the Lease lock inside the gapLock; a test wraps it to
// see the manager lead through the lock it returns.
var newLeaseLock = leaderlock.New

// newGapLock builds the lifecycle Lease's lock with leaderlock.New, as
// controller-runtime's leaderelection.NewResourceLock does (v0.23.1, pkg/
// leaderelection/leader_election.go, lines 66-136: a hostname_UUID identity, a
// "leader-election" user agent, a client timeout of half the renew deadline),
// with its release retried if a late renew made it fail (issue #131), and
// wraps it in a gapLock.
//
// The lock is given no EventRecorder, where controller-runtime's own would
// pass the manager's, so the elector raises none of the legacy
// leader-election events (became leader, stopped leading). They carry no
// action, so under HBEP-8 no tool may act on them. Leadership changes still
// show in the Lease itself, and the gap after a restart is the typed
// ObservationGap (`@hpc-burst-science-agent`, 1 Nov 2026, for #111).
func newGapLock(cfg *rest.Config, renewDeadline time.Duration) (*gapLock, error) {
	inner, err := newLeaseLock(cfg, v1alpha1.ControllerNamespace, v1alpha1.LeaseLifecycle, renewDeadline, ctrl.Log)
	if err != nil {
		return nil, err
	}
	return leaderlock.NewGapLock(inner), nil
}

// gapTime formats a renewTime as the API stores it, to the microsecond, so the
// gap's start names the exact heartbeat.
var gapTime = leaderlock.GapTime

// gapEmitter raises the start's ObservationGap once the Lease is held: it runs
// only on the leader, after the elector's take (HBEP-8, Gaps after a
// restart). One degraded event, regarding the lifecycle Lease, whose message
// names the previous renewTime, the gap's start. Missed transitions are not
// replayed; status is the truth.
type gapEmitter struct {
	lock *gapLock
	// reader reads the Lease for its UID, uncached; nil regards the Lease by
	// name alone.
	reader client.Reader
	rec    Recorder
	once   sync.Once
	// onDone, when set, runs once the start's decision is made and any event
	// handed to the recorder, whether one was raised or not: the emitter has
	// no pass marker of its own, so a test proves that it raised no event by
	// it (HBEP-8, Raised events, condition 3).
	onDone func()
}

// Start implements manager.Runnable. It emits at most once per process and
// returns: a lost lead stops the manager, so a process leads only once.
func (g *gapEmitter) Start(ctx context.Context) error {
	if g.onDone != nil {
		defer g.onDone()
	}
	g.once.Do(func() {
		since, ok := g.lock.GapStart()
		if !ok {
			return
		}
		lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle}}
		if g.reader != nil {
			var got coordinationv1.Lease
			if err := g.reader.Get(ctx, client.ObjectKeyFromObject(lease), &got); err != nil {
				ctrl.LoggerFrom(ctx).Error(err, "reading the lifecycle Lease for the ObservationGap event; it is raised without the Lease's UID")
			} else {
				lease.UID = got.UID
			}
		}
		g.rec.Event(lease, nil, eventDegraded, reasonObservationGap,
			fmt.Sprintf("the lifecycle controller restarted; its previous heartbeat, the lifecycle Lease's renewTime, was at %s, and what it should have observed since then was not observed; status is the truth", gapTime(since)))
	})
	return nil
}

// NeedLeaderElection: the event follows the take.
func (g *gapEmitter) NeedLeaderElection() bool { return true }
