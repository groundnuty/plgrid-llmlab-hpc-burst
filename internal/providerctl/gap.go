package providerctl

// ObservationGap from each Provider manager (beta; HBEP-8, Gaps after a restart;
// Allocation events (beta), After a restart; #625). At its start a Provider
// manager reads its provider-<name> Lease's previous renewTime before it takes
// the Lease (leaderlock.GapLock, which the lifecycle controller's own Lease
// uses too), and once it holds the Lease it raises one degraded event regarding
// that Lease whose note names that renewTime, the start of the gap. A first
// start, with no previous Lease, raises none. Missed transitions are not
// replayed: status is the truth.

import (
	"context"
	"fmt"
	"sync"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/leaderlock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// ReasonObservationGap is the reason of the event a manager raises at its start
// when its Lease shows a gap (the lifecycle controller and the front raise the
// same reason; HBEP-8, Reasons table). The managed catalog controller's series
// at 0 list it too (internal/managed), since its manager raises it through
// AddGapEmitter.
const ReasonObservationGap = "ObservationGap"
const reasonObservationGap = ReasonObservationGap

// gapLocked is a manager that carries the GapLock of its leader Lease
// (NewProviderManager's).
type gapLocked interface{ GapLock() *leaderlock.GapLock }

// AddGapEmitter adds to mgr, the manager of Provider providerName, the runnable that
// raises the start's ObservationGap through rec once the manager holds its
// Lease. A manager that does not carry a GapLock (one a test built, not
// NewProviderManager) gets none, and the error is nil: there is no Lease read to
// report from. rec is the Provider's typed recorder, wrapped by
// eventcount.Events, so the event is counted under provider-<name>.
func AddGapEmitter(mgr manager.Manager, providerName string, rec eventcount.IDRecorder) error {
	g, ok := mgr.(gapLocked)
	if !ok || rec == nil {
		return nil
	}
	return mgr.Add(&gapEmitter{provider: providerName, lock: g.GapLock(), reader: mgr.GetAPIReader(), rec: rec})
}

// gapEmitter raises the start's ObservationGap once the Lease is held: it runs
// only on the leader, after the elector's take.
type gapEmitter struct {
	provider string
	lock     *leaderlock.GapLock
	// reader reads the Lease for its UID, uncached; nil regards the Lease by name
	// alone.
	reader client.Reader
	rec    eventcount.IDRecorder
	once   sync.Once
}

// Start implements manager.Runnable. It raises at most once per process and
// returns: a lost lead stops the manager, so a process leads only once.
func (g *gapEmitter) Start(ctx context.Context) error {
	g.once.Do(func() {
		since, ok := g.lock.GapStart()
		if !ok {
			return
		}
		name := v1alpha1.ProviderLeaseName(g.provider)
		lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: v1alpha1.ControllerNamespace, Name: name}}
		if g.reader != nil {
			var got coordinationv1.Lease
			if err := g.reader.Get(ctx, client.ObjectKeyFromObject(lease), &got); err != nil {
				ctrl.LoggerFrom(ctx).Error(err, "reading the Provider's Lease for the ObservationGap event; it is raised without the Lease's UID")
			} else {
				lease.UID = got.UID
			}
		}
		g.rec.Eventf(lease, nil, provider.EventKindWarning, reasonObservationGap, eventDegraded, "%s",
			provider.CutNote(fmt.Sprintf("the controller of Provider %s restarted; its previous heartbeat, the %s Lease's renewTime, was at %s, and what it should have observed since then was not observed; status is the truth",
				g.provider, name, leaderlock.GapTime(since))))
	})
	return nil
}

// NeedLeaderElection: the event follows the take.
func (g *gapEmitter) NeedLeaderElection() bool { return true }
