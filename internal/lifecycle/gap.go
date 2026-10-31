package lifecycle

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// gapLock is the leader election's lock with the start's observation of HBEP-8,
// Gaps after a restart: the lifecycle controller reads its Lease's previous
// renewTime before it takes the Lease. Every read the elector makes before this
// process's first successful write is such a read, and the last of them is the
// Lease as it stood just before the take: the previous holder's final
// heartbeat, or its release on a clean stop. A read that shows this process as
// the holder before a write has succeeded comes from a write that committed but
// reported an error; it is this run's own heartbeat and does not count.
type gapLock struct {
	resourcelock.Interface

	mu       sync.Mutex
	took     bool
	observed bool
	// previous is the renewTime read before the take; zero when there was no
	// Lease or it had no renewTime (a first start).
	previous time.Time
}

// Get implements resourcelock.Interface.
func (l *gapLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	rec, raw, err := l.Interface.Get(ctx)
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.took {
		switch {
		case apierrors.IsNotFound(err):
			l.observed, l.previous = true, time.Time{}
		case err == nil && rec != nil && rec.HolderIdentity != l.Identity():
			l.observed, l.previous = true, rec.RenewTime.Time
		}
	}
	return rec, raw, err
}

// Create implements resourcelock.Interface.
func (l *gapLock) Create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	err := l.Interface.Create(ctx, ler)
	l.wrote(err)
	return err
}

// Update implements resourcelock.Interface.
func (l *gapLock) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	err := l.Interface.Update(ctx, ler)
	l.wrote(err)
	return err
}

func (l *gapLock) wrote(err error) {
	if err != nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.took = true
}

// gapStart returns the previous renewTime read before the take, and whether
// there is a gap to report: the Lease is held, it was read before the take,
// and that read found a renewTime. A first start, with no previous Lease,
// reports none.
func (l *gapLock) gapStart() (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.previous, l.took && l.observed && !l.previous.IsZero()
}

// newGapLock builds the lifecycle Lease's lock as controller-runtime's
// leaderelection.NewResourceLock does (v0.23.1, pkg/leaderelection/
// leader_election.go, lines 66-136: a hostname_UUID identity, a
// "leader-election" user agent, a client timeout of half the renew deadline),
// wrapped in a gapLock.
func newGapLock(cfg *rest.Config, renewDeadline time.Duration) (*gapLock, error) {
	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	cfg = rest.AddUserAgent(rest.CopyConfig(cfg), "leader-election")
	cfg.Timeout = max(renewDeadline/2, time.Second)
	core, err := corev1client.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	coord, err := coordinationv1client.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	inner, err := resourcelock.New(resourcelock.LeasesResourceLock, v1alpha1.ControllerNamespace, v1alpha1.LeaseLifecycle,
		core, coord, resourcelock.ResourceLockConfig{Identity: host + "_" + string(uuid.NewUUID())})
	if err != nil {
		return nil, err
	}
	return &gapLock{Interface: inner}, nil
}

// gapTime formats a renewTime as the API stores it, to the microsecond
// (metav1.MicroTime), so the gap's start names the exact heartbeat.
func gapTime(t time.Time) string { return t.UTC().Format(metav1.RFC3339Micro) }

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
}

// Start implements manager.Runnable. It emits at most once per process and
// returns: a lost lead stops the manager, so a process leads only once.
func (g *gapEmitter) Start(ctx context.Context) error {
	g.once.Do(func() {
		since, ok := g.lock.gapStart()
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
