package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// HBEP-8's observer reasons. Both are proposed by HBEP-8 and not yet in
// api/v1alpha1's conditions.go, so they are named here, as the front names
// its own (internal/front/lease.go).
const (
	// reasonHeartbeatMissed: an observer's heartbeat is stale, so the
	// readings it fed are unknown (HBEP-8, Observers). Type failing.
	reasonHeartbeatMissed = "HeartbeatMissed"
	// reasonObservationGap: the lifecycle controller restarted, and what it
	// should have observed since its previous heartbeat it did not (HBEP-8,
	// Gaps after a restart). Type degraded.
	reasonObservationGap = "ObservationGap"
)

// frontLeaseName is the front's heartbeat Lease in v1alpha1.ControllerNamespace
// (HBEP-8, Observers). TODO(#122): v1alpha1.LeaseFront once PR 122 merges.
const frontLeaseName = "front"

// frontLeaseDefaultDuration is the front's leaseDurationSeconds (HBEP-8,
// Observers: renewed every 10 s with leaseDurationSeconds 30), the staleness
// bound of a front Lease that carries none.
const frontLeaseDefaultDuration = 30 * time.Second

// observeEvery is how often the observer judges the Providers' and the
// front's heartbeats: a third of the front Lease's 30 s, so a stale heartbeat
// is seen within its staleness rule plus one check (HBEP-8, Test plan, Stop an
// observer). The check reads each heartbeat's own time stamp, so the tick
// cannot alias against the heartbeat (HBEP-8, Risks).
const observeEvery = 10 * time.Second

// Repeat-gate keys of the heartbeats the lifecycle controller judges.
func providerHeartbeatKey(p *v1alpha1.Provider) string { return "heartbeat/provider/" + string(p.UID) }
func instanceHeartbeatKey(in *v1alpha1.Instance) string {
	return "heartbeat/instance/" + string(in.UID)
}

const frontHeartbeatKey = "heartbeat/lease/" + frontLeaseName

// observer judges, on the leader, the heartbeats of the observers the
// lifecycle controller does not depend on: every Provider's status read and
// the front's Lease (HBEP-8, Observers). It runs apart from the reconcile
// loop, which is keyed per ModelDeployment, so a Provider that no deployment
// allows and the front are judged too. The agents' reports are judged in the
// reconcile pass, where AgentUnreachable is written.
type observer struct {
	r     *Reconciler
	every time.Duration
}

// Start judges the heartbeats at once, then every o.every until ctx ends. A
// failed read is logged and judged again at the next tick: a heartbeat that
// cannot be read is unknown, never stale and never fresh.
func (o *observer) Start(ctx context.Context) error {
	t := time.NewTicker(o.every)
	defer t.Stop()
	for {
		if err := o.r.checkHeartbeats(ctx); err != nil {
			ctrl.LoggerFrom(ctx).Error(err, "judging the observers' heartbeats")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// NeedLeaderElection: only the leader judges and emits.
func (o *observer) NeedLeaderElection() bool { return true }

// checkHeartbeats judges every Provider's heartbeat and the front's Lease at
// the controller's clock, and emits failing/HeartbeatMissed when one is stale:
// when it goes stale, then at most once an hour while it stays stale (HBEP-8,
// Observers and Repeating conditions).
func (r *Reconciler) checkHeartbeats(ctx context.Context) error {
	now := r.Clock.Now().UTC()
	mem := r.memory()
	var provs v1alpha1.ProviderList
	errProv := r.Client.List(ctx, &provs)
	if errProv != nil {
		errProv = fmt.Errorf("list Providers: %w", errProv)
	} else {
		for i := range provs.Items {
			p := &provs.Items[i]
			key := providerHeartbeatKey(p)
			stale, msg := providerHeartbeatStale(p, now)
			if !stale {
				mem.clear(key)
				continue
			}
			if mem.repeat(key, now) {
				r.Recorder.Event(p, nil, eventFailing, reasonHeartbeatMissed, msg)
			}
		}
	}
	return errors.Join(errProv, r.checkFrontLease(ctx, now))
}

// providerHeartbeatStale judges a Provider's heartbeat, batch or managed alike,
// by its age alone: lastSuccessfulStatusRead older than
// v1alpha1.ProviderReadyStaleness. The stored Ready is not read, since a
// stopped Provider controller cannot write it (HBEP-8, Observers). A Provider
// with no successful read at all is stale once its creation is older than the
// bound (`@hpc-burst-science-agent`, 31 Oct 2026, for #111): a Provider
// controller that never starts is a stopped observer too.
func providerHeartbeatStale(p *v1alpha1.Provider, now time.Time) (bool, string) {
	bound := v1alpha1.ProviderReadyStaleness
	if last := p.Status.LastSuccessfulStatusRead; last != nil {
		age := now.Sub(last.Time)
		return age > bound, fmt.Sprintf("Provider %s's last successful status read was at %s, %s ago, older than %s; its queue readings are unknown, and it takes no new Allocation",
			p.Name, last.UTC().Format(time.RFC3339), age.Truncate(time.Second), bound)
	}
	created := p.CreationTimestamp.Time
	if created.IsZero() {
		return false, ""
	}
	age := now.Sub(created)
	return age > bound, fmt.Sprintf("Provider %s has had no successful status read since its creation at %s, %s ago, longer than %s; its queue readings are unknown, and it takes no new Allocation",
		p.Name, created.UTC().Format(time.RFC3339), age.Truncate(time.Second), bound)
}

// checkFrontLease judges the front's heartbeat: its Lease is stale once its
// renewTime is older than its leaseDurationSeconds (HBEP-8, Observers). An
// absent Lease is not judged and raises nothing (`@hpc-burst-science-agent`,
// 31 Oct 2026, for #111): hpcburst status shows an absent front.
func (r *Reconciler) checkFrontLease(ctx context.Context, now time.Time) error {
	mem := r.memory()
	var l coordinationv1.Lease
	err := r.Reader.Get(ctx, client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: frontLeaseName}, &l)
	if apierrors.IsNotFound(err) {
		mem.clear(frontHeartbeatKey)
		return nil
	}
	if err != nil {
		return fmt.Errorf("read the front's Lease: %w", err)
	}
	bound := frontLeaseDefaultDuration
	if d := l.Spec.LeaseDurationSeconds; d != nil && *d > 0 {
		bound = time.Duration(*d) * time.Second
	}
	stale, msg := true, fmt.Sprintf("the front's Lease %s/%s has no renewTime", l.Namespace, l.Name)
	if rt := l.Spec.RenewTime; rt != nil {
		age := now.Sub(rt.Time)
		stale = age > bound
		msg = fmt.Sprintf("the front's Lease %s/%s was last renewed at %s, %s ago, longer than its lease duration of %s",
			l.Namespace, l.Name, rt.UTC().Format(time.RFC3339), age.Truncate(time.Second), bound)
	}
	if !stale {
		mem.clear(frontHeartbeatKey)
		return nil
	}
	if mem.repeat(frontHeartbeatKey, now) {
		r.Recorder.Event(&l, nil, eventFailing, reasonHeartbeatMissed, msg)
	}
	return nil
}
