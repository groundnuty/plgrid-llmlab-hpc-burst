package main

// tunnel-state: the writer of the Instances' TunnelConnected condition (HBEP-6, Path state and
// the inventory), one of the two components addTunnel (tunnel.go) adds.

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/frpsplugin"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/lifecycle"
)

// HBEP-6 gives the read period (every 10 s) and the settle time (about a second after
// each NewProxy and CloseProxy). They are variables so that a test can shorten or lengthen
// them; no test in this package runs in parallel, and each restores what it set once the run it
// started has returned.
var (
	tunnelReadPeriod = 10 * time.Second
	tunnelReadSettle = time.Second
)

// orphanEventEvery is HBEP-8's gate for a repeating event: an orphan proxy's TunnelOrphaned is
// emitted when it is first seen, then at most once an hour while it lasts (HBEP-8, Repeating
// conditions).
const orphanEventEvery = time.Hour

// tunnelReads counts the reads of frps's list that tunnel-state has finished
// and written the conditions of. It is for tests, which wait on it.
var tunnelReads atomic.Int64

// pathReader reads frps's proxy list for the given Instance UIDs.
// *frpsplugin.Reader is the one.
type pathReader interface {
	Read(ctx context.Context, uids []string) frpsplugin.Report
}

var _ pathReader = (*frpsplugin.Reader)(nil)

// pathState reads frps's proxy list and writes each Instance's TunnelConnected
// condition from it (HBEP-6, Path state and the inventory): at its start, every
// tunnelReadPeriod, and tunnelReadSettle after a signal of the trigger. It
// writes a condition only from a read of the list, so after a restart it
// rebuilds from the list before it writes any.
//
// The write itself is lifecycle.WriteTunnelConnected, beside the reconciler's
// own status writes, since both share one status and one resourceVersion.
//
// An ended Instance is not written. Its TunnelConnected stays as it was at the
// end, True included: the condition describes the tunnel at the end, and a
// proxy that is still online after it is reported as an orphan (TunnelOrphaned),
// the one place the list's later state about it shows. Nothing reads an ended
// Instance's condition to route or to decide (HBEP-6, The inventory); writing
// False or Unknown at the end would add a write that races the reconciler's
// final one and tell nothing the Instance's end does not.
type pathState struct {
	cache   tunnelCache
	cached  client.Reader // the cache: list the Instances and the Allocations
	fresh   client.Reader // the API server: re-read after a conflict, and the Lease's UID
	writer  client.StatusClient
	reader  pathReader
	trigger <-chan struct{}
	log     logr.Logger

	// rec is the lifecycle controller's own recorder (lifecycle.KubeRecorder over its event
	// recorder): TunnelOrphaned is its event (HBEP-8, Reasons in alpha).
	rec lifecycle.Recorder
	// admitted says which Allocation's credential admitted each proxy, from the plugin's accepted
	// NewProxy calls. It is in memory: after a restart the orphan's event regards the Lease.
	admitted *admissions
	// now is the gate's clock; nil is the wall clock. A test injects its own, so that no test
	// waits an hour.
	now func() time.Time

	orphans map[string]time.Time // when each orphan's event was last emitted; absent once it is gone
}

func (s *pathState) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// run returns nil once ctx ends, also while it waits for the cache.
func (s *pathState) run(ctx context.Context) error {
	if !s.cache.WaitForCacheSync(ctx) {
		if ctx.Err() != nil {
			return nil
		}
		return errors.New("tunnel state: the cache did not sync")
	}
	periodic := time.NewTimer(0) // the read at the start
	defer periodic.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-periodic.C:
		case <-s.trigger:
			// frps registers a proxy a moment after the plugin accepts it, and
			// drops one a moment before it posts CloseProxy's call.
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(tunnelReadSettle):
			}
			// This read covers every signal that came meanwhile.
			select {
			case <-s.trigger:
			default:
			}
		}
		s.readOnce(ctx)
		periodic.Reset(tunnelReadPeriod)
	}
}

// readOnce reads the list once for every Instance that has not ended and
// writes the conditions that changed.
func (s *pathState) readOnce(ctx context.Context) {
	var list v1alpha1.InstanceList
	if err := s.cached.List(ctx, &list); err != nil {
		if ctx.Err() == nil {
			s.log.Error(err, "list Instances; no condition is written from this read")
		}
		return
	}
	var live []*v1alpha1.Instance
	var uids []string
	for i := range list.Items {
		in := &list.Items[i]
		if in.Status.Phase == v1alpha1.InstanceEnded || in.UID == "" {
			continue
		}
		live = append(live, in)
		uids = append(uids, string(in.UID))
	}
	rep := s.reader.Read(ctx, uids)
	if ctx.Err() != nil {
		// A read the stop cut is an error that says nothing about frps: writing
		// Unknown from it would flap every condition at each restart.
		return
	}
	s.reportOrphans(ctx, rep, uids)
	for _, in := range live {
		want, ok := rep.Conditions[string(in.UID)]
		if !ok {
			continue
		}
		if err := s.apply(ctx, in, want); err != nil && ctx.Err() == nil {
			s.log.Error(err, "write the TunnelConnected condition", "instance", client.ObjectKeyFromObject(in).String())
		}
	}
	tunnelReads.Add(1)
}

// apply writes the condition on the Instance when its status or reason differs from the stored
// one: lifecycle.WriteTunnelConnected, which holds the rules (the resourceVersion, the re-read
// after a conflict, the skip of an ended Instance).
func (s *pathState) apply(ctx context.Context, in *v1alpha1.Instance, want metav1.Condition) error {
	return lifecycle.WriteTunnelConnected(ctx, s.writer, s.fresh, in, want)
}

// reportOrphans raises TunnelOrphaned for each online proxy that is no current Instance's UID
// (HBEP-6: an orphan; HBEP-8, Reasons in alpha): when it is first seen, then at most once an hour
// while it lasts (HBEP-8, Repeating conditions; orphanEventEvery). The gate is in memory, so a new
// leader emits once more, which is "when it starts" for that process. An orphan that is gone from a
// read leaves the gate, so one that comes back is a new start. A list that could not be read says
// nothing about the orphans: the gate and the admissions stay as they were.
func (s *pathState) reportOrphans(ctx context.Context, rep frpsplugin.Report, live []string) {
	if rep.Err != nil {
		return
	}
	now := s.clock()
	next := make(map[string]time.Time, len(rep.Orphans))
	keep := make(map[string]struct{}, len(rep.Orphans)+len(live))
	for _, uid := range live {
		keep[uid] = struct{}{}
	}
	for _, name := range rep.Orphans {
		keep[name] = struct{}{}
		last, seen := s.orphans[name]
		if seen && now.Sub(last) < orphanEventEvery {
			next[name] = last
			continue
		}
		s.log.Info("frps lists an online proxy that is no Instance's UID", "proxy", name)
		s.orphanEvent(ctx, name)
		next[name] = now
	}
	s.orphans = next
	// What the plugin admitted is kept for the proxies that are, or may yet be, an orphan: a live
	// Instance's, whose Allocation the event will name if the Instance ends with its tunnel up,
	// and the orphans'. The rest is offline and has no Instance.
	s.admitted.retain(keep)
}

// orphanEvent emits one TunnelOrphaned, a degraded event, regarding the Allocation whose
// credential admitted the tunnel when the plugin's admissions name it and the cache has it, and
// else the lifecycle Lease (HBEP-8, Reasons in alpha).
func (s *pathState) orphanEvent(ctx context.Context, proxy string) {
	subject, how := s.orphanSubject(ctx, proxy)
	s.rec.Event(subject, nil, lifecycle.EventDegraded, v1alpha1.EventReasonTunnelOrphaned,
		fmt.Sprintf("frps lists the proxy %s online, and no current Instance has that UID (an orphan, HBEP-6); %s", proxy, how))
}

// orphanSubject picks the object a proxy's TunnelOrphaned regards, and says why in a clause for
// the event's note.
func (s *pathState) orphanSubject(ctx context.Context, proxy string) (client.Object, string) {
	if uid, ok := s.admitted.allocationOf(proxy); ok {
		var list v1alpha1.AllocationList
		if err := s.cached.List(ctx, &list); err != nil {
			s.log.Error(err, "list Allocations for the TunnelOrphaned event; it regards the lifecycle Lease", "proxy", proxy)
		} else {
			for i := range list.Items {
				if a := &list.Items[i]; string(a.UID) == uid {
					return a, fmt.Sprintf("its tunnel was admitted by the credential of Allocation %s/%s", a.Namespace, a.Name)
				}
			}
		}
	}
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle}}
	var got coordinationv1.Lease
	if err := s.fresh.Get(ctx, client.ObjectKeyFromObject(lease), &got); err != nil {
		s.log.Error(err, "reading the lifecycle Lease for the TunnelOrphaned event; it is raised without the Lease's UID")
	} else {
		lease.UID = got.UID
	}
	return lease, "no Allocation is known to have admitted its tunnel"
}
