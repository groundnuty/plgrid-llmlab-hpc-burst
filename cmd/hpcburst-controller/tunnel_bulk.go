package main

// The controller's half of the bulk path (beta; HBEP-6, The bulk path; #448 part 3), reached only
// with --bulk-path. Everything here is new: the first frps's code in tunnel.go and tunnel_state.go
// is not changed for it (the science agent's ruling S5), and with the flag off nothing here is built.
//
//   - The plugin, per frps. A plugin call carries nothing that names its frps, so the second frps
//     posts to its own path (agentapi.FrpsBulkPluginPath) and a second frpsplugin.Handler serves it
//     on the same server, with the same Index, so the admission rules are the same on both. It has
//     a Notifier of its own, a second `admissions`: a bulk NewProxy never changes what the first
//     frps's admissions record, and the reverse. Both Notifiers wake the one Trigger, so the
//     bulk list is read on the cadence and the signals of the first.
//   - Path state. pathState.readOnce reads both lists. TunnelConnected follows the first list only,
//     so an Instance's first `ready` doesn't wait for the bulk path, and a bulk list that is
//     offline, absent or unreadable changes no condition (HBEP-6, Path state; the plan's R3).
//     An orphan on the second frps is reported as TunnelOrphaned, under the same grace and the same
//     hourly gate as the first's, but with a clock and a gate of their own: the state of each frps
//     is kept apart, so what is keyed by (frps, proxy name) is the pair of ledgers and the name
//     in each. The event's message names frps-bulk. A name listed on both frps is expected, and a
//     name listed twice on one makes that list unreadable (frpsplugin's checkList, per list).
//   - No inventory surface is added: the two lists are the inventory (the plan's R4).

import (
	"context"
	"fmt"
	"net/http"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/frpsplugin"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/lifecycle"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// bulkTunnel is what --bulk-path adds to addTunnel: the handler of the second frps's plugin path,
// and the state that reads its list.
type bulkTunnel struct {
	handler *frpsplugin.Handler
	state   *bulkPathState
}

// newBulkTunnel builds the second frps's handler over the Index the first's handler answers from,
// its Notifier (a second admissions, waking the same trigger), and the reader of its list at base+15
// with its own web password file.
func newBulkTunnel(host agentapi.Host, trigger *frpsplugin.Trigger, index frpsplugin.Index) (*bulkTunnel, error) {
	reader, err := frpsplugin.NewHostBulkReader(host)
	if err != nil {
		return nil, err
	}
	admitted := newAdmissions(trigger)
	handler, err := frpsplugin.NewHandler(index, frpsplugin.Options{Notifier: admitted, Path: agentapi.FrpsBulkPluginPath})
	if err != nil {
		return nil, err
	}
	return &bulkTunnel{handler: handler, state: &bulkPathState{reader: reader, admitted: admitted}}, nil
}

// route is the plugin server's handler with the bulk path on: a call at the bulk path goes to the bulk
// handler, and everything else to the first frps's, which answers 404 anywhere but its own path. Each
// handler also answers 404 at the other's path, so neither serves the other's calls.
func (b *bulkTunnel) route(token http.Handler) http.Handler {
	return bulkRoute{token: token, bulk: b.handler}
}

type bulkRoute struct{ token, bulk http.Handler }

func (r bulkRoute) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path == agentapi.FrpsBulkPluginPath {
		r.bulk.ServeHTTP(w, req)
		return
	}
	r.token.ServeHTTP(w, req)
}

// bulkPathState is the second frps's half of pathState: its list's reader, the plugin's record of
// what it admitted, and its orphans' clock and hourly gate, all apart from the first's.
type bulkPathState struct {
	reader   pathReader
	admitted *admissions
	// orphans and suspects are pathState.orphans and pathState.suspects for this frps: when each
	// orphan's event was last emitted, and when the unbroken run of readable reads that list a proxy
	// online with no Instance began.
	orphans  map[string]time.Time
	suspects map[string]time.Time
	// unreadable says the last read of the list failed, so that the log says so when that changes
	// and not on every read.
	unreadable bool
}

// readBulk reads the second frps's list and reports its orphans. It writes no condition: TunnelConnected
// follows the first list only (HBEP-6, The bulk path). With the bulk path off it does nothing, and
// reads nothing.
func (s *pathState) readBulk(ctx context.Context, uids []string) {
	b := s.bulk
	if b == nil {
		return
	}
	rep := b.reader.Read(ctx, uids)
	if ctx.Err() != nil {
		return // a read the stop cut says nothing about frps-bulk
	}
	switch {
	case rep.Err != nil && !b.unreadable:
		s.log.Error(rep.Err, "frps-bulk's proxy list could not be read; its orphans are not judged from reads that fail, and no condition depends on it")
	case rep.Err == nil && b.unreadable:
		s.log.Info("frps-bulk's proxy list is readable again")
	}
	b.unreadable = rep.Err != nil
	s.reportBulkOrphans(ctx, rep, uids)
}

// reportBulkOrphans is reportOrphans for the second frps: the same rules (HBEP-6, The inventory),
// with the second frps's clock, gate and admissions, and an event whose message names frps-bulk. A
// name that is an orphan on both frps has a clock on each and gets an event for each; a successful
// read of one list that shows the name offline or absent restarts that list's clock only.
func (s *pathState) reportBulkOrphans(ctx context.Context, rep frpsplugin.Report, live []string) {
	b := s.bulk
	if rep.Err != nil {
		return
	}
	now := s.clock()
	next := make(map[string]time.Time, len(b.orphans))
	suspects := make(map[string]time.Time, len(rep.Orphans))
	keep := make(map[string]struct{}, len(rep.Orphans)+len(live))
	for _, uid := range live {
		keep[uid] = struct{}{}
	}
	defer func() { b.suspects = suspects }()
	for name, last := range b.orphans {
		if now.Sub(last) < orphanEventEvery {
			next[name] = last
			keep[name] = struct{}{}
		}
	}
	for _, name := range rep.Orphans {
		keep[name] = struct{}{}
		since, listed := b.suspects[name]
		if !listed {
			since = now
		}
		suspects[name] = since
		if now.Sub(since) < tunnelOrphanGrace {
			continue
		}
		if _, gated := next[name]; gated {
			continue
		}
		s.log.Info("frps-bulk lists an online proxy that is no Instance's UID", "proxy", name)
		s.bulkOrphanEvent(ctx, name)
		next[name] = now
	}
	b.orphans = next
	b.admitted.retain(keep)
}

// bulkOrphanEvent emits one TunnelOrphaned for a proxy on the second frps: the event's message names
// frps-bulk, which is how the two are told apart (HBEP-6, The bulk path), and its regarding object is
// chosen as the first frps's is, from the second plugin's own admissions.
func (s *pathState) bulkOrphanEvent(ctx context.Context, proxy string) {
	subject, how := s.bulkOrphanSubject(ctx, proxy)
	s.rec.Event(subject, nil, lifecycle.EventDegraded, v1alpha1.EventReasonTunnelOrphaned,
		fmt.Sprintf("frps-bulk lists the proxy %s online, and no current Instance has that UID (an orphan, HBEP-6, The bulk path); %s", proxy, how))
}

func (s *pathState) bulkOrphanSubject(ctx context.Context, proxy string) (client.Object, string) {
	if uid, ok := s.bulk.admitted.allocationOf(proxy); ok {
		var list v1alpha1.AllocationList
		if err := s.cached.List(ctx, &list); err != nil {
			s.log.Error(err, "list Allocations for the TunnelOrphaned event of frps-bulk; it regards the lifecycle Lease", "proxy", proxy)
		} else {
			for i := range list.Items {
				if a := &list.Items[i]; string(a.UID) == uid {
					return a, fmt.Sprintf("its bulk tunnel was admitted by the credential of Allocation %s/%s", a.Namespace, a.Name)
				}
			}
		}
	}
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle}}
	var got coordinationv1.Lease
	if err := s.fresh.Get(ctx, client.ObjectKeyFromObject(lease), &got); err != nil {
		s.log.Error(err, "reading the lifecycle Lease for the TunnelOrphaned event of frps-bulk; it is raised without the Lease's UID")
	} else {
		lease.UID = got.UID
	}
	return lease, "no Allocation is known to have admitted its bulk tunnel"
}
