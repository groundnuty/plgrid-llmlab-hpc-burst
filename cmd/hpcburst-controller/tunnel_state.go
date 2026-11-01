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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/frpsplugin"
)

// HBEP-6 gives the read period (every 10 s) and the settle time (about a second after
// each NewProxy and CloseProxy). They are variables so that a test can shorten or lengthen
// them; no test in this package runs in parallel, and each restores what it set once the run it
// started has returned.
var (
	tunnelReadPeriod = 10 * time.Second
	tunnelReadSettle = time.Second
)

const (
	// tunnelWriteAttempts is how often one condition's write is tried against a
	// conflicting writer (the lifecycle reconciler updates the same status).
	tunnelWriteAttempts = 4
	// orphanLogEvery is how often an orphan proxy is named in the log.
	orphanLogEvery = time.Hour
)

// tunnelReads counts the reads of frps's list that tunnel-state has finished
// and written the conditions of. It is for tests, which wait on it.
var tunnelReads atomic.Int64

// tunnelOwner is the field manager of the TunnelConnected writes: the lifecycle
// controller's, since tunnel-state runs in its process and writes what HBEP-7's
// start timeout reads.
var tunnelOwner = client.FieldOwner(v1alpha1.FieldManagerLifecycle)

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
type pathState struct {
	cache   tunnelCache
	cached  client.Reader // the cache: list the Instances
	fresh   client.Reader // the API server: re-read after a conflict
	writer  client.StatusClient
	reader  pathReader
	trigger <-chan struct{}
	log     logr.Logger

	orphans map[string]time.Time // when each orphan proxy was last named in the log
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
	s.logOrphans(rep.Orphans)
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

// apply writes the condition on the Instance when its status or reason differs
// from the stored one. The message is not compared: the Unknown message embeds
// the error's text, which varies per read, and a write at each change of it
// would churn the resourceVersion and keep conflicting with the reconciler.
//
// The write carries the resourceVersion the Instance was read at, as every
// status write does here (a plain merge patch would replace the whole
// conditions array and could drop the reconciler's). On a conflict it re-reads
// the Instance from the API server and decides again on what it finds: the
// Instance may have ended, or already hold the condition.
func (s *pathState) apply(ctx context.Context, in *v1alpha1.Instance, want metav1.Condition) error {
	cur := in
	for range tunnelWriteAttempts {
		if cur.Status.Phase == v1alpha1.InstanceEnded {
			return nil
		}
		have := meta.FindStatusCondition(cur.Status.Conditions, v1alpha1.ConditionTunnelConnected)
		if have != nil && have.Status == want.Status && have.Reason == want.Reason {
			return nil
		}
		next := cur.DeepCopy()
		c := want
		c.ObservedGeneration = cur.Generation
		meta.SetStatusCondition(&next.Status.Conditions, c) // keeps lastTransitionTime while the status holds
		err := s.writer.Status().Update(ctx, next, tunnelOwner)
		switch {
		case err == nil, apierrors.IsNotFound(err):
			return nil
		case !apierrors.IsConflict(err):
			return fmt.Errorf("update Instance %s/%s status: %w", in.Namespace, in.Name, err)
		}
		fresh := &v1alpha1.Instance{}
		switch err := s.fresh.Get(ctx, client.ObjectKeyFromObject(in), fresh); {
		case apierrors.IsNotFound(err):
			return nil
		case err != nil:
			return fmt.Errorf("re-read Instance %s/%s: %w", in.Namespace, in.Name, err)
		}
		if fresh.UID != in.UID {
			return nil // the name belongs to another Instance now; its UID is not the one read
		}
		cur = fresh
	}
	return fmt.Errorf("update Instance %s/%s status: still conflicting after %d attempts; the next read retries", in.Namespace, in.Name, tunnelWriteAttempts)
}

// logOrphans names an online proxy that is no Instance's UID (HBEP-6: an
// orphan), at most once an hour each. The event HBEP-6 proposes for it has no
// reason in api/ yet.
func (s *pathState) logOrphans(names []string) {
	now := time.Now()
	next := make(map[string]time.Time, len(names))
	for _, n := range names {
		last, seen := s.orphans[n]
		if !seen || now.Sub(last) >= orphanLogEvery {
			s.log.Info("frps lists an online proxy that is no Instance's UID", "proxy", n)
			last = now
		}
		next[n] = last
	}
	s.orphans = next
}
