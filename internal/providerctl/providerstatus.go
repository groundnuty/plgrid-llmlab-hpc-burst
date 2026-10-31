package providerctl

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Reasons of the Provider conditions this controller writes.
const (
	ReasonPolled          = "Polled"
	ReasonReadsFailing    = "ReadsFailing"
	ReasonNotPolledYet    = "NotPolledYet"
	ReasonInvalidProfile  = "InvalidProfile"
	reasonMatches         = "Matches"
	reasonMismatch        = "Mismatch"
	reasonResolved        = "Resolved"
	reasonUnresolved      = "Unresolved"
	reasonPresent         = "Present"
	reasonMissing         = "Missing"
	reasonCredentialsOK   = "Accepted"
	reasonCredentialsBad  = "Refused"
	staleIntervals        = 3
	maxStatusWriteRetries = 3
)

// waitSink holds the job IDs whose queue wait is still to be written to the
// Provider's status. An ID stays until a status write that carries its wait
// succeeds, so a failed accounting read or a failed write is retried on the
// next sync, and an ID written once is never added again (HBEP-4, Provider
// status: a repeated observe adds nothing).
type waitSink struct {
	mu      sync.Mutex
	pending map[string]bool
	written map[string]bool
}

func newWaitSink() *waitSink {
	return &waitSink{pending: map[string]bool{}, written: map[string]bool{}}
}

func (w *waitSink) add(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.written[id] {
		w.pending[id] = true
	}
}

func (w *waitSink) list() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, len(w.pending))
	for id := range w.pending {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func (w *waitSink) done(ids []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, id := range ids {
		delete(w.pending, id)
		w.written[id] = true
	}
}

// statusLoop is the single writer of the Provider's status (docs/development.md,
// Single writer): the queue waits, the time of the last successful queue read,
// Ready, DialectMismatch, PathUnresolved, CredentialsInvalid and
// DialHomeKeyMissing. It keeps every condition it does not own, such as the
// lifecycle controller's DialHomeUnreachable. It also polls the queue listing
// once per interval, so the listing is read even with no Allocation.
type statusLoop struct {
	s *shared
}

// NeedLeaderElection implements manager.LeaderElectionRunnable: only the
// leader writes.
func (l *statusLoop) NeedLeaderElection() bool { return true }

// Start implements manager.Runnable.
func (l *statusLoop) Start(ctx context.Context) error {
	t := time.NewTicker(l.s.o.PollInterval)
	defer t.Stop()
	for {
		if err := l.sync(ctx); err != nil && ctx.Err() == nil {
			l.s.o.Log.Info("cannot write the Provider's status", "error", err)
		}
		select {
		case <-ctx.Done():
			l.s.h.close()
			return nil
		case <-t.C:
		}
	}
}

// sync polls the platform, resolves pending queue waits, and writes the
// Provider's status when it changed.
func (l *statusLoop) sync(ctx context.Context) error {
	_, p, cfgErr := l.s.h.get(ctx)
	var waits []v1alpha1.QueueWait
	var waitIDs []string
	if cfgErr == nil {
		_ = p.Poll(ctx) // its outcome shows in Health
		for _, id := range l.s.waits.list() {
			submit, start, ok, err := p.QueueWait(ctx, id)
			if err != nil || !ok {
				continue // retried on the next sync
			}
			waits = append(waits, v1alpha1.QueueWait{JobID: id, SubmitTime: *mt(submit), StartTime: *mt(start)})
			waitIDs = append(waitIDs, id)
		}
	}
	var health slurm.Health
	if p != nil {
		health = p.Health()
	}
	var err error
	for i := 0; i < maxStatusWriteRetries; i++ {
		var prov v1alpha1.Provider
		if err = l.s.reader.Get(ctx, client.ObjectKey{Name: l.s.o.Provider}, &prov); err != nil {
			return fmt.Errorf("get Provider %s: %w", l.s.o.Provider, err)
		}
		old := prov.DeepCopy()
		l.compute(&prov, health, cfgErr, waits)
		if apiequality.Semantic.DeepEqual(old.Status, prov.Status) {
			l.s.waits.done(waitIDs)
			return nil
		}
		err = l.s.kube.Status().Update(ctx, &prov)
		if err == nil {
			l.s.waits.done(waitIDs)
			return nil
		}
		if !kerrors.IsConflict(err) {
			return fmt.Errorf("update Provider %s status: %w", l.s.o.Provider, err)
		}
	}
	return err
}

// compute sets the status fields this controller owns.
func (l *statusLoop) compute(prov *v1alpha1.Provider, h slurm.Health, cfgErr error, waits []v1alpha1.QueueWait) {
	now := *mt(l.s.o.Clock.Now())
	st := &prov.Status
	sort.SliceStable(waits, func(i, k int) bool { return waits[i].StartTime.Before(&waits[k].StartTime) })
	for _, w := range waits {
		st.QueueWaits = v1alpha1.AddQueueWait(st.QueueWaits, w)
	}
	st.ObservedGeneration = prov.Generation
	st.LastReconcileTime = &now
	if !h.LastListing.IsZero() {
		st.LastSuccessfulStatusRead = mt(h.LastListing)
	}
	set := func(t string, status metav1.ConditionStatus, reason, msg string) {
		apimeta.SetStatusCondition(&st.Conditions, metav1.Condition{
			Type: t, Status: status, Reason: reason, Message: msg, ObservedGeneration: prov.Generation, LastTransitionTime: now,
		})
	}

	var invalid *provider.InvalidProfileError
	switch {
	case errors.As(cfgErr, &invalid):
		set(v1alpha1.ConditionProviderReady, metav1.ConditionFalse, ReasonInvalidProfile, truncate(invalid.Error()))
	case errors.Is(cfgErr, provider.ErrCredentials):
		set(v1alpha1.ConditionProviderReady, metav1.ConditionFalse, reasonCredentialsBad, truncate(cfgErr.Error()))
	case h.LastListing.IsZero():
		set(v1alpha1.ConditionProviderReady, metav1.ConditionFalse, ReasonNotPolledYet, "no queue read has succeeded yet")
	case l.s.o.Clock.Now().Sub(h.LastListing) > staleIntervals*l.s.o.PollInterval:
		set(v1alpha1.ConditionProviderReady, metav1.ConditionFalse, ReasonReadsFailing,
			fmt.Sprintf("the last successful queue read is older than %s", staleIntervals*l.s.o.PollInterval))
	case h.ListingFailed:
		// Within the bound a failed read leaves Ready as it was; the age of the
		// last successful read keeps growing.
	default:
		set(v1alpha1.ConditionProviderReady, metav1.ConditionTrue, ReasonPolled, "")
	}

	if errors.Is(cfgErr, provider.ErrCredentials) {
		set(v1alpha1.ConditionCredentialsInvalid, metav1.ConditionTrue, reasonCredentialsBad, truncate(cfgErr.Error()))
	} else if cfgErr == nil {
		set(v1alpha1.ConditionCredentialsInvalid, metav1.ConditionFalse, reasonCredentialsOK, "")
	}
	if cfgErr != nil {
		return
	}
	if h.DialectMismatch != "" {
		set(v1alpha1.ConditionDialectMismatch, metav1.ConditionTrue, reasonMismatch,
			fmt.Sprintf("a reply carries data_parser %s; the profile says otherwise, so replies are not interpreted", h.DialectMismatch))
	} else if !h.LastListing.IsZero() {
		set(v1alpha1.ConditionDialectMismatch, metav1.ConditionFalse, reasonMatches, "")
	}
	if h.PathUnresolved != nil {
		set(v1alpha1.ConditionPathUnresolved, metav1.ConditionTrue, reasonUnresolved, truncate(h.PathUnresolved.Error()))
	} else if apimeta.FindStatusCondition(st.Conditions, v1alpha1.ConditionPathUnresolved) != nil {
		set(v1alpha1.ConditionPathUnresolved, metav1.ConditionFalse, reasonResolved, "")
	}
	if m := h.DialHomeKeyMissing; m != nil {
		if *m {
			set(v1alpha1.ConditionDialHomeKeyMissing, metav1.ConditionTrue, reasonMissing,
				"a dial-home key file is missing in ~/"+agentapi.DialHomeDir+" on the platform; nothing is submitted in ssh mode")
		} else {
			set(v1alpha1.ConditionDialHomeKeyMissing, metav1.ConditionFalse, reasonPresent, "")
		}
	}
}
