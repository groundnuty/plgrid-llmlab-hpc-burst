package providerctl

import (
	"context"
	"errors"
	"fmt"
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

// Reasons of the Provider conditions this controller writes besides Ready,
// whose reasons are v1alpha1.ReasonPolled and v1alpha1.ReasonReadsFailing.
const (
	reasonMatches         = "Matches"
	reasonMismatch        = "Mismatch"
	reasonResolved        = "Resolved"
	reasonUnresolved      = "Unresolved"
	reasonPresent         = "Present"
	reasonMissing         = "Missing"
	reasonCredentialsOK   = "Accepted"
	reasonCredentialsBad  = "Refused"
	maxStatusWriteRetries = 3
)

// statusLoop writes the Provider's status (docs/development.md, Single writer):
// the time of the last successful status read, Ready, DialectMismatch,
// PathUnresolved, CredentialsInvalid and DialHomeKeyMissing. The queue waits
// are written by the observe that first sees a job started (recordQueueWait),
// and DialHomeKeyMissing also by Create (writeDialHomeKeyMissing); all write
// with a resourceVersion precondition and re-read on a conflict. It keeps every
// condition it does not own, such as the lifecycle controller's
// DialHomeUnreachable. It also polls the queue listing once per interval, so
// the listing is read even with no Allocation.
type statusLoop struct {
	s *shared

	// failures is the platform's Health.ReadFailures at the last pass that
	// was written: a pass that sees it changed had a failed read.
	mu       sync.Mutex
	failures uint64
}

// statusRefresh is how stale lastSuccessfulStatusRead may get before a pass
// that changes nothing else writes it again: well under
// v1alpha1.ProviderReadyStaleness, so ProviderReady stays true, and above the
// poll interval, so a healthy Provider is not rewritten at every pass (review
// of PR 76: Opus N3).
const statusRefresh = v1alpha1.ProviderPollInterval

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

// sync polls the platform and writes the Provider's status when it changed.
// The poll is every PollInterval, within v1alpha1.ProviderPollInterval, so
// Ready always rests on a real read (docs/development.md, Provider readiness).
func (l *statusLoop) sync(ctx context.Context) error {
	_, p, cfgErr := l.s.h.get(ctx)
	var pollErr error
	var health slurm.Health
	if cfgErr == nil {
		pollErr = p.Poll(ctx)
		health = p.Health()
	} else if p, _ := l.s.h.current(); p != nil {
		health = p.Health()
	}
	l.mu.Lock()
	readsFailed := health.ReadFailures != l.failures // != : a new Batch counts from 0
	l.mu.Unlock()
	var err error
	for i := 0; i < maxStatusWriteRetries; i++ {
		var prov v1alpha1.Provider
		if err = l.s.reader.Get(ctx, client.ObjectKey{Name: l.s.o.Provider}, &prov); err != nil {
			return fmt.Errorf("get Provider %s: %w", l.s.o.Provider, err)
		}
		if !l.compute(&prov, health, cfgErr, pollErr, readsFailed) {
			break
		}
		err = l.s.kube.Status().Update(ctx, &prov)
		if err == nil || !kerrors.IsConflict(err) {
			break
		}
	}
	if err != nil {
		return fmt.Errorf("update Provider %s status: %w", l.s.o.Provider, err)
	}
	l.mu.Lock()
	l.failures = health.ReadFailures
	l.mu.Unlock()
	return nil
}

// compute sets the status fields this controller owns, keeping the queue
// waits and every condition it does not own as they were read, and reports
// whether the status changed. The pass failed when the profile, the poll, the
// latest listing read, or any required status read since the last pass failed
// (readsFailed: an accounting read counts, Astra B7), as pkg/provider/fake's
// Controller judges a pass.
func (l *statusLoop) compute(prov *v1alpha1.Provider, h slurm.Health, cfgErr, pollErr error, readsFailed bool) bool {
	now := l.s.o.Clock.Now()
	stamp := *mt(now)
	st := &prov.Status
	old := st.DeepCopy()
	st.ObservedGeneration = prov.Generation
	set := func(t string, status metav1.ConditionStatus, reason, msg string) {
		apimeta.SetStatusCondition(&st.Conditions, metav1.Condition{
			Type: t, Status: status, Reason: reason, Message: msg, ObservedGeneration: prov.Generation, LastTransitionTime: stamp,
		})
	}

	// Ready (docs/development.md, Provider readiness): True with Polled after
	// a pass whose reads succeeded; after a failed pass, False with
	// ReadsFailing once the stored last success is older than
	// ProviderReadyStaleness or there is none, else unchanged. The stored time
	// is the judge: the listing can succeed while accounting fails. The
	// message names the time, not its age, so it is the same at every pass.
	failed := cfgErr != nil || pollErr != nil || h.ListingFailed || h.LastListing.IsZero() || readsFailed
	last := st.LastSuccessfulStatusRead
	switch {
	case !failed:
		set(v1alpha1.ConditionProviderReady, metav1.ConditionTrue, v1alpha1.ReasonPolled, "")
	case last == nil || now.Sub(last.Time) > v1alpha1.ProviderReadyStaleness:
		msg := "no status read has succeeded yet"
		if last != nil {
			msg = "the last successful status read was at " + last.UTC().Format(time.RFC3339)
		}
		var invalid *provider.InvalidProfileError
		switch {
		case errors.As(cfgErr, &invalid):
			msg += "; no read is made: " + truncate(invalid.Error())
		case errors.Is(cfgErr, provider.ErrCredentials):
			msg += "; no read is made: the credentials are refused"
		}
		set(v1alpha1.ConditionProviderReady, metav1.ConditionFalse, v1alpha1.ReasonReadsFailing, msg)
	}
	l.conditions(st, set, h, cfgErr)

	changed := !apiequality.Semantic.DeepEqual(*old, *st)
	if !failed {
		read := mt(h.LastListing)
		if last == nil || changed || read.Sub(last.Time) >= statusRefresh {
			st.LastSuccessfulStatusRead = read
			changed = changed || !apiequality.Semantic.DeepEqual(last, read)
		}
	}
	if changed {
		st.LastReconcileTime = &stamp
	}
	return changed
}

// conditions sets CredentialsInvalid, DialectMismatch, PathUnresolved and
// DialHomeKeyMissing from the profile's error and the platform's health.
func (l *statusLoop) conditions(st *v1alpha1.ProviderStatus, set func(string, metav1.ConditionStatus, string, string), h slurm.Health, cfgErr error) {
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
		setDialHomeKeyMissing(set, *m)
	}
}

// setDialHomeKeyMissing sets DialHomeKeyMissing from the latest key check.
func setDialHomeKeyMissing(set func(string, metav1.ConditionStatus, string, string), missing bool) {
	if missing {
		set(v1alpha1.ConditionDialHomeKeyMissing, metav1.ConditionTrue, reasonMissing,
			"a dial-home key file is missing in ~/"+agentapi.DialHomeDir+" on the platform; nothing is submitted in ssh mode")
		return
	}
	set(v1alpha1.ConditionDialHomeKeyMissing, metav1.ConditionFalse, reasonPresent, "")
}

// writeDialHomeKeyMissing writes DialHomeKeyMissing from Create's key check at
// once (HBEP-4, Two controllers: Create "sets DialHomeKeyMissing when they do
// not" exist), not at the status loop's next pass (review of PR 76: Opus N2).
// An unchanged condition costs no write; a conflict re-reads.
func (s *shared) writeDialHomeKeyMissing(ctx context.Context, missing bool) error {
	var err error
	for i := 0; i < maxStatusWriteRetries; i++ {
		var prov v1alpha1.Provider
		if err = s.reader.Get(ctx, client.ObjectKey{Name: s.o.Provider}, &prov); err != nil {
			return fmt.Errorf("get Provider %s: %w", s.o.Provider, err)
		}
		old := prov.Status.DeepCopy()
		stamp := *mt(s.o.Clock.Now())
		setDialHomeKeyMissing(func(t string, status metav1.ConditionStatus, reason, msg string) {
			apimeta.SetStatusCondition(&prov.Status.Conditions, metav1.Condition{
				Type: t, Status: status, Reason: reason, Message: msg, ObservedGeneration: prov.Generation, LastTransitionTime: stamp,
			})
		}, missing)
		if apiequality.Semantic.DeepEqual(*old, prov.Status) {
			return nil
		}
		if err = s.kube.Status().Update(ctx, &prov); err == nil || !kerrors.IsConflict(err) {
			break
		}
	}
	if err != nil {
		return fmt.Errorf("write DialHomeKeyMissing on Provider %s: %w", s.o.Provider, err)
	}
	return nil
}
