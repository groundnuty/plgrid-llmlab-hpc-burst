package providerctl

import (
	"context"
	"errors"
	"fmt"
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
// the time of the last successful queue read, Ready, DialectMismatch,
// PathUnresolved, CredentialsInvalid and DialHomeKeyMissing. The queue waits
// are written by the observe that first sees a job started (recordQueueWait);
// both write with a resourceVersion precondition and re-read on a conflict. It keeps every condition it does not own, such as the
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
	var err error
	for i := 0; i < maxStatusWriteRetries; i++ {
		var prov v1alpha1.Provider
		if err = l.s.reader.Get(ctx, client.ObjectKey{Name: l.s.o.Provider}, &prov); err != nil {
			return fmt.Errorf("get Provider %s: %w", l.s.o.Provider, err)
		}
		old := prov.DeepCopy()
		l.compute(&prov, health, cfgErr, pollErr)
		if apiequality.Semantic.DeepEqual(old.Status, prov.Status) {
			return nil
		}
		err = l.s.kube.Status().Update(ctx, &prov)
		if err == nil || !kerrors.IsConflict(err) {
			break
		}
	}
	if err != nil {
		return fmt.Errorf("update Provider %s status: %w", l.s.o.Provider, err)
	}
	return nil
}

// compute sets the status fields this controller owns, keeping the queue
// waits and every condition it does not own as they were read.
func (l *statusLoop) compute(prov *v1alpha1.Provider, h slurm.Health, cfgErr, pollErr error) {
	now := l.s.o.Clock.Now()
	stamp := *mt(now)
	st := &prov.Status
	st.ObservedGeneration = prov.Generation
	st.LastReconcileTime = &stamp
	if !h.LastListing.IsZero() {
		st.LastSuccessfulStatusRead = mt(h.LastListing)
	}
	set := func(t string, status metav1.ConditionStatus, reason, msg string) {
		apimeta.SetStatusCondition(&st.Conditions, metav1.Condition{
			Type: t, Status: status, Reason: reason, Message: msg, ObservedGeneration: prov.Generation, LastTransitionTime: stamp,
		})
	}

	// Ready (docs/development.md, Provider readiness): True with Polled after
	// a pass whose reads succeeded; after a failed pass, False with
	// ReadsFailing once the last success is older than ProviderReadyStaleness
	// or there is none, else unchanged. The message names the time of the last
	// success, not its age, so it is the same from one pass to the next.
	failed := cfgErr != nil || pollErr != nil || h.ListingFailed || h.LastListing.IsZero()
	switch {
	case !failed:
		set(v1alpha1.ConditionProviderReady, metav1.ConditionTrue, v1alpha1.ReasonPolled, "")
	case h.LastListing.IsZero() || now.Sub(h.LastListing) > v1alpha1.ProviderReadyStaleness:
		msg := "no queue read has succeeded yet"
		if !h.LastListing.IsZero() {
			msg = "the last successful queue read was at " + h.LastListing.UTC().Format(time.RFC3339)
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
