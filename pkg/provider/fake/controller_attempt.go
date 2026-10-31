package fake

import (
	"context"
	"fmt"
	"strconv"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The retry rules for an attempt whose outcome is unknown (HBEP-7, open
// question 11, Hold mode, Fallback mode and The operator condition). They run
// only when no job carrying the tag was found.

// attemptTime is when the attempt was made: its record's time, or, for a record
// with none, the Allocation's creation.
func attemptTime(s *allocState) time.Time {
	if s.rec.Time != nil {
		return s.rec.Time.Time
	}
	return s.a.CreationTimestamp.Time
}

// stall applies the rules to an InFlight or Unknown attempt with no job found.
// It returns resubmit=true when the pass goes on to claim the next attempt;
// otherwise it has set the observation's phase.
func (c *Controller) stall(ctx context.Context, p *pass, s *allocState) (resubmit bool, err error) {
	age := p.now.Sub(attemptTime(s))
	pastWindow := age > c.inflightWindow()
	if c.HoldRelease {
		// The attempt is dropped: n+1 replaces its record. A late job of the
		// attempt lands held, and settle cancels it.
		if pastWindow {
			return true, nil
		}
		showAttempt(s)
		return false, nil
	}
	if s.rec.State == v1alpha1.SubmissionInFlight {
		if !pastWindow {
			showAttempt(s)
			return false, nil
		}
		unknown := *s.rec
		unknown.State, unknown.Time = v1alpha1.SubmissionUnknown, mt(attemptTime(s))
		unknown.Reason = "no job found after the in-flight window"
		if err := c.writeRecord(ctx, s, unknown); err != nil {
			return false, err
		}
	}
	// The attempt is Unknown: only the tag or an operator resolves it.
	if abandonRequested(s) {
		n := s.rec.Attempt
		if err := c.writeRecord(ctx, s, c.newRecord(n, v1alpha1.SubmissionAbandoned, "abandoned by an operator", p.now)); err != nil {
			return false, err
		}
		s.markResolved(p.now, fmt.Sprintf("attempt %d abandoned by an operator", n))
		s.obs.Phase = v1alpha1.AllocationSubmitting
		return false, nil
	}
	if age > c.unknownBound() {
		s.conds = append(s.conds, needsOperator(s.rec.Attempt, c.unknownBound(), p.now))
	}
	showAttempt(s)
	return false, nil
}

// showAttempt sets the observation for an attempt that is being waited on: an
// InFlight one is Submitting, an Unknown one SubmissionUnknown.
func showAttempt(s *allocState) {
	s.obs.Submission = s.rec
	if s.rec.State == v1alpha1.SubmissionInFlight {
		s.obs.Phase = v1alpha1.AllocationSubmitting
		return
	}
	s.obs.Phase = v1alpha1.AllocationSubmissionUnknown
}

// abandonRequested reports whether the abandon-attempt annotation names the
// attempt: its number in decimal. Any other value is ignored.
func abandonRequested(s *allocState) bool {
	v, ok := s.a.Annotations[v1alpha1.AnnotationAbandonAttempt]
	return ok && v == strconv.Itoa(int(s.rec.Attempt))
}

// attemptReleased reports whether a deleting Allocation may let go of its
// unresolved attempt, which has no job: in hold mode once the in-flight window
// has passed (a job that registers later is a held orphan), in fallback mode
// when an operator abandoned the attempt.
func (c *Controller) attemptReleased(s *allocState, now time.Time) bool {
	if c.HoldRelease {
		return now.Sub(attemptTime(s)) > c.inflightWindow()
	}
	return abandonRequested(s)
}

// needsOperator is the SubmissionResolved condition of an attempt that is still
// Unknown after the Unknown bound. Its message carries no time, so the
// condition does not change from one pass to the next.
func needsOperator(attempt int32, bound time.Duration, now time.Time) xpv2.Condition {
	return xpv2.Condition{
		Type:               v1alpha1.ConditionSubmissionResolved,
		Status:             corev1.ConditionFalse,
		Reason:             v1alpha1.ReasonNeedsOperator,
		LastTransitionTime: metav1.NewTime(now),
		Message: fmt.Sprintf("attempt %d is still unknown after %s: check the platform, then set the %s annotation to %d",
			attempt, bound, v1alpha1.AnnotationAbandonAttempt, attempt),
	}
}

// markResolved sets SubmissionResolved True with reason Resolved, but only when
// the condition had been raised (it is False): a condition that was never
// raised is not written. The condition stays once it is True.
func (s *allocState) markResolved(now time.Time, message string) {
	if s.a.GetCondition(v1alpha1.ConditionSubmissionResolved).Status != corev1.ConditionFalse {
		return
	}
	s.conds = append(s.conds, xpv2.Condition{
		Type:               v1alpha1.ConditionSubmissionResolved,
		Status:             corev1.ConditionTrue,
		Reason:             v1alpha1.ReasonResolved,
		LastTransitionTime: metav1.NewTime(now),
		Message:            message,
	})
}
