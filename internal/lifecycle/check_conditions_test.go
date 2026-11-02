package lifecycle

// The status of an Instance's Verified, Qualified and WarmedUp conditions (issue #243; api/v1alpha1/conditions.go,
// ReasonPending and ReasonFailed): a check that has not run yet is Unknown, Kubernetes' "not yet checked", and a check
// that failed is False. Every reader of the three goes through isTrue (writes.go), so Unknown and False both gate:
// the Instance is ready only with all three True. The expected statuses are the contract's, written out here.

import (
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

var checkConditions = []string{v1alpha1.ConditionVerified, v1alpha1.ConditionQualified, v1alpha1.ConditionWarmedUp}

// wantCheck fails unless the Instance's condition has the status and the reason.
func wantCheck(t *testing.T, when string, in *v1alpha1.Instance, typ string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	c := apimeta.FindStatusCondition(in.Status.Conditions, typ)
	if c == nil {
		t.Errorf("%s: %s is absent, want %s/%s", when, typ, status, reason)
		return
	}
	if c.Status != status || c.Reason != reason {
		t.Errorf("%s: %s is %s/%s (%s), want %s/%s", when, typ, c.Status, c.Reason, c.Message, status, reason)
	}
}

// pendingInstance steps a healthy rig until an Instance has registered and its checks are still to run, and returns
// the rig with that Instance as it stood then. It waits on the conditions, not on the clock: the first Step in which an
// Instance shows a Verified condition with the reason Pending.
func pendingInstance(t *testing.T) (*rig, *v1alpha1.Instance) {
	t.Helper()
	rg := newRig(t)
	rg.create(mdSpec("md"))
	var found *v1alpha1.Instance
	rg.until(10*time.Minute, 10*time.Second, "an Instance with its checks pending", func() bool {
		for _, in := range rg.instances("md") {
			if apimeta.FindStatusCondition(in.Status.Conditions, v1alpha1.ConditionVerified) != nil {
				found = in.DeepCopy()
				return true
			}
		}
		return false
	})
	return rg, found
}

// A check not yet run is Unknown with the reason Pending, for each of the three, and a check that passed is True with
// Passed: the Instance gets ready only with all three, so Unknown gates.
func TestAPendingCheckIsUnknownAndAPassedOneTrue(t *testing.T) {
	rg, in := pendingInstance(t)
	for _, c := range checkConditions {
		wantCheck(t, "after registration", in, c, metav1.ConditionUnknown, v1alpha1.ReasonPending)
		if got := apimeta.FindStatusCondition(in.Status.Conditions, c); got != nil && got.Message != "not yet checked" {
			t.Errorf("%s message %q, want %q", c, got.Message, "not yet checked")
		}
		if isTrue(in.Status.Conditions, c) {
			t.Errorf("%s counts as true while pending", c)
		}
	}
	if in.Status.Phase == v1alpha1.InstanceReady {
		t.Fatalf("the Instance is ready with its checks pending")
	}
	rg.until(10*time.Minute, 10*time.Second, "a ready Instance", func() bool { return rg.readyCount("md") == 1 })
	got := rg.instance(in.Name)
	for _, c := range checkConditions {
		wantCheck(t, "when ready", got, c, metav1.ConditionTrue, v1alpha1.ReasonPassed)
	}
}

// Instances written by the controller before this change hold Pending as False. They go on as before: every reader asks
// whether a condition is True, so a stored False and an Unknown are the same to it, and such an Instance verifies,
// qualifies, warms up and becomes ready.
func TestAnInstanceWithItsChecksPendingAsFalseStillGetsReady(t *testing.T) {
	rg, in := pendingInstance(t)
	old := rg.instance(in.Name)
	for i := range old.Status.Conditions {
		for _, c := range checkConditions {
			if old.Status.Conditions[i].Type == c && old.Status.Conditions[i].Reason == v1alpha1.ReasonPending {
				old.Status.Conditions[i].Status = metav1.ConditionFalse
			}
		}
	}
	if err := rg.c.Status().Update(rg.ctx, old); err != nil {
		t.Fatal(err)
	}
	for _, c := range checkConditions {
		wantCheck(t, "after the rewrite", rg.instance(in.Name), c, metav1.ConditionFalse, v1alpha1.ReasonPending)
	}
	rg.until(10*time.Minute, 10*time.Second, "a ready Instance", func() bool { return rg.readyCount("md") == 1 })
	for _, c := range checkConditions {
		wantCheck(t, "when ready", rg.instance(in.Name), c, metav1.ConditionTrue, v1alpha1.ReasonPassed)
	}
}

// A check that runs again after a restart is Unknown with the reason of the restart (api/v1alpha1/conditions.go,
// ReasonWorkerRestart and ReasonInstanceRestart), for the checks the restart repeats: a worker restart repeats
// verification and warm-up and keeps Qualified; a whole-Instance restart repeats all three. The Instance is held at the
// restart (the first Step in which a condition carries the reason), and goes on to ready with all three True.
func TestACheckRunningAgainAfterARestartIsUnknownWithTheRestartsReason(t *testing.T) {
	// The ladder's first rung is the worker's, the second the whole Instance's; one attempt per rung, as health_test.go
	// sets it, so that the second crash climbs.
	rg := newRig(t, withProfile(func(p *v1alpha1.PlatformProfileSpec) {
		p.Restart = &v1alpha1.RestartPolicy{AttemptsPerRung: ptrTo(int32(1))}
	}))
	rg.create(mdSpec("md"))
	in, alloc := startReady(rg)
	for _, tc := range []struct {
		reason            string
		qualifiedRepeated bool
	}{{v1alpha1.ReasonWorkerRestart, false}, {v1alpha1.ReasonInstanceRestart, true}} {
		rg.agentOf(alloc).agent.CrashWorker(healthWorker, 1)
		var held *v1alpha1.Instance
		verifRunUntil(rg, 3*time.Minute, "the "+tc.reason, func() bool {
			got := rg.instance(in.Name)
			if conditionReason(got, v1alpha1.ConditionVerified) == tc.reason {
				held = got
				return true
			}
			return false
		})
		wantCheck(t, "at the "+tc.reason, held, v1alpha1.ConditionVerified, metav1.ConditionUnknown, tc.reason)
		wantCheck(t, "at the "+tc.reason, held, v1alpha1.ConditionWarmedUp, metav1.ConditionUnknown, tc.reason)
		if tc.qualifiedRepeated {
			wantCheck(t, "at the "+tc.reason, held, v1alpha1.ConditionQualified, metav1.ConditionUnknown, tc.reason)
		} else {
			wantCheck(t, "at the "+tc.reason, held, v1alpha1.ConditionQualified, metav1.ConditionTrue, v1alpha1.ReasonPassed)
		}
		if held.Status.Phase == v1alpha1.InstanceReady {
			t.Errorf("at the %s the Instance is still ready", tc.reason)
		}
		verifRunUntil(rg, 10*time.Minute, "ready after the "+tc.reason, func() bool { return rg.readyCount("md") == 1 })
		for _, c := range checkConditions {
			wantCheck(t, "ready after the "+tc.reason, rg.instance(in.Name), c, metav1.ConditionTrue, v1alpha1.ReasonPassed)
		}
	}
}

// isTrue, the one reader of the three: only True is true.
func TestOnlyTrueCountsAsATrueCheck(t *testing.T) {
	for _, tc := range []struct {
		status metav1.ConditionStatus
		want   bool
	}{{metav1.ConditionTrue, true}, {metav1.ConditionFalse, false}, {metav1.ConditionUnknown, false}} {
		conds := []metav1.Condition{{Type: v1alpha1.ConditionVerified, Status: tc.status}}
		if got := isTrue(conds, v1alpha1.ConditionVerified); got != tc.want {
			t.Errorf("isTrue(%s) = %t, want %t", tc.status, got, tc.want)
		}
	}
}

// A failed check is False with the reason Failed, on the condition that failed, and the checks after it are still
// pending (Unknown): verification failing on a contradiction, qualification failing on the KV capacity, and warm-up
// failing because the latency never settles. The Instance ends in each case, and never was ready.
func TestAFailedCheckIsFalseAndTheNextOnesStayPending(t *testing.T) {
	for _, tc := range []struct {
		name      string
		eng       afake.Engine
		endReason string
		want      [3]struct {
			status metav1.ConditionStatus
			reason string
		}
	}{
		{"verification", afake.Engine{Verify: afake.VerifyMaxModelLen("16384")}, v1alpha1.ReasonVerificationFailed, [3]struct {
			status metav1.ConditionStatus
			reason string
		}{{metav1.ConditionFalse, v1alpha1.ReasonFailed}, {metav1.ConditionUnknown, v1alpha1.ReasonPending}, {metav1.ConditionUnknown, v1alpha1.ReasonPending}}},
		{"qualification", afake.Engine{Qualify: afake.QualifyKVCapacity(16383)}, v1alpha1.ReasonQualificationFailed, [3]struct {
			status metav1.ConditionStatus
			reason string
		}{{metav1.ConditionTrue, v1alpha1.ReasonPassed}, {metav1.ConditionFalse, v1alpha1.ReasonFailed}, {metav1.ConditionUnknown, v1alpha1.ReasonPending}}},
		{"warm-up", afake.Engine{WarmUpRound: afake.WarmUpNeverSettles}, v1alpha1.ReasonQualificationFailed, [3]struct {
			status metav1.ConditionStatus
			reason string
		}{{metav1.ConditionTrue, v1alpha1.ReasonPassed}, {metav1.ConditionTrue, v1alpha1.ReasonPassed}, {metav1.ConditionFalse, v1alpha1.ReasonFailed}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg, in := verifRig(t, func() afake.Engine { return tc.eng })
			st, everReady := verifEnded(rg, in.Name)
			if everReady || st.EndReason != tc.endReason {
				t.Fatalf("ever ready %v, end reason %q, want never ready and %s", everReady, st.EndReason, tc.endReason)
			}
			got := rg.instance(in.Name)
			for i, c := range checkConditions {
				wantCheck(t, "after the "+tc.name+" failed", got, c, tc.want[i].status, tc.want[i].reason)
			}
			if tc.name != "verification" {
				// The failing condition says why, in the words of the event.
				if c := apimeta.FindStatusCondition(got.Status.Conditions, checkConditions[map[string]int{"qualification": 1, "warm-up": 2}[tc.name]]); c == nil || c.Message == "" {
					t.Errorf("the failed condition holds no message: %+v", c)
				}
			}
		})
	}
}
