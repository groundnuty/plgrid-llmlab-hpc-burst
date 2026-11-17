package lifecycle

// The gate's verdict on the start-up outcome, through the pass (HBEP-7, How k adjusts, Recorded; Test plan, Outcomes and
// the rate; #616, PR 2): a pass carries Passed with the entry's date, an Instance judged with no entry carries
// RecordOnly and no date, a gate failure is a Failed QualificationFailed outcome carrying Missed with the date.
// outcomes_test.go holds the classifier's rows on hand-built Instances.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// lastOutcome is the newest outcome of the class in the deployment's status.
func lastOutcome(t *testing.T, rg *rig) v1alpha1.StartOutcome {
	t.Helper()
	outs := rg.md("md").Status.StartOutcomes
	if len(outs) != 1 || len(outs[0].Outcomes) == 0 {
		t.Fatalf("outcomes %+v, want the class's", outs)
	}
	return outs[0].Outcomes[len(outs[0].Outcomes)-1]
}

func TestTheOutcomeCarriesWhatTheGateJudged(t *testing.T) {
	ms := time.Millisecond
	t.Run("a pass", func(t *testing.T) {
		rg, _ := gateReady(t, 39*ms, gateMD(), oneEntry())
		rg.run(time.Minute, 10*time.Second)
		if o := lastOutcome(t, rg); o.Result != v1alpha1.StartOutcomeReady || o.Gate != v1alpha1.GatePassed || o.ReferenceDate != "2026-10-23" {
			t.Errorf("outcome %+v, want Ready with gate Passed against 2026-10-23", o)
		}
	})
	t.Run("no entry", func(t *testing.T) {
		rg, _ := gateReady(t, 41*ms, gateMD(), func(string) []v1alpha1.ExpectedPerformance { return nil })
		rg.run(time.Minute, 10*time.Second)
		if o := lastOutcome(t, rg); o.Result != v1alpha1.StartOutcomeReady || o.Gate != v1alpha1.GateRecordOnly || o.ReferenceDate != "" {
			t.Errorf("outcome %+v, want Ready with gate RecordOnly and no date", o)
		}
	})
	t.Run("a gate failure", func(t *testing.T) {
		rg := newRig(t)
		rg.engine = gateSequence(41 * ms)
		name, _ := gateEntries(rg, gateMD(), oneEntry())
		startUntil(rg, "md", 10*time.Minute, "the Instance failed", func() bool { return ended(rg.instance(name)) })
		rg.run(time.Minute, 10*time.Second)
		if o := lastOutcome(t, rg); o.Result != v1alpha1.StartOutcomeFailed || o.Reason != v1alpha1.ReasonQualificationFailed || o.Gate != v1alpha1.GateMissed || o.ReferenceDate != "2026-10-23" {
			t.Errorf("outcome %+v, want Failed, QualificationFailed, gate Missed against 2026-10-23", o)
		}
		if outs := rg.md("md").Status.StartOutcomes[0]; outs.Failures != 1 || outs.QualificationFailures != 1 || outs.Count != 1 {
			t.Errorf("counts %d, %d of %d, want 1 failure, 1 qualification failure, of 1", outs.Failures, outs.QualificationFailures, outs.Count)
		}
	})
}
