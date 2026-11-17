package lifecycle

// What gateJudge does before it reads anything (HBEP-7, The throughput gate; #616, PR 2): it judges an Instance's
// first qualification and its retry only, and a profile with no expected performance, which is every shipped one, is
// recorded only with nothing read. The pass here has no client: a read would panic, which the tests turn into a failure.

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// readsNothing runs f and fails the test when it reads through the pass's missing client.
func readsNothing(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s: it read through the pass's client (%v), want it to decide first", what, r)
		}
	}()
	f()
}

func gateInstance() *v1alpha1.Instance {
	return &v1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "i", Labels: map[string]string{v1alpha1.LabelProvider: "prov"}},
		Spec: v1alpha1.InstanceSpec{
			DeploymentID: v1alpha1.FullDeploymentID("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
			Launch:       v1alpha1.Launch{Command: []string{"vllm", "serve", "/model", "--max-num-seqs=16"}, MaxNumSeqs: 16},
		},
		Status: v1alpha1.InstanceStatus{Conditions: []metav1.Condition{{
			Type: v1alpha1.ConditionQualified, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPassed,
			Message: "every check passed; throughput gate: judged after warm-up",
		}}},
	}
}

func gateJudgePass(profile *v1alpha1.PlatformProfile) *pass {
	return &pass{now: time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC), pvByName: map[string]*providerView{"prov": {profile: profile}}}
}

var gateOneWorker = []gateWorker{{name: "worker-0", tp: gateTP("41ms", "300ms"), record: "ver-x"}}

func gateTP(tpot, ttft string) *v1alpha1.Throughput {
	return &v1alpha1.Throughput{TimePerOutputToken: v1alpha1.Duration(tpot), TimeToFirstToken: v1alpha1.Duration(ttft), TargetSessions: 8}
}

// An Instance with a final verdict, whichever it is, is not judged again: a later warm-up only updates
// status.throughput. The profile holds an entry that 41 ms misses, so a gate that judged would act.
func TestGateJudgesNothingOnceTheVerdictIsSet(t *testing.T) {
	for _, verdict := range []string{v1alpha1.GatePassed, v1alpha1.GateMissed, v1alpha1.GateSuspended, v1alpha1.GateRecordOnly} {
		in := gateInstance()
		in.Status.Gate = &v1alpha1.InstanceGate{Verdict: verdict}
		p := gateJudgePass(gateProfile(gateFixtureEntry(gateKey())))
		readsNothing(t, verdict, func() {
			work, err := p.gateJudge(in, gateShapes, gateOneWorker)
			if err != nil || work.record != nil || work.restart != nil || work.fail != nil {
				t.Errorf("verdict %s: %+v, %v, want no work", verdict, work, err)
			}
		})
	}
}

var gateShapes = shapes{typical: 1024, outputTypical: 128, targetSessions: 8}

// An Instance that has been ready is past its first qualification whatever its record says: one that became ready
// before the gate acted, with no verdict, is neither restarted nor failed by a later warm-up.
func TestGateJudgesNothingOfAnInstanceThatHasBeenReady(t *testing.T) {
	in := gateInstance()
	at := metav1.NewTime(time.Date(2026, 11, 2, 7, 0, 0, 0, time.UTC))
	in.Status.ReadyTime = &at
	in.Status.Gate = &v1alpha1.InstanceGate{Misses: []v1alpha1.GateMiss{{Worker: "worker-0", Throughput: *gateTP("41ms", "300ms"), Time: at}}}
	p := gateJudgePass(gateProfile(gateFixtureEntry(gateKey())))
	readsNothing(t, "a ready Instance with a recorded miss", func() {
		work, err := p.gateJudge(in, gateShapes, gateOneWorker)
		if err != nil || work.record != nil || work.restart != nil || work.fail != nil {
			t.Errorf("%+v, %v, want no work", work, err)
		}
	})
}

// A profile with no expected performance, which is every shipped one, gives RecordOnly with nothing read: no
// verification record, so an unreadable one can't hold an Instance back where nothing can gate it.
func TestGateRecordsOnlyWithNothingReadWhenTheProfileHoldsNoEntry(t *testing.T) {
	for name, profile := range map[string]*v1alpha1.PlatformProfile{"an empty profile": gateProfile(), "no profile": nil} {
		in := gateInstance()
		p := gateJudgePass(profile)
		readsNothing(t, name, func() {
			work, err := p.gateJudge(in, gateShapes, gateOneWorker)
			if err != nil || work.record == nil || work.restart != nil || work.fail != nil {
				t.Fatalf("%s: %+v, %v, want a RecordOnly record", name, work, err)
			}
			s := in.Status.DeepCopy()
			work.record(s)
			if s.Gate == nil || s.Gate.Verdict != v1alpha1.GateRecordOnly || s.Gate.ReferenceDate != "" {
				t.Errorf("%s: status.gate %+v, want RecordOnly with no date", name, s.Gate)
			}
			if got := conditionMessage(&v1alpha1.Instance{Status: *s}, v1alpha1.ConditionQualified); got != "every check passed; throughput gate: records only: no entry matches: the platform profile holds no expected performance" {
				t.Errorf("%s: Qualified's message %q", name, got)
			}
		})
	}
}
