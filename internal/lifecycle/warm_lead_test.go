package lifecycle

// The rows for review finding 6 of #708 (#616 PR 4; written first to fail on head 9ccc6408), ruled a fix, to amendment #711: a replacement
// goes onto a warm Allocation "only when the warm Allocation's drain start is later than its predecessor's by at least the
// lead an Instance needs" (Provisioning, A partition too short): leadOf(pv, md) = the drain window + handoverMargin +
// health.startTimeout. warmFor used a strict "later" (drainsAfter), which seconds satisfy.
//
// The fixture is handover_test.go's (a 4 h job limit, a drain window of 2 h 10 min, a handoverMargin of 30 min, a
// health.startTimeout of 1 h, start-to-ready 50 s), so the lead is 2 h 10 min + 30 min + 1 h = 3 h 40 min = 13,200 s.
// Keep, no maxIdle: the ranked-out extra's Allocation stays warm.

import (
	"fmt"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// TestAWarmAllocationThatDrainsOnlySecondsAfterItsPredecessorTakesNoReplacement: k = 1; the primary P's job starts at
// 08:00:20 (hard end 12:00:20, drain start 09:50:20) and the extra X's 10 s later, at 08:00:30 (hard end 12:00:30, drain
// start 09:50:30): the two drain 10 s apart. P is ready at 08:01:10 and X, ranked out against it, is kept warm (listed, no live
// Instance, 3 h 59 min of lease left at its first moments). P's replacement R is planned in the minutes after 09:19 (drain
// start 09:50:20 less the planning wait, 50 s and the 30 min margin), and X's Allocation, which drains 10 s after P, is
// not 3 h 40 min after it (that would be 13:30:20 or later), so R must be a new Allocation, its job queued and started
// well before 09:50:20. On X's Allocation R would drain at 09:50:30, 10 s after P, and R's own replacement is only planned
// once R is the primary, at 09:50:20: the new job starts 10 s later and is ready 50 s after that, 09:51:20 at the
// soonest, so the deployment would have no ready Instance from 09:50:30 to 09:51:20.
// Expected: at least one Instance is ready at every step from 08:01:10 to 09:53:00, and R is on a new Allocation: 3 jobs.
func TestAWarmAllocationThatDrainsOnlySecondsAfterItsPredecessorTakesNoReplacement(t *testing.T) {
	rg := newRig(t, withMaxTime(4*time.Hour))
	rg.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: 10 * time.Second})
	p := hoStartK(rg, nil, true) // 08:01:10, P ready, X's job started at 08:00:30
	guard := func() {
		t.Helper()
		if n := rg.readyCount(hoMD); n < 1 {
			t.Fatalf("no Instance is ready at %s: a replacement went onto the ranked-out extra's Allocation, which drains 10 s after the primary, and the deployment is without capacity until the replacement's own replacement is ready", rg.clock.Now().Format(time.TimeOnly))
		}
	}
	hoStepTo(rg, hoT(8, 30, 0), time.Minute, guard)
	warm := ""
	for _, a := range rg.allocations(hoMD) {
		if a.Name != p.Spec.AllocationRef.Name {
			warm = a.Name
		}
	}
	if warm == "" || allocReleased(rg, warm) || len(rg.liveInstances(hoMD)) != 1 {
		t.Fatalf("no warm Allocation beside the primary's at 08:30:00 (warm %q, %d live Instances): the row proves nothing", warm, len(rg.liveInstances(hoMD)))
	}
	if a, ok := rg.allocation(warm); !ok || a.Status.AtProvider.StartTime == nil || !a.Status.AtProvider.StartTime.Time.Equal(hoT(8, 0, 30)) {
		t.Fatalf("the warm Allocation %+v, want its job started at 08:00:30, 10 s after the primary's", a)
	}
	hoStepTo(rg, hoT(9, 15, 0), time.Minute, guard)
	hoStepTo(rg, hoT(9, 25, 0), 10*time.Second, guard)
	reps := hoRepl(rg, p.Name)
	if len(reps) != 1 {
		t.Fatalf("%d replacements of %s at 09:25:00, want one", len(reps), p.Name)
	}
	hoStepTo(rg, hoT(9, 49, 0), time.Minute, guard)
	hoStepTo(rg, hoT(9, 53, 0), 10*time.Second, guard)
	if reps[0].Spec.AllocationRef.Name == warm || rg.submissions() != 3 {
		t.Fatalf("the replacement is on %s (the warm Allocation is %s), %d jobs, want a new Allocation and 3 jobs: the warm one drains only 10 s after the primary", reps[0].Spec.AllocationRef.Name, warm, rg.submissions())
	}
}

// TestAReplacementGoesOnAWarmAllocationOnlyIfItDrainsAtLeastTheLeadLater: the boundary of the lead. The rig's partition allows
// 12 h and health.startTimeout is set so that the lead is 2 h 10 min (the drain window) + 30 min (handoverMargin) + the start
// timeout. The first job starts at 08:00:20 (hard end 20:00:20, drain start 17:50:20). instances is raised to 2 at 11:20:00:
// the second job starts at 11:20:20 (hard end 23:20:20, drain start 21:10:20), so its drain start is 21:10:20 - 17:50:20 = 3 h
// 20 min = 12,000 s after the first's. instances is lowered to 1 at 11:21:00, and the second Instance (its worker never runs) is
// the one that ends: its Allocation stays warm. The survivor's replacement is planned at 17:50:20 - (0 + 50 s + 30 min) =
// 17:19:30.
//   - health.startTimeout 40 min: the lead is 2 h 10 min + 30 min + 40 min = 3 h 20 min = 12,000 s, exactly the gap: the warm
//     Allocation is used ("by at least" includes the boundary), 2 submissions;
//   - 40 min 1 s: the lead is 12,001 s, one second more than the gap: it is not used, 3 submissions.
func TestAReplacementGoesOnAWarmAllocationOnlyIfItDrainsAtLeastTheLeadLater(t *testing.T) {
	for _, c := range []struct {
		name         string
		startTimeout time.Duration
		warmUsed     bool
	}{
		{"the warm Allocation drains exactly the lead (3 h 20 min) after the predecessor: used", 40 * time.Minute, true},
		{"the warm Allocation drains one second less than the lead after the predecessor: not used", 40*time.Minute + time.Second, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			rg := newRig(t, withMaxTime(12*time.Hour), withProfile(func(s *v1alpha1.PlatformProfileSpec) {
				s.Health = &v1alpha1.HealthThresholds{StartTimeout: dur(c.startTimeout)}
			}))
			calls := 0
			rg.engine = func(string) afake.Engine {
				calls++
				if calls == 2 {
					return afake.Engine{StartDelay: 6 * time.Hour}
				}
				return afake.Engine{}
			}
			hoStartLease(rg, nil, hoT(20, 0, 20)) // the first job starts 08:00:20, so it drains from 17:50:20
			hoStepTo(rg, hoT(11, 20, 0), time.Minute, nil)
			rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 2 })
			hoStepTo(rg, hoT(11, 21, 0), 10*time.Second, nil)
			rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
			rg.until(2*time.Minute, 10*time.Second, "one live Instance", func() bool { return len(rg.liveInstances(hoMD)) == 1 })
			survivor := rg.liveInstances(hoMD)[0]
			warm := ""
			for _, a := range rg.allocations(hoMD) {
				if a.Name != survivor.Spec.AllocationRef.Name {
					warm = a.Name
				}
			}
			if warm == "" || allocReleased(rg, warm) {
				t.Fatalf("no warm Allocation kept beside the survivor's: the row proves nothing (survivor on %s)", survivor.Spec.AllocationRef.Name)
			}
			if a, ok := rg.allocation(warm); !ok || a.Status.AtProvider.HardEnd == nil || !a.Status.AtProvider.HardEnd.Time.Equal(hoT(23, 20, 20)) {
				t.Fatalf("the warm Allocation %+v, want its hard end at 23:20:20, 3 h 20 min after the survivor's drain start's difference", a)
			}
			hoStepTo(rg, hoT(17, 19, 20), time.Minute, nil)
			if a, ok := rg.allocation(warm); !ok || !a.DeletionTimestamp.IsZero() || a.Status.AtProvider.Phase != v1alpha1.AllocationRunning || len(rg.r.Backend.registrations(a.UID)) == 0 {
				t.Fatalf("at 17:19:20 the warm Allocation %+v (found %v), registrations %d: the row needs it Running, registered and kept", a, ok, func() int {
					if a == nil {
						return -1
					}
					return len(rg.r.Backend.registrations(a.UID))
				}())
			}
			hoStepTo(rg, hoT(17, 19, 40), 10*time.Second, nil)
			reps := hoRepl(rg, survivor.Name)
			if len(reps) != 1 {
				t.Fatalf("%d replacements of %s at 17:19:40, want one (planned 17:19:30)", len(reps), survivor.Name)
			}
			wantSubmissions := 3
			if c.warmUsed {
				wantSubmissions = 2
			}
			if onWarm := reps[0].Spec.AllocationRef.Name == warm; onWarm != c.warmUsed || rg.submissions() != wantSubmissions {
				t.Fatalf("the replacement is on the warm Allocation: %v, %d submissions, want %v and %d (the warm one drains 21:10:20, the predecessor 17:50:20: %s)", onWarm, rg.submissions(), c.warmUsed, wantSubmissions, fmt.Sprintf("12,000 s apart, lead %s", c.startTimeout+2*time.Hour+40*time.Minute))
			}
		})
	}
}
