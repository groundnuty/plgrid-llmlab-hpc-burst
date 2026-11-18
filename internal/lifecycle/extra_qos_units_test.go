package lifecycle

// The extra's unit set charged per QOS (Q1; #756's per-QOS counting through #751's unit view). Where a Provider's partition sets
// qosByGpus, the extra's per-QOS slot term and caps sizes are the SAME units as the Provider-wide terms, filtered by the QOS
// each unit's GPU count submits under (extraUnit.gpus, qosScope.under, through unitTerms' filter). Literals only: Sano's four
// QOS by GPU count (gpu1, gpu2, gpu4, gpu8, a copy of the profile), one GPU per Allocation but where a row says otherwise.

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// TestAnExtrasHandoverUnitsAreChargedToTheirOwnQOS (Q1): on provider-b, gpu1 allows 2 jobs per user. md-e (ends at 09:30, so no
// handover of its own) has its primary pe, Pending and listed under gpu1 (1 GPU). md-o (no end time) has its primary po in flight
// (its Instance made, no Allocation yet), whose replacement will need po's GPUs. md-e's extra asks for 1 GPU, under gpu1. By
// hand, gpu1's count is listed pe 1 plus the units under gpu1:
//   - po on 4 GPUs: its handover is under gpu4, not gpu1, so 1 + 0 = 1 < 2 and the extra is admitted, though the Provider-wide
//     terms count the unit;
//   - po on 1 GPU: its handover is under gpu1, so 1 + 1 = 2 >= 2 and the extra is refused, by gpu1's own limit (scoped).
func TestAnExtrasHandoverUnitsAreChargedToTheirOwnQOS(t *testing.T) {
	end := hoT(9, 30, 0)
	qos := sanoQOS()
	qos[0].MaxSubmitPerUser = i32(2)
	pv := sanoView(qos, false)
	rv, err := pv.forGPUs(1)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name     string
		unitGPUs int32
		ok       bool
	}{
		{"po's handover under gpu4 does not block an extra under gpu1 (1 + 0 < 2)", 4, true},
		{"po's handover under gpu1 blocks it (1 + 1 >= 2)", 1, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &unitFx{}
			f.md("md-e", &end)
			f.md("md-o", nil)
			f.inst("md-e", "pe", provB, "uid-pe", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
			f.alloc("md-e", "pe", provB, false)
			po := f.inst("md-o", "po", provB, "uid-po", hoT(8, 0, 10), v1alpha1.InstancePending, nil) // in flight
			po.Spec.Launch.GPUs = c.unitGPUs
			q := extraSlotAsk(provB, "alloc-x", nil, nil, rigNS+"/md-e", "")
			q.scope, q.gpus, q.caps = rv.scope, 1, rv.limits
			a := ask(t, newSlotTable(), f.reader(t), q)
			if a.ok != c.ok || (!c.ok && !a.scoped) {
				t.Fatalf("ok %v, scoped %v, with a count of %d, want ok %v (refused by gpu1's own limit when not)", a.ok, a.scoped, a.count, c.ok)
			}
		})
	}
}
