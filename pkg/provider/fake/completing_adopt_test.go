package fake

// Adoption and a completing job in the fake controller (#198's final rule:
// "Adoption ranks a completing job as 'a job that ran'"; HBEP-7, Adoption). A
// requeued job that still completes is ["PENDING","COMPLETING"] with a restart
// count of 1 and no end time, so it looks live and unheld; it must not rank in
// the first class (Astra B3 on PR #217). pickAdopted takes hand-built jobs,
// sorted by ID.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

func TestPickAdoptedRanksACompletingJobAsOneThatRan(t *testing.T) {
	start, end := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC), time.Date(2026, 10, 30, 12, 2, 30, 0, time.UTC)
	requeuedCG := func(id string) provider.Job {
		return provider.Job{ID: id, State: []string{"PENDING", "COMPLETING"}, Completing: true, Requeued: true, RestartCount: 1}
	}
	running := func(id string) provider.Job {
		return provider.Job{ID: id, State: []string{"RUNNING"}, StartTime: &start}
	}
	pending := func(id string) provider.Job { return provider.Job{ID: id, State: []string{"PENDING"}} }
	held := func(id string) provider.Job {
		return provider.Job{ID: id, State: []string{"PENDING"}, Hold: provider.HoldUser}
	}
	endedCG := func(id string) provider.Job {
		return provider.Job{ID: id, State: []string{"CANCELLED", "COMPLETING"}, Completing: true, StartTime: &start, EndTime: &end}
	}
	for _, tc := range []struct {
		name string
		jobs []provider.Job
		want string
	}{
		{"a running job beats a requeued completing one with a lower ID", []provider.Job{requeuedCG("998"), running("999")}, "999"},
		{"a pending unheld job beats a requeued completing one with a lower ID", []provider.Job{requeuedCG("998"), pending("999")}, "999"},
		{"a requeued completing job is one that ran: it beats a held job with a lower ID", []provider.Job{held("998"), requeuedCG("999")}, "999"},
		{"an ended completing job beats a held job with a lower ID", []provider.Job{held("998"), endedCG("999")}, "999"},
		{"a running job beats an ended completing one with a lower ID", []provider.Job{endedCG("998"), running("999")}, "999"},
		{"two completing jobs: the lowest ID", []provider.Job{endedCG("998"), requeuedCG("999")}, "998"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := pickAdopted(tc.jobs).ID; got != tc.want {
				t.Errorf("pickAdopted picked %s, want %s", got, tc.want)
			}
		})
	}
}
