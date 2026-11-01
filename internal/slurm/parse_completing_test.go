package slurm

// The COMPLETING flag in job_state (HBEP-4, Phases, A completing job;
// science-agent ruling for #198, 1 Nov 2026): the queue listing's job_state is
// a list, the base state then flags, and every entry is read. The phase
// follows the base state; the flag sets Job.Completing, with the listing's
// end_time as CompletingSince; accounting never sets it. Expected values are
// written out by hand: 1793351853 is 2026-10-30T09:17:33Z (worked out with
// date -u -d @1793351853, never with this package).

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func TestQueueCompletingFlag(t *testing.T) {
	end := at(2026, 10, 30, 9, 17, 33)
	cases := []struct {
		name       string
		state      []string
		restart    float64
		phase      v1alpha1.AllocationPhase
		requeued   bool
		completing bool
	}{
		{"CANCELLED with the flag", []string{"CANCELLED", "COMPLETING"}, 0, v1alpha1.AllocationEnded, false, true},
		{"FAILED with the flag", []string{"FAILED", "COMPLETING"}, 0, v1alpha1.AllocationFailed, false, true},
		{"TIMEOUT with the flag", []string{"TIMEOUT", "COMPLETING"}, 0, v1alpha1.AllocationEnded, false, true},
		{"COMPLETED with the flag", []string{"COMPLETED", "COMPLETING"}, 0, v1alpha1.AllocationEnded, false, true},
		{"NODE_FAIL with the flag", []string{"NODE_FAIL", "COMPLETING"}, 0, v1alpha1.AllocationFailed, false, true},
		{"flag in lower case", []string{"cancelled", "completing"}, 0, v1alpha1.AllocationEnded, false, true},
		{"flag after another flag", []string{"CANCELLED", "REQUEUE_HOLD", "COMPLETING"}, 0, v1alpha1.AllocationEnded, false, true},
		{"RUNNING with the flag, whatever the base state", []string{"RUNNING", "COMPLETING"}, 0, v1alpha1.AllocationRunning, false, true},
		{"PENDING with the flag after a requeue stays Failed", []string{"PENDING", "COMPLETING"}, 1, v1alpha1.AllocationFailed, true, true},
		{"CANCELLED without the flag", []string{"CANCELLED"}, 0, v1alpha1.AllocationEnded, false, false},
		{"another flag is not the flag", []string{"CANCELLED", "REQUEUE_HOLD"}, 0, v1alpha1.AllocationEnded, false, false},
	}
	for _, v := range dialects {
		for _, c := range cases {
			t.Run(v+"/"+c.name, func(t *testing.T) {
				doc := onlyJob(t, readTestdata(t, v, "squeue-running.json"), 4242, func(j map[string]any) {
					j["job_state"] = toAny(c.state...)
					j["restart_cnt"] = c.restart
					j["end_time"] = triple(true, 1793351853)
				})
				j := queueJobs(t, doc, v)["4242"]
				if j.Phase != c.phase || j.Requeued != c.requeued || j.Completing != c.completing {
					t.Errorf("phase %s, Requeued %v, Completing %v; want %s, %v, %v", j.Phase, j.Requeued, j.Completing, c.phase, c.requeued, c.completing)
				}
				if c.completing && !sameTime(j.CompletingSince, &end) {
					t.Errorf("CompletingSince %s, want %s", timeText(j.CompletingSince), timeText(&end))
				}
				if !c.completing && j.CompletingSince != nil {
					t.Errorf("CompletingSince %s with no flag, want nil", timeText(j.CompletingSince))
				}
			})
		}
	}
}

// A job listed with the flag but no end_time has none to say since when:
// CompletingSince is nil, and the flag still stands.
func TestQueueCompletingWithoutEndTime(t *testing.T) {
	for _, v := range dialects {
		doc := onlyJob(t, readTestdata(t, v, "squeue-running.json"), 4242, func(j map[string]any) {
			j["job_state"] = toAny("CANCELLED", "COMPLETING")
			j["end_time"] = triple(false, 0)
		})
		j := queueJobs(t, doc, v)["4242"]
		if !j.Completing || j.CompletingSince != nil || j.EndTime != nil {
			t.Errorf("%s: Completing %v, CompletingSince %s, EndTime %s; want true, nil, nil", v, j.Completing, timeText(j.CompletingSince), timeText(j.EndTime))
		}
	}
}

// S7's real capture of a completing job on Helios (job 4308, GH200 after the
// end): ["FAILED","COMPLETING"], end_time 1793351253, which is
// 2026-10-30T09:07:33Z. No other job in that document carries the flag.
func TestRealCompletingCaptureIsFailedAndCompleting(t *testing.T) {
	for _, v := range dialects {
		jobs := queueJobs(t, readTestdata(t, v, "squeue-derived.json"), v)
		j := jobs["4308"]
		want := at(2026, 10, 30, 9, 7, 33)
		if j.Phase != v1alpha1.AllocationFailed || !j.Completing || !sameTime(j.CompletingSince, &want) || !sameTime(j.EndTime, &want) {
			t.Errorf("%s: job 4308 phase %s, Completing %v, CompletingSince %s, EndTime %s; want Failed, true, %s, %s",
				v, j.Phase, j.Completing, timeText(j.CompletingSince), timeText(j.EndTime), want.Format("2006-01-02T15:04:05Z"), want.Format("2006-01-02T15:04:05Z"))
		}
		for id, other := range jobs {
			if id != "4308" && other.Completing {
				t.Errorf("%s: job %s is completing, want only job 4308", v, id)
			}
		}
	}
}

// Accounting never carries the flag, and a record that did would not set it:
// a job read from accounting is not completing, whatever its state list says.
func TestAccountingNeverSetsCompleting(t *testing.T) {
	for _, v := range dialects {
		doc := onlyJob(t, readTestdata(t, v, "sacct-completed.json"), 4242, func(j map[string]any) {
			j["state"].(map[string]any)["current"] = toAny("CANCELLED", "COMPLETING")
		})
		j := accountingJobs(t, doc, v)["4242"]
		if j.Completing || j.CompletingSince != nil || j.Phase != v1alpha1.AllocationEnded {
			t.Errorf("%s: from accounting Completing %v, CompletingSince %s, phase %s; want false, nil, Ended", v, j.Completing, timeText(j.CompletingSince), j.Phase)
		}
	}
}
