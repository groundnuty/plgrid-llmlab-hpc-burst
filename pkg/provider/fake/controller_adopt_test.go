package fake

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The adoption order (HBEP-7, Adoption): with no job ID recorded and jobs
// carrying the tag, the provider adopts a live job that is not held, then a job
// that ran, then the lowest job ID; within each class the lowest job ID. All
// jobs here are placed at 12:00:00 and adopted by a Sync at 12:05:00, when a
// job with FailAfter 1m has ended (12:01:00) after running, and one with
// QueueWait 1h is still pending.

type adoptJob struct {
	script Script
	hold   bool
	cancel bool // cancelled at once
}

func TestAdoptionOrder(t *testing.T) {
	oneMinute := time.Minute
	held := adoptJob{hold: true}
	ran := adoptJob{script: Script{FailAfter: &oneMinute}}                         // starts at once, fails 12:01:00
	running := adoptJob{}                                                          // starts at once and runs on
	pending := adoptJob{script: Script{QueueWait: time.Hour}}                      // live, not held, never started
	neverRan := adoptJob{script: Script{QueueWait: 5 * time.Minute}, cancel: true} // ended, no start

	for _, tc := range []struct {
		name string
		jobs []adoptJob // submitted in this order: job IDs 998, 999, ...
		want string
	}{
		{"lowest held, middle ran, highest running: the running one", []adoptJob{held, ran, running}, "1000"},
		{"lowest held, middle ran: the one that ran", []adoptJob{held, ran}, "999"},
		{"only held jobs: the lowest", []adoptJob{held, held}, "998"},
		{"a pending unheld job beats one that ran", []adoptJob{ran, pending}, "999"},
		{"two live unheld jobs: the lowest", []adoptJob{held, running, pending}, "999"},
		{"two that ran: the lowest", []adoptJob{held, ran, ran}, "999"},
		{"none live and unheld, none ran: the lowest ID, though that job was cancelled", []adoptJob{neverRan, held}, "998"},
		{"a job that ran beats a cancelled one that never ran", []adoptJob{neverRan, ran}, "999"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.newAlloc("a1", testProvider)
			// The attempt's reply was lost; the jobs it made are in the queue.
			e.setRecordJSON("a1", `{"attempt":1,"state":"Unknown","writer":"fake","time":"2026-10-30T12:00:00Z"}`)
			for _, j := range tc.jobs {
				e.batch.Enqueue(j.script)
			}
			for i, j := range tc.jobs {
				id := e.submitFor("a1", j.hold)
				if j.cancel {
					e.batch.Cancel(context.Background(), []string{id})
				}
				if want := 998 + i; id != strconv.Itoa(want) {
					t.Fatalf("setup: job %d has ID %s, want %d", i, id, want)
				}
			}
			e.syncAt(at(12, 5, 0))
			if got := e.alloc("a1").Annotations[v1alpha1.AnnotationJobID]; got != tc.want {
				t.Fatalf("adopted job %q, want %q", got, tc.want)
			}
			if n := e.batch.Count("sbatch"); n != len(tc.jobs) {
				t.Fatalf("sbatch logged %d times, want %d (the setup's): adoption submits nothing", n, len(tc.jobs))
			}
		})
	}
}
