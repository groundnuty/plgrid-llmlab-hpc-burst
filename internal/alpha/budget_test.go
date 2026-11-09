package alpha

import (
	"os"
	"strings"
	"testing"
	"time"
)

// Each stack row's worst case, added up by hand from the review of PR 284 (B5), not by the code it checks: a no-overlap
// row at a wait of 2 h is 120 for the predecessor to start, 106 for its lease, 120 for the successor to start (346, the
// figure in the review), 60 and 5 for it to become ready, 1 for the guard's last looks and 5 to close. The tunnel rows'
// first wait stops a minute before the first end, the lease less the 15-minute drain window.
func TestEachStackRowsWorstCaseIsItsWaitsAddedUp(t *testing.T) {
	long, err := WithTunnelLease(rowOf("tunnel-hour-athena"), 240*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		row  Row
		// pending and want are in minutes.
		pending, want, wantDeadline int
	}{
		{"handover, 30", rowOf("alpha-athena-handover"), 30, 30 + 91 + 5 + 15 + 5 + 1 + 5, 2*30 + 150},
		{"handover, 2 h", rowOf("alpha-athena-handover"), 120, 120 + 91 + 5 + 15 + 5 + 1 + 5, 2*120 + 150},
		{"no-overlap, 30", rowOf("alpha-helios-nooverlap"), 30, 30 + 106 + 30 + 60 + 5 + 1 + 5, 2*30 + 200},
		{"no-overlap, 2 h", rowOf("alpha-helios-nooverlap"), 120, 120 + 106 + 120 + 60 + 5 + 1 + 5, 2*120 + 200},
		{"the hour on Helios, 30", rowOf("tunnel-hour-helios"), 30, 30 + 122 + 15 + 5 + 5, 30 + 150},
		{"the hour on Athena, 2 h", rowOf("tunnel-hour-athena"), 120, 120 + 122 + 15 + 5 + 5, 120 + 150},
		{"the hour on Athena, its wait capped a minute before the first end", rowOf("tunnel-hour-athena"), 180, 121 + 122 + 15 + 5 + 5, 180 + 150},
		{"the hour at the longest lease", long, 120, 120 + 225 + 15 + 5 + 5, 120 + 255},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := time.Duration(c.pending) * time.Minute
			if got, want := c.row.WorstCase(p), time.Duration(c.want)*time.Minute; got != want {
				t.Errorf("WorstCase = %s, want %s", got, want)
			}
			if got, want := c.row.RunDeadline(p), time.Duration(c.wantDeadline)*time.Minute; got != want {
				t.Errorf("RunDeadline = %s, want %s", got, want)
			}
			// The run's context ends the ledger's read after the scenario, so it must outlast every wait but Close's, which
			// takes a time of its own: otherwise a run in time fails on its last read.
			if got, min := c.row.RunDeadline(p), c.row.WorstCase(p)-CloseTimeout; got < min {
				t.Errorf("the run's deadline %s is below its waits, %s", got, min)
			}
		})
	}
}

// The sums above are only as good as the steps' use of what they add up. These calls are the live steps', which no dry
// run reaches (only the stand-in proof, HPCBURST_ALPHA_STANDIN), so their words are pinned here: a step that waits on a
// length of its own, past a bound of its own or under a deadline of its own, fails this rather than a live run.
func TestTheLiveStepsWaitOnWhatTheSumsCount(t *testing.T) {
	for file, calls := range map[string][]string{
		"alpha_live_test.go": {"context.WithTimeout(context.Background(), cfg.Row.RunDeadline(cfg.PendingMax))"},
		"live_flows_test.go": {
			"r.successorOf(pre.Name, time.Until(leaseEnd)+SuccessorWait(r.tm, p.PendingMax), leaseEnd.Add(p.PendingMax))",
			"r.successorOf(pre.Name, time.Until(ds), time.Time{})",
			"r.waitFor(r.dw+EndMargin, \"the primary to end\"",
			"r.waitFor(r.dw+EndMargin, \"every job of the run to end in the cluster's record\"",
		},
		"steps_live_test.go": {"context.WithTimeout(context.WithoutCancel(ctx), CloseTimeout)", "return r.tm.StartTimeout + ReadyMargin"},
		"guard_live_test.go": {"wait = GuardResultLooks * r.every()", "return GuardEvery"},
	} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, call := range calls {
			if strings.Count(string(src), call) != 1 {
				t.Errorf("%s no longer has, once, %q: the worst case (budget.go) adds up what it waits for", file, call)
			}
		}
	}
}
