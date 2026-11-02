package livechecks

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The rule for the one-job slots (the science agent, 2 Nov 2026, on #222): a row that runs in a one-job slot, on either
// cluster (Helios QOS now, Athena's partition plgrid-now), has a start wait of 10 minutes or less, or a wait in the
// queue of 1,800 seconds or less, and it passes the slot guard before anything is built or submitted. The rule is read
// off the cluster table, not off a list of rows, so a row added later cannot miss it: the test fails and names it.
const (
	slotStartWaitMax = 10   // minutes
	slotPendingMax   = 1800 // seconds
)

type slotRow struct{ name, partition, qos, slot, startWait string }

// readSlotRows are the rows of the cluster table whose slot column names a slot, with the shape's other fields the
// rule needs.
func readSlotRows(t *testing.T) []slotRow {
	t.Helper()
	b, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	var rows []slotRow
	for _, l := range block(t, string(b), "CLUSTER SHAPES") {
		f := strings.Split(l, "|")
		if len(f) != 13 && len(f) != 14 {
			t.Fatalf("shape row %q has %d fields, want 13 or 14", l, len(f))
		}
		if f[5] == "-" {
			continue
		}
		wait := "-"
		if len(f) == 14 {
			wait = f[13]
		}
		rows = append(rows, slotRow{name: f[0], partition: f[3], qos: f[4], slot: f[5], startWait: wait})
	}
	slots := map[string]bool{}
	for _, r := range rows {
		slots[r.slot] = true
	}
	if !slots["now"] || !slots["plgrid-now"] {
		t.Fatalf("the table's slot rows are %v: the rule looked at one cluster's slot or none", rows)
	}
	return rows
}

// slotWorld is the world a slot row runs in under the stubs: the conformance and real-engine rows in the plain one,
// the reach rows (which need the gateway's settings, or they refuse for want of them) in the reach one, the stack and
// accounting rows in the alpha one.
func slotWorld(t *testing.T, name string) *world {
	t.Helper()
	switch {
	case name == "real-engine" || strings.HasPrefix(name, "conformance-"):
		w := newWorld(t)
		w.useStubGo()
		return w
	case strings.HasPrefix(name, "reach-"):
		return newReachWorld(t)
	}
	return newAlphaWorld(t)
}

var (
	pendingEnv     = regexp.MustCompile(`(?m)^HPCBURST_(?:ALPHA|CONF)_PENDING_MAX_SECONDS=(\d+)$`)
	realEngineWait = regexp.MustCompile(`still pending after (?:(\d+) min)? ?(?:(\d+) s)?; cancelling it`)
)

// deliveredBound is the wait in the queue, in seconds, that a slot row with no start wait gets from the script, with no
// override set; 0 when none reaches it. A test of the stack, accounting or reach kind has it in its environment.
// real-engine has no test to hand it to: its run polls the job and gives up at the bound, so the message it gives and
// the polls it made, on a clock the stub moves a minute at each, are the bound. A row of another kind is not read here,
// and fails the rule until this test learns how.
func deliveredBound(t *testing.T, r slotRow) int {
	t.Helper()
	w := slotWorld(t, r.name)
	if r.name == "real-engine" {
		res := w.run(append(w.fakeClock(), "STUB_CLOCK_STEP=60", "STUB_JOB_FOREVER=PENDING"), "--run", r.name, "--yes")
		m := realEngineWait.FindStringSubmatch(res.out)
		if res.code != 1 || m == nil {
			t.Fatalf("%s: exit %d, and no overrun message to read the bound from:\n%s", r.name, res.code, res.out)
		}
		mins, _ := strconv.Atoi(m[1])
		secs, _ := strconv.Atoi(m[2])
		bound := mins*60 + secs
		// The message and the comparison are one value: the polls made are the bound's minutes, and one more.
		if polls := strings.Count(w.stateFile("events"), "squeue %T job\n"); polls != bound/60+1 {
			t.Errorf("%s: the message says %d s, but the run made %d polls of a minute before it gave up", r.name, bound, polls)
		}
		return bound
	}
	if res := w.run(nil, "--run", r.name, "--yes"); res.code != 0 {
		t.Fatalf("%s: exit %d, so no environment to read the bound from:\n%s", r.name, res.code, res.out)
	}
	files, _ := filepath.Glob(filepath.Join(w.state, "test-env-*"))
	for _, f := range files {
		b, _ := os.ReadFile(f)
		if m := pendingEnv.FindStringSubmatch(string(b)); m != nil {
			n, _ := strconv.Atoi(m[1])
			return n
		}
	}
	return 0
}

func TestEveryRowInAOneJobSlotHasAStartWaitOrAPendingBound(t *testing.T) {
	for _, r := range readSlotRows(t) {
		t.Run(r.name, func(t *testing.T) {
			t.Parallel()
			if r.startWait != "-" {
				if n, err := strconv.Atoi(r.startWait); err != nil || n > slotStartWaitMax {
					t.Errorf("%s runs in the %s slot with a start wait of %s min, over the %d the rule allows, and no other bound is delivered with one",
						r.name, r.slot, r.startWait, slotStartWaitMax)
				}
				return
			}
			switch bound := deliveredBound(t, r); {
			case bound == 0:
				t.Errorf("%s runs in the %s slot with neither a start wait nor a wait in the queue delivered to its test", r.name, r.slot)
			case bound > slotPendingMax:
				t.Errorf("%s runs in the %s slot with a wait in the queue of %d s, over the %d the rule allows", r.name, r.slot, bound, slotPendingMax)
			}
		})
	}
}

// The slot guard: with a job of the user's in the slot, a row refuses (exit 3) and nothing is built, submitted or run.
func TestEveryRowInAOneJobSlotPassesTheSlotGuardBeforeAnythingRuns(t *testing.T) {
	for _, r := range readSlotRows(t) {
		t.Run(r.name, func(t *testing.T) {
			t.Parallel()
			w := slotWorld(t, r.name)
			qos := r.qos
			if qos == "-" {
				qos = "normal"
			}
			w.seedQueue("5000|someone-elses-job|" + qos + "|" + r.partition + "|RUNNING")
			res := w.run(nil, "--run", r.name, "--yes")
			if res.code != 3 {
				t.Fatalf("%s: exit %d, want 3 (the slot guard):\n%s", r.name, res.code, res.out)
			}
			w.contains(res, "the one-job slot ("+r.slot+") already holds 1 job(s) of yours; not queuing behind it")
			if w.stateFile("go-argv") != "" || w.stateFile("cancelled") != "" {
				t.Errorf("%s: a refused run built something or cancelled a job: %s %s", r.name, w.stateFile("go-argv"), w.stateFile("cancelled"))
			}
			w.noSubmit()
			if ran, _ := filepath.Glob(filepath.Join(w.state, "test-env-*")); len(ran) != 0 {
				t.Errorf("%s: a refused run ran its test: %v", r.name, ran)
			}
		})
	}
}
