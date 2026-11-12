package livechecks

// gpu-accounting-athena-now (issue #145, the science agent's ruling of 12 Nov 2026): TestGPUAccounting on Athena's
// one-job partition plgrid-now, under profiles/athena-now.yaml, because the A100 queue was about two days out. It has
// the one-job slot's guards and the owner's go of plgrid-now, and the bare name gpu-accounting stands for it. The
// guards are the script's shared ones, so this file shows that they reach this row, in the order the row needs them,
// and the table tests (slotrule_test.go, neverstarted_test.go, alpha_test.go) cover the row beside the others.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
)

const (
	nowAccountingRow = "gpu-accounting-athena-now"
	// An owner's job in the partition's one-job slot, as squeue lists it: id|name|qos|partition|state. The partition is
	// plgrid-now's own slot token; the QOS column says nothing here.
	ownerJobInPlgridNow = "5000|someone-elses-job|normal|plgrid-now|RUNNING"
)

// The row's environment: the shipped profile that names it, the partition, one GPU of the A100 node type's per-GPU size,
// the start wait as its one clock (no wait in the queue, no QOS of its own) and none of the stack's settings. Nothing but
// the account, the credentials, the lock directory, the owner's go and the front label is asked of the settings: the run
// passes with the dial-home route and LIVE_BIN_DIR taken away, which a row that read them would not.
func TestTheAccountingRowOnPlgridNowGivesItsTestItsProfileAndOneClock(t *testing.T) {
	w := newAlphaWorld(t)
	w.alphaSettings(func(m map[string]string) {
		for _, k := range []string{"LIVE_DIALHOME_MODE", "LIVE_DIALHOME_ADDR", "LIVE_DIALHOME_KEY", "LIVE_DIALHOME_LISTEN", "LIVE_BIN_DIR"} {
			delete(m, k)
		}
	})
	res := w.run([]string{"HPCBURST_ALPHA_DRY=1", "HPCBURST_ALPHA_DRY_FAULT=node-two", "HPCBURST_ALPHA_ROW=gpu-accounting-helios", "HPCBURST_ALPHA_OUT=/caller/out"},
		"--run", nowAccountingRow, "--yes")
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	w.contains(res,
		nowAccountingRow+": shape: partition plgrid-now, 1 GPU, 16 CPU, 125Gi, time limit 00:05:00, one job, no requeue, each job to start within 10 min",
		nowAccountingRow+": PASS",
		nowAccountingRow+": slot use (plgrid-now): start ")
	env := w.stateFile("test-env-TestGPUAccounting")
	for _, want := range []string{"HPCBURST_CONFORMANCE=1", "HPCBURST_CONF_PROFILE=athena-now", "HPCBURST_CONF_PARTITION=plgrid-now", "HPCBURST_CONF_GPUS=1",
		"HPCBURST_CONF_CPUS=16", "HPCBURST_CONF_MEMORY=125Gi", "HPCBURST_CONF_START_WAIT=10m", "HPCBURST_FRONT_HOST_LABEL="} {
		if !strings.Contains(env, want) {
			t.Errorf("the test did not get %s:\n%s", want, env)
		}
	}
	// No QOS of its own (the partition's applies), no second clock, and nothing of the caller's dry run or of the stack.
	for _, bad := range []string{"HPCBURST_CONF_QOS", "HPCBURST_CONF_PENDING_MAX_SECONDS", "HPCBURST_ALPHA_", "BIN_DIR", "DIALHOME"} {
		if strings.Contains(env, bad) {
			t.Errorf("%s reached the test:\n%s", bad, env)
		}
	}
	if args := w.stateFile("test-args-TestGPUAccounting"); !strings.Contains(args, "-test.run ^TestGPUAccounting$") || !strings.Contains(args, "-test.timeout 33m") {
		t.Errorf("the test binary ran with %q, want ^TestGPUAccounting$ and the timeout of 32 + 1 minutes", args)
	}
}

// The slot is read first and read again after the builds (the slot's use starts after them), and the owner's go is
// checked before either. The order is the script's own events, the same as conformance-athena's
// (TestBuildsFinishBeforeTheSlotIsTaken), for this row.
func TestTheAccountingRowOnPlgridNowReadsTheSlotBeforeAndAfterItsBuilds(t *testing.T) {
	w := newAlphaWorld(t)
	if res := w.run(nil, "--run", nowAccountingRow, "--yes"); res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	ev := w.events()
	first, est, build, second, base, run := index(ev, "squeue %q|%P", 1), index(ev, "sbatch test-only", 1), index(ev, "go test", 1), index(ev, "squeue %q|%P", 2),
		index(ev, "squeue %i|%j|%P", 1), index(ev, "testbin TestGPUAccounting", 1)
	if !(0 <= first && first < est && est < build && build < second && second < base && base < run) {
		t.Errorf("want the slot read, the estimate, the build, the slot read again, the baseline and the run, in that order; events:\n%s", strings.Join(ev, "\n"))
	}
}

// A job that takes the slot after the first look, while the row builds, refuses the run at the second look: exit 3, with
// the slot guard's words, the test binary never run and nothing submitted. The owner's job is left in the queue untouched.
func TestAJobThatTakesPlgridNowDuringTheBuildsRefusesTheAccountingRow(t *testing.T) {
	w := newAlphaWorld(t)
	res := w.run([]string{"STUB_SEED_AT_TESTC=" + ownerJobInPlgridNow}, "--run", nowAccountingRow, "--yes")
	if res.code != 3 {
		t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
	}
	w.contains(res, nowAccountingRow+": skipped: the plgrid-now slot is in use", "the one-job slot (plgrid-now) already holds 1 job(s) of yours; not queuing behind it")
	// The first look found the slot free, so the run built; the second found it taken.
	if w.stateFile("go-argv") == "" {
		t.Errorf("the run did not build, so the first look was not the one that passed:\n%s", res.out)
	}
	if ev := w.events(); index(ev, "testbin ", 1) >= 0 || index(ev, "squeue %i|%j|%P", 1) >= 0 {
		t.Errorf("the run went on past the second look (a baseline or the test):\n%s", strings.Join(ev, "\n"))
	}
	w.noSubmit()
	if c := w.stateFile("cancelled"); c != "" {
		t.Errorf("a job was cancelled in a run that submitted none: %s", c)
	}
	if q := w.stateFile("queue"); !strings.Contains(q, ownerJobInPlgridNow) {
		t.Errorf("the owner's job is gone from the queue:\n%s", q)
	}
}

// The slot guard's first look, with the slot already held: exit 3 before anything is built, estimated or submitted, and
// with --yes or without it.
func TestTheAccountingRowOnPlgridNowRefusesWhenAJobHoldsTheSlot(t *testing.T) {
	for _, args := range [][]string{{"--run", nowAccountingRow}, {"--run", nowAccountingRow, "--yes"}} {
		t.Run(fmt.Sprintf("with %d flags after the row", len(args)-2), func(t *testing.T) {
			w := newAlphaWorld(t)
			w.seedQueue(ownerJobInPlgridNow)
			res := w.run(nil, args...)
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, nowAccountingRow+": skipped: the plgrid-now slot is in use")
			if strings.Contains(res.out, "queue estimate") || strings.Contains(res.out, "FAIL") {
				t.Errorf("an estimate or a failure was printed for a slot in use:\n%s", res.out)
			}
			for name, got := range map[string]string{"sbatch": w.stateFile("sbatch-argv"), "scancel": w.stateFile("cancelled"), "build": w.stateFile("go-argv"), "test run": w.stateFile("test-env-TestGPUAccounting")} {
				if got != "" {
					t.Errorf("%s happened: %s", name, got)
				}
			}
		})
	}
	// An unreadable slot is a busy one, as on the other rows.
	w := newAlphaWorld(t)
	res := w.run([]string{"STUB_SQUEUE_FAIL=1"}, "--run", nowAccountingRow, "--yes")
	if res.code != 3 {
		t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
	}
	w.contains(res, "the slot could not be read, so it is taken as busy")
}

// The owner's go is checked before the slot is read: with the slot held AND the go missing, the refusal is the go's,
// and the queue was never read. With the go set to yesterday's date it is the same.
func TestTheAccountingRowOnPlgridNowAsksForTheOwnersGoBeforeItReadsTheSlot(t *testing.T) {
	for _, stale := range []string{"", "yesterday"} {
		t.Run("go "+stale, func(t *testing.T) {
			w := newAlphaWorld(t)
			w.seedQueue(ownerJobInPlgridNow)
			yesterday := w.dayPlus(-1)
			w.alphaSettings(func(m map[string]string) {
				if stale == "" {
					delete(m, "LIVE_OWNER_GO_ATHENA_NOW")
				} else {
					m["LIVE_OWNER_GO_ATHENA_NOW"] = yesterday
				}
			})
			res := w.run(nil, "--run", nowAccountingRow, "--yes")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, "setting LIVE_OWNER_GO_ATHENA_NOW must hold today's UTC date ("+w.day+")")
			if strings.Contains(res.out, "slot is in use") {
				t.Errorf("the slot was read before the owner's go was checked:\n%s", res.out)
			}
			if ev := w.events(); index(ev, "squeue", 1) >= 0 {
				t.Errorf("the queue was read for a row the owner has not said yes to:\n%s", strings.Join(ev, "\n"))
			}
		})
	}
}

// The owner's go is read again after the row's builds: a date that rolled over while it built is a go for another day, so
// the row refuses (exit 3) with the test binary never run and only the estimate submitted.
func TestTheOwnersGoOfPlgridNowIsReadAgainAfterTheAccountingRowBuilds(t *testing.T) {
	w := newAlphaWorld(t)
	tomorrow := w.dayPlus(1)
	res := w.run([]string{"STUB_NEXT_DATE=" + tomorrow, "STUB_ROLL_AT_TESTC=1"}, "--run", nowAccountingRow, "--yes")
	if res.code != 3 {
		t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
	}
	w.contains(res, "setting LIVE_OWNER_GO_ATHENA_NOW must hold today's UTC date ("+tomorrow+")")
	if strings.Contains(w.stateFile("events"), "testbin ") {
		t.Errorf("the test binary ran with yesterday's go")
	}
	if argv := lines(w.stateFile("sbatch-argv")); len(argv) != 1 || !strings.Contains(argv[0], "--test-only") {
		t.Errorf("sbatch calls %v, want only the estimate", argv)
	}
}

// The alias inherits the row's gates, and the Athena row of plgrid-gpu-a100 keeps none of them: `--run gpu-accounting` with
// no go is refused as the row is, before anything is built; `gpu-accounting-athena` runs by its name with no go, holds no
// slot and prints no note about one, and the alias never runs it.
func TestTheGPUAccountingAliasInheritsThePlgridNowGatesAndTheAthenaRowHasNone(t *testing.T) {
	w := newAlphaWorld(t)
	w.alphaSettings(func(m map[string]string) { delete(m, "LIVE_OWNER_GO_ATHENA_NOW") })
	res := w.run(nil, "--run", "gpu-accounting", "--yes")
	if res.code != 3 {
		t.Fatalf("the alias with no go: exit %d, want 3:\n%s", res.code, res.out)
	}
	w.contains(res, nowAccountingRow+": may wait up to 10 min to start", "setting LIVE_OWNER_GO_ATHENA_NOW must hold today's UTC date")
	if w.stateFile("go-argv") != "" || w.stateFile("sbatch-argv") != "" {
		t.Errorf("a refused run built or called the cluster: %s %s", w.stateFile("go-argv"), w.stateFile("sbatch-argv"))
	}

	// The same settings run the Athena row of plgrid-gpu-a100 by its own name.
	w = newAlphaWorld(t)
	w.alphaSettings(func(m map[string]string) { delete(m, "LIVE_OWNER_GO_ATHENA_NOW") })
	res = w.run(nil, "--run", "gpu-accounting-athena", "--yes")
	if res.code != 0 {
		t.Fatalf("gpu-accounting-athena with no go: exit %d, want 0:\n%s", res.code, res.out)
	}
	w.contains(res, "gpu-accounting-athena: PASS", "gpu-accounting-athena: cluster run (no one-job slot)")
	if strings.Contains(res.out, "owner's go") || strings.Contains(res.out, "plgrid-now") {
		t.Errorf("a note about the owner's go or plgrid-now on the row that is on plgrid-gpu-a100:\n%s", res.out)
	}
}

// The runbook says what the package and the script say, in each table the plgrid-now rows appear in: not that the row is
// named there, but its hold, its slot, its start wait, its go and its profile, each worked out from the package's row or
// the script's table. A row whose hold changes in one place and not the other fails here.
func TestTheRunbookStatesTheAccountingRowOnPlgridNowAsTheRowHasIt(t *testing.T) {
	r, ok := alpha.RowByName(nowAccountingRow)
	if !ok {
		t.Fatalf("the package has no row %s", nowAccountingRow)
	}
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "docs", "alpha-runbook.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	// section is the text under the heading, to the next heading of the same level.
	section := func(heading string) string {
		i := strings.Index(text, "\n"+heading+"\n")
		if i < 0 {
			t.Fatalf("the runbook has no section %q", heading)
		}
		s := text[i+1:]
		if j := strings.Index(s[len(heading):], "\n## "); j >= 0 {
			s = s[:len(heading)+j]
		}
		return s
	}
	// tableLine is the line of a table in the section whose first cell is exactly the cell given.
	tableLine := func(sec, cell string) string {
		for _, l := range strings.Split(sec, "\n") {
			if strings.HasPrefix(l, "| "+cell+" |") {
				return l
			}
		}
		t.Errorf("no table line starts with %s", cell)
		return ""
	}
	// listedLine is the line of a table in the section whose first cell lists the row among others.
	listedLine := func(sec, marker string) string {
		for _, l := range strings.Split(sec, "\n") {
			if cell, _, _ := strings.Cut(strings.TrimPrefix(l, "|"), "|"); strings.HasPrefix(l, "|") && strings.Contains(cell, marker) {
				return l
			}
		}
		t.Errorf("no table line lists %s", marker)
		return ""
	}
	row := "`" + nowAccountingRow + "`"
	hold := int(r.SlotHold / time.Minute)
	wait := int(r.StartWait / time.Minute)

	// "Alpha rows and beta rows": alpha's, ready, under the shipped profile, with the alias that stands for it.
	l := tableLine(section("## Alpha rows and beta rows"), row)
	for _, want := range []string{"| alpha |", "`ready`", "`profiles/" + r.ProfileName() + ".yaml`", "`gpu-accounting`"} {
		if !strings.Contains(l, want) {
			t.Errorf("the beta-rows table line of the row lacks %q: %s", want, l)
		}
	}
	// "What a run ends as": its bound is the start wait the package gives it.
	l = listedLine(section("## What a run ends as"), row)
	if want := fmt.Sprintf("start wait, %d min", wait); !strings.Contains(l, want) {
		t.Errorf("the never-started table line of the row lacks %q: %s", want, l)
	}
	// "State of the wiring": built, on plgrid-now, behind its gates.
	l = tableLine(section("## State of the wiring"), row)
	for _, want := range []string{"`plgrid-now`", "gates", "never run on a cluster"} {
		if !strings.Contains(l, want) {
			t.Errorf("the wiring table line of the row lacks %q: %s", want, l)
		}
	}
	// The slot table: one job, the slot is plgrid-now's and the hold is the package's.
	slots := section("## The one-job slot, the jobs a run holds, and the owner's go")
	l = tableLine(slots, row)
	for _, want := range []string{fmt.Sprintf("about %d minutes at most", hold), fmt.Sprintf("%d-minute start wait", wait), "`" + r.Partition + "`", "the shared slot"} {
		if !strings.Contains(l, want) {
			t.Errorf("the slot table line of the row lacks %q: %s", want, l)
		}
	}
	if !strings.HasPrefix(strings.TrimPrefix(l, "| "+row+" | "), fmt.Sprintf("%d,", r.MostJobs)) {
		t.Errorf("the slot table says another number of jobs than the package's %d: %s", r.MostJobs, l)
	}
	// The owner's go: the setting is the package's, the settings table lists the row under it, and the note the owner reads
	// before saying yes carries the hold the script's table rounds up to ten minutes.
	l = tableLine(section("## Settings"), "`"+r.OwnerGo+"`")
	if !strings.Contains(l, nowAccountingRow) {
		t.Errorf("the settings table does not list the row under %s: %s", r.OwnerGo, l)
	}
	sb, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	var rounded string
	for _, g := range block(t, string(sb), "OWNER GO") {
		if f := strings.Split(g, "|"); f[0] == nowAccountingRow {
			var m int
			if _, err := fmt.Sscanf(f[2], "%d", &m); err != nil {
				t.Fatalf("owner-go row %q: %v", g, err)
			}
			rounded = fmt.Sprintf("%d h %02d min", m/60, m%60)
		}
	}
	if rounded == "" {
		t.Fatalf("the script's owner-go table has no row %s", nowAccountingRow)
	}
	if want := fmt.Sprintf("may wait up to %d min to start (then it is cancelled by its tag and the run is NOT RUN), then holds the plgrid-now slot for about %s; the owner's go is needed", wait, rounded); !strings.Contains(slots, want) {
		t.Errorf("the runbook's owner's-go paragraph does not carry the row's note %q", want)
	}
}
