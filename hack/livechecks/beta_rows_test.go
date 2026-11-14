package livechecks

// Alpha graduates on Athena alone (the owner's decision of 11 Nov 2026, "Athena alone", recorded on #145 and approved on
// PR #373; issue #386), so the Helios rows are beta's: kept in every table, ready and runnable by their own names, but in no
// alias, so that a Helios row that is skipped or lacks its settings cannot fail alpha's run. #376's two deployment rows on
// plgrid-now are named in the script as planned (pending) and in the runbook with their gates, and run nothing yet.

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
)

// betaRows are the rows that moved to beta: the eight Helios rows. Five are the stack, accounting and reach rows that
// issue #386 names and the aliases ran. The other three, conformance-helios, conformance-helios-gh200 and real-engine, are
// the science agent's ruling on #386 (11 Nov 2026): Helios's conformance was met on 8 Nov 2026 and stays met (HBEP-4;
// #373 keeps the met parts met), and real-engine's remaining item, #94 (the agent's registration through the dial-home
// path on GH200), is beta's. No alias ever ran those three, and none may.
var betaRows = []string{"alpha-helios-nooverlap", "tunnel-hour-helios", "gpu-accounting-helios", "reach-helios", "reach-host-dialed-helios",
	"conformance-helios", "conformance-helios-gh200", "real-engine"}

// betaNowRows are the beta rows on plgrid-now (issue #521; beta's tracker is #522): built, ready in the script and runnable by their own
// names, in no alias, and in the runbook's table as beta rows.
var betaNowRows = []string{"beta-athena-now-router", "beta-athena-now-keeper"}

// pendingAlphaRows are the rows internal/alpha has and the script holds as pending (issue #521): built, judged and dry-run in the package, and
// refused by the script with exit 2 ("is pending and cannot run yet") until the live path they need exists. beta-athena-now-bulk's needs the engine
// double staged on the cluster (docs/alpha-runbook.md, The runs). A tests that compares the script's tables with the package's rows leaves them out;
// the one that lists the script's rows says they are pending and refused. Removing a row from this list and from the script's pending cell is the LAST
// step of making it ready, not the whole of it: the rest is the wiring the keeper row got in the commit that added it to the script, which the drift tests
// that iterate the script's cluster shapes run through the script and so cannot be pre-wired while the row is pending (the live table's line with its test
// pattern and inputs, the shapes, plan stack, most-jobs, owner's-go and front-label tables; alpha_test.go's shape row, rows list and owner's-go note;
// the dry faults and rows of alpha_dry_test.go; budget_test.go's stackRowTests; both maps of neverstarted_test.go; frontlabel_test.go's labelledRows;
// bindir_test.go's stackRowCount; plan_test.go's three expected sets; betaNowRows above; the //livecheck: directive of the row's test; and the host-dialed
// mode world.command gives the keeper row, which names that row alone).
var pendingAlphaRows = []string{"beta-athena-now-bulk"}

// builtRows are the package's rows the script runs: Rows() less the pending ones.
func builtRows() []alpha.Row {
	var out []alpha.Row
	for _, r := range alpha.Rows() {
		if !slices.Contains(pendingAlphaRows, r.Name) {
			out = append(out, r)
		}
	}
	return out
}

// plannedRows are the rows named and not built for a live run: #376's are all built (row B is P4's, row A P3's), and the bulk row of #521 is pending.
var plannedRows = pendingAlphaRows

// builtNowRows are #376's rows that are built: ready in the script, and so in the runbook's table.
var builtNowRows = []string{"alpha-athena-now-serve", "alpha-athena-now-nooverlap"}

func readAliases(t *testing.T) map[string][]string {
	t.Helper()
	b, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]string{}
	for _, l := range block(t, string(b), "ALIASES") {
		f := strings.Split(l, "|")
		if len(f) != 2 {
			t.Fatalf("alias row %q has %d fields, want 2", l, len(f))
		}
		out[f[0]] = strings.Split(f[1], ",")
	}
	return out
}

// No alias stands for a beta row, and each stands for rows that exist and are ready: the aliases are alpha's.
func TestNoAliasStandsForABetaRow(t *testing.T) {
	beta := map[string]bool{}
	for _, r := range append(append([]string{}, betaRows...), betaNowRows...) {
		beta[r] = true
	}
	ready := map[string]bool{}
	for _, r := range readRows(t) {
		if r.status == "ready" {
			ready[r.name] = true
		}
	}
	host := map[string]string{}
	sb, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range block(t, string(sb), "CLUSTER SHAPES") {
		f := strings.Split(l, "|")
		host[f[0]] = f[1]
	}
	aliases := readAliases(t)
	if len(aliases) != 4 {
		t.Errorf("%d aliases, want tunnel-hour, gpu-accounting, reach and reach-host-dialed", len(aliases))
	}
	for alias, rows := range aliases {
		if len(rows) == 0 {
			t.Errorf("alias %s stands for no row", alias)
		}
		for _, r := range rows {
			if beta[r] {
				t.Errorf("alias %s stands for %s, a beta row: a skipped Helios row would fail alpha's run", alias, r)
			}
			if !ready[r] {
				t.Errorf("alias %s stands for %s, which is not a ready row", alias, r)
			}
			// An Athena row is one the cluster table puts on the athena host. Not the name's ending: the accounting row on
			// plgrid-now is gpu-accounting-athena-now (issue #145), and an alias may stand for it.
			if host[r] != "athena" {
				t.Errorf("alias %s stands for %s, which is not an Athena row (the cluster table's host is %q)", alias, r, host[r])
			}
		}
	}
}

// The beta rows are kept: each is in the live table, ready, with its shape, and so runnable by name; the registry and the
// shape tests compare them with the package. A row removed from the tables to hide it would fail here.
func TestTheBetaRowsStayReadyAndRunnableByName(t *testing.T) {
	shapes := map[string]bool{}
	for _, n := range readShapeNames(t) {
		shapes[n] = true
	}
	status := map[string]string{}
	for _, r := range readRows(t) {
		status[r.name] = r.status
	}
	for _, r := range betaRows {
		if status[r] != "ready" {
			t.Errorf("beta row %s has status %q, want ready", r, status[r])
		}
		if !shapes[r] {
			t.Errorf("beta row %s has no cluster shape", r)
		}
	}
	w := newWorld(t)
	res := w.run(nil, "--list")
	for _, r := range betaRows {
		w.contains(res, r+"  cluster  ready")
	}
}

// An alias run asks nothing of Helios: with no Helios account, credentials or owner's go in the settings, each alias still runs
// its Athena row and passes. This is the evidence that a Helios row can no longer fail alpha's run by lacking what it needs.
func TestAnAliasRunNeedsNoHeliosSetting(t *testing.T) {
	noHelios := func(m map[string]string) {
		for _, k := range []string{"ACCOUNT_HELIOS_CPU", "ACCOUNT_HELIOS_GH200", "LIVE_CONF_CREDENTIALS_HELIOS", "LIVE_OWNER_GO_HELIOS_NOW"} {
			delete(m, k)
		}
	}
	for _, c := range []struct {
		alias, row string
		world      func(t *testing.T) *world
	}{
		{"tunnel-hour", "tunnel-hour-athena", newAlphaWorld},
		{"gpu-accounting", "gpu-accounting-athena-now", newAlphaWorld},
		{"reach", "reach-athena", newReachWorld},
		{"reach-host-dialed", "reach-host-dialed-athena", hostDialedWorld},
	} {
		t.Run(c.alias, func(t *testing.T) {
			w := c.world(t)
			w.alphaSettings(func(m map[string]string) {
				noHelios(m)
				if c.alias == "reach-host-dialed" {
					for _, k := range []string{"LIVE_DIALHOME_MODE", "LIVE_DIALHOME_ADDR", "LIVE_PROBE_HOST", "LIVE_PROBE_PORT", "LIVE_PROBE_LABEL"} {
						delete(m, k)
					}
				}
			})
			if c.alias == "reach" {
				w.reachSettings(noHelios)
			}
			res := w.run(nil, "--run", c.alias, "--yes")
			if res.code != 0 || !strings.Contains(res.out, c.row+": PASS") {
				t.Fatalf("exit %d with no Helios setting, want %s: PASS:\n%s", res.code, c.row, res.out)
			}
		})
	}
	// The control: a Helios row, by its own name, does need them, so the test above shows an absence and not a row that
	// asks for nothing.
	w := newAlphaWorld(t)
	w.alphaSettings(noHelios)
	if res := w.run(nil, "--run", "gpu-accounting-helios", "--yes"); res.code != 3 {
		t.Errorf("gpu-accounting-helios with no Helios setting: exit %d, want 3 (a missing setting):\n%s", res.code, res.out)
	}
}

// The planned rows are named, pending and refused: --list shows them, --run says they cannot run yet (exit 2), and
// nothing is built or asked of a cluster.
func TestThePlannedPlgridNowRowsArePendingAndRefused(t *testing.T) {
	w := newWorld(t)
	list := w.run(nil, "--list")
	// The built ones are ready, with their gate: a row that is no longer planned is no longer pending.
	for _, r := range builtNowRows {
		w.contains(list, r+"  cluster  ready  gate HPCBURST_ALPHA_ROW")
	}
	for _, r := range plannedRows {
		w.contains(list, r+"  cluster  pending")
		res := w.run(nil, "--run", r, "--yes")
		if res.code != 2 {
			t.Errorf("%s: exit %d, want 2 (pending, cannot run yet):\n%s", r, res.code, res.out)
		}
		w.contains(res, r+" is pending and cannot run yet")
	}
}

// The runbook is in step with the script: its section on alpha and beta rows names every beta row, says that the aliases stand
// for Athena rows, names the two planned rows as not yet built with their gates, and says where the owner's go for them
// comes from.
func TestTheRunbookIsInStepWithTheTables(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "docs", "alpha-runbook.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	i := strings.Index(text, "\n## Alpha rows and beta rows\n")
	if i < 0 {
		t.Fatal("the runbook has no section \"Alpha rows and beta rows\"")
	}
	section := text[i+1:]
	if j := strings.Index(section[3:], "\n## "); j >= 0 {
		section = section[:j+3]
	}
	for _, r := range betaRows {
		if !strings.Contains(section, "`"+r+"`") {
			t.Errorf("the runbook's section does not name the beta row %s", r)
		}
	}
	for alias, rows := range readAliases(t) {
		if !regexp.MustCompile("`" + regexp.QuoteMeta(alias) + "`[^\n]*`" + regexp.QuoteMeta(rows[0]) + "`").MatchString(section) {
			t.Errorf("the runbook's section does not say that the alias %s stands for %s", alias, rows[0])
		}
	}
	for _, r := range plannedRows {
		if !strings.Contains(section, "`"+r+"`") {
			t.Errorf("the runbook's section does not name the planned row %s", r)
		}
		// Its line of the table says that it is not built, and that the script holds it as pending.
		found := false
		for _, l := range strings.Split(section, "\n") {
			if strings.HasPrefix(l, "|") && strings.Contains(l, "`"+r+"`") {
				found = true
				if !strings.Contains(l, "not yet built") || !strings.Contains(l, "`pending`") {
					t.Errorf("the runbook's table line of %s does not say \"not yet built\" and \"pending\": %s", r, l)
				}
			}
		}
		if !found {
			t.Errorf("the runbook's table has no line for the planned row %s", r)
		}
	}
	// The three rows that moved by the science agent's ruling have their reasons on their own table lines.
	for _, c := range []struct{ row, reason string }{
		{"conformance-helios", "met on 8 Nov 2026"},
		{"conformance-helios-gh200", "met on 8 Nov 2026"},
		{"real-engine", "#94"},
	} {
		found := false
		for _, l := range strings.Split(section, "\n") {
			if strings.HasPrefix(l, "|") && strings.Contains(l, "`"+c.row+"`") {
				found = true
				if !strings.Contains(l, "**beta**") || !strings.Contains(l, c.reason) {
					t.Errorf("the runbook's table line of %s does not say beta and %q: %s", c.row, c.reason, l)
				}
			}
		}
		if !found {
			t.Errorf("the runbook's table has no line for %s", c.row)
		}
	}
	for _, r := range builtNowRows {
		found := false
		for _, l := range strings.Split(section, "\n") {
			if strings.HasPrefix(l, "|") && strings.Contains(l, "`"+r+"`") {
				found = true
				if strings.Contains(l, "not yet built") || !strings.Contains(l, "`ready`") {
					t.Errorf("the runbook's table line of %s does not say it is built and `ready`: %s", r, l)
				}
			}
		}
		if !found {
			t.Errorf("the runbook's table has no line for the built row %s", r)
		}
	}
	for _, want := range []string{"#378", "#380", "#145", "LIVE_OWNER_GO_ATHENA_NOW", "#376"} {
		if !strings.Contains(section, want) {
			t.Errorf("the runbook's section does not say %q", want)
		}
	}
	// The aliases paragraph at the top says Athena, and no stale line about a skipped Helios row failing the run is left.
	head := text[:strings.Index(text, "\n## What a run ends as")]
	if strings.Contains(head, "-helios` and `-athena` rows") {
		t.Error("the runbook's opening still says the aliases stand for a -helios and an -athena row")
	}
}

// The beta rows on plgrid-now are built and ready in the script, runnable by their own names with the same gate as the alpha rows, in no
// alias, and each has its line in the runbook's table as a beta row, its section of "The runs" and its line among the rows whose job
// never starts (issue #521).
func TestTheBetaRowsOnPlgridNowAreReadyInTheScriptAndInTheRunbook(t *testing.T) {
	w := newWorld(t)
	list := w.run(nil, "--list")
	aliases := readAliases(t)
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "docs", "alpha-runbook.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	i := strings.Index(text, "\n## Alpha rows and beta rows\n")
	if i < 0 {
		t.Fatal("the runbook has no section \"Alpha rows and beta rows\"")
	}
	section := text[i+1:]
	if j := strings.Index(section[3:], "\n## "); j >= 0 {
		section = section[:j+3]
	}
	for _, r := range betaNowRows {
		w.contains(list, r+"  cluster  ready  gate HPCBURST_ALPHA_ROW")
		for alias, rows := range aliases {
			if slices.Contains(rows, r) {
				t.Errorf("the alias %s stands for %s, a beta row: a beta row is run by its own name", alias, r)
			}
		}
		found := false
		for _, l := range strings.Split(section, "\n") {
			if strings.HasPrefix(l, "|") && strings.Contains(l, "`"+r+"`") {
				found = true
				for _, want := range []string{"**beta**", "built", "`ready`", "in no alias", "#522"} {
					if !strings.Contains(l, want) {
						t.Errorf("the runbook's table line of %s lacks %q: %s", r, want, l)
					}
				}
			}
		}
		if !found {
			t.Errorf("the runbook's table has no line for the beta row %s", r)
		}
		if !strings.Contains(text, "\n### `"+r+"`\n") {
			t.Errorf("the runbook has no section for %s in \"The runs\"", r)
		}
		never := false
		for _, l := range strings.Split(text, "\n") {
			// The line lists the rows that share the bound, so the row is one of the names before its second cell.
			if cells := strings.Split(l, "|"); len(cells) > 2 && strings.Contains(cells[1], "`"+r+"`") && strings.HasPrefix(strings.TrimSpace(cells[2]), "start wait, 10 min") {
				never = true
			}
		}
		if !never {
			t.Errorf("the runbook's table of the rows whose job never starts has no line for %s", r)
		}
	}
}
