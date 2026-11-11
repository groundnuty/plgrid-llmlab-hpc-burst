package livechecks

// Alpha graduates on Athena alone (the owner's decision of 11 Nov 2026, "Athena alone", recorded on #145 and approved on
// PR #373; issue #386), so the Helios rows are beta's: kept in every table, ready and runnable by their own names, but in no
// alias, so that a Helios row that is skipped or lacks its settings cannot fail alpha's run. #376's two deployment rows on
// plgrid-now are named in the script as planned (pending) and in the runbook with their gates, and run nothing yet.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// betaRows are the rows that moved to beta: the Helios stack, accounting and reach rows, the ones issue #386 names and the
// aliases ran. conformance-helios, conformance-helios-gh200 and real-engine are not among them: no alias ever ran them, and
// whether their criteria are already met is not decided here (HBEP-4, Graduation criteria); they stay as they were.
var betaRows = []string{"alpha-helios-nooverlap", "tunnel-hour-helios", "gpu-accounting-helios", "reach-helios", "reach-host-dialed-helios"}

// plannedRows are #376's P3 and P4: named, not built.
var plannedRows = []string{"alpha-athena-now-serve", "alpha-athena-now-nooverlap"}

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
	for _, r := range betaRows {
		beta[r] = true
	}
	ready := map[string]bool{}
	for _, r := range readRows(t) {
		if r.status == "ready" {
			ready[r.name] = true
		}
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
			if !strings.HasSuffix(r, "-athena") {
				t.Errorf("alias %s stands for %s, which is not an Athena row", alias, r)
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
		{"gpu-accounting", "gpu-accounting-athena", newAlphaWorld},
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
	}
	for _, want := range []string{"not yet built", "#378", "#380", "#145", "LIVE_OWNER_GO_ATHENA_NOW", "#376"} {
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
