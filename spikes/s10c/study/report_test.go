package study

import (
	"os"
	"reflect"
	"strings"
	"testing"

	b "github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10b/study"
)

// The reader and CellsCSV are inverse: S10b's committed cells.csv, read and
// written again, is the same file byte for byte, so a cell that passes
// through the CSV is rounded as the committed ones are.
func TestReaderRoundTripsS10bCells(t *testing.T) {
	want, err := os.ReadFile(s10bResults + "/cells.csv")
	if err != nil {
		t.Fatal(err)
	}
	if got := b.CellsCSV(committed(t)); got != string(want) {
		t.Error("cells.csv read and written again differs from the committed file")
	}
}

func TestReaderRejectsABadFile(t *testing.T) {
	dir := t.TempDir()
	good, _ := os.ReadFile(s10bResults + "/cells.csv")
	lines := strings.Split(string(good), "\n")
	for name, text := range map[string]string{
		"missing column": strings.Replace(string(good), "hit_tokens", "hits", 1),
		"bad number":     lines[0] + "\n" + strings.Replace(lines[1], ",11324,", ",many,", 1) + "\n",
		"empty":          "",
	} {
		p := dir + "/" + strings.ReplaceAll(name, " ", "-") + ".csv"
		if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadCells(p); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

// Evaluate repeats b.Evaluate for five options, because b.Evaluate is fixed
// to S10b's four. For those four the two must agree exactly on S10b's
// committed cells; that is what lets option 5 be judged by the same rule.
func TestEvaluateAgreesWithS10b(t *testing.T) {
	cells := committed(t)
	ours, theirs := Evaluate(cells, string(Key)), b.Evaluate(cells, string(Key))
	if len(ours) != 5 || len(theirs) != 4 {
		t.Fatalf("%d verdicts of ours and %d of S10b's, want 5 and 4", len(ours), len(theirs))
	}
	for i := range theirs {
		if !reflect.DeepEqual(ours[i], theirs[i]) {
			t.Errorf("option %s: ours %+v, S10b's %+v", theirs[i].Option, ours[i], theirs[i])
		}
	}
	if ours[4].Option != Option5.Name || ours[4].Cond1All || ours[4].Cond1Tight {
		t.Errorf("option 5 on S10b's cells alone (no data): %+v; with no runs it must not pass condition 1", ours[4])
	}
}

// The tables reproduce S10b's: every row of S10b's README for round robin and
// options 1-4 (full-membership tables and the drain table) appears in the
// combined tables verbatim, built from the committed summary.csv alone.
func TestTablesReproduceS10bREADME(t *testing.T) {
	sum, err := os.ReadFile(s10bResults + "/summary.csv")
	if err != nil {
		t.Fatal(err)
	}
	readme, err := os.ReadFile("../../s10b/README.md")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := ParseSummary(string(sum))
	if err != nil {
		t.Fatal(err)
	}
	// Option 5's rows are absent from S10b's summary; add placeholders by
	// duplicating option 4's, which have the shape the renderer needs.
	for _, r := range rows {
		if r["option"] == "adaptive 1.25/2.0" {
			c := SummaryRow{}
			for k, v := range r {
				c[k] = v
			}
			c["option"] = Option5.Name
			rows = append(rows, c)
		}
	}
	tables, err := CombinedTables(rows, string(Key))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, line := range strings.Split(tables, "\n") {
		if !strings.HasPrefix(line, "| ") || strings.HasPrefix(line, "| option") || strings.HasPrefix(line, "| 5.") {
			continue
		}
		n++
		if !strings.Contains(string(readme), line+"\n") {
			t.Errorf("row not in S10b's README: %s", line)
		}
	}
	if want := 5 * 4; n != want {
		t.Errorf("%d rows checked, want %d (round robin and options 1-4 in each of the 4 tables)", n, want)
	}
}

// Hand-built cells for the five-option rule. Option 5 passes condition 1
// everywhere (peak 48), loses less after the drain than the capped option 3
// (0.62 against 0.22), and loses a little in the scarce regime (0.30 against
// 0.31): the rankings name different options, so by the rule fixed in S10b no
// winner is named; with the scarce hit raised to 0.32 option 5 wins both.
func TestFiveOptionRule(t *testing.T) {
	key := string(Key)
	mk := func(opt string, peak int, drain, scarce float64) []b.Cell {
		var cs []b.Cell
		for _, rg := range b.Regimes {
			c := b.Cell{Regime: rg.Name, Key: key, Option: opt, Seed: 1, DoneOutput: 1, PostDone: 1, PeakRun: peak}
			if rg.Name == b.Drain.Name {
				c.Hit05, c.Hit515 = drain, drain
			}
			if rg.Name == b.Scarce.Name {
				c.HitRate = scarce
			}
			cs = append(cs, c)
		}
		return cs
	}
	var cells []b.Cell
	for _, rg := range b.Regimes {
		cells = append(cells, b.Cell{Regime: rg.Name, Key: "-", Option: b.RoundRobin.Name, Seed: 1, DoneOutput: 1, PostDone: 1, HitRate: 0.3})
	}
	build := func(scarce5 float64) []b.Cell {
		out := append([]b.Cell(nil), cells...)
		out = append(out, mk("c=1.25", 49, 0.5, 0.30)...)
		out = append(out, mk("c=2.0", 75, 0.77, 0.32)...)
		out = append(out, mk("c=2.0+cap48", 48, 0.22, 0.31)...)
		out = append(out, mk("adaptive 1.25/2.0", 49, 0.62, 0.30)...)
		return append(out, mk(Option5.Name, 48, 0.62, scarce5)...)
	}
	cond := func(v b.OptionVerdict) bool { return v.Cond1All }
	q, bd, bs, w := b.Pass(Evaluate(build(0.30), key), cond)
	if strings.Join(q, ",") != "c=2.0+cap48,"+Option5.Name || bd != Option5.Name || bs != "c=2.0+cap48" || w != "" {
		t.Errorf("qualifying %v, best drain %q, best scarce %q, winner %q; want the capped option and option 5, option 5, the capped option, no winner", q, bd, bs, w)
	}
	q, _, _, w = b.Pass(Evaluate(build(0.32), key), cond)
	if w != Option5.Name || len(q) != 2 {
		t.Errorf("with a better scarce hit: qualifying %v, winner %q; want option 5", q, w)
	}
}

// A set without option 5's runs, or without a regime, gets no verdict.
func TestIncompleteSetsGetNoVerdict(t *testing.T) {
	cells := committed(t)
	m := Missing(cells, string(Key))
	if len(m) != 4 {
		t.Errorf("S10b's cells without option 5: %d groups missing, want 4 (option 5 in each regime)", len(m))
	}
	for _, line := range m {
		if !strings.Contains(line, Option5.Name) || !strings.HasSuffix(line, "seeds 1, 2, 3") {
			t.Errorf("unexpected missing group: %s", line)
		}
	}
	if v := VerdictMarkdown(cells, string(Key)); !strings.Contains(v, "INCOMPLETE") {
		t.Errorf("no INCOMPLETE notice without option 5: %s", v)
	}
}

// The CSV files are not quoted, so a name with a comma would shift every
// column after it.
func TestOptionNamesAreSafeInUnquotedCSV(t *testing.T) {
	for _, o := range append(AllOptions(), ControlCapKept) {
		if strings.ContainsAny(o.Name, ",\n\"") {
			t.Errorf("option name %q has a comma, newline or quote", o.Name)
		}
	}
}
