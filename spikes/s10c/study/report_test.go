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

// The verdict is issued only for exactly one cell per required run. Review
// finding B1 of PR #53: Missing checked presence in a set while Evaluate
// averaged every row, so a repeated seed ("-seeds 1,2,3,2") weighted that
// seed twice for option 5 against three seeds of every other option, and
// turned the split into a win for option 5, while the report still said
// "3 seeds". The fixtures below are the committed complete set (S10b's cells
// and option 5's), changed by hand in one way each.

// fullSet is every cell of a verdict on the issue's key: S10b's committed
// cells and this study's committed cells for option 5.
func fullSet(t *testing.T) []b.Cell {
	t.Helper()
	o5, err := ReadCells(ourResults + "/cells.csv")
	if err != nil {
		t.Fatal(err)
	}
	return append(append([]b.Cell(nil), committed(t)...), o5...)
}

// cellsOf returns copies of the cells that match pred, with Seed set to seed
// when seed is not 0.
func cellsOf(cells []b.Cell, pred func(b.Cell) bool, seed uint64) []b.Cell {
	var out []b.Cell
	for _, c := range cells {
		if pred(c) {
			if seed != 0 {
				c.Seed = seed
			}
			out = append(out, c)
		}
	}
	return out
}

func without(cells []b.Cell, pred func(b.Cell) bool) []b.Cell {
	var out []b.Cell
	for _, c := range cells {
		if !pred(c) {
			out = append(out, c)
		}
	}
	return out
}

// The control for everything below: the unchanged set has no problem, gives
// a verdict that is the committed verdict.md byte for byte (that file was
// written before the validation existed), and carries cells of another key
// (S10b's plain "opening" key), which the check must leave out.
func TestACompleteSetGetsTheCommittedVerdict(t *testing.T) {
	cells, key := fullSet(t), string(Key)
	other := 0
	for _, c := range cells {
		if c.Key != key && c.Key != "-" {
			other++
		}
	}
	if other == 0 {
		t.Fatal("the fixture has no cell of another key, so it does not show that they are left out")
	}
	if m, inv := Missing(cells, key), Invalid(cells, key); len(m) != 0 || len(inv) != 0 {
		t.Fatalf("the complete set: missing %v, invalid %v", m, inv)
	}
	want, err := os.ReadFile(ourResults + "/verdict.md")
	if err != nil {
		t.Fatal(err)
	}
	got := "## " + key + "\n\n" + VerdictMarkdown(cells, key)
	if got != string(want) {
		t.Errorf("the verdict of the complete set differs from results/verdict.md:\n%s", got)
	}
	if !strings.Contains(got, "Key "+key+", 3 seeds.") || !strings.Contains(got, "Qualifying on") {
		t.Errorf("no verdict for the complete set:\n%s", got)
	}
}

func TestUnsoundSeedSetsGetNoVerdict(t *testing.T) {
	key := string(Key)
	isO5 := func(c b.Cell) bool { return c.Option == Option5.Name }
	isO5Seed := func(seed uint64) func(b.Cell) bool {
		return func(c b.Cell) bool { return isO5(c) && c.Seed == seed }
	}
	base := fullSet(t)
	for _, tc := range []struct {
		name string
		set  []b.Cell
		want string // the notice, and a line that names the problem
		line string
	}{
		{"seed 2 run twice for option 5 only (-seeds 1,2,3,2)", append(append([]b.Cell(nil), base...), cellsOf(base, isO5Seed(2), 0)...),
			"INVALID", "seed 2 has 2 cells, want 1"},
		{"seed 2 twice for every cell of option 5 and S10b's own rows (all options)", append(append([]b.Cell(nil), base...), cellsOf(base, func(c b.Cell) bool { return c.Seed == 2 && (c.Key == key || c.Key == "-") }, 0)...),
			"INVALID", "seed 2 has 2 cells, want 1"},
		{"one round-robin cell twice", append(append([]b.Cell(nil), base...), cellsOf(base, func(c b.Cell) bool {
			return c.Option == b.RoundRobin.Name && c.Regime == b.Scarce.Name && c.Seed == 1
		}, 0)...), "INVALID", "round-robin: seed 1 has 2 cells"},
		{"option 5 on a fourth seed in every regime", append(append([]b.Cell(nil), base...), cellsOf(base, isO5Seed(3), 4)...),
			"INVALID", "seed 4 is not a run the verdict asks for"},
		{"option 5 on seeds 1, 2 and 4 instead of 1, 2 and 3", append(without(base, isO5Seed(3)), cellsOf(base, isO5Seed(3), 4)...),
			"INCOMPLETE", "seeds 3"},
		{"a fourth seed for one option in one regime (the sets are unequal)", append(append([]b.Cell(nil), base...), cellsOf(base, func(c b.Cell) bool {
			return c.Option == "c=2.0+cap48" && c.Regime == b.Scarce.Name && c.Key == key && c.Seed == 3
		}, 4)...), "INVALID", "scarce / c=2.0+cap48 / key " + key + ": seed 4 is not a run"},
		{"an extra seed for round robin", append(append([]b.Cell(nil), base...), cellsOf(base, func(c b.Cell) bool {
			return c.Option == b.RoundRobin.Name && c.Regime == b.Tight.Name && c.Seed == 1
		}, 9)...), "INVALID", "seed 9 is not a run"},
		{"option 5 missing seed 3 in the scarce regime", without(base, func(c b.Cell) bool { return isO5(c) && c.Regime == b.Scarce.Name && c.Seed == 3 }),
			"INCOMPLETE", "scarce / " + Option5.Name + ": seeds 3"},
		{"a missing seed and a repeated one together", append(without(base, func(c b.Cell) bool { return isO5(c) && c.Regime == b.Scarce.Name && c.Seed == 3 }),
			cellsOf(base, isO5Seed(2), 0)...), "INCOMPLETE", "seed 2 has 2 cells"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := VerdictMarkdown(tc.set, key)
			if !strings.Contains(v, tc.want) || !strings.Contains(v, tc.line) {
				t.Errorf("want %q and a line with %q, got:\n%s", tc.want, tc.line, v)
			}
			for _, verdictText := range []string{"Qualifying on", "passes", "rankings split", "seeds."} {
				if strings.Contains(v, verdictText) {
					t.Errorf("a verdict was issued (%q in the text):\n%s", verdictText, v)
				}
			}
		})
	}
}

// The mutation behind B1 is the dangerous one: without the check, the rule
// applied to S10b's cells and option 5's with seed 2 twice names option 5 the
// winner, where the complete set names none. (Evaluate and Pass are the
// rule; VerdictMarkdown is what refuses to run it.)
func TestARepeatedSeedWouldHaveChangedTheOutcome(t *testing.T) {
	key, cond := string(Key), func(v b.OptionVerdict) bool { return v.Cond1All }
	base := fullSet(t)
	if _, _, _, w := b.Pass(Evaluate(base, key), cond); w != "" {
		t.Fatalf("the complete set names %q the winner, want none (the rankings split)", w)
	}
	dup := append(append([]b.Cell(nil), base...), cellsOf(base, func(c b.Cell) bool { return c.Option == Option5.Name && c.Seed == 2 }, 0)...)
	if _, _, _, w := b.Pass(Evaluate(dup, key), cond); w != Option5.Name {
		t.Errorf("with seed 2 twice for option 5 the rule names %q, want %q: the repeated seed would not have changed the outcome, so the other tests prove less", w, Option5.Name)
	}
	if v := VerdictMarkdown(dup, key); !strings.Contains(v, "INVALID") {
		t.Errorf("no INVALID notice for the repeated seed:\n%s", v)
	}
}

// The combined tables are built from summary rows, which carry the number of
// seeds they were built from: a row of another count is refused, and the
// complete set gives the committed tables.
func TestTablesNeedExactlyTheRequiredSeeds(t *testing.T) {
	sum, err := os.ReadFile(s10bResults + "/summary.csv")
	if err != nil {
		t.Fatal(err)
	}
	ours, err := os.ReadFile(ourResults + "/summary.csv")
	if err != nil {
		t.Fatal(err)
	}
	rows := func() []SummaryRow {
		r1, err := ParseSummary(string(sum))
		if err != nil {
			t.Fatal(err)
		}
		r2, err := ParseSummary(string(ours))
		if err != nil {
			t.Fatal(err)
		}
		return append(r1, r2...)
	}
	good, err := CombinedTables(rows(), string(Key))
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(ourResults + "/combined-tables.md")
	if err != nil {
		t.Fatal(err)
	}
	if good != string(want) {
		t.Error("the tables of the complete set differ from results/combined-tables.md")
	}
	for _, tc := range []struct{ regime, option string }{
		{b.Scarce.Name, Option5.Name},     // a full-membership table
		{b.Drain.Name, "c=2.0+cap48"},     // the drain table, an S10b row
		{b.Tight.Name, b.RoundRobin.Name}, // the baseline of the drain table
		{b.HalfPrefill.Name, "c=1.25"},    // another full-membership row
	} {
		for _, n := range []string{"4", "2"} {
			rs := rows()
			hit := false
			for _, r := range rs {
				if r["regime"] == tc.regime && r["option"] == tc.option && (r["key"] == string(Key) || r["key"] == "-") {
					r["seeds"], hit = n, true
				}
			}
			if !hit {
				t.Fatalf("no row for %s / %s", tc.regime, tc.option)
			}
			if _, err := CombinedTables(rs, string(Key)); err == nil {
				t.Errorf("%s / %s with %s seeds: no error", tc.regime, tc.option, n)
			}
		}
	}
}
