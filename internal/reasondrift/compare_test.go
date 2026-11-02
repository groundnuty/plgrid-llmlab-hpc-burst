package reasondrift

import (
	"slices"
	"strings"
	"testing"
)

func keysOf(ms []Mismatch) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.Key()
	}
	slices.Sort(out)
	return out
}

func key(kind, action, reason, regarding, pkg, fn string) string {
	return Mismatch{Kind: kind, Action: action, Reason: reason, Regarding: regarding, Package: pkg, Func: fn}.Key()
}

// emit builds a resolved emit site.
func emit(pkg, fn, action, reason string, kinds ...string) Emit {
	return Emit{Package: pkg, Func: fn, Pos: pkg + "/x.go:1", Reasons: []string{reason}, Actions: []string{action}, Kinds: kinds}
}

// A base where the documents and the code agree, written out by hand: two
// reasons with constants, one of them raised in two places.
func base() (*Docs, *Code) {
	d := &Docs{
		Table: []TableEntry{
			{Action: "failing", Reason: "Lost", Kinds: []string{"Instance"}},
			{Action: "degraded", Reason: "Slow", Kinds: []string{"Lease", "ModelDeployment"}},
		},
		Ranking: []string{"Slow", "Lost"},
	}
	c := &Code{
		Constants: map[string][]string{"Lost": {"ReasonLost"}, "Slow": {"ReasonSlow"}},
		Emits: []Emit{
			emit("internal/a", "f", "failing", "Lost", "Instance"),
			emit("internal/a", "g", "degraded", "Slow", "ModelDeployment"),
			emit("internal/b", "h", "degraded", "Slow", "Lease"),
		},
		Ranking: []string{"Slow", "Lost"},
	}
	return d, c
}

func TestCompareAgreeing(t *testing.T) {
	d, c := base()
	if got := Compare(d, c); len(got) != 0 {
		t.Errorf("an agreeing base differs: %v", got)
	}
}

func TestCompareFindsEachKindOfMismatch(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Docs, *Code)
		want   []string
		detail string // part of the first mismatch's Detail
	}{
		{
			"a table reason with no constant",
			func(d *Docs, c *Code) { delete(c.Constants, "Lost") },
			[]string{key(NoConstant, "", "Lost", "", "", "")}, "no Reason or EventReason constant",
		},
		{
			"a ranking reason with no constant",
			func(d *Docs, c *Code) { d.Ranking = append(d.Ranking, "Extra"); c.Ranking = append(c.Ranking, "Extra") },
			[]string{key(NoConstant, "", "Extra", "", "", "")}, "",
		},
		{
			"a table row no code emits",
			func(d *Docs, c *Code) {
				d.Table = append(d.Table, TableEntry{Action: "ready", Reason: "Fine", Kinds: []string{"Instance"}})
				c.Constants["Fine"] = []string{"ReasonFine"}
			},
			[]string{key(TableOnly, "ready", "Fine", "Instance", "", "")}, "no emit site raises Fine",
		},
		{
			"a reason the code raises as another type",
			func(d *Docs, c *Code) { d.Table[0].Action = "degraded" },
			[]string{
				key(CodeOnly, "failing", "Lost", "Instance", "internal/a", ""),
				key(TableOnly, "degraded", "Lost", "Instance", "", ""),
			}, "",
		},
		{
			"a reason the code raises for another kind",
			func(d *Docs, c *Code) { d.Table[1].Kinds = []string{"ModelDeployment"} },
			[]string{key(CodeOnly, "degraded", "Slow", "Lease", "internal/b", "")}, "emitted at internal/b/x.go:1",
		},
		{
			"a kind the table lists and the code does not raise",
			func(d *Docs, c *Code) { d.Table[1].Kinds = []string{"Lease", "ModelDeployment", "Provider"} },
			[]string{key(TableOnly, "degraded", "Slow", "Provider", "", "")}, "the code raises Slow as degraded regarding Lease; degraded regarding ModelDeployment",
		},
		{
			"an emit site of a reason the table lacks",
			func(d *Docs, c *Code) {
				c.Emits = append(c.Emits, emit("internal/c", "k", "failing", "New", "Instance"), emit("internal/d", "k", "failing", "New", "Instance"))
			},
			[]string{
				key(CodeOnly, "failing", "New", "Instance", "internal/c", ""),
				key(CodeOnly, "failing", "New", "Instance", "internal/d", ""),
			}, "",
		},
		{
			"an emit site that cannot be resolved",
			func(d *Docs, c *Code) {
				c.Emits = append(c.Emits, Emit{Package: "internal/m", Func: "T.f", Pos: "internal/m/e.go:9", Unresolved: "the reason: a field"})
			},
			[]string{key(Unresolved, "", "", "", "internal/m", "T.f")}, "the reason: a field (internal/m/e.go:9)",
		},
		{
			"a ranking reason only the HBEP holds",
			func(d *Docs, c *Code) { c.Ranking = []string{"Slow"} },
			[]string{key(RankingHBEPOnly, "", "Lost", "", "", "")}, "",
		},
		{
			"a ranking reason only the code holds",
			func(d *Docs, c *Code) { d.Ranking = []string{"Slow"} },
			[]string{key(RankingCodeOnly, "", "Lost", "", "", "")}, "",
		},
		{
			"a ranking in another order",
			func(d *Docs, c *Code) { c.Ranking = []string{"Lost", "Slow"} },
			[]string{key(RankingOrder, "", "", "", "", "")}, "HBEP-7 ranks [Slow Lost], the code ranks [Lost Slow]",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, code := base()
			c.mutate(d, code)
			got := Compare(d, code)
			if want := slices.Clone(c.want); !slices.Equal(keysOf(got), sortedCopy(want)) {
				t.Fatalf("mismatches %v, want %v", keysOf(got), sortedCopy(want))
			}
			if c.detail != "" && !strings.Contains(got[0].Detail, c.detail) {
				t.Errorf("detail %q lacks %q", got[0].Detail, c.detail)
			}
		})
	}
}

func sortedCopy(s []string) []string {
	s = slices.Clone(s)
	slices.Sort(s)
	return s
}

func TestApplyTakesAllowancesOffAndFindsStaleOnes(t *testing.T) {
	ms := []Mismatch{
		{Kind: NoConstant, Reason: "A"},
		{Kind: NoConstant, Reason: "B"},
		{Kind: CodeOnly, Action: "failing", Reason: "C", Regarding: "Instance", Package: "internal/x"},
		{Kind: CodeOnly, Action: "failing", Reason: "D", Regarding: "Instance", Package: "internal/y"},
	}
	exceptions := []Entry{
		{Kind: CodeOnly, Package: "internal/x", Why: "a whole package"},
		{Kind: NoConstant, Reason: "Gone", Why: "matches nothing"},
	}
	drift := []Entry{
		{Kind: NoConstant, Reason: "A", Why: "one reason"},
		{Kind: CodeOnly, Package: "internal/x", Why: "already an exception, so unreachable"},
	}
	left, stale := Apply(ms, exceptions, drift)
	if want := []string{key(NoConstant, "", "B", "", "", ""), key(CodeOnly, "failing", "D", "Instance", "internal/y", "")}; !slices.Equal(keysOf(left), sortedCopy(want)) {
		t.Errorf("left %v, want %v", keysOf(left), want)
	}
	if len(stale) != 2 || stale[0].Reason != "Gone" || stale[1].Why != "already an exception, so unreachable" {
		t.Errorf("stale %v, want the entry for Gone and the unreachable one", stale)
	}
}

func TestAnEntryMatchesOnTheFieldsItNames(t *testing.T) {
	m := Mismatch{Kind: CodeOnly, Action: "failing", Reason: "R", Regarding: "Instance", Package: "internal/x", Func: "T.f", Detail: "d"}
	for _, e := range []Entry{{}, {Kind: CodeOnly}, {Reason: "R", Package: "internal/x"}, {Func: "T.f"}, {Kind: CodeOnly, Action: "failing", Reason: "R", Regarding: "Instance", Package: "internal/x", Func: "T.f"}} {
		if !e.Matches(m) {
			t.Errorf("%v does not match %v", e, m)
		}
	}
	for _, e := range []Entry{{Kind: TableOnly}, {Action: "degraded"}, {Reason: "S"}, {Regarding: "Lease"}, {Package: "internal/y"}, {Func: "T.g"}} {
		if e.Matches(m) {
			t.Errorf("%v matches %v", e, m)
		}
	}
}
