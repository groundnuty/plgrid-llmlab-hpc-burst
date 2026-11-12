package alpha

// Where a row posts (Row.PostsTo). The science agent's ruling of 12 Nov 2026 on #145, after the accounting row's first live
// run: its place is #145 alone, because #93, its second target, is closed and was about the hold lift. The same target stood on
// the other accounting rows and on the handover row, and stood for the same reason, so all four post to #145 alone.

import (
	"regexp"
	"slices"
	"strings"
	"testing"
)

// The four rows that named #93, and the target each has now, written out.
func TestTheRowsThatNamedIssue93PostToTheAlphaListAlone(t *testing.T) {
	for _, name := range []string{"alpha-athena-handover", "gpu-accounting-helios", "gpu-accounting-athena", "gpu-accounting-athena-now"} {
		r, ok := RowByName(name)
		if !ok {
			t.Errorf("no row %s", name)
			continue
		}
		if r.PostsTo != "#145" {
			t.Errorf("%s posts to %q, want %q alone", name, r.PostsTo, "#145")
		}
	}
}

// openPostTargets are the issues a row may post to, as of 12 Nov 2026, written out and not read from GitHub: #145, the alpha
// list, which every row's result belongs to; #376, the plgrid-now hour rows' plan and build, which rows A and B and the
// pre-staging row post to as well; and #94, the Helios real-engine run, which alpha-helios-nooverlap names.
var openPostTargets = []string{"#145", "#376", "#94"}

// keptClosedPostTargets are the closed issues two groups of rows still name: #225, the reach rows' own issue, and #305, the
// host-dialed rows'. They were not part of the ruling of 12 Nov 2026, which named the accounting row and its #93; whether
// those two post to #145 alone is for the science agent, and the list here is the open question made visible, not a
// rule that they may stay. The test fails if one of these rows stops naming its issue, so the entry is removed with the fix.
var keptClosedPostTargets = map[string][]string{
	"reach-helios":             {"#225"},
	"reach-athena":             {"#225"},
	"reach-host-dialed-helios": {"#305"},
	"reach-host-dialed-athena": {"#305"},
}

var issueRef = regexp.MustCompile(`#[0-9]+`)

// postTargetProblems is what is wrong with the rows' post targets: an issue that is neither one of the open ones nor a kept
// closed one of that very row, and a kept entry its row no longer needs. An issue is closed by being absent from
// openPostTargets, so a new row, or a new target, has to be added there on purpose.
func postTargetProblems(rows []Row, open []string, kept map[string][]string) []string {
	var out []string
	named := map[string]map[string]bool{}
	for _, r := range rows {
		named[r.Name] = map[string]bool{}
		for _, ref := range issueRef.FindAllString(r.PostsTo, -1) {
			named[r.Name][ref] = true
			if !slices.Contains(open, ref) && !slices.Contains(kept[r.Name], ref) {
				out = append(out, r.Name+" posts to "+ref+", which is not one of the open issues "+strings.Join(open, ", ")+" (a closed one, or not listed)")
			}
		}
	}
	for name, refs := range kept {
		for _, ref := range refs {
			if !named[name][ref] {
				out = append(out, name+" no longer names "+ref+": remove it from the kept closed targets")
			}
		}
	}
	slices.Sort(out)
	return out
}

func allRowsOfEveryKind() []Row {
	return slices.Concat(Rows(), AccountingRows(), ReachRows(), HostDialedRows(), StageRows())
}

// No row posts to a closed issue, but the ones the science agent has not yet ruled on, which are listed by name.
func TestNoRowPostsToAClosedIssue(t *testing.T) {
	for _, p := range postTargetProblems(allRowsOfEveryKind(), openPostTargets, keptClosedPostTargets) {
		t.Error(p)
	}
}

// The check itself: a row that names #93 again, a target nobody listed, a kept entry that went stale and a kept entry on
// the wrong row are each found, and a row that posts to open issues is not.
func TestThePostTargetCheckFindsAClosedTargetAndAStaleException(t *testing.T) {
	row := func(name, posts string) Row { return Row{Name: name, PostsTo: posts} }
	open := []string{"#145", "#376"}
	for _, c := range []struct {
		name string
		rows []Row
		kept map[string][]string
		want []string
	}{
		{"open targets only", []Row{row("a", "#145"), row("b", "#145 and #376")}, nil, nil},
		{"a closed target", []Row{row("a", "#145 and #93")}, nil, []string{"a posts to #93, which is not one of the open issues #145, #376 (a closed one, or not listed)"}},
		{"a target nobody listed", []Row{row("a", "#12 and #145")}, nil, []string{"a posts to #12, which is not one of the open issues #145, #376 (a closed one, or not listed)"}},
		{"a kept one is that row's own", []Row{row("a", "#225 and #145")}, map[string][]string{"a": {"#225"}}, nil},
		{"a kept one on another row", []Row{row("a", "#225 and #145"), row("b", "#225")}, map[string][]string{"a": {"#225"}},
			[]string{"b posts to #225, which is not one of the open issues #145, #376 (a closed one, or not listed)"}},
		{"a kept one that went stale", []Row{row("a", "#145")}, map[string][]string{"a": {"#225"}}, []string{"a no longer names #225: remove it from the kept closed targets"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := postTargetProblems(c.rows, open, c.kept)
			if strings.Join(got, "\n") != strings.Join(c.want, "\n") {
				t.Errorf("problems %q, want %q", got, c.want)
			}
		})
	}
}
