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
// pre-staging row post to as well; #94, the Helios real-engine run, which alpha-helios-nooverlap names; and, since 13 Nov 2026,
// #522, beta's tracker, which the beta rows post to (#521). No closed issue is
// on it, and no row has an exception: the reach and host-dialed rows post to #145 and name their closed origin issue in the
// text (Row.Origin; the science agent's ruling of 12 Nov 2026 on #145).
var openPostTargets = []string{"#145", "#376", "#94", "#522"}

var issueRef = regexp.MustCompile(`#[0-9]+`)

// postTargetProblems is what is wrong with the rows' post targets: an issue that is not one of the open ones, and a row that
// posts to its own origin issue, which is closed and is named in the text, never posted to. An issue is closed by being absent
// from open, so a new row, or a new target, has to be added there on purpose.
func postTargetProblems(rows []Row, open []string) []string {
	var out []string
	for _, r := range rows {
		for _, ref := range issueRef.FindAllString(r.PostsTo, -1) {
			if !slices.Contains(open, ref) {
				out = append(out, r.Name+" posts to "+ref+", which is not one of the open issues "+strings.Join(open, ", ")+" (a closed one, or not listed)")
			}
			if r.Origin != "" && ref == r.Origin {
				out = append(out, r.Name+" posts to its origin issue "+ref+", which is closed: the text names it and does not post to it")
			}
		}
	}
	slices.Sort(out)
	return out
}

func allRowsOfEveryKind() []Row {
	return slices.Concat(Rows(), AccountingRows(), ReachRows(), HostDialedRows(), StageRows())
}

// No row posts to a closed issue, and none has an exception.
func TestNoRowPostsToAClosedIssue(t *testing.T) {
	for _, p := range postTargetProblems(allRowsOfEveryKind(), openPostTargets) {
		t.Error(p)
	}
}

// The reach and host-dialed rows (the science agent's ruling of 12 Nov 2026, on #145): each posts to #145, names the closed issue
// it was built under in the text, and never posts to it; the Helios ones go under #145's section for beta. Written out per row.
func TestTheReachRowsPostToTheAlphaListAndNameTheirOriginIssue(t *testing.T) {
	for _, c := range []struct{ name, origin, note string }{
		{"reach-athena", "#225", "posts to #145; origin issue #225"},
		{"reach-helios", "#225", `posts to #145 under "Beta: when GH200 returns"; origin issue #225`},
		{"reach-host-dialed-athena", "#305", "posts to #145; origin issue #305"},
		{"reach-host-dialed-helios", "#305", `posts to #145 under "Beta: when GH200 returns"; origin issue #305`},
	} {
		r, ok := RowByName(c.name)
		if !ok {
			t.Errorf("no row %s", c.name)
			continue
		}
		if r.PostsTo != "#145" || r.Origin != c.origin || r.PostsNote() != c.note {
			t.Errorf("%s: PostsTo %q, Origin %q, note %q; want %q, %q, %q", c.name, r.PostsTo, r.Origin, r.PostsNote(), "#145", c.origin, c.note)
		}
	}
}

// The check itself: a row that names #93 again, a target nobody listed, and a row that posts to its own origin issue are each
// found, and a row that posts to open issues, or names a closed origin in its text only, is not.
func TestThePostTargetCheckFindsAClosedTargetAndAnOriginPostedTo(t *testing.T) {
	row := func(name, posts, origin string) Row { return Row{Name: name, PostsTo: posts, Origin: origin} }
	open := []string{"#145", "#376"}
	for _, c := range []struct {
		name string
		rows []Row
		want []string
	}{
		{"open targets only", []Row{row("a", "#145", ""), row("b", "#145 and #376", "")}, nil},
		{"a closed origin named in the text only", []Row{row("a", "#145", "#225")}, nil},
		{"a closed target", []Row{row("a", "#145 and #93", "")}, []string{"a posts to #93, which is not one of the open issues #145, #376 (a closed one, or not listed)"}},
		{"a target nobody listed", []Row{row("a", "#12 and #145", "")}, []string{"a posts to #12, which is not one of the open issues #145, #376 (a closed one, or not listed)"}},
		{"a row posting to its origin issue", []Row{row("a", "#225 and #145", "#225")}, []string{
			"a posts to #225, which is not one of the open issues #145, #376 (a closed one, or not listed)",
			"a posts to its origin issue #225, which is closed: the text names it and does not post to it"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := postTargetProblems(c.rows, open)
			if strings.Join(got, "\n") != strings.Join(c.want, "\n") {
				t.Errorf("problems %q, want %q", got, c.want)
			}
		})
	}
}
