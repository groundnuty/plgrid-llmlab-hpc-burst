package conformance

// The first line of the text a reach row posts, written out for each row (`@hpc-burst-science-agent`, 12 Nov 2026, on #145): the
// text goes to #145 and names the row's origin issue, #225, which is closed, so it is named and not posted to; the Helios row's
// goes under #145's section for beta. The lines are not read from the rows' fields, so a target or an origin that changes
// fails here on the text that is posted. The rest of the text is the run's own and is judged by the reach tests.

import (
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
)

func TestTheReachTextPostsToTheAlphaListAndNamesItsOriginIssue(t *testing.T) {
	for row, want := range map[string]string{
		"reach-athena": "reach-athena: PASS (posts to #145; origin issue #225)",
		"reach-helios": `reach-helios: PASS (posts to #145 under "Beta: when GH200 returns"; origin issue #225)`,
	} {
		r, ok := alpha.RowByName(row)
		if !ok {
			t.Errorf("no row %s", row)
			continue
		}
		posts := reachPosts(r, "the label", "PASS", "refused", "", "plgrid-now (the partition's own; none requested)", 1, 1, "1Gi")
		if first, _, _ := strings.Cut(posts, "\n"); first != want {
			t.Errorf("%s: the first line of the text to post is %q, want %q", row, first, want)
		}
	}
}
