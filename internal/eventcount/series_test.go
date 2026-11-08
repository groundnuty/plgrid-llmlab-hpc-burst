package eventcount_test

// The series each controller creates at 0 (HBEP-8, Raised events: series are
// created at 0 for every reason, action and type that the lifecycle
// controller, the front and a managed Provider can raise) checked against
// HBEP-8's own text, as internal/reasondrift parses it: the Reasons table for
// the lifecycle controller and the front together, the managed catalog
// controller's bullet for a managed Provider. The split of the table between
// the two controllers is the drift guard's, against their emit sites.

import (
	"path/filepath"
	"sort"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/lifecycle"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/reasondrift"
)

func pairs(series ...[]eventcount.Series) []string {
	set := map[string]bool{}
	for _, ss := range series {
		for _, s := range ss {
			set[s.Action+"/"+s.Reason] = true
		}
	}
	out := make([]string, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

func diff(t *testing.T, what string, got, want []string) {
	t.Helper()
	in := func(xs []string) map[string]bool {
		m := map[string]bool{}
		for _, x := range xs {
			m[x] = true
		}
		return m
	}
	g, w := in(got), in(want)
	for _, x := range want {
		if !g[x] {
			t.Errorf("%s: HBEP-8 lists %s, which no series at 0 has", what, x)
		}
	}
	for _, x := range got {
		if !w[x] {
			t.Errorf("%s: a series at 0 for %s, which HBEP-8 does not list", what, x)
		}
	}
}

func TestTheSeriesAtZeroAreHBEP8s(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	docs, err := reasondrift.LoadDocs(root)
	if err != nil {
		t.Fatal(err)
	}
	var table []string
	for _, e := range docs.Table {
		table = append(table, e.Action+"/"+e.Reason)
	}
	diff(t, "the lifecycle controller and the front", pairs(lifecycle.RaisedEvents(), front.RaisedEvents()), table)
	var catalog []string
	for _, e := range docs.Managed {
		catalog = append(catalog, e.Action+"/"+e.Reason)
	}
	diff(t, "a managed Provider", pairs(managed.RaisedEvents()), catalog)
}
