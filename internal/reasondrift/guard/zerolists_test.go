package guard_test

import (
	"fmt"
	"slices"
	"sort"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/lifecycle"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/reasondrift"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// pair is one series of a controller without its controller and type: an event
// of this action with this reason.
type pair struct{ action, reason string }

func (p pair) String() string { return p.action + "/" + p.reason }

// wantType is HBEP-8's type of an event of an action (Events, "type"): Warning
// for degraded and failing, Normal for the other four.
func wantType(action string) string {
	if action == "degraded" || action == "failing" {
		return provider.EventKindWarning
	}
	return provider.EventKindOrdinary
}

// emitPairs returns the (action, reason) pairs of the emit sites of the given
// packages. Each site's reasons and actions are the sets that reach it, so a
// site with several of each gives their product, as the table comparison of
// the drift guard reads them. A site the scan could not resolve, or that only
// builds an event, is returned in bad: nothing is read from it.
func emitPairs(code *reasondrift.Code, pkgs ...string) (pairs map[pair]bool, bad []string) {
	pairs = map[pair]bool{}
	for _, e := range code.Emits {
		if !slices.Contains(pkgs, e.Package) {
			continue
		}
		if e.Unresolved != "" || e.Unproven != "" {
			bad = append(bad, fmt.Sprintf("%s.%s at %s is unresolved (%q) or unproven (%q)", e.Package, e.Func, e.Pos, e.Unresolved, e.Unproven))
			continue
		}
		for _, r := range e.Reasons {
			for _, a := range e.Actions {
				pairs[pair{a, r}] = true
			}
		}
	}
	return pairs, bad
}

// managedPairs returns what the managed Provider's controller can raise, as the
// drift guard's comparison with HBEP-8's bullet reads it: the reasons it can
// write into the conditions it watches, each a bad state (with the type its map
// of bad states gives) or the ready event of a cleared condition, and the typed
// events it builds that are not shown to reach a recorder, in internal/managed
// and in pkg/provider, which builds the catalog's. pkg/provider/fake is a test
// double and internal/providerctl the batch Provider, which has no zero list.
// Its events from emitTransitions are unresolved by design; every other emit
// site of those packages that is unresolved is returned in bad.
func managedPairs(code *reasondrift.Code) (pairs map[pair]bool, bad []string) {
	pairs = map[pair]bool{}
	for _, w := range code.ManagedWrites {
		action := "ready"
		if a, ok := code.ManagedBad[w.Reason]; ok {
			action = a
		}
		pairs[pair{action, w.Reason}] = true
	}
	for _, e := range code.Emits {
		if e.Package != "internal/managed" && e.Package != "pkg/provider" {
			continue
		}
		if e.Unresolved != "" {
			if e.Package == "internal/managed" && e.Func == "Controller.emitTransitions" {
				continue
			}
			bad = append(bad, fmt.Sprintf("%s.%s at %s is unresolved: %s", e.Package, e.Func, e.Pos, e.Unresolved))
			continue
		}
		if e.Unproven == "" {
			bad = append(bad, fmt.Sprintf("%s.%s at %s raises an event the managed controller's list does not read", e.Package, e.Func, e.Pos))
			continue
		}
		for _, r := range e.Reasons {
			for _, a := range e.Actions {
				pairs[pair{a, r}] = true
			}
		}
	}
	return pairs, bad
}

// zeroDiff compares a controller's list of series created at 0 with the pairs
// its emit sites raise, both ways, and each series' type with HBEP-8's rule. It
// returns one line for each difference, sorted; none means they agree.
func zeroDiff(list []eventcount.Series, want map[pair]bool) []string {
	var out []string
	have := map[pair]bool{}
	for _, s := range list {
		p := pair{s.Action, s.Reason}
		switch {
		case have[p]:
			out = append(out, "listed twice: "+p.String())
		case !want[p]:
			out = append(out, "listed, and no emit site raises it: "+p.String())
		}
		have[p] = true
		if s.Type != wantType(s.Action) {
			out = append(out, fmt.Sprintf("type of %s is %s, HBEP-8 gives %s", p, s.Type, wantType(s.Action)))
		}
	}
	for p := range want {
		if !have[p] {
			out = append(out, "raised, and not listed: "+p.String())
		}
	}
	sort.Strings(out)
	return out
}

// zeroLists are the controllers' lists of series created at 0, each with the
// packages whose emit sites it must equal: the lifecycle controller's with
// cmd/hpcburst-controller's (the tunnel-state reader raises TunnelOrphaned
// through the lifecycle controller's recorder), the front's, and a managed
// Provider's.
type zeroList struct {
	name  string
	list  func() []eventcount.Series
	pairs func(*reasondrift.Code) (map[pair]bool, []string)
}

var zeroLists = []zeroList{
	{"lifecycle", lifecycle.RaisedEvents, func(c *reasondrift.Code) (map[pair]bool, []string) {
		return emitPairs(c, "internal/lifecycle", "cmd/hpcburst-controller")
	}},
	{"front", front.RaisedEvents, func(c *reasondrift.Code) (map[pair]bool, []string) {
		return emitPairs(c, "internal/front")
	}},
	{"managed", managed.RaisedEvents, managedPairs},
}

// TestZeroListsMatchTheEmitSites: each controller's series created at 0 equal,
// both ways, the (action, reason) pairs its emit sites raise, and carry HBEP-8's
// type. A reason added to a controller without its list shows a real 0 nowhere
// until the event is raised, which is the absence the counter exists to rule
// out (HBEP-8, Raised events).
func TestZeroListsMatchTheEmitSites(t *testing.T) {
	code := realScan(t)
	for _, z := range zeroLists {
		t.Run(z.name, func(t *testing.T) {
			want, bad := z.pairs(code)
			for _, b := range bad {
				t.Errorf("%s: %s", z.name, b)
			}
			if len(want) == 0 {
				t.Fatalf("%s: the scan found no emit sites to compare with", z.name)
			}
			for _, d := range zeroDiff(z.list(), want) {
				t.Errorf("%s: %s", z.name, d)
			}
		})
	}
}

// TestAZeroListThatDiffersFails is the mutant check of the comparison: a copy of
// each list, edited in memory, is reported as exactly the difference made.
func TestAZeroListThatDiffersFails(t *testing.T) {
	code := realScan(t)
	for _, c := range []struct {
		name    string
		dropped pair // the series dropped from the copy
	}{
		{"lifecycle", pair{"started", "AgentRegistered"}},
		{"front", pair{"degraded", "CountsDrift"}},
		{"managed", pair{"failing", "Expired"}},
	} {
		z := zeroLists[slices.IndexFunc(zeroLists, func(x zeroList) bool { return x.name == c.name })]
		want, _ := z.pairs(code)
		list := z.list()
		if d := zeroDiff(list, want); len(d) != 0 {
			t.Fatalf("%s: the unchanged list differs: %v", c.name, d)
		}
		at := slices.IndexFunc(list, func(s eventcount.Series) bool { return s.Action == c.dropped.action && s.Reason == c.dropped.reason })
		if at < 0 {
			t.Fatalf("%s: the list has no %s to drop", c.name, c.dropped)
		}
		kept := slices.Concat(list[:at:at], list[at+1:])

		// A reason dropped, written down before the run: it is raised and not listed.
		got := zeroDiff(kept, want)
		if wantOne := []string{"raised, and not listed: " + c.dropped.String()}; !slices.Equal(got, wantOne) {
			t.Errorf("%s with %s dropped: %v, want %v", c.name, c.dropped, got, wantOne)
		}
		// A phantom series, a repeated one and a wrong type are each their own line.
		phantom := append(slices.Clone(list), eventcount.Series{Reason: "PhantomReason", Action: "failing", Type: provider.EventKindWarning})
		if got, wantOne := zeroDiff(phantom, want), []string{"listed, and no emit site raises it: failing/PhantomReason"}; !slices.Equal(got, wantOne) {
			t.Errorf("%s with a phantom: %v, want %v", c.name, got, wantOne)
		}
		twice := append(slices.Clone(list), list[at])
		if got, wantOne := zeroDiff(twice, want), []string{"listed twice: " + c.dropped.String()}; !slices.Equal(got, wantOne) {
			t.Errorf("%s with a series twice: %v, want %v", c.name, got, wantOne)
		}
		flipped := slices.Clone(list)
		flipped[at].Type = map[string]string{provider.EventKindWarning: provider.EventKindOrdinary, provider.EventKindOrdinary: provider.EventKindWarning}[flipped[at].Type]
		wantFlip := fmt.Sprintf("type of %s is %s, HBEP-8 gives %s", c.dropped, flipped[at].Type, wantType(c.dropped.action))
		if got := zeroDiff(flipped, want); !slices.Equal(got, []string{wantFlip}) {
			t.Errorf("%s with a flipped type: %v, want %v", c.name, got, wantFlip)
		}
	}
}
