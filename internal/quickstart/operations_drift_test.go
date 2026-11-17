package quickstart

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/reasondrift"
)

// The guide is held to the two lists the drift guard (#239, internal/reasondrift) holds the code
// to, as its parsers read them from the HBEPs: HBEP-7's ranking of the Degraded reasons, and
// HBEP-8's table of reasons in alpha. The expected sets come from the HBEPs, never from the guide,
// so the guide, the code and the HBEPs agree through the HBEPs:
//   - the guide's Degraded headings are HBEP-7's ranking, in its order;
//   - every event the guide states, a type and a reason, is a row of HBEP-8's table with that type;
//   - every failing reason HBEP-8's table regards an Instance with, kinds beyond it or not, is stated
//     by the guide, whose section on failing reasons says an Instance that fails ends with one of
//     them. The reasons of HBEP-8's Observers table are left out (HeartbeatMissed today): an observer
//     going stale does not end the Instance it regards, so it is not in that section. A test pins
//     that set to the table's text.

// guideEvent is an HBEP-8 event type with a reason.
type guideEvent struct{ action, reason string }

// guideEvents returns the events the guide states: each "`<type>` event `<Reason>`" of its prose,
// with "or `<Reason>`" for a second reason, and each reason of its table of failing reasons, whose
// events the guide says are failing ones. The types are HBEP-8's, as the drift guard names them.
func guideEvents(t *testing.T) []guideEvent {
	t.Helper()
	stated := regexp.MustCompile("`(" + strings.Join(reasondrift.Actions, "|") + ")` event `([A-Z][A-Za-z]*)`(?: or `([A-Z][A-Za-z]*)`)?")
	var out []guideEvent
	for _, s := range opsSegments(t) {
		if s.fenced {
			continue
		}
		for _, m := range stated.FindAllStringSubmatch(s.text, -1) {
			out = append(out, guideEvent{m[1], m[2]})
			if m[3] != "" {
				out = append(out, guideEvent{m[1], m[3]})
			}
		}
	}
	if len(out) == 0 {
		t.Fatal("the guide states no event as `<type>` event `<Reason>`")
	}

	doc := readRepoFile(t, opsGuide)
	start := strings.Index(doc, "\n### Its failing reasons\n")
	if start < 0 {
		t.Fatal("the guide has no section \"Its failing reasons\"")
	}
	section := doc[start+1:]
	if end := strings.Index(section[4:], "\n#"); end >= 0 {
		section = section[:end+4]
	}
	if !strings.Contains(section, "and a `failing` event with the same reason") {
		t.Fatal("the section on failing reasons no longer says their events are failing ones")
	}
	name := regexp.MustCompile("`([A-Z][A-Za-z]*)`")
	rows := 0
	for _, l := range strings.Split(section, "\n") {
		if !strings.HasPrefix(l, "| `") {
			continue
		}
		rows++
		first := strings.SplitN(strings.TrimPrefix(l, "|"), "|", 2)[0]
		for _, m := range name.FindAllStringSubmatch(first, -1) {
			out = append(out, guideEvent{"failing", m[1]})
		}
	}
	if rows == 0 {
		t.Fatal("found no row in the table of failing reasons")
	}
	return out
}

// guideDegradedHeadings returns the reasons the guide's section on a degraded ModelDeployment has
// a heading for, in its order.
func guideDegradedHeadings(t *testing.T) []string {
	t.Helper()
	doc := readRepoFile(t, opsGuide)
	start := strings.Index(doc, "\n## Why a ModelDeployment is degraded\n")
	if start < 0 {
		t.Fatal("the guide has no section \"Why a ModelDeployment is degraded\"")
	}
	section := doc[start+1:]
	if end := strings.Index(section[3:], "\n## "); end >= 0 {
		section = section[:end+3]
	}
	var out []string
	for _, m := range regexp.MustCompile("(?m)^### `([A-Za-z]+)`\n").FindAllStringSubmatch(section, -1) {
		out = append(out, m[1])
	}
	if len(out) == 0 {
		t.Fatal("found no heading in the section on a degraded ModelDeployment")
	}
	return out
}

// failsAnInstance is true for a failing reason that HBEP-8's table regards an Instance with, among
// other kinds or alone, unless it is one of the observers' reasons (the reasons of HBEP-8's
// Observers table): a stale observer is not the end of what it regards. A reason that regards
// more kinds than the Instance, such as an Allocation in beta, is one the guide still states.
func failsAnInstance(e reasondrift.TableEntry, observers []string) bool {
	return e.Action == "failing" && slices.Contains(e.Kinds, "Instance") && !slices.Contains(observers, e.Reason)
}

// driftProblems sets the guide's headings and events against what the HBEPs state, as the drift
// guard's parsers read them, and returns one line for each difference.
func driftProblems(docs *reasondrift.Docs, headings []string, events []guideEvent) []string {
	var out []string
	if !slices.Equal(headings, docs.Ranking) {
		out = append(out, fmt.Sprintf("the guide's Degraded headings\n%v\nare not HBEP-7's ranking\n%v", headings, docs.Ranking))
	}
	table := map[guideEvent]bool{}
	for _, e := range docs.Table {
		table[guideEvent{e.Action, e.Reason}] = true
	}
	stated := map[guideEvent]bool{}
	for _, ev := range events {
		stated[ev] = true
		if !table[ev] {
			out = append(out, fmt.Sprintf("the guide states a %s event %s, which HBEP-8's table does not list with that type", ev.action, ev.reason))
		}
	}
	for _, e := range docs.Table {
		if failsAnInstance(e, docs.ObserverReasons) && !stated[guideEvent{e.Action, e.Reason}] {
			out = append(out, fmt.Sprintf("HBEP-8 lists a failing event %s regarding an Instance, which the guide does not state", e.Reason))
		}
	}
	return out
}

func loadDriftDocs(t *testing.T) *reasondrift.Docs {
	t.Helper()
	docs, err := reasondrift.LoadDocs(root)
	if err != nil {
		t.Fatalf("the drift guard cannot read the HBEPs: %v", err)
	}
	return docs
}

// The guide follows the HBEPs as the drift guard reads them.
func TestOperationsGuideFollowsTheDriftGuard(t *testing.T) {
	for _, p := range driftProblems(loadDriftDocs(t), guideDegradedHeadings(t), guideEvents(t)) {
		t.Error(p)
	}
}

// The comparison fails on the faults it exists for, run on the guide's own headings and events with
// one change each: a reason renamed, a reason missing, a heading missing and two swapped. A
// comparison that passed them would hold the guide to nothing.
func TestOperationsDriftComparisonFailsOnAMutant(t *testing.T) {
	docs := loadDriftDocs(t)
	heads, events := guideDegradedHeadings(t), guideEvents(t)
	if p := driftProblems(docs, heads, events); len(p) != 0 {
		t.Fatalf("the guide itself differs from the HBEPs, so a mutant proves nothing:\n%s", strings.Join(p, "\n"))
	}
	// A failing reason the guide states in its table of failing reasons, which HBEP-8 regards an
	// Instance with.
	var victim string
	for _, e := range docs.Table {
		if failsAnInstance(e, docs.ObserverReasons) && slices.Contains(events, guideEvent{"failing", e.Reason}) {
			victim = e.Reason
			break
		}
	}
	if victim == "" {
		t.Fatal("HBEP-8 lists no failing reason regarding an Instance that the guide states")
	}
	renamed := slices.Clone(events)
	for i, ev := range renamed {
		if ev == (guideEvent{"failing", victim}) {
			renamed[i].reason = victim + "Renamed"
		}
	}
	// The same reason under another type: the type is checked too.
	retyped := slices.Clone(events)
	for i, ev := range retyped {
		if ev == (guideEvent{"failing", victim}) {
			retyped[i].action = "ready"
		}
	}
	missing := slices.DeleteFunc(slices.Clone(events), func(ev guideEvent) bool { return ev == guideEvent{"failing", victim} })
	dropped := slices.Delete(slices.Clone(heads), 0, 1)
	swapped := slices.Clone(heads)
	swapped[0], swapped[1] = swapped[1], swapped[0]

	for _, m := range []struct {
		name  string
		heads []string
		evs   []guideEvent
		want  string // a text the failure must hold
	}{
		{"a reason renamed", heads, renamed, victim + "Renamed"},
		{"a reason missing", heads, missing, "lists a failing event " + victim},
		{"a reason under another type", heads, retyped, "states a ready event " + victim},
		{"a heading missing", dropped, events, "not HBEP-7's ranking"},
		{"two headings swapped", swapped, events, "not HBEP-7's ranking"},
	} {
		got := strings.Join(driftProblems(docs, m.heads, m.evs), "\n")
		if !strings.Contains(got, m.want) {
			t.Errorf("%s: the comparison reports\n%s\nwant a line with %q", m.name, got, m.want)
		}
	}
}

// observerReasonsInTheTable reads the reasons HBEP-8's Observers table raises, from the text of the
// table, without the drift guard's parser (ParseObserverReasons), so that a test of the exclusion
// does not check the parser against itself: the rows are the lines of pipes under the lead-in
// "**Observers.**", the column is the one headed "Event when stale", and a reason is the backticked
// CamelCase word that the cell follows with ", regarding".
func observerReasonsInTheTable(t *testing.T) []string {
	t.Helper()
	lines := strings.Split(readRepoFile(t, reasondrift.HBEP8Path), "\n")
	start := -1
	for i, l := range lines {
		if strings.HasPrefix(l, "**Observers.**") {
			start = i
		}
	}
	if start < 0 {
		t.Fatal("HBEP-8 has no \"**Observers.**\" lead-in")
	}
	reason := regexp.MustCompile("`([A-Z][A-Za-z0-9]*)`, regarding")
	column := -1
	seen := map[string]bool{}
	rows := 0
	for _, l := range lines[start+1:] {
		if !strings.HasPrefix(l, "|") {
			if column >= 0 {
				break // the table ended
			}
			continue
		}
		cells := strings.Split(l, "|")
		if column < 0 {
			for i, c := range cells {
				if strings.TrimSpace(c) == "Event when stale" {
					column = i
				}
			}
			if column < 0 {
				t.Fatalf("the first table row after the Observers lead-in is not the header: %q", l)
			}
			continue
		}
		if strings.Contains(l, "---") {
			continue // the separator line
		}
		rows++
		found := reason.FindStringSubmatch(cells[column])
		if found == nil {
			t.Fatalf("the Observers table's row %d has no reason in its \"Event when stale\" cell: %q", rows, cells[column])
		}
		seen[found[1]] = true
	}
	if rows == 0 {
		t.Fatal("found no row in the Observers table")
	}
	var out []string
	for r := range seen {
		out = append(out, r)
	}
	slices.Sort(out)
	return out
}

// The reasons the guide's check leaves out are those of HBEP-8's Observers table, by the table's
// text: a reason added to the table is left out at once, and one that is not in the table never is.
func TestTheObserverReasonsLeftOutAreTheObserversTables(t *testing.T) {
	want := observerReasonsInTheTable(t)
	got := loadDriftDocs(t).ObserverReasons
	if !slices.Equal(got, want) {
		t.Errorf("the check leaves out %v, HBEP-8's Observers table raises %v", got, want)
	}
	if !slices.Contains(want, "HeartbeatMissed") {
		t.Errorf("the Observers table raises %v, and not HeartbeatMissed, which this test was written for", want)
	}
}

// An observer's reason is no end reason of an Instance, so the guide is not required to state it:
// the exclusion decides that. Run on the guide's events with its statements of HeartbeatMissed
// removed (the real guide states it, in its table of the Instance's conditions and in its own
// entry, so the real comparison passes with or without the exclusion today), the comparison passes
// with the exclusion and, without it, reports exactly HeartbeatMissed.
func TestTheObserverExclusionSparesTheGuideAStatement(t *testing.T) {
	docs := loadDriftDocs(t)
	heads, events := guideDegradedHeadings(t), guideEvents(t)
	if p := driftProblems(docs, heads, events); len(p) != 0 {
		t.Fatalf("the guide itself differs from the HBEPs, so a mutant proves nothing:\n%s", strings.Join(p, "\n"))
	}
	if !slices.Contains(events, guideEvent{"failing", "HeartbeatMissed"}) {
		t.Fatal("the guide no longer states a failing event HeartbeatMissed: this test removes what it states")
	}
	unstated := slices.DeleteFunc(slices.Clone(events), func(ev guideEvent) bool { return ev.reason == "HeartbeatMissed" })
	if p := driftProblems(docs, heads, unstated); len(p) != 0 {
		t.Errorf("with the exclusion, a guide that does not state HeartbeatMissed is reported: %q", p)
	}
	without := *docs
	without.ObserverReasons = nil
	got := driftProblems(&without, heads, unstated)
	const want = "HBEP-8 lists a failing event HeartbeatMissed regarding an Instance, which the guide does not state"
	if len(got) != 1 || got[0] != want {
		t.Errorf("without the exclusion the comparison reports %q, want only %q", got, want)
	}
}
