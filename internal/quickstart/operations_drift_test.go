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
//   - every failing reason HBEP-8's table regards an Instance alone with is stated by the guide,
//     whose section on failing reasons says an Instance that fails ends with one of them. A reason
//     that regards other kinds too, such as HeartbeatMissed, is not an end reason of an Instance
//     and is not in that section.

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

// failsAnInstance is true for a failing reason that HBEP-8's table regards an Instance alone with.
func failsAnInstance(e reasondrift.TableEntry) bool {
	return e.Action == "failing" && slices.Equal(e.Kinds, []string{"Instance"})
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
		if failsAnInstance(e) && !stated[guideEvent{e.Action, e.Reason}] {
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
		if failsAnInstance(e) && slices.Contains(events, guideEvent{"failing", e.Reason}) {
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
