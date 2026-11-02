// Package reasondrift guards HBEP-8's event reasons and HBEP-7's Degraded
// ranking against the code (#239).
//
// The documents are read here, strictly: a table or list that cannot be parsed
// is an error, never an empty result (the lesson of #237). The code side is read
// by internal/reasondrift/scan, which finds the emit sites by reference with
// go/packages and go/ssa. Compare sets the two against each other, and the
// exception lists in exceptions.go and knowndrift.go hold the only allowances.
//
// Docs and its parsers need no type-checker, so another test (the operations
// guide, #240) can import this package to check the reasons its text names.
package reasondrift

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// Actions are HBEP-8's event types, which go in an event's action (HBEP-8,
// Events, Form).
var Actions = []string{"started", "ready", "degraded", "failing", "ending-soon", "replaced"}

// Kinds are the kinds of object an event in alpha can regard (HBEP-8, Reasons
// in alpha, Regarding column). A Regarding cell names its kinds by these words.
var Kinds = []string{"Allocation", "Instance", "Lease", "ModelDeployment", "PlatformProfile", "Provider"}

// TableEntry is one reason of HBEP-8's "Reasons in alpha" table under one
// type: the kinds of object it regards in the table's rows, merged when the
// table lists the reason under the type more than once.
type TableEntry struct {
	Action string
	Reason string
	Kinds  []string
}

// Docs is what the two HBEPs state.
type Docs struct {
	// Table is HBEP-8's "Reasons in alpha" table, one entry per (type,
	// reason), sorted by type and reason.
	Table []TableEntry
	// Ranking is HBEP-7's ranking of the Degraded reasons, first to last.
	Ranking []string
}

// Paths of the two HBEPs, relative to the repository root.
const (
	HBEP8Path = "hbep/0008-observability-and-events/README.md"
	HBEP7Path = "hbep/0007-lifecycle-controller/README.md"
)

// LoadDocs reads and parses both HBEPs under the repository root.
func LoadDocs(root string) (*Docs, error) {
	md8, err := os.ReadFile(filepath.Join(root, HBEP8Path))
	if err != nil {
		return nil, fmt.Errorf("reading HBEP-8: %w", err)
	}
	md7, err := os.ReadFile(filepath.Join(root, HBEP7Path))
	if err != nil {
		return nil, fmt.Errorf("reading HBEP-7: %w", err)
	}
	return ParseDocs(md8, md7)
}

// ParseDocs parses the text of HBEP-8 and HBEP-7.
func ParseDocs(hbep8, hbep7 []byte) (*Docs, error) {
	table, err := ParseEventTable(hbep8)
	if err != nil {
		return nil, fmt.Errorf("HBEP-8: %w", err)
	}
	ranking, err := ParseRanking(hbep7)
	if err != nil {
		return nil, fmt.Errorf("HBEP-7: %w", err)
	}
	return &Docs{Table: table, Ranking: ranking}, nil
}

// TableReasons returns the distinct reasons of the table, sorted.
func (d *Docs) TableReasons() []string {
	seen := map[string]bool{}
	for _, e := range d.Table {
		seen[e.Reason] = true
	}
	out := make([]string, 0, len(seen))
	for r := range seen {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

// Table markers in HBEP-8: the line of the bold lead-in that precedes each
// table.
const (
	reasonsMarker   = "**Reasons in alpha.**"
	observersMarker = "**Observers.**"
)

var (
	parenthetical = regexp.MustCompile(`\([^()]*\)`)
	reasonToken   = regexp.MustCompile("^`([A-Z][A-Za-z0-9]*)`$")
	kindWord      = regexp.MustCompile(`\b(` + strings.Join(Kinds, "|") + `)\b`)
	separatorCell = regexp.MustCompile(`^:?-{3,}:?$`)
	// reasonsRow is the shape of a row of the reasons table: its first cell is
	// one of HBEP-8's six types in backticks.
	reasonsRow  = regexp.MustCompile("^\\|\\s*`(" + strings.Join(Actions, "|") + ")`\\s*\\|")
	rankingName = regexp.MustCompile("^`([A-Z][A-Za-z0-9]*)`")
)

// wantReasonsHeader is the header HBEP-8's reasons table must have.
var wantReasonsHeader = []string{"Type", "Reasons", "Regarding", "Emitter"}

// ParseEventTable reads HBEP-8's "Reasons in alpha" table. It returns an error
// for a table it cannot read: a missing marker or table, a different header, a
// row of the wrong width, a type that is not one of HBEP-8's six, a reasons
// cell with anything but reason names (and parenthetical remarks), or a
// Regarding cell that names no kind.
//
// A Regarding cell that points at "(Observers)" takes the kinds of the
// Observers table's "Event when stale" column, where HBEP-8 states them.
func ParseEventTable(md []byte) ([]TableEntry, error) {
	lines := strings.Split(string(md), "\n")
	header, rows, start, end, err := tableAfter(lines, reasonsMarker)
	if err != nil {
		return nil, fmt.Errorf("the reasons table: %w", err)
	}
	// No other line of the document is a row of this table: a row cut off from
	// it, or a second copy, would be left out of the parse.
	for i, l := range lines {
		if (i < start || i >= end) && reasonsRow.MatchString(strings.TrimSpace(l)) {
			return nil, fmt.Errorf("line %d looks like a row of the reasons table and lies outside it: %q", i+1, clip(strings.TrimSpace(l)))
		}
	}
	if !slices.Equal(header, wantReasonsHeader) {
		return nil, fmt.Errorf("the reasons table's header is %q, want %q", header, wantReasonsHeader)
	}
	var observers []string // parsed on first need
	byKey := map[[2]string]map[string]bool{}
	for n, row := range rows {
		where := fmt.Sprintf("the reasons table, row %d", n+1)
		if len(row) != len(wantReasonsHeader) {
			return nil, fmt.Errorf("%s has %d cells, want %d: %q", where, len(row), len(wantReasonsHeader), row)
		}
		action := strings.Trim(row[0], "`")
		if !slices.Contains(Actions, action) || row[0] != "`"+action+"`" {
			return nil, fmt.Errorf("%s: the type %q is none of %q", where, row[0], Actions)
		}
		reasons, err := parseReasonGroups(row[1])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", where, err)
		}
		regarding := splitGroups(row[2])
		groupKinds := make([][]string, len(regarding))
		for i, cell := range regarding {
			kinds := kindsIn(cell)
			if len(kinds) == 0 && strings.Contains(cell, "(Observers)") {
				if observers == nil {
					if observers, err = ParseObserverKinds(md); err != nil {
						return nil, fmt.Errorf("%s: its Regarding cell points at the Observers table: %w", where, err)
					}
				}
				kinds = observers
			}
			if len(kinds) == 0 {
				return nil, fmt.Errorf("%s: the Regarding cell %q names none of %q", where, cell, Kinds)
			}
			groupKinds[i] = kinds
		}
		switch {
		case len(reasons) == len(regarding): // paired, or one group each
		case len(regarding) == 1: // one Regarding for every group of reasons
		case len(reasons) == 1: // one group of reasons, regarding several kinds
		default:
			return nil, fmt.Errorf("%s: %d groups of reasons against %d of Regarding, which neither pair nor share", where, len(reasons), len(regarding))
		}
		for i, group := range reasons {
			var kinds []string
			switch {
			case len(reasons) == len(regarding):
				kinds = groupKinds[i]
			default:
				for _, k := range groupKinds {
					kinds = append(kinds, k...)
				}
			}
			for _, reason := range group {
				key := [2]string{action, reason}
				if byKey[key] == nil {
					byKey[key] = map[string]bool{}
				}
				for _, k := range kinds {
					byKey[key][k] = true
				}
			}
		}
	}
	out := make([]TableEntry, 0, len(byKey))
	for key, kinds := range byKey {
		e := TableEntry{Action: key[0], Reason: key[1]}
		for k := range kinds {
			e.Kinds = append(e.Kinds, k)
		}
		sort.Strings(e.Kinds)
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Action != out[j].Action {
			return out[i].Action < out[j].Action
		}
		return out[i].Reason < out[j].Reason
	})
	if len(out) == 0 {
		return nil, errors.New("the reasons table holds no reason")
	}
	return out, nil
}

// ParseObserverKinds reads HBEP-8's Observers table and returns the kinds its
// "Event when stale" column regards, sorted. Every row must raise
// HeartbeatMissed and name a kind.
func ParseObserverKinds(md []byte) ([]string, error) {
	header, rows, _, _, err := tableAfter(strings.Split(string(md), "\n"), observersMarker)
	if err != nil {
		return nil, fmt.Errorf("the Observers table: %w", err)
	}
	col := slices.Index(header, "Event when stale")
	if col < 0 {
		return nil, fmt.Errorf("the Observers table's header %q has no \"Event when stale\" column", header)
	}
	seen := map[string]bool{}
	for n, row := range rows {
		if len(row) != len(header) {
			return nil, fmt.Errorf("the Observers table, row %d has %d cells, want %d", n+1, len(row), len(header))
		}
		cell := row[col]
		if !strings.Contains(cell, "`HeartbeatMissed`") {
			return nil, fmt.Errorf("the Observers table, row %d: %q does not raise HeartbeatMissed", n+1, cell)
		}
		kinds := kindsIn(cell)
		if len(kinds) == 0 {
			return nil, fmt.Errorf("the Observers table, row %d: %q names none of %q", n+1, cell, Kinds)
		}
		for _, k := range kinds {
			seen[k] = true
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

// rankingMarker is the bold lead-in of HBEP-7's ranking paragraph, and
// rankingListStart is the text that ends the sentence announcing the list: the
// colon after the file that holds the code's order.
const (
	rankingMarker    = "**The ranking**"
	rankingListStart = "status.go`):"
)

// ParseRanking reads HBEP-7's ranking of the Degraded reasons: the names in
// backticks after "status.go`):" in the paragraph that starts "**The ranking**",
// separated by commas and ended by a period. It returns an error when the
// paragraph or the list is missing or malformed, when the list is empty, or when
// a name repeats.
func ParseRanking(md []byte) ([]string, error) {
	var found []string
	for _, line := range strings.Split(string(md), "\n") {
		if strings.Contains(line, rankingMarker) {
			found = append(found, line)
		}
	}
	if len(found) != 1 {
		return nil, fmt.Errorf("want one paragraph containing %q, found %d", rankingMarker, len(found))
	}
	line := found[0]
	at := strings.Index(line, rankingListStart)
	if at < 0 {
		return nil, fmt.Errorf("the ranking paragraph has no %q before its list", rankingListStart)
	}
	rest := line[at+len(rankingListStart):]
	var out []string
	for {
		rest = strings.TrimLeft(rest, " ")
		m := rankingName.FindStringSubmatch(rest)
		if m == nil {
			return nil, fmt.Errorf("the ranking list after %d names expects a reason name in backticks, found %q", len(out), clip(rest))
		}
		if slices.Contains(out, m[1]) {
			return nil, fmt.Errorf("the ranking names %s twice", m[1])
		}
		out = append(out, m[1])
		rest = rest[len(m[0]):]
		switch {
		case strings.HasPrefix(rest, ","):
			rest = rest[1:]
		case strings.HasPrefix(rest, "."):
			return out, nil
		default:
			return nil, fmt.Errorf("the ranking list after %s expects a comma or a period, found %q", m[1], clip(rest))
		}
	}
}

func clip(s string) string {
	if len(s) > 30 {
		return s[:30] + "..."
	}
	return s
}

// tableAfter finds the one line that contains marker and returns the header
// and rows of the Markdown table that follows it, within the next three lines,
// and the span of lines [start, end) the table occupies. Every row's cells are
// trimmed. The table needs a separator line after its header and at least one
// row. A table that is interrupted by a blank line and goes on is refused: the
// rows after the break would be silently left out.
func tableAfter(lines []string, marker string) (header []string, rows [][]string, start, end int, err error) {
	at := -1
	for i, l := range lines {
		if strings.Contains(l, marker) {
			if at >= 0 {
				return nil, nil, 0, 0, fmt.Errorf("%q appears more than once", marker)
			}
			at = i
		}
	}
	if at < 0 {
		return nil, nil, 0, 0, fmt.Errorf("no line contains %q", marker)
	}
	start = -1
	for i := at + 1; i < len(lines) && i <= at+3; i++ {
		if strings.HasPrefix(strings.TrimSpace(lines[i]), "|") {
			start = i
			break
		}
	}
	if start < 0 {
		return nil, nil, 0, 0, fmt.Errorf("no table within three lines after %q", marker)
	}
	header = splitRow(lines[start])
	if start+1 >= len(lines) {
		return nil, nil, 0, 0, errors.New("the table ends after its header")
	}
	sep := splitRow(lines[start+1])
	if len(sep) != len(header) {
		return nil, nil, 0, 0, fmt.Errorf("the separator line has %d cells, the header %d", len(sep), len(header))
	}
	for _, c := range sep {
		if !separatorCell.MatchString(c) {
			return nil, nil, 0, 0, fmt.Errorf("the separator line has the cell %q", c)
		}
	}
	end = start + 2
	for ; end < len(lines) && strings.HasPrefix(strings.TrimSpace(lines[end]), "|"); end++ {
		rows = append(rows, splitRow(lines[end]))
	}
	if len(rows) == 0 {
		return nil, nil, 0, 0, errors.New("the table has no row")
	}
	// A blank line, then more table rows, is a table that goes on after a break.
	for i := end; i < len(lines) && strings.TrimSpace(lines[i]) == ""; i++ {
		if i+1 < len(lines) && strings.HasPrefix(strings.TrimSpace(lines[i+1]), "|") {
			return nil, nil, 0, 0, fmt.Errorf("line %d: the table goes on after a blank line, and its rows would be left out", i+2)
		}
	}
	return header, rows, start, end, nil
}

func splitRow(line string) []string {
	line = strings.TrimSpace(line)
	line = strings.TrimPrefix(line, "|")
	line = strings.TrimSuffix(line, "|")
	cells := strings.Split(line, "|")
	for i := range cells {
		cells[i] = strings.TrimSpace(cells[i])
	}
	return cells
}

// splitGroups splits a cell on semicolons, trimming each group.
func splitGroups(cell string) []string {
	groups := strings.Split(cell, ";")
	for i := range groups {
		groups[i] = strings.TrimSpace(groups[i])
	}
	return groups
}

// parseReasonGroups reads a Reasons cell: groups separated by semicolons, each
// a comma-separated list of reason names in backticks, each name optionally
// followed by a parenthetical remark.
func parseReasonGroups(cell string) ([][]string, error) {
	plain := parenthetical.ReplaceAllString(cell, "")
	var out [][]string
	for _, group := range splitGroups(plain) {
		var names []string
		for _, tok := range strings.Split(group, ",") {
			tok = strings.TrimSpace(tok)
			m := reasonToken.FindStringSubmatch(tok)
			if m == nil {
				return nil, fmt.Errorf("the Reasons cell %q holds %q, which is not a reason name in backticks", cell, tok)
			}
			names = append(names, m[1])
		}
		out = append(out, names)
	}
	return out, nil
}

// kindsIn returns the distinct kinds a cell names, sorted.
func kindsIn(cell string) []string {
	seen := map[string]bool{}
	for _, m := range kindWord.FindAllString(cell, -1) {
		seen[m] = true
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
