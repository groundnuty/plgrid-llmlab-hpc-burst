package reasondrift

import (
	"fmt"
	"slices"
	"sort"
	"strings"
)

// NoAction is the action of an event that has none: the Provider controller's
// events go through crossplane-runtime's recorder, which writes no action
// (HBEP-8, Events, open question 14). NoKind is the regarding kind of an emit
// site that does not name one.
const (
	NoAction = "(none)"
	NoKind   = "(none)"
)

// Emit is one emit site of the code: a call that raises an event, found by
// reference. Each of Reasons, Actions and Kinds is the set of values that can
// reach the call's argument, resolved from constants through variables,
// parameters and results. When one of them cannot be resolved, Unresolved says
// which and why, and the three sets are not used.
type Emit struct {
	// Package is the package's directory relative to the module root, such as
	// internal/lifecycle.
	Package string
	// Func is the enclosing function, such as pass.fail.
	Func string
	// Pos is file:line, for messages. Keys never hold it.
	Pos string

	Reasons []string
	// LiteralReasons are the reasons, among Reasons, that reached the call as a
	// string literal at a call site: the argument of the recorder call, or of
	// the call of a function that forwards it. A reason a named constant
	// carries is not in it, whatever package the constant is in.
	LiteralReasons []string
	Actions        []string
	Kinds          []string
	Unresolved     string
}

// Code is what the scan of the code found.
type Code struct {
	// Constants maps the value of each reason constant in api/v1alpha1 (a
	// string constant named Reason... or EventReason...) to the constants'
	// names.
	Constants map[string][]string
	// Emits are the emit sites of the module's non-test code.
	Emits []Emit
	// Ranking is the order of the Degraded reasons in the code, first to last.
	Ranking []string
	// Wrappers are the pass-through wrappers the scan met, by name
	// (Wrapper.Func): a function whose Event or Eventf call forwards the
	// arguments it was called with.
	Wrappers []string
}

// Mismatch kinds.
const (
	// NoConstant: a reason that HBEP-8's table names, and that an emit site
	// passes as a string literal, has no Reason constant in api/v1alpha1. A
	// reason an emit site takes from a named constant, in api/v1alpha1 or in the
	// emitter's package, is fine (`@hpc-burst-science-agent`, 2 Nov 2026, for
	// #239); a bare literal is not.
	NoConstant = "no-constant"
	// TableOnly: the table lists a reason under a type and a regarding kind
	// that no emit site raises.
	TableOnly = "table-only"
	// CodeOnly: an emit site raises a reason under a type and a regarding kind
	// that the table does not list.
	CodeOnly = "code-only"
	// Unresolved: an emit site's reason, type or regarding kind cannot be
	// resolved to constants.
	Unresolved = "unresolved"
	// RankingHBEPOnly and RankingCodeOnly: a reason only HBEP-7's ranking, or
	// only the code's, holds. RankingOrder: both hold the same reasons in a
	// different order.
	RankingHBEPOnly = "ranking-hbep-only"
	RankingCodeOnly = "ranking-code-only"
	RankingOrder    = "ranking-order"
)

// Mismatch is one difference between the HBEPs and the code. Its fields other
// than Detail make its identity: an Entry matches on them, and none is a line
// number, so a mismatch keeps its identity while the code around it moves.
type Mismatch struct {
	Kind      string
	Action    string
	Reason    string
	Regarding string
	Package   string
	Func      string
	// Detail explains the mismatch for a reader. It is not part of the identity.
	Detail string
}

// Key is the identity of a mismatch.
func (m Mismatch) Key() string {
	return strings.Join([]string{m.Kind, m.Action, m.Reason, m.Regarding, m.Package, m.Func}, "|")
}

func (m Mismatch) String() string {
	var parts []string
	for _, p := range []struct{ name, val string }{
		{"action", m.Action}, {"reason", m.Reason}, {"regarding", m.Regarding}, {"package", m.Package}, {"func", m.Func},
	} {
		if p.val != "" {
			parts = append(parts, p.name+"="+p.val)
		}
	}
	s := m.Kind + " " + strings.Join(parts, " ")
	if m.Detail != "" {
		s += ": " + m.Detail
	}
	return s
}

type triple struct{ action, reason, kind string }

// Compare sets what the HBEPs state against what the code does, both ways, and
// returns every difference, sorted by identity. The allowances are applied
// afterwards (Apply).
func Compare(d *Docs, c *Code) []Mismatch {
	var out []Mismatch

	// A reason of the table that an emit site passes as a string literal has a
	// constant in api/v1alpha1.
	// Only typed events count: the Provider controller's untyped events are no
	// rows of the table, and write their reasons as literals.
	literal := map[string]bool{}
	for _, e := range c.Emits {
		if e.Unresolved == "" && !slices.Contains(e.Actions, NoAction) {
			for _, r := range e.LiteralReasons {
				literal[r] = true
			}
		}
	}
	named := map[string]bool{}
	for _, e := range d.Table {
		named[e.Reason] = true
	}
	for r := range named {
		if literal[r] && len(c.Constants[r]) == 0 {
			out = append(out, Mismatch{Kind: NoConstant, Reason: r,
				Detail: "an emit site passes it as a string literal, and no Reason or EventReason constant in api/v1alpha1 holds this value"})
		}
	}

	// Events: the table's (type, reason, kind) triples against the emit sites'.
	table := map[triple]bool{}
	for _, e := range d.Table {
		for _, k := range e.Kinds {
			table[triple{e.Action, e.Reason, k}] = true
		}
	}
	type site struct{ pkg, pos string }
	code := map[triple][]site{}
	emitted := map[string][]triple{} // reason -> triples, for the messages
	for _, e := range c.Emits {
		if e.Unresolved != "" {
			out = append(out, Mismatch{Kind: Unresolved, Package: e.Package, Func: e.Func, Detail: e.Unresolved + " (" + e.Pos + ")"})
			continue
		}
		for _, r := range e.Reasons {
			for _, a := range e.Actions {
				for _, k := range e.Kinds {
					t := triple{a, r, k}
					code[t] = append(code[t], site{e.Package, e.Pos})
					emitted[r] = append(emitted[r], t)
				}
			}
		}
	}
	for t := range table {
		if _, ok := code[t]; ok {
			continue
		}
		m := Mismatch{Kind: TableOnly, Action: t.action, Reason: t.reason, Regarding: t.kind}
		if others := distinct(emitted[t.reason]); len(others) == 0 {
			m.Detail = "no emit site raises " + t.reason
		} else {
			m.Detail = "the code raises " + t.reason + " as " + describe(others)
		}
		out = append(out, m)
	}
	for t, sites := range code {
		if table[t] {
			continue
		}
		perPkg := map[string][]string{}
		for _, s := range sites {
			perPkg[s.pkg] = append(perPkg[s.pkg], s.pos)
		}
		for pkg, pos := range perPkg {
			sort.Strings(pos)
			out = append(out, Mismatch{Kind: CodeOnly, Action: t.action, Reason: t.reason, Regarding: t.kind, Package: pkg,
				Detail: "emitted at " + strings.Join(slices.Compact(pos), ", ") + ", and the table has no such row"})
		}
	}

	out = append(out, compareRanking(d.Ranking, c.Ranking)...)

	sort.Slice(out, func(i, j int) bool { return out[i].Key() < out[j].Key() })
	return out
}

func distinct(ts []triple) []triple {
	seen := map[triple]bool{}
	var out []triple
	for _, t := range ts {
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		return a.action+"/"+a.kind < b.action+"/"+b.kind
	})
	return out
}

func describe(ts []triple) string {
	parts := make([]string, len(ts))
	for i, t := range ts {
		parts[i] = t.action + " regarding " + t.kind
	}
	return strings.Join(parts, "; ")
}

// compareRanking compares HBEP-7's ranking with the code's order: first the
// reasons only one of them holds, and, when both hold the same reasons, their
// order.
func compareRanking(hbep, code []string) []Mismatch {
	var out []Mismatch
	for _, r := range hbep {
		if !slices.Contains(code, r) {
			out = append(out, Mismatch{Kind: RankingHBEPOnly, Reason: r, Detail: "HBEP-7's ranking holds it; the code's order does not"})
		}
	}
	for _, r := range code {
		if !slices.Contains(hbep, r) {
			out = append(out, Mismatch{Kind: RankingCodeOnly, Reason: r, Detail: "the code's order holds it; HBEP-7's ranking does not"})
		}
	}
	if len(out) == 0 && !slices.Equal(hbep, code) {
		out = append(out, Mismatch{Kind: RankingOrder, Detail: fmt.Sprintf("HBEP-7 ranks %v, the code ranks %v", hbep, code)})
	}
	return out
}
