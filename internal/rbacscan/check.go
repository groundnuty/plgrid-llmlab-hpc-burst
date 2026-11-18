package rbacscan

import (
	"fmt"
	"sort"
	"strings"
)

// Finding is one way the code and the roles disagree.
type Finding struct {
	Kind  string // "not granted", "unused rule" or "problem"
	Ident string
	Need  Triple
	Msg   string
}

func (f Finding) String() string { return f.Kind + ": " + f.Msg }

// Check compares what the scan found with what the roles grant, in both directions: a site whose rights an identity's role
// lacks, naming the call and the triples it lacks, and a triple a role grants that no site or library entry needs. A problem
// the scan could not read is a finding too.
func Check(res *Result, roles Roles) []Finding {
	var out []Finding
	for _, p := range res.Problems {
		out = append(out, Finding{Kind: "problem", Msg: p.String()})
	}
	needed := map[string]map[Triple][]string{}
	for _, st := range res.Sites {
		if needed[st.Ident] == nil {
			needed[st.Ident] = map[Triple][]string{}
		}
		for _, t := range st.Need {
			needed[st.Ident][t] = append(needed[st.Ident][t], st.Pos)
		}
		granted := roles[st.Ident]
		if granted == nil {
			out = append(out, Finding{Kind: "not granted", Ident: st.Ident, Msg: fmt.Sprintf("%s has no role at all, and %s needs rights (%s)", st.Ident, st.Pos, st.What)})
			continue
		}
		var lacks []string
		for _, t := range st.Need {
			if !granted[t] {
				lacks = append(lacks, t.String())
				out = append(out, Finding{Kind: "not granted", Ident: st.Ident, Need: t,
					Msg: fmt.Sprintf("%s's role does not grant %q, which %s needs (%s in %s)", st.Ident, t, st.Pos, st.What, st.Func)})
			}
		}
		_ = lacks
	}
	for _, id := range roles.Idents() {
		var unused []Triple
		for t := range roles[id] {
			if len(needed[id][t]) == 0 {
				unused = append(unused, t)
			}
		}
		sort.Slice(unused, func(i, j int) bool { return lessTriple(unused[i], unused[j]) })
		for _, t := range unused {
			out = append(out, Finding{Kind: "unused rule", Ident: id, Need: t,
				Msg: fmt.Sprintf("%s's role grants %q, which no call site and no library entry needs", id, t)})
		}
	}
	return out
}

// Format joins findings, one to a line.
func Format(fs []Finding) string {
	var b strings.Builder
	for _, f := range fs {
		b.WriteString("  " + f.String() + "\n")
	}
	return b.String()
}
