package front

import (
	"net/http"
	"testing"
)

// HBEP-5, Workloads, Naming it: the header names a Workload; an empty one names
// none, as an empty session header does, and a header sent twice with two
// different names names no Workload that exists. The values are written out.
// Only a space and a tab are trimmed; a non-breaking space, which the test writes
// as a Go escape, is part of the value (the review of #778, finding 2).
func TestWorkloadHeaderValue(t *testing.T) {
	h := func(vals ...string) http.Header {
		out := http.Header{}
		for _, v := range vals {
			out.Add("X-Hpcburst-Workload", v)
		}
		return out
	}
	cases := []struct {
		name string
		hdr  http.Header
		cfg  string
		want string
	}{
		{"one value", h("study-a"), "x-hpcburst-workload", "study-a"},
		{"spaces around it", h("  study-a "), "x-hpcburst-workload", "study-a"},
		{"absent", http.Header{}, "x-hpcburst-workload", ""},
		{"empty", h(""), "x-hpcburst-workload", ""},
		{"blank", h("   "), "x-hpcburst-workload", ""},
		{"the same name twice", h("a", "a"), "x-hpcburst-workload", "a"},
		{"two names", h("a", "b"), "x-hpcburst-workload", "a,b"},
		{"three names stop at the second", h("a", "b", "c"), "x-hpcburst-workload", "a,b"},
		{"a repeat, then a different one", h("a", "a", "b"), "x-hpcburst-workload", "a,b"},
		{"a space and a tab are trimmed", h(" \t a \t "), "x-hpcburst-workload", "a"},
		{"only a space and a tab is empty", h(" \t"), "x-hpcburst-workload", ""},
		{"a non-breaking space is no whitespace here", h("\u00a0"), "x-hpcburst-workload", "\u00a0"},
		{"a non-breaking space before a name", h("\u00a0a"), "x-hpcburst-workload", "\u00a0a"},
		{"a name between non-breaking spaces", h("\u00a0a\u00a0"), "x-hpcburst-workload", "\u00a0a\u00a0"},
		{"a form feed is no whitespace here", h("\fa"), "x-hpcburst-workload", "\fa"},
		{"a list in one line", h("a, b"), "x-hpcburst-workload", "a, b"},
		{"the header is not configured", h("a"), "", ""},
		{"another name configured", h("a"), "x-other", ""},
	}
	for _, c := range cases {
		if got := workloadHeaderValue(c.hdr, c.cfg); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
}
