package main

import (
	"strings"
	"testing"
	"time"
)

func s(n int) time.Duration { return time.Duration(n) * time.Second }

// The thresholds come from the issue, not from the code: a stream shorter than about 30 minutes proves nothing,
// and the 1,800 s deadlines seen in the field must be outlived both in total and after the removal.
func TestRemovalVerdict(t *testing.T) {
	good := removalFacts{RemovalEffective: true, StreamComplete: true, StreamElapsed: s(2100), AfterRemoval: s(2080),
		MinStream: s(1800), MinAfterRemoval: s(1800)}
	cases := []struct {
		name string
		mod  func(*removalFacts)
		pass bool
		want string // substring of one reason
	}{
		{"35 min stream, removal effective", func(f *removalFacts) {}, true, ""},
		{"exactly at both minimums", func(f *removalFacts) { f.StreamElapsed, f.AfterRemoval = s(1800), s(1800) }, true, ""},
		{"complete but only 25 min", func(f *removalFacts) { f.StreamElapsed, f.AfterRemoval = s(1500), s(1480) }, false, "less than the 1800 s minimum"},
		{"long stream, removal came late", func(f *removalFacts) { f.AfterRemoval = s(900) }, false, "after the removal"},
		{"stream cut", func(f *removalFacts) { f.StreamComplete = false }, false, "did not complete"},
		{"worker still reached", func(f *removalFacts) { f.RemovalEffective = false }, false, "did not take effect"},
		{"short smoke run, minimums lowered", func(f *removalFacts) {
			f.StreamElapsed, f.AfterRemoval, f.MinStream, f.MinAfterRemoval = s(100), s(80), s(60), s(60)
		}, true, ""},
	}
	for _, c := range cases {
		f := good
		c.mod(&f)
		pass, why := f.verdict()
		if pass != c.pass {
			t.Errorf("%s: pass=%v want %v (%v)", c.name, pass, c.pass, why)
		}
		if c.want != "" && !strings.Contains(strings.Join(why, "; "), c.want) {
			t.Errorf("%s: reasons %v do not mention %q", c.name, why, c.want)
		}
	}
}
