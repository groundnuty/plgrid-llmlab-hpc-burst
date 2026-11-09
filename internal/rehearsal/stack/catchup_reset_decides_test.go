package stack

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/watch"
)

// A reset fails the test at its end, as #292 has it, unless the test said it judges nothing from the watch: a first job that never started,
// on the scheduler's own read (internal/alpha; the science agent, on PR 284). That can be said before the reset or after it.
func TestWhatAResetAtTheEndOfTheTestCountsFor(t *testing.T) {
	for _, c := range []struct {
		name          string
		before, after func(*Recorder)
		wantFail      bool
	}{
		{"nothing said", func(*Recorder) {}, func(*Recorder) {}, true},
		{"said before it", (*Recorder).ResetsDecideNothing, func(*Recorder) {}, false},
		{"said after it", func(*Recorder) {}, (*Recorder).ResetsDecideNothing, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := newScripted(t)
			ct := &cleanupT{T: t}
			rec := startWatch(ct, s, Namespace, nil)
			c1 := s.next(t, allocList)
			c1.w.Action(watch.Added, alloc("a1", "alloc-a", "5"))
			waitReached(t, rec, KindAllocations, "5")
			c.before(rec)
			expireStream(t, s, rec, c1, "100")
			c.after(rec)
			ct.run()
			if failed := len(ct.errs) != 0; failed != c.wantFail {
				t.Errorf("the end of the test reported %q; want a failure: %t", ct.errs, c.wantFail)
			}
			if c.wantFail && !strings.Contains(ct.errs[0], "not known to be complete") {
				t.Errorf("the end of the test reported %q", ct.errs)
			}
		})
	}
}

// A nil Recorder takes it: a run that stopped before its watch began has nothing to say it to.
func TestANilRecorderTakesThatResetsDecideNothing(t *testing.T) {
	var rec *Recorder
	rec.ResetsDecideNothing()
}
