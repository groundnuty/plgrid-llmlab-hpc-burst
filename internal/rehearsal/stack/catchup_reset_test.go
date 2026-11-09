package stack

// What a reset's failure says of the reads, and the count a row posts (PR #292's
// A1 and A2). A reset fails the run, on any kind; the message says whether any
// read's window crossed it, so that a row that failed on a reset no read crossed
// is seen as that (docs/alpha-runbook.md).

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/watch"
)

// expireStream ends the connection with an expired version, and waits for the
// stream to have passed on the LIST that starts it again; that LIST is at the
// version given. It returns the next connection.
func expireStream(t *testing.T, s *scripted, rec *Recorder, c watchCall, listRV string) watchCall {
	t.Helper()
	s.set(listRV)
	c.w.Action(watch.Error, expiredStatus())
	c.w.Stop()
	waitReached(t, rec, KindAllocations, listRV)
	return s.next(t, allocList)
}

// A reset no read's window crossed says so, a read before it does not count, and
// a read of another kind does not count either.
func TestAResetsFailureSaysNoReadCrossedIt(t *testing.T) {
	s := newScripted(t)
	ct := &cleanupT{T: t}
	rec := startWatch(ct, s, Namespace, nil)
	c := s.next(t, allocList)
	c.w.Action(watch.Added, alloc("a1", "alloc-a", "5"))
	waitReached(t, rec, KindAllocations, "5")
	rec.Allocations() // before the reset
	expireStream(t, s, rec, c, "100")
	rec.UIDs() // after it, on a stream that was not reset
	ct.run()
	if len(ct.errs) != 1 || !strings.Contains(ct.errs[0], "the Allocation watch was reset") || !strings.Contains(ct.errs[0], "no read's window crossed it") {
		t.Errorf("the end of the test reported %q; want the reset, and that no read's window crossed it", ct.errs)
	}
}

// A read of the whole record after a reset crosses it, and so does a catch-up: the
// message names them.
func TestAResetsFailureNamesTheReadsThatCrossedIt(t *testing.T) {
	s := newScripted(t)
	ct := &cleanupT{T: t}
	rec := startWatch(ct, s, Namespace, nil)
	c := s.next(t, allocList)
	c.w.Action(watch.Added, alloc("a1", "alloc-a", "5"))
	waitReached(t, rec, KindAllocations, "5")
	expireStream(t, s, rec, c, "100")
	rec.Allocations()
	res := rec.CatchUp(bg, noDeadlineT{t}, s, Namespace, KindAllocations, "the handover's result")
	want := `these reads' windows crossed it: Allocations, the catch-up for the handover's result`
	if !strings.Contains(res.Unknown, want) {
		t.Errorf("the catch-up says %q; want it to say %q", res.Unknown, want)
	}
	ct.run()
	if len(ct.errs) != 1 || !strings.Contains(ct.errs[0], want) {
		t.Errorf("the end of the test reported %q; want it to say %q", ct.errs, want)
	}
}

// A read with a window crosses a reset only if the window reaches the reset's gap:
// from the stream's last progress before it to the LIST after it. Instance reads, on
// a stream fed by hand, against the Recorder's own clock.
func TestAReadsWindowCrossesAResetOnlyIfItReachesTheGap(t *testing.T) {
	never := func(Observation) bool { return false }
	for name, c := range map[string]struct {
		read    func(rec *Recorder, before, inGap, after time.Time)
		crossed string // empty: none
	}{
		"First, from before the gap":  {func(rec *Recorder, before, _, _ time.Time) { rec.First("u", before, never) }, "First"},
		"First, from inside the gap":  {func(rec *Recorder, _, inGap, _ time.Time) { rec.First("u", inGap, never) }, "First"},
		"First, from after the gap":   {func(rec *Recorder, _, _, after time.Time) { rec.First("u", after, never) }, ""},
		"Between, up to before it":    {func(rec *Recorder, before, _, _ time.Time) { rec.Between("u", before.Add(-time.Hour), before) }, ""},
		"Between, up to inside it":    {func(rec *Recorder, before, inGap, _ time.Time) { rec.Between("u", before, inGap) }, "Between"},
		"Between, across it":          {func(rec *Recorder, before, _, after time.Time) { rec.Between("u", before, after) }, "Between"},
		"Between, from after it":      {func(rec *Recorder, _, _, after time.Time) { rec.Between("u", after, after.Add(time.Hour)) }, ""},
		"Of, the whole record":        {func(rec *Recorder, _, _, _ time.Time) { rec.Of("u") }, "Of"},
		"OnAllocation, the whole one": {func(rec *Recorder, _, _, _ time.Time) { rec.OnAllocation("a") }, "OnAllocation"},
		"UIDs, the whole one":         {func(rec *Recorder, _, _, _ time.Time) { rec.UIDs() }, "UIDs"},
		"Last, through UIDs":          {func(rec *Recorder, _, _, _ time.Time) { rec.Last() }, "UIDs"},
	} {
		t.Run(name, func(t *testing.T) {
			rec := &Recorder{}
			st := rec.stream(KindInstances, nil)
			before := time.Now().Add(-time.Millisecond)
			st.Reached("5")
			st.Reset("the watch could not resume from version 5")
			inGap := time.Now()
			st.Reached("100") // the LIST after it
			c.read(rec, before, inGap, time.Now().Add(time.Millisecond))
			got := rec.resets(KindInstances)
			if c.crossed == "" && !strings.Contains(got, "no read's window crossed it") {
				t.Errorf("%q; want no read's window crossing it", got)
			}
			if c.crossed != "" && !strings.Contains(got, "windows crossed it: "+c.crossed) {
				t.Errorf("%q; want %s's window crossing it", got, c.crossed)
			}
		})
	}
}

// A read on a stream with a reset whose LIST has not come yet has no end to the gap:
// any window from inside it crosses.
func TestAReadCrossesAResetStillWithoutItsList(t *testing.T) {
	rec := &Recorder{}
	st := rec.stream(KindInstances, nil)
	st.Reached("5")
	st.Reset("the watch could not resume from version 5")
	rec.First("u", time.Now().Add(time.Hour), func(Observation) bool { return false })
	if got := rec.resets(KindInstances); !strings.Contains(got, "windows crossed it: First") {
		t.Errorf("%q; want First's window crossing it", got)
	}
}

// The count over a run, on all the streams; none for no watch and none with no
// reset. An ordinary reconnect is not a reset.
func TestResetCountCountsEveryStreamsResets(t *testing.T) {
	if n := (*Recorder)(nil).ResetCount(); n != 0 {
		t.Errorf("no watch: %d, want 0", n)
	}
	s := newScripted(t)
	ct := &cleanupT{T: t}
	rec := startWatch(ct, s, Namespace, nil)
	defer ct.run()
	c := s.next(t, allocList)
	c.w.Action(watch.Bookmark, bookmark("5"))
	waitReached(t, rec, KindAllocations, "5")
	c.w.Stop() // an ordinary close
	c = s.next(t, allocList)
	if n := rec.ResetCount(); n != 0 {
		t.Errorf("after an ordinary reconnect: %d, want 0", n)
	}
	c = expireStream(t, s, rec, c, "100")
	if n := rec.ResetCount(); n != 1 {
		t.Errorf("after one reset: %d, want 1", n)
	}
	expireStream(t, s, rec, c, "200")
	rec.stream(KindInstances, nil).Reset("another stream's")
	if n := rec.ResetCount(); n != 3 {
		t.Errorf("after three, on two streams: %d, want 3", n)
	}
}
