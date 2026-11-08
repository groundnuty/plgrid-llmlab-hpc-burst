package scenario_test

// The absence checks' own rules, on fixtures (#256; HBEP-8, Metrics,
// Controller metrics: "Proving that an event was not raised"): the
// TunnelOrphaned check's condition 1 and its witness barrier, and the
// ReplacementLate check's start-to-ready time. The rehearsal cannot make frps
// fail a read or a measurement overwrite a class's entry at will, so these
// cases are checked here.

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
)

// TestOrphanFirstReadProblems: condition 1 of the TunnelOrphaned check holds
// only for proxies whose Instance had not ended at the first read: one live in
// the list taken just after it, or one created after it. An Instance that had
// ended before the first read, whose proxy outlived tunnelOrphanGrace and then
// went offline before the list of frps's proxies, is the case the review of
// PR 278 found: its TunnelOrphaned is in both counts, so the check must be
// inconclusive, not "not raised".
func TestOrphanFirstReadProblems(t *testing.T) {
	first := time.Date(2026, 11, 8, 6, 0, 0, 500_000_000, time.UTC)
	created := map[types.UID]time.Time{
		"live":           first.Add(-time.Hour),
		"later":          first.Add(2 * time.Second),
		"ended-before":   first.Add(-time.Hour),
		"same-second":    first.Truncate(time.Second), // created in the first read's second: maybe before it
		"never-observed": {},
	}
	createdAt := func(uid types.UID) (time.Time, bool) {
		c, ok := created[uid]
		return c, ok && !c.IsZero()
	}
	live := map[string]bool{"live": true}
	end := first.Add(10 * time.Minute)
	ok := map[types.UID]time.Time{"live": end, "later": end}
	if p := orphanFirstReadProblems(ok, live, createdAt, first); len(p) != 0 {
		t.Fatalf("a live Instance and one created after the first read: %v, want none", p)
	}
	for uid, want := range map[types.UID]string{
		"ended-before":   "may have ended before the first read",
		"same-second":    "may have ended before the first read",
		"never-observed": "its creation time is unknown",
	} {
		ends := map[types.UID]time.Time{"live": end, uid: end}
		p := orphanFirstReadProblems(ends, live, createdAt, first)
		if len(p) != 1 || !strings.Contains(p[0], string(uid)) || !strings.Contains(p[0], want) {
			t.Errorf("%s: %v, want one problem that says %q", uid, p, want)
		}
	}
}

// TestWitnessBarrier: the witness is a barrier only with a TunnelConnected
// written from a successful read of frps (True/ProxyOnline or
// False/ProxyOffline). A failed read writes Unknown/ProxyListUnreadable and
// judges no proxy, so with that condition the check is inconclusive, never
// "not raised", although the count did not rise.
func TestWitnessBarrier(t *testing.T) {
	t0 := time.Date(2026, 11, 8, 6, 0, 0, 0, time.UTC)
	read := func(at time.Time) eventcount.Read {
		return eventcount.Read{Sent: at, Answered: at.Add(10 * time.Millisecond), ProcessStart: 1, HasProcessStart: true,
			Collectors: map[string]bool{"accounting": true, "instances": true}, Count: 0, HasCount: true}
	}
	created, seen, due := t0.Add(5*time.Minute), t0.Add(5*time.Minute+10*time.Second), t0.Add(4*time.Minute)
	judge := func(c *metav1.Condition) eventcount.Verdict {
		a := eventcount.Absence{Key: keyTunnelOrphaned, First: read(t0), Second: read(t0.Add(6 * time.Minute)), Change: t0.Add(time.Second),
			Collectors: eventcount.LifecycleCollectors, Barriers: []eventcount.Barrier{witnessBarrier("w", created, c, seen, due)}, Quiet: orphanQuiet}
		v, why := a.Judge()
		t.Logf("%s: %s", conditionString(c), why)
		return v
	}
	for _, c := range []*metav1.Condition{
		{Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonProxyOffline},
		{Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonProxyOnline},
	} {
		if v := judge(c); v != eventcount.NotRaised {
			t.Errorf("a successful read's %s: %s, want not raised", conditionString(c), v)
		}
	}
	for _, c := range []*metav1.Condition{
		{Status: metav1.ConditionUnknown, Reason: v1alpha1.ReasonProxyListUnreadable},
		{Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonProxyListUnreadable},
		{Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonProxyOffline},
		nil,
	} {
		if v := judge(c); v != eventcount.Inconclusive {
			t.Errorf("%s: %s, want inconclusive", conditionString(c), v)
		}
	}
}

// TestLateStartToReady: ReplacementLate's due time counts back by the
// primary's class's start-to-ready time as the controller held it at the first
// read. A measurement after the first read overwrites the class's entry
// (60 s measured at the first read, then 40 s), so the value read at the end
// alone would put the due time 20 s later than the controller's; the larger
// one counts. A class with no entry counts the controller's 30-minute default,
// and so does a deployment with no snapshot.
func TestLateStartToReady(t *testing.T) {
	gh := v1alpha1.AcceleratorClass{GPUModel: "GH200", CPUArch: "aarch64"}
	h1 := v1alpha1.AcceleratorClass{GPUModel: "H100", CPUArch: "x86_64"}
	for _, tc := range []struct {
		name         string
		atFirst, end map[v1alpha1.AcceleratorClass]time.Duration
		want         time.Duration
	}{
		{"overwritten by a shorter measurement", map[v1alpha1.AcceleratorClass]time.Duration{gh: time.Minute}, map[v1alpha1.AcceleratorClass]time.Duration{gh: 40 * time.Second}, time.Minute},
		{"overwritten by a longer measurement", map[v1alpha1.AcceleratorClass]time.Duration{gh: 40 * time.Second}, map[v1alpha1.AcceleratorClass]time.Duration{gh: time.Minute}, time.Minute},
		{"another class's entry only", map[v1alpha1.AcceleratorClass]time.Duration{h1: time.Minute}, map[v1alpha1.AcceleratorClass]time.Duration{h1: time.Minute}, defaultStartToReady},
		{"measured only after the first read", map[v1alpha1.AcceleratorClass]time.Duration{}, map[v1alpha1.AcceleratorClass]time.Duration{gh: time.Minute}, defaultStartToReady},
		{"no snapshot", nil, map[v1alpha1.AcceleratorClass]time.Duration{gh: time.Minute}, defaultStartToReady},
	} {
		if got := lateStartToReady(gh, tc.atFirst, tc.end); got != tc.want {
			t.Errorf("%s: %s, want %s", tc.name, got, tc.want)
		}
	}
}
