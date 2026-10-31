package front

import (
	"sync"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// TestJoinBetweenViewAndPick (PR #95 review: Astra B3): Instance B joins
// after a request read the view and before it picks from the live group. The
// group then selects B, which the request's view does not hold. The request
// must go to B, not panic on a nil target and leak B's reservation.
func TestJoinBetweenViewAndPick(t *testing.T) {
	f := newScFix(t, scOpts{instances: 2, desired: 1, inst: func(i int, o *instanceOpts) {
		if i == 1 {
			o.phase = v1alpha1.InstanceVerifying // B is not routable yet
		}
	}})
	a, b := f.names[0], f.names[1]
	if f.scGroup(scDep).Members() != 1 {
		t.Fatalf("the group has %d members, want only A", f.scGroup(scDep).Members())
	}
	key := scKeys(t, f.names, b, 1)[0] // B is the key's home once it joins
	var once sync.Once
	f.router.beforePick = func() {
		once.Do(func() {
			f.inst(f.uids[1]).Status.Phase = v1alpha1.InstanceReady
			f.apply()
		})
	}
	resp, body := scChatOnce(t, f.harness, key)
	if resp.StatusCode != 200 || scInstanceOf(resp) != b {
		t.Fatalf("status %d instance %q, want 200 from B (%s), which joined between the view and the pick: %s", resp.StatusCode, scInstanceOf(resp), b, body)
	}
	if n := f.scLoad(scDep, b); n != 0 {
		t.Errorf("B counts %d in flight after the request ended", n)
	}
	_ = a
}
