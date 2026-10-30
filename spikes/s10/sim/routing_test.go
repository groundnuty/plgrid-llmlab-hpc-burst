package sim

// Routing and membership behaviour, with expectations from HBEP-5 and from
// the definition of consistent hashing with bounded loads.

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/third_party/forked/kubeai/loadbalancer"
)

// A loaded cluster: n conversations arriving together, long turns, so many
// requests are in flight at once.
func loadedConvs(n int, sharedSys bool) []Conversation {
	var convs []Conversation
	for i := 0; i < n; i++ {
		c := conv(i, float64(i)*0.01, 1600, 160, 8, 160, 160, 0.5)
		if sharedSys {
			c.Sys = []Seg{{ID: SegID(200), Len: 1600}}
		}
		convs = append(convs, c)
	}
	return convs
}

func slowCluster(n int) Cluster {
	return Cluster{Instances: n, Spare: 1, KVTokens: 10_000_000, BlockSize: 16, MaxRunning: 64, PrefillRate: 20000, TPOT: 0.02}
}

func instancesUsed(m *Metrics) map[int]int {
	used := map[int]int{}
	for _, r := range m.Recs {
		used[r.Inst]++
	}
	return used
}

// Every conversation has the same system prompt and the key is the system
// prompt: pure affinity sends everything to one Instance.
func TestSystemKeyCollapsesUnderAffinity(t *testing.T) {
	m := runSim(t, slowCluster(4), Policy{Kind: Affinity, Key: KeySystem}, loadedConvs(32, true), nil)
	if used := instancesUsed(m); len(used) != 1 {
		t.Errorf("affinity on one shared key used %d Instances, want 1: %v", len(used), used)
	}
	if d, top := m.Keys(); d != 1 || top != 1 {
		t.Errorf("distinct keys %d, top share %.2f; want 1 and 1.00", d, top)
	}
}

// The same collapse under CHWBL: the bound spreads the load over the
// ring, and no placement puts a target above c times the mean (HBEP-5:
// "a target above its bound passes the key to the next one").
func TestCHWBLSpillsCollapsedKeyWithinBound(t *testing.T) {
	for _, pct := range []int{125, 150, 200} {
		m := runSim(t, slowCluster(4), Policy{Kind: CHWBL, Key: KeySystem, Percent: pct}, loadedConvs(32, true), nil)
		if used := instancesUsed(m); len(used) < 2 {
			t.Errorf("c=%d%%: used %d Instances, want spill to at least 2", pct, len(used))
		}
		if m.KubeRuleViol != 0 {
			t.Errorf("c=%d%%: %d placements above the bound, want 0", pct, m.KubeRuleViol)
		}
		if m.Placements != 32*8 {
			t.Errorf("c=%d%%: %d placements, want 256", pct, m.Placements)
		}
	}
}

// Distinct openings spread over the ring under the opening key.
func TestOpeningKeySpreads(t *testing.T) {
	m := runSim(t, slowCluster(4), Policy{Kind: Affinity, Key: KeyOpening}, loadedConvs(64, true), nil)
	if d, _ := m.Keys(); d != 64 {
		t.Errorf("distinct opening keys %d, want 64", d)
	}
	if used := instancesUsed(m); len(used) != 4 {
		t.Errorf("used %d Instances, want 4", len(used))
	}
}

// Consistent hashing: when one Instance drains, only the conversations
// homed on it move. With pure affinity nothing else moves. All 64 loaded
// conversations start before the drain at 3 s and run for about 30 s (8
// turns of about 3.9 s), so exactly 64 are checked; 10 conversations that
// start at 10 s had no Instance before the drain and must not be counted.
// The forced count is taken from a separate ring of the same four
// Instances: the conversations whose opening hashes to instance-2.
func TestDrainMovesOnlyItsConversations(t *testing.T) {
	convs := loadedConvs(64, false)
	for i := 0; i < 10; i++ {
		convs = append(convs, conv(1000+i, 10, 1600, 160, 4, 160, 160, 0.5))
	}
	ev := []Event{{Time: 3, Kind: Drain, Instance: 2}}
	m := runSim(t, slowCluster(4), Policy{Kind: Affinity, Key: KeyOpening}, convs, ev)
	rc := m.Rehomed[0]
	if rc.Checked != 64 {
		t.Errorf("checked %d conversations, want 64", rc.Checked)
	}
	if rc.Collateral != 0 {
		t.Errorf("collateral moves %d, want 0", rc.Collateral)
	}
	ring := loadbalancer.NewGroup(256)
	ring.SetEndpoints([]string{"instance-0", "instance-1", "instance-2", "instance-3"})
	want := 0
	for i := 0; i < 64; i++ {
		c := convs[i]
		home, done, err := ring.PickPrefixHash(SegsKey(c.Sys, c.FirstUser), AffinityPercent)
		if err != nil {
			t.Fatal(err)
		}
		done()
		if home == "instance-2" {
			want++
		}
	}
	if want == 0 {
		t.Fatal("no conversation is homed on instance-2; the test proves nothing")
	}
	if rc.Forced != want {
		t.Errorf("forced re-homes %d, want %d (conversations homed on instance-2)", rc.Forced, want)
	}
	for _, r := range m.Recs {
		if r.Inst == 2 && r.Dispatch > 3 {
			t.Errorf("request dispatched at %.2f went to the drained Instance", r.Dispatch)
		}
	}
}

// An Instance that leaves cuts its in-flight requests; each is sent again
// and every turn of every conversation completes exactly once.
func TestLeaveResendsCutRequests(t *testing.T) {
	convs := loadedConvs(32, false)
	ev := []Event{{Time: 2, Kind: Leave, Instance: 1}}
	for _, pol := range []Policy{{Kind: RoundRobin}, {Kind: CHWBL, Key: KeyOpening, Percent: 125}} {
		m := runSim(t, slowCluster(4), pol, convs, ev)
		if m.Aborted == 0 {
			t.Errorf("%s: nothing was in flight on the leaving Instance; the test proves nothing", pol.Name())
		}
		seen := map[[2]int]int{}
		for _, r := range m.Recs {
			seen[[2]int{r.Conv, r.Turn}]++
		}
		if len(seen) != 32*8 {
			t.Errorf("%s: %d distinct turns completed, want 256", pol.Name(), len(seen))
		}
		for k, n := range seen {
			if n != 1 {
				t.Errorf("%s: conv %d turn %d completed %d times", pol.Name(), k[0], k[1], n)
			}
		}
	}
}

// A joining Instance takes a share of the keys under consistent hashing:
// conversations either stay or move to the new Instance, never elsewhere.
func TestJoinMovesKeysOnlyToNewInstance(t *testing.T) {
	ev := []Event{{Time: 3, Kind: Join, Instance: 4}}
	m := runSim(t, slowCluster(4), Policy{Kind: Affinity, Key: KeyOpening}, loadedConvs(64, false), ev)
	rc := m.Rehomed[0]
	if rc.Collateral != 0 {
		t.Errorf("collateral moves %d, want 0", rc.Collateral)
	}
	if rc.ToJoined == 0 {
		t.Errorf("no conversation moved to the joining Instance")
	}
}

// During a drain KubeAI keeps the drained Instance's open requests in its
// total while n drops, so its mean exceeds the members' mean and a
// placement can pass KubeAI's check yet break HBEP-5's bound. Here 64
// conversations are in flight when instance-2 drains at 3 s: unfixed, some
// placements must break the members' bound. With the drained requests
// released at drain start, KubeAI's total must equal the members' total
// (inflation 0). That is the release's test; zero violations then follow
// from the accounting. The drained requests' completions call the same
// once-guarded release again; a double decrement would fail the run's
// crossCheck.
func TestDrainInflatesKubeAIMeanUnlessReleased(t *testing.T) {
	ev := []Event{{Time: 3, Kind: Drain, Instance: 2}}
	pol := Policy{Kind: CHWBL, Key: KeySystem, Percent: 125}
	plain := runSim(t, slowCluster(4), pol, loadedConvs(64, true), ev)
	if plain.MaxKubeInflation == 0 || plain.KubeRuleViol == 0 {
		t.Errorf("unfixed drain: inflation %d, violations %d; want both above 0", plain.MaxKubeInflation, plain.KubeRuleViol)
	}
	pol.ReleaseOnDrain = true
	fixed := runSim(t, slowCluster(4), pol, loadedConvs(64, true), ev)
	if fixed.MaxKubeInflation != 0 {
		t.Errorf("release on drain: inflation %d, want 0", fixed.MaxKubeInflation)
	}
}

// The ring itself is balanced: on 200,000 distinct opening keys, each of
// the four Instances is home to about a quarter. With 256 points per
// Instance the expected spread is a few points, so the test allows 25% +- 3.
// The README cites instance-1's share from this test's log.
func TestRingShareIsBalanced(t *testing.T) {
	ring := loadbalancer.NewGroup(256)
	ring.SetEndpoints([]string{"instance-0", "instance-1", "instance-2", "instance-3"})
	const keys = 200_000
	count := map[string]int{}
	for i := 0; i < keys; i++ {
		home, done, err := ring.PickPrefixHash(SegsKey([]Seg{{ID: SegID(9, uint64(i)), Len: 1000}}), AffinityPercent)
		if err != nil {
			t.Fatal(err)
		}
		done()
		count[home]++
	}
	for name, n := range count {
		share := float64(n) / keys
		t.Logf("%s: %.1f%% of %d keys", name, 100*share, keys)
		if share < 0.22 || share > 0.28 {
			t.Errorf("%s holds %.1f%% of the keys, want 25%% +- 3", name, 100*share)
		}
	}
}
