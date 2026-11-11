package livechecks

// Tests for the leak guard's handling of hex-only seeds (#366). The guard
// failed by chance whenever one of those seeds turned up inside a random commit
// hash in the output; it must pass such an output, and must still fail an output
// that carries the seed as a value, whatever characters stand beside it.

import (
	"fmt"
	"strings"
	"testing"
)

// The plan's range line of CI run 42939135845, which failed on a fixed seed: its
// second hash holds "40321" at its third character.
const hashHoldingTheSeed = "range: e1151e8c8c5d3fd8a60c77c4ea35e3dba0617486..c04032199613610d1911832c22386ad186d39796, 1 file(s) changed\n"

// A hex-only seed inside a hash (a run of 9 or more hex digits, bounded by
// other characters), wherever in the hash, is not a leak. The outputs are
// fixed, not drawn at random, so the test does not depend on finding a hash.
func TestAHexOnlySeedInsideAHashIsNotALeak(t *testing.T) {
	cases := map[string]string{
		"the failure of run 42939135845":                hashHoldingTheSeed,
		"at the start of a hash":                        "range: 40321a0b7e5d4c3b2a1908f7e6d5c4b3a2f1e0d9..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n",
		"at the end of a hash":                          "range: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa..9e8d7c6b5a49382716f5e4d3c2b1a0f9e8d40321\n",
		"in the 9-digit abbreviated hash of a run name": "live-20261111120000-b40321cde\n",
		"in a 9-digit run, the shortest hash":           "abcd40321\n",
		"in an uppercase hash":                          "digest C0D40321E99613610D1911832C22386AD186D3979\n",
		"in a long digit string":                        "tag 2026404032112 built\n",
		"the other hex-only seed in a hash":             "range: a1b2c3d4e5f6a7b8c9d0424242e1f2a3b4c5d6e7f\n",
	}
	for name, out := range cases {
		t.Run(name, func(t *testing.T) {
			if got := leaks(out, leakSeeds); len(got) != 0 {
				t.Errorf("a hash-held seed was reported as a leak: %q in %q", got, out)
			}
		})
	}
}

// The same seeds as values are leaks, whatever stands beside them: a
// delimiter, the ends of the output, or a hex digit that does not make a hash
// of 9 or more with it (a printed planted value can sit in "probe40321",
// "jobs/job424242.out" or "40321bytes"). And a seed with a character no hash
// holds is a plain substring, even right after hex digits.
func TestASeedStandingAloneIsStillALeak(t *testing.T) {
	cases := []struct{ name, out, seed string }{
		{"after a colon", "dialing host:40321\n", "40321"},
		{"after an equals sign", "LIVE_PROBE_PORT=40321\n", "40321"},
		{"in a quoted value", `port "40321" refused` + "\n", "40321"},
		{"in a label", "gateway-40321\n", "40321"},
		{"on its own line", "40321", "40321"},
		{"in a path", "/40321/\n", "40321"},
		{"after 0x", "0x40321\n", "40321"},
		{"beside a letter that is not hex", "port40321 and x40321y\n", "40321"},
		{"after a hex letter: probe40321", "probe40321\n", "40321"},
		{"before a hex letter: 40321bytes", "40321bytes\n", "40321"},
		{"the job id", "sbatch: Job 424242 to start\n", "424242"},
		{"the job id after a dash", "--- FAIL: TestX/SEEDKEY-424242 (0.00s)\n", "424242"},
		{"the job id in a file name: jobs/job424242.out", "jobs/job424242.out\n", "424242"},
		{"a run of 8 hex digits is not a hash", "bcd40321\n", "40321"},
		{"a seed with a non-hex character, after hex digits", "deadbeefSEEDKEY\n", "SEEDKEY"},
		{"a seed with a non-hex character, inside a hash-like run", "0123456789abcdefseed.example0123456789abcdef\n", "seed.example"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := leaks(c.out, leakSeeds)
			found := false
			for _, s := range got {
				found = found || s == c.seed
			}
			if !found {
				t.Errorf("the seed %q standing in %q was not reported (reported: %q)", c.seed, c.out, got)
			}
		})
	}
}

// A seed that is as long as a hash's shortest run, or longer, and stands on its
// own is a value and is reported: the run IS the seed, so there is no hash for
// it to lie in. (PR 367's condition: a standalone 9-digit job id.) Inside a
// longer run it is a hash's part and is not reported.
func TestAStandaloneSeedAsLongAsAHashIsStillALeak(t *testing.T) {
	for _, seed := range []string{"123456789", "9876543210", "abcdef123456789"} {
		for name, out := range map[string]string{
			"in a sentence":       "sbatch: Job " + seed + " queued\n",
			"as the whole output": seed,
			"after an equals":     "JOB=" + seed + "\n",
			"in a path":           "jobs/job-" + seed + ".out\n",
		} {
			t.Run(seed+"/"+name, func(t *testing.T) {
				if got := leaks(out, []string{seed}); len(got) != 1 || got[0] != seed {
					t.Errorf("a standalone seed %q in %q: reported %q, want it as a leak", seed, out, got)
				}
			})
		}
		t.Run(seed+"/inside a longer run", func(t *testing.T) {
			out := "digest e1f2" + seed + "a3b4c5\n"
			if got := leaks(out, []string{seed}); len(got) != 0 {
				t.Errorf("a seed %q inside a longer hex run %q was reported: %q", seed, out, got)
			}
		})
	}
}

// The rule's boundary, stated: a hex-only seed inside a run that is at least
// hashRunMin long and strictly longer than the seed, by however little, is
// taken for a part of a hash. So an 8-digit seed with one hex digit glued to it
// (a 9-digit run) is not reported, and nor is a 9-digit seed with one glued
// (a 10-digit run); with the glued digit gone, the same seed is a leak. The
// seeds in use are 5 and 6 digits, so for them the boundary is the run's
// length alone; this is where the rule stops for a longer seed.
func TestARunOneLongerThanALongSeedIsTakenForAHash(t *testing.T) {
	for _, seed := range []string{"12345678", "123456789"} {
		if got := leaks("job a"+seed+" queued\n", []string{seed}); len(got) != 0 {
			t.Errorf("seed %q with one hex digit glued before it was reported: %q", seed, got)
		}
		if got := leaks("job "+seed+" queued\n", []string{seed}); len(got) != 1 {
			t.Errorf("seed %q standing alone was not reported: %q", seed, got)
		}
	}
}

// A hash that holds a seed does not hide a real leak in the same output, on
// another line or on the same one.
func TestAHashHoldingASeedDoesNotHideALeakBesideIt(t *testing.T) {
	sameLine := strings.TrimSuffix(hashHoldingTheSeed, "\n") + "; port=40321\n"
	for name, out := range map[string]string{
		"another line":  hashHoldingTheSeed + "stub: port 40321 refused\n",
		"the same line": sameLine,
	} {
		t.Run(name, func(t *testing.T) {
			if got := leaks(out, leakSeeds); len(got) != 1 || got[0] != "40321" {
				t.Errorf("reported %q, want the one real leak of 40321", got)
			}
		})
	}
}

// recorder stands for *testing.T in noLeak: it records what is reported.
type recorder struct{ reports []string }

func (r *recorder) Helper() {}
func (r *recorder) Errorf(format string, args ...any) {
	r.reports = append(r.reports, fmt.Sprintf(format, args...))
}

// The world's guard is the wired one, not only the rule behind it: noLeak
// itself passes an output that carries a seed only inside a hash, reports a
// real leak, reports it when a hash that holds a seed stands beside it, and
// stays silent about a clean output.
func TestNoLeakReportsARealLeakAndPassesAHash(t *testing.T) {
	cases := []struct {
		name, out string
		reports   int
	}{
		{"a seed only inside a hash", hashHoldingTheSeed, 0},
		{"three hashes holding a seed", strings.Repeat("c04032199613610d1911832c22386ad186d39796\n", 3), 0},
		{"a clean output", "all rows passed\n", 0},
		{"a real leak", "stub: port 40321 refused\n", 1},
		{"a real leak glued to a hex letter", "probe40321\n", 1},
		{"a real leak beside a hash that holds a seed", strings.TrimSuffix(hashHoldingTheSeed, "\n") + "; port=40321\n", 1},
		{"a non-hex seed", "key SEEDKEY here\n", 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := &recorder{}
			w := &world{t: t, dir: t.TempDir(), leakSink: rec}
			w.noLeak(c.out)
			if len(rec.reports) != c.reports {
				t.Errorf("noLeak made %d reports, want %d: %q", len(rec.reports), c.reports, rec.reports)
			}
		})
	}
}
