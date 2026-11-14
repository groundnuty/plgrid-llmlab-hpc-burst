package alpha

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// The bulk row's logic (betabulk.go), each check with its words written out and its boundary.

func TestTheBulkRowIsRowAsHoldOfThePostedSlot(t *testing.T) {
	r, a := BetaBulkRow(), NowServeRow()
	if r.Name != "beta-athena-now-bulk" || r.PostsTo != "#522" {
		t.Errorf("the row is %q posting to %q, want beta-athena-now-bulk posting to #522", r.Name, r.PostsTo)
	}
	b := r
	b.Name, b.PostsTo = a.Name, a.PostsTo
	if b != a {
		t.Errorf("the row differs from row A beyond its name and tracker:\n%+v\n%+v", r, a)
	}
}

// The measurement's figures are S17's and the rulings', written out here, not derived: eight streams of 20 tokens a second, a reply of 64 MiB, the
// 85 ms rule and S4's 30 ms margin.
func TestTheBulkMeasurementIsS17sWrittenOut(t *testing.T) {
	if BulkStreams != 8 || BulkTokenDelay != 50*time.Millisecond || BulkStreamTokens != 600 || BulkReplyBytes != 64<<20 || BulkRule != 85*time.Millisecond || BulkMargin != 30*time.Millisecond {
		t.Errorf("the figures are %d streams, %s a token, %d tokens, %d bytes, rule %s, margin %s", BulkStreams, BulkTokenDelay, BulkStreamTokens, BulkReplyBytes, BulkRule, BulkMargin)
	}
	// 600 tokens at 50 ms are 30 s, well past a transfer of seconds.
	if time.Duration(BulkStreamTokens)*BulkTokenDelay != 30*time.Second {
		t.Errorf("a stream lasts %s, want 30 s", time.Duration(BulkStreamTokens)*BulkTokenDelay)
	}
}

func TestGapsOverlappingTheTransferAreTheStall(t *testing.T) {
	t0 := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	ticks := []time.Time{at(0), at(50), at(100), at(300), at(350), at(400)}
	for _, c := range []struct {
		name       string
		start, end int
		want       []time.Duration
	}{
		{"the transfer inside one gap", 120, 200, []time.Duration{200 * time.Millisecond}},
		{"the transfer across two", 40, 320, []time.Duration{50 * time.Millisecond, 50 * time.Millisecond, 200 * time.Millisecond, 50 * time.Millisecond}},
		{"a transfer after every tick", 500, 600, nil},
		{"a transfer before every tick", -100, -50, nil},
		{"a gap that starts exactly at the end is not overlapped", 0, 100, []time.Duration{50 * time.Millisecond, 50 * time.Millisecond}},
		{"a gap that ends exactly at the start is not overlapped", 300, 320, []time.Duration{50 * time.Millisecond}},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := GapsOverlapping(ticks, at(c.start), at(c.end))
			if !slices.Equal(got, c.want) {
				t.Errorf("GapsOverlapping = %v, want %v", got, c.want)
			}
		})
	}
	if got := GapsOverlapping(nil, at(0), at(1)); got != nil {
		t.Errorf("no ticks: %v", got)
	}
	if got := GapsOverlapping(ticks[:1], at(0), at(1000)); got != nil {
		t.Errorf("one tick has no gap: %v", got)
	}
}

func TestPercentileIsTheNearestRank(t *testing.T) {
	d := make([]time.Duration, 100)
	for i := range d {
		d[i] = time.Duration(i+1) * time.Millisecond // 1..100 ms
	}
	rev := slices.Clone(d)
	slices.Reverse(rev) // order does not matter
	for _, c := range []struct {
		p    float64
		want time.Duration
	}{{0.99, 99 * time.Millisecond}, {0.5, 50 * time.Millisecond}, {1, 100 * time.Millisecond}, {0.01, time.Millisecond}, {0.995, 100 * time.Millisecond}} {
		if got := Percentile(rev, c.p); got != c.want {
			t.Errorf("Percentile(%v) = %s, want %s", c.p, got, c.want)
		}
	}
	for _, bad := range []float64{0, -1, 1.5} {
		if got := Percentile(d, bad); got != 0 {
			t.Errorf("Percentile(%v) = %s, want 0", bad, got)
		}
	}
	if got := Percentile(nil, 0.99); got != 0 {
		t.Errorf("no gaps: %s", got)
	}
	// The input is not reordered.
	if d[0] != time.Millisecond || d[99] != 100*time.Millisecond {
		t.Error("Percentile sorted its argument")
	}
}

func goodBulkObs(row Row) BulkObs {
	return BulkObs{Tags: []string{"tag-1"}, Deployment: row.Plan(), ReadyAt: time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC), Registered: 1,
		Control: dryBulkPhase(150*time.Millisecond, 0), Bulk: dryBulkPhase(55*time.Millisecond, 1),
		Double: DoubleObs{Name: "the double", Digest: strings.Repeat("0a", 32)}, Responses: 18, Attributed: 18}
}

// Each failed check, the broken path's and the double's, with its words and its boundary; none of them is also an INVALID, so a failure is never
// reported as a run that measured nothing.
func TestEachBulkCheckNamesItsOwnFailure(t *testing.T) {
	row := BetaBulkRow()
	judge := func(o BulkObs) []string {
		var f []string
		f = append(f, pathChecks(o)...)
		f = append(f, phaseChecks("control", o.Control)...)
		f = append(f, phaseChecks("bulk", o.Bulk)...)
		f = append(f, doubleChecks(o.Double)...)
		return f
	}
	if f, w := judge(goodBulkObs(row)), BulkInvalid(goodBulkObs(row)); len(f) != 0 || len(w) != 0 {
		t.Fatalf("a good run fails %q or is invalid %q", f, w)
	}
	for _, c := range []struct {
		name string
		mut  func(o *BulkObs)
		want string
	}{
		{"not registered", func(o *BulkObs) { o.Registered = 0 }, "no Instance's bulk transport was registered at the front"},
		{"a front without the bulk path", func(o *BulkObs) { o.Registered = -1 }, "no Instance's bulk transport was registered at the front"},
		{"the control took the bulk path", func(o *BulkObs) { o.Control.BulkSent = 1 }, "the control transfer took the bulk path: it is no control"},
		{"the bulk reply never engaged", func(o *BulkObs) { o.Bulk.BulkSent = 0 }, "the front's bulk counter did not rise by exactly one across the bulk reply: it did not take the bulk path"},
		{"two bulk replies", func(o *BulkObs) { o.Bulk.BulkSent = 2 }, "the front's bulk counter did not rise by exactly one across the bulk reply: it did not take the bulk path"},
		{"the counter unread (bulk)", func(o *BulkObs) { o.Bulk.BulkSent = -1 }, "the front's bulk counter could not be read"},
		{"the counter unread (control)", func(o *BulkObs) { o.Control.BulkSent = -1 }, "the front's bulk counter could not be read"},
		{"a fallback on the bulk reply", func(o *BulkObs) { o.Bulk.FallbackSent = 1 }, "a reply fell back to the token transport"},
		{"a fallback on the control", func(o *BulkObs) { o.Control.FallbackSent = 1 }, "a reply fell back to the token transport"},
		{"a reply one byte short", func(o *BulkObs) { o.Bulk.ReplyBytes = BulkReplyBytes - 1 }, "the bulk transfer did not deliver its 67108864 bytes with a 200"},
		{"a control reply one byte short", func(o *BulkObs) { o.Control.ReplyBytes = BulkReplyBytes - 1 }, "the control transfer did not deliver its 67108864 bytes with a 200"},
		{"a 502 control", func(o *BulkObs) { o.Control.Status = 502 }, "the control transfer did not deliver its 67108864 bytes with a 200"},
		{"a control read cut after its bytes", func(o *BulkObs) { o.Control.Cut = true }, "the control transfer's read was cut before its end"},
		{"a bulk read cut after its bytes", func(o *BulkObs) { o.Bulk.Cut = true }, "the bulk transfer's read was cut before its end"},
		{"a stream cut", func(o *BulkObs) { o.Bulk.StreamsDone = BulkStreams - 1 }, "a stream beside the bulk transfer was cut or did not finish with its tokens"},
		{"a control stream cut", func(o *BulkObs) { o.Control.StreamsDone = BulkStreams - 1 }, "a stream beside the control transfer was cut or did not finish with its tokens"},
		// A cut stream did not end long either: the cut is the failure, and no witness check speaks for it.
		{"a control stream cut that also ended first", func(o *BulkObs) { o.Control.StreamsDone, o.Control.StreamsLong = BulkStreams-1, BulkStreams-1 }, "a stream beside the control transfer was cut or did not finish with its tokens"},
		{"no double named", func(o *BulkObs) { o.Double.Name = "" }, "the engine double was not named with the SHA-256 of its binary"},
		{"a digest of 63 characters", func(o *BulkObs) { o.Double.Digest = o.Double.Digest[:63] }, "the engine double was not named with the SHA-256 of its binary"},
		{"an upper-case digest", func(o *BulkObs) { o.Double.Digest = strings.ToUpper(o.Double.Digest) }, "the engine double was not named with the SHA-256 of its binary"},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := goodBulkObs(row)
			c.mut(&o)
			if f := judge(o); !slices.Contains(f, c.want) {
				t.Errorf("the checks say %q, want them to hold %q", f, c.want)
			}
			if w := BulkInvalid(o); len(w) != 0 {
				t.Errorf("a broken path is also reported as a run with no witness: %q", w)
			}
		})
	}
	// The control's p99 is no check (the ruling: 85 ms is the reading's threshold, not a validity bar): a control one nanosecond under it fails
	// nothing and is no INVALID; nor is a control exactly at it, or exactly the gaps needed.
	for _, c := range []struct {
		name string
		mut  func(o *BulkObs)
	}{
		{"a control one nanosecond under the threshold", func(o *BulkObs) { o.Control.P99 = BulkRule - time.Nanosecond }},
		{"a control at the threshold", func(o *BulkObs) { o.Control.P99 = BulkRule }},
		{"a control with no stall at all", func(o *BulkObs) { o.Control.P99 = 50 * time.Millisecond }},
		{"exactly the gaps needed", func(o *BulkObs) { o.Bulk.Gaps, o.Control.Gaps = minGaps(o.Bulk.Took), minGaps(o.Control.Took) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := goodBulkObs(row)
			c.mut(&o)
			if f, w := judge(o), BulkInvalid(o); len(f) != 0 || len(w) != 0 {
				t.Errorf("fails %q or is invalid %q, want neither", f, w)
			}
		})
	}
}

// The saturation witness's checks (BulkInvalid), each with its words and its boundary: the transfer timed, its streams across the whole of it, and
// gaps enough. Each makes the run INVALID and fails nothing.
func TestEachMissingWitnessNamesItselfAndFailsNothing(t *testing.T) {
	row := BetaBulkRow()
	for _, c := range []struct {
		name string
		mut  func(o *BulkObs)
		want []string
	}{
		{"a control stream that ended first", func(o *BulkObs) { o.Control.StreamsLong = BulkStreams - 1 },
			[]string{"a stream beside the control transfer ended before it did: its gaps do not cover the transfer"}},
		{"a bulk stream that ended first", func(o *BulkObs) { o.Bulk.StreamsLong = BulkStreams - 1 },
			[]string{"a stream beside the bulk transfer ended before it did: its gaps do not cover the transfer"}},
		{"one control gap too few", func(o *BulkObs) { o.Control.Gaps = minGaps(o.Control.Took) - 1 },
			[]string{"too few gaps overlap the control transfer to measure it"}},
		{"one bulk gap too few", func(o *BulkObs) { o.Bulk.Gaps = minGaps(o.Bulk.Took) - 1 },
			[]string{"too few gaps overlap the bulk transfer to measure it"}},
		{"no control transfer time", func(o *BulkObs) { o.Control.Took = 0 },
			[]string{"the control transfer was not timed, so it has no rate", "too few gaps overlap the control transfer to measure it"}},
		{"a negative bulk transfer time", func(o *BulkObs) { o.Bulk.Took = -time.Millisecond },
			[]string{"the bulk transfer was not timed, so it has no rate", "too few gaps overlap the bulk transfer to measure it"}},
		{"both phases at once", func(o *BulkObs) { o.Control.StreamsLong, o.Bulk.Gaps = 0, 0 },
			[]string{"a stream beside the control transfer ended before it did: its gaps do not cover the transfer", "too few gaps overlap the bulk transfer to measure it"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := goodBulkObs(row)
			c.mut(&o)
			if got := BulkInvalid(o); !slices.Equal(got, c.want) {
				t.Errorf("BulkInvalid = %q, want %q", got, c.want)
			}
			// The run-level verdict with a real ledger is TestTheBulkRowsInvalidRunsThroughRun's.
			if f := append(append(pathChecks(o), phaseChecks("control", o.Control)...), phaseChecks("bulk", o.Bulk)...); len(f) != 0 {
				t.Errorf("a missing witness fails the row: %q", f)
			}
		})
	}
}

// A control under the threshold, a bulk reading at or above it, or a bulk reading under it but less than the margin below a control that stalled, is a
// finding and no failure: the verdict passes, the post has one FINDING line for each, and the words are the ruling's. Under the threshold the
// control's own finding stands alone: the margin is a stalled control's.
func TestABulkFindingNeverFailsTheRow(t *testing.T) {
	row := BetaBulkRow()
	const noStall = "under the 85ms threshold: no stall to remove on this path (HBEP-6, open question 12)"
	for _, c := range []struct {
		name          string
		control, bulk time.Duration
		findings      []string
	}{
		{"the layout protects the streams", 150 * time.Millisecond, 55 * time.Millisecond, nil},
		{"exactly the rule", 150 * time.Millisecond, BulkRule, []string{"the bulk path's gap p99 is 85ms, at or above the 85ms rule"}},
		{"above the rule", 150 * time.Millisecond, 200 * time.Millisecond, []string{"the bulk path's gap p99 is 200ms, at or above the 85ms rule"}},
		{"under the rule by a nanosecond and well below the control", 300 * time.Millisecond, BulkRule - time.Nanosecond, nil},
		{"under the rule, less than the margin below the control", 100 * time.Millisecond, 80 * time.Millisecond, []string{"under the rule but only 20ms below the control's 100ms"}},
		{"exactly the margin below the control", 100 * time.Millisecond, 70 * time.Millisecond, nil},
		{"a control exactly at the threshold, the margin below it", BulkRule, 55 * time.Millisecond, nil},
		// A nanosecond under: the control's finding, and no margin one, though the bulk path is less than 30 ms below it.
		{"a control a nanosecond under the threshold", BulkRule - time.Nanosecond, 55 * time.Millisecond, []string{"the control's gap p99 is 84.999999ms, " + noStall}},
		{"a control with no stall and a bulk path just under it", 60 * time.Millisecond, 58 * time.Millisecond, []string{"the control's gap p99 is 60ms, " + noStall}},
		{"a control with no stall and a bulk path above the rule", 50 * time.Millisecond, 120 * time.Millisecond,
			[]string{"the control's gap p99 is 50ms, " + noStall, "the bulk path's gap p99 is 120ms, at or above the 85ms rule"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := goodBulkObs(row)
			o.Control.P99, o.Bulk.P99 = c.control, c.bulk
			got := o.Findings()
			if len(got) != len(c.findings) {
				t.Fatalf("Findings = %q, want %d holding %q", got, len(c.findings), c.findings)
			}
			for i, want := range c.findings {
				if !strings.Contains(got[i], want) {
					t.Errorf("finding %d is %q, want one holding %q", i, got[i], want)
				}
			}
			post := betaBulkPost(row, o, nil)
			if n := strings.Count(post, "- FINDING (not a failure): "); n != len(c.findings) {
				t.Errorf("the post has %d FINDING lines, want %d:\n%s", n, len(c.findings), post)
			}
			if !strings.Contains(post, "beta-athena-now-bulk: PASS (posts to #522)\n") {
				t.Errorf("a finding changed the verdict:\n%s", post)
			}
			if protects := strings.Contains(post, "the layout protects the token streams on this path"); protects != (len(c.findings) == 0) {
				t.Errorf("the post says the layout protects the streams: %t, want %t:\n%s", protects, len(c.findings) == 0, post)
			}
		})
	}
}

func TestTheBulkPostNamesTheDoubleAndItsDigestAndNoProcess(t *testing.T) {
	row := BetaBulkRow()
	o := goodBulkObs(row)
	post := betaBulkPost(row, o, nil)
	for _, want := range []string{
		"beta-athena-now-bulk: PASS (posts to #522)\n",
		"THE ENGINE WAS A DOUBLE, not vLLM: the double, SHA-256 " + o.Double.Digest,
		"the control, the transfer down the token transport as a streamed reply: gap p99 150ms (p50 52ms) over 480 gaps; the transfer took 3s (22.4 MB/s)",
		"the bulk path, the same bytes as a non-streamed reply: gap p99 55ms",
		"the front's bulk counter rose by 1, its fallback by 0",
		"the difference, control less bulk path: 95ms; HBEP-6's threshold is 85ms (open question 12's reading, not a validity bar) and S4's margin 30ms",
		// The ruling's answer 1: the control is accepted, and the post names its difference from S4's.
		"- the control is not S4's: it is this same bulk-on stack carrying the 64 MiB down the token transport as a streamed reply, where S4's is a second stack with the bulk path off carrying it as a non-streamed reply (the science agent's ruling on #521)\n",
		// The saturation witness, worked out by hand: 67108864 bytes in 3 s are 22.37 MB/s; 8 streams at 20 tokens a second give 480 gaps in 3 s, half of them 240.
		"- the saturation witness: the control's 67108864 bytes counted at the front's client listener in 3s, 22.4 MB/s, with all 8 streams running across the transfer (480 gaps overlap it, at least 240 needed)\n",
		"- the layout protects the token streams on this path: the control stalled them to at least 85ms, and the bulk path's p99 is under 85ms and at least 30ms below the control's\n",
		"responses with attribution: 18 of 18",
	} {
		if !strings.Contains(post, want) {
			t.Errorf("the text to post lacks %q:\n%s", want, post)
		}
	}
	if p := betaBulkPost(row, o, []string{"x"}); !strings.Contains(p, "beta-athena-now-bulk: FAIL (posts to #522)") {
		t.Errorf("a failed run's text lacks its verdict line:\n%s", p)
	}
}

// A run with no saturation witness posts INVALID with why, the witness ABSENT with the same words, and no reading and no finding, even where the p99s
// would make one; with a failure beside it, the failure's verdict wins.
func TestAnInvalidBulkRunPostsWhyAndNoFinding(t *testing.T) {
	row := BetaBulkRow()
	o := goodBulkObs(row)
	o.Control.StreamsLong = BulkStreams - 1
	o.Bulk.P99 = 200 * time.Millisecond // a finding, were the run a measurement
	const why = "a stream beside the control transfer ended before it did: its gaps do not cover the transfer"
	post := betaBulkPost(row, o, nil)
	for _, want := range []string{
		"beta-athena-now-bulk: INVALID (posts to #522)\n- no saturation witness, so no reading: " + why + "\n",
		"- the saturation witness: ABSENT (" + why + "): the gaps measure nothing, so there is no reading and no finding\n",
	} {
		if !strings.Contains(post, want) {
			t.Errorf("the text to post lacks %q:\n%s", want, post)
		}
	}
	for _, not := range []string{"PASS", "FAIL", "FINDING", "the layout protects", "counted at the front's client listener"} {
		if strings.Contains(post, not) {
			t.Errorf("an INVALID run's text says %q:\n%s", not, post)
		}
	}
	if p := betaBulkPost(row, o, []string{"a reply fell back to the token transport"}); !strings.HasPrefix(p, "beta-athena-now-bulk: FAIL (posts to #522)\n") || strings.Contains(p, "INVALID") {
		t.Errorf("a failure beside a missing witness is not the verdict:\n%s", p)
	}
}

// The bulk row runs in host-dialed mode alone, the path the criterion names.
func TestTheBulkRowRefusesAnotherDialHomeMode(t *testing.T) {
	const row = BetaBulkName
	run := func(mode string) error {
		m := liveSettings(row)
		m[envProfile] = BetaBulkRow().ProfileName()
		m[envMode] = mode
		if mode == "host-dialed" {
			delete(m, envAddr)
		}
		if mode == "ssh" {
			m[envKey] = "/private/key"
		}
		_, skip, err := ConfigFromEnv(env(m), row)
		if skip != "" {
			t.Fatalf("skip %q", skip)
		}
		return err
	}
	if err := run("host-dialed"); err != nil {
		t.Errorf("host-dialed: %v", err)
	}
	for _, mode := range []string{"direct", "ssh"} {
		if err := run(mode); err == nil || !strings.Contains(err.Error(), "must be host-dialed for "+BetaBulkName) {
			t.Errorf("%s: err = %v, want a refusal that names the bulk row and host-dialed", mode, err)
		}
	}
}

// The bulk row's stack runs the bulk path and the other rows' do not run it.
func TestOnlyTheBulkRowsStackRunsTheBulkPath(t *testing.T) {
	opts := hostOptionsFor(BetaBulkName)
	if len(opts) != 1 || reflect.ValueOf(opts[0]).Pointer() != reflect.ValueOf(stack.BulkPath).Pointer() {
		t.Fatalf("the bulk row's host options are %d, want exactly stack.BulkPath", len(opts))
	}
	for _, name := range []string{BetaRouterName, BetaKeeperName} {
		for _, o := range hostOptionsFor(name) {
			if reflect.ValueOf(o).Pointer() == reflect.ValueOf(stack.BulkPath).Pointer() {
				t.Errorf("%s runs the bulk path", name)
			}
		}
	}
}

// A finding passes through Run: the verdict has no failure and no error, the record says pass, and the text to post says PASS with the finding's
// words. A control under 85 ms is one of them (the ruling, 1(b)), never the failure it was before the ruling.
func TestABulkReadingAtOrAboveTheRuleIsAFindingNeverAFailure(t *testing.T) {
	for _, c := range []struct{ fault, finding string }{
		{"bulk-at-the-rule", "- FINDING (not a failure): the bulk path's gap p99 is 85ms, at or above the 85ms rule"},
		{"bulk-above-the-rule", "- FINDING (not a failure): the bulk path's gap p99 is 200ms, at or above the 85ms rule"},
		{"bulk-small-margin", "- FINDING (not a failure): the bulk path's gap p99 is 80ms, under the rule but only 20ms below the control's 100ms"},
		{"control-no-stall", "- FINDING (not a failure): the control's gap p99 is 84.999999ms, under the 85ms threshold: no stall to remove on this path (HBEP-6, open question 12)\n"},
	} {
		t.Run(c.fault, func(t *testing.T) {
			out, failures, err := runBulkDry(t, c.fault)
			if err != nil || len(failures) != 0 {
				t.Fatalf("failures %v, err %v; want a pass", failures, err)
			}
			post, _ := os.ReadFile(filepath.Join(out, PostsFile))
			if !strings.HasPrefix(string(post), "beta-athena-now-bulk: PASS (posts to #522)\n") || !strings.Contains(string(post), c.finding) {
				t.Errorf("the text to post does not say PASS with %q:\n%s", c.finding, post)
			}
			if v := recordVerdict(t, out); v != "pass" {
				t.Errorf("the record's verdict is %q, want pass", v)
			}
		})
	}
}

// runBulkDry is one dry run of the bulk row with the fault, through Run: its record directory, its failed checks and its error.
func runBulkDry(t *testing.T, fault string) (string, []string, error) {
	t.Helper()
	cfg := Config{Row: BetaBulkRow(), Dry: true, DryFault: fault, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
	rec, err := NewRecorder(cfg.Out)
	if err != nil {
		t.Fatal(err)
	}
	failures, err := Run(context.Background(), cfg, newDrySteps(t, cfg, "betabulk"), rec, t.Logf)
	return cfg.Out, failures, err
}

// recordVerdict is the verdict the run's record holds.
func recordVerdict(t *testing.T, out string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(out, RecordFile))
	if err != nil {
		t.Fatal(err)
	}
	var r struct {
		Verdict  string   `json:"verdict"`
		Failures []string `json:"failures"`
	}
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	if r.Verdict != "fail" && len(r.Failures) != 0 {
		t.Errorf("the record's verdict is %q beside failures %q", r.Verdict, r.Failures)
	}
	return r.Verdict
}

// A run with no saturation witness and no failure is INVALID through Run: ErrInvalid with the witness's words, no failed check, never NOT RUN,
// the record's verdict "invalid", and the text to post headed INVALID with why. Each fault takes one of the witness's checks away.
func TestTheBulkRowsInvalidRunsThroughRun(t *testing.T) {
	for _, c := range betaBulkInvalidCases {
		t.Run(c.fault, func(t *testing.T) {
			out, failures, err := runBulkDry(t, c.fault)
			if len(failures) != 0 {
				t.Fatalf("an INVALID run failed checks %q", failures)
			}
			var ie *InvalidError
			if !errors.As(err, &ie) || !errors.Is(err, ErrInvalid) || !Invalid(BetaBulkRow(), err) {
				t.Fatalf("err = %v, want an InvalidError", err)
			}
			if NotRun(BetaBulkRow(), err) {
				t.Errorf("an INVALID run is also NOT RUN: %v", err)
			}
			if !slices.Equal(ie.Why, c.why) {
				t.Errorf("why = %q, want %q", ie.Why, c.why)
			}
			if v := recordVerdict(t, out); v != "invalid" {
				t.Errorf("the record's verdict is %q, want invalid", v)
			}
			// The head's two lines, with the front's line between them (InsertFrontLine).
			post, _ := os.ReadFile(filepath.Join(out, PostsFile))
			if !strings.HasPrefix(string(post), "beta-athena-now-bulk: INVALID (posts to #522)\n") {
				t.Errorf("the text to post is not headed INVALID:\n%s", post)
			}
			if why := "\n- no saturation witness, so no reading: " + strings.Join(c.why, "; ") + "\n"; !strings.Contains(string(post), why) {
				t.Errorf("the text to post lacks %q:\n%s", why, post)
			}
		})
	}
}

// betaBulkInvalidCases are the bulk row's dry faults that end INVALID, each with the witness's words. They are not in betaBulkCases, whose runner
// wants a pass or a failure with no error.
var betaBulkInvalidCases = []struct {
	fault string
	why   []string
}{
	{"control-stream-short", []string{"a stream beside the control transfer ended before it did: its gaps do not cover the transfer"}},
	{"bulk-stream-short", []string{"a stream beside the bulk transfer ended before it did: its gaps do not cover the transfer"}},
	{"control-few-gaps", []string{"too few gaps overlap the control transfer to measure it"}},
	{"bulk-few-gaps", []string{"too few gaps overlap the bulk transfer to measure it"}},
	{"control-untimed", []string{"the control transfer was not timed, so it has no rate", "too few gaps overlap the control transfer to measure it"}},
}

// A close that fails after an INVALID run is a failure, never an INVALID: it may have left a job. The control: the same run whose close succeeds is
// INVALID.
func TestAFailedCloseAfterAnInvalidRunIsAFailure(t *testing.T) {
	for _, c := range []struct {
		name     string
		closeErr error
		invalid  bool
	}{{"the close succeeds", nil, true}, {"the close fails", errors.New("the stack did not stop"), false}} {
		t.Run(c.name, func(t *testing.T) {
			cfg := Config{Row: BetaBulkRow(), Dry: true, DryFault: "control-few-gaps", Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
			rec, err := NewRecorder(cfg.Out)
			if err != nil {
				t.Fatal(err)
			}
			_, err = Run(context.Background(), cfg, closeFails{newDrySteps(t, cfg, "betabulk"), c.closeErr}, rec, t.Logf)
			if Invalid(cfg.Row, err) != c.invalid || NotRun(cfg.Row, err) {
				t.Fatalf("err = %v: INVALID %t, want %t, and never NOT RUN", err, Invalid(cfg.Row, err), c.invalid)
			}
			if !c.invalid && (err == nil || !strings.Contains(err.Error(), "closing the steps after a run with no saturation witness: the stack did not stop")) {
				t.Errorf("err = %v, want the close's failure named", err)
			}
		})
	}
}

// closeFails is the dry steps with a close that returns err.
type closeFails struct {
	*drySteps
	err error
}

func (c closeFails) Close(context.Context) error { return c.err }

// The live flow refuses at once, before it touches the cluster, when the world cannot name the engine double and its digest: a real vLLM cannot be
// held saturating the path, so a run that read the bulk path's buffer on one would read nothing.
func TestTheBulkLiveFlowRefusesWithoutAStagedDouble(t *testing.T) {
	r := &liveRun{} // nothing of it is touched before the refusal
	if _, err := r.BetaBulk(context.Background(), Plan{}); !errors.Is(err, ErrNoStagedDouble) {
		t.Fatalf("BetaBulk with no staged double = %v, want ErrNoStagedDouble", err)
	}
	if NotRun(BetaBulkRow(), ErrNoStagedDouble) {
		t.Error("a refusal for want of a staged double ends the run NOT RUN: it is a failure of the setup, and the script holds the row pending so that it is never reached")
	}
}

// minGaps is half of what BulkStreams streams at BulkTokenDelay give across the transfer, written out for the two transfers the stand-in and the
// rehearsal measured: a stream of 20 tokens a second gives 20 gaps a second.
func TestMinGapsIsHalfOfWhatEightStreamsGive(t *testing.T) {
	for _, c := range []struct {
		took time.Duration
		want int
	}{{3 * time.Second, 240}, {2704 * time.Millisecond, 216}, {time.Second, 80}, {49 * time.Millisecond, 0}, {0, 0}} {
		if got := minGaps(c.took); got != c.want {
			t.Errorf("minGaps(%s) = %d, want %d", c.took, got, c.want)
		}
	}
}

// The bulk path takes up's block from 13 ports to 16 (base+13 to base+15, the second frps), so the bulk row's client listener may not be where the
// other rows' is, base+13; every port of its stack is distinct and inside the alpha stack's block of IMPL-COMMON, 29700 to 29719.
func TestTheBulkRowsPortsAreDistinctAndInsideTheAlphaBlock(t *testing.T) {
	bulk := portsFor(BetaBulkName, livePorts("127.0.0.1:29714"))
	bulk.BulkPath = true
	seen := map[int]bool{}
	for _, p := range bulk.All() {
		if seen[p] {
			t.Errorf("port %d is used twice in the bulk row's stack %v", p, bulk.All())
		}
		seen[p] = true
		if p < 29700 || p > 29719 {
			t.Errorf("port %d is outside the alpha stack's block 29700-29719", p)
		}
	}
	if bulk.ClientAddr != "127.0.0.1:29716" || bulk.DialHomeListen != "127.0.0.1:29717" {
		t.Errorf("the bulk row's client and dial-home listeners are %s and %s, want 29716 and 29717", bulk.ClientAddr, bulk.DialHomeListen)
	}
	// The other rows keep the base block's listeners.
	for _, name := range []string{BetaRouterName, BetaKeeperName, "alpha-athena-now-serve"} {
		if p := portsFor(name, livePorts("127.0.0.1:29714")); p.ClientAddr != "127.0.0.1:29713" || p.DialHomeListen != "127.0.0.1:29714" {
			t.Errorf("%s's listeners changed: %s and %s", name, p.ClientAddr, p.DialHomeListen)
		}
	}
	// The control of this test: the base block's client listener is inside the bulk path's block, which is why the bulk row moved it.
	base := livePorts("")
	base.BulkPath = true
	count := 0
	for _, p := range base.All() {
		if p == 29713 {
			count++
		}
	}
	if count != 2 {
		t.Errorf("port 29713 appears %d times in the base block with the bulk path, want 2 (the client listener and the second frps): the reason the row moved", count)
	}
}

func TestAStreamBesideATransferIsDoneWhenCompleteAndWholeAndLongWhenItOutlastsIt(t *testing.T) {
	t0 := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	end := t0.Add(3 * time.Second)
	whole := Stream{Complete: true, Tokens: BulkStreamTokens}
	for _, c := range []struct {
		name       string
		s          Stream
		lastTick   time.Time
		noTicks    bool
		done, long bool
	}{
		{"whole and outlasting the transfer", whole, end.Add(time.Nanosecond), false, true, true},
		{"whole and ending at the transfer's last byte", whole, end, false, true, false},
		{"whole and ending before it", whole, end.Add(-time.Second), false, true, false},
		{"whole with no ticks", whole, time.Time{}, true, true, false},
		{"one token short", Stream{Complete: true, Tokens: BulkStreamTokens - 1}, end.Add(time.Second), false, false, false},
		{"one token over", Stream{Complete: true, Tokens: BulkStreamTokens + 1}, end.Add(time.Second), false, false, false},
		{"cut", Stream{Complete: true, CutBy: "tunnel", Tokens: BulkStreamTokens}, end.Add(time.Second), false, false, false},
		{"incomplete", Stream{Tokens: BulkStreamTokens}, end.Add(time.Second), false, false, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			var ticks []time.Time
			if !c.noTicks {
				ticks = []time.Time{t0, c.lastTick}
			}
			if done, long := StreamBeside(c.s, ticks, end); done != c.done || long != c.long {
				t.Errorf("StreamBeside = done %t, long %t; want %t, %t", done, long, c.done, c.long)
			}
		})
	}
}
