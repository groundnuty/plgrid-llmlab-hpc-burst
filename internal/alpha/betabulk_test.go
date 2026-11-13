package alpha

import (
	"context"
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

func TestEachBulkCheckNamesItsOwnFailure(t *testing.T) {
	row := BetaBulkRow()
	judge := func(o BulkObs) []string {
		var f []string
		f = append(f, pathChecks(o)...)
		f = append(f, phaseChecks("control", o.Control)...)
		f = append(f, phaseChecks("bulk", o.Bulk)...)
		f = append(f, stallChecks(o)...)
		f = append(f, doubleChecks(o.Double)...)
		return f
	}
	if f := judge(goodBulkObs(row)); len(f) != 0 {
		t.Fatalf("a good run fails: %q", f)
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
		{"a control one nanosecond under the rule", func(o *BulkObs) { o.Control.P99 = BulkRule - time.Nanosecond }, "the control shows no stall: its gap p99 is under 85 ms, so this run cannot show a stall for the bulk path to remove"},
		{"a reply one byte short", func(o *BulkObs) { o.Bulk.ReplyBytes = BulkReplyBytes - 1 }, "the bulk transfer did not deliver its 67108864 bytes with a 200"},
		{"a control reply one byte short", func(o *BulkObs) { o.Control.ReplyBytes = BulkReplyBytes - 1 }, "the control transfer did not deliver its 67108864 bytes with a 200"},
		{"a 502 control", func(o *BulkObs) { o.Control.Status = 502 }, "the control transfer did not deliver its 67108864 bytes with a 200"},
		{"a stream cut", func(o *BulkObs) { o.Bulk.StreamsDone = BulkStreams - 1 }, "a stream beside the bulk transfer was cut or did not finish with its tokens"},
		{"a control stream cut", func(o *BulkObs) { o.Control.StreamsDone = BulkStreams - 1 }, "a stream beside the control transfer was cut or did not finish with its tokens"},
		{"a stream that ended first", func(o *BulkObs) { o.Bulk.StreamsLong = BulkStreams - 1 }, "a stream beside the bulk transfer ended before it did: its gaps do not cover the transfer"},
		{"a control stream that ended first", func(o *BulkObs) { o.Control.StreamsLong = BulkStreams - 1 }, "a stream beside the control transfer ended before it did: its gaps do not cover the transfer"},
		{"one gap too few", func(o *BulkObs) { o.Bulk.Gaps = minGaps(o.Bulk.Took) - 1 }, "too few gaps overlap the bulk transfer to measure it"},
		{"no transfer time", func(o *BulkObs) { o.Control.Took = 0 }, "too few gaps overlap the control transfer to measure it"},
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
		})
	}
	// Boundaries that pass: a control exactly at the rule is a run; gaps exactly at the minimum are enough.
	o := goodBulkObs(row)
	o.Control.P99 = BulkRule
	o.Bulk.Gaps = minGaps(o.Bulk.Took)
	if f := judge(o); len(f) != 0 {
		t.Errorf("a control at the rule, or exactly the gaps needed, fails: %q", f)
	}
}

// A reading at or above the rule, or under it but not the margin below the control, is a finding and no failure: the verdict passes, the post says
// FINDING, and the numbers it gives are the phases'.
func TestABulkFindingNeverFailsTheRow(t *testing.T) {
	row := BetaBulkRow()
	for _, c := range []struct {
		name          string
		control, bulk time.Duration
		finding       string
	}{
		{"the layout protects the streams", 150 * time.Millisecond, 55 * time.Millisecond, ""},
		{"exactly the rule", 150 * time.Millisecond, BulkRule, "the bulk path's gap p99 is 85ms, at or above the 85ms rule"},
		{"above the rule", 150 * time.Millisecond, 200 * time.Millisecond, "the bulk path's gap p99 is 200ms, at or above the 85ms rule"},
		{"under the rule by a nanosecond and well below the control", 300 * time.Millisecond, BulkRule - time.Nanosecond, ""},
		{"under the rule, less than the margin below the control", 100 * time.Millisecond, 80 * time.Millisecond, "under the rule but only 20ms below the control's 100ms"},
		{"exactly the margin below the control", 100 * time.Millisecond, 70 * time.Millisecond, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := goodBulkObs(row)
			o.Control.P99, o.Bulk.P99 = c.control, c.bulk
			got := o.Finding()
			if (c.finding == "") != (got == "") || !strings.Contains(got, c.finding) {
				t.Errorf("Finding = %q, want one holding %q", got, c.finding)
			}
			post := betaBulkPost(row, o, nil)
			if has := strings.Contains(post, "FINDING (not a failure)"); has != (c.finding != "") {
				t.Errorf("the post says FINDING: %t, want %t:\n%s", has, c.finding != "", post)
			}
			if !strings.Contains(post, "beta-athena-now-bulk: PASS (posts to #522)") {
				t.Errorf("a finding changed the verdict:\n%s", post)
			}
		})
	}
}

func TestTheBulkPostNamesTheDoubleAndItsDigestAndNoProcess(t *testing.T) {
	row := BetaBulkRow()
	o := goodBulkObs(row)
	post := betaBulkPost(row, o, nil)
	for _, want := range []string{
		"THE ENGINE WAS A DOUBLE, not vLLM: the double, SHA-256 " + o.Double.Digest,
		"the control, the transfer down the token transport as a streamed reply: gap p99 150ms",
		"the bulk path, the same bytes as a non-streamed reply: gap p99 55ms",
		"the front's bulk counter rose by 1, its fallback by 0",
		"the difference, control less bulk path: 95ms; HBEP-6's rule is 85ms and S4's margin 30ms",
		"the layout protects the token streams on this path",
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

// A finding passes through Run: the verdict has no failure, the text to post says FINDING, and the record holds the phases.
func TestABulkReadingAtOrAboveTheRuleIsAFindingNeverAFailure(t *testing.T) {
	row := BetaBulkRow()
	for _, fault := range []string{"bulk-at-the-rule", "bulk-above-the-rule", "bulk-small-margin"} {
		t.Run(fault, func(t *testing.T) {
			cfg := Config{Row: row, Dry: true, DryFault: fault, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
			rec, err := NewRecorder(cfg.Out)
			if err != nil {
				t.Fatal(err)
			}
			failures, err := Run(context.Background(), cfg, newDrySteps(t, cfg, "betabulk"), rec, t.Logf)
			if err != nil || len(failures) != 0 {
				t.Fatalf("failures %v, err %v; want a pass", failures, err)
			}
			post, _ := os.ReadFile(filepath.Join(cfg.Out, PostsFile))
			if !strings.Contains(string(post), "beta-athena-now-bulk: PASS") || !strings.Contains(string(post), "FINDING (not a failure)") {
				t.Errorf("the text to post does not say PASS with a FINDING:\n%s", post)
			}
		})
	}
}

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
