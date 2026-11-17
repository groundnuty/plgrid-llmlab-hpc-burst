package lifecycle

// Unit tests of the throughput gate's pure parts (HBEP-7, Over-allocation and
// qualification (beta), The throughput gate; Test plan, Over-allocation and the
// throughput gate): the key, matching, the limits and the message. Every
// vector, entry, time and limit is a literal worked out by hand from the HBEP,
// never rendered by renderLaunch and parsed back, never taken from a gate
// helper's own output (gate_rig_test.go runs the same rows through the pass).

import (
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func gateFlags(kv ...string) map[string]v1alpha1.FlagValue {
	out := map[string]v1alpha1.FlagValue{}
	for i := 0; i < len(kv); i += 2 {
		out[kv[i]] = v1alpha1.FlagValue(kv[i+1])
	}
	return out
}

// gateKey is the fixture Instance's key (HBEP-7's Test plan): max-num-seqs 16,
// 8 sessions, contexts of 1,024 and outputs of 128 tokens.
func gateKey() v1alpha1.ReferenceKey {
	return v1alpha1.ReferenceKey{DeploymentID: "0123456789ab", CapacityFlags: gateFlags("max-num-seqs", "16"), Sessions: 8, ContextTokens: 1024, OutputTokens: 128}
}

// gateFixtureEntry is the fixture's entry for key: 20 ms per output token and
// 400 ms to first token, the default tolerance, dated 2026-10-23.
func gateFixtureEntry(key v1alpha1.ReferenceKey) v1alpha1.ExpectedPerformance {
	return v1alpha1.ExpectedPerformance{
		ReferenceKey: key, TimePerOutputToken: "20ms", TimeToFirstToken: "400ms",
		Method: "a hand-written fixture", Source: "gate_test.go", Date: "2026-10-23",
	}
}

func gateProfile(entries ...v1alpha1.ExpectedPerformance) *v1alpha1.PlatformProfile {
	return &v1alpha1.PlatformProfile{Spec: v1alpha1.PlatformProfileSpec{ExpectedPerformance: entries}}
}

// The command vectors are written by hand, as renderLaunch's doc says it writes
// them: the program, the framework's own flags (each name and value two
// arguments), then --name=value, a boolean as --name or --no-name, and
// --max-num-seqs=<n> last.
func TestLaunchCapacityFlagsFromHandWrittenCommands(t *testing.T) {
	framework := []string{"vllm", "serve", "/model", "--uds", "/run/engine-0.sock", "--served-model-name", "example-model"}
	for _, tc := range []struct {
		name       string
		tail       []string
		maxNumSeqs int32
		want       map[string]v1alpha1.FlagValue
	}{
		{"no capacity flag but max-num-seqs", []string{"--max-model-len=32768", "--dtype=bfloat16", "--max-num-seqs=16"}, 16,
			gateFlags("max-num-seqs", "16")},
		{"a value flag keeps its text", []string{"--gpu-memory-utilization=0.90", "--max-num-seqs=16"}, 16,
			gateFlags("max-num-seqs", "16", "gpu-memory-utilization", "0.90")},
		{"a boolean as --name is true and --no-name is false", []string{"--enable-prefix-caching", "--no-enable-chunked-prefill", "--max-num-seqs=16"}, 16,
			gateFlags("max-num-seqs", "16", "enable-prefix-caching", "true", "enable-chunked-prefill", "false")},
		{"every capacity flag", []string{"--enable-prefix-caching", "--max-num-batched-tokens=8192", "--enable-chunked-prefill", "--gpu-memory-utilization=0.5", "--kv-cache-memory-bytes=1000000", "--cpu-offload-gb=4", "--max-num-seqs=8"}, 8,
			gateFlags("max-num-seqs", "8", "enable-prefix-caching", "true", "max-num-batched-tokens", "8192", "enable-chunked-prefill", "true", "gpu-memory-utilization", "0.5", "kv-cache-memory-bytes", "1000000", "cpu-offload-gb", "4")},
		{"an input flag, a negated input boolean and a transport flag are no capacity flag", []string{"--no-enforce-eager", "--quantization=fp8", "--host=0.0.0.0", "--max-num-seqs=16"}, 16,
			gateFlags("max-num-seqs", "16")},
		{"the last of a repeated flag wins, as in argparse", []string{"--gpu-memory-utilization=0.5", "--gpu-memory-utilization=0.8", "--max-num-seqs=16"}, 16,
			gateFlags("max-num-seqs", "16", "gpu-memory-utilization", "0.8")},
		{"max-num-seqs is launch.maxNumSeqs, not the command's", []string{"--max-num-seqs=32"}, 16,
			gateFlags("max-num-seqs", "16")},
		{"max-num-seqs is there when the command has none", []string{"--dtype=bfloat16"}, 16,
			gateFlags("max-num-seqs", "16")},
		{"a spelling with an underscore is the dashed name", []string{"--gpu_memory_utilization=0.7", "--max-num-seqs=16"}, 16,
			gateFlags("max-num-seqs", "16", "gpu-memory-utilization", "0.7")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := v1alpha1.Launch{Command: append(append([]string(nil), framework...), tc.tail...), MaxNumSeqs: tc.maxNumSeqs}
			got := launchCapacityFlags(l)
			if len(got) != len(tc.want) {
				t.Fatalf("flags %v, want %v", got, tc.want)
			}
			for name, v := range tc.want {
				if got[name] != v {
					t.Errorf("flag %s = %q, want %q (all: %v)", name, got[name], v, got)
				}
			}
		})
	}
}

// The key's shape is the one the warm-up ran, handed to instanceKey: it takes the sessions, the typical context and
// the typical output as given, and its ID is the display form. (Where the shape comes from, the verification record
// and the settled round, is gate_shape_test.go's.)
func TestInstanceKeyTakesTheShapeTheWarmUpRan(t *testing.T) {
	full := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	in := &v1alpha1.Instance{Spec: v1alpha1.InstanceSpec{
		DeploymentID: v1alpha1.FullDeploymentID(full),
		Launch:       v1alpha1.Launch{Command: []string{"vllm", "serve", "/model", "--max-num-seqs=16"}, MaxNumSeqs: 16},
	}}
	k := instanceKey(in, ranShape{sessions: 9, context: 2048, output: 64})
	if k.DeploymentID != "0123456789ab" || k.Sessions != 9 || k.ContextTokens != 2048 || k.OutputTokens != 64 || len(k.CapacityFlags) != 1 || k.CapacityFlags["max-num-seqs"] != "16" {
		t.Errorf("key %+v, want ID 0123456789ab, 9 sessions, 2048 and 64 tokens, the one flag max-num-seqs 16", k)
	}
}

// Matching (HBEP-4, Expected performance; HBEP-7, The throughput gate): the
// ID, the flag-name set and the shape equal, each value equal by sameSetting.
func TestMatchReferenceKey(t *testing.T) {
	now := time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)
	with := func(edit func(*v1alpha1.ReferenceKey)) v1alpha1.ReferenceKey {
		k := gateKey()
		k.CapacityFlags = gateFlags("max-num-seqs", "16")
		edit(&k)
		return k
	}
	for _, tc := range []struct {
		name     string
		instance v1alpha1.ReferenceKey
		entry    v1alpha1.ReferenceKey
		matches  bool
		why      string // a text of the record-only message when nothing matches
	}{
		{"the same key", gateKey(), gateKey(), true, ""},
		{"0.90 is 0.9",
			with(func(k *v1alpha1.ReferenceKey) {
				k.CapacityFlags = gateFlags("max-num-seqs", "16", "gpu-memory-utilization", "0.90")
			}),
			with(func(k *v1alpha1.ReferenceKey) {
				k.CapacityFlags = gateFlags("max-num-seqs", "16", "gpu-memory-utilization", "0.9")
			}),
			true, ""},
		{"true is True",
			with(func(k *v1alpha1.ReferenceKey) {
				k.CapacityFlags = gateFlags("max-num-seqs", "16", "enable-prefix-caching", "true")
			}),
			with(func(k *v1alpha1.ReferenceKey) {
				k.CapacityFlags = gateFlags("max-num-seqs", "16", "enable-prefix-caching", "True")
			}),
			true, ""},
		{"0.90 is not 0.85",
			with(func(k *v1alpha1.ReferenceKey) {
				k.CapacityFlags = gateFlags("max-num-seqs", "16", "gpu-memory-utilization", "0.90")
			}),
			with(func(k *v1alpha1.ReferenceKey) {
				k.CapacityFlags = gateFlags("max-num-seqs", "16", "gpu-memory-utilization", "0.85")
			}),
			false, "gpu-memory-utilization=0.90"},
		{"an entry flag the Instance does not declare",
			gateKey(),
			with(func(k *v1alpha1.ReferenceKey) {
				k.CapacityFlags = gateFlags("max-num-seqs", "16", "enable-prefix-caching", "true")
			}),
			false, "max-num-seqs=16"},
		{"an Instance flag the entry does not carry",
			with(func(k *v1alpha1.ReferenceKey) {
				k.CapacityFlags = gateFlags("max-num-seqs", "16", "enable-prefix-caching", "true")
			}),
			gateKey(),
			false, "enable-prefix-caching=true"},
		{"another flag of the same count",
			with(func(k *v1alpha1.ReferenceKey) {
				k.CapacityFlags = gateFlags("max-num-seqs", "16", "cpu-offload-gb", "4")
			}),
			with(func(k *v1alpha1.ReferenceKey) {
				k.CapacityFlags = gateFlags("max-num-seqs", "16", "kv-cache-memory-bytes", "4")
			}),
			false, "cpu-offload-gb=4"},
		{"max-num-seqs 32",
			with(func(k *v1alpha1.ReferenceKey) { k.CapacityFlags = gateFlags("max-num-seqs", "32") }), gateKey(), false, "max-num-seqs=32"},
		{"16 sessions", with(func(k *v1alpha1.ReferenceKey) { k.Sessions = 16 }), gateKey(), false, "16 sessions"},
		{"a typical context of 2,048", with(func(k *v1alpha1.ReferenceKey) { k.ContextTokens = 2048 }), gateKey(), false, "2048 context tokens"},
		{"a typical output of 256", with(func(k *v1alpha1.ReferenceKey) { k.OutputTokens = 256 }), gateKey(), false, "256 output tokens"},
		{"another deployment ID", with(func(k *v1alpha1.ReferenceKey) { k.DeploymentID = "ba9876543210" }), gateKey(), false, "deployment ID ba9876543210"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := gateFixtureEntry(tc.entry)
			ref := matchReference(gateProfile(e), tc.instance, now)
			if tc.matches {
				if ref.entry == nil || ref.date != "2026-10-23" || ref.why != "" {
					t.Fatalf("reference %+v, want the entry dated 2026-10-23", ref)
				}
				return
			}
			if ref.entry != nil || ref.date != "" || !strings.Contains(ref.why, "no entry matches") || !strings.Contains(ref.why, tc.why) {
				t.Fatalf("reference %+v, want no entry, whose reason holds %q and the key", ref, tc.why)
			}
		})
	}
}

// Two entries with one key: neither is the reference, the message names each by
// its date, and an ambiguous profile never gates, aged or not (plan, Q10).
func TestMatchReferenceTwoEntriesNameEachDate(t *testing.T) {
	now := time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)
	older := gateFixtureEntry(gateKey())
	older.Date = "2026-01-05" // 90 days or more old: still counted, since it matches
	newer := gateFixtureEntry(gateKey())
	ref := matchReference(gateProfile(newer, older), gateKey(), now)
	if ref.entry != nil || ref.date != "" {
		t.Fatalf("an ambiguous profile gave the reference %+v", ref)
	}
	for _, want := range []string{"2 entries match", "2026-01-05", "2026-10-23"} {
		if !strings.Contains(ref.why, want) {
			t.Errorf("reason %q does not hold %q", ref.why, want)
		}
	}
}

// Two entries whose keys differ only by flag values that sameSetting reads as
// equal (0.9 and 0.90) are both the Instance's match, so the gate records only:
// the CRD's uniqueness rule compares those values as text and admits the pair,
// and this is what covers it (HBEP-4: entries are unique by key, and a CRD
// rule cannot compare values as sameSetting does).
func TestMatchReferenceTwoEntriesEqualOnlyBySameSetting(t *testing.T) {
	now := time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)
	key := func(util string) v1alpha1.ReferenceKey {
		k := gateKey()
		k.CapacityFlags = gateFlags("max-num-seqs", "16", "gpu-memory-utilization", util)
		return k
	}
	a, b := gateFixtureEntry(key("0.9")), gateFixtureEntry(key("0.90"))
	a.Date, b.Date = "2026-10-20", "2026-10-23"
	for _, instance := range []string{"0.9", "0.90", "0.900"} {
		ref := matchReference(gateProfile(a, b), key(instance), now)
		if ref.entry != nil || ref.date != "" || !strings.Contains(ref.why, "2 entries match") || !strings.Contains(ref.why, "2026-10-20") || !strings.Contains(ref.why, "2026-10-23") {
			t.Errorf("an Instance at %s: reference %+v, want no entry and a reason naming both dates", instance, ref)
		}
	}
	// Control: with 0.85 as the second entry's value only the first matches.
	c := gateFixtureEntry(key("0.85"))
	if ref := matchReference(gateProfile(a, c), key("0.90"), now); ref.entry == nil || ref.entry.Date != "2026-10-20" {
		t.Errorf("one entry equal and one different: reference %+v, want the equal entry", ref)
	}
}

// A date that does not parse matches nothing, and the message names it; the
// same key with a readable date still matches alone.
func TestMatchReferenceUnreadableDateMatchesNothing(t *testing.T) {
	now := time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)
	bad := gateFixtureEntry(gateKey())
	bad.Date = "2026-13-45" // the CRD's pattern admits it; no calendar does
	ref := matchReference(gateProfile(bad), gateKey(), now)
	if ref.entry != nil || !strings.Contains(ref.why, `"2026-13-45"`) || !strings.Contains(ref.why, "does not parse") {
		t.Fatalf("reference %+v, want no entry and a reason naming the date 2026-13-45", ref)
	}
	good := gateFixtureEntry(gateKey())
	if ref := matchReference(gateProfile(bad, good), gateKey(), now); ref.entry == nil || ref.entry.Date != "2026-10-23" {
		t.Errorf("an unreadable-dated duplicate made the readable entry no reference: %+v", ref)
	}
	other := gateFixtureEntry(gateKey())
	other.Date, other.DeploymentID = "2026-13-45", "ba9876543210"
	if ref := matchReference(gateProfile(other), gateKey(), now); strings.Contains(ref.why, "2026-13-45") {
		t.Errorf("an unreadable date on an entry of another key is named: %q", ref.why)
	}
}

// The age (HBEP-4, Ageing and refresh): an entry gates until 00:00:00 UTC of
// the 90th day after its date. 2026-10-23 plus 90 days is 2027-01-21: 8 days to
// Oct 31, 30 to Nov 30, 31 to Dec 31, 21 to Jan 21. The clock is read once and
// handed in.
func TestMatchReferenceAgeBoundary(t *testing.T) {
	e := gateFixtureEntry(gateKey())
	for _, tc := range []struct {
		now   time.Time
		aged  bool
		label string
	}{
		{time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC), false, "on its date"},
		{time.Date(2027, 1, 20, 23, 59, 0, 0, time.UTC), false, "23:59 the day before the 90th day"},
		{time.Date(2027, 1, 20, 23, 59, 59, 999999999, time.UTC), false, "a nanosecond before the 90th day"},
		{time.Date(2027, 1, 21, 0, 0, 0, 0, time.UTC), true, "00:00 UTC on the 90th day"},
		{time.Date(2027, 1, 21, 0, 0, 1, 0, time.UTC), true, "a second after it"},
		{time.Date(2027, 6, 1, 12, 0, 0, 0, time.UTC), true, "months after"},
	} {
		ref := matchReference(gateProfile(e), gateKey(), tc.now)
		if tc.aged {
			if ref.entry != nil || ref.date != "2026-10-23" || !strings.Contains(ref.why, "90 days or more old") || !strings.Contains(ref.why, "2026-10-23") {
				t.Errorf("%s: reference %+v, want records only, naming the date 2026-10-23", tc.label, ref)
			}
		} else if ref.entry == nil || ref.why != "" {
			t.Errorf("%s: reference %+v, want the entry to gate", tc.label, ref)
		}
	}
}

// measuresOne is one worker's measurement, 300 ms to first token unless given.
func measuresOne(tpot time.Duration, ttft ...time.Duration) []workerMeasure {
	t := 300 * time.Millisecond
	if len(ttft) > 0 {
		t = ttft[0]
	}
	return []workerMeasure{{worker: "worker-0", tpot: tpot, ttft: t}}
}

// The limits (HBEP-7, The limits), hand-worked: with 20 ms and 400 ms at the
// default tolerance 100, 40 ms, and max(800, 500) = 800 ms; with 400 ms
// replaced by 50 ms, max(100, 150) = 150 ms; with tolerance 50, 30 ms, and
// max(600, 500) = 600 ms. Each limit is tried at it and one nanosecond over it,
// and a measurement under it.
func TestGateLimitsAtTheirEdges(t *testing.T) {
	ms := time.Millisecond
	entry := func(ttft string, tol *int32) *v1alpha1.PlatformProfile {
		e := gateFixtureEntry(gateKey())
		e.TimeToFirstToken, e.TolerancePercent = v1alpha1.Duration(ttft), tol
		return gateProfile(e)
	}
	now := time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		profile *v1alpha1.PlatformProfile
		m       []workerMeasure
		passes  bool
		limits  string // both limits as the message prints them
	}{
		{"tpot 39 ms", entry("400ms", nil), measuresOne(39 * ms), true, "(limit 40ms)"},
		{"tpot exactly 40 ms", entry("400ms", nil), measuresOne(40 * ms), true, "(limit 40ms)"},
		{"tpot 40 ms and 1 ns", entry("400ms", nil), measuresOne(40*ms + 1), false, "(limit 40ms)"},
		{"tpot 41 ms", entry("400ms", nil), measuresOne(41 * ms), false, "(limit 40ms)"},
		{"ttft 790 ms", entry("400ms", nil), measuresOne(20*ms, 790*ms), true, "(limit 800ms)"},
		{"ttft exactly 800 ms", entry("400ms", nil), measuresOne(20*ms, 800*ms), true, "(limit 800ms)"},
		{"ttft 800 ms and 1 ns", entry("400ms", nil), measuresOne(20*ms, 800*ms+1), false, "(limit 800ms)"},
		{"ttft 810 ms", entry("400ms", nil), measuresOne(20*ms, 810*ms), false, "(limit 800ms)"},
		{"entry ttft 50 ms: 140 ms", entry("50ms", nil), measuresOne(20*ms, 140*ms), true, "(limit 150ms)"},
		{"entry ttft 50 ms: exactly 150 ms", entry("50ms", nil), measuresOne(20*ms, 150*ms), true, "(limit 150ms)"},
		{"entry ttft 50 ms: 150 ms and 1 ns", entry("50ms", nil), measuresOne(20*ms, 150*ms+1), false, "(limit 150ms)"},
		{"entry ttft 50 ms: 160 ms", entry("50ms", nil), measuresOne(20*ms, 160*ms), false, "(limit 150ms)"},
		{"tolerance 50: tpot 30 ms", entry("400ms", ptrTo(int32(50))), measuresOne(30 * ms), true, "(limit 30ms)"},
		{"tolerance 50: tpot 31 ms", entry("400ms", ptrTo(int32(50))), measuresOne(31 * ms), false, "(limit 30ms)"},
		{"tolerance 50: ttft exactly 600 ms", entry("400ms", ptrTo(int32(50))), measuresOne(20*ms, 600*ms), true, "(limit 600ms)"},
		{"tolerance 50: ttft 600 ms and 1 ns", entry("400ms", ptrTo(int32(50))), measuresOne(20*ms, 600*ms+1), false, "(limit 600ms)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := judgeGate(tc.profile, gateKey(), tc.m, now, nil)
			shown := v.text + v.detail // a pass's message, or a miss's measurements and limits
			if !strings.Contains(shown, tc.limits) || v.date != "2026-10-23" {
				t.Errorf("verdict %+v does not name the limit %s and the entry's date", v, tc.limits)
			}
			if tc.passes && (v.status != v1alpha1.GatePassed || !strings.HasPrefix(v.text, "passed")) {
				t.Errorf("verdict %+v, want Passed against 2026-10-23", v)
			}
			if !tc.passes && (v.status != "" || len(v.missed) != 1 || v.missed[0] != "worker-0" || v.text != "") {
				t.Errorf("verdict %+v, want a miss by worker-0 with no final status", v)
			}
		})
	}
}

// With several workers each is judged on its own measurement: 20 ms and 45 ms
// miss by worker-1 alone, though their mean, 32.5 ms, is within 40 ms; two
// workers at 20 ms and 39 ms pass, the message giving each one's.
func TestGateJudgesEachWorkerNotTheirMean(t *testing.T) {
	now := time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)
	ms := time.Millisecond
	two := func(a, b time.Duration) []workerMeasure {
		return []workerMeasure{{"worker-0", a, 300 * ms}, {"worker-1", b, 300 * ms}}
	}
	v := judgeGate(gateProfile(gateFixtureEntry(gateKey())), gateKey(), two(20*ms, 45*ms), now, nil)
	if v.status != "" || len(v.missed) != 1 || v.missed[0] != "worker-1" ||
		!strings.Contains(v.detail, "worker-0: time per output token 20ms (limit 40ms)") || !strings.Contains(v.detail, "worker-1: time per output token 45ms (limit 40ms)") {
		t.Errorf("verdict %+v, want worker-1 alone to miss, each worker's measurement shown", v)
	}
	v = judgeGate(gateProfile(gateFixtureEntry(gateKey())), gateKey(), two(20*ms, 39*ms), now, nil)
	if v.status != v1alpha1.GatePassed || !strings.Contains(v.text, "worker-1: time per output token 39ms (limit 40ms)") {
		t.Errorf("verdict %+v, want Passed with each worker's measurement", v)
	}
}

// A gate that cannot judge records only and says why: no measurement, or an
// entry whose durations do not parse.
func TestGateRecordsOnlyWhatItCannotJudge(t *testing.T) {
	now := time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)
	p := gateProfile(gateFixtureEntry(gateKey()))
	if v := judgeGate(p, gateKey(), nil, now, nil); v.status != v1alpha1.GateRecordOnly || v.date != "2026-10-23" || !strings.Contains(v.text, "no readable measurement") {
		t.Errorf("no measurement: %+v", v)
	}
	bad := gateFixtureEntry(gateKey())
	bad.TimePerOutputToken = "fast"
	if v := judgeGate(gateProfile(bad), gateKey(), measuresOne(20*time.Millisecond), now, nil); v.status != v1alpha1.GateRecordOnly || !strings.Contains(v.text, `"fast" does not parse`) {
		t.Errorf("an unreadable entry: %+v", v)
	}
	if v := judgeGate(nil, gateKey(), measuresOne(20*time.Millisecond), now, nil); v.status != v1alpha1.GateRecordOnly || !strings.Contains(v.text, "no entry matches") {
		t.Errorf("no profile: %+v", v)
	}
}

// The message keeps the checks' text, the behaviour check's context included,
// and the gate's part is replaced, never added to.
func TestQualifiedMessageKeepsTheChecksText(t *testing.T) {
	q := judgeQualifyResult(16384)
	q.Checks[3].ContextTokens = 8192
	ok, checks := judgeQualify(q, judgeShapes(), "example-model")
	if !ok || checks != "every check passed; the behaviour check ran at 8192 context tokens" {
		t.Fatalf("judgeQualify: %v %q, want the checks' text alone, with no sentence about a throughput reference", ok, checks)
	}
	in := &v1alpha1.Instance{}
	pending := qualifiedMessage(in, checks)
	if pending != checks+"; throughput gate: judged after warm-up" {
		t.Errorf("before the gate judges: %q", pending)
	}
	once := withGate(pending, "passed the entry dated 2026-10-23: x")
	twice := withGate(once, "would miss the entry dated 2026-10-23: y")
	if once != checks+"; throughput gate: passed the entry dated 2026-10-23: x" || twice != checks+"; throughput gate: would miss the entry dated 2026-10-23: y" {
		t.Errorf("the gate's part is not replaced in place: %q then %q", once, twice)
	}
	in.Status.Gate = &v1alpha1.InstanceGate{Verdict: v1alpha1.GatePassed}
	if m := qualifiedMessage(in, checks); !strings.HasPrefix(m, checks+"; throughput gate: judged on the first qualification (Passed)") {
		t.Errorf("after a verdict: %q", m)
	}
}
