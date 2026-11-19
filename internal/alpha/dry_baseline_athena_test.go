package alpha

// The Athena twin of the baseline row (baseline.go, AthenaBaselineRow) over the fake cluster: the same sequencer, judge and dry world as Row 1's
// (dry_baseline_test.go), whose healthy run is built from the row's own cap, KV and deployment ID. The cases here are a short list of the faults whose words
// carry the row's own numbers (the cap of 3, N = 5, the KV need of 3 x 32,768, the Athena ID) and of the rest that each end FAIL or INVALID in one way; the
// whole of Row 1's list stays Row 1's. Each is held through Run (TestDryRunsOfEveryRowAndEveryFault for the FAILs, TestTheAthenaBaselinesInvalidRunsThroughRun
// for the INVALIDs) and through the script (hack/livechecks, alpha_dry_test.go).

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// athenaBaselineCases are the Athena twin's dry faults that end FAIL, each with the failed check's words.
var athenaBaselineCases = []dryCase{
	{"", ""},
	{"llmlab-in-the-control", "request(s), and the control gives it none"},
	{"llmlab-one-too-many-first-turns", "LLMLab answered the first turn of 3 distinct session(s), want exactly k = 2"},
	{"llmlab-over-its-guard", "LLMLab was asked for more than 144 requests in one phase, so the generator was stopped"},
	{"llmlab-after-the-detach", "request(s) after it was detached"},
	{"detach-session-missing", "only 4 of the 5 sessions' first turns ended with 200: the others were never sent or never answered"},
	{"waiting-not-returning", "the front's waiting count was 2 when the phase ended"},
	{"engine-over-the-cap", "the engine ran more than the cap of 3 requests"},
	{"engine-waiting", "the engine had requests waiting in 5 consecutive distinct reads"},
	{"engine-preempts", "the engine preempted"},
	{"over-the-cap-in-flight", "the Instance had more than its cap of 3 requests in flight"},
	{"session-moves", "a session did not stay on its Instance"},
	{"instance-fails", "a request failed (status 500, emitter \"instance\")"},
	{"no-gate", "the gate was not sent"},
	{"gate-503", "the gate's request at model@<provider> was not answered by the served Instance (status 503"},
	{"p0-answered-by-an-instance", "P0 was not answered by LLMLab"},
	{"p0-front-fails", "P0's request failed and the front answered it"},
	{"metric-unread", "the front's per-provider request metric could not be read, so the 144-request guard could not run"},
	{"front-log-unread", "the front's log could not be read, so a failed dial of the Instance could not be ruled out"},
	{"kv-unread", "the engine's logged KV was not read"},
	{"no-routing-block", "does not hold routing.instanceCap at the row's cap of 3"},
	{"two-instances", "does not ask for exactly 1 Instance"},
	{"wrong-deployment-id", "the served Instance's deployment ID is not the one the shipped example derives (965fa2409193)"},
	{"untagged", "a job tag was not written down before its submission"},
	{"wrong-partition", "a job ran on another partition than the row's"},
	{"default-lease", "the deployment's leaseLimit is not the row's shortest lease"},
	{"short-lease", "not longer than the lead an Instance needs (PartitionTooShort)"},
	{"extra-allocation", "exactly one Allocation"},
}

// athenaBaselineInvalidCases are the Athena twin's dry faults that end INVALID, each with the reason's words: the reading measures nothing, and no check failed.
var athenaBaselineInvalidCases = []struct{ fault, why string }{
	{"cap-never-binds", "the cap never bound: LLMLab was asked for no request, so no spill was shown"},
	{"detach-503", "P3: a request was answered 503 (emitter \"front\")"},
	{"waiting-never-two", "the front's own waiting count reached at most 1, want 2"},
	// 98,304 is 3 x 32,768: the logged KV below it ends the run before any phase (the dry fault logs half of it).
	{"kv-below-the-cap", "the engine's logged KV is 49152 tokens, below the cap's need of 3 x 32768 = 98304"},
	{"llmlab-fails", "a request LLMLab answered failed (status 500)"},
	{"p0-llmlab-fails", "P0's request failed and LLMLab answered it (status 500)"},
	{"p1-serialized", "P1: the front's records show at most 1 requests in flight at once on the Instance, want the cap's 3"},
	{"long-serialized", "P1-long: the front's records show at most 1 requests in flight at once on the Instance, want the cap's 3"},
	{"phases-do-not-fit", "did not fit before the Instance's drain start"},
	{"ratio-impossible", "no word count reaches 24000 to 28672 tokens"},
	{"dial-failure-p2", "P2: 1 request(s) of the phase were resent after a failed dial of the Instance"},
	{"llmlab-spill-below-the-cap", "LLMLab request(s) arrived while the front's records showed fewer than the cap's 3 requests in flight on the Instance"},
	{"long-no-read-in-window", "no distinct engine read was taken inside the stretch with the cap's 3 requests in flight"},
}

func init() { dryCases[BetaAthenaBaselineName] = athenaBaselineCases }

// runAthenaBaselineDry is one dry run of the Athena twin through Run: its record directory, its failed checks, its error and the steps.
func runAthenaBaselineDry(t *testing.T, fault string) (out string, failures []string, err error, st *drySteps) {
	t.Helper()
	cfg := Config{Row: AthenaBaselineRow(), Dry: true, DryFault: fault, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel,
		Phases: DefaultPhases(CapAthena)}
	rec, rerr := NewRecorder(cfg.Out)
	if rerr != nil {
		t.Fatal(rerr)
	}
	st = newDrySteps(t, cfg, familyOf(cfg.Row.Name))
	failures, err = Run(context.Background(), cfg, st, rec, t.Logf)
	return cfg.Out, failures, err, st
}

// Each fault that makes the Athena reading measure nothing ends INVALID through Run, as Row 1's do: ErrBaselineInvalid with the reason's words, no failed
// check, never NOT RUN, the record's verdict "invalid" and the text to post headed INVALID with the Athena twin's name.
func TestTheAthenaBaselinesInvalidRunsThroughRun(t *testing.T) {
	for _, c := range athenaBaselineInvalidCases {
		t.Run(c.fault, func(t *testing.T) {
			out, failures, err, _ := runAthenaBaselineDry(t, c.fault)
			if len(failures) != 0 {
				t.Fatalf("an INVALID run failed checks %q", failures)
			}
			var ie *BaselineInvalidError
			row := AthenaBaselineRow()
			if !errors.As(err, &ie) || !errors.Is(err, ErrBaselineInvalid) || !BaselineInvalidRun(row, err) || NotRun(row, err) || Invalid(row, err) {
				t.Fatalf("err = %v, want an INVALID baseline run alone", err)
			}
			if !strings.Contains(err.Error(), c.why) {
				t.Errorf("the reason %q lacks %q", err.Error(), c.why)
			}
			if v := recordVerdict(t, out); v != "invalid" {
				t.Errorf("the record's verdict is %q, want invalid", v)
			}
			post, _ := os.ReadFile(filepath.Join(out, PostsFile))
			if !strings.HasPrefix(string(post), "beta-athena-now-llmlab-baseline: INVALID (posts to #764)\n") || !strings.Contains(string(post), "\n- the reading is invalid: ") || !strings.Contains(string(post), c.why) {
				t.Errorf("the text to post:\n%s", post)
			}
			if note := BaselineInvalidNote(row); !strings.Contains(note, baselineInvalidWords) || strings.Contains(note, c.why) {
				t.Errorf("the marker's note %q holds the fixed words alone", note)
			}
		})
	}
}

// A logged KV under the Athena cap's need ends the run before any phase, as on Sano: the need is 3 x 32,768 = 98,304 tokens, and the derived KV of the 40 GB
// card (about 121,150) is above it, so the healthy run reads it and goes on. The dry fault's figure, half the need, is below it.
func TestTheAthenaBaselinesKVCheckIsItsOwnCapsNeed(t *testing.T) {
	if need := int64(CapAthena) * MaxModelLen; need != 98_304 || KVAthenaDerived() < need {
		t.Fatalf("the need is %d and the derived KV %d: the cap does not fit the card's derived cache", need, KVAthenaDerived())
	}
	out, failures, err, _ := runAthenaBaselineDry(t, "")
	if err != nil || len(failures) != 0 {
		t.Fatalf("the healthy run: failures %v, err %v", failures, err)
	}
	if o := obsOfRecord(t, out); o.Row != BetaAthenaBaselineName || o.Cap != CapAthena || !o.KVRead || o.LoggedKV != KVAthenaDerived() || len(o.Phases) != 4 {
		t.Errorf("the observation: row %q, cap %d, KV %d read %t, %d phases; want the Athena twin at cap 3 and KV %d, 4 phases", o.Row, o.Cap, o.LoggedKV, o.KVRead, len(o.Phases), KVAthenaDerived())
	}
	out, _, err, _ = runAthenaBaselineDry(t, "kv-below-the-cap")
	if !errors.Is(err, ErrBaselineInvalid) {
		t.Fatalf("a KV below the cap's need: %v", err)
	}
	if o := obsOfRecord(t, out); len(o.Phases) != 0 || !o.KVRead || o.LoggedKV != 49_152 {
		t.Errorf("the run made %d phases with a logged KV of %d: none was to be run", len(o.Phases), o.LoggedKV)
	}
}

// The Athena twin's phases are Row 1's, at the Athena cap: P1 and P1-long with N = 3, P2 and P3 with N = 5 (cap + k, k = 2); and the text to post names the
// row's own facts, Athena, the derived KV and its basis, and no word of Sano's.
func TestTheAthenaBaselineRunsTheCapsPhasesAndPostsItsOwnFacts(t *testing.T) {
	out, failures, err, _ := runAthenaBaselineDry(t, "")
	if err != nil || len(failures) != 0 {
		t.Fatalf("the healthy run: failures %v, err %v", failures, err)
	}
	var got []string
	for _, po := range obsOfRecord(t, out).Phases {
		got = append(got, po.Phase.Name+"="+strconv.Itoa(po.Phase.Sessions))
	}
	if want := "P1=3,P1-long=3,P2=5,P3=5"; strings.Join(got, ",") != want {
		t.Errorf("the phases' session counts %v, want %s (cap 3, k = 2)", got, want)
	}
	post, _ := os.ReadFile(filepath.Join(out, PostsFile))
	for _, want := range []string{
		"beta-athena-now-llmlab-baseline: PASS (posts to #764)\n",
		"one Instance on Athena and LLMLab attached by a managed Provider",
		fmt.Sprintf("- the cap is 3, a constant fixed before the run: floor(0.95 x %d / 32768) from the engine's KV derived for the 40 GB card (nothing has measured this model on it); the engine's logged KV after ready was %d tokens (a check, never an input)\n", KVAthenaDerived(), KVAthenaDerived()),
		fmt.Sprintf("against KV %d is %d sessions, capped at max-num-seqs 256, where the baseline's cap is 3\n", KVAthenaDerived(), KVAthenaDerived()/1152),
		"  - the served Instance (Athena): ",
	} {
		if !strings.Contains(string(post), want) {
			t.Errorf("the text to post lacks %q:\n%s", want, post)
		}
	}
	if strings.Contains(string(post), "Sano") || strings.Contains(string(post), "389120") {
		t.Errorf("the Athena twin's text to post names Sano's facts:\n%s", post)
	}
}
