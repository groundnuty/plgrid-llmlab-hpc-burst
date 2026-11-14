package alpha

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/routerbytes/cases"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// The router row's logic (betarouter.go), each check with its words written out and its boundary.

const pinnedDigestFixture = "1d64506e25c1844f146162ddb2ece7010fa4e784faddb40a079021491f1f03cc"

// goodRouterObs is a healthy run's observation: every check passes, so that each case changes one thing.
func goodRouterObs(row Row) RouterObs {
	ready := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	return RouterObs{
		Tags: []string{"tag-1"}, Deployment: row.Plan(), ReadyAt: ready, DrainStart: ready.Add(30 * time.Minute),
		LaunchRouter: "envoy", HealthyAtReady: 1, HealthyAfterRestart: 1,
		Envoy:          EnvoyObs{Procs: 1, PinnedDigest: pinnedDigestFixture, RunningDigest: pinnedDigestFixture, Serving: true},
		Glibc:          GlibcReads{Probe: GlibcRead{OK: true, Version: GlibcVersion{2, 34}}},
		Rate:           30,
		Bytes:          dryRouterBytes(),
		EngineVersion:  "0.32.0",
		WorkerRestarts: 1, WorkerRestart: RestartTimes{}, Ladder: LadderObs{WorkerRestarts: 1}, Responses: 460, Attributed: 460,
	}
}

func TestParseGetconfGlibc(t *testing.T) {
	for _, c := range []struct {
		in   string
		want GlibcVersion
		ok   bool
	}{
		{"glibc 2.34\n", GlibcVersion{2, 34}, true},
		{"glibc 2.28", GlibcVersion{2, 28}, true},
		{"glibc 2.30  \n", GlibcVersion{2, 30}, true},
		{"glibc 2.3x", GlibcVersion{}, false},
		{"musl 1.2", GlibcVersion{}, false},
		{"", GlibcVersion{}, false},
		{"glibc 2.34\nglibc 2.28\n", GlibcVersion{}, false},
	} {
		got, ok := ParseGetconfGlibc([]byte(c.in))
		if got != c.want || ok != c.ok {
			t.Errorf("ParseGetconfGlibc(%q) = %v, %t; want %v, %t", c.in, got, ok, c.want, c.ok)
		}
	}
}

// The agent's refusal, as its log carries it, and the clause the row reads; the source pin holds the clause to the code that raises it.
func TestParseGlibcRefusal(t *testing.T) {
	line := `time=2026-11-14T10:00:01Z level=ERROR msg="router did not start" router=envoy err="envoy refused: Envoy needs glibc 2.30 or later, and this node's glibc is 2.28: Envoy cannot run bare here"`
	if v, ok := ParseGlibcRefusal(line); !ok || v != (GlibcVersion{2, 28}) {
		t.Errorf("ParseGlibcRefusal read %v, %t; want 2.28", v, ok)
	}
	for _, no := range []string{
		"",
		`msg="router is serving" router=envoy pid=4242 generation=1`,
		`err="envoy refused: Envoy needs glibc 2.30 or later through its loader the node does not have"`,
		"Envoy needs glibc 2.30 or later, and reading this node's glibc (getconf GNU_LIBC_VERSION) failed",
	} {
		if v, ok := ParseGlibcRefusal(no); ok {
			t.Errorf("ParseGlibcRefusal(%q) read %v; want nothing", no, v)
		}
	}
	src, err := os.ReadFile("../agent/envoyglibc.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "Envoy needs glibc %d.%d or later, and this node's glibc is %d.%d") {
		t.Error("internal/agent/envoyglibc.go no longer words its refusal as the row reads it")
	}
}

func TestGlibcVersionOrder(t *testing.T) {
	for _, c := range []struct {
		v, w GlibcVersion
		less bool
	}{
		{GlibcVersion{2, 29}, MinEnvoyGlibc, true},
		{GlibcVersion{2, 30}, MinEnvoyGlibc, false},
		{GlibcVersion{2, 31}, MinEnvoyGlibc, false},
		{GlibcVersion{1, 99}, MinEnvoyGlibc, true},
		{GlibcVersion{3, 0}, MinEnvoyGlibc, false},
		{GlibcVersion{2, 28}, GlibcVersion{2, 34}, true},
	} {
		if got := c.v.Less(c.w); got != c.less {
			t.Errorf("%v.Less(%v) = %t, want %t", c.v, c.w, got, c.less)
		}
	}
	if MinEnvoyGlibc.String() != "2.30" {
		t.Errorf("the minimum is %s, HBEP-5:133 says 2.30", MinEnvoyGlibc)
	}
}

func TestGlibcReadsTooOldIsNotRunFromEitherSource(t *testing.T) {
	read := func(maj, minor int) GlibcRead { return GlibcRead{OK: true, Version: GlibcVersion{maj, minor}} }
	for _, c := range []struct {
		name      string
		r         GlibcReads
		notRun    bool
		fromAgent bool
		version   string
	}{
		{"nothing read", GlibcReads{}, false, false, ""},
		{"the probe at the minimum", GlibcReads{Probe: read(2, 30)}, false, false, ""},
		{"the probe above", GlibcReads{Probe: read(2, 34)}, false, false, ""},
		{"the probe under", GlibcReads{Probe: read(2, 28)}, true, false, "2.28"},
		{"the probe at 2.29, one under", GlibcReads{Probe: read(2, 29)}, true, false, "2.29"},
		{"the agent's refusal alone", GlibcReads{Agent: read(2, 28)}, true, true, "2.28"},
		{"the probe above, the agent's refusal under", GlibcReads{Probe: read(2, 34), Agent: read(2, 28)}, true, true, "2.28"},
		{"both under, the agent's is named", GlibcReads{Probe: read(2, 28), Agent: read(2, 27)}, true, true, "2.27"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := c.r.TooOld()
			if (err != nil) != c.notRun {
				t.Fatalf("TooOld = %v, want NOT RUN %t", err, c.notRun)
			}
			if !c.notRun {
				return
			}
			var g *GlibcTooOldError
			if !errors.Is(err, ErrGlibcTooOld) || !errors.As(err, &g) || g.Version.String() != c.version || g.FromAgent != c.fromAgent {
				t.Errorf("TooOld = %v (%+v), want glibc %s from the agent %t", err, g, c.version, c.fromAgent)
			}
			if !NotRun(BetaRouterRow(), err) {
				t.Error("the error does not end the run NOT RUN")
			}
		})
	}
}

func TestGlibcOfTakesTheOlderReading(t *testing.T) {
	v := func(maj, minor int) GlibcRead { return GlibcRead{OK: true, Version: GlibcVersion{maj, minor}} }
	for _, c := range []struct {
		r    GlibcReads
		want string
		ok   bool
	}{
		{GlibcReads{}, "", false},
		{GlibcReads{Probe: v(2, 34)}, "2.34", true},
		{GlibcReads{Agent: v(2, 28)}, "2.28", true},
		{GlibcReads{Probe: v(2, 34), Agent: v(2, 28)}, "2.28", true},
		{GlibcReads{Probe: v(2, 28), Agent: v(2, 34)}, "2.28", true},
	} {
		got, ok := GlibcOf(c.r)
		if ok != c.ok || (ok && got.String() != c.want) {
			t.Errorf("GlibcOf(%+v) = %v, %t; want %q, %t", c.r, got, ok, c.want, c.ok)
		}
	}
}

func TestRouterRowIsRowAsHoldOfThePostedSlot(t *testing.T) {
	r, a := BetaRouterRow(), NowServeRow()
	if r.Name != "beta-athena-now-router" || r.PostsTo != "#522" {
		t.Errorf("the row is %q posting to %q, want beta-athena-now-router posting to #522", r.Name, r.PostsTo)
	}
	// Everything but the name and the tracker is row A's hold of plgrid-now's one-job slot.
	b := r
	b.Name, b.PostsTo = a.Name, a.PostsTo
	if b != a {
		t.Errorf("the row differs from row A beyond its name and tracker:\n%+v\n%+v", r, a)
	}
	if r.OwnerGo != OwnerGoAthenaNow || r.MostJobs != 1 || r.GPUs != 1 || r.Partition != "plgrid-now" {
		t.Errorf("the row does not hold plgrid-now's slot with one job of one GPU under the owner's go: %+v", r)
	}
}

func TestEnvoyChecksNameEachWayItFails(t *testing.T) {
	good := EnvoyObs{Procs: 1, PinnedDigest: pinnedDigestFixture, RunningDigest: pinnedDigestFixture, Serving: true}
	if f := envoyChecks(good); len(f) != 0 {
		t.Fatalf("a good Envoy fails: %v", f)
	}
	other := strings.Repeat("ab", 32)
	for _, c := range []struct {
		name string
		mut  func(e *EnvoyObs)
		want string
	}{
		{"the router did not start", func(e *EnvoyObs) { e.Refused, e.Serving = true, false }, "the agent's log says the router did not start"},
		{"the agent never says it serves", func(e *EnvoyObs) { e.Serving = false }, "the agent's log does not say the router is serving"},
		{"no Envoy process", func(e *EnvoyObs) { e.Procs = 0 }, "the node does not show exactly one Envoy process in the job"},
		{"two Envoy processes", func(e *EnvoyObs) { e.Procs = 2 }, "the node does not show exactly one Envoy process in the job"},
		{"not bare", func(e *EnvoyObs) { e.Contained = true }, "Envoy does not run bare: an Apptainer process is among its ancestors"},
		{"no pin", func(e *EnvoyObs) { e.PinnedDigest = "" }, "the digest the Provider stages Envoy by was not read"},
		{"the executable unread", func(e *EnvoyObs) { e.RunningDigest = "" }, "the running Envoy's executable was not read on the node"},
		{"another build runs", func(e *EnvoyObs) { e.RunningDigest = other }, "the running Envoy is not the build the Provider staged by its pinned digest"},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := good
			c.mut(&e)
			if f := envoyChecks(e); !slices.Contains(f, c.want) {
				t.Errorf("envoyChecks = %q, want it to hold %q", f, c.want)
			}
		})
	}
}

func TestBytesChecksRequireACompleteReadingOfEveryVariant(t *testing.T) {
	good := dryRouterBytes()
	if f := bytesChecks(good); len(f) != 0 {
		t.Fatalf("a good reading fails: %v", f)
	}
	// The ruling (c) on #521: the healthy fixture's dryRefusals refusals are each in one of S13's refusal groups, a 4xx, so none fails.
	if len(good.NonOK) != dryRefusals {
		t.Fatalf("the healthy fixture has %d valid variants answered other than 200, want %d", len(good.NonOK), dryRefusals)
	}
	for _, v := range good.NonOK {
		if !v.allowed() {
			t.Errorf("the healthy fixture's refusal %s is not one the ruling allows", v)
		}
	}
	outside := func(n int) func(b *BytesObs) {
		return func(b *BytesObs) {
			for i := range n {
				b.NonOK = append(b.NonOK, NonOKVariant{Key: fmt.Sprintf("chat/c%02d#plain", i), Status: 400})
			}
		}
	}
	for _, c := range []struct {
		name string
		mut  func(b *BytesObs)
		want string
	}{
		{"a variant not sent", func(b *BytesObs) { b.Variants = 439 }, "the response-level reading did not send every variant of S13's cases"},
		{"no expected count", func(b *BytesObs) { b.Expected = 0 }, "the response-level reading did not send every variant of S13's cases"},
		{"a response that did not finish", func(b *BytesObs) { b.Complete, b.Status200 = 439, 399 }, "a response of the response-level reading did not finish"},
		{"a failure the router made", func(b *BytesObs) { b.RouterMade = 1 }, "a response of the response-level reading was a failure the router or the path made"},
		{"no streamed 200", func(b *BytesObs) { b.StreamOK = 0 }, "no streamed variant answered 200"},
		{"no non-streamed 200", func(b *BytesObs) { b.NonStreamOK = 0 }, "no non-streamed variant answered 200"},
		{"no chat 200", func(b *BytesObs) { b.ChatOK = 0 }, "no chat variant answered 200"},
		{"no completions 200", func(b *BytesObs) { b.CompletionsOK = 0 }, "no completions variant answered 200"},
		{"counts that do not add up", func(b *BytesObs) { b.Status4xx = 41 }, "the response-level reading's status counts do not add up"},
		{"an invalid variant answered 200", func(b *BytesObs) { b.InvalidRefused = 1 }, "a variant S13 says vLLM refuses was not refused with a 4xx"},
		{"no invalid variant sent", func(b *BytesObs) { b.InvalidSent, b.InvalidRefused = 0, 0 }, "a variant S13 says vLLM refuses was not refused with a 4xx"},
		{"a valid variant outside the groups refused", func(b *BytesObs) {
			b.NonOK = append(b.NonOK, NonOKVariant{"chat/temperature#plain", 400, ""})
		}, "a valid variant of S13's cases was answered other than 200, which is allowed only as a 4xx in one of S13's groups that a server may refuse: chat/temperature#plain 400 [outside S13's groups]"},
		{"a valid variant in a group answered 500", func(b *BytesObs) {
			b.NonOK = append(b.NonOK, NonOKVariant{"completions/suffix#plain", 500, "suffix"})
		}, "a valid variant of S13's cases was answered with a 5xx: completions/suffix#plain 500 [suffix]"},
		{"a valid variant in a group answered with a 3xx", func(b *BytesObs) {
			b.NonOK = append(b.NonOK, NonOKVariant{"chat/chat_template#plain", 302, "a request chat_template"})
		}, "a valid variant of S13's cases was answered other than 200, which is allowed only as a 4xx in one of S13's groups that a server may refuse: chat/chat_template#plain 302 [a request chat_template]"},
		{"eleven outside: ten named and the rest counted", outside(11),
			"a valid variant of S13's cases was answered other than 200, which is allowed only as a 4xx in one of S13's groups that a server may refuse: chat/c00#plain 400 [outside S13's groups], chat/c01#plain 400 [outside S13's groups], chat/c02#plain 400 [outside S13's groups], chat/c03#plain 400 [outside S13's groups], chat/c04#plain 400 [outside S13's groups], chat/c05#plain 400 [outside S13's groups], chat/c06#plain 400 [outside S13's groups], chat/c07#plain 400 [outside S13's groups], chat/c08#plain 400 [outside S13's groups], chat/c09#plain 400 [outside S13's groups], and 1 more, named in the post"},
		{"a status that is none of 200, 4xx and 5xx, uncounted", func(b *BytesObs) { b.Complete++ }, "the response-level reading's status counts do not add up"},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := good
			c.mut(&b)
			if f := bytesChecks(b); !slices.Contains(f, c.want) {
				t.Errorf("bytesChecks = %q, want it to hold %q", f, c.want)
			}
		})
	}
}

func TestHealthyChecksAreOneAtReadyAndAfterTheRestart(t *testing.T) {
	o := goodRouterObs(BetaRouterRow())
	if f := healthyChecks(o); len(f) != 0 {
		t.Fatalf("a healthy Instance fails: %v", f)
	}
	for _, n := range []int{-1, 0, 2} {
		o := o
		o.HealthyAtReady = n
		if f := healthyChecks(o); !slices.Contains(f, "status.healthyWorkers was not 1 when the Instance was first ready") {
			t.Errorf("healthyWorkers %d at ready: %q", n, f)
		}
		o = goodRouterObs(BetaRouterRow())
		o.HealthyAfterRestart = n
		if f := healthyChecks(o); !slices.Contains(f, "status.healthyWorkers was not 1 once the Instance served again after the worker restart") {
			t.Errorf("healthyWorkers %d after the restart: %q", n, f)
		}
	}
}

// The text to post says what the Bytes are, in the words the ruling asks for, and names the glibc by its version alone.
func TestBetaRouterPostNamesTheBytesAsAResponseLevelReading(t *testing.T) {
	row := BetaRouterRow()
	o := goodRouterObs(row)
	post := betaRouterPost(row, o, nil)
	for _, want := range []string{
		"beta-athena-now-router: PASS (posts to #522)",
		"RESPONSE-LEVEL reading on vLLM, not bytes",
		"exact bytes are CI's",
		"440 of 440 variants sent, 440 answered completely (400 with 200, 40 with 4xx, 0 with 5xx, 0 with another status), 0 failures made by the router or the path, 0 still awaited at the harness's own bound of 10m0s",
		"- the engine's vLLM version: 0.32.0; S13's cases were enumerated against vLLM v0.32.0\n",
		"- valid variants answered other than 200, every one (38; a 4xx is allowed only in one of S13's groups that a server may still refuse, the rest fail): chat/chat_template#plain 400 [a request chat_template]; chat/chat_template#stream 400 [a request chat_template]; chat/media_io_kwargs#plain 400 [mm_processor_kwargs and media_io_kwargs]; ",
		"the compute node's glibc: 2.34 (Envoy needs 2.30)",
		"status.healthyWorkers 1 when ready and 1 after the worker restart",
	} {
		if !strings.Contains(post, want) {
			t.Errorf("the text to post lacks %q:\n%s", want, post)
		}
	}
	if fail := betaRouterPost(row, o, []string{"x"}); !strings.Contains(fail, "beta-athena-now-router: FAIL (posts to #522)") {
		t.Errorf("a failed run's text lacks its verdict line:\n%s", fail)
	}
	// Every valid variant answered other than 200 is posted, by key, status and group: all 38 here.
	if n := strings.Count(post, " 400 ["); n != dryRefusals {
		t.Errorf("the text to post names %d variants answered 400, want %d:\n%s", n, dryRefusals, post)
	}
	// None answered other than 200, and an engine version that was not read, say so.
	o.Bytes.NonOK, o.EngineVersion = nil, ""
	plain := betaRouterPost(row, o, nil)
	for _, want := range []string{"- valid variants answered other than 200: none\n", "- the engine's vLLM version: not read; S13's cases were enumerated against vLLM v0.32.0\n"} {
		if !strings.Contains(plain, want) {
			t.Errorf("the text to post lacks %q:\n%s", want, plain)
		}
	}
}

// A node whose glibc is under 2.30 ends the run NOT RUN, read on the node or from the agent's own log, naming the version and nothing
// else: Envoy cannot run bare there, so the router was never put to the test. At the minimum the node is fine. The run opened nothing and
// the job was cancelled by its tag (the dry steps do it as the live ones do).
func TestARouterNodeWithAnOldGlibcIsNotRun(t *testing.T) {
	row := BetaRouterRow()
	for _, c := range []struct {
		fault   string
		version string
	}{
		{"glibc-old-probe", "2.28"},
		{"glibc-old-agent", "2.28"},
		{"glibc-one-under", "2.29"},
	} {
		t.Run(c.fault, func(t *testing.T) {
			cfg := Config{Row: row, Dry: true, DryFault: c.fault, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
			rec, err := NewRecorder(cfg.Out)
			if err != nil {
				t.Fatal(err)
			}
			failures, err := Run(context.Background(), cfg, newDrySteps(t, cfg, "betarouter"), rec, t.Logf)
			if !errors.Is(err, ErrGlibcTooOld) || !NotRun(row, err) || len(failures) != 0 {
				t.Fatalf("failures %v, err %v; want the NOT RUN of a node whose glibc is too old", failures, err)
			}
			note := NotRunNote(row, cfg.PendingMax, err)
			want := "beta-athena-now-router: the compute node's glibc is older than the 2.30 Envoy needs (glibc " + c.version + "); the run sent no request through the front; this run's jobs were cancelled by tag\n"
			if note != want {
				t.Errorf("the marker's note is\n%q\nwant\n%q", note, want)
			}
			// No record and no text to post of a run that opened nothing.
			for _, f := range []string{RecordFile, PostsFile} {
				if _, err := os.Stat(filepath.Join(cfg.Out, f)); err == nil {
					t.Errorf("%s was written for a run that sent nothing", f)
				}
			}
		})
	}
	// An error that is not this one, wrapped or not, is no NOT RUN.
	if NotRun(row, errors.New("the node's glibc was not read")) {
		t.Error("an unrelated error ends the run NOT RUN")
	}
}

// AdaptBody changes S13's model name and bounds generation where the case sets none, and nothing else.
func TestAdaptBodyChangesOnlyTheModelAndTheBound(t *testing.T) {
	for _, c := range []struct{ name, in, model, want string }{
		{"the model and the bound", `{"model":"s13-model","messages":[{"role":"user","content":"hello"}]}`, "Qwen/Qwen2.5-7B-Instruct",
			`{"max_tokens":8,"model":"Qwen/Qwen2.5-7B-Instruct","messages":[{"role":"user","content":"hello"}]}`},
		{"a case that sets max_tokens keeps it", `{"model":"s13-model","prompt":"hello","max_tokens":32}`, "m", `{"model":"m","prompt":"hello","max_tokens":32}`},
		{"a null max_tokens is the case's", `{"model":"s13-model","prompt":"hello","max_tokens":null}`, "m", `{"model":"m","prompt":"hello","max_tokens":null}`},
		{"max_completion_tokens counts as a bound", `{"model":"s13-model","messages":[],"max_completion_tokens":48}`, "m", `{"model":"m","messages":[],"max_completion_tokens":48}`},
		{"a streamed variant keeps its stream member first", `{"stream":true,"model":"s13-model","prompt":"hello"}`, "m", `{"max_tokens":8,"stream":true,"model":"m","prompt":"hello"}`},
		{"no model name in the body", `{"prompt":"hello"}`, "m", `{"max_tokens":8,"prompt":"hello"}`},
		{"S13's pretty-printed case keeps its spaces", "{\n  \"messages\": [ {\"content\": \"hello\", \"role\": \"user\"} ],\n  \"temperature\": 0.3,\n  \"model\": \"s13-model\"\n}", "m",
			"{\"max_tokens\":8,\n  \"messages\": [ {\"content\": \"hello\", \"role\": \"user\"} ],\n  \"temperature\": 0.3,\n  \"model\": \"m\"\n}"},
		{"an empty model keeps S13's", `{"model":"s13-model","prompt":"x"}`, "", `{"max_tokens":8,"model":"s13-model","prompt":"x"}`},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := string(AdaptBody([]byte(c.in), c.model)); got != c.want {
				t.Errorf("AdaptBody = %s, want %s", got, c.want)
			}
		})
	}
}

func TestClassifyReplyNamesWhatTheRouterMade(t *testing.T) {
	okJSON := []byte(`{"choices":[{"text":"hi"}]}`)
	for _, c := range []struct {
		name   string
		status int
		body   []byte
		stream bool
		cut    bool
		want   ReplyClass
	}{
		{"a whole answer", 200, okJSON, false, false, ReplyClass{Complete: true}},
		{"a whole stream", 200, []byte("data: {\"x\":1}\n\ndata: [DONE]\n\n"), true, false, ReplyClass{Complete: true}},
		{"a stream that never ended", 200, []byte("data: {\"x\":1}\n\n"), true, false, ReplyClass{}},
		{"the engine's refusal", 400, []byte(`{"error":{"message":"no"}}`), false, false, ReplyClass{Complete: true}},
		{"a refusal of a streamed request is a JSON body", 400, []byte(`{"error":{"message":"no"}}`), true, false, ReplyClass{Complete: true}},
		{"the engine's own 500", 500, []byte(`{"error":{"message":"boom"}}`), false, false, ReplyClass{Complete: true}},
		{"a gateway 503", 503, []byte("no healthy upstream"), false, false, ReplyClass{RouterMade: true}},
		{"a gateway 502 with a JSON body", 502, okJSON, false, false, ReplyClass{Complete: true, RouterMade: true}},
		{"a gateway 504", 504, []byte("upstream request timeout"), false, false, ReplyClass{RouterMade: true}},
		{"a gateway 503 with a JSON body", 503, okJSON, false, false, ReplyClass{Complete: true, RouterMade: true}},
		{"a gateway 504 with a JSON body", 504, okJSON, false, false, ReplyClass{Complete: true, RouterMade: true}},
		{"a 501 with a JSON body is the engine's", 501, okJSON, false, false, ReplyClass{Complete: true}},
		{"Envoy's text under another status", 500, []byte("upstream connect error or disconnect/reset before headers"), false, false, ReplyClass{RouterMade: true}},
		{"no response", 0, nil, false, false, ReplyClass{RouterMade: true}},
		{"a body cut short", 200, okJSON, false, true, ReplyClass{RouterMade: true}},
		{"a 200 that is not JSON", 200, []byte("<html>"), false, false, ReplyClass{}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := ClassifyReply(c.status, c.body, c.stream, c.cut); got != c.want {
				t.Errorf("ClassifyReply = %+v, want %+v", got, c.want)
			}
		})
	}
}

// BytesObs.Add is where the reading's counts are made: each class of response in its own count, an invalid variant's refusal by its 4xx, every
// valid variant answered other than 200 by its key, status and group, a failure by its key (up to FailedKept of them), and a response the
// harness's own bound cut apart from the router's failures.
func TestBytesObsAddCountsEachResponseInItsOwnPlace(t *testing.T) {
	okJSON := []byte(`{"choices":[{"text":"hi"}]}`)
	okStream := []byte("data: {\"x\":1}\n\ndata: [DONE]\n\n")
	refusal := []byte(`{"error":{"message":"no"}}`)
	ans := func(chat, stream, valid bool, key string, status int, body []byte, cut bool) Answer {
		return Answer{Chat: chat, Stream: stream, Valid: valid, Key: key, Status: status, Body: body, Cut: cut}
	}
	var b BytesObs
	// Two chat answers and one completions answer that are not streamed, and one streamed chat answer: every count different from its pair, so
	// that a swap of chat with completions, or of streamed with not, shows.
	b.Add(ans(true, false, true, "c1", 200, okJSON, false))
	b.Add(ans(true, false, true, "c2", 200, okJSON, false))
	b.Add(ans(true, true, true, "c3", 200, okStream, false))
	b.Add(ans(false, false, true, "c4", 200, okJSON, false))
	want := BytesObs{Variants: 4, Complete: 4, Status200: 4, StreamOK: 1, NonStreamOK: 3, ChatOK: 3, CompletionsOK: 1}
	if !equalBytes(b, want) || len(b.NonOK) != 0 {
		t.Fatalf("after four answers: %+v, want %+v", b, want)
	}
	// An invalid variant refused with a 4xx: counted as sent and refused, as a 4xx, and no 200 of any mode.
	b.Add(ans(true, false, false, "bad", 400, refusal, false))
	// An invalid one answered 200: sent, not refused.
	b.Add(ans(true, false, false, "bad2", 200, okJSON, false))
	// An invalid one answered with a 5xx: sent, not refused, since vLLM refuses by a 4xx.
	b.Add(ans(true, false, false, "bad3", 500, refusal, false))
	// A valid one the engine refuses with a 4xx of its own, in a refusal group, and an engine's own 500: complete, not the router's, and both
	// kept with their status and group. An invalid variant's answers are not among them.
	inGroup := ans(true, false, true, "v4xx", 422, refusal, false)
	inGroup.Group = "suffix"
	b.Add(inGroup)
	b.Add(ans(false, false, true, "v500", 500, refusal, false))
	if b.InvalidSent != 3 || b.InvalidRefused != 1 || b.Status4xx != 2 || b.Status5xx != 2 || b.Status200 != 5 || b.RouterMade != 0 || len(b.Failed) != 0 {
		t.Errorf("after the refusals: %+v", b)
	}
	if wantNon := []NonOKVariant{{"v4xx", 422, "suffix"}, {"v500", 500, ""}}; !slices.Equal(b.NonOK, wantNon) {
		t.Errorf("NonOK = %+v, want %+v", b.NonOK, wantNon)
	}
	// A whole answer with a status that is none of 200, 4xx and 5xx is counted apart, and kept as a valid variant's non-200.
	b.Add(ans(true, false, true, "moved", 302, okJSON, false))
	if b.StatusOther != 1 || b.NonOK[len(b.NonOK)-1] != (NonOKVariant{"moved", 302, ""}) {
		t.Errorf("after a 302: StatusOther %d, NonOK %+v", b.StatusOther, b.NonOK)
	}
	// A gateway status, no answer and a cut body are the router's or the path's, named by key; none of them is a complete 200. The gateway's
	// 503 is a valid variant's non-200; no answer, and a cut 200, are not.
	b.Add(ans(true, false, true, "gw", 503, []byte("no healthy upstream"), false))
	b.Add(ans(true, true, true, "none", 0, nil, false))
	b.Add(ans(false, true, true, "cut", 200, okStream[:10], true))
	if b.RouterMade != 3 || !slices.Equal(b.Failed, []string{"gw", "none", "cut"}) {
		t.Errorf("after the failures: RouterMade %d, failed %v", b.RouterMade, b.Failed)
	}
	if n := len(b.NonOK); n != 4 || b.NonOK[3] != (NonOKVariant{"gw", 503, ""}) {
		t.Errorf("after the failures: NonOK %+v", b.NonOK)
	}
	// A response the reading's own bound cut is the harness's: counted apart, its key kept, never the router's, never a non-200.
	b.Add(Answer{Chat: true, Valid: true, Key: "late", Status: 0, Cut: true, PastBound: true})
	if b.PastBound != 1 || b.RouterMade != 3 || b.Failed[len(b.Failed)-1] != "late" || len(b.NonOK) != 4 {
		t.Errorf("after a response past the bound: PastBound %d, RouterMade %d, failed %v, NonOK %d", b.PastBound, b.RouterMade, b.Failed, len(b.NonOK))
	}
	if b.Variants != 14 {
		t.Errorf("Variants = %d, want the 4 answered, the 6 others, the 3 failures and the one past the bound, 14", b.Variants)
	}
	// An engine's own 5xx is complete, and counted apart from the 2xx and 4xx: the status counts add up to Complete.
	if b.Status200+b.Status4xx+b.Status5xx+b.StatusOther != b.Complete {
		t.Errorf("the status counts %d+%d+%d+%d do not add up to Complete %d", b.Status200, b.Status4xx, b.Status5xx, b.StatusOther, b.Complete)
	}
	// Only FailedKept keys are kept.
	var many BytesObs
	for i := 0; i < FailedKept+5; i++ {
		many.Add(ans(true, false, true, "k", 0, nil, false))
	}
	if len(many.Failed) != FailedKept || many.RouterMade != FailedKept+5 {
		t.Errorf("kept %d keys and counted %d failures, want %d and %d", len(many.Failed), many.RouterMade, FailedKept, FailedKept+5)
	}
}

func equalBytes(a, b BytesObs) bool {
	return a.Variants == b.Variants && a.Complete == b.Complete && a.Status200 == b.Status200 && a.Status4xx == b.Status4xx && a.Status5xx == b.Status5xx && a.StatusOther == b.StatusOther && a.PastBound == b.PastBound &&
		a.StreamOK == b.StreamOK && a.NonStreamOK == b.NonStreamOK && a.ChatOK == b.ChatOK && a.CompletionsOK == b.CompletionsOK &&
		a.InvalidSent == b.InvalidSent && a.InvalidRefused == b.InvalidRefused && a.RouterMade == b.RouterMade
}

// healthyOf reads status.healthyWorkers as the row records it: the count, and -1 when the field is absent (no report of the router's view yet).
func TestHealthyOfIsTheCountOrMinusOneWhenAbsent(t *testing.T) {
	n := func(v int32) *int32 { return &v }
	for _, c := range []struct {
		name string
		in   *int32
		want int
	}{{"absent", nil, -1}, {"zero", n(0), 0}, {"one", n(1), 1}, {"two", n(2), 2}} {
		var in v1alpha1.Instance
		in.Status.HealthyWorkers = c.in
		if got := healthyOf(in); got != c.want {
			t.Errorf("%s: healthyOf = %d, want %d", c.name, got, c.want)
		}
	}
}

// Only the router row's stack runs Instances behind Envoy: its host options are the router's alone, and every other row's are none. The live
// world and the stand-in's both take them from hostOptionsFor.
func TestOnlyTheRouterRowsStackRunsInstancesBehindEnvoy(t *testing.T) {
	opts := hostOptionsFor(BetaRouterName)
	if len(opts) != 1 || reflect.ValueOf(opts[0]).Pointer() != reflect.ValueOf(stack.InstanceRouter).Pointer() {
		t.Fatalf("the router row's host options are %d, want exactly stack.InstanceRouter", len(opts))
	}
	for _, r := range slices.Concat(Rows(), AccountingRows(), ReachRows(), HostDialedRows(), StageRows()) {
		if r.Name != BetaRouterName && r.Name != BetaKeeperName && len(hostOptionsFor(r.Name)) != 0 {
			t.Errorf("%s has host options: only the router row runs Instances behind a router, and only the keeper row the relay keeper", r.Name)
		}
	}
}

// The refusal groups are S13's own list (spikes/s13/README.md, "Valid requests a server may still refuse"): the README's bullets are read
// here, and each group's anchor is in exactly one of them, and each bullet holds exactly one group's anchor. A group retyped from memory, or a
// bullet S13 adds, fails.
func TestTheRefusalGroupsAreS13sList(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(repoRootForTest(t), "spikes", "s13", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	var bullets []string
	in := false
	for _, l := range strings.Split(string(b), "\n") {
		switch {
		case strings.HasPrefix(l, "- **Valid requests a server may still refuse.**"):
			in = true
		case in && strings.HasPrefix(l, "  - "):
			bullets = append(bullets, l)
		case in && strings.HasPrefix(l, "    ") && len(bullets) > 0:
			bullets[len(bullets)-1] += " " + strings.TrimSpace(l) // a bullet's continuation line
		case in && len(bullets) > 0 && !strings.HasPrefix(l, "  "):
			in = false
		}
	}
	if len(bullets) != len(refusalGroups) {
		t.Fatalf("S13's list has %d bullets, and refusalGroups %d groups:\n%s", len(bullets), len(refusalGroups), strings.Join(bullets, "\n"))
	}
	for _, g := range refusalGroups {
		n := 0
		for _, bl := range bullets {
			if strings.Contains(bl, g.anchor) {
				n++
			}
		}
		if n != 1 {
			t.Errorf("group %q's anchor %q is in %d of S13's bullets, want exactly 1", g.name, g.anchor, n)
		}
	}
	for _, bl := range bullets {
		n := 0
		for _, g := range refusalGroups {
			if strings.Contains(bl, g.anchor) {
				n++
			}
		}
		if n != 1 {
			t.Errorf("S13's bullet %q holds %d groups' anchors, want exactly 1", bl, n)
		}
	}
}

// repoRootForTest is the module's root, from this package's directory.
func repoRootForTest(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("no go.mod at %s: %v", root, err)
	}
	return root
}

// Each refusal group holds S13's cases of its fields, and a case that sets a plain sampling field is in none. Written out from the README's
// bullets and S13's case names, not from RefusalGroupOf.
func TestEachRefusalGroupHoldsItsCasesAndAPlainCaseNone(t *testing.T) {
	want := map[string]string{
		"chat/chat_template":                         "a request chat_template",
		"chat/mm_processor_kwargs":                   "mm_processor_kwargs and media_io_kwargs",
		"chat/media_io_kwargs":                       "mm_processor_kwargs and media_io_kwargs",
		"completions/prompt_embeds":                  "prompt_embeds",
		"chat/msg/user-part-prompt_embeds":           "prompt_embeds",
		"chat/priority":                              "a non-zero priority",
		"completions/priority":                       "a non-zero priority",
		"completions/suffix":                         "suffix",
		"chat/msg/user-part-image_url":               "the media content parts",
		"chat/msg/user-part-input_audio":             "the media content parts",
		"chat/msg/user-part-file":                    "the media content parts",
		"chat/msg/user-part-video_embeds":            "the media content parts",
		"chat/msg/user-part-simple-audio":            "the media content parts",
		"chat/response_format-structural_tag":        "the structural tags",
		"chat/response_format-structural_tag-legacy": "the structural tags",
		"chat/structured_outputs-structural_tag":     "the structural tags",
		// In none: plain fields, text and non-media parts, and chat_template_kwargs, which is not the request chat_template.
		"chat/temperature":                  "",
		"completions/top_p":                 "",
		"chat/minimal":                      "",
		"chat/msg/user-part-text":           "",
		"chat/msg/user-part-tool_reference": "",
		"chat/msg/assistant-part-refusal":   "",
		"chat/chat_template_kwargs":         "",
	}
	seen := map[string]bool{}
	for _, c := range cases.All() {
		g, ok := want[c.Key()]
		if !ok {
			continue
		}
		seen[c.Key()] = true
		if got := RefusalGroupOf(c); got != g {
			t.Errorf("%s is in group %q, want %q", c.Key(), got, g)
		}
	}
	for k := range want {
		if !seen[k] {
			t.Errorf("S13's cases have no %s", k)
		}
	}
	for _, g := range refusalGroups {
		n := 0
		for _, c := range cases.All() {
			if RefusalGroupOf(c) == g.name {
				n++
			}
		}
		if n == 0 {
			t.Errorf("group %q holds none of S13's cases", g.name)
		}
	}
}

// probeGlibc tries the step up to its tries, logs each failed try with its exit code and error, and fails naming the step when every try
// failed: a transport error, a non-zero exit and an answer that is not getconf's are each a failed try, never a reading of the node.
func TestProbeGlibcRetriesThenFailsNamingTheStep(t *testing.T) {
	ok := slurm.Result{Stdout: []byte("glibc 2.34\n")}
	for _, c := range []struct {
		name    string
		answers []func() (slurm.Result, error)
		want    GlibcVersion
		logs    int
		err     string
	}{
		{"the first try", []func() (slurm.Result, error){func() (slurm.Result, error) { return ok, nil }}, GlibcVersion{2, 34}, 0, ""},
		{"a transport error, then the answer", []func() (slurm.Result, error){
			func() (slurm.Result, error) { return slurm.Result{}, errors.New("ssh: connection reset") },
			func() (slurm.Result, error) { return ok, nil },
		}, GlibcVersion{2, 34}, 1, ""},
		{"a non-zero exit and a stray answer, then the answer", []func() (slurm.Result, error){
			func() (slurm.Result, error) { return slurm.Result{ExitCode: 1}, nil },
			func() (slurm.Result, error) {
				return slurm.Result{Stdout: []byte("srun: error: Unable to create step")}, nil
			},
			func() (slurm.Result, error) { return ok, nil },
		}, GlibcVersion{2, 34}, 2, ""},
		{"three non-zero exits", []func() (slurm.Result, error){
			func() (slurm.Result, error) { return slurm.Result{ExitCode: 1}, nil },
			func() (slurm.Result, error) { return slurm.Result{ExitCode: 1}, nil },
			func() (slurm.Result, error) { return slurm.Result{ExitCode: 137}, nil },
		}, GlibcVersion{}, 3, "the srun step that runs getconf exited 137, in each of 3 tries"},
		{"three transport errors", []func() (slurm.Result, error){
			func() (slurm.Result, error) { return slurm.Result{}, errors.New("ssh: a") },
			func() (slurm.Result, error) { return slurm.Result{}, errors.New("ssh: b") },
			func() (slurm.Result, error) { return slurm.Result{ExitCode: 255}, errors.New("ssh: c") },
		}, GlibcVersion{}, 3, "the srun step that runs getconf failed: ssh: c (exit 255), in each of 3 tries"},
		{"three answers that are not getconf's", []func() (slurm.Result, error){
			func() (slurm.Result, error) { return slurm.Result{Stdout: []byte("x")}, nil },
			func() (slurm.Result, error) { return slurm.Result{Stdout: []byte("y")}, nil },
			func() (slurm.Result, error) { return slurm.Result{Stdout: []byte("musl 1.2")}, nil },
		}, GlibcVersion{}, 3, "the srun step that runs getconf answered \"musl 1.2\", not getconf's \"glibc <major>.<minor>\", in each of 3 tries"},
	} {
		t.Run(c.name, func(t *testing.T) {
			calls, logs := 0, 0
			run := func(_ context.Context, cmd slurm.Command) (slurm.Result, error) {
				if cmd.Line != "srun-line" || string(cmd.Stdin) != GlibcScript {
					t.Errorf("the step ran %q with %q", cmd.Line, cmd.Stdin)
				}
				calls++
				return c.answers[calls-1]()
			}
			v, err := probeGlibc(context.Background(), run, "srun-line", GlibcScript, glibcProbeTries, 0, func(string, ...any) { logs++ })
			if c.err == "" {
				if err != nil || v != c.want {
					t.Fatalf("probeGlibc = %v, %v; want %v", v, err, c.want)
				}
			} else if err == nil || err.Error() != c.err {
				t.Fatalf("probeGlibc's error = %v, want %q", err, c.err)
			}
			if calls != len(c.answers) || logs != c.logs {
				t.Errorf("%d tries and %d log lines, want %d and %d", calls, logs, len(c.answers), c.logs)
			}
		})
	}
	if glibcProbeTries != 3 {
		t.Errorf("glibcProbeTries = %d, want 3 (team-lead's two or three, #530's review)", glibcProbeTries)
	}
}

// EngineVersionOf reads the verification record's engineVersion, and only a value of a version's shape: anything else is not posted.
func TestEngineVersionOfReadsTheVerificationRecord(t *testing.T) {
	for _, c := range []struct{ raw, want string }{
		{`{"instance":"i","engineVersion":"0.32.0","imageDigest":"sha256:x"}`, "0.32.0"},
		{`{"engineVersion":"0.32.1.dev3+gabc123"}`, "0.32.1.dev3+gabc123"},
		{`{"instance":"i"}`, ""},
		{`{"engineVersion":"0.32.0 on host-x"}`, ""},
		{`{"engineVersion":"` + strings.Repeat("9", 41) + `"}`, ""},
		{`not json`, ""},
	} {
		if got := EngineVersionOf([]byte(c.raw)); got != c.want {
			t.Errorf("EngineVersionOf(%s) = %q, want %q", c.raw, got, c.want)
		}
	}
	if S13VLLMVersion != "v0.32.0" {
		t.Errorf("S13VLLMVersion = %q, want S13's v0.32.0 (spikes/s13/fields/vllm-v0.32.0.tsv)", S13VLLMVersion)
	}
}

// Through the engine double, without a stack, every one of S13's variants is answered so that the reading fails nothing: the double reads
// the four prompt forms vLLM reads (enginedouble, promptOf) and refuses only what S13 says vLLM refuses. It is what the router stand-in's
// verdict says (TestStandInBetaRouter), read here where CI runs it. The control: the reading still fails a valid variant the double is made to
// refuse outside S13's groups.
func TestThroughTheDoubleS13sVariantsFailNothing(t *testing.T) {
	cfg := enginedouble.DefaultConfig()
	srv := httptest.NewServer(enginedouble.Handler(cfg))
	defer srv.Close()
	var b BytesObs
	for _, c := range cases.All() {
		for _, v := range c.Variants() {
			resp, err := http.Post(srv.URL+string(c.Endpoint), "application/json", strings.NewReader(string(AdaptBody(v.Body, cfg.ServedName))))
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			b.Add(Answer{Chat: c.Endpoint == cases.Chat, Stream: v.Stream, Valid: v.Valid(), Group: RefusalGroupOf(c), Key: v.Key(), Status: resp.StatusCode, Body: body})
		}
	}
	b.Expected = b.Variants
	if f := bytesChecks(b); len(f) != 0 {
		t.Errorf("bytesChecks through the double = %q, want none", f)
	}
	if len(b.NonOK) != 0 || b.InvalidRefused != b.InvalidSent || b.InvalidSent != 2 {
		t.Errorf("through the double: %d valid variants answered other than 200 (%v), %d of %d invalid ones refused; want none, and both", len(b.NonOK), b.NonOK, b.InvalidRefused, b.InvalidSent)
	}
	b.NonOK = append(b.NonOK, NonOKVariant{"completions/prompt-token-ids#plain", 400, ""})
	if f := bytesChecks(b); len(f) != 1 {
		t.Errorf("a refusal outside the groups added to the double's reading: bytesChecks = %q, want it named", f)
	}
}
