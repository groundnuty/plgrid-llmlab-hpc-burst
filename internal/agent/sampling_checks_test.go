package agent_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The checks' sampling, end to end (HBEP-7, The allocation agent, Checks;
// science-agent rulings, 1 Nov 2026, issues #125 and #151): the real agent
// stages a checkpoint whose generation_config.json the test serves, runs
// verification, qualification and warm-up against the engine double as a
// process, and the double's log shows what reached it. The expected values are
// written out by hand from each fixture file; none passes through the agent's
// reader or agentapi.CheckSampling.

func fp(v float64) *float64 { return &v }
func ip(v int64) *int64     { return &v }
func bp(v bool) *bool       { return &v }

// requestChecks names the check of a recorded request by the closing words of
// its prompt, written out here from the agent's fixed prompts.
func requestCheck(tail string) string {
	switch {
	case strings.HasSuffix(tail, "Reply with one word: ready."):
		return "verify"
	case strings.HasSuffix(tail, "Say hello in one short sentence."):
		return "chat"
	case strings.HasSuffix(tail, "Call the get_weather tool to find out."):
		return "toolCall"
	case strings.HasSuffix(tail, "separated by commas."):
		return "corruption"
	case strings.Contains(tail, "Ignore the list of words above."):
		return "behaviour"
	case strings.HasSuffix(tail, "Continue the list of words above."), strings.HasSuffix(tail, "Call the get_weather tool for the city of Krakow."):
		return "warmUp"
	}
	return "unknown: " + tail
}

// chatEvents returns the engine double's recorded chat requests so far.
func (h *harness) chatEvents() []enginedouble.LogEvent {
	var out []enginedouble.LogEvent
	for _, line := range readLines(h.t, h.engLog) {
		var ev enginedouble.LogEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			h.t.Fatalf("engine log line %q: %v", line, err)
		}
		if ev.Event == "request" && ev.Path == "/v1/chat/completions" {
			out = append(out, ev)
		}
	}
	return out
}

// checkCounts is how many requests each check sent once, counted by hand:
// verification's fingerprint 1, the chat check 1, the tool-call check 1, the
// corruption check's three identical requests 3, the behaviour check 1.
var checkCounts = map[string]int{"verify": 1, "chat": 1, "toolCall": 1, "corruption": 3, "behaviour": 1}

func countsOf(events []enginedouble.LogEvent) map[string]int {
	n := map[string]int{}
	for _, ev := range events {
		if c := requestCheck(ev.Tail); c != "warmUp" {
			n[c]++
		}
	}
	return n
}

const samplingRevision = "cccccccccccccccccccccccccccccccccccccccc"

// samplingCase is one model: whether its launch declares a reasoning parser,
// the generation_config.json its checkpoint holds ("" for none), and what the
// checks must send.
type samplingCase struct {
	name      string
	reasoning bool
	file      string
	// want is the sampling every request must carry, except the seed;
	// seeded says each check's own seed rides along.
	want   enginedouble.RequestSampling
	seeded bool
	// config and doSample are what verification reports from the file.
	config   map[string]string
	doSample *bool
}

// greedyWant is HBEP-7's greedy request: temperature 0, top_p 1,
// repetition_penalty 1, presence_penalty 0, frequency_penalty 0.
var greedyWant = enginedouble.RequestSampling{Temperature: fp(0), TopP: fp(1), RepetitionPenalty: fp(1), PresencePenalty: fp(0), FrequencyPenalty: fp(0)}

// qwen3Want is HBEP-7's Test plan for a reasoning model whose file gives
// temperature 0.6, top_k 20 and top_p 0.95: those three, min_p 0, the
// penalties neutral.
var qwen3Want = enginedouble.RequestSampling{Temperature: fp(0.6), TopP: fp(0.95), TopK: ip(20), MinP: fp(0), RepetitionPenalty: fp(1), PresencePenalty: fp(0), FrequencyPenalty: fp(0)}

var seedOfCheck = map[string]int64{"verify": 1101, "chat": 1102, "toolCall": 1103, "corruption": 1104, "behaviour": 1105, "warmUp": 1106}

var samplingCases = []samplingCase{
	{
		name: "a reasoning model with Qwen3-0.6B's file is sampled with its values", reasoning: true,
		file: `{"bos_token_id": 151643, "do_sample": true, "eos_token_id": [151645, 151643], "pad_token_id": 151643, "temperature": 0.6, "top_k": 20, "top_p": 0.95, "transformers_version": "4.51.0"}`,
		want: qwen3Want, seeded: true,
		config: map[string]string{"temperature": "0.6", "top_k": "20", "top_p": "0.95"}, doSample: bp(true),
	},
	{
		name: "a file without do_sample still samples a reasoning model", reasoning: true,
		file: `{"temperature": 0.6, "top_k": 20, "top_p": 0.95}`,
		want: qwen3Want, seeded: true,
		config: map[string]string{"temperature": "0.6", "top_k": "20", "top_p": "0.95"},
	},
	{
		name: "the same checkpoint without a reasoning parser stays greedy", reasoning: false,
		file:   `{"do_sample": true, "temperature": 0.6, "top_k": 20, "top_p": 0.95}`,
		want:   greedyWant,
		config: map[string]string{"temperature": "0.6", "top_k": "20", "top_p": "0.95"}, doSample: bp(true),
	},
	{
		name: "without a parser a file with repetition_penalty 1.1 still sends 1 and the record holds the 1.1", reasoning: false,
		file:   `{"do_sample": true, "repetition_penalty": 1.1, "temperature": 0.7, "top_p": 0.8, "top_k": 20}`,
		want:   greedyWant,
		config: map[string]string{"repetition_penalty": "1.1", "temperature": "0.7", "top_p": "0.8", "top_k": "20"}, doSample: bp(true),
	},
	{
		name: "a reasoning model whose file gives none of the seven keys stays greedy", reasoning: true,
		file: `{"bos_token_id": 151643, "do_sample": true, "max_new_tokens": 2048}`,
		want: greedyWant,
		// max_new_tokens is one of the keys the record holds, not one of the seven.
		config: map[string]string{"max_new_tokens": "2048"}, doSample: bp(true),
	},
	{
		name: "a reasoning model whose file sets do_sample false stays greedy", reasoning: true,
		file:   `{"do_sample": false, "temperature": 0.6, "top_k": 20, "top_p": 0.95}`,
		want:   greedyWant,
		config: map[string]string{"temperature": "0.6", "top_k": "20", "top_p": "0.95"}, doSample: bp(false),
	},
	{
		name: "a reasoning model with no file stays greedy", reasoning: true,
		file: "",
		want: greedyWant,
	},
}

func TestTheChecksSendTheSamplingOfTheirModel(t *testing.T) {
	for _, tc := range samplingCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := harnessOptions{}
			files := defaultFiles
			if tc.file != "" {
				sum := sha256.Sum256([]byte(tc.file))
				opts.hub = map[string]string{"/org/model/resolve/" + samplingRevision + "/generation_config.json": tc.file}
				files = append(append([]agentapi.FileDigest(nil), defaultFiles...), agentapi.FileDigest{Path: "generation_config.json", SHA256: hex.EncodeToString(sum[:])})
			}
			h := newHarness(t, opts)
			checks := &agentapi.Checks{
				VerificationGeneration: 1,
				Verify:                 &agentapi.VerifyRequest{Files: files},
				Qualify:                &agentapi.QualifyRequest{Shapes: hShapes, ToolCalls: true},
				WarmUp:                 &agentapi.WarmUpRequest{Shapes: hShapes, ToolCalls: true, TargetSessions: 1},
			}
			d := h.runState("reasoning-first=20", 1, 0, checks)
			if tc.reasoning {
				d = withReasoning(d)
			}
			h.set(d)
			r := h.waitReport("qualification and a warm-up round", 90*time.Second, func(r agentapi.Report) bool {
				c := checksOf(r)
				return qualified(1)(r) && c.WarmUp != nil && len(c.WarmUp.Rounds) >= 1
			})
			for _, o := range r.Instance.Checks.Qualify.Checks {
				if !o.Passed {
					t.Errorf("check %s failed: %s %s", o.Name, o.Reason, o.Message)
				}
			}
			// What verification reported from the file.
			v := r.Instance.Checks.Verify
			if !reflect.DeepEqual(v.GenerationConfig, tc.config) {
				t.Errorf("GenerationConfig %v, want %v", v.GenerationConfig, tc.config)
			}
			if !reflect.DeepEqual(v.DoSample, tc.doSample) {
				t.Errorf("DoSample %v, want %v", ptrText(v.DoSample), ptrText(tc.doSample))
			}
			// End warm-up and wait for it to be done, so every request is in the log.
			end := *d.Instance.Checks
			end.WarmUp = nil
			stopped := d
			inst := *d.Instance
			inst.Checks = &end
			stopped.Instance = &inst
			h.set(stopped)
			h.waitReport("warm-up done", 60*time.Second, warmDone)

			events := h.chatEvents()
			if got := countsOf(events); !reflect.DeepEqual(got, checkCounts) {
				t.Errorf("requests per check %v, want %v: a check was sent twice or not at all", got, checkCounts)
			}
			warm := 0
			for i, ev := range events {
				check := requestCheck(ev.Tail)
				want := tc.want
				if tc.seeded {
					want.Seed = ip(seedOfCheck[check])
				}
				if ev.Sampling == nil || !reflect.DeepEqual(*ev.Sampling, want) {
					t.Errorf("request %d (%s): sampling %s, want %s", i, check, samplingText(ev.Sampling), samplingText(&want))
				}
				if check == "warmUp" {
					warm++
				}
			}
			// Six requests make a round with a tool-call shape and one concurrent request.
			if warm < 6 {
				t.Errorf("%d warm-up requests logged, want at least a round of 6", warm)
			}
		})
	}
}

func ptrText(p *bool) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprint(*p)
}

func samplingText(s *enginedouble.RequestSampling) string {
	if s == nil {
		return "nil"
	}
	b, _ := json.Marshal(s)
	return string(b)
}

// Verification alone, with nothing after it: its fingerprint request is a
// check request like the others (HBEP-7, Checks: "every request of
// verification, qualification and warm-up"), so on a reasoning model with
// Qwen3-0.6B's file it carries the file's values and verification's seed, 1101.
// The agent therefore reads the file before it sends the fingerprint request
// (PR 1 review, N8). With no qualification or warm-up to resolve the sampling
// first, a fingerprint request sent before the read would go out greedy.
func TestTheFingerprintRequestIsSampledLikeTheOtherChecks(t *testing.T) {
	file := `{"do_sample": true, "temperature": 0.6, "top_k": 20, "top_p": 0.95}`
	sum := sha256.Sum256([]byte(file))
	files := append(append([]agentapi.FileDigest(nil), defaultFiles...), agentapi.FileDigest{Path: "generation_config.json", SHA256: hex.EncodeToString(sum[:])})
	h := newHarness(t, harnessOptions{hub: map[string]string{"/org/model/resolve/" + samplingRevision + "/generation_config.json": file}})
	h.set(withReasoning(h.runState("", 1, 0, verifyChecks(1, files))))
	r := h.waitReport("verification", 60*time.Second, verified(1))
	if v := r.Instance.Checks.Verify; v.Error != "" || v.Fingerprint == "" {
		t.Fatalf("verification: error %q, fingerprint %q", v.Error, v.Fingerprint)
	}
	// Wait on the double's log for the fingerprint request: the handler records
	// it as it returns, which can follow the agent's report by a moment.
	var events []enginedouble.LogEvent
	deadline := time.Now().Add(20 * time.Second)
	for len(events) == 0 {
		if events = h.chatEvents(); len(events) == 0 {
			if time.Now().After(deadline) {
				t.Fatal("the engine double logged no chat request")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if len(events) != 1 || requestCheck(events[0].Tail) != "verify" {
		t.Fatalf("%d chat requests %+v, want the one fingerprint request", len(events), events)
	}
	want := qwen3Want
	want.Seed = ip(1101)
	if got := events[0].Sampling; got == nil || !reflect.DeepEqual(*got, want) {
		t.Errorf("the fingerprint request carried %s, want %s", samplingText(got), samplingText(&want))
	}
}

// A staged generation_config.json with a value vLLM refuses fails
// verification for every model, greedy or sampled, before any check request
// (HBEP-7; science-agent ruling, 1 Nov 2026, N7): vLLM applies the file's
// values as the server's defaults and would refuse them on every request. The
// agent reports the refusal as a struct, GenerationConfigRefused, naming the
// key and the value; it sends the engine no chat request at all (the
// fingerprint, qualification and warm-up included), though the desired state
// asks for all three. The expected refusals are written out by hand from
// vLLM's rules (temperature in [0, 2], top_k an integer).
func TestARefusedFileFailsVerificationAndSendsNoCheckRequest(t *testing.T) {
	for _, tc := range []struct {
		name      string
		reasoning bool
		file      string
		want      agentapi.GenerationConfigError
		config    map[string]string
	}{
		{"a reasoning model with temperature 2.5", true, `{"do_sample": true, "temperature": 2.5, "top_k": 20, "top_p": 0.95}`,
			agentapi.GenerationConfigError{Param: "temperature", Value: "2.5", Want: "[0, 2]"},
			map[string]string{"temperature": "2.5", "top_k": "20", "top_p": "0.95"}},
		{"a model without a parser with temperature 2.5", false, `{"do_sample": true, "temperature": 2.5, "top_k": 20, "top_p": 0.95}`,
			agentapi.GenerationConfigError{Param: "temperature", Value: "2.5", Want: "[0, 2]"},
			map[string]string{"temperature": "2.5", "top_k": "20", "top_p": "0.95"}},
		{"a reasoning model with temperature \"warm\"", true, `{"temperature": "warm"}`,
			agentapi.GenerationConfigError{Param: "temperature", Value: `"warm"`, Want: "a number"}, nil},
		{"a model without a parser with top_k 20.0", false, `{"temperature": 0.6, "top_k": 20.0}`,
			agentapi.GenerationConfigError{Param: "top_k", Value: "20.0", Want: "an integer"},
			map[string]string{"temperature": "0.6"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertRefusedFileSendsNothing(t, tc.reasoning, tc.file, tc.want, tc.config)
		})
	}
}

// A present generation_config.json that the agent cannot judge fails
// verification as a whole, with no check request, exactly as a refused value
// does, never read as absent and run greedy (PR #169 review, Astra B1 and Opus
// N3): a valid file above 1 MiB whose first key is temperature 2.5, and a file
// with the NaN, Infinity or -Infinity that Python's json accepts and Go's
// refuses. The report carries an empty Param and plain words, and none of the
// file's content. The expected refusals are written out by hand.
func TestAFileTheAgentCannotJudgeFailsVerificationAndSendsNoCheckRequest(t *testing.T) {
	big := `{"temperature": 2.5, "pad": "` + strings.Repeat("x", 1<<20) + `"}`
	if len(big) <= 1<<20 {
		t.Fatalf("the oversized fixture is %d bytes", len(big))
	}
	oversized := agentapi.GenerationConfigError{Param: "", Value: "larger than 1 MiB", Want: "a JSON object of at most 1 MiB"}
	notJSON := agentapi.GenerationConfigError{Param: "", Value: "not valid JSON", Want: "a JSON object of at most 1 MiB"}
	for _, tc := range []struct {
		name      string
		reasoning bool
		file      string
		want      agentapi.GenerationConfigError
	}{
		{"a valid file above 1 MiB with temperature 2.5, on a reasoning model", true, big, oversized},
		{"the same without a parser", false, big, oversized},
		{"temperature NaN", true, `{"temperature": NaN, "top_k": 20}`, notJSON},
		{"top_p Infinity", true, `{"top_p": Infinity}`, notJSON},
		{"repetition_penalty -Infinity, without a parser", false, `{"repetition_penalty": -Infinity}`, notJSON},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertRefusedFileSendsNothing(t, tc.reasoning, tc.file, tc.want, nil)
		})
	}
}

// assertRefusedFileSendsNothing stages a checkpoint that holds file as its
// generation_config.json, asks for verification, qualification and warm-up,
// and checks that the agent reports want as the refusal and the file's numbers
// as config, reads nothing from the engine, and sends it no chat request at all.
func assertRefusedFileSendsNothing(t *testing.T, reasoning bool, file string, want agentapi.GenerationConfigError, config map[string]string) {
	t.Helper()
	sum := sha256.Sum256([]byte(file))
	files := append(append([]agentapi.FileDigest(nil), defaultFiles...), agentapi.FileDigest{Path: "generation_config.json", SHA256: hex.EncodeToString(sum[:])})
	h := newHarness(t, harnessOptions{hub: map[string]string{"/org/model/resolve/" + samplingRevision + "/generation_config.json": file}})
	checks := &agentapi.Checks{
		VerificationGeneration: 1,
		Verify:                 &agentapi.VerifyRequest{Files: files},
		Qualify:                &agentapi.QualifyRequest{Shapes: hShapes, ToolCalls: true},
		WarmUp:                 &agentapi.WarmUpRequest{Shapes: hShapes, ToolCalls: true, TargetSessions: 1},
	}
	d := h.runState("", 1, 0, checks)
	if reasoning {
		d = withReasoning(d)
	}
	h.set(d)
	r := h.waitReport("verification", 60*time.Second, verified(1))
	v := r.Instance.Checks.Verify
	if !reflect.DeepEqual(v.GenerationConfigRefused, &want) {
		t.Errorf("GenerationConfigRefused %+v, want %+v", v.GenerationConfigRefused, want)
	}
	if v.Error != "" || v.Fingerprint != "" || v.EngineVersion != "" {
		t.Errorf("verification read the engine despite the refusal: error %q, fingerprint %q, version %q", v.Error, v.Fingerprint, v.EngineVersion)
	}
	if !reflect.DeepEqual(v.GenerationConfig, config) {
		t.Errorf("GenerationConfig %v, want %v", v.GenerationConfig, config)
	}
	// The agent is told to stop; once the engine has gone, its log is final.
	h.set(agentapi.DesiredState{Action: agentapi.ActionIdle, Settings: d.Settings})
	last := h.waitReport("worker stopped", 60*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Phase == agentapi.PartStopped
	})
	if c := checksOf(last); c != nil && (c.Qualify != nil || c.WarmUp != nil) {
		t.Errorf("a refused generation produced qualification %+v or warm-up %+v", c.Qualify, c.WarmUp)
	}
	if events := h.chatEvents(); len(events) != 0 {
		t.Errorf("%d chat requests reached the engine, want none: %+v", len(events), events)
	}
}
