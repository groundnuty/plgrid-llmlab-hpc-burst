package results

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s13/rawjson"
)

// These tests hold the README's claims to the committed runs: each verdict is recomputed from the
// recorded bytes, so a claim that the evidence does not carry fails here.

func load(t *testing.T, name string) *Run {
	t.Helper()
	r, err := Load(name + "/results.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestControlSawEveryBodyAsItsBytes(t *testing.T) {
	r := load(t, "direct-control")
	if len(r.Variants) != 440 {
		t.Fatalf("%d variants", len(r.Variants))
	}
	for k, v := range r.Variants {
		if v.Arrivals != 1 || !v.Identical || len(v.Diffs) != 0 {
			t.Errorf("%s: arrivals %d, identical %v, diffs %v", k, v.Arrivals, v.Identical, v.Diffs)
		}
	}
}

// field returns the plain and streamed verdicts of one cover, "-" for a variant that is not sent.
func field(r *Run, c, f string) string {
	for k, m := range r.Fields {
		if k.Case == c && k.Field == f {
			p, s := "-", "-"
			if v, ok := m[false]; ok {
				p = string(v.Verdict)
			}
			if v, ok := m[true]; ok {
				s = string(v.Verdict)
			}
			return p + "|" + s
		}
	}
	return "no such cover"
}

func both(v rawjson.Verdict) string       { return string(v) + "|" + string(v) }
func streamOnly(v rawjson.Verdict) string { return "-|" + string(v) }

// Losses both releases share, under HBEP-5's policy.
var shared = map[[2]string]string{
	{"chat/tools-function-strict", "tools[].function.strict"}:                                     both(rawjson.Dropped),
	{"chat/tools-function-defer_loading", "tools[].function.defer_loading"}:                       both(rawjson.Dropped),
	{"chat/tools-defer_loading", "tools[].defer_loading"}:                                         both(rawjson.Dropped),
	{"chat/tools-no-parameters", "tools[].function.name"}:                                         both(Refused),
	{"chat/response_format-json_schema", "response_format.json_schema.description"}:               both(rawjson.Dropped),
	{"chat/response_format-json_schema", "response_format.json_schema.strict"}:                    both(rawjson.Kept),
	{"chat/response_format-structural_tag", "response_format(structural_tag).format"}:             both(Refused),
	{"completions/response_format-structural_tag", "response_format(structural_tag).format"}:      both(rawjson.Kept),
	{"chat/stream_options", "stream_options.continuous_usage_stats"}:                              streamOnly(rawjson.Dropped),
	{"completions/stream_options", "stream_options.continuous_usage_stats"}:                       streamOnly(rawjson.Dropped),
	{"completions/max_tokens-null", "max_tokens"}:                                                 both(rawjson.Dropped),
	{"chat/msg/assistant-refusal", "messages[](assistant).refusal"}:                               both(rawjson.Dropped),
	{"chat/msg/assistant-audio", "messages[](assistant).audio"}:                                   both(rawjson.Dropped),
	{"chat/msg/assistant-tool_calls-custom", "messages[](assistant).tool_calls[](custom).custom"}: both(rawjson.Dropped),
	{"chat/msg/user-task", "messages[](custom).task"}:                                             both(rawjson.Dropped),
	{"chat/msg/custom-role", "messages[](custom).role"}:                                           both(rawjson.Kept),
	{"chat/msg/system-array-content", "messages[](system).content"}:                               both(rawjson.Kept),
	{"chat/ext/messages-x_ext", "ext:messages[].x_ext"}:                                           both(rawjson.Dropped),
	{"chat/ext/tools-function-x_ext", "ext:tools[].function.x_ext"}:                               both(rawjson.Dropped),
	{"chat/ext/top-level-object", "ext:x_top"}:                                                    both(rawjson.Reencoded),
	{"chat/ext/openai-sdk-fields", "ext:store"}:                                                   both(rawjson.Kept),
	{"chat/ext/assistant-reasoning_content", "ext:messages[](assistant).reasoning_content"}:       both(rawjson.Renamed),
	{"chat/msg/harmony", "messages[](harmony).author.role"}:                                       both(Refused),
	{"chat/enc/duplicate-known-field", "ext:a field written twice"}:                               both(Refused),
	{"chat/enc/duplicate-in-dict", "ext:a member written twice inside a dict[str, Any]"}:          both(rawjson.DuplicateRemoved),
	{"chat/enc/duplicate-unknown-top", "ext:an unknown top-level member written twice"}:           both(rawjson.DuplicateRemoved),
	{"chat/enc/key-escaped-top", "ext:a top-level key written with an escape"}:                    both(rawjson.KeyReencoded),
	{"chat/enc/key-escaped-nested", "ext:a key inside a dict[str, Any] written with an escape"}:   both(rawjson.KeyReencoded),
	{"chat/logprob_token_ids", "logprob_token_ids"}:                                               both(rawjson.Kept),
	{"completions/prompt_logprobs", "prompt_logprobs"}:                                            "kept|-",
	{"chat/enc/temperature-integer", "ext:temperature as an integer literal"}:                     both(rawjson.Retyped),
	{"chat/enc/temperature-20-digits", "ext:a float beyond float64 precision"}:                    both(rawjson.Changed),
	{"chat/enc/explicit-nulls", "ext:max_tokens: null"}:                                           both(rawjson.Dropped),
	{"chat/real/agent-turn", "ext:agent turn: tools[].function.strict"}:                           streamOnly(rawjson.Dropped),
	{"chat/temperature", "temperature"}:                                                           both(rawjson.Kept),
	{"chat/seed-int64-max", "seed"}:                                                               both(rawjson.Kept),
}

// Content parts 0.1.16 replaces with an empty string or removes, and 0.1.17 delivers.
var contentParts = map[[2]string]rawjson.Verdict{
	{"chat/msg/user-part-input_audio", "messages[].content[](input_audio).input_audio"}:    rawjson.Retyped,
	{"chat/msg/user-part-file", "messages[].content[](file).file"}:                         rawjson.Retyped,
	{"chat/msg/user-part-video_url", "messages[].content[](video_url).video_url"}:          rawjson.Retyped,
	{"chat/msg/user-part-image_embeds", "messages[].content[](image_embeds).image_embeds"}: rawjson.Retyped,
	{"chat/msg/assistant-part-refusal", "messages[].content[](refusal).refusal"}:           rawjson.Dropped,
	{"chat/msg/assistant-part-thinking", "messages[].content[](thinking).thinking"}:        rawjson.Dropped,
	{"chat/ext/content-part-cache_control", "ext:messages[].content[].cache_control"}:      rawjson.Dropped,
}

func TestReleasesUnderHBEP5Policy(t *testing.T) {
	for _, v := range []string{"0.1.16", "0.1.17"} {
		r := load(t, "vllm-router-"+v+"-consistent_hash")
		for k, want := range shared {
			if got := field(r, k[0], k[1]); got != want {
				t.Errorf("%s %s %s: %s, want %s", v, k[0], k[1], got, want)
			}
		}
		for k, old := range contentParts {
			want := both(rawjson.Kept)
			if v == "0.1.16" {
				want = both(old)
			}
			if got := field(r, k[0], k[1]); got != want {
				t.Errorf("%s %s %s: %s, want %s", v, k[0], k[1], got, want)
			}
		}
		for key, x := range r.Variants {
			if x.Identical {
				t.Errorf("%s: %s arrived as its bytes; the README says none does", v, key)
			}
		}
	}
}

func injectedInto(r *Run, key string) string {
	var out []string
	for _, d := range r.Variants[key].Diffs {
		if d.Verdict == rawjson.Injected {
			out = append(out, d.Path+"="+d.Received)
		}
	}
	sort.Strings(out)
	return strings.Join(out, " ")
}

func TestInjectedIntoMinimalRequests(t *testing.T) {
	chat := "continue_final_message=false ignore_eos=false include_reasoning=true logprobs=false no_stop_trim=false " +
		"return_hidden_states=false separate_reasoning=true skip_special_tokens=true stream=false stream_reasoning=true"
	compl := "echo=false ignore_eos=false no_stop_trim=false return_hidden_states=false skip_special_tokens=true stream=false"
	for _, v := range []string{"0.1.16", "0.1.17"} {
		r := load(t, "vllm-router-"+v+"-consistent_hash")
		if got := injectedInto(r, "chat/minimal#plain"); got != chat {
			t.Errorf("%s chat: %s", v, got)
		}
		if got := injectedInto(r, "completions/minimal#plain"); got != compl {
			t.Errorf("%s completions: %s", v, got)
		}
	}
}

// diffSet is a variant's diffs in a fixed order: a router's maps serialise in no fixed order, so
// the order of injected members may differ between runs.
func diffSet(v *Variant) string {
	var s []string
	for _, d := range v.Diffs {
		s = append(s, fmt.Sprintf("%+v", d))
	}
	sort.Strings(s)
	return strings.Join(s, "\n") + fmt.Sprintf("|status %d|arrivals %d", v.Status, v.Arrivals)
}

func TestPolicyDoesNotChangeTheBody(t *testing.T) {
	for _, v := range []string{"0.1.16", "0.1.17"} {
		a, b := load(t, "vllm-router-"+v+"-consistent_hash"), load(t, "vllm-router-"+v+"-round_robin")
		for key, x := range a.Variants {
			if diffSet(x) != diffSet(b.Variants[key]) {
				t.Errorf("%s %s: consistent_hash and round_robin differ", v, key)
			}
		}
	}
}

func TestKvAwareForwardsCompletionsAsBytes(t *testing.T) {
	kv, ch := load(t, "vllm-router-0.1.17-kv_aware"), load(t, "vllm-router-0.1.17-consistent_hash")
	n := 0
	for key, x := range kv.Variants {
		if x.Endpoint == "/v1/completions" {
			n++
			if !x.Identical {
				t.Errorf("%s: not its bytes under kv_aware", key)
			}
			continue
		}
		if diffSet(x) != diffSet(ch.Variants[key]) {
			t.Errorf("%s: chat under kv_aware differs from consistent_hash", key)
		}
	}
	if n != 142 {
		t.Errorf("%d completions variants, want 142", n)
	}
	if !reflect.DeepEqual(kv.Delivered, map[string]int{"/v1/chat/completions": 288, "/v1/completions": 142}) {
		t.Errorf("delivered %v", kv.Delivered)
	}
}

func TestStreamingDoesNotChangeAVerdict(t *testing.T) {
	for _, v := range []string{"0.1.16", "0.1.17"} {
		r := load(t, "vllm-router-"+v+"-consistent_hash")
		both := 0
		for k, m := range r.Fields {
			p, hp := m[false]
			s, hs := m[true]
			if !hp || !hs {
				continue
			}
			both++
			if p.Verdict != s.Verdict {
				t.Errorf("%s %s %s: plain %s, stream %s", v, k.Case, k.Field, p.Verdict, s.Verdict)
			}
		}
		if both < 250 {
			t.Errorf("%s: only %d covers were sent both ways", v, both)
		}
	}
}

func TestEnvoyRingHashDeliversEveryBodyAsItsBytes(t *testing.T) {
	a, b := load(t, "envoy-1.40.2-ring_hash"), load(t, "envoy-1.33.2-ring_hash")
	workers := map[string]int{}
	for key, x := range a.Variants {
		if x.Arrivals != 1 || !x.Identical {
			t.Errorf("1.40.2 %s: arrivals %d, identical %v", key, x.Arrivals, x.Identical)
		}
		y := b.Variants[key]
		if y.Arrivals != 1 || !y.Identical {
			t.Errorf("1.33.2 %s: arrivals %d, identical %v", key, y.Arrivals, y.Identical)
		}
		// Placement is a function of the request: both releases put every variant on the same worker.
		if x.Worker != y.Worker {
			t.Errorf("%s: worker %s on 1.40.2, %s on 1.33.2", key, x.Worker, y.Worker)
		}
		workers[x.Worker]++
	}
	if len(a.Variants) != 440 || len(workers) != 2 {
		t.Errorf("%d variants over workers %v", len(a.Variants), workers)
	}
}

func TestLlmdForwardsWhatItAcceptsAsBytes(t *testing.T) {
	r := load(t, "llm-d-epp-envoy-1.33.2")
	var refused []string
	delivered := 0
	for key, x := range r.Variants {
		switch {
		case x.Arrivals == 0:
			if x.Status != 400 || !strings.Contains(x.Response, "must have valid messages field") {
				t.Errorf("%s: status %d, %q", key, x.Status, x.Response)
			}
			refused = append(refused, key)
		case x.Arrivals != 1 || !x.Identical:
			t.Errorf("%s: arrivals %d, identical %v", key, x.Arrivals, x.Identical)
		default:
			delivered++
		}
	}
	sort.Strings(refused)
	var want []string
	for _, c := range []string{"chat/msg/user-part-bare-string", "chat/msg/user-part-simple-audio", "chat/msg/user-part-simple-image", "chat/msg/user-part-simple-video"} {
		want = append(want, c+"#plain", c+"#stream")
	}
	if !reflect.DeepEqual(refused, want) || delivered != 432 {
		t.Errorf("refused %v, delivered %d; want %v and 432", refused, delivered, want)
	}
}

// Envoy and the EPP pass the encodings vllm-router changes: escaped keys and duplicate members.
func TestProxiesKeepKeysAndDuplicates(t *testing.T) {
	for _, name := range []string{"envoy-1.40.2-ring_hash", "llm-d-epp-envoy-1.33.2"} {
		r := load(t, name)
		for _, c := range []string{"chat/enc/key-escaped-top", "chat/enc/key-escaped-nested", "chat/enc/duplicate-known-field",
			"chat/enc/duplicate-in-dict", "chat/enc/duplicate-unknown-top", "chat/msg/harmony"} {
			for _, v := range []string{"#plain", "#stream"} {
				if x := r.Variants[c+v]; x == nil || !x.Identical {
					t.Errorf("%s %s%s: not its bytes", name, c, v)
				}
			}
		}
	}
}

// The case vLLM refuses on purpose is marked, and no other is.
func TestOnlyTheMarkedCaseIsInvalid(t *testing.T) {
	r := load(t, "direct-control")
	for key, x := range r.Variants {
		if (x.Invalid != "") != strings.HasPrefix(key, "chat/enc/message-name-null#") {
			t.Errorf("%s: invalid %q", key, x.Invalid)
		}
	}
}

var allRuns = []string{"direct-control", "vllm-router-0.1.16-consistent_hash", "vllm-router-0.1.16-round_robin",
	"vllm-router-0.1.17-consistent_hash", "vllm-router-0.1.17-round_robin", "vllm-router-0.1.17-kv_aware",
	"envoy-1.40.2-ring_hash", "envoy-1.33.2-ring_hash", "llm-d-epp-envoy-1.33.2"}

// Every delivered variant reached a worker once (no retry), as application/json: vLLM refuses a
// body of another type, which the recorder would accept.
func TestOneArrivalAsJSON(t *testing.T) {
	for _, name := range allRuns {
		for key, x := range load(t, name).Variants {
			if x.Arrivals > 0 && (x.Arrivals != 1 || x.WorkerContentType != "application/json") {
				t.Errorf("%s %s: arrivals %d, Content-Type %q", name, key, x.Arrivals, x.WorkerContentType)
			}
		}
	}
}

// The transport checks: chunked request bodies, and streams held after their first text chunk.
func TestTransport(t *testing.T) {
	for _, name := range allRuns {
		r := load(t, name)
		passThrough := !strings.HasPrefix(name, "vllm-router")
		chunked, heldStreams := 0, 0
		for _, c := range r.Transport {
			if c.Status != 200 || c.Arrivals != 1 || c.Error != "" || c.WorkerContentType != "application/json" && c.Name == "chunked request body" {
				t.Errorf("%s %s: status %d, arrivals %d, error %q, Content-Type %q", name, c.Key, c.Status, c.Arrivals, c.Error, c.WorkerContentType)
			}
			switch c.Name {
			case "chunked request body":
				chunked++
				// Envoy, the EPP and the control forward the client's chunked bytes as they came.
				if passThrough && (!c.Identical || c.WorkerContentLength != -1) {
					t.Errorf("%s %s: identical %v, worker Content-Length %d", name, c.Key, c.Identical, c.WorkerContentLength)
				}
			case "stream held after its first chunk":
				heldStreams++
				// Every router passes a stream through: the client read the first chunk during the hold.
				if !c.StreamedThrough || !c.Held || c.TimedOut {
					t.Errorf("%s %s: streamed through %v, held %v, timed out %v", name, c.Key, c.StreamedThrough, c.Held, c.TimedOut)
				}
			}
		}
		if chunked != 4 || heldStreams != 2 {
			t.Errorf("%s: %d chunked and %d held checks, want 4 and 2", name, chunked, heldStreams)
		}
	}
}

// A record whose identical_bytes flag disagrees with its bytes is refused, so no claim rests on the flag.
func TestLoadRejectsAFlagTheBytesContradict(t *testing.T) {
	raw, err := os.ReadFile("envoy-1.40.2-ring_hash/results.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[1]), &rec); err != nil {
		t.Fatal(err)
	}
	if rec["identical_bytes"] != true {
		t.Fatalf("the first record is not identical: %v", rec["identical_bytes"])
	}
	rec["received"] = rec["received"].(string) + " " // other bytes, flag left true
	b, _ := json.Marshal(rec)
	lines[1] = string(b)
	path := filepath.Join(t.TempDir(), "results.jsonl")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "identical_bytes") {
		t.Fatalf("Load accepted a contradicted flag: %v", err)
	}
}
