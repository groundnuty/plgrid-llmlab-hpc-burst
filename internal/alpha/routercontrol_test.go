package alpha

// The direct-to-engine control's comparison (routercontrol.go), on bodies of vLLM v0.32.0's shapes written here by hand: invented ids,
// texts and counts, never a live run's. Each rule has a case that differs on it alone, and each set-aside field a case that differs on it
// alone and agrees.

import (
	"strings"
	"testing"
)

const (
	chatOK = `{"id":"chatcmpl-aaa","object":"chat.completion","created":1700000001,"model":"m","choices":[{"index":0,` +
		`"message":{"role":"assistant","content":"Hello there","reasoning":null,"tool_calls":[]},"logprobs":null,"finish_reason":"length","stop_reason":null}],` +
		`"usage":{"prompt_tokens":12,"total_tokens":20,"completion_tokens":8,"prompt_tokens_details":null},"system_fingerprint":"fp-1"}`
	chatOK2 = `{"id":"chatcmpl-bbb","object":"chat.completion","created":1700000002,"model":"m","choices":[{"index":0,` +
		`"message":{"role":"assistant","content":null,"reasoning":"hmm","tool_calls":[{"id":"call-1","type":"function","function":{"name":"f","arguments":"{}"}}]},"logprobs":null,"finish_reason":"tool_calls","stop_reason":null}],` +
		`"usage":{"prompt_tokens":12,"total_tokens":15,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":8}},"system_fingerprint":"fp-2"}`
	refusal  = `{"error":{"message":"\"auto\" tool choice requires --enable-auto-tool-choice and --tool-call-parser to be set","type":"BadRequestError","param":null,"code":400}}`
	refusal2 = `{"error":{"message":"beam search is not supported here","type":"BadRequestError","param":null,"code":400}}`
	stream1  = "data: {\"id\":\"c-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"c-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"c-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" you\"},\"finish_reason\":\"length\"}]}\n\n" +
		"data: {\"id\":\"c-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[],\"usage\":{\"prompt_tokens\":9,\"total_tokens\":11,\"completion_tokens\":2}}\n\n" +
		"data: [DONE]\n\n"
	streamErr = "data: {\"id\":\"c-2\",\"object\":\"chat.completion.chunk\",\"created\":2,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"error\":{\"message\":\"the reasoning parser is not set\",\"type\":\"BadRequestError\",\"param\":null,\"code\":400}}\n\n" +
		"data: [DONE]\n\n"
)

func ok(body string) Reply            { return Reply{Status: 200, Body: []byte(body)} }
func status(s int, body string) Reply { return Reply{Status: s, Body: []byte(body)} }

func TestTheControlComparesWhatIsTheRequestsAndSetsAsideWhatIsTheSamplers(t *testing.T) {
	stream2 := strings.NewReplacer(`"Hi"`, `"Yo"`, `" you"`, `" all"`, `"c-1"`, `"c-9"`, `"created":1`, `"created":7`, `"completion_tokens":2`, `"completion_tokens":5`).Replace(stream1)
	for _, c := range []struct {
		name          string
		front, direct Reply
		stream        bool
		want          string // "" for agreement, else a substring of the difference
	}{
		// Agreement.
		{"the engine refuses both alike: the deployment's refusal, not the router's", status(400, refusal), status(400, refusal), false, ""},
		{"text, ids, created, the fingerprint, finish_reason, tool calls and the completion's counts are the sampler's", ok(chatOK), ok(chatOK2), false, ""},
		{"a stream with other token text and ids, the same events", ok(stream1), ok(stream2), true, ""},
		{"a stream that the engine ends with the same error event on both", ok(streamErr), ok(streamErr), true, ""},
		// Differences, one rule at a time.
		{"a status", status(400, refusal), ok(chatOK), false, "status 400 through the router, 200 straight to the engine"},
		{"a 502 the path made", status(502, "upstream connect error"), status(400, refusal), false, "status 502 through the router, 400"},
		{"the error's text", status(400, refusal), status(400, refusal2), false, "the error differs"},
		{"usage.prompt_tokens: the router changed the prompt", ok(chatOK), ok(strings.Replace(chatOK, `"prompt_tokens":12`, `"prompt_tokens":13`, 1)), false, "usage.prompt_tokens 12 through the router, 13 straight to the engine"},
		{"a key the router added", ok(strings.Replace(chatOK, `"model":"m",`, `"model":"m","x_router":true,`, 1)), ok(chatOK), false, "the shape differs"},
		{"a key's type", ok(strings.Replace(chatOK, `"logprobs":null`, `"logprobs":{"content":[]}`, 1)), ok(chatOK), false, "the shape differs"},
		{"the number of choices", ok(strings.Replace(chatOK, `}],"usage"`, `},{"index":1,"message":{"role":"assistant","content":"x"},"logprobs":null,"finish_reason":"stop","stop_reason":null}],"usage"`, 1)), ok(chatOK), false, "the choices differ: 2 through the router, 1"},
		{"the model", ok(strings.Replace(chatOK, `"model":"m"`, `"model":"other"`, 1)), ok(chatOK), false, "object or model differ"},
		{"a body that is not JSON", status(200, "<html>"), ok(chatOK), false, "ended not json through the router, json"},
		{"cut through the router", Reply{Status: 200, Body: []byte(chatOK[:40]), Cut: true}, ok(chatOK), false, "ended cut through the router, json"},
		{"no answer straight to the engine", ok(chatOK), Reply{}, false, "status 200 through the router, 0 straight to the engine"},
		{"a stream that ended with [DONE] on one path and an error event on the other", ok(stream1), ok(streamErr), true, "ended done through the router, error"},
		{"a stream that never finished", ok(strings.TrimSuffix(stream1, "data: [DONE]\n\n")), ok(stream1), true, "ended unfinished through the router, done"},
		{"a stream's prompt tokens", ok(strings.Replace(stream1, `"prompt_tokens":9`, `"prompt_tokens":10`, 1)), ok(stream1), true, "usage.prompt_tokens 10 through the router, 9"},
		{"a stream without its usage chunk", ok(strings.Replace(stream1, "data: {\"id\":\"c-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[],\"usage\":{\"prompt_tokens\":9,\"total_tokens\":11,\"completion_tokens\":2}}\n\n", "", 1)), ok(stream1), true, "usage.prompt_tokens none through the router, 9"},
		{"a stream's role chunk", ok(strings.Replace(stream1, `"role":"assistant",`, ``, 1)), ok(stream1), true, "role false"},
		{"a stream's error text", ok(streamErr), ok(strings.Replace(streamErr, "parser is not set", "parser is missing", 1)), true, "the error differs"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := CompareReplies(c.front, c.direct, c.stream)
			if c.want == "" && got != "" {
				t.Fatalf("CompareReplies = %q, want agreement", got)
			}
			if c.want != "" && !strings.Contains(got, c.want) {
				t.Fatalf("CompareReplies = %q, want it to say %q", got, c.want)
			}
		})
	}
}

// The post names a refusal by its type and message, clipped to a line.
func TestErrorTextIsTheTypeAndTheMessageClipped(t *testing.T) {
	if got := ErrorText(status(400, refusal2), false); got != "BadRequestError: beam search is not supported here" {
		t.Errorf("ErrorText = %q", got)
	}
	if got := ErrorText(ok(streamErr), true); got != "BadRequestError: the reasoning parser is not set" {
		t.Errorf("a stream's ErrorText = %q", got)
	}
	if got := ErrorText(ok(chatOK), false); got != "" {
		t.Errorf("a 200's ErrorText = %q, want none", got)
	}
	long := status(400, `{"error":{"message":"`+strings.Repeat("é", 200)+`","type":"BadRequestError","param":null,"code":400}}`)
	if got := ErrorText(long, false); len(got) > errorTextMax+len("…") || !strings.HasSuffix(got, "…") || !strings.HasPrefix(got, "BadRequestError: é") {
		t.Errorf("a long error is %d bytes: %q", len(got), got)
	}
}
