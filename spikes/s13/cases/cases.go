// Package cases holds the request bodies the harness sends, as the literal bytes that go on the
// wire. Nothing here is marshalled: a body is a string, and what reaches the worker is compared
// with that string.
//
// A case sets one field (or one small group that vLLM needs together) on a minimal request, so a
// router that refuses or drops one field cannot hide what it does to another. Each case says which
// enumerated fields it covers (fields/vllm-v0.32.0.tsv) and where each sits in its body; the tests
// check that every enumerated field is covered and that every cover points into its body.
package cases

import "strings"

// Endpoint is the path a case is sent to.
type Endpoint string

const (
	Chat        Endpoint = "/v1/chat/completions"
	Completions Endpoint = "/v1/completions"
)

// Cover ties one field of the enumeration (its json_path column), or an extension field named
// "ext:...", to the path where this case's body sets it.
type Cover struct {
	Field string
	At    string
}

// Case is one request body.
type Case struct {
	ID       string
	Endpoint Endpoint
	Body     string
	Covers   []Cover
	// Stream says how the case is sent: "both" (as written, and with "stream":true added after the
	// opening brace), "only" (the body streams already), or "never" (the body sets stream itself).
	Stream string
	Note   string
}

// Variant is one body as sent: a case, streamed or not.
type Variant struct {
	Case   *Case
	Stream bool
	Body   []byte
}

// Key names a variant in the recorder's header and in the results.
func (v Variant) Key() string {
	if v.Stream {
		return v.Case.Key() + "#stream"
	}
	return v.Case.Key() + "#plain"
}

// Variants returns the bodies to send for c. The streamed body of a "both" case is the plain body
// with `"stream":true,` inserted after its first byte '{': a byte edit, not a re-encoding.
func (c *Case) Variants() []Variant {
	switch c.Stream {
	case "only":
		return []Variant{{Case: c, Stream: true, Body: []byte(c.Body)}}
	case "never":
		return []Variant{{Case: c, Stream: false, Body: []byte(c.Body)}}
	}
	i := strings.IndexByte(c.Body, '{')
	streamed := c.Body[:i+1] + `"stream":true,` + c.Body[i+1:]
	return []Variant{{Case: c, Stream: false, Body: []byte(c.Body)}, {Case: c, Stream: true, Body: []byte(streamed)}}
}

const (
	model = `"model":"s13-model"`
	user  = `{"role":"user","content":"hello"}`
)

func chatBody(extra string) string {
	return `{` + model + `,"messages":[` + user + `],` + extra + `}`
}

func msgBody(list string) string { return `{` + model + `,"messages":[` + list + `]}` }

func complBody(extra string) string { return `{` + model + `,"prompt":"hello",` + extra + `}` }

func top(fields ...string) []Cover {
	var cs []Cover
	for _, f := range fields {
		cs = append(cs, Cover{f, f})
	}
	return cs
}

func at(pairs ...string) []Cover {
	var cs []Cover
	for i := 0; i+1 < len(pairs); i += 2 {
		cs = append(cs, Cover{pairs[i], pairs[i+1]})
	}
	return cs
}

// shared sets a field that both endpoints accept; each becomes one chat and one completions case.
type shared struct {
	name   string
	extra  string
	covers []Cover
	stream string
}

const tool = `{"type":"function","function":{"name":"get_weather","description":"Weather for a city","parameters":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}}`

var sharedFields = []shared{
	{"frequency_penalty", `"frequency_penalty":0.1`, top("frequency_penalty"), ""},
	{"logit_bias", `"logit_bias":{"50256":-100,"198":2.5}`, top("logit_bias"), ""},
	{"max_tokens", `"max_tokens":32`, top("max_tokens"), ""},
	{"n", `"n":2`, top("n"), ""},
	{"presence_penalty", `"presence_penalty":-0.3`, top("presence_penalty"), ""},
	{"seed-int64-max", `"seed":9223372036854775807`, top("seed"), ""},
	{"seed-negative-beyond-2^53", `"seed":-9007199254740993`, top("seed"), ""},
	{"stop-string", `"stop":"END"`, top("stop"), ""},
	{"stop-array", `"stop":["END","STOP"]`, top("stop"), ""},
	{"stream-false", `"stream":false`, top("stream"), "never"},
	{"stream-true", `"stream":true`, top("stream"), "only"},
	{"stream_options", `"stream":true,"stream_options":{"include_usage":true,"continuous_usage_stats":true}`,
		top("stream_options", "stream_options.include_usage", "stream_options.continuous_usage_stats"), "only"},
	{"temperature", `"temperature":0.3`, top("temperature"), ""},
	{"top_p", `"top_p":0.9`, top("top_p"), ""},
	{"user", `"user":"s13-user"`, top("user"), ""},
	{"use_beam_search", `"use_beam_search":true`, top("use_beam_search"), ""},
	{"top_k", `"top_k":40`, top("top_k"), ""},
	{"min_p", `"min_p":0.05`, top("min_p"), ""},
	{"repetition_penalty", `"repetition_penalty":1.1`, top("repetition_penalty"), ""},
	{"watermarking", `"watermarking":true`, top("watermarking"), ""},
	{"length_penalty", `"length_penalty":0.8`, top("length_penalty"), ""},
	{"stop_token_ids", `"stop_token_ids":[7,8]`, top("stop_token_ids"), ""},
	{"include_stop_str_in_output", `"include_stop_str_in_output":true`, top("include_stop_str_in_output"), ""},
	{"ignore_eos", `"ignore_eos":true`, top("ignore_eos"), ""},
	{"min_tokens", `"min_tokens":3`, top("min_tokens"), ""},
	{"skip_special_tokens", `"skip_special_tokens":false`, top("skip_special_tokens"), ""},
	{"spaces_between_special_tokens", `"spaces_between_special_tokens":false`, top("spaces_between_special_tokens"), ""},
	{"truncate_prompt_tokens", `"truncate_prompt_tokens":512`, top("truncate_prompt_tokens"), ""},
	{"truncation_side", `"truncation_side":"left"`, top("truncation_side"), ""},
	{"prompt_logprobs", `"prompt_logprobs":2`, top("prompt_logprobs"), ""},
	{"logprob_token_ids", `"logprob_token_ids":[11,12]`, top("logprob_token_ids"), ""},
	{"allowed_token_ids", `"allowed_token_ids":[1,2,3]`, top("allowed_token_ids"), ""},
	{"bad_words", `"bad_words":["bad"]`, top("bad_words"), ""},
	{"echo", `"echo":true`, top("echo"), ""},
	{"priority", `"priority":5`, top("priority"), ""},
	{"request_id", `"request_id":"s13-request-1"`, top("request_id"), ""},
	{"session_id", `"session_id":"s13-session"`, top("session_id"), ""},
	{"return_tokens_as_token_ids", `"return_tokens_as_token_ids":true`, top("return_tokens_as_token_ids"), ""},
	{"return_token_ids", `"return_token_ids":true`, top("return_token_ids"), ""},
	{"routed_experts_prompt_start", `"routed_experts_prompt_start":4`, top("routed_experts_prompt_start"), ""},
	{"return_token_offsets", `"return_token_offsets":true`, top("return_token_offsets"), ""},
	{"mm_payload", `"mm_payload":"none"`, top("mm_payload"), ""},
	{"return_mm_kwargs", `"return_mm_kwargs":true`, top("return_mm_kwargs"), ""},
	{"cache_salt", `"cache_salt":"s13-salt"`, top("cache_salt"), ""},
	{"kv_transfer_params", `"kv_transfer_params":{"do_remote_prefill":false,"remote_block_ids":[1,2]}`, top("kv_transfer_params"), ""},
	{"ec_transfer_params", `"ec_transfer_params":{"engine":"e1"}`, top("ec_transfer_params"), ""},
	{"vllm_xargs", `"vllm_xargs":{"a":"x","b":3,"c":0.5,"d":["y",4]}`, top("vllm_xargs"), ""},
	{"repetition_detection", `"repetition_detection":{"max_pattern_size":8,"min_pattern_size":2,"min_count":3}`,
		top("repetition_detection", "repetition_detection.max_pattern_size", "repetition_detection.min_pattern_size", "repetition_detection.min_count"), ""},
	{"thinking_token_budget", `"thinking_token_budget":256`, top("thinking_token_budget"), ""},
	{"stream_interval", `"stream_interval":4`, top("stream_interval"), ""},
	{"add_special_tokens", `"add_special_tokens":false`, top("add_special_tokens"), ""},
	// Response formats.
	{"response_format-text", `"response_format":{"type":"text"}`, top("response_format", "response_format.type"), ""},
	{"response_format-json_object", `"response_format":{"type":"json_object"}`, top("response_format.type"), ""},
	{"response_format-json_schema",
		`"response_format":{"type":"json_schema","json_schema":{"name":"s13_schema","description":"A schema","schema":{"type":"object","properties":{"a":{"type":"integer"}},"required":["a"],"additionalProperties":false},"strict":true}}`,
		at("response_format.json_schema", "response_format.json_schema",
			"response_format.json_schema.name", "response_format.json_schema.name",
			"response_format.json_schema.description", "response_format.json_schema.description",
			"response_format.json_schema.json_schema", "response_format.json_schema.schema", // alias "schema"
			"response_format.json_schema.strict", "response_format.json_schema.strict"), ""},
	{"response_format-structural_tag",
		`"response_format":{"type":"structural_tag","format":{"type":"triggered_tags","triggers":["<f>"],"tags":[{"begin":"<f>","content":{"type":"json_schema","json_schema":{"type":"object"}},"end":"</f>"}]}}`,
		at("response_format(structural_tag).type", "response_format.type",
			"response_format(structural_tag).format", "response_format.format"), ""},
	{"response_format-structural_tag-legacy",
		`"response_format":{"type":"structural_tag","structures":[{"begin":"<f>","schema":{"type":"object"},"end":"</f>"}],"triggers":["<f>"]}`,
		at("response_format(structural_tag, legacy).type", "response_format.type",
			"response_format(structural_tag, legacy).structures", "response_format.structures",
			"response_format(structural_tag, legacy).triggers", "response_format.triggers",
			"response_format(structural_tag, legacy).structures[].begin", "response_format.structures[0].begin",
			"response_format(structural_tag, legacy).structures[].structural_tag_schema", "response_format.structures[0].schema", // alias "schema"
			"response_format(structural_tag, legacy).structures[].end", "response_format.structures[0].end"), ""},
	// Structured outputs: vLLM accepts exactly one of json, regex, choice, grammar, json_object,
	// structural_tag per request (sampling_params.py:117-138), so each is its own case.
	{"structured_outputs-json",
		`"structured_outputs":{"json":{"type":"object"},"disable_any_whitespace":true,"disable_additional_properties":true,"whitespace_pattern":"[ ]?"}`,
		top("structured_outputs", "structured_outputs.json", "structured_outputs.disable_any_whitespace",
			"structured_outputs.disable_additional_properties", "structured_outputs.whitespace_pattern"), ""},
	{"structured_outputs-regex", `"structured_outputs":{"regex":"[a-z]+"}`, top("structured_outputs.regex"), ""},
	{"structured_outputs-choice", `"structured_outputs":{"choice":["yes","no"]}`, top("structured_outputs.choice"), ""},
	{"structured_outputs-grammar", `"structured_outputs":{"grammar":"root ::= \"yes\" | \"no\""}`, top("structured_outputs.grammar"), ""},
	{"structured_outputs-json_object", `"structured_outputs":{"json_object":true}`, top("structured_outputs.json_object"), ""},
	{"structured_outputs-structural_tag",
		`"structured_outputs":{"structural_tag":"{\"type\":\"structural_tag\",\"format\":{\"type\":\"const_string\",\"value\":\"x\"}}"}`,
		top("structured_outputs.structural_tag"), ""},
}

// msg is a chat case whose subject is its messages.
type msg struct {
	name, list string
	covers     []Cover
	note       string
}

var messages = []msg{
	{"system-string-name", `{"role":"system","content":"Be brief.","name":"sys1"},` + user,
		at("messages[](system).role", "messages[0].role", "messages[](system).content", "messages[0].content", "messages[](system).name", "messages[0].name"), ""},
	{"system-array-content", `{"role":"system","content":[{"type":"text","text":"Be brief."}]},` + user,
		at("messages[](system).content", "messages[0].content", "messages[].content[](text).type", "messages[0].content[0].type", "messages[].content[](text).text", "messages[0].content[0].text"),
		"upstream vllm-project/router issue 201"},
	{"developer", `{"role":"developer","content":"Be terse.","name":"dev1"},` + user,
		at("messages[](developer).role", "messages[0].role", "messages[](developer).content", "messages[0].content", "messages[](developer).name", "messages[0].name"), ""},
	{"user-name", `{"role":"user","content":"hello","name":"alice"}`,
		at("messages[](user).role", "messages[0].role", "messages[](user).content", "messages[0].content", "messages[](user).name", "messages[0].name"), ""},
	{"user-part-text", `{"role":"user","content":[{"type":"text","text":"hello"}]}`,
		at("messages[](user).content", "messages[0].content", "messages[].content[](text).text", "messages[0].content[0].text"), ""},
	{"user-part-image_url", `{"role":"user","content":[{"type":"text","text":"What is this?"},{"type":"image_url","image_url":{"url":"https://example.com/a.png","detail":"low"}}]}`,
		at("messages[].content[](image_url).type", "messages[0].content[1].type", "messages[].content[](image_url).image_url", "messages[0].content[1].image_url",
			"messages[].content[](image_url).image_url.url", "messages[0].content[1].image_url.url", "messages[].content[](image_url).image_url.detail", "messages[0].content[1].image_url.detail"), ""},
	{"user-part-input_audio", `{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"UklGRg==","format":"wav"}}]}`,
		at("messages[].content[](input_audio).type", "messages[0].content[0].type", "messages[].content[](input_audio).input_audio", "messages[0].content[0].input_audio",
			"messages[].content[](input_audio).input_audio.data", "messages[0].content[0].input_audio.data", "messages[].content[](input_audio).input_audio.format", "messages[0].content[0].input_audio.format"), ""},
	{"user-part-file", `{"role":"user","content":[{"type":"file","file":{"file_data":"JVBERi0=","file_id":"file-s13","filename":"a.pdf"}}]}`,
		at("messages[].content[](file).type", "messages[0].content[0].type", "messages[].content[](file).file", "messages[0].content[0].file",
			"messages[].content[](file).file.file_data", "messages[0].content[0].file.file_data", "messages[].content[](file).file.file_id", "messages[0].content[0].file.file_id",
			"messages[].content[](file).file.filename", "messages[0].content[0].file.filename"), ""},
	{"user-part-audio_url", `{"role":"user","content":[{"type":"audio_url","audio_url":{"url":"https://example.com/a.wav"}}]}`,
		at("messages[].content[](audio_url).type", "messages[0].content[0].type", "messages[].content[](audio_url).audio_url", "messages[0].content[0].audio_url",
			"messages[].content[](audio_url).audio_url.url", "messages[0].content[0].audio_url.url"), ""},
	{"user-part-video_url", `{"role":"user","content":[{"type":"video_url","video_url":{"url":"https://example.com/a.mp4"}}]}`,
		at("messages[].content[](video_url).type", "messages[0].content[0].type", "messages[].content[](video_url).video_url", "messages[0].content[0].video_url",
			"messages[].content[](video_url).video_url.url", "messages[0].content[0].video_url.url"), ""},
	{"user-part-image_embeds", `{"role":"user","content":[{"type":"image_embeds","image_embeds":"AAAA","uuid":"img-1"}]}`,
		at("messages[].content[](image_embeds).type", "messages[0].content[0].type", "messages[].content[](image_embeds).image_embeds", "messages[0].content[0].image_embeds",
			"messages[].content[](image_embeds).uuid", "messages[0].content[0].uuid"), ""},
	{"user-part-audio_embeds", `{"role":"user","content":[{"type":"audio_embeds","audio_embeds":"AAAA","uuid":"aud-1"}]}`,
		at("messages[].content[](audio_embeds).type", "messages[0].content[0].type", "messages[].content[](audio_embeds).audio_embeds", "messages[0].content[0].audio_embeds",
			"messages[].content[](audio_embeds).uuid", "messages[0].content[0].uuid"), ""},
	{"user-part-video_embeds", `{"role":"user","content":[{"type":"video_embeds","video_embeds":"AAAA","uuid":"vid-1"}]}`,
		at("messages[].content[](video_embeds).type", "messages[0].content[0].type", "messages[].content[](video_embeds).video_embeds", "messages[0].content[0].video_embeds",
			"messages[].content[](video_embeds).uuid", "messages[0].content[0].uuid"), ""},
	{"user-part-prompt_embeds", `{"role":"user","content":[{"type":"prompt_embeds","data":"AAAA"}]}`,
		at("messages[].content[](prompt_embeds).type", "messages[0].content[0].type", "messages[].content[](prompt_embeds).data", "messages[0].content[0].data"), ""},
	{"user-part-simple-image", `{"role":"user","content":[{"image_url":"https://example.com/b.png","uuid":"img-2"}]}`,
		at("messages[].content[](simple image).image_url", "messages[0].content[0].image_url", "messages[].content[](simple image).uuid", "messages[0].content[0].uuid"), ""},
	{"user-part-simple-audio", `{"role":"user","content":[{"audio_url":"https://example.com/b.wav"}]}`,
		at("messages[].content[](simple audio).audio_url", "messages[0].content[0].audio_url"), ""},
	{"user-part-simple-video", `{"role":"user","content":[{"video_url":"https://example.com/b.mp4","uuid":"vid-2"}]}`,
		at("messages[].content[](simple video).video_url", "messages[0].content[0].video_url", "messages[].content[](simple video).uuid", "messages[0].content[0].uuid"), ""},
	{"user-part-tool_reference", `{"role":"user","content":[{"type":"tool_reference","name":"get_weather"}]}`,
		at("messages[].content[](tool_reference).type", "messages[0].content[0].type", "messages[].content[](tool_reference).name", "messages[0].content[0].name"), ""},
	{"user-part-bare-string", `{"role":"user","content":["hello",{"type":"text","text":"world"}]}`,
		at("ext:messages[].content[](str)", "messages[0].content[0]"), "a bare string is a member of vLLM's content-part union (renderers/chat_utils.py:356)"},
	{"assistant-content-name", user + `,{"role":"assistant","content":"Hi.","name":"bot"},` + user,
		at("messages[](assistant).role", "messages[1].role", "messages[](assistant).content", "messages[1].content", "messages[](assistant).name", "messages[1].name"), ""},
	{"assistant-part-refusal", user + `,{"role":"assistant","content":[{"type":"refusal","refusal":"I cannot."}]},` + user,
		at("messages[].content[](refusal).type", "messages[1].content[0].type", "messages[].content[](refusal).refusal", "messages[1].content[0].refusal"), ""},
	{"assistant-part-thinking", user + `,{"role":"assistant","content":[{"type":"thinking","thinking":"Let me see.","closed":true},{"type":"text","text":"Hi."}]},` + user,
		at("messages[].content[](thinking).type", "messages[1].content[0].type", "messages[].content[](thinking).thinking", "messages[1].content[0].thinking",
			"messages[].content[](thinking).closed", "messages[1].content[0].closed"), ""},
	{"assistant-refusal", user + `,{"role":"assistant","content":null,"refusal":"No."},` + user,
		at("messages[](assistant).refusal", "messages[1].refusal"), ""},
	{"assistant-audio", user + `,{"role":"assistant","audio":{"id":"audio-s13"}},` + user,
		at("messages[](assistant).audio", "messages[1].audio", "messages[](assistant).audio.id", "messages[1].audio.id"), ""},
	{"assistant-function_call", user + `,{"role":"assistant","content":null,"function_call":{"name":"get_weather","arguments":"{\"city\":\"Krakow\"}"}},{"role":"function","name":"get_weather","content":"sunny"}`,
		at("messages[](assistant).function_call", "messages[1].function_call", "messages[](assistant).function_call.name", "messages[1].function_call.name",
			"messages[](assistant).function_call.arguments", "messages[1].function_call.arguments",
			"messages[](function).role", "messages[2].role", "messages[](function).name", "messages[2].name", "messages[](function).content", "messages[2].content"), ""},
	{"assistant-tool_calls-function", user + `,{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Krakow\"}"}}]},{"role":"tool","tool_call_id":"call_1","content":"sunny"}`,
		at("messages[](assistant).tool_calls", "messages[1].tool_calls",
			"messages[](assistant).tool_calls[](function).id", "messages[1].tool_calls[0].id", "messages[](assistant).tool_calls[](function).type", "messages[1].tool_calls[0].type",
			"messages[](assistant).tool_calls[](function).function", "messages[1].tool_calls[0].function",
			"messages[](assistant).tool_calls[](function).function.name", "messages[1].tool_calls[0].function.name",
			"messages[](assistant).tool_calls[](function).function.arguments", "messages[1].tool_calls[0].function.arguments",
			"messages[](tool).role", "messages[2].role", "messages[](tool).tool_call_id", "messages[2].tool_call_id", "messages[](tool).content", "messages[2].content"), ""},
	{"assistant-tool_calls-custom", user + `,{"role":"assistant","tool_calls":[{"id":"call_2","type":"custom","custom":{"name":"run","input":"ls"}}]},{"role":"tool","tool_call_id":"call_2","content":"a.txt"}`,
		at("messages[](assistant).tool_calls[](custom).id", "messages[1].tool_calls[0].id", "messages[](assistant).tool_calls[](custom).type", "messages[1].tool_calls[0].type",
			"messages[](assistant).tool_calls[](custom).custom", "messages[1].tool_calls[0].custom",
			"messages[](assistant).tool_calls[](custom).custom.name", "messages[1].tool_calls[0].custom.name",
			"messages[](assistant).tool_calls[](custom).custom.input", "messages[1].tool_calls[0].custom.input"), ""},
	{"assistant-reasoning", user + `,{"role":"assistant","content":"Hi.","reasoning":"The user greets."},` + user,
		at("messages[](custom).reasoning", "messages[1].reasoning"), ""},
	{"tool-array-content", user + `,{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{}"}}]},{"role":"tool","tool_call_id":"call_1","content":[{"type":"text","text":"sunny"}]}`,
		at("messages[](tool).content", "messages[2].content"), ""},
	{"custom-role", `{"role":"s13-custom","content":"A note.","name":"c1"},` + user,
		at("messages[](custom).role", "messages[0].role", "messages[](custom).content", "messages[0].content", "messages[](custom).name", "messages[0].name"), ""},
	{"custom-tool_call_id", `{"role":"s13-custom","content":"A note.","tool_call_id":"call_9"},` + user,
		at("messages[](custom).tool_call_id", "messages[0].tool_call_id"), ""},
	{"custom-tool_calls", `{"role":"s13-custom","content":"A note.","tool_calls":[{"id":"call_3","type":"function","function":{"name":"f","arguments":"{}"}}]},` + user,
		at("messages[](custom).tool_calls", "messages[0].tool_calls"), ""},
	{"developer-tools", `{"role":"developer","content":"Use the tools.","tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"}}}]},` + user,
		at("messages[](custom).tools", "messages[0].tools"), ""},
	{"user-task", `{"role":"user","content":"hello","task":"s13-task"}`,
		at("messages[](custom).task", "messages[0].task"), ""},
	{"user-content-array-for-custom", `{"role":"s13-custom","content":[{"type":"text","text":"A note."}]},` + user,
		at("messages[](custom).content", "messages[0].content"), ""},
}

// chatOnly are chat fields that completions does not have.
var chatOnly = []shared{
	{"logprobs-top_logprobs", `"logprobs":true,"top_logprobs":5`, top("logprobs", "top_logprobs"), ""},
	{"max_completion_tokens", `"max_completion_tokens":48`, top("max_completion_tokens"), ""},
	{"tools", `"tools":[` + tool + `]`,
		at("tools", "tools", "tools[].type", "tools[0].type", "tools[].function", "tools[0].function", "tools[].function.name", "tools[0].function.name",
			"tools[].function.description", "tools[0].function.description", "tools[].function.parameters", "tools[0].function.parameters"), ""},
	{"tools-function-strict", `"tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"],"additionalProperties":false},"strict":true}}]`,
		at("tools[].function.strict", "tools[0].function.strict"), ""},
	{"tools-function-defer_loading", `"tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"},"defer_loading":true}}]`,
		at("tools[].function.defer_loading", "tools[0].function.defer_loading"), ""},
	{"tools-defer_loading", `"tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"}},"defer_loading":true}]`,
		at("tools[].defer_loading", "tools[0].defer_loading"), ""},
	{"tools-no-parameters", `"tools":[{"type":"function","function":{"name":"get_time"}}]`,
		at("tools[].function.name", "tools[0].function.name"), ""},
	{"tool_choice-none", `"tools":[` + tool + `],"tool_choice":"none"`, top("tool_choice"), ""},
	{"tool_choice-auto", `"tools":[` + tool + `],"tool_choice":"auto"`, top("tool_choice"), ""},
	{"tool_choice-required", `"tools":[` + tool + `],"tool_choice":"required"`, top("tool_choice"), ""},
	{"tool_choice-named", `"tools":[` + tool + `],"tool_choice":{"type":"function","function":{"name":"get_weather"}}`,
		at("tool_choice", "tool_choice", "tool_choice.type", "tool_choice.type", "tool_choice.function", "tool_choice.function", "tool_choice.function.name", "tool_choice.function.name"), ""},
	{"reasoning_effort-high", `"reasoning_effort":"high"`, top("reasoning_effort"), ""},
	{"reasoning_effort-max", `"reasoning_effort":"max"`, top("reasoning_effort"), ""},
	{"include_reasoning", `"include_reasoning":false`, top("include_reasoning"), ""},
	{"parallel_tool_calls", `"tools":[` + tool + `],"parallel_tool_calls":false`, top("parallel_tool_calls"), ""},
	{"documents", `"documents":[{"title":"Doc","text":"Body."}]`, top("documents"), ""},
	{"chat_template", `"chat_template":"{% for m in messages %}{{ m.content }}{% endfor %}"`, top("chat_template"), ""},
	{"chat_template_kwargs", `"chat_template_kwargs":{"enable_thinking":false,"depth":2}`, top("chat_template_kwargs"), ""},
	{"media_io_kwargs", `"media_io_kwargs":{"video":{"num_frames":8}}`, top("media_io_kwargs"), ""},
	{"mm_processor_kwargs", `"mm_processor_kwargs":{"fps":2}`, top("mm_processor_kwargs"), ""},
	{"return_prompt_text", `"return_prompt_text":true`, top("return_prompt_text"), ""},
}

// complOnly are completions fields that chat does not have, and prompt forms.
var complOnly = []shared{
	{"logprobs", `"logprobs":3`, top("logprobs"), ""},
	{"suffix", `"suffix":"s13-suffix"`, top("suffix"), ""},
	{"prompt_embeds", `"prompt_embeds":"AAAAAA=="`, top("prompt_embeds"), ""},
	{"max_tokens-null", `"max_tokens":null`, top("max_tokens"), ""},
}

// Extension fields: unknown to the router's structs, accepted by vLLM (OpenAIBaseModel has
// extra="allow", serve/engine/protocol.py:39), and the encodings a re-serialiser can change.
var extensions = []Case{
	{ID: "ext/messages-x_ext", Endpoint: Chat, Body: msgBody(`{"role":"user","content":"hello","x_ext":{"trace":"t-1","n":1}}`),
		Covers: at("ext:messages[].x_ext", "messages[0].x_ext"), Note: "an extension object inside messages[]"},
	{ID: "ext/content-part-cache_control", Endpoint: Chat, Body: msgBody(`{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]}`),
		Covers: at("ext:messages[].content[].cache_control", "messages[0].content[0].cache_control"), Note: "the prompt-caching marker some clients add"},
	{ID: "ext/system-part-cache_control", Endpoint: Chat, Body: msgBody(`{"role":"system","content":[{"type":"text","text":"Be brief.","cache_control":{"type":"ephemeral"}}]},` + user),
		Covers: at("ext:messages[](system).content[].cache_control", "messages[0].content[0].cache_control")},
	{ID: "ext/assistant-reasoning_content", Endpoint: Chat, Body: msgBody(user + `,{"role":"assistant","content":"Hi.","reasoning_content":"The user greets."},` + user),
		Covers: at("ext:messages[](assistant).reasoning_content", "messages[1].reasoning_content"), Note: "vLLM's earlier name for reasoning"},
	{ID: "ext/harmony-message", Endpoint: Chat, Body: msgBody(`{"author":{"role":"user"},"content":[{"type":"text","text":"hello"}]}`),
		Covers: at("ext:messages[](harmony)", "messages[0]"), Note: "openai_harmony's Message, the third member of vLLM's message union (renderers/chat_utils.py:393-397); it has no role key"},
	{ID: "ext/tools-function-x_ext", Endpoint: Chat, Body: chatBody(`"tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"},"x_ext":{"a":1}}}]`),
		Covers: at("ext:tools[].function.x_ext", "tools[0].function.x_ext")},
	{ID: "ext/tools-x_ext", Endpoint: Chat, Body: chatBody(`"tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"}},"x_ext":{"a":1}}]`),
		Covers: at("ext:tools[].x_ext", "tools[0].x_ext")},
	{ID: "ext/tool_calls-function-x_ext", Endpoint: Chat, Body: msgBody(user + `,{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{}","x_ext":1}}]},{"role":"tool","tool_call_id":"call_1","content":"sunny"}`),
		Covers: at("ext:messages[].tool_calls[].function.x_ext", "messages[1].tool_calls[0].function.x_ext")},
	{ID: "ext/tool_choice-x_ext", Endpoint: Chat, Body: chatBody(`"tools":[` + tool + `],"tool_choice":{"type":"function","function":{"name":"get_weather"},"x_ext":1}`),
		Covers: at("ext:tool_choice.x_ext", "tool_choice.x_ext")},
	{ID: "ext/response_format-json_schema-x_ext", Endpoint: Chat, Body: chatBody(`"response_format":{"type":"json_schema","json_schema":{"name":"s","schema":{"type":"object"},"x_ext":1}}`),
		Covers: at("ext:response_format.json_schema.x_ext", "response_format.json_schema.x_ext")},
	{ID: "ext/stream_options-x_ext", Endpoint: Chat, Stream: "only", Body: chatBody(`"stream":true,"stream_options":{"include_usage":true,"x_ext":1}`),
		Covers: at("ext:stream_options.x_ext", "stream_options.x_ext")},
	{ID: "ext/structured_outputs-x_ext", Endpoint: Chat, Body: chatBody(`"structured_outputs":{"regex":"[a-z]+","x_ext":1}`),
		Covers: at("ext:structured_outputs.x_ext", "structured_outputs.x_ext")},
	{ID: "ext/top-level-object", Endpoint: Chat, Body: chatBody(`"x_top":{"a":[1,2.50,"s"],"b":null}`),
		Covers: at("ext:x_top", "x_top"), Note: "an extra_body-style top-level extra"},
	{ID: "ext/top-level-object", Endpoint: Completions, Body: complBody(`"x_top":{"a":[1,2.50,"s"],"b":null}`),
		Covers: at("ext:x_top", "x_top"), Note: "an extra_body-style top-level extra"},
	{ID: "ext/extra_body-literal", Endpoint: Chat, Body: chatBody(`"extra_body":{"top_k":5}`),
		Covers: at("ext:extra_body", "extra_body"), Note: "a client that sends its extra_body as a key"},
	{ID: "ext/guided_json", Endpoint: Chat, Body: chatBody(`"guided_json":{"type":"object"}`),
		Covers: at("ext:guided_json", "guided_json"), Note: "a guided-decoding field vLLM removed and warns about (serve/engine/protocol.py:63-68)"},
	{ID: "ext/openai-sdk-fields", Endpoint: Chat, Body: chatBody(`"store":false,"metadata":{"k":"v"},"prompt_cache_key":"pc-1","service_tier":"auto","verbosity":"low","modalities":["text"]`),
		Covers: at("ext:store", "store", "ext:metadata", "metadata", "ext:prompt_cache_key", "prompt_cache_key", "ext:service_tier", "service_tier", "ext:verbosity", "verbosity", "ext:modalities", "modalities"),
		Note:   "OpenAI request fields vLLM v0.32.0 does not declare"},
	{ID: "ext/session_params", Endpoint: Chat, Body: chatBody(`"session_params":{"session_id":"s13-session"}`),
		Covers: at("ext:session_params", "session_params"), Note: "the router's own routing key (HBEP-5)"},
}

// Encodings: the same values written in ways a re-serialiser can change.
var encodings = []Case{
	{ID: "enc/pretty-printed-reordered", Endpoint: Chat, Body: "{\n  \"messages\": [ {\"content\": \"hello\", \"role\": \"user\"} ],\n  \"temperature\": 0.3,\n  \"model\": \"s13-model\"\n}",
		Covers: at("ext:whitespace and member order", "temperature")},
	{ID: "enc/temperature-integer", Endpoint: Chat, Body: chatBody(`"temperature":1`), Covers: at("ext:temperature as an integer literal", "temperature")},
	{ID: "enc/temperature-integer", Endpoint: Completions, Body: complBody(`"temperature":1`), Covers: at("ext:temperature as an integer literal", "temperature")},
	{ID: "enc/top_p-exponent", Endpoint: Chat, Body: chatBody(`"top_p":9e-1`), Covers: at("ext:top_p in exponent form", "top_p")},
	{ID: "enc/temperature-20-digits", Endpoint: Chat, Body: chatBody(`"temperature":0.12345678901234567890`), Covers: at("ext:a float beyond float64 precision", "temperature")},
	{ID: "enc/repetition_penalty-1.0", Endpoint: Chat, Body: chatBody(`"repetition_penalty":1.0`), Covers: at("ext:a float with a zero fraction", "repetition_penalty")},
	{ID: "enc/kwargs-numbers", Endpoint: Chat, Body: chatBody(`"chat_template_kwargs":{"big":123456789012345678901234567890,"u64":18446744073709551615,"i":1,"f":1.0,"neg":-0.0}`),
		Covers: at("ext:numbers inside a dict[str, Any]", "chat_template_kwargs")},
	{ID: "enc/explicit-nulls", Endpoint: Chat, Body: chatBody(`"max_tokens":null,"temperature":null,"stop":null,"user":null,"seed":null,"tools":null`),
		Covers: at("ext:max_tokens: null", "max_tokens", "ext:temperature: null", "temperature", "ext:stop: null", "stop", "ext:user: null", "user", "ext:seed: null", "seed", "ext:tools: null", "tools")},
	{ID: "enc/message-name-null", Endpoint: Chat, Body: msgBody(`{"role":"user","content":"hello","name":null}`), Covers: at("ext:messages[].name: null", "messages[0].name")},
}

// escapeCase is built at run time from a backslash, so the source holds no escape sequence an
// editor could decode into the character it stands for.
func escapeCases() []Case {
	b := string(rune(92))
	content := `"caf` + b + `u00e9 ` + b + `ud83d` + b + `ude00 ` + b + `/ <&> ` + b + `u0001 ` + b + `t"`
	return []Case{
		{ID: "enc/string-escapes", Endpoint: Chat, Body: msgBody(`{"role":"user","content":` + content + `}`),
			Covers: at("ext:string escapes (an escaped e-acute, a surrogate pair, an escaped slash, a control character)", "messages[0].content")},
		{ID: "enc/string-escapes", Endpoint: Completions, Body: `{` + model + `,"prompt":` + content + `}`,
			Covers: at("ext:string escapes (an escaped e-acute, a surrogate pair, an escaped slash, a control character)", "prompt")},
	}
}

// A request shaped like an agent's turn through the OpenAI SDK: strict tools, a tool-call round trip,
// content parts, streaming with usage.
var realistic = []Case{
	{ID: "real/agent-turn", Endpoint: Chat, Stream: "only", Body: `{` + model + `,"messages":[` +
		`{"role":"system","content":"You are a coding agent."},` +
		`{"role":"user","content":[{"type":"text","text":"Read a.txt."}]},` +
		`{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"read_file","arguments":"{\"path\":\"a.txt\"}"}}]},` +
		`{"role":"tool","tool_call_id":"call_1","content":"hello"},` +
		`{"role":"user","content":"Summarise it."}],` +
		`"tools":[{"type":"function","function":{"name":"read_file","description":"Read a file","parameters":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"],"additionalProperties":false},"strict":true}}],` +
		`"tool_choice":"auto","parallel_tool_calls":false,"temperature":0,"max_completion_tokens":1024,"stream":true,"stream_options":{"include_usage":true}}`,
		Covers: at("ext:agent turn: tools[].function.strict", "tools[0].function.strict", "ext:agent turn: temperature 0", "temperature",
			"ext:agent turn: system string content", "messages[0].content", "ext:agent turn: user content parts", "messages[1].content")},
}

// Minimal requests: anything that arrives beyond them was added on the way.
var minimal = []Case{
	{ID: "minimal", Endpoint: Chat, Body: msgBody(user), Covers: at("model", "model", "messages", "messages",
		"messages[](user).role", "messages[0].role", "messages[](user).content", "messages[0].content")},
	{ID: "minimal", Endpoint: Completions, Body: `{` + model + `,"prompt":"hello"}`, Covers: top("model", "prompt")},
	{ID: "prompt-list-of-strings", Endpoint: Completions, Body: `{` + model + `,"prompt":["hello","world"]}`, Covers: top("prompt")},
	{ID: "prompt-token-ids", Endpoint: Completions, Body: `{` + model + `,"prompt":[1,2,3]}`, Covers: top("prompt")},
	{ID: "prompt-token-id-lists", Endpoint: Completions, Body: `{` + model + `,"prompt":[[1,2],[3,4]]}`, Covers: top("prompt")},
}

// notes explain the cases whose reason is not their name.
var notes = map[string]string{
	"chat/reasoning_effort-max":     "a vLLM value outside OpenAI's set (chat_completion/protocol.py:252-263)",
	"completions/prompt_embeds":     "bytes, sent as base64 text",
	"completions/max_tokens-null":   "vLLM: null is the rest of the context, absent is 16 (completion/protocol.py:63)",
	"chat/msg/system-array-content": "upstream vllm-project/router issue 201",
}

// special are chat cases that need more than one message.
var special = []Case{
	{ID: "continue_final_message", Endpoint: Chat,
		Body:   `{` + model + `,"messages":[` + user + `,{"role":"assistant","content":"The answer is"}],"add_generation_prompt":false,"continue_final_message":true}`,
		Covers: top("add_generation_prompt", "continue_final_message"),
		Note:   "vLLM refuses continue_final_message with add_generation_prompt true (chat_completion/protocol.py)"},
}

// Excluded lists the enumerated fields no case sets, with the reason.
var Excluded = map[string]string{
	"messages[].content[](image_pil).image_pil": "a PIL image object (renderers/chat_utils.py:226-238); JSON cannot carry one",
	"messages[].content[](image_pil).uuid":      "belongs to the PIL image part, which JSON cannot carry",
}

// All returns every case, chat first, each ID once per endpoint.
func All() []*Case {
	var out []*Case
	add := func(c Case) {
		if c.Stream == "" {
			c.Stream = "both"
		}
		if n, ok := notes[c.Key()]; ok && c.Note == "" {
			c.Note = n
		}
		out = append(out, &c)
	}
	for _, c := range minimal {
		add(c)
	}
	for _, s := range sharedFields {
		add(Case{ID: s.name, Endpoint: Chat, Body: chatBody(s.extra), Covers: s.covers, Stream: s.stream})
	}
	for _, s := range chatOnly {
		add(Case{ID: s.name, Endpoint: Chat, Body: chatBody(s.extra), Covers: s.covers, Stream: s.stream})
	}
	for _, m := range messages {
		add(Case{ID: "msg/" + m.name, Endpoint: Chat, Body: msgBody(m.list), Covers: m.covers, Note: m.note})
	}
	for _, s := range sharedFields {
		add(Case{ID: s.name, Endpoint: Completions, Body: complBody(s.extra), Covers: s.covers, Stream: s.stream})
	}
	for _, s := range complOnly {
		add(Case{ID: s.name, Endpoint: Completions, Body: complBody(s.extra), Covers: s.covers, Stream: s.stream})
	}
	for _, cs := range [][]Case{special, extensions, encodings, escapeCases(), realistic} {
		for _, c := range cs {
			add(c)
		}
	}
	return out
}

// Key names a case on its endpoint.
func (c *Case) Key() string {
	if c.Endpoint == Chat {
		return "chat/" + c.ID
	}
	return "completions/" + c.ID
}
