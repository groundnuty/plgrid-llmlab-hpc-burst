package alpha

// The router row's direct-to-engine control (the science agent's ruling (a) on the router row, #522, comment 6874666948): each of S13's
// variants is sent twice, through the front and Envoy, and straight to the worker's engine socket on the same node, and the row fails only
// where the two answers differ. Which valid variants vLLM refuses depends on the deployment's flags and model, so a refusal is the
// deployment's when the engine gives it on both paths, and the router's only when the paths disagree. This file is the comparison; the
// live flow reaches the socket (betarouter_flow_test.go, directReading).
//
// What is compared, and why:
//   - the status, always;
//   - an error (a JSON body with "error", or a stream's error event): its message, type, param and code, whole. vLLM's refusals are
//     deterministic for one body, and they are what the control is about;
//   - a 200 JSON body: its shape (each key and its JSON type, at every level, arrays by the union of their elements), its object, model and
//     number of choices, and usage.prompt_tokens: a body the router changed changes the prompt's tokens, and nothing else of the answer
//     is both deterministic and that close to the request;
//   - a 200 stream: how it ended ([DONE], an error event with its error, or cut) and the shape of its events (their objects, a role chunk,
//     a usage chunk and its prompt_tokens, the choice indexes, the events' keys), never the token text.
//
// Set aside, by name (setAside): what differs between two sends of one body without the router doing anything. The per-request fields:
// id, created, system_fingerprint, and the token-id fields a request asks to be returned. And what the sampler decides: the generated text
// and reasoning (text, content, reasoning, reasoning_content), the tool calls and function call it made, finish_reason and stop_reason, and
// the completion's counts (completion_tokens, total_tokens and both token-details objects, whose cached count follows the prefix cache).
// S13's variants sample, and vLLM at temperature 0 is not batch-invariant either, so no token text is compared, even at temperature 0.
// Headers are not compared at all: the front adds its attribution headers, and Envoy its own.

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Reply is one path's answer to a variant: the status (0 for no answer), the body, and whether it was cut short.
type Reply struct {
	Status int
	Body   []byte
	Cut    bool
}

// setAside are the keys whose values, and whose presence, the comparison ignores wherever they occur (see the file's comment).
var setAside = map[string]bool{
	"id": true, "created": true, "system_fingerprint": true, "prompt_token_ids": true, "token_ids": true,
	"text": true, "content": true, "reasoning": true, "reasoning_content": true, "tool_calls": true, "function_call": true,
	"finish_reason": true, "stop_reason": true,
	"completion_tokens": true, "total_tokens": true, "completion_tokens_details": true, "prompt_tokens_details": true,
}

// replyView is what of a reply the comparison reads.
type replyView struct {
	status   int
	ending   string // "json", "done", "error", "cut", "unfinished", "not json"
	err      string // the error, message|type|param|code, for an error body or a stream's error event
	shape    string // the body's or the events' shape, set-aside keys removed
	object   string
	model    string
	choices  string // the choices' count of a body, the choice indexes of a stream
	prompt   string // usage.prompt_tokens, "" when the reply has none
	roleSeen bool
	usage    bool
}

// CompareReplies is "" when the front's and the engine's answers to one variant agree, else what differs, in a few words naming both.
func CompareReplies(front, direct Reply, stream bool) string {
	f, d := viewReply(front, stream), viewReply(direct, stream)
	switch {
	case f.status != d.status:
		return fmt.Sprintf("status %d through the router, %d straight to the engine", f.status, d.status)
	case f.ending != d.ending:
		return fmt.Sprintf("the answer ended %s through the router, %s straight to the engine", f.ending, d.ending)
	case f.err != d.err:
		return fmt.Sprintf("the error differs: %q through the router, %q straight to the engine", clipText(f.err), clipText(d.err))
	case f.object != d.object || f.model != d.model:
		return fmt.Sprintf("object or model differ: %s %s through the router, %s %s straight to the engine", f.object, f.model, d.object, d.model)
	case f.choices != d.choices:
		return fmt.Sprintf("the choices differ: %s through the router, %s straight to the engine", f.choices, d.choices)
	case f.prompt != d.prompt:
		return fmt.Sprintf("usage.prompt_tokens %s through the router, %s straight to the engine", orNone(f.prompt), orNone(d.prompt))
	case f.roleSeen != d.roleSeen || f.usage != d.usage:
		return fmt.Sprintf("the stream's role or usage chunk differs: role %t, usage %t through the router; role %t, usage %t straight to the engine",
			f.roleSeen, f.usage, d.roleSeen, d.usage)
	case f.shape != d.shape:
		return fmt.Sprintf("the shape differs: %s through the router, %s straight to the engine", clipText(f.shape), clipText(d.shape))
	}
	return ""
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

// viewReply reads a reply for the comparison.
func viewReply(r Reply, stream bool) replyView {
	v := replyView{status: r.Status}
	switch {
	case r.Status == 0 || r.Cut:
		v.ending = "cut"
		return v
	case stream && r.Status == 200:
		return viewStream(v, r.Body)
	}
	var doc any
	if err := json.Unmarshal(r.Body, &doc); err != nil {
		v.ending = "not json"
		return v
	}
	v.ending = "json"
	readDoc(&v, doc)
	return v
}

// readDoc reads one JSON document of a reply: its error, or its object, model, choices, prompt tokens and shape.
func readDoc(v *replyView, doc any) {
	m, ok := doc.(map[string]any)
	if !ok {
		v.shape = shapeOf(doc)
		return
	}
	if e, ok := errorOf(m); ok {
		v.err = e
		return
	}
	v.object, _ = m["object"].(string)
	v.model, _ = m["model"].(string)
	if cs, ok := m["choices"].([]any); ok {
		v.choices = strconv.Itoa(len(cs))
	}
	v.prompt = promptTokens(m)
	v.shape = shapeOf(doc)
}

// errorOf is an error document's error, message|type|param|code: vLLM's {"error":{...}}, the older {"object":"error",...}, or a bare
// {"error":"..."} (an engine's 401).
func errorOf(m map[string]any) (string, bool) {
	e, has := m["error"]
	switch {
	case has:
		if em, ok := e.(map[string]any); ok {
			return fields(em), true
		}
		return fmt.Sprint(e), true
	case m["object"] == "error":
		return fields(m), true
	}
	return "", false
}

func fields(m map[string]any) string {
	return fmt.Sprintf("%v|%v|%v|%v", m["message"], m["type"], m["param"], m["code"])
}

func promptTokens(m map[string]any) string {
	u, ok := m["usage"].(map[string]any)
	if !ok {
		return ""
	}
	if p, ok := u["prompt_tokens"]; ok && p != nil {
		return fmt.Sprint(p)
	}
	return ""
}

// viewStream reads a streamed 200: its events, how it ended, and their shape.
func viewStream(v replyView, body []byte) replyView {
	done := false
	objects, indexes, shapes := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, line := range strings.Split(string(body), "\n") {
		data, ok := strings.CutPrefix(strings.TrimRight(line, "\r"), "data:")
		if !ok {
			continue
		}
		data = strings.TrimSpace(data)
		if data == "[DONE]" {
			done = true
			continue
		}
		var doc any
		if json.Unmarshal([]byte(data), &doc) != nil {
			v.ending = "not json"
			return v
		}
		m, _ := doc.(map[string]any)
		if e, ok := errorOf(m); ok {
			v.err = e
			continue
		}
		if o, ok := m["object"].(string); ok {
			objects[o] = true
		}
		v.model, _ = m["model"].(string)
		if p := promptTokens(m); p != "" {
			v.prompt, v.usage = p, true
		}
		cs, _ := m["choices"].([]any)
		for _, c := range cs {
			cm, _ := c.(map[string]any)
			indexes[fmt.Sprint(cm["index"])] = true
			if d, ok := cm["delta"].(map[string]any); ok && d["role"] != nil {
				v.roleSeen = true
			}
		}
		shapes[shapeOf(doc)] = true
	}
	switch {
	case v.err != "":
		v.ending = "error"
	case done:
		v.ending = "done"
	default:
		v.ending = "unfinished"
	}
	v.object, v.choices, v.shape = joinKeys(objects), joinKeys(indexes), joinKeys(shapes)
	return v
}

// shapeOf is a JSON value's shape: an object as its keys (set-aside keys removed) and their shapes, an array as the union of its
// elements' shapes, and a scalar as its JSON type. It holds no value.
func shapeOf(x any) string {
	switch t := x.(type) {
	case map[string]any:
		var parts []string
		for k, v := range t {
			if setAside[k] {
				continue
			}
			parts = append(parts, k+":"+shapeOf(v))
		}
		sort.Strings(parts)
		return "{" + strings.Join(parts, ",") + "}"
	case []any:
		elems := map[string]bool{}
		for _, e := range t {
			elems[shapeOf(e)] = true
		}
		return "[" + joinKeys(elems) + "]"
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "bool"
	case nil:
		return "null"
	}
	return "?"
}

func joinKeys(m map[string]bool) string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return strings.Join(ks, "|")
}

// ErrorText is a reply's error as the post names it, "type: message" clipped, "" when the reply carries none.
func ErrorText(r Reply, stream bool) string {
	v := viewReply(r, stream)
	if v.err == "" {
		return ""
	}
	parts := strings.SplitN(v.err, "|", 4)
	if len(parts) < 2 {
		return clipText(v.err)
	}
	return clipText(parts[1] + ": " + parts[0])
}

// clipText is s cut to errorTextMax bytes, on a rune boundary, with an ellipsis: an error the post names is a line, not a page.
func clipText(s string) string {
	if len(s) <= errorTextMax {
		return s
	}
	cut := errorTextMax
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

// errorTextMax is the longest error text the post and a failed check carry.
const errorTextMax = 160
