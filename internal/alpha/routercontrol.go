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
// Values are compared only where named above; every other field is compared by its presence and its JSON type alone, so its value (an
// id, a timestamp, a count of generated tokens) never is. A map keyed by data (dataKeyed: the logprobs of each position, keyed by token)
// is read as an opaque map, by neither its keys nor its size. Set aside even from the shape, by name (setAside), are the fields whose presence
// or type itself follows what the sampler did or a cache: the generated text and reasoning (text, content, reasoning,
// reasoning_content: a string or null), the tool calls and function call it made (there or not), finish_reason and stop_reason (null on
// a stream's chunks until the one that ends it, which falls where the sampler stopped; stop_reason a string, a token or null), the
// fingerprint (there or not), and both token-details objects (null, or an object once the prefix cache holds a part). S13's variants
// sample, and vLLM at temperature 0 is not batch-invariant either, so no token text is compared, even at temperature 0. Headers are not
// compared at all: the front adds its attribution headers, and Envoy its own.

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Reply is one path's answer to a variant: the status (0 for no answer), the body, and whether it was cut short.
type Reply struct {
	Status int
	Body   []byte
	Cut    bool
}

// dataKeyed are the keys whose value is a map keyed by data, or a list of such maps: completions' logprobs.top_logprobs, each position a
// map from a token's text to its logprob, and prompt_logprobs, each position null or a map from a token's id to its logprob (vLLM
// v0.32.0, completion/protocol.py and the logprob builders of the serving code). Their keys are tokens, which the sampler decides, so the
// shape reads each such map as an opaque map, by neither its keys nor its number of entries: a position holds the top-k and, when the
// sampled token is not among them, that token too, so its width is k or k+1. Chat's logprobs.content[].top_logprobs is a list of objects
// with fixed keys (token, logprob, bytes), which an opaque reading compares no worse.
var dataKeyed = map[string]bool{"top_logprobs": true, "prompt_logprobs": true}

// setAside are the keys whose presence and type, as well as their values, the comparison ignores wherever they occur (see the file's
// comment).
var setAside = map[string]bool{
	"text": true, "content": true, "reasoning": true, "reasoning_content": true, "tool_calls": true, "function_call": true,
	"finish_reason": true, "stop_reason": true,
	"system_fingerprint": true, "completion_tokens_details": true, "prompt_tokens_details": true,
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
			switch {
			case setAside[k]:
				continue
			case dataKeyed[k]:
				parts = append(parts, k+":"+opaqueShape(v))
			default:
				parts = append(parts, k+":"+shapeOf(v))
			}
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

// opaqueShape is a data-keyed value's shape (dataKeyed): an object is "map", whatever its keys, a list the union of its elements' opaque
// shapes, and a scalar its JSON type.
func opaqueShape(x any) string {
	switch t := x.(type) {
	case map[string]any:
		return "map"
	case []any:
		elems := map[string]bool{}
		for _, e := range t {
			elems[opaqueShape(e)] = true
		}
		return "[" + joinKeys(elems) + "]"
	}
	return shapeOf(x)
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

// ControlObs is the direct-to-engine control as the run read it.
type ControlObs struct {
	// Read says the control was made: the step ran on the node, found the worker's socket in Envoy's configuration, and the socket
	// answered. Why says, naming the step, why it was not.
	Read bool   `json:"read"`
	Why  string `json:"why,omitempty"`
	// Sent counts the variants sent straight to the engine, Answered those it answered completely, Compared those whose two answers were
	// compared (answered completely on both paths).
	Sent     int `json:"sent"`
	Answered int `json:"answered"`
	Compared int `json:"compared"`
	// Took is how long the step that sent the variants straight to the engine took, from the host: under ControlBound.
	Took time.Duration `json:"took"`
	// Unanswered names, by S13's key, the variants the engine did not answer completely straight from the node (at most FailedKept).
	Unanswered []string `json:"unanswered,omitempty"`
	// Differ is every variant whose two answers differ: the router's doing, or the path's. Alike is every variant answered other than 200
	// alike on both paths, or ended alike by the engine's own error event: the deployment's own refusals, the engine's, listed.
	Differ []VariantDiff  `json:"differ,omitempty"`
	Alike  []AlikeVariant `json:"alike,omitempty"`
	// EndedAlike counts the variants among Alike whose answer through the front did not finish by the reading's rule (no [DONE]) and
	// whose two answers are alike: a stream the engine ended with its own error event on both paths. The front passes that event on as the
	// stream's one error event and drops the engine's [DONE] after it (internal/front, judge.go; the science agent, 31 Oct 2026), so the
	// front's reading cannot call it finished; the control says it is the engine's, and the reading's completeness check discounts it.
	EndedAlike int `json:"endedAlike"`
}

// VariantDiff is a variant answered differently through the router than straight to the engine: S13's key, the two statuses, and what
// differs (CompareReplies).
type VariantDiff struct {
	Key    string `json:"key"`
	Front  int    `json:"front"`
	Direct int    `json:"direct"`
	Why    string `json:"why"`
}

func (d VariantDiff) String() string { return d.Key + ": " + d.Why }

// AlikeVariant is a variant answered other than 200, or ended by the engine's error event, alike on both paths: S13's key, the status, the
// engine's error (ErrorText), and whether it ended the stream (a 200 that did not finish through the front, its error event the terminal).
type AlikeVariant struct {
	Key    string `json:"key"`
	Status int    `json:"status"`
	Error  string `json:"error"`
	Ended  bool   `json:"ended,omitempty"`
}

func (a AlikeVariant) String() string {
	s := fmt.Sprintf("%s %d", a.Key, a.Status)
	if a.Error != "" {
		s = fmt.Sprintf("%s %d (%s)", a.Key, a.Status, a.Error)
	}
	if a.Ended {
		s += " [the stream ended by the engine's error event on both paths]"
	}
	return s
}

// ControlVariant is one variant as the control compares it: S13's key, whether it streams, and its two answers. Direct is absent (nil)
// when the engine gave no answer straight from the node.
type ControlVariant struct {
	Key            string
	Stream         bool
	Front          Reply
	Direct         *Reply
	FrontPastBound bool
}

// ControlOf compares each variant's two answers, in the order of S13's keys. A variant the front never answered within the reading's own
// bound is counted by the front's reading, not compared here; one the engine did not answer completely straight from the node is
// unanswered, a failure the control names.
func ControlOf(vs []ControlVariant) ControlObs {
	c := ControlObs{Read: true}
	sorted := slices.Clone(vs)
	slices.SortStableFunc(sorted, func(x, y ControlVariant) int { return strings.Compare(x.Key, y.Key) })
	for _, v := range sorted {
		if v.Direct == nil {
			c.unanswered(v.Key)
			continue
		}
		c.Sent++
		d := *v.Direct
		if !ClassifyReply(d.Status, d.Body, v.Stream, d.Cut).Complete {
			c.unanswered(v.Key)
			continue
		}
		c.Answered++
		if v.FrontPastBound {
			continue
		}
		c.Compared++
		if why := CompareReplies(v.Front, d, v.Stream); why != "" {
			c.Differ = append(c.Differ, VariantDiff{Key: v.Key, Front: v.Front.Status, Direct: d.Status, Why: why})
			continue
		}
		ended := !ClassifyReply(v.Front.Status, v.Front.Body, v.Stream, v.Front.Cut).Complete
		if ended {
			c.EndedAlike++
		}
		// A stream ended alike carries the same error event on both paths, so its error lists it.
		if d.Status != 200 || ErrorText(d, v.Stream) != "" {
			c.Alike = append(c.Alike, AlikeVariant{Key: v.Key, Status: d.Status, Error: ErrorText(d, v.Stream), Ended: ended})
		}
	}
	return c
}

func (c *ControlObs) unanswered(key string) {
	if len(c.Unanswered) < FailedKept {
		c.Unanswered = append(c.Unanswered, key)
	}
}

// controlNamedMax is how many variants a failed check names; the post names every one.
const controlNamedMax = 10

// controlChecks: the control was made, the engine answered every variant straight from the node, and no variant was answered differently
// through the router than straight to the engine (the ruling (a) on the router row, #522).
func controlChecks(b BytesObs, c ControlObs) []string {
	if !c.Read {
		return []string{"the direct-to-engine control could not be read: " + c.Why}
	}
	var f []string
	if c.Sent != b.Expected || c.Answered != c.Sent || len(c.Unanswered) > 0 {
		f = append(f, fmt.Sprintf("the engine did not answer every variant completely straight from the node: %d of %d answered%s", c.Answered, b.Expected, keysNamed(c.Unanswered)))
	}
	if len(c.Differ) > 0 {
		var ds []string
		for i, d := range c.Differ {
			if i == controlNamedMax {
				ds = append(ds, fmt.Sprintf("and %d more, named in the post", len(c.Differ)-controlNamedMax))
				break
			}
			ds = append(ds, d.String())
		}
		f = append(f, "a variant was answered differently through the router than straight to the engine: "+strings.Join(ds, "; "))
	}
	return f
}

func keysNamed(keys []string) string {
	if len(keys) == 0 {
		return ""
	}
	return " (" + strings.Join(keys, ", ") + ")"
}

// ControlHow is how the control reached the engine, as the post says it (the ruling asks for it): from inside the run's own job, never
// through product code.
const ControlHow = "an srun --overlap step in the run's own job sent each variant with curl to the worker's engine socket named in Envoy's own configuration"

// controlLines are the control's lines of the text to post: how it was made, its counts, every variant that differed, and every variant
// answered other than 200 alike on both paths, the engine's own refusals with their errors.
func controlLines(c ControlObs) string {
	if !c.Read {
		return "- the direct-to-engine control: not read (" + c.Why + ")\n"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "- the direct-to-engine control (%s): %d variants sent straight to the engine in %s (its bound %s), %d answered completely, %d compared with the front's answer; %d answered differently through the router; %d ended by the engine's own error event alike on both paths, which the front's reading cannot call finished (the front drops the engine's [DONE] after its error event)\n",
		ControlHow, c.Sent, c.Took.Round(100*time.Millisecond), ControlBound, c.Answered, c.Compared, len(c.Differ), c.EndedAlike)
	if len(c.Differ) > 0 {
		var ds []string
		for _, d := range c.Differ {
			ds = append(ds, d.String())
		}
		fmt.Fprintf(&b, "- answered differently through the router than straight to the engine, every one: %s\n", strings.Join(ds, "; "))
	}
	if len(c.Alike) == 0 {
		b.WriteString("- answered other than 200 alike on both paths (the engine's own refusals under this deployment): none\n")
	} else {
		var as []string
		for _, a := range c.Alike {
			as = append(as, a.String())
		}
		fmt.Fprintf(&b, "- answered other than 200 alike on both paths (the engine's own refusals under this deployment), every one (%d): %s\n", len(c.Alike), strings.Join(as, "; "))
	}
	return b.String()
}

// WorkerSocketOf is the worker's engine socket on the node, read from Envoy's own configuration (internal/agent, envoyconf.go: each
// cluster's endpoints, as pipe paths): the path Envoy routes to, so that the control reaches exactly Envoy's upstream. It is an error unless
// the configuration names exactly one worker socket, a path of plain characters.
func WorkerSocketOf(envoyJSON []byte) (string, error) {
	var cfg struct {
		StaticResources struct {
			Clusters []struct {
				LoadAssignment struct {
					Endpoints []struct {
						LbEndpoints []struct {
							Endpoint struct {
								Address struct {
									Pipe *struct {
										Path string `json:"path"`
									} `json:"pipe"`
								} `json:"address"`
							} `json:"endpoint"`
						} `json:"lb_endpoints"`
					} `json:"endpoints"`
				} `json:"load_assignment"`
			} `json:"clusters"`
		} `json:"static_resources"`
	}
	if err := json.Unmarshal(envoyJSON, &cfg); err != nil {
		return "", fmt.Errorf("Envoy's configuration is not JSON: %w", err)
	}
	var socks []string
	for _, c := range cfg.StaticResources.Clusters {
		for _, e := range c.LoadAssignment.Endpoints {
			for _, l := range e.LbEndpoints {
				if p := l.Endpoint.Address.Pipe; p != nil && p.Path != "" {
					socks = append(socks, p.Path)
				}
			}
		}
	}
	if len(socks) != 1 {
		return "", fmt.Errorf("Envoy's configuration names %d worker sockets, want the one worker's", len(socks))
	}
	if !plainPath.MatchString(socks[0]) {
		return "", errors.New("the worker socket's path in Envoy's configuration has characters a step may not quote")
	}
	return socks[0], nil
}

// plainPath is a path the control's step may carry in single quotes as it is.
var plainPath = regexp.MustCompile(`^/[A-Za-z0-9/._-]+$`)

// ControlStepMinutes is the step's time limit on the node (srun --time), above ControlBound so that the step is never the one cut first.
const ControlStepMinutes = 12

// ControlBound bounds the whole direct reading: the step's run on the node, read from the host.
const ControlBound = 10 * time.Minute

// controlCurlMaxTime bounds each variant's request straight to the engine, inside the step.
const controlCurlMaxTime = 60

// ControlSrunLine is the control's step: an srun --overlap in the run's own job, as the other steps of the row, with a time limit that holds
// the direct reading (SrunLine's is one minute).
func ControlSrunLine(jobID string) (string, error) {
	if _, err := SrunLine(jobID); err != nil {
		return "", err
	}
	return "srun --overlap --jobid=" + jobID + " --ntasks=1 --time=" + strconv.Itoa(ControlStepMinutes) + " bash -s", nil
}

// ControlRequest is one variant as the step sends it: its endpoint and its body, as the front's reading sent it (AdaptBody).
type ControlRequest struct {
	Endpoint string
	Body     []byte
}

// ControlScript is the control's step script, carried on the step's stdin with the bodies and the Instance key in it: nothing of them on
// any command line. On the node it writes the key to a header file in a private temporary directory, removed on exit, and sends each
// variant with curl to the worker's socket, one at a time, writing one line per variant: "R <index> <curl's exit> <status> <body,
// base64>". It exits 21 without curl, 22 without base64, 23 without a temporary directory, and 24 when the socket is not a socket.
func ControlScript(socket, key string, reqs []ControlRequest) (string, error) {
	if !plainPath.MatchString(socket) {
		return "", errors.New("the worker socket's path has characters the step may not quote")
	}
	if agentapi.CheckKey([]byte(key)) != nil {
		return "", errors.New("the Instance's key is not of a key's form (agentapi.CheckKey), so the step does not carry it")
	}
	var b strings.Builder
	b.WriteString("set -u\n")
	b.WriteString("command -v curl >/dev/null 2>&1 || { echo 'control: curl is not on the node'; exit 21; }\n")
	b.WriteString("command -v base64 >/dev/null 2>&1 || { echo 'control: base64 is not on the node'; exit 22; }\n")
	b.WriteString("umask 077\nd=$(mktemp -d) || { echo 'control: no temporary directory'; exit 23; }\ntrap 'rm -rf \"$d\"' EXIT\n")
	fmt.Fprintf(&b, "printf 'Authorization: Bearer %%s\\n' '%s' > \"$d/h\"\n", key)
	fmt.Fprintf(&b, "sock='%s'\n[ -S \"$sock\" ] || { echo 'control: the worker socket is not a socket on the node'; exit 24; }\n", socket)
	b.WriteString("while read -r i ep body; do\n")
	b.WriteString("  : > \"$d/o\"\n")
	b.WriteString("  printf '%s' \"$body\" | base64 -d > \"$d/b\" 2>/dev/null || { printf 'R %s 98 000 \\n' \"$i\"; continue; }\n")
	fmt.Fprintf(&b, "  code=$(curl -sS -N --max-time %d --unix-socket \"$sock\" -H @\"$d/h\" -H 'Content-Type: application/json' --data-binary @\"$d/b\" -o \"$d/o\" -w '%%{http_code}' \"http://localhost$ep\" 2>/dev/null)\n", controlCurlMaxTime)
	b.WriteString("  rc=$?\n  printf 'R %s %s %s %s\\n' \"$i\" \"$rc\" \"${code:-000}\" \"$(base64 -w0 < \"$d/o\" 2>/dev/null)\"\n")
	b.WriteString("done <<'HB_CONTROL_VARIANTS'\n")
	for i, r := range reqs {
		if r.Endpoint != "/v1/chat/completions" && r.Endpoint != "/v1/completions" {
			return "", fmt.Errorf("variant %d's endpoint %q is not one the control sends to", i, r.Endpoint)
		}
		fmt.Fprintf(&b, "%d %s %s\n", i, r.Endpoint, base64.StdEncoding.EncodeToString(r.Body))
	}
	b.WriteString("HB_CONTROL_VARIANTS\n")
	return b.String(), nil
}

// ParseControl reads the step's lines: each variant's answer by its index, nil for one with no line. A curl that ended with an error is a
// cut answer (its status, or 0 for none). connected says some variant reached the socket: a curl that could not connect (exit 7) for every
// variant means the socket answered nothing.
func ParseControl(out []byte, n int) (replies []*Reply, connected bool) {
	replies = make([]*Reply, n)
	for _, line := range strings.Split(string(out), "\n") {
		f := strings.Fields(line)
		if len(f) < 4 || f[0] != "R" {
			continue
		}
		i, err1 := strconv.Atoi(f[1])
		rc, err2 := strconv.Atoi(f[2])
		code, err3 := strconv.Atoi(f[3])
		if err1 != nil || err2 != nil || err3 != nil || i < 0 || i >= n || replies[i] != nil {
			continue
		}
		var body []byte
		if len(f) > 4 {
			body, err1 = base64.StdEncoding.DecodeString(f[4])
			if err1 != nil {
				continue
			}
		}
		if rc != 7 {
			connected = true
		}
		replies[i] = &Reply{Status: code, Body: body, Cut: rc != 0}
	}
	return replies, connected
}
