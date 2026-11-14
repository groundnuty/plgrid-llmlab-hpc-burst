package alpha

// The direct-to-engine control's comparison (routercontrol.go), on bodies of vLLM v0.32.0's shapes written here by hand: invented ids,
// texts and counts, never a live run's. Each rule has a case that differs on it alone, and each set-aside field a case that differs on it
// alone and agrees.

import (
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
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

// oneChunk and twoChunks are two streams the sampler made of one request: one token, in the chunk that ends the stream; and two tokens, then
// an empty chunk that ends it. Only where finish_reason falls tells them apart.
var (
	oneChunk = "data: {\"id\":\"c-3\",\"object\":\"chat.completion.chunk\",\"created\":3,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"c-3\",\"object\":\"chat.completion.chunk\",\"created\":3,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: [DONE]\n\n"
	twoChunks = "data: {\"id\":\"c-4\",\"object\":\"chat.completion.chunk\",\"created\":4,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"c-4\",\"object\":\"chat.completion.chunk\",\"created\":4,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"c-4\",\"object\":\"chat.completion.chunk\",\"created\":4,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
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
		{"a stream that ended a chunk earlier on one path: finish_reason's place is the sampler's",
			ok(stream1), ok(strings.Replace(stream1, `"delta":{"content":" you"},"finish_reason":"length"}]}`+"\n\n", `"delta":{"content":" you"},"finish_reason":null}]}`+"\n\n"+
				"data: {\"id\":\"c-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"length\"}]}\n\n", 1)), true, ""},
		{"one token on one path, its chunk ending the stream; two on the other, then a chunk that ends it", ok(oneChunk), ok(twoChunks), true, ""},
		{"a stop string the sampler hit on one path", ok(chatOK), ok(strings.Replace(chatOK, `"stop_reason":null`, `"stop_reason":"\n\n"`, 1)), false, ""},
		{"no fingerprint on one path", ok(chatOK), ok(strings.Replace(chatOK, `,"system_fingerprint":"fp-1"`, ``, 1)), false, ""},
		{"the prefix cache held part of the prompt on one path", ok(chatOK), ok(strings.Replace(chatOK, `"prompt_tokens_details":null`, `"prompt_tokens_details":{"cached_tokens":8}`, 1)), false, ""},
		{"a reasoning field the sampler filled on one path", ok(chatOK), ok(strings.Replace(chatOK, `"reasoning":null`, `"reasoning":"so"`, 1)), false, ""},
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

// The worker's socket is read from Envoy's configuration as the agent renders it (internal/agent, RenderEnvoyConfig): the one worker's
// socket, never Envoy's own listener or admin socket. Two workers, or none, is an error, as is a path the step could not quote.
func TestWorkerSocketOfReadsTheOneWorkersSocketFromEnvoysOwnConfiguration(t *testing.T) {
	render := func(workers ...string) []byte {
		t.Helper()
		var ws []agent.EnvoyWorker
		for i, s := range workers {
			ws = append(ws, agent.EnvoyWorker{Name: agentapi.WorkerName(i), Socket: s})
		}
		b, err := agent.RenderEnvoyConfig(agent.EnvoyConfig{RunDir: "/scratch/run", Workers: ws, ReadinessPort: 28160})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	if got, err := WorkerSocketOf(render("/scratch/run/worker-0.sock")); err != nil || got != "/scratch/run/worker-0.sock" {
		t.Fatalf("WorkerSocketOf = %q, %v; want the worker's socket", got, err)
	}
	for name, cfg := range map[string][]byte{
		"two workers":         render("/scratch/run/worker-0.sock", "/scratch/run/worker-1.sock"),
		"a path with a quote": render("/scratch/run/worker'0.sock"),
		"not JSON":            []byte("{"),
	} {
		if got, err := WorkerSocketOf(cfg); err == nil {
			t.Errorf("%s: WorkerSocketOf = %q, want an error", name, got)
		}
	}
}

// unixServer serves h on a Unix socket in a short directory of its own (a socket path has a length limit), until the test ends.
func unixServer(t *testing.T, h http.Handler) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "rc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "worker-0.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })
	return sock
}

// runStep runs a control step's script as srun would, with bash reading it on its stdin, and returns its output and exit status.
func runStep(t *testing.T, script string, env ...string) ([]byte, int) {
	t.Helper()
	cmd := exec.Command("bash", "-s")
	cmd.Stdin = strings.NewReader(script)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.Output()
	var ee *exec.ExitError
	switch {
	case err == nil:
		return out, 0
	case errors.As(err, &ee):
		return out, ee.ExitCode()
	}
	t.Fatalf("the step did not run: %v", err)
	return nil, -1
}

// The control's step, run through bash and curl on this host against a server on a Unix socket, as srun runs it on the node: each variant
// reaches the socket with the Instance's key, as the front sends it, and its answer comes back whole, a stream included; a key the engine
// does not take is the engine's 401 on that path; and ParseControl reads the lines back by place. The key is never on a command line: the
// server sees it in the header, and the script is stdin.
func TestTheControlStepSendsEachVariantToTheSocketWithTheKey(t *testing.T) {
	if _, err := exec.LookPath("curl"); err != nil {
		t.Skip("curl is not on this host")
	}
	const key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	sock := unixServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+key {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":"Unauthorized"}`)
			return
		}
		body, _ := io.ReadAll(r.Body)
		switch {
		case strings.Contains(string(body), `"stream":true`):
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, stream1)
		case strings.Contains(string(body), "refuse me"):
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, refusal)
		default:
			_, _ = io.WriteString(w, `{"path":"`+r.URL.Path+`","got":`+strconv.Itoa(len(body))+`}`)
		}
	}))
	reqs := []ControlRequest{
		{Endpoint: "/v1/chat/completions", Body: []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)},
		{Endpoint: "/v1/completions", Body: []byte(`{"model":"m","prompt":"refuse me"}`)},
		{Endpoint: "/v1/chat/completions", Body: []byte(`{"model":"m","stream":true,"messages":[]}`)},
	}
	script, err := ControlScript(sock, key, reqs)
	if err != nil {
		t.Fatal(err)
	}
	out, code := runStep(t, script)
	if code != 0 {
		t.Fatalf("the step exited %d: %s", code, out)
	}
	replies, connected := ParseControl(out, len(reqs))
	if !connected {
		t.Fatalf("no variant reached the socket: %s", out)
	}
	want := []Reply{
		{Status: 200, Body: []byte(`{"path":"/v1/chat/completions","got":57}`)},
		{Status: 400, Body: []byte(refusal)},
		{Status: 200, Body: []byte(stream1)},
	}
	for i, w := range want {
		if replies[i] == nil || replies[i].Status != w.Status || string(replies[i].Body) != string(w.Body) || replies[i].Cut {
			t.Errorf("variant %d: %+v, want %+v", i, replies[i], w)
		}
	}
	if strings.Contains(script, "Bearer "+key+"\n") || strings.Contains(string(out), key) {
		t.Error("the key is written out where it should not be: in the step's output, or as a header line of the script")
	}
	// Another key: the engine's own 401 on the direct path.
	other := strings.Repeat("f", 64)
	script, _ = ControlScript(sock, other, reqs[:1])
	out, _ = runStep(t, script)
	if replies, _ := ParseControl(out, 1); replies[0] == nil || replies[0].Status != 401 {
		t.Errorf("another key: %+v, want the engine's 401", replies[0])
	}
}

// The step's own failures are its exit statuses, never answers: no curl on the node (21), and a socket that is not one (24); a socket that
// refuses connections is curl's exit 7 for every variant, which ParseControl reads as nothing reached. Bad inputs are refused before any
// step: a path the step could not quote, a key of another form, an endpoint the control does not send to.
func TestTheControlStepsFailuresAreNamedNeverAnswers(t *testing.T) {
	if _, err := exec.LookPath("curl"); err != nil {
		t.Skip("curl is not on this host")
	}
	const key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	reqs := []ControlRequest{{Endpoint: "/v1/completions", Body: []byte(`{"prompt":"x"}`)}}
	dir, err := os.MkdirTemp("", "rc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	// No curl: a PATH with the step's other tools and not curl.
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, tool := range []string{"base64", "mktemp", "rm"} {
		p, err := exec.LookPath(tool)
		if err != nil {
			t.Skipf("%s is not on this host", tool)
		}
		if err := os.Symlink(p, filepath.Join(bin, tool)); err != nil {
			t.Fatal(err)
		}
	}
	sock := filepath.Join(dir, "worker-0.sock")
	script, err := ControlScript(sock, key, reqs)
	if err != nil {
		t.Fatal(err)
	}
	if out, code := runStep(t, script, "PATH="+bin); code != 21 || !strings.Contains(string(out), "control: curl is not on the node") {
		t.Errorf("no curl: exit %d, %q; want 21", code, out)
	}
	// No socket at the path.
	if out, code := runStep(t, script); code != 24 || !strings.Contains(string(out), "not a socket") {
		t.Errorf("no socket: exit %d, %q; want 24", code, out)
	}
	// A socket nobody listens on: curl cannot connect, for every variant.
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	if ul, ok := l.(*net.UnixListener); ok {
		ul.SetUnlinkOnClose(false)
	}
	l.Close()
	out, code := runStep(t, script)
	if replies, connected := ParseControl(out, 1); code != 0 || connected || replies[0] == nil || !replies[0].Cut {
		t.Errorf("a dead socket: exit %d, connected %t, %+v; want every variant unreached", code, connected, replies[0])
	}
	for name, f := range map[string]func() (string, error){
		"a path with a quote":   func() (string, error) { return ControlScript("/run/a'b.sock", key, reqs) },
		"a key of another form": func() (string, error) { return ControlScript(sock, "not-a-key", reqs) },
		"another endpoint":      func() (string, error) { return ControlScript(sock, key, []ControlRequest{{Endpoint: "/v1/models"}}) },
	} {
		if _, err := f(); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if line, err := ControlSrunLine("12x"); err == nil {
		t.Errorf("ControlSrunLine took a job ID that is not digits: %q", line)
	}
	if line, _ := ControlSrunLine("1234567"); line != "srun --overlap --jobid=1234567 --ntasks=1 --time=12 bash -s" {
		t.Errorf("ControlSrunLine = %q", line)
	}
}

// ParseControl reads only well-formed lines, each place once: a line for a place already read, out of range, or with a body that is not
// base64 is skipped, so a variant without a good line is unanswered (nil).
func TestParseControlReadsEachPlaceOnce(t *testing.T) {
	ok := base64.StdEncoding.EncodeToString([]byte("{}"))
	out := "noise\nR 0 0 200 " + ok + "\nR 0 0 500 " + ok + "\nR 5 0 200 " + ok + "\nR 1 0 200 !!!\nR 2 28 000 \ncontrol: done\n"
	replies, connected := ParseControl([]byte(out), 3)
	if !connected || replies[0] == nil || replies[0].Status != 200 || replies[1] != nil || replies[2] == nil || !replies[2].Cut || replies[2].Status != 0 {
		t.Errorf("ParseControl = %+v, %+v, %+v (connected %t)", replies[0], replies[1], replies[2], connected)
	}
}

// ControlOf: a variant the engine gave no answer straight from the node is unanswered, and so is one it did not answer completely; a
// variant answered other than 200 alike on both paths is listed with the engine's error, and so is a 200 stream that the engine ended with
// the same error event on both; a 200 alike is not listed; a variant the front's own bound cut is answered but not compared.
func TestControlOfCountsAndListsEachVariant(t *testing.T) {
	cut := Reply{Status: 200, Body: []byte(chatOK[:30]), Cut: true}
	refused, okBoth, errStream := status(400, refusal), ok(chatOK), ok(streamErr)
	c := ControlOf([]ControlVariant{
		{Key: "d/none#plain", Front: okBoth, Direct: nil},
		{Key: "c/cut#plain", Front: okBoth, Direct: &cut},
		{Key: "b/refused#plain", Front: refused, Direct: &refused},
		{Key: "a/stream-error#stream", Stream: true, Front: errStream, Direct: &errStream},
		{Key: "e/fine#plain", Front: okBoth, Direct: &okBoth},
		{Key: "f/late#plain", Front: Reply{Cut: true}, Direct: &okBoth, FrontPastBound: true},
	})
	if c.Sent != 5 || c.Answered != 4 || c.Compared != 3 || len(c.Differ) != 0 {
		t.Errorf("sent %d, answered %d, compared %d, differ %v; want 5, 4, 3, none", c.Sent, c.Answered, c.Compared, c.Differ)
	}
	if want := []string{"c/cut#plain", "d/none#plain"}; !slices.Equal(c.Unanswered, want) {
		t.Errorf("unanswered %v, want %v (in key order)", c.Unanswered, want)
	}
	want := []AlikeVariant{
		{Key: "a/stream-error#stream", Status: 200, Error: "BadRequestError: the reasoning parser is not set"},
		{Key: "b/refused#plain", Status: 400, Error: `BadRequestError: "auto" tool choice requires --enable-auto-tool-choice and --tool-call-parser to be set`},
	}
	if !slices.Equal(c.Alike, want) {
		t.Errorf("alike %+v, want %+v", c.Alike, want)
	}
	if f := controlChecks(BytesObs{Expected: 6}, c); len(f) != 1 || !strings.Contains(f[0], "4 of 6 answered (c/cut#plain, d/none#plain)") {
		t.Errorf("controlChecks = %q, want the unanswered named", f)
	}
}
