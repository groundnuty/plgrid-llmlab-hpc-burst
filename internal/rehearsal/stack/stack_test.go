package stack

// The parts of the package that need no stack: the request bodies, the reading
// of a stream's end and of the front's errors, the binaries check, the
// environment for up, and the root k3s guard. Starting the stack is tested by
// its users (internal/alpha's stand-in run, the rehearsal).

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestChatBody(t *testing.T) {
	pad := "a \"quoted\" line\nand a \\ backslash"
	for name, c := range map[string]struct {
		body   string
		stream bool
		msgs   int
	}{
		"plain":           {ChatBody("example-org/m", 8, 0, false, ""), false, 1},
		"streamed, fixed": {ChatBody("example-org/m", 8, 8, true, ""), true, 1},
		"padded":          {ChatBody("example-org/m", 8, 0, false, pad), false, 2},
	} {
		var b struct {
			Model     string `json:"model"`
			MaxTokens int    `json:"max_tokens"`
			MinTokens int    `json:"min_tokens"`
			Stream    bool   `json:"stream"`
			Messages  []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.Unmarshal([]byte(c.body), &b); err != nil {
			t.Fatalf("%s: not JSON: %v: %s", name, err, c.body)
		}
		if b.Model != "example-org/m" || b.MaxTokens != 8 || b.Stream != c.stream || len(b.Messages) != c.msgs {
			t.Errorf("%s: %+v", name, b)
		}
		if c.msgs == 2 && b.Messages[1].Content != pad {
			t.Errorf("%s: the pad came back as %q", name, b.Messages[1].Content)
		}
	}
	var fixed struct {
		MinTokens int `json:"min_tokens"`
	}
	_ = json.Unmarshal([]byte(ChatBody("m", 8, 8, true, "")), &fixed)
	if fixed.MinTokens != 8 {
		t.Errorf("min_tokens %d, want 8", fixed.MinTokens)
	}
}

func TestSSEDoneAndErrors(t *testing.T) {
	full := "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\ndata: {\"choices\":[{\"finish_reason\":\"length\"}]}\n\ndata: [DONE]\n"
	if done, fin := SSEDone([]byte(full)); !done || !fin {
		t.Errorf("a full stream: done %v finished %v", done, fin)
	}
	if done, fin := SSEDone([]byte("data: {\"choices\":[{\"delta\":{}}]}\n\ndata: [DONE]\n")); !done || fin {
		t.Errorf("no finish reason: done %v finished %v", done, fin)
	}
	cut := "data: {\"choices\":[{\"delta\":{}}]}\n\ndata: {\"error\":{\"code\":\"stream_cut\",\"detail\":null}}\n"
	if done, _ := SSEDone([]byte(cut)); done {
		t.Error("a cut stream read as done")
	}
	if c := StreamErrorCode([]byte(cut)); c != "stream_cut" {
		t.Errorf("StreamErrorCode = %q, want stream_cut", c)
	}
	if c := StreamErrorCode([]byte(full)); c != "" {
		t.Errorf("a stream with no error event: %q", c)
	}
	if e := ErrorOf([]byte(`{"error":{"code":"no_ready_target"}}`)); e.Code != "no_ready_target" {
		t.Errorf("ErrorOf = %+v", e)
	}
	if e := ErrorOf([]byte(`not json`)); e.Code != "" {
		t.Errorf("ErrorOf of no JSON = %+v", e)
	}
}

// Only a chunk that carries tokens is a token's arrival: the head-of-line measure's gaps are between tokens, and a
// role-only, finish-only or usage chunk, an error event or [DONE] that comes late is no gap between two of them.
func TestTokenEvent(t *testing.T) {
	for _, c := range []struct {
		line string
		want bool
	}{
		{`data: {"choices":[{"index":0,"delta":{"content":"a"}}]}`, true},
		{`data:{"choices":[{"index":0,"delta":{"content":" b"},"finish_reason":null}]}`, true},
		{`data: {"choices":[{"index":0,"text":"a"}]}`, true},
		{`data: {"choices":[{"index":0,"delta":{"reasoning_content":"so"}}]}`, true},
		{`data: {"choices":[{"index":0,"delta":{"reasoning":"so"}}]}`, true},
		{`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{"}}]}}]}`, true},
		{`data: {"choices":[{"index":0,"delta":{"role":"assistant","content":""}}]}`, false},
		{`data: {"choices":[{"index":0,"delta":{"role":"assistant"}}]}`, false},
		{`data: {"choices":[{"index":0,"delta":{},"finish_reason":"length"}]}`, false},
		{`data: {"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":8}}`, false},
		{`data: {"choices":[{"index":0,"delta":{"content":null}}]}`, false},
		{`data: {"error":{"code":"stream_cut","detail":null}}`, false},
		{`data: [DONE]`, false},
		{`event: ping`, false},
		{`{"choices":[{"delta":{"content":"a"}}]}`, false},
	} {
		if got := TokenEvent(c.line); got != c.want {
			t.Errorf("TokenEvent(%s) = %v, want %v", c.line, got, c.want)
		}
	}
}

// SendTimed returns when the client wrote the request's last byte: inside the call, before its response (the server
// reads the whole body first), and zero for a request that was never written (nothing listens).
func TestSendTimed(t *testing.T) {
	read := make(chan time.Time, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		read <- time.Now()
		_, _ = w.Write([]byte(strconv.Itoa(len(b))))
	}))
	defer srv.Close()
	c := &Client{T: t, H: &Host{Ports: Ports{ClientAddr: strings.TrimPrefix(srv.URL, "http://")}, ClientKey: "k"}, MaxRequest: time.Second}
	body := strings.Repeat("x", 1<<20)
	before := time.Now()
	rp, sent := c.SendTimed("/v1/chat/completions", body, "")
	after := time.Now()
	if rp.Err != nil || rp.Status != http.StatusOK || string(rp.Body) != strconv.Itoa(1<<20) {
		t.Fatalf("reply %d %q %v; want 200 and the body's length", rp.Status, rp.Body, rp.Err)
	}
	if sent.IsZero() || sent.Before(before) || sent.After(after) {
		t.Errorf("sent at %v; want inside the call, %v to %v", sent, before, after)
	}
	if at := <-read; at.Before(before) {
		t.Errorf("the server read the body at %v, before the call at %v", at, before)
	}
	// Nothing listens at a closed listener's address: the request is never written.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	c.H.Ports.ClientAddr = addr
	if rp, sent := c.SendTimed("/v1/chat/completions", body, ""); rp.Err == nil || !sent.IsZero() {
		t.Errorf("an unreachable front: err %v, sent at %v; want an error and no time", rp.Err, sent)
	}
}

// SendCtx ends with its parent, which is how a background sender's stop cancels the request it has in flight.
func TestSendCtxEndsWithItsParent(t *testing.T) {
	arrived := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(arrived)
		<-release
	}))
	defer srv.Close()
	defer close(release)
	c := &Client{T: t, H: &Host{Ports: Ports{ClientAddr: strings.TrimPrefix(srv.URL, "http://")}, ClientKey: "k"}, MaxRequest: time.Minute}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan Reply, 1)
	go func() { done <- c.SendCtx(ctx, "/v1/chat/completions", "{}", "") }()
	<-arrived
	cancel()
	if rp := <-done; !errors.Is(rp.Err, context.Canceled) {
		t.Errorf("a cancelled parent: %d %v, want context.Canceled", rp.Status, rp.Err)
	}
}

// SendCtx carries the client's key and, when one is given, the conversation key, as Send does.
func TestSendCtxHeaders(t *testing.T) {
	type seen struct{ auth, session, kind, body string }
	got := make(chan seen, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- seen{r.Header.Get("Authorization"), r.Header.Get("X-Session-Id"), r.Header.Get("Content-Type"), string(b)}
		w.Header().Set("Hpcburst-Request-Id", "req-1")
	}))
	defer srv.Close()
	c := &Client{T: t, H: &Host{Ports: Ports{ClientAddr: strings.TrimPrefix(srv.URL, "http://")}, ClientKey: "k1"}, MaxRequest: time.Minute}
	rp := c.SendCtx(context.Background(), "/v1/chat/completions", "{}", "conv")
	if rp.Err != nil || rp.Status != http.StatusOK || rp.ID != "req-1" {
		t.Fatalf("reply %d %q %v", rp.Status, rp.ID, rp.Err)
	}
	if s := <-got; s != (seen{"Bearer k1", "conv", "application/json", "{}"}) {
		t.Errorf("the request carried %+v", s)
	}
}

func TestPorts(t *testing.T) {
	p := Ports{Base: 29520, ClientAddr: "127.0.0.1:29500", DialHomeListen: "127.0.0.1:29501"}
	if u := p.URL(6, "/readyz"); u != "http://127.0.0.1:29526/readyz" {
		t.Errorf("URL = %q", u)
	}
	want := []int{29500, 29501, 29520, 29521, 29522, 29523, 29524, 29525, 29526, 29527, 29528, 29529, 29530, 29531, 29532}
	if a := p.All(); !slices.Equal(a, want) {
		t.Errorf("All = %v, want %v", a, want)
	}
	// With no dial-home listener set, up takes base+7, already in the block.
	if a := (Ports{Base: 29520, ClientAddr: "127.0.0.1:29500"}).All(); !slices.Equal(a, append([]int{29500}, want[2:]...)) {
		t.Errorf("All with no dial-home listener = %v", a)
	}
}

// A job's apptainer is the double's, or what the wrapper puts in its place; the caller's job environment has the last word.
func TestClusterJobEnv(t *testing.T) {
	double := filepath.Join("fk", "apptainer")
	wrap := func(fakes string) string { return filepath.Join(fakes, "gate", "apptainer") }
	wrapped := filepath.Join("fk", "gate", "apptainer")
	for name, c := range map[string]struct {
		wrap func(string) string
		job  map[string]string
		want string
	}{
		"the double":                  {nil, nil, double},
		"the wrapper":                 {wrap, nil, wrapped},
		"the caller over the double":  {nil, map[string]string{"HPCBURST_APPTAINER": "own"}, "own"},
		"the caller over the wrapper": {wrap, map[string]string{"HPCBURST_APPTAINER": "own"}, "own"},
	} {
		env := clusterJobEnv("fk", c.wrap, c.job)
		if env["HPCBURST_APPTAINER"] != c.want {
			t.Errorf("%s: apptainer %q, want %q", name, env["HPCBURST_APPTAINER"], c.want)
		}
		if env["HPCBURST_NVIDIA_SMI"] != filepath.Join("fk", "no-nvidia-smi") {
			t.Errorf("%s: nvidia-smi %q", name, env["HPCBURST_NVIDIA_SMI"])
		}
	}
}

func TestHubFileIsLFS(t *testing.T) {
	if !HubFileIsLFS("model.safetensors") {
		t.Error("the weights are in LFS")
	}
	for _, p := range []string{"config.json", "generation_config.json", "no-such-file"} {
		if HubFileIsLFS(p) {
			t.Errorf("%s reads as an LFS file", p)
		}
	}
}

// ServedDeployment is ServedDeploymentAs under the rehearsal's own names, and the names given reach the object.
func TestServedDeploymentAs(t *testing.T) {
	tm := Timing{Lease: "7m", MaxRequest: "90s"}
	d := ServedDeployment(tm)
	if d.Name != MDName || d.Spec.Model != Model || d.Spec.Providers.Names[0] != ProviderName || d.Spec.Source.Served.Flags[0].Value != "4096" {
		t.Errorf("ServedDeployment: %s, %s, %v, %v", d.Name, d.Spec.Model, d.Spec.Providers.Names, d.Spec.Source.Served.Flags)
	}
	o := ServedDeploymentAs(Served{Name: "n", Model: "example-org/m", Provider: "p", MaxModelLen: "8192"}, tm)
	if o.Name != "n" || o.Namespace != Namespace || o.Spec.Model != "example-org/m" || o.Spec.Providers.Names[0] != "p" || o.Spec.Source.Served.Flags[0].Value != "8192" {
		t.Errorf("ServedDeploymentAs: %s/%s, %s, %v, %v", o.Namespace, o.Name, o.Spec.Model, o.Spec.Providers.Names, o.Spec.Source.Served.Flags)
	}
}

// The usage record's member names are HBEP-5's, written out here from the scenario's own reading of them: UsageRow is
// the front's own type, so a member renamed there would still decode in a run that read it through that type alone.
func TestUsageRowReadsTheWireNames(t *testing.T) {
	wire := `{"counter":7,"requestID":"r","startTime":"s","endTime":"e","path":"/p","requestedModel":"m","provider":"pr",
		"modelDeployment":"md","deploymentID":"d","instance":"i","instanceUID":"u","emitter":"em","status":200,"stream":true,
		"finish":"f","cutBy":"c","errorCode":"x","completionTokens":3,"durationMs":9,"firstTokenMs":4}`
	var r UsageRow
	if err := json.Unmarshal([]byte(wire), &r); err != nil {
		t.Fatal(err)
	}
	if r.Counter != 7 || r.RequestID != "r" || r.StartTime != "s" || r.EndTime != "e" || r.Path != "/p" || r.RequestedModel != "m" ||
		r.Provider != "pr" || r.ModelDeployment != "md" || r.DeploymentID != "d" || r.Instance != "i" || r.InstanceUID != "u" ||
		r.Emitter != "em" || r.Status != 200 || !r.Stream || r.DurationMs != 9 {
		t.Errorf("the plain members: %+v", r)
	}
	if r.Finish == nil || *r.Finish != "f" || r.CutBy == nil || *r.CutBy != "c" || r.ErrorCode == nil || *r.ErrorCode != "x" ||
		r.CompletionTokens == nil || *r.CompletionTokens != 3 || r.FirstTokenMs == nil || *r.FirstTokenMs != 4 {
		t.Errorf("the optional members: %+v", r)
	}
}

// DownErr runs hpcburst down once, with the state directory it was given, and returns its failure with its output.
func TestDownErrRunsOnce(t *testing.T) {
	dir := t.TempDir()
	calls := filepath.Join(dir, "calls")
	bin := filepath.Join(dir, "hpcburst")
	script := "#!/bin/sh\necho \"$@\" >> " + calls + "\necho went wrong\nexit 3\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(dir, "st")
	h := &Host{T: t, B: Binaries{HPCBurst: bin}, State: state, Env: os.Environ()}
	if err := h.DownErr(); err != nil || h.Downed() {
		t.Fatalf("no state directory: %v, downed %v; want nothing to stop", err, h.Downed())
	}
	if err := os.Mkdir(state, 0o755); err != nil {
		t.Fatal(err)
	}
	err := h.DownErr()
	if err == nil || !strings.Contains(err.Error(), "hpcburst down") || !strings.Contains(err.Error(), "went wrong") {
		t.Errorf("a failing down: %v; want its name and output", err)
	}
	if !h.Downed() {
		t.Error("down has run, and Downed says it has not")
	}
	if err := h.DownErr(); err != nil {
		t.Errorf("a second call: %v, want nothing", err)
	}
	b, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(b), "down --state-dir "+state+"\n"; got != want {
		t.Errorf("the commands run: %q, want %q once", got, want)
	}
}

// A down past its bound: by default its error is the command's own, as the alpha rows' has been; with NameDownBound it
// also says the bound, as the rehearsal's has been. The expected texts are those of main before PR #283.
func TestDownPastItsBound(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "hpcburst")
	// It prints nothing and runs until the bound kills it; exec, so that the kill leaves no child behind.
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexec sleep 600\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	const bound = 3 * time.Second
	for name, c := range map[string]struct {
		opts []HostOption
		want string
	}{
		"the default":   {nil, "hpcburst down: signal: killed\n"},
		"NameDownBound": {[]HostOption{NameDownBound}, "hpcburst down: signal: killed\nhpcburst did not end within 3s\n"},
	} {
		h := &Host{T: t, B: Binaries{HPCBurst: bin}, State: dir, Env: os.Environ(), DownBound: bound}
		for _, o := range c.opts {
			o(h)
		}
		if err := h.DownErr(); err == nil || err.Error() != c.want {
			t.Errorf("%s: %q, want %q", name, err, c.want)
		}
	}
}

func TestBinariesMissing(t *testing.T) {
	dir := t.TempDir()
	exe := func(name string, mode os.FileMode) string {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("#!/bin/sh\n"), mode); err != nil {
			t.Fatal(err)
		}
		return p
	}
	b := Binaries{HPCBurst: exe("hpcburst", 0o755), Controller: exe("controller", 0o755), Front: exe("front", 0o644),
		Agent: filepath.Join(dir, "absent"), K3sDir: dir, Frps: exe("frps", 0o755), Frpc: exe("frpc", 0o755)}
	exe("k3s-1.36.5/k3s", 0o755)
	m := b.Missing()
	if !slices.Equal(m, []string{b.Front, b.Agent}) {
		t.Errorf("Missing = %v, want the front (not executable) and the agent (absent)", m)
	}
	// Before frps and frpc are known: MissingBuilds does not ask for them, and Missing does.
	b.Frps, b.Frpc = "", ""
	if m := b.MissingBuilds(); !slices.Equal(m, []string{b.Front, b.Agent}) {
		t.Errorf("MissingBuilds = %v, want the front and the agent", m)
	}
	if m := b.Missing(); !slices.Equal(m, []string{b.Front, b.Agent, "", ""}) {
		t.Errorf("Missing with no frps or frpc = %q", m)
	}
	// A directory has the executable bit and is not a binary: the alpha rows' check
	// counts it as missing, and the rehearsal's leaves it to fail when it is run.
	b.HPCBurst = dir
	if m := b.Missing(); !slices.Contains(m, dir) {
		t.Errorf("Missing = %v, want the directory named as hpcburst", m)
	}
	if m := b.MissingBuilds(); slices.Contains(m, dir) {
		t.Errorf("MissingBuilds = %v, want the directory left out", m)
	}
}

func TestCleanEnvDropsTheFakes(t *testing.T) {
	t.Setenv("HPCBURST_SSH", "/fake/ssh")
	t.Setenv("HPCBURST_APPTAINER", "/fake/apptainer")
	t.Setenv("HB_TEST_ROLE", "engine")
	env := CleanEnv("HF_ENDPOINT=http://127.0.0.1:1")
	for _, kv := range env {
		for _, p := range []string{"HPCBURST_SSH=", "HPCBURST_APPTAINER=", "HB_TEST_ROLE="} {
			if strings.HasPrefix(kv, p) {
				t.Errorf("CleanEnv kept %s", kv)
			}
		}
	}
	if env[len(env)-1] != "HF_ENDPOINT=http://127.0.0.1:1" {
		t.Errorf("CleanEnv dropped the extra: %v", env[len(env)-1])
	}
}

// TestRootK3sGuard: a host with no system k3s, such as a CI runner, may run the stack unless HPCBURST_TEST_ROOT_K3S=1
// says one runs there; a change of its listeners across the run fails it, whichever they were.
func TestRootK3sGuard(t *testing.T) {
	for _, c := range []struct {
		before   string
		required bool
		ok       bool
	}{
		{"", false, true},                // a CI runner
		{"6443,6444,10250", false, true}, // this VM, unstated
		{"6443,6444,10250", true, true},
		{"", true, false},
		{"6443,10250", true, false},
	} {
		if err := RootK3sBefore(c.before, c.required); (err == nil) != c.ok {
			t.Errorf("RootK3sBefore(%q, %v) = %v, want ok %v", c.before, c.required, err, c.ok)
		}
	}
	for _, c := range []struct {
		before, after string
		ok            bool
	}{
		{"", "", true},
		{"6443,6444,10250", "6443,6444,10250", true},
		{"6443,6444,10250", "6443,10250", false},
		{"", "6443", false},
	} {
		if err := RootK3sAfter(c.before, c.after); (err == nil) != c.ok {
			t.Errorf("RootK3sAfter(%q, %q) = %v, want ok %v", c.before, c.after, err, c.ok)
		}
	}
}
