package dryrun

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
)

// The dry run's worker. This test binary is three programs (TestMain):
//   - named "apptainer" (a symbolic link the test makes), it is the engine double's fake Apptainer
//     (enginedouble.ApptainerMain), which takes the agent's exec invocation as it is, maps the bound
//     container paths to host paths, keeps only the carried environment (--cleanenv) and starts
//     "vllm" as this binary again with HB_TEST_ROLE=engine;
//   - with HB_TEST_ROLE=engine, it is the worker: the engine double's handler (enginedouble.Handler,
//     vLLM v0.32.0's HTTP subset with a block-level prefix cache and its counters) behind two things
//     the live worker gets from vLLM and from S2c's middleware, which the double lacks:
//       - POST /reset_prefix_cache (vLLM's dev-mode endpoint, VLLM_SERVER_DEV_MODE=1): a fresh
//         engine, so an empty prefix cache, answering {"success":true}. The double's counters start
//         again from zero, where vLLM's carry on; the analysis reads only their growth over an arm;
//       - the placement log: the Go twin of py/s2c_placement.py, the same line in the same file;
//   - otherwise, the tests.
// The double is imported only here, in a _test.go file (its own rule: enginedouble's import guard).

func TestMain(m *testing.M) {
	if os.Getenv(enginedouble.EnvRole) == enginedouble.RoleEngine {
		os.Exit(workerMain(os.Args[1:]))
	}
	if filepath.Base(os.Args[0]) == "apptainer" {
		os.Exit(enginedouble.ApptainerMain(os.Args[1:]))
	}
	os.Exit(m.Run())
}

// workerArgs is the part of vLLM's command line the double reads. Every flag the harness passes is
// known here; any other is refused, so a flag the harness starts passing is not ignored unseen.
type workerArgs struct {
	uds, served string
	maxLen      int
	maxSeqs     int
	kvBlocks    int
}

var (
	valueFlags = map[string]bool{"--uds": true, "--served-model-name": true, "--max-model-len": true, "--max-num-seqs": true,
		"--num-gpu-blocks-override": true, "--gpu-memory-utilization": true, "--middleware": true}
	boolFlags = map[string]bool{"--enable-prefix-caching": true, "--enable-prompt-tokens-details": true, "--disable-uvicorn-access-log": true}
)

func parseWorkerArgs(args []string) (workerArgs, error) {
	var a workerArgs
	if len(args) < 2 || args[0] != "serve" {
		return a, errors.New("want: serve <model> [flags]")
	}
	vals := map[string]string{}
	for i := 2; i < len(args); i++ {
		name, val, has := strings.Cut(args[i], "=")
		switch {
		case boolFlags[name] && !has:
		case valueFlags[name] && has:
			vals[name] = val
		case valueFlags[name] && i+1 < len(args):
			i++
			vals[name] = args[i]
		default:
			return a, fmt.Errorf("unknown or malformed vLLM flag %q", args[i])
		}
	}
	a.uds, a.served = vals["--uds"], vals["--served-model-name"]
	if a.uds == "" || a.served == "" {
		return a, errors.New("--uds and --served-model-name are required")
	}
	for flag, dst := range map[string]*int{"--max-model-len": &a.maxLen, "--max-num-seqs": &a.maxSeqs, "--num-gpu-blocks-override": &a.kvBlocks} {
		if v, ok := vals[flag]; ok {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 {
				return a, fmt.Errorf("%s %q", flag, v)
			}
			*dst = n
		}
	}
	return a, nil
}

var placementTag = regexp.MustCompile(`\[s2b:([A-Za-z0-9_.-]+):(\d+):(\d+)\]`)

// currentTag is the highest turn's tag in a request body (py/s2c_placement.py's rule).
func currentTag(body []byte) (run string, session, turn int, ok bool) {
	for _, m := range placementTag.FindAllSubmatch(body, -1) {
		s, _ := strconv.Atoi(string(m[2]))
		t, _ := strconv.Atoi(string(m[3]))
		if !ok || t > turn {
			run, session, turn, ok = string(m[1]), s, t, true
		}
	}
	return
}

type worker struct {
	cfg    enginedouble.Config
	engine atomic.Pointer[http.Handler]
	dev    bool
	name   string
	logMu  sync.Mutex
	log    *os.File
}

func (w *worker) fresh() {
	h := enginedouble.Handler(w.cfg)
	w.engine.Store(&h)
}

func (w *worker) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	h := *w.engine.Load()
	switch {
	case r.URL.Path == "/reset_prefix_cache":
		if !w.dev || r.Method != http.MethodPost {
			http.NotFound(rw, r)
			return
		}
		w.fresh()
		rw.Header().Set("Content-Type", "application/json")
		_, _ = rw.Write([]byte(`{"success":true}`))
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/chat/completions") && w.log != nil:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		start := time.Now()
		h.ServeHTTP(rw, r)
		end := time.Now()
		if run, s, t, ok := currentTag(body); ok {
			sid := r.Header.Get("x-session-id")
			if sid == "" {
				sid = "-"
			}
			w.logMu.Lock()
			fmt.Fprintf(w.log, "%.6f\t%.6f\t%s\t%s\t%d\t%d\t%s\n", unix(start), unix(end), w.name, run, s, t, sid)
			w.logMu.Unlock()
		}
	default:
		h.ServeHTTP(rw, r)
	}
}

func unix(t time.Time) float64 { return float64(t.UnixNano()) / 1e9 }

func workerMain(args []string) int {
	a, err := parseWorkerArgs(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "s2c double:", err)
		return 2
	}
	cfg := enginedouble.DefaultConfig()
	cfg.ServedName, cfg.ModelPath = a.served, args[1]
	if a.maxLen > 0 {
		cfg.MaxModelLen = a.maxLen
	}
	cfg.MaxNumSeqs = a.maxSeqs
	if a.kvBlocks > 0 {
		cfg.KVCacheTokens = int64(a.kvBlocks) * int64(cfg.BlockSize)
	}
	cfg.TokenDelay = 2 * time.Millisecond
	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, "s2c double:", err)
		return 2
	}
	w := &worker{cfg: cfg, dev: os.Getenv("VLLM_SERVER_DEV_MODE") == "1", name: os.Getenv("S2B_WORKER")}
	if p := os.Getenv("S2B_PLACEMENT_LOG"); p != "" {
		if w.log, err = os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "s2c double:", err)
			return 1
		}
		defer w.log.Close()
	}
	w.fresh()
	ln, err := net.Listen("unix", a.uds)
	if err != nil {
		fmt.Fprintln(os.Stderr, "s2c double:", err)
		return 1
	}
	srv := &http.Server{Handler: w, ReadHeaderTimeout: 10 * time.Second}
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigs
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, "s2c double:", err)
		return 1
	}
	os.Remove(a.uds)
	return 0
}

// The double's own behaviour that the dry run relies on.
func TestWorkerDoubleResetAndPlacement(t *testing.T) {
	dir, err := os.MkdirTemp("", "s2cd")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	logp := filepath.Join(dir, "placement.tsv")
	cfg := enginedouble.DefaultConfig()
	cfg.ServedName = "m"
	cfg.TokenDelay = time.Millisecond
	lf, _ := os.Create(logp)
	w := &worker{cfg: cfg, dev: true, name: "2", log: lf}
	w.fresh()
	srv := &http.Server{Handler: w}
	ln, _ := net.Listen("tcp", "127.0.0.1:0") // kernel-chosen
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()
	base := "http://" + ln.Addr().String()
	sysPrompt := strings.Repeat("shared words ", 40)
	chat := func(turn int) {
		body := fmt.Sprintf(`{"model":"m","max_tokens":4,"min_tokens":4,"messages":[{"role":"system","content":"%s"},{"role":"user","content":"[s2b:B1-rr:7:%d] go"}]}`, sysPrompt, turn)
		req, _ := http.NewRequest(http.MethodPost, base+"/v1/chat/completions", strings.NewReader(body))
		req.Header.Set("x-session-id", "B1-rr-s007")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("chat: %d", resp.StatusCode)
		}
	}
	hits := func() float64 {
		resp, err := http.Get(base + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		for _, l := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(l, "vllm:prefix_cache_hits_total{") {
				f, _ := strconv.ParseFloat(l[strings.LastIndex(l, " ")+1:], 64)
				return f
			}
		}
		t.Fatal("no vllm:prefix_cache_hits_total")
		return 0
	}
	chat(0)
	chat(1) // the same 80-word system prompt again: at least one 16-token block is a hit
	if h := hits(); h < 16 {
		t.Fatalf("hits after a repeated prefix: %v, want at least one block", h)
	}
	resp, err := http.Post(base+"/reset_prefix_cache", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != `{"success":true}` {
		t.Fatalf("reset: %d %q", resp.StatusCode, b)
	}
	if h := hits(); h != 0 {
		t.Fatalf("hits after a reset: %v, want 0 (a fresh engine)", h)
	}
	chat(2)
	if h := hits(); h != 0 {
		t.Fatalf("the first request after a reset hit %v tokens: the cache was not emptied", h)
	}
	lf.Close()
	got, _ := os.ReadFile(logp)
	lines := strings.Split(strings.TrimSpace(string(got)), "\n")
	if len(lines) != 3 {
		t.Fatalf("placement lines: %q", got)
	}
	for i, l := range lines {
		f := strings.Split(l, "\t")
		if len(f) != 7 || f[2] != "2" || f[3] != "B1-rr" || f[4] != "7" || f[5] != strconv.Itoa(i) || f[6] != "B1-rr-s007" {
			t.Errorf("line %d: %q", i, l)
		}
		s, _ := strconv.ParseFloat(f[0], 64)
		e, _ := strconv.ParseFloat(f[1], 64)
		if e < s {
			t.Errorf("line %d ends before it starts", i)
		}
	}
	// Without dev mode the endpoint does not exist, as in vLLM.
	w.dev = false
	resp, _ = http.Post(base+"/reset_prefix_cache", "", nil)
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("reset without dev mode: %d, want 404", resp.StatusCode)
	}
}

func TestWorkerArgsRefuseUnknownFlags(t *testing.T) {
	ok := []string{"serve", "/m", "--uds", "/r/worker-0.sock", "--served-model-name", "Qwen/X", "--gpu-memory-utilization=0.6",
		"--max-model-len=16384", "--enable-prefix-caching", "--enable-prompt-tokens-details", "--disable-uvicorn-access-log",
		"--middleware=s2c_placement.Placement", "--num-gpu-blocks-override=6000", "--max-num-seqs=256"}
	a, err := parseWorkerArgs(ok)
	if err != nil || a.kvBlocks != 6000 || a.maxSeqs != 256 || a.uds != "/r/worker-0.sock" {
		t.Fatalf("%+v %v", a, err)
	}
	if _, err := parseWorkerArgs(append(ok, "--tensor-parallel-size=2")); err == nil {
		t.Error("an unknown flag accepted")
	}
}
