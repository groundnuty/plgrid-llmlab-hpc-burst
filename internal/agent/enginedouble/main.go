package enginedouble

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Environment variables the engine process reads.
const (
	EnvAPIKey = "VLLM_API_KEY"
	// EnvRole selects the program a re-executed test binary becomes.
	EnvRole = "HB_TEST_ROLE"
	// RoleEngine runs Main; RoleEngineChild is the second process of the
	// ignore-term mode.
	RoleEngine      = "engine"
	RoleEngineChild = "engine-child"
)

// engineArgs are the parts of a "vllm serve" command line that the double uses.
type engineArgs struct {
	model       string
	uds         string
	servedName  string
	maxModelLen int
	maxNumSeqs  int
	// extra are the other flags, as given.
	extra []string
}

// parseEngineArgs reads vllm-serve-style arguments: an optional leading "vllm",
// then "serve", the model, and --name=value flags (--name value for the four
// the double uses). Any other flag is kept in extra and otherwise ignored.
func parseEngineArgs(args []string) (engineArgs, error) {
	var a engineArgs
	if len(args) > 0 && args[0] == "vllm" {
		args = args[1:]
	}
	if len(args) > 0 && args[0] == "serve" {
		args = args[1:]
	}
	takes := map[string]*string{}
	var maxLen, maxSeqs string
	takes["--uds"], takes["--served-model-name"] = &a.uds, &a.servedName
	takes["--max-model-len"], takes["--max-num-seqs"] = &maxLen, &maxSeqs
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "--") {
			if a.model == "" {
				a.model = arg
			} else {
				a.extra = append(a.extra, arg)
			}
			continue
		}
		name, val, hasVal := strings.Cut(arg, "=")
		dst, known := takes[name]
		switch {
		case known && hasVal:
			*dst = val
		case known && i+1 < len(args):
			i++
			*dst = args[i]
		case known:
			return a, fmt.Errorf("%s needs a value", name)
		default:
			a.extra = append(a.extra, arg)
		}
	}
	var err error
	if a.maxModelLen, err = optInt("--max-model-len", maxLen); err != nil {
		return a, err
	}
	if a.maxNumSeqs, err = optInt("--max-num-seqs", maxSeqs); err != nil {
		return a, err
	}
	if a.uds == "" {
		return a, errors.New("no --uds: the double serves on a Unix socket only")
	}
	if a.servedName == "" {
		a.servedName = a.model
	}
	return a, nil
}

func optInt(flag, s string) (int, error) {
	if s == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s %q is not a positive number", flag, s)
	}
	return n, nil
}

// Main is the engine process: "vllm serve <model> --uds=<path>
// --served-model-name=<name> --max-model-len=<n> --max-num-seqs=<n>", with the
// API key in VLLM_API_KEY and the modes in HB_ENGINE_DOUBLE. args are the
// arguments after the program name. It serves Handler on the Unix socket until
// SIGTERM, then removes the socket and returns 0; the startup modes can make
// it return earlier or never stop on TERM (see the package doc).
func Main(args []string) int {
	fail := func(err error) int {
		fmt.Fprintln(os.Stderr, "enginedouble:", err)
		return 2
	}
	a, err := parseEngineArgs(args)
	if err != nil {
		return fail(err)
	}
	cfg, err := ParseModes(os.Getenv(EnvModes))
	if err != nil {
		return fail(err)
	}
	cfg.ServedName, cfg.ModelPath, cfg.APIKey = a.servedName, a.model, os.Getenv(EnvAPIKey)
	if a.maxModelLen > 0 {
		cfg.MaxModelLen = a.maxModelLen
	}
	cfg.MaxNumSeqs = a.maxNumSeqs
	if err := cfg.Validate(); err != nil {
		return fail(err)
	}

	var lf *logFile
	if p := os.Getenv(EnvEngineLog); p != "" {
		lf = &logFile{path: p}
	}
	logv := func(v any) {
		if lf != nil {
			if err := lf.write(v); err != nil {
				fmt.Fprintln(os.Stderr, "enginedouble: log:", err)
			}
		}
	}

	// ignore-term comes first: nothing may be logged, so nothing may be
	// signalled, before the process is ready to ignore TERM.
	var childPID int
	if cfg.IgnoreTerm {
		signal.Ignore(syscall.SIGTERM)
		if childPID, err = startChild(); err != nil {
			return fail(err)
		}
	}
	logv(LogEvent{Event: "start", Argv: args, Env: redactedEnv(os.Environ(), secretName), PID: os.Getpid()})
	if cfg.WriteCaches || cfg.WriteHome {
		if err := writeCaches(os.Getenv, cfg.WriteCaches, cfg.WriteHome); err != nil {
			return fail(err)
		}
	}
	if cfg.IgnoreTerm {
		logv(LogEvent{PIDs: []int{os.Getpid(), childPID}})
	}

	exit := make(chan int, 1)
	if cfg.ExitAfterSet {
		time.AfterFunc(cfg.ExitAfter, func() { exit <- cfg.ExitCode })
	}
	term := make(chan os.Signal, 1)
	if !cfg.IgnoreTerm {
		signal.Notify(term, syscall.SIGTERM)
	}
	if cfg.StartDelay > 0 { // exit-after and TERM both cut the delay short
		select {
		case <-time.After(cfg.StartDelay):
		case code := <-exit:
			return code
		case <-term:
			return 0
		}
	}

	_ = os.Remove(a.uds)
	ln, err := net.Listen("unix", a.uds)
	if err != nil {
		return fail(err)
	}
	srv := &http.Server{Handler: newHandler(cfg, func(ev LogEvent) { logv(ev) })}
	go func() { _ = srv.Serve(ln) }()

	select {
	case <-term:
		// Close, not Shutdown: a stalled request would hold Shutdown forever.
		_ = srv.Close()
		_ = os.Remove(a.uds)
		return 0
	case code := <-exit:
		_ = srv.Close() // the socket file stays, as after a crash
		return code
	}
}

// startChild starts the second process of the ignore-term mode: this binary
// again, with HB_TEST_ROLE=engine-child, in the same process group. It returns
// once the child has ignored TERM, so that a TERM sent afterwards cannot reach
// it unarmed.
func startChild() (int, error) {
	exe, err := os.Executable()
	if err != nil {
		return 0, err
	}
	cmd := exec.Command(exe)
	cmd.Env = append(os.Environ(), EnvRole+"="+RoleEngineChild)
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		return 0, err
	}
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	ready := make(chan error, 1)
	go func() {
		line, err := bufio.NewReader(out).ReadString('\n')
		if err == nil && strings.TrimSpace(line) != "ready" {
			err = fmt.Errorf("the child said %q", line)
		}
		ready <- err
	}()
	select {
	case err := <-ready:
		if err != nil {
			_ = cmd.Process.Kill()
			return 0, fmt.Errorf("the ignore-term child: %w", err)
		}
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		return 0, errors.New("the ignore-term child did not come up")
	}
	go func() { _ = cmd.Wait() }()
	return cmd.Process.Pid, nil
}

// ChildMain is the ignore-term child: it ignores TERM, says "ready", and waits
// to be killed. It exits if its parent goes away, so a failed test leaves no
// process behind.
func ChildMain() int {
	signal.Ignore(syscall.SIGTERM)
	fmt.Println("ready")
	parent := os.Getppid()
	for {
		time.Sleep(100 * time.Millisecond)
		if os.Getppid() != parent {
			return 0
		}
	}
}

// writeCaches is the write-caches and write-home modes. write-caches writes
// one file in each cache a real engine fills at start, each where its variable
// points, as #94's run found them (issue #94, comment 6550908687, finding 5):
// vLLM (VLLM_CACHE_ROOT, else XDG_CACHE_HOME/vllm), TorchInductor
// (TORCHINDUCTOR_CACHE_DIR, when set), Triton (TRITON_CACHE_DIR, else
// HOME/.triton/cache), FlashInfer (FLASHINFER_WORKSPACE_BASE, else HOME, then
// .cache/flashinfer), the CUDA compute cache (CUDA_CACHE_PATH, else
// HOME/.nv/ComputeCache) and an XDG writer (XDG_CACHE_HOME, else HOME/.cache).
// write-home writes one file under HOME whatever is set, as a cache that
// ignores its variable does; in a container without the home directory it
// fails, and so does the start.
func writeCaches(getenv func(string) string, caches, homeOnly bool) error {
	home := getenv("HOME")
	or := func(name, def string) string {
		if v := getenv(name); v != "" {
			return v
		}
		return def
	}
	xdg := or("XDG_CACHE_HOME", filepath.Join(home, ".cache"))
	var dirs []string
	if caches {
		dirs = append(dirs,
			filepath.Join(xdg, "hb-double-jit"),
			filepath.Join(or("VLLM_CACHE_ROOT", filepath.Join(xdg, "vllm")), "torch_compile_cache"),
			or("TRITON_CACHE_DIR", filepath.Join(home, ".triton", "cache")),
			filepath.Join(or("FLASHINFER_WORKSPACE_BASE", home), ".cache", "flashinfer"),
			or("CUDA_CACHE_PATH", filepath.Join(home, ".nv", "ComputeCache")),
		)
		if d := getenv("TORCHINDUCTOR_CACHE_DIR"); d != "" {
			dirs = append(dirs, d)
		}
	}
	if homeOnly {
		dirs = append(dirs, filepath.Join(home, ".hb-double-ignores-variables"))
	}
	for _, d := range dirs {
		if !filepath.IsAbs(d) {
			return fmt.Errorf("write-caches: cache directory %q is not absolute (HOME %q)", d, home)
		}
		if err := os.MkdirAll(d, 0o700); err != nil {
			return fmt.Errorf("write-caches: %w", err)
		}
		if err := os.WriteFile(filepath.Join(d, "entry"), []byte("cached\n"), 0o600); err != nil {
			return fmt.Errorf("write-caches: %w", err)
		}
	}
	return nil
}
