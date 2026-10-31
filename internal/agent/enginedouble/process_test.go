package enginedouble

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// installed returns a directory holding the fake apptainer and ssh, which are
// symlinks to this test binary.
func installed(t *testing.T) string {
	t.Helper()
	dir := shortDir(t)
	if err := Install(dir); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"apptainer", "ssh"} {
		exe, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		target, err := filepath.EvalSymlinks(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if want, _ := filepath.EvalSymlinks(exe); target != want {
			t.Fatalf("%s links to %s, want the test binary %s", name, target, want)
		}
	}
	return dir
}

// proc is a started process in its own process group, killed with its group at
// the end of the test, and its exit.
type proc struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
	out  bytes.Buffer
}

func start(t *testing.T, cmd *exec.Cmd) *proc {
	t.Helper()
	p := &proc{cmd: cmd, done: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = &p.out, &p.out
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.err = cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			t.Errorf("process %d did not end after KILL", cmd.Process.Pid)
		}
	})
	return p
}

func (p *proc) pid() int { return p.cmd.Process.Pid }

// signalGroup sends sig to the process group.
func (p *proc) signalGroup(sig syscall.Signal) { _ = syscall.Kill(-p.pid(), sig) }

// exitCode waits up to d for the process to end and returns its exit code, or
// -1 if a signal ended it.
func (p *proc) exitCode(t *testing.T, d time.Duration) int {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(d):
		t.Fatalf("process %d still running after %v; output:\n%s", p.pid(), d, p.out.String())
	}
	var ee *exec.ExitError
	if p.err == nil {
		return 0
	}
	if !errors.As(p.err, &ee) {
		t.Fatalf("waiting: %v", p.err)
	}
	return ee.ExitCode()
}

// alive says whether the process exists and is not a zombie, which kill(pid, 0)
// cannot tell.
func alive(pid int) bool {
	if syscall.Kill(pid, 0) != nil {
		return false
	}
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	// "pid (comm) S ...": the state follows the last ")"
	i := bytes.LastIndexByte(b, ')')
	return i >= 0 && i+2 < len(b) && b[i+2] != 'Z'
}

// waitSocket waits until something accepts on the Unix socket.
func waitSocket(t *testing.T, path string) {
	t.Helper()
	eventually(t, 10*time.Second, "a listener on "+path, func() bool {
		c, err := net.Dial("unix", path)
		if err == nil {
			_ = c.Close()
		}
		return err == nil
	})
}

// readLog returns the JSON lines of a log file as LogEvents.
func readLog(t *testing.T, path string) []LogEvent {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	var out []LogEvent
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var ev LogEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		out = append(out, ev)
	}
	return out
}

// engineCmd starts this test binary as the engine, with the given modes.
func engineCmd(t *testing.T, socket, logPath, modes string, extraArgs ...string) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	args := append([]string{"serve", "/ckpt", "--uds=" + socket, "--served-model-name=m"}, extraArgs...)
	cmd := exec.Command(exe, args...)
	cmd.Env = []string{EnvRole + "=" + RoleEngine, EnvModes + "=" + modes, EnvEngineLog + "=" + logPath}
	return cmd
}

// The ignore-term mode: TERM to the process group leaves both processes alive,
// KILL to the group ends both. The control: without it TERM ends the engine.
func TestIgnoreTermSurvivesTermAndDiesOnKill(t *testing.T) {
	dir := shortDir(t)
	socket, logPath := sockPath(t, dir, "e.sock"), filepath.Join(dir, "log")
	p := start(t, engineCmd(t, socket, logPath, "ignore-term"))

	var pids []int
	eventually(t, 10*time.Second, "the pids line in the log", func() bool {
		for _, ev := range readLog(t, logPath) {
			if len(ev.PIDs) > 0 {
				pids = ev.PIDs
			}
		}
		return pids != nil
	})
	waitSocket(t, socket)
	if len(pids) != 2 || pids[0] != p.pid() || pids[1] == p.pid() {
		t.Fatalf("pids = %v, want the engine's %d and another", pids, p.pid())
	}
	if !alive(pids[0]) || !alive(pids[1]) {
		t.Fatalf("not both alive before the signal: %v", pids)
	}

	p.signalGroup(syscall.SIGTERM)
	time.Sleep(300 * time.Millisecond)
	for _, pid := range pids {
		if !alive(pid) {
			t.Errorf("process %d ended on TERM, ignore-term must survive it", pid)
		}
	}

	p.signalGroup(syscall.SIGKILL)
	if code := p.exitCode(t, 3*time.Second); code != -1 {
		t.Errorf("the engine exited with code %d, want it killed by the signal", code)
	}
	eventually(t, 3*time.Second, "the second process to end on KILL", func() bool { return !alive(pids[1]) })
}

func TestTermEndsTheEngineWithoutIgnoreTerm(t *testing.T) {
	dir := shortDir(t)
	socket := sockPath(t, dir, "e.sock")
	p := start(t, engineCmd(t, socket, filepath.Join(dir, "log"), ""))
	waitSocket(t, socket)
	p.signalGroup(syscall.SIGTERM)
	if code := p.exitCode(t, 2*time.Second); code != 0 {
		t.Errorf("exit code %d on TERM, want 0", code)
	}
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Errorf("the socket is still there after TERM: %v", err)
	}
}

func TestExitAfterAndStartDelay(t *testing.T) {
	dir := shortDir(t)
	t0 := time.Now()
	p := start(t, engineCmd(t, sockPath(t, dir, "a.sock"), filepath.Join(dir, "alog"), "exit-after=300ms:3"))
	if code := p.exitCode(t, 5*time.Second); code != 3 || time.Since(t0) < 300*time.Millisecond {
		t.Errorf("exit-after=300ms:3: code %d after %v, want 3 after at least 300ms", code, time.Since(t0))
	}
	q := start(t, engineCmd(t, sockPath(t, dir, "b.sock"), filepath.Join(dir, "blog"), "exit-after=100ms"))
	if code := q.exitCode(t, 5*time.Second); code != 0 {
		t.Errorf("exit-after=100ms: code %d, want 0", code)
	}

	late := sockPath(t, dir, "c.sock")
	start(t, engineCmd(t, late, filepath.Join(dir, "clog"), "start-delay=3s"))
	time.Sleep(300 * time.Millisecond)
	if _, err := os.Stat(late); err == nil {
		t.Error("the socket exists during start-delay")
	}
	waitSocket(t, late)
}

// The engine process takes its settings from the vllm-serve arguments, the key
// from VLLM_API_KEY and the modes from HB_ENGINE_DOUBLE, and logs what it was
// started with and each request.
func TestEngineMainArgsEnvAndLog(t *testing.T) {
	dir := shortDir(t)
	socket, logPath := sockPath(t, dir, "e.sock"), filepath.Join(dir, "log")
	cmd := engineCmd(t, socket, logPath, "kv-low", "--max-model-len=2048", "--max-num-seqs", "4", "--gpu-memory-utilization=0.5", "--enable-chunked-prefill")
	cmd.Env = append(cmd.Env, EnvAPIKey+"=the-key")
	p := start(t, cmd)
	waitSocket(t, socket)
	c := unixClient(socket)
	s := &testServer{t: t, hc: c}

	if r := s.get("/v1/models"); r.status != 401 {
		t.Errorf("/v1/models without the key: HTTP %d, want 401", r.status)
	}
	var models struct {
		Data []struct {
			ID          string `json:"id"`
			Root        string `json:"root"`
			MaxModelLen int    `json:"max_model_len"`
		}
	}
	s.do("GET", "/v1/models", "the-key", nil).json(t, &models)
	if d := models.Data[0]; d.ID != "m" || d.Root != "/ckpt" || d.MaxModelLen != 2048 {
		t.Errorf("model card = %+v, want m, /ckpt, 2048 from the arguments", d)
	}
	if lookup(labelsOf(t, grepLine(s.metrics(), "vllm:cache_config_info")), "kv_cache_size_tokens") != "1000" {
		t.Error("kv-low from HB_ENGINE_DOUBLE did not reach the engine")
	}
	s.do("POST", "/v1/chat/completions", "the-key", chatBody(3, map[string]any{"max_tokens": 4, "ignore_eos": true, "stream": true}))
	p.signalGroup(syscall.SIGTERM)
	p.exitCode(t, 2*time.Second)

	log := readLog(t, logPath)
	if len(log) < 2 || log[0].Event != "start" {
		t.Fatalf("log = %+v, want a start line first", log)
	}
	st := log[0]
	wantArgv := []string{"serve", "/ckpt", "--uds=" + socket, "--served-model-name=m", "--max-model-len=2048", "--max-num-seqs", "4", "--gpu-memory-utilization=0.5", "--enable-chunked-prefill"}
	if strings.Join(st.Argv, "\x00") != strings.Join(wantArgv, "\x00") || st.PID != p.pid() {
		t.Errorf("start = argv %q pid %d, want argv %q pid %d", st.Argv, st.PID, wantArgv, p.pid())
	}
	if !contains(st.Env, EnvAPIKey+"="+sha256Ref("the-key")) {
		t.Errorf("start env %q lacks VLLM_API_KEY=<sha256 of the key>", st.Env)
	}
	raw, _ := os.ReadFile(logPath)
	if bytes.Contains(raw, []byte("the-key")) {
		t.Error("the API key is in the log in clear")
	}
	var req LogEvent
	for _, ev := range log {
		if ev.Event == "request" {
			req = ev
		}
	}
	if req.Path != "/v1/chat/completions" || req.PromptTokens != 12 || req.MaxTokens != 4 || !req.Stream || req.Outcome != "completed" || req.Tokens != 4 {
		t.Errorf("request line = %+v, want the streamed 12-token prompt, max 4, completed after 4 tokens", req)
	}
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func grepLine(text, prefix string) string {
	for _, l := range strings.Split(text, "\n") {
		if strings.HasPrefix(l, prefix) {
			return l
		}
	}
	return ""
}

// sha256Ref is the sha256 hex of s, computed here and not with the double's
// helper.
func sha256Ref(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// listenTestPort binds 127.0.0.1 in the #60 block's test range (29290-29299)
// and returns the listener.
func listenTestPort(t *testing.T) net.Listener {
	t.Helper()
	for port := 29290; port <= 29299; port++ {
		if ln, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port)); err == nil {
			t.Cleanup(func() { _ = ln.Close() })
			return ln
		}
	}
	t.Fatal("no free port in 29290-29299")
	return nil
}
