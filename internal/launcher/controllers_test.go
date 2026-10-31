package launcher

// Helpers shared by the supervisor tests: a fake controller binary built once from
// testdata/fakecontroller, a state directory that records it, and readers of its record files.
// Processes are judged from /proc (runningNow, waitNotRunning) and from the files the fake writes
// itself, not from the launcher's own bookkeeping.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// superviseEnv, set to a state directory, turns the test binary into "hpcburst supervise" for that
// directory: the launcher's own tests have no hpcburst executable to start.
const superviseEnv = "HPCBURST_TEST_SUPERVISE"

var (
	fakeCtlOnce sync.Once
	fakeCtlDir  string
	fakeCtlErr  error
)

// fakeControllerBin builds testdata/fakecontroller once per test run, into a directory that
// TestMain removes. It needs the go tool, as go test itself does.
func fakeControllerBin(t *testing.T) string {
	t.Helper()
	fakeCtlOnce.Do(func() {
		if fakeCtlDir, fakeCtlErr = os.MkdirTemp("", "hbfake"); fakeCtlErr != nil {
			return
		}
		cmd := exec.Command("go", "build", "-o", filepath.Join(fakeCtlDir, "hpcburst-controller"), ".")
		cmd.Dir = filepath.Join("testdata", "fakecontroller")
		if out, err := cmd.CombinedOutput(); err != nil {
			fakeCtlErr = fmt.Errorf("go build of the fake controller: %v\n%s", err, out)
		}
	})
	if fakeCtlErr != nil {
		t.Fatal(fakeCtlErr)
	}
	return filepath.Join(fakeCtlDir, "hpcburst-controller")
}

func removeFakeController() {
	if fakeCtlDir != "" {
		os.RemoveAll(fakeCtlDir)
	}
}

// runSupervisorHelper is what the test binary does when superviseEnv is set. It takes the handoff
// pipe at descriptor HandoffFD through OpenHandoff, as "hpcburst supervise" does.
func runSupervisorHelper(dir string) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	h, err := OpenHandoff(HandoffFD)
	if err != nil {
		fmt.Fprintln(os.Stderr, "supervise:", err)
		os.Exit(1)
	}
	if err := Supervise(ctx, SuperviseOptions{StateDir: dir, Handoff: h, Log: os.Stderr}); err != nil {
		fmt.Fprintln(os.Stderr, "supervise:", err)
		os.Exit(1)
	}
	os.Exit(0)
}

// ctlFixture is a state directory whose record names the fake controller, and a control
// directory for the fake.
type ctlFixture struct {
	t     *testing.T
	l     layout
	ctl   string // $FAKECTRL_DIR
	base  int    // the port base; the controller's /healthz is base+6
	ports Ports
	st    *State
}

// freePort returns a loopback port that nothing held a moment ago.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// newCtlFixture writes state.json for a host whose "k3s" is this test process (the supervisor
// needs a valid record to read, nothing more) and a record of the controller to run.
func newCtlFixture(t *testing.T, front Front) *ctlFixture {
	t.Helper()
	l, err := newLayout(filepath.Join(shortDir(t), "st"))
	if err != nil {
		t.Fatal(err)
	}
	self, err := newRec("k3s", os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	// The block is laid out as the spec has it (base, +1, +2, +3 the agent endpoint, +5, +6 the
	// controller's /healthz), around a free port for the controller, so the fake can bind it.
	base := freePort(t) - 6
	f := &ctlFixture{t: t, l: l, ctl: t.TempDir(), base: base,
		ports: Ports{API: base, HTTPS: base + 1, KCM: base + 2, Agent: base + 3, LB: base + 5, Controller: base + 6}}
	f.st = &State{Version: "v1.36.5+k3s1", Ports: f.ports, Procs: []ProcRec{self}, StartedAt: time.Now(), Front: front,
		Controllers: &ControllerConfig{Binary: fakeControllerBin(t)}}
	for _, d := range []string{l.dir, l.logDir()} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.saveState(f.st); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKECTRL_DIR", f.ctl)
	// Runs after the test's own clean-ups (it is registered first): whatever still has the state
	// directory as its working directory is a supervisor or controller the test left behind.
	t.Cleanup(func() { noLeftovers(t, l.dir) })
	return f
}

// steer creates a control file of the fake.
func (f *ctlFixture) steer(name, content string) {
	f.t.Helper()
	if err := os.WriteFile(filepath.Join(f.ctl, name), []byte(content), 0o644); err != nil {
		f.t.Fatal(err)
	}
}

func (f *ctlFixture) unsteer(name string) { _ = os.Remove(filepath.Join(f.ctl, name)) }

// confirmedHandoff is a handoff the launcher has already confirmed: the byte is in the pipe.
func confirmedHandoff(t *testing.T) io.ReadCloser {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte{handoffGo}); err != nil {
		t.Fatal(err)
	}
	w.Close()
	t.Cleanup(func() { r.Close() })
	return r
}

// supervise runs Supervise in this process until the returned stop is called (or the test ends),
// with a handoff that is already confirmed unless o names one. stop returns what Supervise returned.
func (f *ctlFixture) supervise(o SuperviseOptions) (stop func() error) {
	f.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	o.StateDir = f.l.dir
	if o.Handoff == nil {
		o.Handoff = confirmedHandoff(f.t)
	}
	if o.Log == nil {
		o.Log = testLog{f.t}
	}
	res := make(chan error, 1)
	go func() { res <- Supervise(ctx, o) }()
	var once sync.Once
	var err error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-res:
			case <-time.After(30 * time.Second):
				err = fmt.Errorf("Supervise did not return within 30 s of its context being cancelled")
			}
		})
		return err
	}
	f.t.Cleanup(func() { _ = stop() })
	return stop
}

// record reads supervisor.json, retrying while it is being replaced.
func (f *ctlFixture) record() *SupervisorFile {
	f.t.Helper()
	var last error
	for i := 0; i < 50; i++ {
		sf, err := readSupervisorFile(f.l)
		if err == nil {
			return sf
		}
		last = err
		time.Sleep(10 * time.Millisecond)
	}
	f.t.Fatalf("cannot read supervisor.json: %v", last)
	return nil
}

// await polls supervisor.json until cond holds for the controller's entry.
func (f *ctlFixture) await(what string, cond func(e EntryRecord) bool) EntryRecord {
	f.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if sf, err := readSupervisorFile(f.l); err == nil && len(sf.Entries) == 1 && cond(sf.Entries[0]) {
			return sf.Entries[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.t.Fatalf("timed out waiting for %s; supervisor.json: %s", what, fileOf(f.l.supervisorFile()))
	return EntryRecord{}
}

func fileOf(path string) string { b, _ := os.ReadFile(path); return string(b) }

func (f *ctlFixture) awaitRunning() EntryRecord {
	f.t.Helper()
	return f.await("the controller to run", func(e EntryRecord) bool { return e.State == entryRunning && e.Proc != nil })
}

// argvOf is the command line the fake recorded for pid.
func (f *ctlFixture) argvOf(pid int) []string {
	f.t.Helper()
	var v []string
	f.readJSON(fmt.Sprintf("argv-%d.json", pid), &v)
	return v
}

func (f *ctlFixture) envOf(pid int) []string {
	f.t.Helper()
	var v []string
	f.readJSON(fmt.Sprintf("env-%d.json", pid), &v)
	return v
}

func (f *ctlFixture) readJSON(name string, v any) {
	f.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		b, err := os.ReadFile(filepath.Join(f.ctl, name))
		if err == nil {
			if err := json.Unmarshal(b, v); err != nil {
				f.t.Fatalf("%s: %v", name, err)
			}
			return
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("the fake controller wrote no %s: %v", name, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// starts returns the PIDs the fake recorded at its starts, in order.
func (f *ctlFixture) starts() []int {
	var pids []int
	for _, line := range strings.Split(strings.TrimSpace(fileOf(filepath.Join(f.ctl, "starts"))), "\n") {
		if p, err := strconv.Atoi(strings.Fields(line + " ")[0]); err == nil {
			pids = append(pids, p)
		}
	}
	return pids
}

// startTicksOf reads field 22 of /proc/<pid>/stat with a parser of its own.
func startTicksOf(t *testing.T, pid int) uint64 {
	t.Helper()
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	fields := strings.Fields(s[strings.LastIndexByte(s, ')')+1:])
	n, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// healthzCode asks addr for /healthz, as curl would.
func healthzCode(addr string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return getStatus(ctx, &http.Client{Timeout: time.Second}, "http://"+addr+"/healthz")
}

// TestFakeControllerFlags: the fake accepts exactly the controller's command line and refuses the
// earlier one, so a launcher that still passes --kubeconfig, --credentials-dir or
// --agent-builds-dir fails its start in every test. "always-exit" 0 is read only after the flags
// parse: exit 0 means the command line was accepted, exit 2 that it was refused.
func TestFakeControllerFlags(t *testing.T) {
	bin := fakeControllerBin(t)
	cases := []struct {
		name string
		args []string
		want int
	}{
		{"the contract", []string{"--state-dir", "/st", "--port-base", "29220", "--health-addr", "127.0.0.1:29226"}, 0},
		{"the contract with a front", []string{"--state-dir", "/st", "--port-base", "29220", "--health-addr", "127.0.0.1:29226",
			"--front-mode", "direct", "--front-addr", "front.example.test:8443"}, 0},
		{"the earlier command line", []string{"--kubeconfig", "/st/kubeconfig", "--credentials-dir", "/st/controller/credentials", "--health-addr", "127.0.0.1:29226"}, 2},
		{"the contract and --kubeconfig", []string{"--state-dir", "/st", "--port-base", "29220", "--health-addr", "127.0.0.1:29226", "--kubeconfig", "/st/kubeconfig"}, 2},
		{"the contract and --credentials-dir", []string{"--state-dir", "/st", "--port-base", "29220", "--health-addr", "127.0.0.1:29226", "--credentials-dir", "/st/credentials"}, 2},
		{"the contract and --agent-builds-dir", []string{"--state-dir", "/st", "--port-base", "29220", "--health-addr", "127.0.0.1:29226", "--agent-builds-dir", "/b"}, 2},
		{"no state directory", []string{"--port-base", "29220", "--health-addr", "127.0.0.1:29226"}, 2},
		{"no port base", []string{"--state-dir", "/st", "--health-addr", "127.0.0.1:29226"}, 2},
		{"a port base that is not a number", []string{"--state-dir", "/st", "--port-base", "base", "--health-addr", "127.0.0.1:29226"}, 2},
		{"no health address", []string{"--state-dir", "/st", "--port-base", "29220"}, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "always-exit"), []byte("0"), 0o644); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(bin, c.args...)
			cmd.Env = append(os.Environ(), "FAKECTRL_DIR="+dir)
			err := cmd.Run()
			code := 0
			if ee, ok := err.(*exec.ExitError); ok {
				code = ee.ExitCode()
			} else if err != nil {
				t.Fatal(err)
			}
			if code != c.want {
				t.Errorf("fake controller %q exited %d, want %d", c.args, code, c.want)
			}
		})
	}
}

// TestPrepareControllerDirs: the credentials directory is <state>/credentials with mode 0700, even
// when an older one is looser; the agent builds directory is <state>/agent-builds, created with
// mode 0700 when absent and left alone (mode and contents) when it exists; the earlier
// <state>/controller/credentials is not created. The expected paths are written out from the
// shared host layout (docs/development.md, Runtime conventions).
func TestPrepareControllerDirs(t *testing.T) {
	mode := func(t *testing.T, path string) os.FileMode {
		t.Helper()
		fi, err := os.Stat(path)
		if err != nil || !fi.IsDir() {
			t.Fatalf("%s: %v, %v; want a directory", path, fi, err)
		}
		return fi.Mode().Perm()
	}
	ports := Ports{API: 29220, HTTPS: 29221, KCM: 29222, Agent: 29223, LB: 29225, Controller: 29226}
	newState := func(t *testing.T) layout {
		t.Helper()
		l, err := newLayout(filepath.Join(shortDir(t), "st"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(l.dir, 0o700); err != nil {
			t.Fatal(err)
		}
		return l
	}
	t.Run("fresh", func(t *testing.T) {
		l := newState(t)
		if err := prepareControllerDirs(l, ports); err != nil {
			t.Fatal(err)
		}
		for _, d := range []string{"controller", "credentials", "agent-builds"} {
			if m := mode(t, filepath.Join(l.dir, d)); m != 0o700 {
				t.Errorf("%s has mode %v, want 0700", d, m)
			}
		}
		if _, err := os.Stat(filepath.Join(l.dir, "controller", "credentials")); !os.IsNotExist(err) {
			t.Errorf("controller/credentials was created (%v)", err)
		}
	})
	t.Run("existing directories", func(t *testing.T) {
		l := newState(t)
		for _, d := range []string{"credentials", "agent-builds"} {
			if err := os.Mkdir(filepath.Join(l.dir, d), 0o755); err != nil {
				t.Fatal(err)
			}
		}
		build := filepath.Join(l.dir, "agent-builds", "hb-agent-x86_64")
		if err := os.WriteFile(build, []byte("build"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := prepareControllerDirs(l, ports); err != nil {
			t.Fatal(err)
		}
		if m := mode(t, filepath.Join(l.dir, "credentials")); m != 0o700 {
			t.Errorf("credentials has mode %v, want 0700: a looser one must be tightened", m)
		}
		if m := mode(t, filepath.Join(l.dir, "agent-builds")); m != 0o755 {
			t.Errorf("agent-builds has mode %v, want the operator's 0755 left alone", m)
		}
		if b, err := os.ReadFile(build); err != nil || string(b) != "build" {
			t.Errorf("the agent build was disturbed: %q, %v", b, err)
		}
	})
}
