package launcher

// Helpers shared by the supervisor tests: a fake controller binary built once from
// testdata/fakecontroller, a state directory that records it, and readers of its record files.
// Processes are judged from /proc (runningNow, waitNotRunning) and from the files the fake writes
// itself, not from the launcher's own bookkeeping.

import (
	"context"
	"encoding/json"
	"fmt"
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

// runSupervisorHelper is what the test binary does when superviseEnv is set.
func runSupervisorHelper(dir string) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	if err := Supervise(ctx, SuperviseOptions{StateDir: dir, Log: os.Stderr}); err != nil {
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
func newCtlFixture(t *testing.T, front Front, agentBuilds string) *ctlFixture {
	t.Helper()
	l, err := newLayout(filepath.Join(shortDir(t), "st"))
	if err != nil {
		t.Fatal(err)
	}
	self, err := newRec("k3s", os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	f := &ctlFixture{t: t, l: l, ctl: t.TempDir(), ports: Ports{API: 1, HTTPS: 2, KCM: 3, LB: 4, Controller: freePort(t), Agent: 5}}
	f.st = &State{Version: "v1.36.5+k3s1", Ports: f.ports, Procs: []ProcRec{self}, StartedAt: time.Now(), Front: front,
		Controllers: &ControllerConfig{Binary: fakeControllerBin(t), AgentBuildsDir: agentBuilds}}
	for _, d := range []string{l.dir, l.logDir()} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.saveState(f.st); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKECTRL_DIR", f.ctl)
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

// supervise runs Supervise in this process until the returned stop is called (or the test ends).
// stop returns what Supervise returned.
func (f *ctlFixture) supervise(o SuperviseOptions) (stop func() error) {
	f.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	o.StateDir = f.l.dir
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
	return getStatus(&http.Client{Timeout: time.Second}, "http://"+addr+"/healthz")
}
