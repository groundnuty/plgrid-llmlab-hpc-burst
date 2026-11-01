package main

// Integration tests of the hpcburst command against a real k3s, started as an ordinary user
// on this machine. They drive the command the way a user does: the test binary re-executes
// itself as the CLI (see TestMain), so k3s is started by one process and outlives it, exactly
// as in production.
//
// Every judgement here is made with tools that do not share code with the launcher: ss(8) for
// listeners, /proc read with a separate parser, client-go for objects (UIDs from the create
// responses), and the root-owned k3s's own listeners. Tests that start k3s need pinned binaries:
// HPCBURST_BIN_DIR must point at a directory laid out by hack/fetch-k3s.sh. Without them the tests
// skip, loudly, unless HPCBURST_REQUIRE_INTEGRATION=1, which turns the skip into a failure (also
// the skip that -short asks for). hack/test-launcher.sh sets that variable and fails on any skip.

import (
	"bytes"
	"context"
	"fmt"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const asCLIEnv = "HPCBURST_TEST_AS_CLI"

func TestMain(m *testing.M) {
	if os.Getenv(asCLIEnv) == "1" {
		main() // runs the command and exits
		return
	}
	rootK3sStart = rootK3sPorts()
	code := m.Run()
	removeFakeController()
	removeFrontBinary()
	os.Exit(code)
}

type cliResult struct {
	out, err string
	code     int
}

// leakyEnv is the launcher's environment in these tests. It carries variables that would
// change k3s's behaviour if they reached it.
func leakyEnv(extra ...string) []string {
	return append(append(os.Environ(), asCLIEnv+"=1",
		"K3S_TOKEN=leaked-token", "K3S_URL=https://leak.example.test:6443", "K3S_NODE_NAME=leaked-node",
		"KUBECONFIG=/etc/rancher/k3s/k3s.yaml", "HTTPS_PROXY=http://leak.example.test:3128"), extra...)
}

// cli runs `hpcburst args...` in a child process and returns its output and exit code.
func cli(t *testing.T, args ...string) cliResult {
	t.Helper()
	return cliIn(t, "", args...)
}

// cliIn is cli with the child's working directory set to dir (empty: this process's). The test
// binary is named by an absolute path, which a relative os.Args[0] would not survive.
func cliIn(t *testing.T, dir string, args ...string) cliResult {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(self, args...)
	cmd.Dir = dir
	cmd.Env = leakyEnv()
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	err = cmd.Run()
	code := 0
	if err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("run %v: %v", args, err)
		}
		code = ee.ExitCode()
	}
	where := ""
	if dir != "" {
		where = "(in " + dir + ") "
	}
	t.Logf("%s$ hpcburst %s\nexit %d\n%s%s", where, strings.Join(args, " "), code, so.String(), se.String())
	return cliResult{so.String(), se.String(), code}
}

// ---- gates ----

func requireIntegration() bool { return os.Getenv("HPCBURST_REQUIRE_INTEGRATION") == "1" }

func binDirOrSkip(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		if requireIntegration() {
			t.Fatal("HPCBURST_REQUIRE_INTEGRATION=1, but -short asks to skip the tests that start k3s: a skip is not a pass")
		}
		t.Skip("short mode: the tests that start k3s are skipped")
	}
	dir := os.Getenv("HPCBURST_BIN_DIR")
	missing := ""
	if dir == "" {
		missing = "HPCBURST_BIN_DIR is not set"
	} else {
		for _, v := range []string{"1.35.9", "1.36.5"} {
			if _, err := os.Stat(filepath.Join(dir, "k3s-"+v, "k3s")); err != nil {
				missing = err.Error()
			}
		}
	}
	if missing != "" {
		msg := "SKIPPED, NOT TESTED: no pinned k3s binaries (" + missing + "); run hack/fetch-k3s.sh and set HPCBURST_BIN_DIR, or use hack/test-launcher.sh"
		if requireIntegration() {
			t.Fatal(msg)
		}
		t.Skip(msg)
	}
	return dir
}

func fileOf(path string) string { b, _ := os.ReadFile(path); return string(b) }

// gate waits for a quiet machine, as the shared-VM rules require before starting a server:
// 1-minute load at most 4.0, at least 8 GiB of memory available (HPCBURST_TEST_MIN_AVAIL_GIB
// changes the 8), at least 3 GiB free on the disk that holds the test directories.
func gate(t *testing.T) {
	t.Helper()
	minAvailGiB := 8.0
	if v := os.Getenv("HPCBURST_TEST_MIN_AVAIL_GIB"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f < 0 {
			t.Fatalf("HPCBURST_TEST_MIN_AVAIL_GIB=%q is not a number of GiB", v)
		}
		minAvailGiB = f
	}
	deadline := time.Now().Add(15 * time.Minute)
	for {
		load, _ := strconv.ParseFloat(strings.Fields(fileOf("/proc/loadavg"))[0], 64)
		var availKB float64
		for _, l := range strings.Split(fileOf("/proc/meminfo"), "\n") {
			if strings.HasPrefix(l, "MemAvailable:") {
				availKB, _ = strconv.ParseFloat(strings.Fields(l)[1], 64)
			}
		}
		var st syscall.Statfs_t
		_ = syscall.Statfs(os.TempDir(), &st)
		freeGiB := float64(st.Bavail) * float64(st.Bsize) / (1 << 30)
		if load <= 4.0 && availKB/1048576 >= minAvailGiB && freeGiB >= 3 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("machine stayed busy for 15 min: load %.1f, %.1f GiB available, %.1f GiB free in %s", load, availKB/1048576, freeGiB, os.TempDir())
		}
		t.Logf("waiting for a quiet machine: load %.1f, %.1f GiB available, %.1f GiB free", load, availKB/1048576, freeGiB)
		time.Sleep(30 * time.Second)
	}
}

// ---- ss(8): listeners, with owners ----

type ssRow struct {
	addr string
	port int
	pids []int
}

var pidRe = regexp.MustCompile(`pid=(\d+)`)

func ssListeners(t testing.TB) []ssRow {
	t.Helper()
	out, err := exec.Command("ss", "-ltnpH").Output()
	if err != nil {
		t.Fatalf("ss: %v (these tests need ss(8) from iproute2)", err)
	}
	var rows []ssRow
	for _, l := range strings.Split(string(out), "\n") {
		f := strings.Fields(l)
		if len(f) < 4 {
			continue
		}
		i := strings.LastIndex(f[3], ":")
		port, _ := strconv.Atoi(f[3][i+1:])
		r := ssRow{addr: strings.Trim(f[3][:i], "[]"), port: port}
		for _, m := range pidRe.FindAllStringSubmatch(l, -1) {
			p, _ := strconv.Atoi(m[1])
			r.pids = append(r.pids, p)
		}
		rows = append(rows, r)
	}
	return rows
}

// listenersOf returns "addr:port" for every listener whose owners include pid, sorted.
func listenersOf(t testing.TB, pid int) []string {
	var out []string
	for _, r := range ssListeners(t) {
		for _, p := range r.pids {
			if p == pid {
				out = append(out, fmt.Sprintf("%s:%d", r.addr, r.port))
			}
		}
	}
	sort.Strings(out)
	return out
}

func listeningOn(t testing.TB, port int) bool {
	for _, r := range ssListeners(t) {
		if r.port == port {
			return true
		}
	}
	return false
}

// launcherPortsLo and launcherPortsHi bound the launcher tests' two blocks of 13 ports: the
// default base 29220 (29220-29232) and the second base 29233 (29233-29245) of
// TestUpRefusesWhenAPortIsTaken.
const launcherPortsLo, launcherPortsHi = 29220, 29245

// listeningInRange lists every listener, whoever owns it, on a port in [lo, hi].
func listeningInRange(t testing.TB, lo, hi int) []string {
	var out []string
	for _, r := range ssListeners(t) {
		if r.port >= lo && r.port <= hi {
			out = append(out, fmt.Sprintf("%s:%d pids=%v", r.addr, r.port, r.pids))
		}
	}
	return out
}

// ---- the root-owned k3s on this machine ----

var (
	rootK3sStart  []int
	forbiddenSys  = []string{"/etc/rancher", "/var/lib/rancher", "/run/k3s"}
	rootK3sNeeded = []int{6443, 6444, 10250}
)

// rootK3sPorts returns which of the root k3s's ports (rootK3sNeeded) listen now.
func rootK3sPorts() []int {
	out, err := exec.Command("ss", "-ltnH").Output()
	if err != nil {
		return nil
	}
	var have []int
	for _, p := range rootK3sNeeded {
		for _, l := range strings.Split(string(out), "\n") {
			f := strings.Fields(l)
			if len(f) >= 4 && strings.HasSuffix(f[3], ":"+strconv.Itoa(p)) {
				have = append(have, p)
				break
			}
		}
	}
	return have
}

func sysStamp() map[string]string {
	m := map[string]string{}
	for _, p := range forbiddenSys {
		if fi, err := os.Stat(p); err == nil {
			m[p] = fi.ModTime().UTC().Format(time.RFC3339Nano)
		} else {
			m[p] = "absent"
		}
	}
	return m
}

// guardRootK3s records the root k3s's listeners and its system paths now, and checks at the end of
// the test that they are unchanged. HPCBURST_TEST_ROOT_K3S=1 states that a root k3s must be
// running on this machine: then a missing one fails the test at the start instead of making the
// check vacuous.
func guardRootK3s(t *testing.T) {
	t.Helper()
	before := rootK3sPorts()
	if os.Getenv("HPCBURST_TEST_ROOT_K3S") == "1" && len(before) != len(rootK3sNeeded) {
		t.Fatalf("HPCBURST_TEST_ROOT_K3S=1, but the root k3s listens only on %v of %v", before, rootK3sNeeded)
	}
	stamp := sysStamp()
	t.Cleanup(func() {
		after := rootK3sPorts()
		if fmt.Sprint(after) != fmt.Sprint(before) {
			t.Errorf("root k3s listeners changed: before %v, after %v", before, after)
		}
		if fmt.Sprint(sysStamp()) != fmt.Sprint(stamp) {
			t.Errorf("a system k3s path changed: before %v, after %v", stamp, sysStamp())
		}
		out, _ := exec.Command("ss", "-ltnH").Output()
		var keep []string
		for _, l := range strings.Split(string(out), "\n") {
			if strings.Contains(l, ":6443 ") || strings.Contains(l, ":6444 ") || strings.Contains(l, ":10250 ") {
				keep = append(keep, strings.Join(strings.Fields(l), " "))
			}
		}
		t.Logf("root k3s check after the test: listens on %v (before: %v)\n$ ss -ltnH | grep -E ':(6443|6444|10250) '\n%s", after, before, strings.Join(keep, "\n"))
	})
}

// ---- a state directory with a guaranteed clean-up ----

type host struct {
	t      *testing.T
	dir    string
	binDir string
}

// newHost makes a state directory with a short path, and registers the clean-up: stop
// whatever was started (through the CLI), check nothing of ours still listens or runs, delete the
// directory. Registered after guardRootK3s, so the root k3s check runs last.
func newHost(t *testing.T) *host {
	t.Helper()
	bin := binDirOrSkip(t)
	guardRootK3s(t)
	gate(t)
	d := shortTmp(t, false)
	h := &host{t: t, dir: filepath.Join(d, "st"), binDir: bin}
	t.Cleanup(func() {
		if _, err := os.Stat(h.dir); err == nil {
			// The default grace: a host that runs the front refuses a shorter one.
			r := cli(t, "down", "--state-dir", h.dir)
			if r.code != 0 {
				t.Errorf("clean-up: down exited %d: %s", r.code, r.err)
			}
		}
		if pids := k3sProcessesUsing(h.dir); len(pids) > 0 {
			t.Errorf("clean-up: processes still use %s: %v", h.dir, pids)
		}
	})
	return h
}

// shortTmp makes a temporary directory with a short path (k3s's unix sockets limit how long a
// state directory path may be, and t.TempDir names are long). With a long $TMPDIR it falls back
// to /tmp. It is removed at the end of the test unless keep is true.
func shortTmp(t testing.TB, keep bool) string {
	t.Helper()
	d, err := os.MkdirTemp("", "hb")
	if err != nil {
		t.Fatal(err)
	}
	if len(d) > 60 {
		os.RemoveAll(d)
		if d, err = os.MkdirTemp("/tmp", "hb"); err != nil {
			t.Fatal(err)
		}
	}
	if !keep {
		t.Cleanup(func() { os.RemoveAll(d) })
	}
	return d
}

func (h *host) args(extra ...string) []string {
	return append(extra, "--state-dir", h.dir)
}

func (h *host) upArgs(base int, extra ...string) []string {
	return append([]string{"up", "--state-dir", h.dir, "--bin-dir", h.binDir, "--port-base", strconv.Itoa(base), "--nice", "10"}, extra...)
}

// frpsArgs names the pinned frps for an up that runs the controllers, which also runs frps
// (HBEP-6): the frps of $HPCBURST_FRP_DIR, so the same frps runs here and in CI, where it is not
// under --bin-dir. testenv.FrpBinaries skips without it, or fails under HPCBURST_REQUIRE_FRP=1.
func frpsArgs(t *testing.T) []string {
	t.Helper()
	frps, _ := testenv.FrpBinaries(t)
	return []string{"--frps-bin", frps}
}

// k3sProcessesUsing finds processes whose environment names the state directory (HOME is inside
// it). It reads /proc itself, independently of the launcher's records.
func k3sProcessesUsing(dir string) []int {
	ents, _ := os.ReadDir("/proc")
	var out []int
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		if env, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid)); err == nil && bytes.Contains(env, []byte(dir)) {
			if running(pid) {
				out = append(out, pid)
			}
		}
	}
	return out
}

// procState reads the state letter of /proc/<pid>/stat with its own parser. 0: no such process.
func procState(pid int) byte {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0
	}
	s := string(b)
	rest := strings.TrimSpace(s[strings.LastIndex(s, ")")+1:])
	return rest[0]
}

// running is true while any thread of the process lives. The state letter of the thread-group
// leader alone is not enough: it reads Z while another thread runs, for instance while k3s
// re-executes itself from a non-leader thread.
func running(pid int) bool {
	st := procState(pid)
	if st == 0 {
		return false
	}
	if st != 'Z' && st != 'X' {
		return true
	}
	tasks, _ := os.ReadDir(fmt.Sprintf("/proc/%d/task", pid))
	for _, tk := range tasks {
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%s/stat", pid, tk.Name()))
		if err != nil {
			continue
		}
		s := string(b)
		if c := strings.TrimSpace(s[strings.LastIndex(s, ")")+1:])[0]; c != 'Z' && c != 'X' {
			return true
		}
	}
	return false
}

func startTicks(t testing.TB, pid int) uint64 {
	t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	f := strings.Fields(s[strings.LastIndex(s, ")")+1:])
	v, err := strconv.ParseUint(f[19], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func waitFor(t testing.TB, d time.Duration, what string, ok func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if ok() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", d, what)
}

// ---- clients ----

type clients struct {
	cs  *kubernetes.Clientset
	dyn dynamic.Interface
}

func (h *host) clients() clients {
	h.t.Helper()
	cfg, err := clientcmd.BuildConfigFromFlags("", filepath.Join(h.dir, "kubeconfig"))
	if err != nil {
		h.t.Fatal(err)
	}
	cfg.Timeout = 30 * time.Second
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		h.t.Fatal(err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		h.t.Fatal(err)
	}
	return clients{cs, dyn}
}

func testCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Minute)
}

func itoa(i int) string      { return strconv.Itoa(i) }
func itoa64(i uint64) string { return strconv.FormatUint(i, 10) }
