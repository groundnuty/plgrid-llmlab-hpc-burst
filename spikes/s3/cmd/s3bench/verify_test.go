package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---- helpers ----

// TestHelperProcess is not a test: the tests below re-execute the test binary with
// S3_TEST_HELPER=listen to get a separate process that holds a listening socket.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("S3_TEST_HELPER") != "listen" {
		return
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	fmt.Println(ln.Addr().(*net.TCPAddr).Port)
	_, _ = io.Copy(io.Discard, os.Stdin) // lives until the test closes its stdin
	os.Exit(0)
}

// startHelper starts a separate process that listens on a loopback port and returns it as a
// harness proc together with the port.
func startHelper(t *testing.T) (*proc, int) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
	cmd.Env = append(os.Environ(), "S3_TEST_HELPER=listen")
	stdin, _ := cmd.StdinPipe()
	out, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &proc{name: "helper", cmd: cmd, done: make(chan struct{})}
	go func() { _ = cmd.Wait(); close(p.done) }()
	t.Cleanup(func() { stdin.Close(); _ = cmd.Process.Kill(); <-p.done })
	var port int
	if _, err := fmt.Fscan(bufio.NewReader(out), &port); err != nil || port == 0 {
		t.Fatalf("helper did not report a port: %v", err)
	}
	return p, port
}

// startSleeper starts a process that holds no socket.
func startSleeper(t *testing.T) *proc {
	t.Helper()
	p, err := startProc("sleeper", filepath.Join(t.TempDir(), "sleeper.log"), nil, "sleep", "120")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = p.stop(5 * time.Second) })
	return p
}

// fakeHost satisfies host with only what startReady touches before it launches anything.
type fakeHost struct {
	host
	listen, reserved []int
	started          bool
}

func (f *fakeHost) name() string     { return "fake" }
func (f *fakeHost) ports() hostPorts { return hostPorts{Listen: f.listen, Reserved: f.reserved} }
func (f *fakeHost) start(string, string, string) (*running, error) {
	f.started = true
	return nil, errors.New("fake host: start must not be reached in this test")
}

func busyPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().(*net.TCPAddr).Port
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return p
}

// ---- B2: a port that is already in use makes the launch fail ----

func TestStartReadyRefusesBusyPort(t *testing.T) {
	admit := func(context.Context, string) error { return nil }
	for name, mk := range map[string]func(p int) *fakeHost{
		"listen port":   func(p int) *fakeHost { return &fakeHost{listen: []int{p}} },
		"reserved port": func(p int) *fakeHost { return &fakeHost{listen: []int{freePort(t)}, reserved: []int{p}} },
	} {
		t.Run(name, func(t *testing.T) {
			f := mk(busyPort(t))
			b := &bench{h: f, res: &result{}, admit: admit, scratch: t.TempDir()}
			_, _, err := b.startReady(context.Background(), "v", t.TempDir(), "cold")
			if err == nil || !strings.Contains(err.Error(), "not free") {
				t.Fatalf("want a 'not free' error, got %v", err)
			}
			if f.started {
				t.Fatal("the host was launched although its port was taken")
			}
		})
	}
	t.Run("free ports reach the launch", func(t *testing.T) { // control: the check does not refuse everything
		f := &fakeHost{listen: []int{freePort(t)}}
		b := &bench{h: f, res: &result{}, admit: admit, scratch: t.TempDir()}
		_, _, _ = b.startReady(context.Background(), "v", t.TempDir(), "cold")
		if !f.started {
			t.Fatal("free ports must let the launch proceed")
		}
	})
}

// ---- B2: listeners must exist and belong to the launched processes ----

func TestCheckOwnedCases(t *testing.T) {
	l := func(p int) listener { return listener{"127.0.0.1", p} }
	cases := []struct {
		name         string
		expected     []int
		owned, all   []listener
		wantErrSubst string
	}{
		{"all held by the launched processes", []int{1, 2}, []listener{l(1), l(2)}, []listener{l(1), l(2), l(9)}, ""},
		{"empty set", []int{1}, nil, []listener{l(1)}, "hold no listening socket"},
		{"empty set with nothing expected", nil, nil, nil, "hold no listening socket"},
		{"foreign owner", []int{1, 2}, []listener{l(1)}, []listener{l(1), l(2)}, "another process"},
		{"missing", []int{1, 2}, []listener{l(1)}, []listener{l(1)}, "missing"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkOwned(c.expected, c.owned, c.all)
			switch {
			case c.wantErrSubst == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case c.wantErrSubst != "" && (err == nil || !strings.Contains(err.Error(), c.wantErrSubst)):
				t.Fatalf("want error containing %q, got %v", c.wantErrSubst, err)
			}
		})
	}
}

// The real /proc: a port held by another PID is rejected even though the launched process
// holds a socket of its own, the same port held by the launched PID is accepted, and a launched
// process without any socket is rejected.
func TestVerifyHostRejectsForeignListener(t *testing.T) {
	launched, ownPort := startHelper(t) // the process we "launched"
	_, foreignPort := startHelper(t)    // another PID, playing a leftover or foreign server
	idle := startSleeper(t)             // a launched process that holds no socket

	r := &running{procs: []*proc{launched}, hostPorts: []int{ownPort, foreignPort}}
	if _, err := verifyHost(r); err == nil || !strings.Contains(err.Error(), "another process") {
		t.Fatalf("a listener owned by a foreign PID must be rejected, got %v", err)
	}
	ls, err := verifyHost(&running{procs: []*proc{launched}, hostPorts: []int{ownPort}}) // control
	if err != nil {
		t.Fatalf("a listener owned by the launched PID must be accepted: %v", err)
	}
	if len(ls) == 0 || !strings.HasSuffix(ls[0], fmt.Sprintf(":%d", ownPort)) {
		t.Fatalf("listeners = %v", ls)
	}
	if _, err := verifyHost(&running{procs: []*proc{idle}, hostPorts: []int{ownPort}}); err == nil {
		t.Fatal("an empty listener set must be a failure")
	}
	_, _ = idle.stop(5 * time.Second)
	if _, err := verifyHost(&running{procs: []*proc{idle}}); err == nil || !strings.Contains(err.Error(), "exited") {
		t.Fatalf("an exited process must be a failure, got %v", err)
	}
}

// ---- B2: the controller manager launched for this run must be alive and own its port ----

func TestWaitHealthzNeedsOwnLiveProcess(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	defer srv.Close()
	port := srv.Listener.Addr().(*net.TCPAddr).Port // answers 200, but is held by this test process

	gone, err := startProc("gone", filepath.Join(t.TempDir(), "g.log"), nil, "true")
	if err != nil {
		t.Fatal(err)
	}
	<-gone.done
	if err := waitHealthz(context.Background(), gone, port, 2*time.Second); err == nil || !strings.Contains(err.Error(), "exited") {
		t.Fatalf("a 200 while the launched process is dead must fail, got %v", err)
	}
	alive := startSleeper(t)
	if err := waitHealthz(context.Background(), alive, port, 2*time.Second); err == nil || !strings.Contains(err.Error(), "not from") {
		t.Fatalf("a 200 from a server the launched process does not hold must fail, got %v", err)
	}
}

// ---- B2: an unreadable or exited process makes the sample invalid ----

func fakeProc(t *testing.T, root string, pid, ppid int, status string, mode os.FileMode) string {
	t.Helper()
	d := filepath.Join(root, fmt.Sprint(pid))
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	for f, c := range map[string]string{"stat": fmt.Sprintf("%d (x) S %d 0 0\n", pid, ppid), "comm": "x\n"} {
		if err := os.WriteFile(filepath.Join(d, f), []byte(c), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sp := filepath.Join(d, "status")
	if err := os.WriteFile(sp, []byte(status), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(sp, mode); err != nil {
		t.Fatal(err)
	}
	return sp
}

func TestRSSOfRejectsUnreadableProcess(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("mode 000 does not make a file unreadable for root")
	}
	root := t.TempDir()
	old := procRoot
	procRoot = root
	defer func() { procRoot = old }()
	fakeProc(t, root, 100, 1, "VmRSS:\t2048 kB\n", 0o644)
	child := fakeProc(t, root, 101, 100, "VmRSS:\t1024 kB\n", 0o000)

	// The earlier harness summed what it could read (2 MiB) and said nothing about the rest.
	if total, _, err := rssOf([]int{100}); err == nil {
		t.Fatalf("an unreadable descendant must invalidate the reading; got total %.1f MiB and no error", total)
	}
	if err := os.Chmod(child, 0o644); err != nil { // control: readable, the sum is exact
		t.Fatal(err)
	}
	total, _, err := rssOf([]int{100})
	if err != nil || total != 3.0 {
		t.Fatalf("control: total %.1f err %v, want 3.0 MiB", total, err)
	}
	if _, _, err := rssOf([]int{4242}); err == nil {
		t.Fatal("a required process that does not exist must be an error")
	}
	rootStatus := fakeProc(t, root, 200, 1, "VmRSS:\t512 kB\n", 0o000)
	if _, _, err := rssOf([]int{200}); err == nil {
		t.Fatalf("an unreadable root must be an error (%s)", rootStatus)
	}
	fakeProc(t, root, 300, 1, "Name:\tzombie\nState:\tZ (zombie)\n", 0o644) // no VmRSS line
	if _, _, err := rssOf([]int{300}); err == nil {
		t.Fatal("a root without VmRSS (exited, not reaped) must be an error")
	}
}

func TestSampleRejectsExitedProcess(t *testing.T) {
	oldN, oldGap := numSamples, sampleGap
	numSamples, sampleGap = 3, 300*time.Millisecond
	defer func() { numSamples, sampleGap = oldN, oldGap }()
	b := &bench{h: &fakeHost{}, res: &result{}, scratch: t.TempDir()}

	p := startSleeper(t)
	r := &running{procs: []*proc{p}}
	s, err := b.sample(context.Background(), r, nil, 0)
	if err != nil || s.MedianMiB <= 0 {
		t.Fatalf("control: a live process must sample: %v %+v", err, s)
	}
	go func() { time.Sleep(100 * time.Millisecond); _, _ = p.stop(5 * time.Second) }() // dies between readings
	if _, err := b.sample(context.Background(), r, nil, 0); err == nil || !strings.Contains(err.Error(), "exited") {
		t.Fatalf("a process that exits during sampling must reject the sample, got %v", err)
	}
	ctrl := startSleeper(t)
	live := startSleeper(t)
	_, _ = ctrl.stop(5 * time.Second)
	if _, err := b.sample(context.Background(), &running{procs: []*proc{live}}, ctrl, 0); err == nil || !strings.Contains(err.Error(), "controller") {
		t.Fatalf("an exited controller must reject the sample, got %v", err)
	}
}

func TestPortsFree(t *testing.T) {
	if err := portsFree([]int{freePort(t)}); err != nil {
		t.Fatal(err)
	}
	if err := portsFree([]int{freePort(t), busyPort(t)}); err == nil {
		t.Fatal("a taken port must be reported")
	}
}

// ---- nit 4: unexpected outcomes are problems; the expected sets are required ----

func e2e(pass, fail, skip []string, exit int) e2eResult {
	return e2eResult{Passed: pass, Failed: fail, Skipped: skip, Exit: exit}
}

func TestExpectationCheck(t *testing.T) {
	good := expectSuite(false)
	cases := []struct {
		name string
		exp  expectation
		got  e2eResult
		want string // substring of one problem; "" means no problem
	}{
		{"exact match", good, e2e(suiteTests, nil, phaseTests, 0), ""},
		{"negative control fails exactly the cascade tests", expectSuite(true),
			e2e(without(suiteTests, cascadeTests), cascadeTests, phaseTests, 1), ""},
		{"zero tests passed with exit 0", good, e2e(nil, nil, nil, 0), "no test passed"},
		{"a test is missing", good, e2e(suiteTests[1:], nil, phaseTests, 0), "TestDiscovery expected but not seen"},
		{"an unexpected failure", good, e2e(without(suiteTests, []string{"TestWatch"}), []string{"TestWatch"}, phaseTests, 1), "TestWatch seen but not expected"},
		{"negative control passes a cascade test", expectSuite(true),
			e2e(append(without(suiteTests, cascadeTests), "TestCascadeBackground"), []string{"TestCascadeForeground", "TestFinalizerThroughCascade"}, phaseTests, 1), "TestCascadeBackground seen but not expected"},
		{"failures but exit 0", expectSuite(true), e2e(without(suiteTests, cascadeTests), cascadeTests, phaseTests, 0), "exit status 0"},
		{"all pass but exit non-zero", good, e2e(suiteTests, nil, phaseTests, 1), "exit status 1"},
		{"a phase test runs in a plain run", good, e2e(suiteTests, nil, nil, 0), "TestPhaseLoad expected but not seen"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			probs := c.exp.check("t", c.got)
			if c.want == "" && len(probs) != 0 {
				t.Fatalf("unexpected problems: %v", probs)
			}
			if c.want != "" && !strings.Contains(strings.Join(probs, "|"), c.want) {
				t.Fatalf("want a problem containing %q, got %v", c.want, probs)
			}
		})
	}
}

func completeResult(neg bool, suite e2eResult, post e2eResult) *result {
	return &result{Option: "x", NegControl: neg,
		Phases: []phaseRecord{{Name: "main"}, {Name: "upgrade"}},
		E2E:    &suite,
		Upgrade: &upgradeResult{ObjectsBefore: 103, ObjectsKept: 103, StopToReadyMS: 3000,
			E2EAfter: suite, PostUpgrade: post},
	}
}

func TestFinishProblems(t *testing.T) {
	pass := e2e(suiteTests, nil, phaseTests, 0)
	bare := e2e(without(suiteTests, cascadeTests), cascadeTests, phaseTests, 1)
	postOK := e2e([]string{"TestPhasePostUpgrade"}, nil, nil, 0)
	postFail := e2e(nil, []string{"TestPhasePostUpgrade"}, nil, 1)

	r := completeResult(false, pass, postOK)
	r.finish()
	if len(r.Problems) != 0 {
		t.Fatalf("a clean result has problems: %v", r.Problems)
	}
	r = completeResult(true, bare, postFail)
	r.finish()
	if len(r.Problems) != 0 {
		t.Fatalf("the negative control's designed failures are not problems: %v", r.Problems)
	}
	for _, b := range r.Budget {
		if (b.Line == "e2e suite" || b.Line == "minor upgrade") && (b.Pass || !b.Expected) {
			t.Fatalf("budget line %+v must be a failure marked as expected", b)
		}
	}
	r = completeResult(false, bare, postFail) // the same failures on a host that must garbage-collect
	r.finish()
	if len(r.Problems) == 0 {
		t.Fatal("cascade failures on a host with garbage collection must be problems")
	}
	r = completeResult(false, e2e(nil, nil, nil, 0), postOK) // exit 0, but nothing ran
	r.finish()
	if !strings.Contains(strings.Join(r.Problems, "|"), "no test passed") {
		t.Fatalf("zero passing tests must be a problem: %v", r.Problems)
	}
	r = completeResult(false, pass, postOK)
	r.Upgrade.Error = "boom"
	r.finish()
	if !strings.Contains(strings.Join(r.Problems, "|"), "upgrade error: boom") {
		t.Fatalf("an upgrade error must be a problem: %v", r.Problems)
	}
	r = completeResult(false, pass, postOK)
	r.Phases = r.Phases[:1]
	r.finish()
	if !strings.Contains(strings.Join(r.Problems, "|"), "upgrade phase") {
		t.Fatalf("a result without its upgrade phase must be a problem: %v", r.Problems)
	}
}

func TestAuditLogs(t *testing.T) {
	dir := t.TempDir()
	clean := filepath.Join(dir, "clean.log")
	dirty := filepath.Join(dir, "dirty.log")
	_ = os.WriteFile(clean, []byte("level=info msg=\"data dir /scratch/data\"\n"), 0o600)
	_ = os.WriteFile(dirty, []byte("ok\nUsing private registry config file at /etc/rancher/k3s/registries.yaml\n"), 0o600)
	if err := auditLogs([]string{clean}, k3sForbidden); err != nil {
		t.Fatal(err)
	}
	if err := auditLogs([]string{clean, dirty}, k3sForbidden); err == nil || !strings.Contains(err.Error(), "/etc/rancher") {
		t.Fatalf("want a mention of /etc/rancher, got %v", err)
	}
}
