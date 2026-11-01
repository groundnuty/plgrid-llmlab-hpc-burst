package main

// Integration tests of the controllers' supervision against a real k3s, with a fake controller
// binary (internal/launcher/testdata/fakecontroller) in place of hpcburst-controller. The judgements
// are made from /proc, ss(8), client-go and the files the fake writes, not from the launcher's own
// reports: a status that agreed with itself would prove nothing.

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

var (
	fakeCtlOnce sync.Once
	fakeCtlDir  string
	fakeCtlErr  error
)

// fakeController builds the fake controller once per run and returns a copy of it, named
// hpcburst-controller, in a directory of the test's own, which is also where the fake writes its
// records (it records beside its binary when $FAKECTRL_DIR is not set).
func fakeController(t *testing.T) (bin, dir string) {
	t.Helper()
	return fakeBinary(t, "hpcburst-controller")
}

// fakeFront returns a copy of the same fake installed as hpcburst-front, the name under which it
// takes the front's command line and reports a usage state (its usage file) in /healthz.
func fakeFront(t *testing.T) (bin, dir string) {
	t.Helper()
	return fakeBinary(t, "hpcburst-front")
}

func fakeBinary(t *testing.T, name string) (bin, dir string) {
	t.Helper()
	fakeCtlOnce.Do(func() {
		if fakeCtlDir, fakeCtlErr = os.MkdirTemp("", "hbfake"); fakeCtlErr != nil {
			return
		}
		cmd := exec.Command("go", "build", "-o", filepath.Join(fakeCtlDir, "fake"), ".")
		cmd.Dir = filepath.Join("..", "..", "internal", "launcher", "testdata", "fakecontroller")
		if out, err := cmd.CombinedOutput(); err != nil {
			fakeCtlErr = fmt.Errorf("go build of the fake controller: %v\n%s", err, out)
		}
	})
	if fakeCtlErr != nil {
		t.Fatal(fakeCtlErr)
	}
	b, err := os.ReadFile(filepath.Join(fakeCtlDir, "fake"))
	if err != nil {
		t.Fatal(err)
	}
	dir = t.TempDir()
	bin = filepath.Join(dir, name)
	if err := os.WriteFile(bin, b, 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, dir
}

func removeFakeController() {
	if fakeCtlDir != "" {
		os.RemoveAll(fakeCtlDir)
	}
}

// stateProc is a process of state.json.
type stateProc struct {
	Name       string `json:"name"`
	PID        int    `json:"pid"`
	StartTicks uint64 `json:"start_ticks"`
}

func recordedProcs(t *testing.T, dir string) []stateProc {
	t.Helper()
	var s struct {
		Processes []stateProc `json:"processes"`
	}
	parseJSON(t, fileOf(filepath.Join(dir, "state.json")), &s)
	return s.Processes
}

// supervisorRecord is the part of supervisor.json the tests read.
type supervisorRecord struct {
	Supervisor stateProc `json:"supervisor"`
	Entries    []struct {
		Name     string     `json:"name"`
		State    string     `json:"state"`
		Proc     *stateProc `json:"proc"`
		Restarts int        `json:"restarts"`
		LastExit *struct {
			Code   int    `json:"code"`
			Detail string `json:"detail"`
		} `json:"last_exit"`
	} `json:"entries"`
}

// readSupervisorRecord reads the record of a host that runs the controller and frps (HBEP-6),
// in that order.
func readSupervisorRecord(t testing.TB, dir string) (supervisorRecord, bool) {
	r, ok := readSupervisorRecordN(t, dir, 2)
	return r, ok && r.Entries[1].Name == "frps"
}

// readSupervisorRecordN reads supervisor.json, which must hold exactly n entries, the controller's
// first.
func readSupervisorRecordN(t testing.TB, dir string, n int) (supervisorRecord, bool) {
	var r supervisorRecord
	b, err := os.ReadFile(filepath.Join(dir, "controller", "supervisor.json"))
	if err != nil || json.Unmarshal(b, &r) != nil || len(r.Entries) != n || r.Entries[0].Name != "controller" {
		return r, false
	}
	return r, true
}

// ppidOfPID reads the parent of pid from /proc with a parser of its own.
func ppidOfPID(t *testing.T, pid int) int {
	t.Helper()
	s := fileOf("/proc/" + itoa(pid) + "/stat")
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		t.Fatalf("no /proc/%d/stat", pid)
	}
	n, err := strconv.Atoi(strings.Fields(s[i+1:])[1])
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// statusJSON runs status --json and decodes the parts the tests judge.
type controllerStatus struct {
	Health      string `json:"health"`
	Reasons     []string
	FrontAbsent bool `json:"front_absent"`
	Supervisor  *struct {
		Running bool `json:"running"`
		PID     int  `json:"pid"`
	} `json:"supervisor"`
	Controllers []struct {
		Name       string `json:"name"`
		Running    bool   `json:"running"`
		PID        int    `json:"pid"`
		Restarts   int    `json:"restarts"`
		HealthzOK  *bool  `json:"healthz_ok"`
		ReadyzOK   *bool  `json:"readyz_ok"`
		UsageState string `json:"usage_state"`
		Degraded   string `json:"degraded"`
		LastExit   *struct {
			Detail string `json:"detail"`
		} `json:"last_exit"`
	} `json:"controllers"`
}

func statusOf(t *testing.T, h *host) (controllerStatus, int) {
	t.Helper()
	r := cli(t, h.args("status", "--json", "--no-gc-probe")...)
	var s controllerStatus
	parseJSON(t, r.out, &s)
	return s, r.code
}

// verifiedKill sends SIGKILL to the process only after /proc agrees that it is the recorded one.
func verifiedKill(t *testing.T, p stateProc) {
	t.Helper()
	if !running(p.PID) || startTicks(t, p.PID) != p.StartTicks {
		t.Fatalf("pid %d is not the recorded %s (start time %d)", p.PID, p.Name, p.StartTicks)
	}
	if err := syscall.Kill(p.PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
}

// endpointPair is the agent endpoint's certificate and key as the files hold them, and the pin.
type endpointPair struct {
	cert, key []byte
	pin       string
}

// readEndpointPair checks the layout of <state>/agent-endpoint (directory 0700, key 0600), loads
// the pair as the agent API's server does (tls.LoadX509KeyPair) and returns it.
func readEndpointPair(t *testing.T, dir string) endpointPair {
	t.Helper()
	host := agentapi.Host{StateDir: dir, PortBase: 29220}
	if fi, err := os.Stat(filepath.Dir(host.AgentCertFile())); err != nil || !fi.IsDir() || fi.Mode().Perm() != 0o700 {
		t.Errorf("agent-endpoint: %v, %v; want a directory with mode 0700", fi, err)
	}
	if fi, err := os.Stat(host.AgentKeyFile()); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("tls.key: %v, %v; want mode 0600", fi, err)
	}
	kp, err := tls.LoadX509KeyPair(host.AgentCertFile(), host.AgentKeyFile())
	if err != nil {
		t.Fatalf("the agent endpoint's pair does not load: %v", err)
	}
	leaf, err := x509.ParseCertificate(kp.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return endpointPair{cert: []byte(fileOf(host.AgentCertFile())), key: []byte(fileOf(host.AgentKeyFile())), pin: agentapi.PinOf(leaf)}
}

// TestControllerSupervision: up creates the namespace and starts the supervisor and the
// controller with the exact command line of the contract; status reports them and checks the
// controller's own health port; a killed controller is restarted and counted; a killed supervisor
// makes status unhealthy; and down leaves no process of ours.
func TestControllerSupervision(t *testing.T) {
	const base = 29220
	h := newHost(t)
	bin, ctlDir := fakeController(t)
	c := h.clients // a method value: the clients exist once up has written the kubeconfig

	r := cli(t, h.upArgs(base, append(frpsArgs(t), "--json", "--controller-bin", bin,
		"--front-mode", "direct", "--front-addr", "front.example.test:8443")...)...)
	if r.code != exitOK {
		t.Fatalf("up exited %d", r.code)
	}
	if strings.Contains(r.err, "no controller binary found") {
		t.Errorf("a controller binary was given, yet up warns: %q", r.err)
	}
	var up struct {
		Ports struct {
			Controller int `json:"controller"`
			Agent      int `json:"agent"`
		} `json:"ports"`
		SupervisorPID    int    `json:"supervisor_pid"`
		ControllerHealth string `json:"controller_health"`
	}
	parseJSON(t, r.out, &up)
	if up.Ports.Controller != base+6 || up.Ports.Agent != base+3 || up.ControllerHealth != "127.0.0.1:"+itoa(base+6) {
		t.Errorf("up reports %+v", up)
	}

	// ---- what up recorded and started ----
	procs := recordedProcs(t, h.dir)
	if len(procs) != 2 || procs[0].Name != "k3s" || procs[1].Name != "supervisor" || procs[1].PID != up.SupervisorPID {
		t.Fatalf("state.json records %+v", procs)
	}
	k3s, sup := procs[0], procs[1]
	if !running(sup.PID) || startTicks(t, sup.PID) != sup.StartTicks {
		t.Fatalf("the recorded supervisor is not the running process: %+v", sup)
	}
	ns, err := c().cs.CoreV1().Namespaces().Get(t.Context(), "hpcburst-system", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("namespace hpcburst-system: %v", err)
	}
	t.Logf("namespace %s created %s", ns.Name, ns.CreationTimestamp)
	for _, d := range []string{"controller", "credentials", "agent-builds"} {
		if fi, err := os.Stat(filepath.Join(h.dir, d)); err != nil || !fi.IsDir() || fi.Mode().Perm() != 0o700 {
			t.Errorf("%s: %v, %v; want a directory with mode 0700", d, fi, err)
		}
	}
	// up made the agent endpoint's certificate and key before it started the controller.
	pair := readEndpointPair(t, h.dir)
	if _, err := os.Stat(filepath.Join(h.dir, "controller", "credentials")); !os.IsNotExist(err) {
		t.Errorf("the earlier controller/credentials was created (%v)", err)
	}
	if fi, err := os.Stat(filepath.Join(h.dir, "controller", "supervisor.json")); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("supervisor.json: %v, %v", fi, err)
	}
	rec, ok := readSupervisorRecord(t, h.dir)
	if !ok || rec.Supervisor.PID != sup.PID || rec.Entries[0].Proc == nil {
		t.Fatalf("supervisor.json: %+v", rec)
	}
	child := *rec.Entries[0].Proc
	if !running(child.PID) || startTicks(t, child.PID) != child.StartTicks || ppidOfPID(t, child.PID) != sup.PID {
		t.Fatalf("the controller (%+v) is not a running child of the supervisor (pid %d)", child, sup.PID)
	}
	wantArgv := []string{
		"--state-dir", h.dir,
		"--port-base", "29220",
		"--health-addr", "127.0.0.1:29226",
		"--front-mode", "direct", "--front-addr", "front.example.test:8443",
	}
	var gotArgv []string
	parseJSON(t, fileOf(filepath.Join(ctlDir, fmt.Sprintf("argv-%d.json", child.PID))), &gotArgv)
	if !slices.Equal(gotArgv, wantArgv) {
		t.Errorf("controller argv\n got %q\nwant %q", gotArgv, wantArgv)
	}
	if got := listenersOf(t, child.PID); !slices.Equal(got, []string{"127.0.0.1:" + itoa(base+6)}) {
		t.Errorf("the controller listens on %v, want only its health port", got)
	}
	if got := listenersOf(t, sup.PID); len(got) != 0 {
		t.Errorf("the supervisor listens on %v", got)
	}
	if got := listenersOf(t, k3s.PID); len(got) != 4 {
		t.Errorf("k3s listens on %v: the controllers' ports must not be k3s's", got)
	}

	// ---- status: healthy, with the controller checked through its own port ----
	waitFor(t, 20*time.Second, "status to report the controller healthy", func() bool {
		s, code := statusOf(t, h)
		return code == exitOK && s.Health == "healthy"
	})
	s, _ := statusOf(t, h)
	if s.Supervisor == nil || !s.Supervisor.Running || s.Supervisor.PID != sup.PID ||
		len(s.Controllers) != 2 || s.Controllers[1].Name != "frps" || !s.Controllers[1].Running || // frps runs beside the controller (HBEP-6)
		!s.Controllers[0].Running || s.Controllers[0].PID != child.PID ||
		s.Controllers[0].HealthzOK == nil || !*s.Controllers[0].HealthzOK || s.Controllers[0].Restarts != 0 {
		t.Fatalf("status: %+v", s)
	}
	if r := cli(t, h.args("status", "--no-gc-probe")...); !strings.Contains(r.out, "supervised controller: running=true pid "+itoa(child.PID)) {
		t.Errorf("the text status does not show the controller: %s", r.out)
	}

	// ---- the controller is judged by its /readyz, and a host with no front runs says so ----
	// This host recorded a front address but runs no front process: the front is absent, and the
	// host is healthy. (TestFrontSupervision is the control: a host that runs a front is not absent.)
	if !s.FrontAbsent || s.Controllers[0].ReadyzOK == nil || !*s.Controllers[0].ReadyzOK {
		t.Errorf("status: front absent %v, controller /readyz %v; want true and true", s.FrontAbsent, s.Controllers[0].ReadyzOK)
	}
	if r := cli(t, h.args("status", "--no-gc-probe")...); r.code != exitOK || !strings.Contains(r.out, "front: absent (no front runs here; the recorded address is direct front.example.test:8443)\n") ||
		!strings.Contains(r.out, "GET /readyz: true") {
		t.Errorf("status text (exit %d):\n%s", r.code, r.out)
	}
	// Alive but not ready: the fake keeps answering 200 on /healthz and answers 500 on /readyz, as
	// the lifecycle controller does while it does not lead its Lease or its caches have not synced.
	if err := os.WriteFile(filepath.Join(ctlDir, "not-ready"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 20*time.Second, "status to report the controller alive but not ready", func() bool {
		s, code := statusOf(t, h)
		return code == exitFail && s.Health == "unhealthy"
	})
	s, notReadyCode := statusOf(t, h)
	if notReadyCode != exitFail || s.Health != "unhealthy" || !s.Controllers[0].Running || s.Controllers[0].PID != child.PID ||
		s.Controllers[0].HealthzOK == nil || !*s.Controllers[0].HealthzOK || s.Controllers[0].ReadyzOK == nil || *s.Controllers[0].ReadyzOK ||
		len(s.Reasons) != 1 || !strings.Contains(s.Reasons[0], "controller: not ready: GET /readyz on 127.0.0.1:29226 answered 500, failing check: serving") {
		t.Errorf("status exited %d: %+v", notReadyCode, s)
	}
	if r := cli(t, h.args("status", "--no-gc-probe")...); r.code != exitFail || !strings.HasPrefix(r.out, "unhealthy\n  - controller: not ready: ") ||
		!strings.Contains(r.out, "GET /healthz on 127.0.0.1:29226: true, GET /readyz: false (failing: serving)") {
		t.Errorf("status text (exit %d):\n%s", r.code, r.out)
	}
	if err := os.Remove(filepath.Join(ctlDir, "not-ready")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 20*time.Second, "status to report the controller ready again", func() bool {
		s, code := statusOf(t, h)
		return code == exitOK && s.Health == "healthy"
	})
	s, _ = statusOf(t, h)

	// ---- a killed controller is restarted ----
	verifiedKill(t, stateProc{Name: "controller", PID: child.PID, StartTicks: child.StartTicks})
	if !waitDone(child.PID) {
		t.Fatalf("the killed controller (pid %d) still runs", child.PID)
	}
	waitFor(t, 30*time.Second, "the controller to be restarted and healthy", func() bool {
		s, code := statusOf(t, h)
		return code == exitOK && len(s.Controllers) == 2 && s.Controllers[0].Restarts == 1 && s.Controllers[0].PID != child.PID
	})
	s, _ = statusOf(t, h)
	newChild := s.Controllers[0].PID
	if s.Controllers[0].LastExit == nil || !strings.Contains(s.Controllers[0].LastExit.Detail, "killed") {
		t.Errorf("last exit %+v, want a kill", s.Controllers[0].LastExit)
	}
	if ppidOfPID(t, newChild) != sup.PID || running(child.PID) {
		t.Errorf("new controller pid %d has parent %d (supervisor %d); old one running: %v", newChild, ppidOfPID(t, newChild), sup.PID, running(child.PID))
	}

	// ---- down leaves nothing ----
	if r := cli(t, h.args("down", "--grace", "20s")...); r.code != exitOK {
		t.Fatalf("down exited %d", r.code)
	}
	for name, pid := range map[string]int{"supervisor": sup.PID, "controller": newChild, "k3s": k3s.PID} {
		if !waitDone(pid) {
			t.Errorf("%s (pid %d) still runs after down", name, pid)
		}
	}
	if rows := listeningInRange(t, launcherPortsLo, launcherPortsHi); len(rows) != 0 {
		t.Errorf("listeners remain after down: %v", rows)
	}
	if _, err := os.Stat(filepath.Join(h.dir, "state.json")); err == nil {
		t.Error("state.json remains after down")
	}
	if r := cli(t, h.args("status")...); r.code != exitStopped {
		t.Errorf("status after down: exit %d, want %d", r.code, exitStopped)
	}

	// ---- a killed supervisor: unhealthy, and down still cleans up ----
	if r := cli(t, h.upArgs(base, append(frpsArgs(t), "--json", "--controller-bin", bin)...)...); r.code != exitOK {
		t.Fatalf("second up exited %d", r.code)
	}
	// The pair survived the stop and the second start: the same files, so the same pin.
	if again := readEndpointPair(t, h.dir); again.pin != pair.pin || !bytes.Equal(again.cert, pair.cert) || !bytes.Equal(again.key, pair.key) {
		t.Errorf("the agent endpoint's pair changed across down and up: pin %s -> %s", pair.pin, again.pin)
	}
	procs = recordedProcs(t, h.dir)
	rec, ok = readSupervisorRecord(t, h.dir)
	if len(procs) != 2 || !ok || rec.Entries[0].Proc == nil {
		t.Fatalf("second start: %+v, %+v", procs, rec)
	}
	k3s, sup, child = procs[0], procs[1], *rec.Entries[0].Proc
	verifiedKill(t, sup)
	if !waitDone(sup.PID) {
		t.Fatalf("the killed supervisor (pid %d) still runs", sup.PID)
	}
	// The kernel kills the controller with its supervisor (Pdeathsig), so none is left behind.
	if !waitDone(child.PID) {
		t.Errorf("the controller (pid %d) outlived its supervisor", child.PID)
	}
	s, code := statusOf(t, h)
	if code != exitFail || s.Health != "unhealthy" || s.Supervisor == nil || s.Supervisor.Running ||
		!strings.Contains(strings.Join(s.Reasons, ";"), "supervisor") {
		t.Fatalf("status with the supervisor dead: exit %d, %+v", code, s)
	}
	if !running(k3s.PID) {
		t.Error("k3s stopped along with the supervisor")
	}
	if r := cli(t, h.args("down", "--grace", "20s")...); r.code != exitOK {
		t.Fatalf("down after the supervisor was killed: exit %d", r.code)
	}
	if !waitDone(k3s.PID) || running(sup.PID) || running(child.PID) {
		t.Errorf("after down: k3s running %v, supervisor %v, controller %v", running(k3s.PID), running(sup.PID), running(child.PID))
	}

	// ---- a key that others can read: up refuses before it starts anything ----
	keyFile := filepath.Join(h.dir, "agent-endpoint", "tls.key")
	if err := os.Chmod(keyFile, 0o644); err != nil {
		t.Fatal(err)
	}
	r = cli(t, h.upArgs(base, append(frpsArgs(t), "--json", "--controller-bin", bin)...)...)
	if r.code != exitFail || !strings.Contains(r.err, keyFile) || !strings.Contains(r.err, "0644") {
		t.Errorf("up with a world-readable key: exit %d, stderr %q; want exit %d naming %s", r.code, r.err, exitFail, keyFile)
	}
	if _, err := os.Stat(filepath.Join(h.dir, "state.json")); err == nil {
		t.Error("up started k3s though the agent endpoint's key was refused")
	}
	if rows := listeningInRange(t, launcherPortsLo, launcherPortsHi); len(rows) != 0 {
		t.Errorf("listeners remain after the refused up: %v", rows)
	}
	if got := fileOf(keyFile); got != string(pair.key) {
		t.Error("the refused key file was modified")
	}
}
