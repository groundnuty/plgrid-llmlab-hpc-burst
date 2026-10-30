package testenv

import (
	"bufio"
	"fmt"
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

// TestMain doubles as the parent process of the Pdeathsig tests: the test
// binary runs itself with TESTENV_HELPER set, and that run starts a child and
// waits to be killed.
func TestMain(m *testing.M) {
	switch os.Getenv("TESTENV_HELPER") {
	case "":
		os.Exit(m.Run())
	case "pdeathsig":
		// The child is started the way the control plane starts its processes.
		p, err := startProc("sleeper", filepath.Join(os.TempDir(), "testenv-helper.log"), "sleep", "300")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Println(p.pid())
		select {}
	case "setpgid-only":
		// The control: the same child without Pdeathsig, as the code was before.
		cmd := exec.Command("sleep", "300")
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := cmd.Start(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Println(cmd.Process.Pid)
		select {}
	}
}

func TestPortBase(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int
	}{
		{"", 29200}, {"29200", 29200}, {"29208", 29208}, {"29216", 29216}, // 29216+3 = 29219, the last port
	} {
		got, err := portBase(tc.value)
		if err != nil || got != tc.want {
			t.Errorf("portBase(%q) = %d, %v; want %d", tc.value, got, err, tc.want)
		}
	}
	for _, v := range []string{
		"29199",                   // one below the range
		"29217", "29218", "29219", // the block would run past 29219
		"29220",                                     // #41's block
		"29240", "0", "-1", "6443", "6444", "10250", // the root k3s's ports and others
		"abc", "29200.5", " 29200", "29200 ", "0x7210",
	} {
		if got, err := portBase(v); err == nil {
			t.Errorf("portBase(%q) = %d, want a refusal", v, got)
		}
	}
}

func TestStartRefusesAPortBlockOutsideItsRange(t *testing.T) {
	t.Setenv(envPortBase, "29217")
	// The refusal comes before binaries are looked up or anything starts.
	t.Setenv(envUnpinned, "")
	cp, err := Start(t.Context(), t.TempDir(), Options{})
	if err == nil {
		_ = cp.Stop()
		t.Fatal("Start accepted a port block that runs past 29219")
	}
	if !strings.Contains(err.Error(), envPortBase) || !strings.Contains(err.Error(), "29200-29219") {
		t.Errorf("the refusal does not name the variable and the range: %v", err)
	}
}

func TestCheckServer(t *testing.T) {
	for _, server := range []string{
		"https://127.0.0.1:29200", "https://127.0.0.1:29220", "https://127.0.0.1:29239", "http://127.0.0.1:29225",
		"https://[::1]:29230", "https://localhost:29221", "127.0.0.1:29222", "https://127.0.0.2:29223/",
	} {
		if err := CheckServer(server); err != nil {
			t.Errorf("CheckServer(%q): %v", server, err)
		}
	}
	for _, tc := range []struct{ server, why string }{
		{"https://127.0.0.1:6443", "the root k3s"},
		{"https://127.0.0.1:6444", "the root k3s's hidden apiserver port"},
		{"https://127.0.0.1:10250", "the root k3s's kubelet"},
		{"https://127.0.0.1:29199", "one below the range"},
		{"https://127.0.0.1:29240", "one above the range"},
		{"https://127.0.0.1", "no port, so 443"},
		{"https://127.0.0.1:0", "port 0"},
		{"https://127.0.0.1:abc", "not a port"},
		{"https://192.0.2.10:29220", "not loopback (a documentation address)"},
		{"https://k3s.invalid:29220", "a host name that is not localhost"},
		{"https://[2001:db8::1]:29220", "a non-loopback IPv6 address"},
		{"https://0.0.0.0:29220", "the unspecified address"},
		{"ftp://127.0.0.1:29220", "not http or https"},
		{"", "no server"},
		{"://", "not an address"},
	} {
		if err := CheckServer(tc.server); err == nil {
			t.Errorf("CheckServer(%q) accepted %s", tc.server, tc.why)
		}
	}
}

func kubeconfigFor(t *testing.T, server string, extra string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kubeconfig")
	body := fmt.Sprintf(`apiVersion: v1
kind: Config
current-context: c
clusters:
- name: c
  cluster:
    server: %s
    insecure-skip-tls-verify: true
%s
users:
- name: u
  user:
    token: example-token
contexts:
- name: c
  context: {cluster: c, user: u}
`, server, extra)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestExternalConfig(t *testing.T) {
	cfg, err := ExternalConfig(kubeconfigFor(t, "https://127.0.0.1:29225", ""))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Host != "https://127.0.0.1:29225" || cfg.BearerToken != "example-token" {
		t.Errorf("config = host %q, token %q", cfg.Host, cfg.BearerToken)
	}
	for _, tc := range []struct{ name, server, extra string }{
		{"the root k3s's API server", "https://127.0.0.1:6443", ""},
		{"the root k3s's hidden apiserver port", "https://127.0.0.1:6444", ""},
		{"a remote server on an allowed port", "https://192.0.2.10:29225", ""},
		{"an allowed address behind a proxy", "https://127.0.0.1:29225", "    proxy-url: http://192.0.2.10:3128"},
	} {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			if cfg, err := ExternalConfig(kubeconfigFor(t, tc.server, tc.extra)); err == nil {
				t.Fatalf("accepted: host %q", cfg.Host)
			}
		})
	}
	if _, err := ExternalConfig(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("a missing kubeconfig was accepted")
	}
}

// A refusal must come from reading the file, not from trying the server. The
// listener stands in for a server that must not be contacted: its port is
// outside the allowed range, the kubeconfig points at it, and a connection
// attempt would sit in its accept queue, where Accept would find it.
func TestExternalConfigRefusalDoesNotContactTheServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port
	if port >= PortRangeFirst && port <= ExternalPortLast {
		t.Skipf("the kernel gave the ephemeral port %d, which is inside the allowed range", port)
	}
	if _, err := ExternalConfig(kubeconfigFor(t, "https://127.0.0.1:"+strconv.Itoa(port), "")); err == nil {
		t.Fatal("a server outside the allowed ports was accepted")
	}
	// Control: this listener does record a connection, so silence means none was made.
	_ = ln.(*net.TCPListener).SetDeadline(time.Now().Add(300 * time.Millisecond))
	if conn, err := ln.Accept(); err == nil {
		_ = conn.Close()
		t.Fatal("the refused kubeconfig's server was contacted")
	}
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = ln.(*net.TCPListener).SetDeadline(time.Now().Add(2 * time.Second))
	if c, err := ln.Accept(); err != nil {
		t.Fatalf("the listener does not record connections, so the silence above proves nothing: %v", err)
	} else {
		_ = c.Close()
	}
}

// Lines as the kernel writes them. The ports are converted by hand: 6443 is
// 0x192B, 6444 is 0x192C, 10250 is 0x280A, 8080 is 0x1F90, 31337 is 0x7A69.
const tcpTable = `  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 00000000:192B 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 11111 1 0000000000000000 100 0 0 10 0
   1: 0100007F:192C 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 11112 1 0000000000000000 100 0 0 10 0
   2: 0100007F:7A69 0100007F:192B 01 00000000:00000000 00:00000000 00000000  1000        0 11113 1 0000000000000000 20 4 30 10 -1
   3: 0100007F:1F90 0100007F:9C40 06 00000000:00000000 03:00000A2B 00000000     0        0 0 3 0000000000000000
`

const tcp6Table = `  sl  local_address                         remote_address                        st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 00000000000000000000000000000000:280A 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 22221 1 0000000000000000 100 0 0 10 0
   1: 00000000000000000000000001000000:7A69 00000000000000000000000001000000:192B 01 00000000:00000000 00:00000000 00000000  1000        0 22222 1 0000000000000000 20 4 30 10 -1
`

func TestParseListening(t *testing.T) {
	ports := map[int]bool{}
	for _, table := range []string{tcpTable, tcp6Table} {
		if err := parseListening(strings.NewReader(table), ports); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []int{6443, 6444, 10250} { // two on IPv4, one on IPv6 only
		if !ports[p] {
			t.Errorf("port %d listens in the fixture and was not found", p)
		}
	}
	for _, p := range []int{31337, 8080} { // an established connection and a TIME_WAIT socket
		if ports[p] {
			t.Errorf("port %d is not in the LISTEN state and was reported", p)
		}
	}
	if len(ports) != 3 {
		t.Errorf("ports = %v, want exactly 6443, 6444 and 10250", ports)
	}
	if err := parseListening(strings.NewReader("not a table\n"), map[int]bool{}); err == nil {
		t.Error("a table with an unexpected header was accepted")
	}
}

// The parser works on the live kernel format: a listener this test opens shows up.
func TestListeningPortsSeesALiveListener(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ports, err := listeningPorts()
	if err != nil {
		t.Fatal(err)
	}
	if !ports[port] {
		t.Errorf("port %d listens and listeningPorts does not report it", port)
	}
	_ = ln.Close()
	if ports, err = listeningPorts(); err != nil {
		t.Fatal(err)
	} else if ports[port] {
		t.Errorf("port %d was closed and is still reported", port)
	}
	if ln6, err := net.Listen("tcp", "[::1]:0"); err == nil {
		defer ln6.Close()
		port6 := ln6.Addr().(*net.TCPAddr).Port
		if ports, err := listeningPorts(); err != nil || !ports[port6] {
			t.Errorf("IPv6 port %d listens and listeningPorts reports %v, %v", port6, ports[port6], err)
		}
	}
}

func TestSystemPortsIncludeTheHiddenAPIServerPort(t *testing.T) {
	have := map[int]bool{}
	for _, p := range systemPorts {
		have[p] = true
	}
	for _, p := range []int{6443, 6444, 10250} {
		if !have[p] {
			t.Errorf("system port %d is not guarded", p)
		}
	}
}

// childState returns the state letter of a process from /proc, or "" when it is gone.
func childState(pid int) string {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return ""
	}
	s := string(b)
	return strings.TrimSpace(s[strings.LastIndexByte(s, ')')+1:])[:1]
}

func runHelper(t *testing.T, mode string) (parent *exec.Cmd, child int) {
	t.Helper()
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), "TESTENV_HELPER="+mode)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("the helper printed no child PID: %v", err)
	}
	child, err = strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("the helper printed %q: %v", line, err)
	}
	t.Cleanup(func() { _ = syscall.Kill(child, syscall.SIGKILL) })
	return cmd, child
}

// A child started by startProc dies with its parent, however the parent dies.
// The parent here is a second run of this test binary, killed with SIGKILL, as a
// go test timeout or an out-of-memory kill would kill it.
func TestChildrenDieWithTheirParent(t *testing.T) {
	parent, child := runHelper(t, "pdeathsig")
	if st := childState(child); st == "" || st == "Z" {
		t.Fatalf("the child is not running before the parent dies (state %q)", st)
	}
	_ = parent.Process.Kill()
	_, _ = parent.Process.Wait()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if st := childState(child); st == "" || st == "Z" { // a zombie is dead, waiting to be reaped
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("the child (pid %d, state %q) is still running 5 s after its parent was killed", child, childState(child))
}

// The control for the test above: a child started with Setpgid alone, as before,
// outlives its parent. If this fails, the test above cannot tell the two apart.
func TestWithoutPdeathsigAChildOutlivesItsParent(t *testing.T) {
	parent, child := runHelper(t, "setpgid-only")
	_ = parent.Process.Kill()
	_, _ = parent.Process.Wait()
	time.Sleep(time.Second)
	if st := childState(child); st == "" || st == "Z" {
		t.Fatalf("the child died with its parent (state %q) without Pdeathsig: the test above proves nothing", st)
	}
}

// The checksum script runs even when the environment names the binaries, so a
// binary that is not the pinned one is refused (review nit: explicit paths
// skipped the check). All three files exist with wrong content: a missing one
// would be downloaded, not refused.
func TestFindBinariesChecksTheShaOfBinariesNamedInTheEnvironment(t *testing.T) {
	dir := t.TempDir()
	for _, f := range []string{"etcd", "kube-apiserver", "kube-controller-manager"} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	kcm := filepath.Join(dir, "kube-controller-manager")
	t.Setenv("HPCBURST_TOOLS_CACHE", filepath.Join(dir, "cache")) // never the real cache
	t.Setenv(envAssets, dir)
	t.Setenv(envKCM, kcm)
	t.Setenv(envUnpinned, "")

	if b, err := FindBinaries(); err == nil {
		t.Fatalf("binaries that are not the pinned ones were accepted: %+v", b)
	} else if !strings.Contains(err.Error(), "sha256") {
		t.Errorf("the refusal does not say why: %v", err)
	}

	// The explicit unpinned override uses the same files, and says so.
	t.Setenv(envUnpinned, "1")
	b, err := FindBinaries()
	if err != nil {
		t.Fatalf("the unpinned override refused executable binaries: %v", err)
	}
	if b.Etcd != filepath.Join(dir, "etcd") || b.KubeAPIServer != filepath.Join(dir, "kube-apiserver") || b.KubeControllerManager != kcm {
		t.Errorf("binaries = %+v", b)
	}

	// The override needs both variables; it does not fall back to the cache.
	t.Setenv(envKCM, "")
	if _, err := FindBinaries(); err == nil {
		t.Error("the unpinned override accepted a missing " + envKCM)
	}
	t.Setenv(envKCM, kcm)
	t.Setenv(envAssets, "")
	if _, err := FindBinaries(); err == nil {
		t.Error("the unpinned override accepted a missing " + envAssets)
	}

	// An override that names a file that is not executable is refused too.
	t.Setenv(envAssets, dir)
	if err := os.Chmod(filepath.Join(dir, "etcd"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := FindBinaries(); err == nil {
		t.Error("the unpinned override accepted a file that is not executable")
	}
}
