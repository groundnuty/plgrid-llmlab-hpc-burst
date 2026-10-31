package launcher

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func statusFixture(t *testing.T, rec ProcRec, ports Ports) layout {
	t.Helper()
	l, err := newLayout(shortDir(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.saveState(&State{Version: "v1.36.5+k3s1", Ports: ports, Procs: []ProcRec{rec}, StartedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	return l
}

func TestStatusStopped(t *testing.T) {
	for name, dir := range map[string]string{"missing directory": filepath.Join(t.TempDir(), "none"), "empty directory": shortDir(t)} {
		rep, err := Status(context.Background(), StatusOptions{StateDir: dir})
		if err != nil || rep.Health != Stopped || len(rep.Reasons) == 0 {
			t.Errorf("%s: %+v, %v", name, rep, err)
		}
	}
}

func TestStatusGoneAndForeignProcesses(t *testing.T) {
	cmd := startBystander(t, "sleep", "300")
	rec, _ := newRec("k3s", cmd.Process.Pid)

	foreign := rec
	foreign.StartTicks++
	rep, err := Status(context.Background(), StatusOptions{StateDir: statusFixture(t, foreign, Ports{API: 1, HTTPS: 2, KCM: 3, LB: 4}).dir})
	if err != nil || rep.Health != Unhealthy || rep.Processes[0].Verdict != "foreign" || !strings.Contains(strings.Join(rep.Reasons, ";"), "not running") {
		t.Fatalf("foreign: %+v, %v", rep, err)
	}

	_ = cmd.Process.Kill()
	if !waitNotRunning(cmd.Process.Pid, 5*time.Second) {
		t.Fatal("could not kill the fixture")
	}
	rep, err = Status(context.Background(), StatusOptions{StateDir: statusFixture(t, rec, Ports{API: 1, HTTPS: 2, KCM: 3, LB: 4}).dir})
	if err != nil || rep.Health != Unhealthy || rep.Processes[0].Verdict != "gone" {
		t.Fatalf("gone: %+v, %v", rep, err)
	}
}

// answeringServer listens on an ephemeral loopback port and answers 200 to everything, like a
// healthy API server would answer /readyz. It returns the port.
func answeringServer(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })}
	go srv.Serve(l)
	t.Cleanup(func() { srv.Close() })
	return l.Addr().(*net.TCPAddr).Port
}

// TestStatusIgnoresWhatAnswersOnThePort: the recorded process is alive and is the recorded one,
// but the three expected ports are held by this test, and every one of them answers /readyz
// with 200. A status that probes ports would call that healthy. It must not.
func TestStatusIgnoresWhatAnswersOnThePort(t *testing.T) {
	cmd := startBystander(t, "sleep", "300")
	rec, _ := newRec("k3s", cmd.Process.Pid)
	ports := Ports{API: answeringServer(t), HTTPS: answeringServer(t), KCM: answeringServer(t), LB: 1}
	// Evidence that the trap is real: a port probe says yes on every expected port.
	for _, p := range ports.Expected() {
		resp, err := http.Get("http://127.0.0.1:" + strconv.Itoa(p) + "/readyz")
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("the decoy on port %d does not answer: %v", p, err)
		}
		resp.Body.Close()
	}
	for _, noAPI := range []bool{true, false} {
		rep, err := Status(context.Background(), StatusOptions{StateDir: statusFixture(t, rec, ports).dir, NoAPI: noAPI})
		if err != nil {
			t.Fatal(err)
		}
		if rep.Health != Unhealthy {
			t.Fatalf("noAPI=%v: a recorded live process with foreign listeners on its ports was reported %s", noAPI, rep.Health)
		}
		joined := strings.Join(rep.Reasons, ";")
		if !strings.Contains(joined, "no listening socket") {
			t.Errorf("noAPI=%v: reasons %q do not say that the launched process holds no listener", noAPI, joined)
		}
		if rep.Readyz != nil {
			t.Errorf("noAPI=%v: /readyz was consulted although the listeners are not the process's", noAPI)
		}
	}
}

func TestRestConfigRefusesAForeignServer(t *testing.T) {
	write := func(server string) string {
		p := filepath.Join(t.TempDir(), "kubeconfig")
		y := "apiVersion: v1\nkind: Config\nclusters:\n- name: c\n  cluster:\n    server: " + server + "\n    insecure-skip-tls-verify: true\ncontexts:\n- name: c\n  context: {cluster: c, user: u}\ncurrent-context: c\nusers:\n- name: u\n  user: {token: t}\n"
		if err := os.WriteFile(p, []byte(y), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	ports := Ports{API: 29220, HTTPS: 29221, KCM: 29222, LB: 29225}
	if _, err := restConfig(write("https://127.0.0.1:29221"), ports, time.Second); err != nil {
		t.Errorf("own address refused: %v", err)
	}
	for _, server := range []string{"https://127.0.0.1:6443", "https://127.0.0.1:29220", "https://203.0.113.5:29221", "https://k3s.example.test:29221"} {
		if _, err := restConfig(write(server), ports, time.Second); err == nil {
			t.Errorf("a kubeconfig for %s was accepted as this launcher's", server)
		}
	}
}
