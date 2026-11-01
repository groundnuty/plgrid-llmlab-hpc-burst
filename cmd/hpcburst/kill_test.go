package main

import (
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"
)

// decoy listens on a loopback port and answers 200 to every request, as a healthy API server
// would answer /readyz.
func decoy(t *testing.T, port int) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:"+itoa(port))
	if err != nil {
		t.Fatalf("cannot start the decoy on %d: %v", port, err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })}
	go srv.Serve(l)
	t.Cleanup(func() { srv.Close() })
}

// TestStatusAfterTheProcessIsKilled kills the recorded k3s, then puts a server that answers 200
// on its ports. A status built on port probes would call that healthy; this one must call it
// unhealthy because the recorded process is gone. down afterwards must not touch the decoys.
func TestStatusAfterTheProcessIsKilled(t *testing.T) {
	const base = 29220
	h := newHost(t)
	if r := cli(t, h.upArgs(base)...); r.code != exitOK {
		t.Fatalf("up exited %d", r.code)
	}
	st := readState(t, h.dir)
	pid := st.Processes[0].PID
	if r := cli(t, h.args("status")...); r.code != exitOK {
		t.Fatalf("status of the fresh host: exit %d", r.code)
	}

	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 15*time.Second, "k3s to die", func() bool { return !running(pid) })
	waitFor(t, 15*time.Second, "the kernel to release k3s's sockets", func() bool { return len(listeningInRange(t, launcherPortsLo, launcherPortsHi)) == 0 })

	for _, p := range []int{base, base + 1, base + 2} {
		decoy(t, p)
	}
	// The trap is real: every expected port now answers a probe with 200.
	for _, p := range []int{base, base + 1, base + 2} {
		resp, err := http.Get("http://127.0.0.1:" + itoa(p) + "/readyz")
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("the decoy on %d does not answer: %v", p, err)
		}
		resp.Body.Close()
	}

	for _, noAPI := range []bool{false, true} {
		args := h.args("status", "--json")
		if noAPI {
			args = append(args, "--no-api")
		}
		r := cli(t, args...)
		var rep struct {
			Health    string   `json:"health"`
			Reasons   []string `json:"reasons"`
			Processes []struct {
				PID     int    `json:"pid"`
				Verdict string `json:"verdict"`
			} `json:"processes"`
			Readyz *bool `json:"readyz_ok"`
		}
		parseJSON(t, r.out, &rep)
		if r.code != exitFail || rep.Health != "unhealthy" {
			t.Fatalf("noAPI=%v: exit %d, health %q: a dead host with a server on its ports was not reported unhealthy", noAPI, r.code, rep.Health)
		}
		if len(rep.Processes) != 1 || rep.Processes[0].PID != pid || rep.Processes[0].Verdict != "gone" {
			t.Errorf("noAPI=%v: processes %+v", noAPI, rep.Processes)
		}
		if !strings.Contains(strings.Join(rep.Reasons, ";"), "is not running") {
			t.Errorf("noAPI=%v: reasons %v do not say that the process is not running", noAPI, rep.Reasons)
		}
		if rep.Readyz != nil {
			t.Errorf("noAPI=%v: the decoy's /readyz was consulted", noAPI)
		}
	}
	if r := cli(t, h.args("status")...); r.code != exitFail || !strings.HasPrefix(r.out, "unhealthy") {
		t.Errorf("text status: exit %d: %q", r.code, r.out)
	}

	// down: the record is cleared, and the decoys (this test process's sockets) are left alone.
	if r := cli(t, h.args("down")...); r.code != exitOK {
		t.Fatalf("down exited %d", r.code)
	}
	rows := listeningInRange(t, launcherPortsLo, launcherPortsHi)
	if len(rows) != 3 {
		t.Errorf("down disturbed the decoys: %v", rows)
	}
	for _, p := range []int{base, base + 1, base + 2} {
		c, err := net.DialTimeout("tcp", "127.0.0.1:"+itoa(p), 2*time.Second)
		if err != nil {
			t.Errorf("decoy %d no longer accepts: %v", p, err)
			continue
		}
		c.Close()
	}
}

// TestFailedStartLeavesNothingBehind: a start that does not become ready in time must stop what it
// started. The timeout is shorter than a cold start, so the wait fails with k3s still unpacking.
func TestFailedStartLeavesNothingBehind(t *testing.T) {
	h := newHost(t)
	r := cli(t, h.upArgs(29220, "--ready-timeout", "2s")...)
	if r.code != exitFail || !strings.Contains(r.err, "not ready after 2s") {
		t.Fatalf("exit %d, stderr %q", r.code, r.err)
	}
	if !strings.Contains(r.err, "last lines of") {
		t.Errorf("the error does not show the end of the k3s log: %q", r.err)
	}
	waitFor(t, 20*time.Second, "the failed start's k3s to be gone", func() bool { return len(k3sProcessesUsing(h.dir)) == 0 })
	if rows := listeningInRange(t, launcherPortsLo, launcherPortsHi); len(rows) != 0 {
		t.Errorf("listeners remain: %v", rows)
	}
	if r := cli(t, h.args("status")...); r.code != exitStopped {
		t.Errorf("status after a failed start: exit %d: %s", r.code, r.out)
	}
}
