package agent_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// sshConfig is agent.json in ssh mode, with a dial-home directory holding the
// key and the pinned host key.
func sshConfig(t *testing.T, root string) agentapi.AgentConfig {
	t.Helper()
	dh := filepath.Join(root, "dial-home")
	if err := os.MkdirAll(dh, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{agentapi.DialHomeKeyFile, agentapi.DialHomeKnownHostsFile} {
		if err := os.WriteFile(filepath.Join(dh, f), []byte("test\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return agentapi.AgentConfig{
		ProtocolVersion: agentapi.ProtocolVersion, Tag: hTag, Mode: agentapi.DialHomeSSH,
		Endpoint: "127.0.0.1:22", SSH: &agentapi.SSHForward{User: "hb", AgentPort: 29299, DialHomePort: 29303},
		CertSPKISHA256: strings.Repeat("a", 64), FrpcDigest: strings.Repeat("d", 64), StagingDir: filepath.Join(root, "staging"), DialHomeDir: dh,
	}
}

// When the forward stops, every ssh process it started is stopped and reaped:
// TERM, then KILL after the grace for one that ignores TERM. The processes are
// found by the PID and start time the forward recorded, never by name, and
// none is alive once Run has returned (HBEP-6, teardown).
func TestSSHForwardStopsAndReapsItsChildren(t *testing.T) {
	root, err := os.MkdirTemp("", "hbf")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	bin := filepath.Join(root, "bin")
	os.MkdirAll(bin, 0o700)
	if err := enginedouble.Install(bin); err != nil {
		t.Fatal(err)
	}
	// An ssh that ignores TERM, with a child in its process group whose PID it
	// writes down, so the test can see the group's KILL reach it.
	stubborn := filepath.Join(bin, "stubborn-ssh")
	childPIDFile := filepath.Join(root, "child.pid")
	script := "#!/bin/sh\ntrap '' TERM\nsleep 300 &\necho $! > " + childPIDFile + "\nwhile :; do sleep 1; done\n"
	if err := os.WriteFile(stubborn, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	const grace = 400 * time.Millisecond
	// An ssh that exits at once on TERM, leaving a child that ignores it: the
	// leader's exit is not the group's (Astra B4).
	quitter := filepath.Join(bin, "quitter-ssh")
	qscript := "#!/bin/sh\n(trap '' TERM; exec sleep 300) &\necho $! > " + childPIDFile + "\ntrap 'exit 0' TERM\nwhile :; do sleep 1; done\n"
	if err := os.WriteFile(quitter, []byte(qscript), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		ssh     string
		minTook time.Duration
		child   bool
	}{
		{"honours TERM", filepath.Join(bin, "ssh"), 0, false},
		{"ignores TERM", stubborn, grace, true},
		{"exits on TERM, child stays", quitter, grace, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var started []agent.ProcessID
			fwd := &agent.SSHForward{
				SSH: tc.ssh, RemotePort: 29299, Config: sshConfig(t, root),
				Log: io.Discard, StopGrace: grace,
				OnStart: func(id agent.ProcessID) { mu.Lock(); started = append(started, id); mu.Unlock() },
			}
			os.Remove(childPIDFile)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{}) // closed once Run has returned
			go func() { _ = fwd.Run(ctx); close(done) }()
			// Whatever happens below, the forward is stopped and nothing it
			// started outlives the test.
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
				}
				mu.Lock()
				defer mu.Unlock()
				for _, id := range started {
					if id.Alive() {
						_ = syscall.Kill(id.PID, syscall.SIGKILL)
					}
				}
				killRecordedChild(childPIDFile)
			})
			deadline := time.Now().Add(10 * time.Second)
			for {
				mu.Lock()
				n := len(started)
				mu.Unlock()
				if n > 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("the forward started no ssh")
				}
				time.Sleep(10 * time.Millisecond)
			}
			time.Sleep(200 * time.Millisecond) // the shell arms its trap
			mu.Lock()
			ids := append([]agent.ProcessID(nil), started...)
			mu.Unlock()
			for _, id := range ids {
				if id.StartTime == 0 || !id.Alive() {
					t.Fatalf("recorded %+v is not a live process", id)
				}
			}
			if tc.child {
				startChildWatch(t, childPIDFile)
			}
			stopped := time.Now()
			cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("Run did not return after its context ended")
			}
			if took := time.Since(stopped); took < tc.minTook {
				t.Fatalf("stopped after %s, before the %s grace", took, tc.minTook)
			}
			for _, id := range ids {
				if id.Alive() {
					t.Fatalf("ssh %+v still runs after the forward stopped", id)
				}
			}
			if tc.child {
				// KILL has been sent to the group; give the kernel a moment to
				// end the child, which is not this test's to reap.
				child := recordedChild(t, childPIDFile)
				deadline := time.Now().Add(2 * time.Second)
				for child.Alive() && time.Now().Before(deadline) {
					time.Sleep(10 * time.Millisecond)
				}
				if child.Alive() {
					t.Fatalf("the stubborn ssh's child %+v still runs: the KILL did not reach its process group", child)
				}
			}
		})
	}
}

// recordedChild returns the PID the stubborn script wrote, with the start
// time read at once, while the child still runs.
func recordedChild(t *testing.T, path string) agent.ProcessID {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("the stubborn script wrote no child PID: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		t.Fatal(err)
	}
	return agent.ProcessID{PID: pid, StartTime: childStart[pid]}
}

// childStart holds the start time of the stubborn script's child, read while
// it ran (startChildWatch).
var childStart = map[int]uint64{}

func killRecordedChild(path string) {
	b, err := os.ReadFile(path)
	if err != nil {
		return
	}
	if pid, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil {
		if id := (agent.ProcessID{PID: pid, StartTime: childStart[pid]}); id.StartTime != 0 && id.Alive() {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
}

// A descriptor the agent's process holds without close-on-exec, such as a
// lock its parent passed down, does not reach ssh: the forward marks every
// inherited descriptor close-on-exec before it starts ssh (HBEP-6,
// teardown). The test opens a marker file, duplicates its descriptor (a
// duplicate carries no close-on-exec flag), starts the forward, and reads
// the ssh process's descriptors by their targets.
func TestSSHGetsNoInheritedDescriptor(t *testing.T) {
	root, err := os.MkdirTemp("", "hbg")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	marker := filepath.Join(root, "marker-lock")
	f, err := os.Create(marker)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	leak, err := syscall.Dup(int(f.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(leak)
	stub := filepath.Join(root, "ssh")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\nwhile :; do sleep 1; done\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var started []agent.ProcessID
	fwd := &agent.SSHForward{
		SSH: stub, RemotePort: 29299, Config: sshConfig(t, root),
		Log: io.Discard, StopGrace: 200 * time.Millisecond,
		OnStart: func(id agent.ProcessID) { mu.Lock(); started = append(started, id); mu.Unlock() },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{}) // closed once Run has returned
	go func() { _ = fwd.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	var id agent.ProcessID
	for {
		mu.Lock()
		if len(started) > 0 {
			id = started[0]
		}
		mu.Unlock()
		if id.PID != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the forward started no ssh")
		}
		time.Sleep(10 * time.Millisecond)
	}
	fds, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", id.PID))
	if err != nil {
		t.Fatal(err)
	}
	if len(fds) < 3 {
		t.Fatalf("read %d descriptors of ssh: the check would see nothing", len(fds))
	}
	for _, fd := range fds {
		target, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", id.PID, fd.Name()))
		if err == nil && target == marker {
			t.Fatalf("ssh holds descriptor %s on %s, which the agent's process held without close-on-exec", fd.Name(), marker)
		}
	}
}

// startChildWatch records the start time of the child the stubborn script
// wrote down, while it runs, so the later check is by PID and start time.
func startChildWatch(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		b, err := os.ReadFile(path)
		if pid, perr := strconv.Atoi(strings.TrimSpace(string(b))); err == nil && perr == nil {
			st, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
			if err != nil {
				t.Fatalf("the stubborn script's child %d is not running: %v", pid, err)
			}
			s := string(st)
			i := strings.LastIndexByte(s, ')')
			f := strings.Fields(s[i+1:])
			v, err := strconv.ParseUint(f[19], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			childStart[pid] = v
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the stubborn script wrote no child PID")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
