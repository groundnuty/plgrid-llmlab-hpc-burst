package agent_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

// The router's launch with the real Envoy 1.40.2 (envoy.go, envoysockets.go): the binary is checked
// against its pin before it runs, and after it starts its listener and admin are mode-0600 Unix
// sockets in the run directory and the only things it listens on.

// shortDir is a directory whose paths fit a Unix socket's 107 bytes: a test's own TempDir can be too
// long under a deep TMPDIR.
func shortDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "e")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

func startTestEnvoy(t *testing.T) (*agent.EnvoyProc, string) {
	t.Helper()
	dir := shortDir(t)
	return startTestEnvoyIn(t, dir, startReadiness(t, dir, false)), dir
}

// startReadiness starts the agent's readiness endpoint for worker-0 in dir, verified or not, and stops
// it when the test ends.
func startReadiness(t *testing.T, dir string, verified bool) *agent.Readiness {
	t.Helper()
	rd, err := agent.NewReadiness()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rd.Close() })
	rd.Set(map[string]agent.ReadyWorker{"worker-0": {Socket: filepath.Join(dir, "worker-0.sock"), Verified: verified}})
	return rd
}

// startTestEnvoyIn starts Envoy through the agent in dir, for worker-0, health-checked through rd.
func startTestEnvoyIn(t *testing.T, dir string, rd *agent.Readiness) *agent.EnvoyProc {
	t.Helper()
	bin := testenv.EnvoyBinary(t)
	p, err := agent.StartEnvoy(agent.EnvoyStart{
		Binary: bin, Ready: 30 * time.Second,
		Config: agent.EnvoyConfig{RunDir: dir, Workers: []agent.EnvoyWorker{{Name: "worker-0", Socket: filepath.Join(dir, "worker-0.sock")}}, ReadinessPort: rd.Port()},
	}, time.Now())
	if err != nil {
		t.Fatalf("StartEnvoy: %v", err)
	}
	t.Cleanup(func() {
		p.Group.Stop(5 * time.Second)
		<-p.Group.Done()
		p.Close()
	})
	return p
}

func TestStartEnvoyListensOnlyOnItsPipes(t *testing.T) {
	p, dir := startTestEnvoy(t)
	listener, admin := filepath.Join(dir, "router.sock"), filepath.Join(dir, "envoy-admin.sock")
	for _, s := range []string{listener, admin} {
		fi, err := os.Lstat(s)
		if err != nil || fi.Mode()&os.ModeSocket == 0 || fi.Mode().Perm() != 0o600 {
			t.Errorf("%s: %v, %v; want a socket of mode 0600", s, fi, err)
		}
	}
	// StartEnvoy checked it once; the check holds on the running process too.
	if err := agent.CheckEnvoySockets(p.Group.PID(), listener, admin); err != nil {
		t.Errorf("the running Envoy: %v", err)
	}
	cfg, err := os.Stat(filepath.Join(dir, "envoy.json"))
	if err != nil || cfg.Mode().Perm() != 0o600 {
		t.Errorf("envoy.json: %v, %v; want mode 0600", cfg, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "envoy.log")); err != nil {
		t.Errorf("Envoy's log is not in the run directory: %v", err)
	}
	// It routes: with no worker on worker-0.sock, a request through the listener gets Envoy's own 503.
	c := unixClient(listener)
	resp, err := c.Get("http://instance/v1/models")
	if err != nil {
		t.Fatalf("a request through the listener: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Errorf("with no worker: %d %q, want Envoy's 503", resp.StatusCode, body)
	}
}

// helperListenEnv names, for TestHelperProcessListens, a directory and what to listen on in it.
const helperListenEnv = "HPCBURST_TEST_ENVOY_HELPER_LISTEN"

// TestHelperProcessListens is run only as the child of listenerChild: it listens as told, writes a
// ready file, and holds its sockets until its standard input closes.
func TestHelperProcessListens(t *testing.T) {
	spec := os.Getenv(helperListenEnv)
	if spec == "" {
		return
	}
	dir, what, _ := strings.Cut(spec, "|")
	var keep []io.Closer
	for _, w := range strings.Split(what, ",") {
		switch {
		case w == "tcp":
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			keep = append(keep, l)
		case w == "udp":
			c, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			keep = append(keep, c)
		default: // a Unix socket name[:mode]
			name, mode, _ := strings.Cut(w, ":")
			p := filepath.Join(dir, name)
			l, err := net.Listen("unix", p)
			if err != nil {
				t.Fatal(err)
			}
			perm := os.FileMode(0o600)
			if mode == "0644" {
				perm = 0o644
			}
			if err := os.Chmod(p, perm); err != nil {
				t.Fatal(err)
			}
			keep = append(keep, l)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "ready"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	for _, c := range keep {
		c.Close()
	}
}

// listenerChild starts a child that listens as what says, waits for its ready file, and returns its
// PID; the child ends when the test does.
func listenerChild(t *testing.T, dir, what string) int {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcessListens$", "-test.count=1")
	cmd.Env = append(os.Environ(), helperListenEnv+"="+dir+"|"+what)
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { in.Close(); _ = cmd.Wait() })
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(dir, "ready")); err == nil {
			return cmd.Process.Pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("the listening child did not get ready")
		}
		time.Sleep(20 * time.Millisecond) // polls the child's ready file, an observable state
	}
}

// The checker can fail: pointed at a child that listens on what the router must not, it refuses
// each, and it passes the child that listens on exactly its two sockets.
func TestCheckEnvoySocketsSeesWhatAProcessListensOn(t *testing.T) {
	for _, c := range []struct {
		name, what string
		want       string // "" for a pass
	}{
		{"exactly the two sockets", "a.sock,b.sock", ""},
		{"a TCP listener too", "a.sock,b.sock,tcp", "listens on TCP 0100007F:"},
		{"a UDP socket too", "a.sock,b.sock,udp", "holds a UDP socket"},
		{"a third Unix socket", "a.sock,b.sock,c.sock", "that it should not"},
		{"one socket absent", "a.sock", "b.sock: no such file"},
		// b.sock exists, held by this process, so only the check of whose listener it is refuses it.
		{"one socket another process's", "a.sock|parent:b.sock", "and not on"},
		{"a socket of mode 0644", "a.sock:0644,b.sock", "has mode 0644"},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := shortDir(t)
			what, parent, _ := strings.Cut(c.what, "|parent:")
			if parent != "" {
				l, err := net.Listen("unix", filepath.Join(dir, parent))
				if err != nil {
					t.Fatal(err)
				}
				defer l.Close()
				if err := os.Chmod(filepath.Join(dir, parent), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			pid := listenerChild(t, dir, what)
			err := agent.CheckEnvoySockets(pid, filepath.Join(dir, "a.sock"), filepath.Join(dir, "b.sock"))
			switch {
			case c.want == "" && err != nil:
				t.Fatalf("refused: %v", err)
			case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
				t.Fatalf("%v; want a refusal containing %q", err, c.want)
			case c.want != "" && !errors.Is(err, agent.ErrEnvoyRefused):
				t.Errorf("the refusal %v is not ErrEnvoyRefused", err)
			}
		})
	}
}

// A binary that is not the pinned one is refused before it runs: no config is written and nothing
// starts.
func TestStartEnvoyRefusesABinaryThatIsNotThePin(t *testing.T) {
	dir := shortDir(t)
	fake := filepath.Join(dir, "envoy")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexec sleep 600\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := agent.StartEnvoy(agent.EnvoyStart{
		Binary: fake, Ready: 5 * time.Second,
		Config: agent.EnvoyConfig{RunDir: dir, Workers: []agent.EnvoyWorker{{Name: "worker-0", Socket: filepath.Join(dir, "worker-0.sock")}}, ReadinessPort: 28160},
	}, time.Now())
	if err == nil || !errors.Is(err, agent.ErrEnvoyRefused) || !strings.Contains(err.Error(), "not the pin of Envoy 1.40.2") {
		t.Fatalf("StartEnvoy with another binary: %v, want a refusal naming the pin", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "envoy.json")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a refused start wrote its config: %v", err)
	}
}

// unixClient is an HTTP client whose every connection goes to the Unix socket at path.
func unixClient(path string) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", path)
		},
	}}
}
