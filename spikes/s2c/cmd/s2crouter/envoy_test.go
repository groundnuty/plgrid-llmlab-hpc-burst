package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
)

// The Envoy arm with the real Envoy 1.40.2 (HPCBURST_ENVOY_DIR=<dir holding envoy-1.40.2/envoy>, as
// internal/testenv reads it; HPCBURST_REQUIRE_ENVOY=1 makes its absence a failure) in front of four
// workers that the test serves on their engine sockets. No TCP port is bound but the agent's readiness
// endpoint, which the kernel chooses. Expected values: every request reaches a worker (Envoy's own
// rq_total per host equals what each worker counted, and the sum is what was sent), the arm's mode is
// "product" exactly when the arm's bound is what the agent renders, and a replica start runs the same
// argument vector and environment as the product's.

func TestMain(m *testing.M) {
	if os.Getenv("S2CROUTER_TEST_MAIN") == "1" {
		main() // the binary under test, re-executed: os.Args[1] is the subcommand
		return
	}
	os.Exit(m.Run())
}

func envoyBinary(t *testing.T) string {
	t.Helper()
	p := filepath.Join(os.Getenv("HPCBURST_ENVOY_DIR"), "envoy-1.40.2", "envoy")
	if _, err := os.Stat(p); os.Getenv("HPCBURST_ENVOY_DIR") == "" || err != nil {
		if os.Getenv("HPCBURST_REQUIRE_ENVOY") == "1" {
			t.Fatalf("no Envoy 1.40.2 at %q (HPCBURST_ENVOY_DIR)", p)
		}
		t.Skipf("no Envoy 1.40.2 at %q: set HPCBURST_ENVOY_DIR", p)
	}
	return p
}

type fakeWorkers struct{ chats [4]atomic.Int64 }

func serveWorkers(t *testing.T, run string) *fakeWorkers {
	t.Helper()
	fw := &fakeWorkers{}
	for i := 0; i < 4; i++ {
		ln, err := net.Listen("unix", filepath.Join(run, fmt.Sprintf("worker-%d.sock", i)))
		if err != nil {
			t.Fatal(err)
		}
		i := i
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/health":
			case "/v1/chat/completions":
				fw.chats[i].Add(1)
				fmt.Fprintf(w, "w%d", i)
			default:
				http.NotFound(w, r)
			}
		})}
		go func() { _ = srv.Serve(ln) }()
		t.Cleanup(func() { srv.Close() })
	}
	return fw
}

func unixClient(sock string) *http.Client {
	return &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", sock)
	}}}
}

// self runs this test binary as s2crouter.
func self(args ...string) *exec.Cmd {
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), "S2CROUTER_TEST_MAIN=1")
	return cmd
}

// waitFile waits for path to exist, or for the process to exit, within d.
func waitFile(t *testing.T, path string, exited <-chan error, d time.Duration) {
	t.Helper()
	deadline := time.After(d)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		select {
		case err := <-exited:
			t.Fatalf("s2crouter exited before %s appeared: %v", filepath.Base(path), err)
		case <-deadline:
			t.Fatalf("%s did not appear within %v", path, d)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func rqTotals(t *testing.T, admin string) map[string]int64 {
	t.Helper()
	b, err := unixGet(admin, "http://admin/clusters?format=json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Clusters []struct {
			Name  string `json:"name"`
			Hosts []struct {
				Hostname string `json:"hostname"`
				Stats    []struct {
					Name  string          `json:"name"`
					Value json.RawMessage `json:"value"`
				} `json:"stats"`
			} `json:"host_statuses"`
		} `json:"cluster_statuses"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, c := range doc.Clusters {
		if c.Name != "workers" {
			continue
		}
		for _, h := range c.Hosts {
			for _, s := range h.Stats {
				if s.Name == "rq_total" {
					var v int64
					_ = json.Unmarshal(s.Value, &v)
					if v == 0 {
						var str string
						if json.Unmarshal(s.Value, &str) == nil {
							fmt.Sscan(str, &v)
						}
					}
					out[h.Hostname] = v
				}
			}
		}
	}
	return out
}

func TestEnvoyArmsWithTheRealEnvoy(t *testing.T) {
	bin := envoyBinary(t)
	stage := t.TempDir()
	staged := agent.StagedEnvoyPath(stage)
	if staged == "" {
		t.Skip("no Envoy pinned for this architecture")
	}
	if err := os.MkdirAll(filepath.Dir(staged), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(bin, staged); err != nil {
		t.Fatal(err)
	}
	run, err := os.MkdirTemp("", "s2c") // short: Unix socket paths
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(run) })
	out := t.TempDir()
	fw := serveWorkers(t, run)

	smoke := self("envoy", "-product-smoke", "-run-dir", run, "-workers", "4", "-stage-dir", stage, "-out", filepath.Join(out, "smoke"))
	if b, err := smoke.CombinedOutput(); err != nil {
		t.Fatalf("product smoke: %v\n%s", err, b)
	}
	var ref startRecord
	if err := readJSON(filepath.Join(out, "smoke", "product-start.json"), &ref); err != nil || ref.Result != "started" || ref.Glibc == "" {
		t.Fatalf("product-start.json: %+v %v", ref, err)
	}

	// What the agent renders decides which arm is the product's: before #518 no bound, after it 125.
	rendersBound := strings.Contains(string(render4(t)), `"hash_balance_factor"`)
	cases := []struct {
		name, bound, fault string
		wantFactor         *int
		wantProduct        bool
	}{
		{"none", "none", "", nil, !rendersBound},
		{"bound", "125", "", ptr(125), rendersBound},
		{"strip", "125", "strip-bound", nil, !rendersBound},
	}
	sent := 0
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := filepath.Join(out, c.name)
			args := []string{"envoy", "-run-dir", run, "-workers", "4", "-stage-dir", stage, "-bound", c.bound, "-out", dir,
				"-reference", filepath.Join(out, "smoke", "product-start.json")}
			if c.fault != "" {
				args = append(args, "-fault", c.fault)
			}
			cmd := self(args...)
			var logb strings.Builder
			cmd.Stdout, cmd.Stderr = &logb, &logb
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			exited := make(chan error, 1)
			go func() { exited <- cmd.Wait() }()
			defer func() {
				_ = cmd.Process.Signal(syscall.SIGTERM) // through the child's pidfd (os/exec on Linux)
				select {
				case <-exited:
				case <-time.After(30 * time.Second):
					_ = cmd.Process.Kill()
				}
			}()
			waitFile(t, filepath.Join(dir, "READY"), exited, 90*time.Second)

			before := rqTotals(t, filepath.Join(run, agent.EnvoyAdminSocket))
			var counts0 [4]int64
			for i := range counts0 {
				counts0[i] = fw.chats[i].Load()
			}
			cl := unixClient(filepath.Join(run, agent.EnvoyListenerSocket))
			const n = 40
			for i := 0; i < n; i++ {
				req, _ := http.NewRequest(http.MethodPost, "http://instance/v1/chat/completions", strings.NewReader("{}"))
				req.Header.Set("x-session-id", fmt.Sprintf("%s-s%03d", c.name, i))
				resp, err := cl.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 || !strings.HasPrefix(string(body), "w") {
					t.Fatalf("request %d: %d %q", i, resp.StatusCode, body)
				}
			}
			sent += n
			after := rqTotals(t, filepath.Join(run, agent.EnvoyAdminSocket))
			var sum int64
			for i := 0; i < 4; i++ {
				host := fmt.Sprintf("worker-%d", i)
				d := after[host] - before[host]
				served := fw.chats[i].Load() - counts0[i]
				if d != served {
					t.Errorf("%s: Envoy counted %d, the worker served %d", host, d, served)
				}
				sum += d
			}
			if sum != n {
				t.Errorf("Envoy counted %d requests, %d were sent", sum, n)
			}

			var rec startRecord
			if err := readJSON(filepath.Join(dir, "start.json"), &rec); err != nil {
				t.Fatal(err)
			}
			if (rec.Mode == "product") != c.wantProduct {
				t.Errorf("mode %q, want product=%v", rec.Mode, c.wantProduct)
			}
			if (rec.Factor == nil) != (c.wantFactor == nil) || (rec.Factor != nil && *rec.Factor != *c.wantFactor) {
				t.Errorf("factor %v, want %v", rec.Factor, c.wantFactor)
			}
			if rec.Bound != c.bound {
				t.Errorf("recorded bound %q, want %q", rec.Bound, c.bound)
			}
			wantDiff := []string{}
			if !c.wantProduct {
				wantDiff = []string{boundPath}
			}
			if !slices.Equal(rec.Diff, wantDiff) {
				t.Errorf("diff %q, want %q", rec.Diff, wantDiff)
			}
			if !slices.Equal(rec.Argv, ref.Argv) || !slices.Equal(rec.Env, ref.Env) || len(rec.Env) != 0 {
				t.Errorf("argv %q env %q, the product's %q %q", rec.Argv, rec.Env, ref.Argv, ref.Env)
			}
			runCfg, _ := os.ReadFile(filepath.Join(dir, "run.json"))
			if f, _ := factorOf(runCfg); (f == nil) != (c.wantFactor == nil) {
				t.Errorf("run.json factor %v", f)
			}

			_ = cmd.Process.Signal(syscall.SIGTERM)
			select {
			case err := <-exited:
				if err != nil {
					t.Fatalf("s2crouter after TERM: %v\n%s", err, logb.String())
				}
				exited <- nil // for the deferred stop
			case <-time.After(30 * time.Second):
				t.Fatal("s2crouter did not exit within 30 s of TERM")
			}
			if err := syscall.Kill(rec.PID, 0); !errors.Is(err, syscall.ESRCH) {
				t.Errorf("Envoy (pid %d) still exists after the arm stopped: %v", rec.PID, err)
			}
		})
	}
}

func ptr(n int) *int { return &n }
