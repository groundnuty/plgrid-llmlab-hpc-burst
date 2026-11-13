package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/envoypin"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// One Envoy arm (HBEP-5, Inside an Instance; #429). The configuration is the agent's own:
// agent.RenderEnvoyConfig's bytes, which agent.CheckEnvoyConfig must accept, for the Instance's
// workers on their engine sockets, health-checked through the agent's own readiness endpoint
// (agent.NewReadiness) with every worker verified. The arm's bound (bound.go) changes at most one field.
//
// Two ways to start it:
//   - "product": the arm's configuration IS the agent's rendering (applyBound changed nothing), so
//     agent.StartEnvoy starts it, exactly as the agent does: the pin checked through the descriptor it
//     runs, the node's glibc checked, the guard, the bootstrap written 0600, a process group of its own,
//     the wait for both sockets, and the check of what Envoy listens on.
//   - "replica": the configuration was edited, which agent.StartEnvoy cannot run (it renders its own
//     bytes, and after #518 its guard pins the factor to 125, envoyguard.go). The same steps are taken
//     here with the edited bytes: the pin (envoypin) through the descriptor that runs, the bootstrap
//     written 0600, the agent's argument vector and empty environment in the run directory, a process
//     group of its own (procgroup), then agent.CheckEnvoySockets. The glibc check is the product
//     smoke's, run first on the same node with the same binary (job.sh skips every Envoy arm when it
//     refuses). Every replica start compares Envoy's /proc cmdline and environ with the product smoke's
//     (-reference), so the two starts are shown to run the same program the same way.
//
// envoyArgs restates agent.StartEnvoy's vector (internal/agent/envoy.go); the reference comparison is
// what holds it to the product's.

const (
	envoyReadyWithin = 60 * time.Second // agent's envoyReady
	envoyStopGrace   = 10 * time.Second
)

func envoyArgs(runDir string) []string {
	return []string{"-c", filepath.Join(runDir, agent.EnvoyConfigFile), "--disable-hot-restart", "--concurrency", "2",
		"--log-path", filepath.Join(runDir, agent.EnvoyLogFile), "--log-level", "warn"}
}

// startRecord is start.json (and, for the smoke, product-start.json).
type startRecord struct {
	Mode         string   `json:"mode"` // product | replica | smoke
	Bound        string   `json:"bound,omitempty"`
	Factor       *int     `json:"factor"`
	ConfigSHA256 string   `json:"config_sha256,omitempty"`
	PID          int      `json:"pid,omitempty"`
	Argv         []string `json:"argv"`
	Env          []string `json:"env"`
	Diff         []string `json:"diff"`
	EnvoySHA256  string   `json:"envoy_sha256,omitempty"`
	Glibc        string   `json:"glibc"`
	Result       string   `json:"result,omitempty"` // the smoke: started | refused: <error>
}

type envoyOpts struct {
	runDir, stageDir, bound, out, reference, fault string
	workers                                        int
	smoke                                          bool
}

func envoyMain(args []string) int {
	fs := flag.NewFlagSet("envoy", flag.ContinueOnError)
	var o envoyOpts
	fs.StringVar(&o.runDir, "run-dir", "", "the run directory (absolute)")
	fs.IntVar(&o.workers, "workers", 0, "workers, on <run-dir>/worker-<i>.sock")
	fs.StringVar(&o.stageDir, "stage-dir", "", "the staging directory holding envoy/<sha256>/envoy")
	fs.StringVar(&o.bound, "bound", "", "hash_balance_factor, or none")
	fs.StringVar(&o.out, "out", "", "directory for the arm's records")
	fs.StringVar(&o.reference, "reference", "", "the product smoke's product-start.json")
	fs.StringVar(&o.fault, "fault", "", "test seam: strip-bound runs without the bound while recording -bound")
	fs.BoolVar(&o.smoke, "product-smoke", false, "start agent.StartEnvoy unchanged once, record it, stop it")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if o.runDir == "" || !filepath.IsAbs(o.runDir) || o.workers < 1 || o.stageDir == "" || o.out == "" {
		return fail("envoy: -run-dir (absolute), -workers, -stage-dir and -out are required")
	}
	if !o.smoke && o.bound == "" {
		return fail("envoy: -bound is required")
	}
	if o.fault != "" && o.fault != "strip-bound" {
		return fail("envoy: unknown -fault %q", o.fault)
	}
	if err := os.MkdirAll(o.out, 0o755); err != nil {
		return fail("envoy: %v", err)
	}
	if o.smoke {
		return envoySmoke(o)
	}
	return envoyArm(o)
}

// instanceConfig is the agent's EnvoyConfig for the workers, with a readiness endpoint holding each
// verified.
func instanceConfig(runDir string, n int) (agent.EnvoyConfig, *agent.Readiness, error) {
	ready, err := agent.NewReadiness()
	if err != nil {
		return agent.EnvoyConfig{}, nil, err
	}
	cfg := agent.EnvoyConfig{RunDir: runDir, ReadinessPort: ready.Port()}
	ws := map[string]agent.ReadyWorker{}
	for i := 0; i < n; i++ {
		name := agentapi.WorkerName(i)
		sock := filepath.Join(runDir, name+".sock")
		cfg.Workers = append(cfg.Workers, agent.EnvoyWorker{Name: name, Socket: sock})
		ws[name] = agent.ReadyWorker{Socket: sock, Verified: true}
	}
	ready.Set(ws)
	return cfg, ready, nil
}

func envoySmoke(o envoyOpts) int {
	cfg, ready, err := instanceConfig(o.runDir, o.workers)
	if err != nil {
		return fail("envoy smoke: readiness: %v", err)
	}
	defer ready.Close()
	rec := startRecord{Mode: "smoke", Glibc: nodeGlibc(), Diff: []string{}}
	bin := agent.StagedEnvoyPath(o.stageDir)
	p, err := agent.StartEnvoy(agent.EnvoyStart{Binary: bin, Config: cfg, Ready: envoyReadyWithin}, time.Now())
	if err != nil {
		rec.Result = "refused: " + err.Error()
		_ = writeJSON(filepath.Join(o.out, "product-start.json"), rec)
		fmt.Fprintln(os.Stderr, "s2crouter: envoy smoke: agent.StartEnvoy refused:", err)
		return 3
	}
	rec.Result, rec.PID = "started", p.Group.PID()
	rec.Argv, rec.Env, err = procArgvEnv(rec.PID)
	if err == nil {
		rec.EnvoySHA256, err = fileSHA256(bin)
	}
	stopEnvoy(p.Group)
	p.Close()
	if err != nil {
		return fail("envoy smoke: %v", err)
	}
	if err := writeJSON(filepath.Join(o.out, "product-start.json"), rec); err != nil {
		return fail("envoy smoke: %v", err)
	}
	return 0
}

func envoyArm(o envoyOpts) int {
	factor, none, err := parseBound(o.bound)
	if err != nil {
		return fail("envoy: %v", err)
	}
	var ref startRecord
	if o.reference != "" {
		if err := readJSON(o.reference, &ref); err != nil {
			return fail("envoy: reference: %v", err)
		}
		if ref.Result != "started" {
			return fail("envoy: the product smoke did not start Envoy (%s): no arm runs it", ref.Result)
		}
	}
	cfg, ready, err := instanceConfig(o.runDir, o.workers)
	if err != nil {
		return fail("envoy: readiness: %v", err)
	}
	defer ready.Close()
	rendered, err := agent.RenderEnvoyConfig(cfg)
	if err != nil {
		return fail("envoy: render: %v", err)
	}
	if err := agent.CheckEnvoyConfig(rendered, cfg); err != nil {
		return fail("envoy: the agent's guard refuses its own rendering: %v", err)
	}
	cfgFactor, cfgNone := factor, none
	if o.fault == "strip-bound" {
		cfgNone = true // the seam: Envoy runs without the bound while start.json records -bound
	}
	runCfg, changed, err := applyBound(rendered, cfgFactor, cfgNone)
	if err != nil {
		return fail("envoy: bound: %v", err)
	}
	bin := agent.StagedEnvoyPath(o.stageDir)
	if bin == "" {
		return fail("envoy: no Envoy is pinned for %s", runtime.GOARCH)
	}
	rec := startRecord{Mode: "product", Bound: o.bound, Glibc: nodeGlibc()}
	var g *procgroup.Group
	var closeLog func()
	if !changed {
		p, err := agent.StartEnvoy(agent.EnvoyStart{Binary: bin, Config: cfg, Ready: envoyReadyWithin}, time.Now())
		if err != nil {
			return fail("envoy: agent.StartEnvoy: %v", err)
		}
		g, closeLog = p.Group, p.Close
	} else {
		rec.Mode = "replica"
		g, closeLog, err = replicaStart(bin, cfg, runCfg)
		if err != nil {
			return fail("envoy: replica start: %v", err)
		}
	}
	defer closeLog()
	code := func() int {
		rec.PID = g.PID()
		if rec.Argv, rec.Env, err = procArgvEnv(rec.PID); err != nil {
			return fail("envoy: %v", err)
		}
		if o.reference != "" && (!slices.Equal(rec.Argv, ref.Argv) || !slices.Equal(rec.Env, ref.Env)) {
			return fail("envoy: this start's argv %q and env %q differ from the product start's %q and %q", rec.Argv, rec.Env, ref.Argv, ref.Env)
		}
		cfgPath := ""
		for i, a := range rec.Argv {
			if a == "-c" && i+1 < len(rec.Argv) {
				cfgPath = rec.Argv[i+1]
			}
		}
		started, err := os.ReadFile(cfgPath)
		if err != nil {
			return fail("envoy: the configuration Envoy was started with (%q): %v", cfgPath, err)
		}
		sum := sha256.Sum256(started)
		rec.ConfigSHA256 = hex.EncodeToString(sum[:])
		if rec.Factor, err = factorOf(started); err != nil {
			return fail("envoy: %v", err)
		}
		if rec.Diff, err = diffPaths(rendered, started); err != nil {
			return fail("envoy: %v", err)
		}
		if rec.EnvoySHA256, err = fileSHA256(bin); err != nil {
			return fail("envoy: %v", err)
		}
		for name, b := range map[string][]byte{"rendered.json": rendered, "run.json": started} {
			if err := os.WriteFile(filepath.Join(o.out, name), b, 0o644); err != nil {
				return fail("envoy: %v", err)
			}
		}
		if err := writeJSON(filepath.Join(o.out, "start.json"), rec); err != nil {
			return fail("envoy: %v", err)
		}
		admin := filepath.Join(o.runDir, agent.EnvoyAdminSocket)
		if err := waitHealthy(g, admin, o.workers, envoyReadyWithin); err != nil {
			return fail("envoy: %v", err)
		}
		if err := os.WriteFile(filepath.Join(o.out, "READY"), nil, 0o644); err != nil {
			return fail("envoy: %v", err)
		}
		sigs := make(chan os.Signal, 2)
		signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
		select {
		case <-sigs:
			return 0
		case <-g.Exited():
			return fail("envoy exited with code %d while serving", g.ExitCode())
		}
	}()
	stopEnvoy(g)
	return code
}

// replicaStart is agent.StartEnvoy's sequence for an edited configuration (see the file comment).
func replicaStart(binPath string, c agent.EnvoyConfig, raw []byte) (*procgroup.Group, func(), error) {
	pin, err := envoypin.For(runtime.GOARCH)
	if err != nil {
		return nil, nil, err
	}
	bin, err := os.Open(binPath)
	if err != nil {
		return nil, nil, err
	}
	defer bin.Close()
	st, err := bin.Stat()
	if err != nil {
		return nil, nil, err
	}
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(bin, 0, st.Size())); err != nil {
		return nil, nil, err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != pin.SHA256 {
		return nil, nil, fmt.Errorf("%s has SHA-256 %s, not the pin of Envoy %s for %s", binPath, got, pin.Version, pin.Arch)
	}
	cfgPath := filepath.Join(c.RunDir, agent.EnvoyConfigFile)
	if err := writeAtomic(cfgPath, raw, 0o600); err != nil {
		return nil, nil, err
	}
	listener, admin := filepath.Join(c.RunDir, agent.EnvoyListenerSocket), filepath.Join(c.RunDir, agent.EnvoyAdminSocket)
	for _, p := range []string{listener, admin} {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, nil, err
		}
	}
	var r syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &r); err == nil {
		r.Cur = r.Max
		_ = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &r)
	}
	logf, err := os.OpenFile(filepath.Join(c.RunDir, agent.EnvoyLogFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, nil, err
	}
	g, err := procgroup.Start(procgroup.Spec{
		Path: "/proc/self/fd/3", Argv0: "envoy", ExtraFiles: []*os.File{bin},
		Args: envoyArgs(c.RunDir), Env: []string{}, Dir: c.RunDir, Stdout: logf, Stderr: logf,
	}, time.Now())
	if err != nil {
		logf.Close()
		return nil, nil, err
	}
	// Both sockets listening, and nothing else: the agent's own check, polled until it passes.
	deadline := time.Now().Add(envoyReadyWithin)
	for {
		err := agent.CheckEnvoySockets(g.PID(), listener, admin)
		if err == nil {
			return g, func() { logf.Close() }, nil
		}
		select {
		case <-g.Exited():
			logf.Close()
			return nil, nil, fmt.Errorf("envoy exited with code %d before it listened", g.ExitCode())
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			stopEnvoy(g)
			logf.Close()
			return nil, nil, fmt.Errorf("envoy did not listen on its two sockets within %v: %v", envoyReadyWithin, err)
		}
	}
}

func stopEnvoy(g *procgroup.Group) {
	g.Stop(envoyStopGrace)
	<-g.Done()
}

// waitHealthy waits until Envoy's own /clusters?format=json holds every worker healthy: its active
// check passed, none pending, none failing, none ejected (the agent's parseClusters rule).
func waitHealthy(g *procgroup.Group, admin string, n int, within time.Duration) error {
	deadline := time.Now().Add(within)
	var last string
	for {
		hs, err := clusterHealth(admin)
		if err == nil {
			healthy := 0
			for _, ok := range hs {
				if ok {
					healthy++
				}
			}
			if len(hs) == n && healthy == n {
				return nil
			}
			last = fmt.Sprintf("%d of %d hosts healthy (%d listed)", healthy, n, len(hs))
		} else {
			last = err.Error()
		}
		select {
		case <-g.Exited():
			return fmt.Errorf("envoy exited with code %d before its workers were healthy", g.ExitCode())
		case <-time.After(200 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the workers were not all healthy in Envoy within %v: %s", within, last)
		}
	}
}

// clusterHealth reads the workers' cluster from the admin socket: hostname to healthy.
func clusterHealth(admin string) (map[string]bool, error) {
	body, err := unixGet(admin, "http://admin/clusters?format=json")
	if err != nil {
		return nil, err
	}
	var doc struct {
		Clusters []struct {
			Name  string `json:"name"`
			Hosts []struct {
				Hostname string `json:"hostname"`
				Health   struct {
					FailedActive  bool `json:"failed_active_health_check"`
					FailedOutlier bool `json:"failed_outlier_check"`
					PendingActive bool `json:"pending_active_hc"`
				} `json:"health_status"`
			} `json:"host_statuses"`
		} `json:"cluster_statuses"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, err
	}
	for _, c := range doc.Clusters {
		if c.Name == "workers" {
			out := map[string]bool{}
			for _, h := range c.Hosts {
				out[h.Hostname] = !h.Health.FailedActive && !h.Health.FailedOutlier && !h.Health.PendingActive
			}
			return out, nil
		}
	}
	return nil, errors.New("no workers cluster in /clusters")
}

// unixGet sends one GET over a Unix socket and returns the body of a 200.
func unixGet(socket, url string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socket)
	}, DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	return b, nil
}

// procArgvEnv reads a process's argument vector and environment from /proc.
func procArgvEnv(pid int) ([]string, []string, error) {
	split := func(b []byte) []string {
		out := []string{}
		for _, s := range bytes.Split(bytes.TrimRight(b, "\x00"), []byte{0}) {
			if len(s) > 0 {
				out = append(out, string(s))
			}
		}
		return out
	}
	cl, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		return nil, nil, err
	}
	env, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
	if err != nil {
		return nil, nil, err
	}
	return split(cl), split(env), nil
}

var glibcRe = regexp.MustCompile(`^glibc (\d+\.\d+)`)

// nodeGlibc is the node's glibc version from getconf GNU_LIBC_VERSION (the agent's probe), version only.
func nodeGlibc() string {
	out, err := exec.Command("getconf", "GNU_LIBC_VERSION").Output()
	if err != nil {
		return "unknown"
	}
	if m := glibcRe.FindStringSubmatch(strings.TrimSpace(string(out))); m != nil {
		return m[1]
	}
	return "unknown"
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func writeAtomic(path string, b []byte, mode os.FileMode) error {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	_, werr := f.Write(b)
	if cerr := f.Chmod(mode); werr == nil {
		werr = cerr
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = os.Rename(tmp, path)
	}
	if werr != nil {
		os.Remove(tmp)
	}
	return werr
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
