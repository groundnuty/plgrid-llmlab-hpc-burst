package dryrun

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The dry run (issue #62's brief): spike S2c's job, submitted with its own sbatch line to the fake
// Slurm in run mode (internal/slurm/fakeslurm: the job runs here as a host process, with
// CUDA_VISIBLE_DEVICES=0,1,2,3 for its 4 GPUs), through every arm, with:
//   - the workers: the engine double, started by job.sh through s2crouter worker, so through the
//     agent's own Apptainer invocation, which the double's fake Apptainer takes as it is;
//   - Envoy 1.40.2's real release binary (HPCBURST_ENVOY_DIR), staged by digest;
//   - vllm-router 0.1.17's real release (S2C_VR_BIN, from scripts/setup-vr.sh).
// It needs S2C_DRY_BIN (s2crouter and s2cload built for this host) too; scripts/test.sh sets all of
// them. Without them it skips, unless S2C_REQUIRE_DRYRUN=1.
//
// What it requires, from the brief and the issue, never from the code under test:
//   - the job's shape: plgrid-gpu-a100, 1 node, 4 GPUs, 64 CPUs, 500 GiB, a time limit of at most
//     2 h, --no-requeue;
//   - every worker on its own one device of the job's four, behind a router with keep-alive 120 s;
//   - per arm, the output a live run gives: the hit rate, each worker's request share, TTFT
//     percentiles; and the per-arm router check (the router's own counters equal the workers' and the
//     client's) passing for every arm;
//   - a silent fallback fails the job: the client sent to the other router in an Envoy arm, and the
//     bound arm run without its bound, each end the job FAILED with the arm named in failures.txt.

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type dryEnv struct {
	root, home, stage, image, ckpt, apptainer, vrBin, settings, appLog string
}

func needEnv(t *testing.T) (bin, vr, envoyBin string) {
	t.Helper()
	bin, vr = os.Getenv("S2C_DRY_BIN"), os.Getenv("S2C_VR_BIN")
	envoyBin = filepath.Join(os.Getenv("HPCBURST_ENVOY_DIR"), "envoy-1.40.2", "envoy")
	var missing []string
	for name, p := range map[string]string{"S2C_DRY_BIN/s2crouter": filepath.Join(bin, "s2crouter"), "S2C_DRY_BIN/s2cload": filepath.Join(bin, "s2cload"),
		"S2C_VR_BIN": vr, "HPCBURST_ENVOY_DIR/envoy-1.40.2/envoy": envoyBin} {
		if fi, err := os.Stat(p); bin == "" || err != nil || fi.IsDir() {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		if os.Getenv("S2C_REQUIRE_DRYRUN") == "1" {
			t.Fatalf("the dry run needs %v (scripts/test.sh sets them)", missing)
		}
		t.Skipf("the dry run needs %v (scripts/test.sh sets them)", missing)
	}
	return bin, vr, envoyBin
}

func copyFile(t *testing.T, from, to string, mode os.FileMode) {
	t.Helper()
	b, err := os.ReadFile(from)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(to, b, mode); err != nil {
		t.Fatal(err)
	}
}

// stage lays out what the live run stages: $S2C_HOME/stage (binaries, middleware, scripts, Envoy by
// digest), and the product's staging directory (the image as a sandbox directory, the checkpoint).
func stage(t *testing.T) dryEnv {
	bin, vr, envoyBin := needEnv(t)
	root, err := os.MkdirTemp("", "s2cdr") // short: the job's sockets live under root/j/<id>/tmp
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("kept the dry run's directory for a look: %s", root)
			return
		}
		os.RemoveAll(root)
	})
	e := dryEnv{root: root, home: filepath.Join(root, "home", "s2c"), vrBin: vr}
	e.stage = filepath.Join(e.home, "stage")
	copyFile(t, filepath.Join(bin, "s2crouter"), filepath.Join(e.stage, "bin", "s2crouter"), 0o755)
	copyFile(t, filepath.Join(bin, "s2cload"), filepath.Join(e.stage, "bin", "s2cload"), 0o755)
	copyFile(t, "../py/s2c_placement.py", filepath.Join(e.stage, "py", "s2c_placement.py"), 0o644)
	copyFile(t, "../scripts/job.sh", filepath.Join(e.stage, "scripts", "job.sh"), 0o755)
	copyFile(t, "../scripts/analyze.py", filepath.Join(e.stage, "scripts", "analyze.py"), 0o755)
	staged := agent.StagedEnvoyPath(e.stage)
	if err := os.MkdirAll(filepath.Dir(staged), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(envoyBin, staged); err != nil {
		t.Fatal(err)
	}
	// The product's staging layout (agentapi), with the example deployment's digest and revision.
	staging := filepath.Join(root, "staging")
	e.image = filepath.Join(staging, agentapi.StagedImagePath("sha256:befccf0e7e4089aa23926bb6f887a85c0d46c2a99e6de5c5ffdf5cb7957c5b34", agentapi.ImageFormatSandbox))
	e.ckpt = filepath.Join(staging, agentapi.StagedCheckpointDir("Qwen/Qwen2.5-1.5B-Instruct", "989aa7980e4cf806f80c7fef2b1adb7bc71aa306"))
	for _, d := range []string{e.image, e.ckpt, filepath.Join(root, "bin")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(e.image, ".fake-sandbox"), []byte("fake-sif docker.io/vllm/vllm-openai@sha256:befccf0e7e4089aa23926bb6f887a85c0d46c2a99e6de5c5ffdf5cb7957c5b34\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	e.apptainer = filepath.Join(root, "bin", "apptainer")
	if err := os.Symlink(self, e.apptainer); err != nil {
		t.Fatal(err)
	}
	e.settings = filepath.Join(root, "private.env")
	if err := os.WriteFile(e.settings, []byte("ACCOUNT_ATHENA_A100=example-a100\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.appLog = filepath.Join(root, "apptainer.log")
	return e
}

// submitLine is the job's sbatch line, printed by scripts/submit-line.sh: the same text the live
// submit sends.
func submitLine(t *testing.T, e dryEnv, jobEnv map[string]string) string {
	t.Helper()
	cmd := exec.Command("bash", "../scripts/submit-line.sh")
	cmd.Env = append(os.Environ(), "S2C_PRIVATE_SETTINGS="+e.settings, "S2C_HOME="+e.home)
	for k, v := range jobEnv {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("submit-line.sh: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	return lines[len(lines)-1]
}

// The dry run's loads: S2b's workload shape, scaled down (it proves the plumbing, not the numbers), with
// more sessions than workers in both (#62: fewer sessions than workers proves nothing).
const (
	dryLoad  = "-sessions 16 -turns 3 -concurrency 4 -agents 0.5,0.3,0.2 -sys-words 200 -task-words 40 -tool-min 20 -tool-max 60 -think-min 20ms -think-max 60ms -reply-tokens 8 -seed 1 -start-jitter 200ms"
	dryLoadH = "-sessions 32 -turns 3 -concurrency 8 -agents 0.5,0.3,0.2 -sys-words 200 -task-words 40 -tool-min 20 -tool-max 60 -think-min 20ms -think-max 60ms -reply-tokens 8 -seed 1 -start-jitter 200ms"
)

// runJob submits one job and waits for it to finish; it returns the job and its results directory.
func runJob(t *testing.T, p *fakeslurm.Platform, s *fakeslurm.Session, e dryEnv, tag string, extra map[string]string, within time.Duration) (fakeslurm.Job, string) {
	t.Helper()
	jobEnv := map[string]string{
		"MODE": "double", "IMAGE": e.image, "CHECKPOINT_DIR": e.ckpt, "APPTAINER": e.apptainer, "VR_BIN": e.vrBin,
		"RESULT_TAG": tag, "LOAD_ARGS": dryLoad, "LOAD_ARGS_H": dryLoadH, "PORT_BASE": "30420",
	}
	for k, v := range extra {
		jobEnv[k] = v
	}
	line := submitLine(t, e, jobEnv)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := s.Run(ctx, slurm.Command{Line: line, Timeout: 60 * time.Second})
	if err != nil || res.ExitCode != 0 {
		t.Fatalf("sbatch: %v exit %d: %s", err, res.ExitCode, res.Stderr)
	}
	idText, _, _ := strings.Cut(strings.TrimSpace(string(res.Stdout)), ";")
	id, err := strconv.Atoi(idText)
	if err != nil {
		t.Fatalf("sbatch printed %q", res.Stdout)
	}
	deadline := time.Now().Add(within)
	for {
		j, ok := p.Job(id)
		if !ok {
			t.Fatalf("job %d is gone", id)
		}
		if j.Finished && (j.State == "COMPLETED" || j.State == "FAILED" || j.State == "TIMEOUT" || j.State == "CANCELLED") {
			return j, filepath.Join(e.home, "results", tag)
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %d still %s after %v", id, j.State, within)
		}
		time.Sleep(500 * time.Millisecond) // polling the job's observable state
	}
}

// jobLog is the job's output: --output, or slurm-<id>.out in the submit directory (the login's HOME).
func jobLog(t *testing.T, e dryEnv, j fakeslurm.Job) string {
	p := j.Output
	if p == "" {
		p = fmt.Sprintf("slurm-%d.out", j.ID)
	}
	if !filepath.IsAbs(p) {
		p = filepath.Join(e.root, "home", p)
	}
	b, _ := os.ReadFile(p)
	return string(b)
}

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestDryRunEveryArm(t *testing.T) {
	e := stage(t)
	jobDir := filepath.Join(e.root, "j")
	p := fakeslurm.New(realClock{}, fakeslurm.Config{
		Partitions: []fakeslurm.Partition{{Name: "plgrid-gpu-a100", GPUOnly: true}},
		NotListed:  fakeslurm.NotListedAthena, FirstJobID: 1234560,
		Run: &fakeslurm.RunMode{Root: e.root, JobDir: jobDir, Env: map[string]string{
			"PATH": os.Getenv("PATH"), "HB_FAKE_APPTAINER_LOG": e.appLog,
		}},
	})
	t.Cleanup(func() { _ = p.Close() })
	s := p.Login(fakeslurm.Login{User: "exampleuser", Env: map[string]string{"HOME": filepath.Join(e.root, "home"), "SCRATCH": filepath.Join(e.root, "scratch")}})

	j, results := runJob(t, p, s, e, "dry-all", nil, 20*time.Minute)
	if j.State != "COMPLETED" {
		t.Fatalf("the dry run ended %s:\n%s\nfailures: %s", j.State, tail(jobLog(t, e, j), 60), readMaybe(filepath.Join(results, "failures.txt")))
	}

	t.Run("shape", func(t *testing.T) {
		if j.Partition != "plgrid-gpu-a100" || j.GPUs != 4 || j.CPUs != 64 || j.MemMiB != 500*1024 || j.TimeLimit <= 0 || j.TimeLimit > 120 {
			t.Errorf("job shape: partition %s, %d GPUs, %d CPUs, %d MiB, %d min", j.Partition, j.GPUs, j.CPUs, j.MemMiB, j.TimeLimit)
		}
		var sb []string
		for _, c := range p.Calls() {
			if len(c.Words) > 0 && c.Words[0] == "sbatch" {
				sb = c.Words
			}
		}
		if !slices.Contains(sb, "--no-requeue") || !slices.Contains(sb, "--nodes=1") {
			t.Errorf("sbatch words %q lack --no-requeue or --nodes=1", sb)
		}
	})

	t.Run("workers", func(t *testing.T) {
		// Each start of a worker, as the fake Apptainer recorded the container's environment: one
		// device of the job's four, distinct per regime, and keep-alive 120.
		f, err := os.Open(e.appLog)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		devs := map[string]int{}
		n := 0
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			var r enginedoubleExec
			if err := json.Unmarshal(sc.Bytes(), &r); err != nil || len(r.Argv) == 0 || r.Argv[0] != "exec" {
				continue
			}
			n++
			env := map[string]string{}
			for _, kv := range r.Env {
				k, v, _ := strings.Cut(kv, "=")
				env[k] = v
			}
			if env["VLLM_HTTP_TIMEOUT_KEEP_ALIVE"] != "120" {
				t.Errorf("a worker without keep-alive 120: %q", env["VLLM_HTTP_TIMEOUT_KEEP_ALIVE"])
			}
			devs[env["CUDA_VISIBLE_DEVICES"]]++
		}
		// Three steps (B, H, A), four workers each, and the probe's restart of the victim, the last
		// worker: each device three times and device 3 four, never a list.
		want := map[string]int{"0": 3, "1": 3, "2": 3, "3": 4}
		if n != 13 || len(devs) != 4 {
			t.Fatalf("%d worker starts on devices %v, want 13: %v", n, devs, want)
		}
		for d, w := range want {
			if devs[d] != w {
				t.Errorf("device %s started %d workers, want %d", d, devs[d], w)
			}
		}
	})

	t.Run("environment", func(t *testing.T) {
		env := readFile(t, filepath.Join(results, "env.txt"))
		if !regexp.MustCompile(`glibc[= ]+2\.\d+`).MatchString(env) {
			t.Errorf("env.txt names no glibc version:\n%s", env)
		}
		var ps struct{ Result string }
		if err := json.Unmarshal([]byte(readFile(t, filepath.Join(results, "product-start.json"))), &ps); err != nil || ps.Result != "started" {
			t.Errorf("product start: %+v %v", ps, err)
		}
	})

	arms := []string{"B1-rr", "B1-envoy-bound", "B1-envoy-nobound", "B1-vr-hash", "B2-vr-hash", "B2-envoy-nobound", "B2-envoy-bound", "B2-rr",
		"H1-rr", "H1-envoy-bound", "H1-envoy-nobound", "H1-vr-hash", "A1-rr", "A1-envoy-bound", "A1-envoy-nobound", "A1-vr-hash"}
	t.Run("arms", func(t *testing.T) {
		for _, a := range arms {
			dir := filepath.Join(results, "arms", a)
			check := readMaybe(filepath.Join(dir, "check.txt"))
			if check == "" || strings.Contains(check, "FAIL") {
				t.Errorf("%s: check.txt %q", a, check)
			}
			if strings.Count(check, "\nok ")+boolInt(strings.HasPrefix(check, "ok ")) < 5 {
				t.Errorf("%s: fewer than five passing checks:\n%s", a, check)
			}
		}
	})

	t.Run("probe", func(t *testing.T) {
		var p struct {
			Victim    string   `json:"victim"`
			Checks    []string `json:"checks"`
			Passed    bool     `json:"passed"`
			Non200    int      `json:"non_200"`
			Early     int      `json:"victim_rq_success_before_verification"`
			Placed    int      `json:"victim_placement_lines_before_verification"`
			Verified  *float64 `json:"victim_verified_s"`
			Admitted  *float64 `json:"victim_admitted_s"`
			Healthy   *float64 `json:"victim_healthy_s"`
			HoldS     float64  `json:"hold_s"`
			Samples   []any    `json:"samples"`
			StreamTok int      `json:"stream_tokens"`
		}
		if err := json.Unmarshal([]byte(readFile(t, filepath.Join(results, "arms", "H1-probe", "probe.json"))), &p); err != nil {
			t.Fatal(err)
		}
		if !p.Passed || p.Victim != "worker-3" || p.Non200 != 0 || p.Early != 0 || p.Placed != 0 || len(p.Checks) != 5 {
			t.Fatalf("probe: %+v", p)
		}
		for _, c := range p.Checks {
			if !strings.HasPrefix(c, "ok ") {
				t.Errorf("probe check %q", c)
			}
		}
		// The order of events, and the hold: the restarted engine answered, stayed unverified for the hold,
		// then took its key's requests again; requests went on throughout.
		if p.Healthy == nil || p.Verified == nil || p.Admitted == nil || *p.Verified < *p.Healthy+p.HoldS || *p.Admitted < *p.Verified || len(p.Samples) < 10 {
			t.Errorf("probe timeline: healthy %v verified %v admitted %v (hold %v s), %d samples", p.Healthy, p.Verified, p.Admitted, p.HoldS, len(p.Samples))
		}
	})

	t.Run("summary", func(t *testing.T) {
		out := filepath.Join(e.root, "summary.json")
		cmd := exec.Command("python3", "-I", "-B", "../scripts/analyze.py", results, "--json", out)
		if b, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("analyze.py: %v\n%s", err, b)
		}
		checkSummary(t, readFile(t, out), arms)
	})
}

// enginedoubleExec is one line of the fake Apptainer's log (enginedouble.ExecRecord).
type enginedoubleExec struct {
	Argv []string `json:"argv"`
	Env  []string `json:"env"`
}

func TestDryRunFallbacksFail(t *testing.T) {
	e := stage(t)
	p := fakeslurm.New(realClock{}, fakeslurm.Config{
		Partitions: []fakeslurm.Partition{{Name: "plgrid-gpu-a100", GPUOnly: true}},
		NotListed:  fakeslurm.NotListedAthena, FirstJobID: 1234570,
		Run: &fakeslurm.RunMode{Root: e.root, JobDir: filepath.Join(e.root, "j"), Env: map[string]string{"PATH": os.Getenv("PATH"), "HB_FAKE_APPTAINER_LOG": e.appLog}},
	})
	t.Cleanup(func() { _ = p.Close() })
	s := p.Login(fakeslurm.Login{User: "exampleuser", Env: map[string]string{"HOME": filepath.Join(e.root, "home"), "SCRATCH": filepath.Join(e.root, "scratch")}})
	for _, c := range []struct{ fault, plan, arm, file string }{
		{"client-to-other-router", "B:1:envoy-bound", "B1-envoy-bound", "check.txt"},
		{"strip-bound", "B:1:envoy-bound", "B1-envoy-bound", "check.txt"},
		{"early-admit", "H:1:probe", "H1-probe", "probe.out"},
	} {
		t.Run(c.fault, func(t *testing.T) {
			j, results := runJob(t, p, s, e, "fault-"+c.fault, map[string]string{"S2C_FAULT": c.fault, "PLAN": c.plan}, 10*time.Minute)
			if j.State != "FAILED" {
				t.Fatalf("with S2C_FAULT=%s the job ended %s, want FAILED:\n%s", c.fault, j.State, tail(jobLog(t, e, j), 40))
			}
			f := readMaybe(filepath.Join(results, "failures.txt"))
			if !strings.Contains(f, c.arm) {
				t.Fatalf("failures.txt does not name %s:\n%s", c.arm, f)
			}
			if check := readMaybe(filepath.Join(results, "arms", c.arm, c.file)); !strings.Contains(check, "FAIL") {
				t.Fatalf("%s's %s shows no FAIL:\n%s", c.arm, c.file, check)
			}
			if c.fault == "early-admit" && !strings.Contains(readMaybe(filepath.Join(results, "arms", c.arm, c.file)), "FAIL nothing reaches the restarted worker before its verification") {
				t.Fatalf("early admission was not caught by its own check:\n%s", readMaybe(filepath.Join(results, "arms", c.arm, c.file)))
			}
		})
	}
}

func readMaybe(p string) string { b, _ := os.ReadFile(p); return string(b) }

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func tail(s string, n int) string {
	l := strings.Split(s, "\n")
	if len(l) > n {
		l = l[len(l)-n:]
	}
	return strings.Join(l, "\n")
}

// checkSummary reads analyze.py's --json rows: per arm, the output a live run gives (the hit rate from
// the workers' counters, each worker's request share, TTFT percentiles, the router's own counts, every
// check passing, the pass rule against rr), and the probe's row.
func checkSummary(t *testing.T, js string, arms []string) {
	t.Helper()
	var rows []struct {
		Name      string   `json:"name"`
		Arm       string   `json:"arm"`
		Skipped   bool     `json:"skipped"`
		HitRate   *float64 `json:"worker_hit_rate"`
		P50       *float64 `json:"ttft_p50_ms"`
		P95       *float64 `json:"ttft_p95_ms"`
		P99       *float64 `json:"ttft_p99_ms"`
		Counts    []int    `json:"router_counts"`
		Errors    []string `json:"errors"`
		Placement *struct {
			Requests []int     `json:"requests_per_worker"`
			Share    []float64 `json:"request_share_per_worker"`
			Peak     []int     `json:"peak_in_flight_per_worker"`
		} `json:"placement"`
		Checks []struct {
			Check string `json:"check"`
			OK    bool   `json:"ok"`
		} `json:"checks"`
		PassRule *struct {
			Verdict string `json:"verdict"`
		} `json:"pass_rule"`
	}
	if err := json.Unmarshal([]byte(js), &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, r := range rows {
		seen[r.Name] = true
		if r.Skipped || len(r.Errors) > 0 {
			t.Errorf("%s: skipped %v, errors %v", r.Name, r.Skipped, r.Errors)
			continue
		}
		for _, c := range r.Checks {
			if !c.OK {
				t.Errorf("%s: check %s failed", r.Name, c.Check)
			}
		}
		if r.Arm == "probe" || r.Arm == "relay-control" {
			if want := map[string]int{"probe": 5, "relay-control": 3}[r.Arm]; len(r.Checks) != want {
				t.Errorf("%s: %d checks, want %d", r.Name, len(r.Checks), want)
			}
			continue
		}
		if r.HitRate == nil || r.P50 == nil || r.P95 == nil || r.P99 == nil || *r.P50 > *r.P95 || *r.P95 > *r.P99 {
			t.Errorf("%s: hit rate %v, TTFT %v / %v / %v", r.Name, r.HitRate, r.P50, r.P95, r.P99)
		}
		if r.Placement == nil || len(r.Placement.Share) != 4 || len(r.Placement.Requests) != 4 || len(r.Placement.Peak) != 4 || len(r.Counts) != 4 {
			t.Errorf("%s: placement %+v, router counts %v", r.Name, r.Placement, r.Counts)
			continue
		}
		var share float64
		for i, x := range r.Placement.Share {
			share += x
			if r.Counts[i] != r.Placement.Requests[i] {
				t.Errorf("%s: worker %d: the router counted %d, the worker %d", r.Name, i, r.Counts[i], r.Placement.Requests[i])
			}
		}
		if share < 0.999 || share > 1.001 {
			t.Errorf("%s: shares sum to %v", r.Name, share)
		}
		if r.PassRule == nil || (r.Arm == "rr") != (r.PassRule.Verdict == "baseline") {
			t.Errorf("%s: pass rule %+v", r.Name, r.PassRule)
		}
	}
	for _, a := range append(arms, "H1-probe", "A1-relay-control") {
		if !seen[a] {
			t.Errorf("no summary row for %s", a)
		}
	}
}

// TestDryRunProbeRepeat runs the probe alone S2C_DRY_PROBE_REPEAT times (it skips when that is unset) and
// reports each run's failed probe requests with Envoy's own reply: the evidence for how often, and why, a
// request racing the victim's death is answered 503.
func TestDryRunProbeRepeat(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("S2C_DRY_PROBE_REPEAT"))
	if n < 1 {
		t.Skip("set S2C_DRY_PROBE_REPEAT=<runs> to run the probe alone repeatedly")
	}
	e := stage(t)
	p := fakeslurm.New(realClock{}, fakeslurm.Config{
		Partitions: []fakeslurm.Partition{{Name: "plgrid-gpu-a100", GPUOnly: true}},
		NotListed:  fakeslurm.NotListedAthena, FirstJobID: 1234580,
		Run: &fakeslurm.RunMode{Root: e.root, JobDir: filepath.Join(e.root, "j"), Env: map[string]string{"PATH": os.Getenv("PATH"), "HB_FAKE_APPTAINER_LOG": e.appLog}},
	})
	t.Cleanup(func() { _ = p.Close() })
	s := p.Login(fakeslurm.Login{User: "exampleuser", Env: map[string]string{"HOME": filepath.Join(e.root, "home"), "SCRATCH": filepath.Join(e.root, "scratch")}})
	failedRuns := 0
	for i := 0; i < n; i++ {
		tag := fmt.Sprintf("probe-%d", i)
		_, results := runJob(t, p, s, e, tag, map[string]string{"PLAN": "H:1:probe"}, 10*time.Minute)
		var pr struct {
			Samples []struct {
				AtS    float64 `json:"at_s"`
				Status int     `json:"status"`
				Err    string  `json:"err"`
			} `json:"samples"`
			Non200 int `json:"non_200"`
		}
		_ = json.Unmarshal([]byte(readMaybe(filepath.Join(results, "arms", "H1-probe", "probe.json"))), &pr)
		if pr.Non200 > 0 {
			failedRuns++
		}
		for _, x := range pr.Samples {
			if x.Status != 200 || x.Err != "" {
				t.Logf("run %d: %.4f s after the kill: %d %s", i, x.AtS, x.Status, x.Err)
			}
		}
	}
	t.Logf("%d of %d probe runs saw a failed request", failedRuns, n)
}
