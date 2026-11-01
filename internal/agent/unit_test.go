package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

const testKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func testLaunch() agentapi.Launch {
	return agentapi.Launch{
		Image:       "docker.io/vllm/vllm-openai",
		ImageDigest: "sha256:" + strings.Repeat("ab", 32),
		Command:     []string{"vllm", "serve", "/hpcburst/model", "--uds=/hpcburst/run/worker-0.sock", "--max-num-seqs=256"},
		Env:         []agentapi.EnvVar{{Name: "HF_HUB_OFFLINE", Value: "1"}},
		KeyEnv:      "VLLM_API_KEY",
		Checkpoint:  agentapi.Checkpoint{Repository: "org/model", Revision: strings.Repeat("c", 40)},
		GPUs:        1,
		MaxNumSeqs:  256,
	}
}

// The argument vector is HBEP-7's: exec with GPU support and a clean
// environment, the checkpoint read-only at /hpcburst/model, the run directory
// at /hpcburst/run, then the image and the launch's Command unchanged. The
// expected vector is written out by hand.
func TestWorkerExecArgs(t *testing.T) {
	w := WorkerExec{Image: "/s/images/x.sif", CheckpointDir: "/s/checkpoints/org/model/rev", RunDir: "/tmp/hb-1", CacheDir: "/s/alloc/hb-x/cache", Launch: testLaunch(), Key: testKey}
	want := []string{
		"exec", "--nv", "--cleanenv", "--no-home",
		"--bind", "/s/checkpoints/org/model/rev:/hpcburst/model:ro",
		"--bind", "/tmp/hb-1:/hpcburst/run",
		"--bind", "/s/alloc/hb-x/cache:/hpcburst/cache",
		"/s/images/x.sif",
		"vllm", "serve", "/hpcburst/model", "--uds=/hpcburst/run/worker-0.sock", "--max-num-seqs=256",
	}
	if got := w.Args(); !reflect.DeepEqual(got, want) {
		t.Fatalf("args:\n got %q\nwant %q", got, want)
	}
}

// The key never reaches the argument vector: it is passed only as
// APPTAINERENV_<KeyEnv>.
func TestWorkerExecKeyOnlyInEnvironment(t *testing.T) {
	w := WorkerExec{Image: "/i.sif", CheckpointDir: "/c", RunDir: "/r", Launch: testLaunch(), Key: testKey, BaseEnv: []string{"PATH=/usr/bin"}}
	for _, a := range w.Args() {
		if strings.Contains(a, testKey) {
			t.Fatalf("the key is on the command line: %q", a)
		}
	}
	env, err := w.Env()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(env, "APPTAINERENV_VLLM_API_KEY="+testKey) {
		t.Fatalf("the key is not passed as APPTAINERENV_VLLM_API_KEY: %q", env)
	}
}

// The engine's environment is exactly the launch's: inherited APPTAINERENV_
// and SINGULARITYENV_ variables are dropped, the launch's are added with the
// prefix.
func TestWorkerExecEnvironmentIsTheLaunchs(t *testing.T) {
	w := WorkerExec{Launch: testLaunch(), Key: testKey, BaseEnv: []string{
		"PATH=/usr/bin", "APPTAINERENV_LEAK=1", "SINGULARITYENV_OLD=2", "HOME=/home/u",
	}}
	env, err := w.Env()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"PATH=/usr/bin", "HOME=/home/u", "APPTAINERENV_HF_HUB_OFFLINE=1",
		"APPTAINERENV_XDG_CACHE_HOME=/hpcburst/cache/xdg",
		"APPTAINERENV_VLLM_CACHE_ROOT=/hpcburst/cache/vllm",
		"APPTAINERENV_TORCHINDUCTOR_CACHE_DIR=/hpcburst/cache/torchinductor",
		"APPTAINERENV_TRITON_CACHE_DIR=/hpcburst/cache/triton",
		"APPTAINERENV_FLASHINFER_WORKSPACE_BASE=/hpcburst/cache/flashinfer",
		"APPTAINERENV_CUDA_CACHE_PATH=/hpcburst/cache/cuda",
		"APPTAINERENV_VLLM_API_KEY=" + testKey}
	if !reflect.DeepEqual(env, want) {
		t.Fatalf("env:\n got %q\nwant %q", env, want)
	}
}

// A launch that sets a cache variable itself keeps its value: the launch's
// variables are a deployment-ID input, so the agent adds a cache variable only
// where the launch is silent (science-agent ruling, 1 Nov 2026, issue #125).
func TestWorkerExecLaunchCacheVariableWins(t *testing.T) {
	l := testLaunch()
	l.Env = append(l.Env, agentapi.EnvVar{Name: "VLLM_CACHE_ROOT", Value: "/opt/vllm-cache"})
	env, err := (WorkerExec{Launch: l, Key: testKey}).Env()
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, kv := range env {
		if strings.HasPrefix(kv, "APPTAINERENV_VLLM_CACHE_ROOT=") {
			got = append(got, kv)
		}
	}
	if want := []string{"APPTAINERENV_VLLM_CACHE_ROOT=/opt/vllm-cache"}; !reflect.DeepEqual(got, want) {
		t.Errorf("VLLM_CACHE_ROOT carried as %q, want only the launch's %q", got, want)
	}
	if !slices.Contains(env, "APPTAINERENV_TRITON_CACHE_DIR=/hpcburst/cache/triton") {
		t.Errorf("the other cache variables are no longer set: %q", env)
	}
}

func TestWorkerExecRefusesKeyVariableInLaunchEnv(t *testing.T) {
	l := testLaunch()
	l.Env = append(l.Env, agentapi.EnvVar{Name: "VLLM_API_KEY", Value: "x"})
	if _, err := (WorkerExec{Launch: l, Key: testKey}).Env(); err == nil {
		t.Fatal("a launch that sets the key variable itself was accepted")
	}
	l = testLaunch()
	l.Env = []agentapi.EnvVar{{Name: "BAD-NAME", Value: "x"}}
	if _, err := (WorkerExec{Launch: l, Key: testKey}).Env(); err == nil {
		t.Fatal("a launch variable with a hyphen was accepted")
	}
}

func writeAgentJSON(t *testing.T, dir string, c agentapi.AgentConfig) {
	t.Helper()
	b, err := c.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, agentapi.ConfigFileName), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func directConfig() agentapi.AgentConfig {
	return agentapi.AgentConfig{
		ProtocolVersion: agentapi.ProtocolVersion,
		Tag:             "hb-example-abcdef012345",
		Mode:            agentapi.DialHomeDirect,
		Endpoint:        "127.0.0.1:29290",
		CertSPKISHA256:  strings.Repeat("a", 64),
		FrpcDigest:      strings.Repeat("d", 64),
		StagingDir:      "/scratch/staging",
		DialHomeDir:     "/home/u/.hpcburst/dial-home",
	}
}

func TestLoadJob(t *testing.T) {
	dir := t.TempDir()
	writeAgentJSON(t, dir, directConfig())
	if err := agentapi.WriteKeyFile(filepath.Join(dir, CredentialFileName), testKey); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"SLURM_JOB_ID": "123", "SLURM_RESTART_COUNT": "2", "TMPDIR": "/tmp/x"}
	j, err := LoadJob(dir, func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if j.JobID != "123" || j.RestartCount != 2 || j.Credential != testKey || j.TmpDir != "/tmp/x" || j.Config.Tag != "hb-example-abcdef012345" {
		t.Fatalf("job: %+v", j)
	}
}

// A job without SLURM_JOB_ID, with a bad restart count and no credential
// fails, naming each problem and never the credential.
func TestLoadJobReportsEveryProblem(t *testing.T) {
	dir := t.TempDir()
	writeAgentJSON(t, dir, directConfig())
	env := map[string]string{"SLURM_RESTART_COUNT": "x"}
	_, err := LoadJob(dir, func(k string) string { return env[k] })
	if err == nil {
		t.Fatal("no error")
	}
	for _, want := range []string{"SLURM_JOB_ID", "SLURM_RESTART_COUNT", "credential"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
	}
}

func TestMakeRunDirRefusesALongPath(t *testing.T) {
	long := filepath.Join(t.TempDir(), strings.Repeat("d", 90))
	if err := os.MkdirAll(long, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := MakeRunDir(long); err == nil || !strings.Contains(err.Error(), "TMPDIR") {
		t.Fatalf("a run directory too long for a socket was accepted: %v", err)
	}
	short, err := os.MkdirTemp("", "hbt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(short)
	d, err := MakeRunDir(short)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(d)
	if err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("run directory mode: %v %v", fi.Mode(), err)
	}
}

// nvidia-smi's CSV, as --query-gpu=index,name,memory.total
// --format=csv,noheader,nounits prints it.
func TestParseGPUs(t *testing.T) {
	out := "0, NVIDIA GH200 480GB, 97871\n1, NVIDIA GH200 480GB, 97871\n"
	got, err := parseGPUs([]byte(out))
	if err != nil {
		t.Fatal(err)
	}
	want := []agentapi.GPU{{Index: 0, Name: "NVIDIA GH200 480GB", MemoryMiB: 97871}, {Index: 1, Name: "NVIDIA GH200 480GB", MemoryMiB: 97871}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v", got)
	}
}

func TestCPUArchIsTheKernelsMachineName(t *testing.T) {
	got, err := CPUArch()
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile("/proc/sys/kernel/arch")
	if err != nil {
		t.Skipf("no /proc/sys/kernel/arch to compare with: %v", err)
	}
	if want := strings.TrimSpace(string(b)); got != want {
		t.Fatalf("CPUArch %q, kernel says %q", got, want)
	}
}

// The ssh forward has the options HBEP-7 and HBEP-6 (Dial-home modes) ask
// for: the restricted key and the pinned host key from the dial-home
// directory, batch mode, ExitOnForwardFailure, ServerAliveInterval=10 with
// ServerAliveCountMax=3, the ssh server's port from agent.json's endpoint
// (any port, not only 22), and a forward from the node's IPv4 loopback
// address to the remote loopback port.
func TestSSHForwardArgs(t *testing.T) {
	c := directConfig()
	c.Mode = agentapi.DialHomeSSH
	c.Endpoint = "cp.example:2222"
	c.SSH = &agentapi.SSHForward{User: "hb", AgentPort: 29223, DialHomePort: 29227}
	for _, tc := range []struct {
		remote, local int
		forward       string
	}{
		{29223, 41000, "127.0.0.1:41000:127.0.0.1:29223"}, // connection 1, to the agent endpoint
		{29227, 41001, "127.0.0.1:41001:127.0.0.1:29227"}, // connection 2, to the dial-home listener
	} {
		f := &SSHForward{Config: c, RemotePort: tc.remote}
		got, err := f.Args(tc.local)
		if err != nil {
			t.Fatal(err)
		}
		want := []string{
			"-N",
			"-F", "/dev/null",
			"-o", "ControlMaster=no",
			"-o", "ControlPath=none",
			"-o", "BatchMode=yes",
			"-o", "IdentitiesOnly=yes",
			"-o", "StrictHostKeyChecking=yes",
			"-o", "UserKnownHostsFile=/home/u/.hpcburst/dial-home/known_hosts",
			"-o", "ExitOnForwardFailure=yes",
			"-o", "ServerAliveInterval=10",
			"-o", "ServerAliveCountMax=3",
			"-o", "ConnectTimeout=20",
			"-i", "/home/u/.hpcburst/dial-home/key",
			"-p", "2222",
			"-L", tc.forward,
			"hb@cp.example",
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("args:\n got %q\nwant %q", got, want)
		}
	}
	for _, bad := range []struct{ remote, local int }{{0, 41000}, {29223, 0}, {70000, 41000}} {
		f := &SSHForward{Config: c, RemotePort: bad.remote}
		if _, err := f.Args(bad.local); err == nil {
			t.Errorf("Args accepted remote %d, local %d", bad.remote, bad.local)
		}
	}
}

// Free text is cut at the protocol's bounds, at a rune boundary.
func TestCutFreeText(t *testing.T) {
	long := strings.Repeat("é", 300) // 600 bytes
	c := agentapi.CheckResults{
		Verify:  &agentapi.VerifyResult{Error: long},
		Qualify: &agentapi.QualifyResult{Checks: []agentapi.CheckOutcome{{Name: agentapi.CheckChat, Message: long}}},
		WarmUp:  &agentapi.WarmUpResult{Rounds: []agentapi.WarmUpRound{{Round: 1, Requests: []agentapi.RequestTiming{{Error: long}}}}},
	}
	cutFreeText(&c)
	if n := len(c.Verify.Error); n != 256 {
		t.Errorf("verify error is %d bytes, want 256", n)
	}
	if n := len(c.Qualify.Checks[0].Message); n != 256 {
		t.Errorf("check message is %d bytes, want 256", n)
	}
	if n := len(c.WarmUp.Rounds[0].Requests[0].Error); n != 64 {
		t.Errorf("timing error is %d bytes, want 64", n)
	}
}

// --cleanenv drops the job's CUDA_VISIBLE_DEVICES, so the agent forwards it as
// APPTAINERENV_CUDA_VISIBLE_DEVICES, unless the launch sets it (Opus nit).
func TestWorkerExecForwardsTheJobsGPUs(t *testing.T) {
	w := WorkerExec{Launch: testLaunch(), Key: testKey, BaseEnv: []string{"PATH=/usr/bin", "CUDA_VISIBLE_DEVICES=2"}}
	env, err := w.Env()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(env, "APPTAINERENV_CUDA_VISIBLE_DEVICES=2") {
		t.Fatalf("env %q does not forward the job's GPUs", env)
	}
	l := testLaunch()
	l.Env = append(l.Env, agentapi.EnvVar{Name: "CUDA_VISIBLE_DEVICES", Value: "0"})
	env, err = (WorkerExec{Launch: l, Key: testKey, BaseEnv: []string{"CUDA_VISIBLE_DEVICES=2"}}).Env()
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(env, "APPTAINERENV_CUDA_VISIBLE_DEVICES=2") || !slices.Contains(env, "APPTAINERENV_CUDA_VISIBLE_DEVICES=0") {
		t.Fatalf("env %q: the launch's own value must win", env)
	}
	if env, _ := (WorkerExec{Launch: testLaunch(), Key: testKey, BaseEnv: []string{"PATH=/usr/bin"}}).Env(); slices.ContainsFunc(env, func(s string) bool { return strings.Contains(s, "CUDA_VISIBLE_DEVICES") }) {
		t.Fatalf("env %q sets GPUs the job has not", env)
	}
}

// readLogTail keeps the last MaxLogTailBytes of the engine's log, starting at a
// rune boundary, with invalid bytes replaced and the Instance's key replaced
// (WorkerReport.LogTail). The log here is 9,000 bytes of "é" (two bytes each,
// so an odd cut splits one), an invalid byte, the key and a last line.
func TestReadLogTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "engine-worker-0.log")
	body := strings.Repeat("é", 4500) + "\xff key=" + testKey + "\nlast line\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	got := readLogTail(path, testKey)
	if len(got) > agentapi.MaxLogTailBytes {
		t.Errorf("tail is %d bytes, above %d", len(got), agentapi.MaxLogTailBytes)
	}
	if !utf8.ValidString(got) {
		t.Errorf("tail is not valid UTF-8")
	}
	if strings.Contains(got, testKey) {
		t.Errorf("tail carries the key")
	}
	if !strings.HasSuffix(got, "key="+agentapi.RedactedValue+"\nlast line\n") {
		t.Errorf("tail does not end with the redacted key and the last line: %q", got[max(0, len(got)-80):])
	}
	if !strings.HasPrefix(got, "é") {
		t.Errorf("tail does not start at a rune boundary: %q", got[:8])
	}
	if readLogTail(filepath.Join(t.TempDir(), "missing.log"), testKey) != "" {
		t.Errorf("a missing log gives a tail")
	}
}

// readGenerationConfig reads the staged checkpoint's generation_config.json:
// each top-level key with its value as compact JSON text (science-agent
// rulings, 1 Nov 2026, issue #125). The file is Qwen2.5's shape, with the
// values #94's run saw vLLM take (repetition_penalty 1.1, finding 6). An absent
// file or one that is not a JSON object gives none.
func TestReadGenerationConfig(t *testing.T) {
	dir := t.TempDir()
	write := func(body string) {
		if err := os.WriteFile(filepath.Join(dir, "generation_config.json"), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if got := readGenerationConfig(dir); got != nil {
		t.Errorf("no file: %v, want none", got)
	}
	write(`{"bos_token_id": 151643, "do_sample": true, "eos_token_id": [151645, 151643],
  "repetition_penalty": 1.1, "temperature": 0.7, "top_p": 0.8, "top_k": 20, "transformers_version": "4.37.0"}`)
	want := map[string]string{"bos_token_id": "151643", "do_sample": "true", "eos_token_id": "[151645,151643]",
		"repetition_penalty": "1.1", "temperature": "0.7", "top_p": "0.8", "top_k": "20", "transformers_version": `"4.37.0"`}
	if got := readGenerationConfig(dir); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v\nwant %v", got, want)
	}
	write(`[1, 2]`)
	if got := readGenerationConfig(dir); got != nil {
		t.Errorf("not an object: %v, want none", got)
	}
	// Bounds: 40 keys keep the first 32 in key order; a long value is cut.
	var b strings.Builder
	b.WriteString("{")
	for i := range 40 {
		fmt.Fprintf(&b, "%q: %d,", fmt.Sprintf("k%02d", i), i)
	}
	fmt.Fprintf(&b, "%q: %q}", "a_long", strings.Repeat("v", 500))
	write(b.String())
	got := readGenerationConfig(dir)
	if len(got) != agentapi.MaxGenerationConfigKeys || got["a_long"] == "" || len(got["a_long"]) > agentapi.MaxGenerationConfigValueBytes || got["k30"] != "30" || got["k31"] != "" {
		t.Errorf("bounds: %d keys, a_long %d bytes, k30 %q, k31 %q", len(got), len(got["a_long"]), got["k30"], got["k31"])
	}
}
