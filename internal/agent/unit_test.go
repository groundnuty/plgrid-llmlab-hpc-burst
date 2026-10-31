package agent

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

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
	w := WorkerExec{Image: "/s/images/x.sif", CheckpointDir: "/s/checkpoints/org/model/rev", RunDir: "/tmp/hb-1", Launch: testLaunch(), Key: testKey}
	want := []string{
		"exec", "--nv", "--cleanenv",
		"--bind", "/s/checkpoints/org/model/rev:/hpcburst/model:ro",
		"--bind", "/tmp/hb-1:/hpcburst/run",
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
	want := []string{"PATH=/usr/bin", "HOME=/home/u", "APPTAINERENV_HF_HUB_OFFLINE=1", "APPTAINERENV_VLLM_API_KEY=" + testKey}
	if !reflect.DeepEqual(env, want) {
		t.Fatalf("env:\n got %q\nwant %q", env, want)
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
	c.SSH = &agentapi.SSHForward{User: "hb", AgentPort: 29223}
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
