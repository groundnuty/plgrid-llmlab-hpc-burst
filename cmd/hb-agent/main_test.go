package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agentserver"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// roleAgent makes this test binary run the agent's main, as the bootstrap
// runs hb-agent: the same code path as the built binary, in its own process.
const roleAgent = "hb-agent-under-test"

func TestMain(m *testing.M) {
	// The fakes first: the agent's children (apptainer, ssh) inherit its role
	// variable but are told apart by their program name.
	enginedouble.Dispatch()
	// No process a test starts inherits a descriptor this binary got, such
	// as a build lock's (an orphan would hold it).
	if err := agent.CloseInheritedFDs(); err != nil {
		panic(err)
	}
	if os.Getenv("HB_TEST_ROLE") == roleAgent {
		os.Exit(run(os.Args[1:], os.Getenv, os.Stderr))
	}
	os.Exit(m.Run())
}

// The test's Allocation.
const (
	eTag   = "hb-example-abcdef012345"
	eUID   = "99999999-8888-7777-6666-555555555555"
	eJobID = "777"
	eInst  = "aaaaaaaa-1111-2222-3333-444444444444"
)

// Ports 29290-29299 are #60's block for the agent's tests; this package takes
// the upper half, the agent's own tests the rest.
const portLo, portHi = 29295, 29299

func listenInBlock(t *testing.T) net.Listener {
	t.Helper()
	for p := portLo; p <= portHi; p++ {
		ln, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(p))
		if err == nil {
			return ln
		}
		if !errors.Is(err, syscall.EADDRINUSE) {
			t.Fatal(err)
		}
	}
	t.Fatalf("no free port in %d-%d", portLo, portHi)
	return nil
}

type mapLookup map[string]agentserver.Allocation

func (m mapLookup) ByTag(_ context.Context, tag string) (agentserver.Allocation, error) {
	a, ok := m[tag]
	if !ok {
		return agentserver.Allocation{}, agentserver.ErrNotFound
	}
	return a, nil
}

// endpoint is the controller's side: the real agent API server with the
// launcher's certificate, over a fake Backend.
type endpoint struct {
	addr    string
	pin     string
	backend *fake.Backend
}

func startEndpoint(t *testing.T, root string) endpoint {
	t.Helper()
	host := agentapi.Host{StateDir: filepath.Join(root, "state"), PortBase: 29220}
	pin := writeCert(t, host)
	credDir := host.CredentialsDir()
	if err := agentapi.WriteKeyFile(agentapi.AllocationCredentialPath(credDir, eUID), testCredential); err != nil {
		t.Fatal(err)
	}
	b := fake.NewBackend()
	srv, err := agentserver.New(agentserver.Config{
		Lookup:         mapLookup{eTag: {Namespace: "ns", Name: "alloc", UID: eUID, JobID: eJobID}},
		CredentialsDir: credDir,
		Backend:        b,
		Host:           host,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		PollInterval:   20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ln := listenInBlock(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return endpoint{addr: ln.Addr().String(), pin: pin, backend: b}
}

const testCredential = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

// writeCert writes a self-signed ECDSA P-256 certificate and key where the
// launcher puts the agent endpoint's, and returns its pin.
func writeCert(t *testing.T, host agentapi.Host) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "agent-endpoint"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(host.AgentCertFile()), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(host.AgentCertFile(), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(host.AgentKeyFile(), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600); err != nil {
		t.Fatal(err)
	}
	return agentapi.PinOf(cert)
}

// job is the compute node's side: the Allocation's directory, the staging
// directory, and the fake tools.
type job struct {
	root, allocDir, stagingDir, bin, appLog, sshLog, engLog string
	extraEnv                                                []string
	ssh                                                     bool // agent.json's mode is ssh
}

func newJob(t *testing.T, root string, cfg agentapi.AgentConfig) job {
	t.Helper()
	j := job{
		root:       root,
		allocDir:   filepath.Join(root, "alloc", eTag),
		stagingDir: filepath.Join(root, "staging"),
		bin:        filepath.Join(root, "bin"),
		appLog:     filepath.Join(root, "apptainer.jsonl"),
		sshLog:     filepath.Join(root, "ssh.jsonl"),
		engLog:     filepath.Join(root, "engine.jsonl"),
	}
	for _, d := range []string{j.allocDir, j.stagingDir, j.bin, filepath.Join(root, "tmp")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := enginedouble.Install(j.bin); err != nil {
		t.Fatal(err)
	}
	j.ssh = cfg.Mode == agentapi.DialHomeSSH
	cfg.StagingDir = j.stagingDir
	ckpt := filepath.Join(j.stagingDir, agentapi.StagedCheckpointDir("org/model", strings.Repeat("e", 40)))
	if err := os.MkdirAll(ckpt, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ckpt, preStaged.Path), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.DialHomeDir = filepath.Join(root, "dial-home")
	b, err := cfg.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(j.allocDir, agentapi.ConfigFileName), b, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := agentapi.WriteKeyFile(filepath.Join(j.allocDir, "credential"), testCredential); err != nil {
		t.Fatal(err)
	}
	return j
}

// start runs the agent in its own process, as the job's bootstrap does.
func (j job) start(t *testing.T) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(self, j.allocDir)
	cmd.Env = append(os.Environ(),
		"HB_TEST_ROLE="+roleAgent,
		"SLURM_JOB_ID="+eJobID,
		"SLURM_RESTART_COUNT=0",
		"TMPDIR="+filepath.Join(j.root, "tmp"),
		"HPCBURST_APPTAINER="+filepath.Join(j.bin, "apptainer"),
		"HPCBURST_SSH="+filepath.Join(j.bin, "ssh"),
		"HPCBURST_NVIDIA_SMI="+filepath.Join(j.bin, "no-nvidia-smi"),
		"HB_FAKE_APPTAINER_LOG="+j.appLog,
		"HB_FAKE_SSH_LOG="+j.sshLog,
		"GORACE=atexit_sleep_ms=0", // a race-built fake must not sleep at its exit
	)
	cmd.Env = append(cmd.Env, j.extraEnv...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			done := make(chan struct{})
			go func() { _ = cmd.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
			}
		}
		// Whatever the outcome, nothing the job started may outlive the test:
		// ssh, Apptainer and the engine all name the job's root in their
		// arguments. A failed or timed-out run is cleaned up here too.
		if left := killUnder(j.root); len(left) > 0 && !t.Failed() {
			t.Errorf("processes left behind after the test: %q", left)
		}
		if t.Failed() {
			t.Logf("agent output:\n%s", out.String())
		}
	})
	return cmd, &out
}

// killUnder kills every process, other than this one, whose command line
// names root, waits until each is gone, and returns their command lines.
func killUnder(root string) []string {
	var left []string
	entries, _ := os.ReadDir("/proc")
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == os.Getpid() {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/cmdline")
		if err != nil || !strings.Contains(string(b), root) {
			continue
		}
		st, err := procStart(pid)
		if err != nil {
			continue
		}
		id := agent.ProcessID{PID: pid, StartTime: st}
		if !id.Alive() {
			continue
		}
		left = append(left, strings.ReplaceAll(string(b), "\x00", " "))
		_ = syscall.Kill(pid, syscall.SIGKILL)
		deadline := time.Now().Add(5 * time.Second)
		for id.Alive() && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
	}
	return left
}

// procStart reads a process's start time, the 22nd field of /proc/<pid>/stat.
func procStart(pid int) (uint64, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	f := strings.Fields(s[i+1:])
	if i < 0 || len(f) < 20 {
		return 0, fmt.Errorf("short stat")
	}
	return strconv.ParseUint(f[19], 10, 64)
}

// sshChildren returns the ssh processes the agent logged starting, by PID and
// start time.
func sshChildren(t *testing.T, out string) []agent.ProcessID {
	t.Helper()
	var ids []agent.ProcessID
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, `msg="ssh forward started"`) {
			continue
		}
		var id agent.ProcessID
		for _, f := range strings.Fields(line) {
			if v, ok := strings.CutPrefix(f, "pid="); ok {
				id.PID, _ = strconv.Atoi(v)
			}
			if v, ok := strings.CutPrefix(f, "startTime="); ok {
				id.StartTime, _ = strconv.ParseUint(v, 10, 64)
			}
		}
		if id.PID == 0 || id.StartTime == 0 {
			t.Fatalf("an ssh start line without its PID and start time: %s", line)
		}
		ids = append(ids, id)
	}
	return ids
}

// assertSSHReaped checks that no ssh process the agent started outlives it,
// found by the recorded PID and start time, never by name.
func assertSSHReaped(t *testing.T, out string, wantAtLeast int) {
	t.Helper()
	ids := sshChildren(t, out)
	if len(ids) < wantAtLeast {
		t.Fatalf("the agent logged %d ssh starts, want at least %d", len(ids), wantAtLeast)
	}
	for _, id := range ids {
		if id.Alive() {
			t.Errorf("ssh %+v, started by the agent, still runs after the agent exited", id)
		}
	}
}

// The identity record's one file, which newJob pre-stages, so the job needs
// no Hub. printf '{}\n' | sha256sum
var preStaged = agentapi.FileDigest{Path: "config.json", SHA256: "ca3d163bab055381827226140568f3bef7eaac187cebd76878e0b63e9e442356"}

// runState runs the test's Instance. Every start instruction carries the
// file list (verification with Files), which the agent needs to start; checks
// that ask for more add it themselves.
func runState(engLog string, checks *agentapi.Checks) agentapi.DesiredState {
	if checks == nil {
		checks = &agentapi.Checks{VerificationGeneration: 1}
	}
	if checks.Verify == nil {
		checks.Verify = &agentapi.VerifyRequest{}
	}
	if len(checks.Verify.Files) == 0 {
		checks.Verify.Files = []agentapi.FileDigest{preStaged}
	}
	return agentapi.DesiredState{
		Action: agentapi.ActionRun,
		Instance: &agentapi.DesiredInstance{
			Name: "inst", UID: eInst, DeploymentID: strings.Repeat("d", 64),
			Launch: agentapi.Launch{
				Image:       "docker.io/vllm/vllm-openai",
				ImageDigest: "sha256:" + strings.Repeat("2", 64),
				Command: []string{"vllm", "serve", agentapi.ContainerModelDir,
					"--uds=" + agentapi.ContainerEngineSocket("worker-0"),
					"--served-model-name=example", "--max-model-len=4096", "--max-num-seqs=8"},
				Env:        []agentapi.EnvVar{{Name: "HB_ENGINE_DOUBLE_LOG", Value: engLog}},
				KeyEnv:     "VLLM_API_KEY",
				Checkpoint: agentapi.Checkpoint{Repository: "org/model", Revision: strings.Repeat("e", 40)},
				GPUs:       1,
				MaxNumSeqs: 8,
			},
			Generation: 1,
			Workers:    []agentapi.DesiredWorker{{Name: "worker-0", EngineSocket: agentapi.ContainerEngineSocket("worker-0")}},
			Checks:     checks,
		},
		Settings: agentapi.AgentSettings{
			ProbeInterval: agentapi.Duration(200 * time.Millisecond),
			StopGrace:     agentapi.Duration(2 * time.Second),
		},
	}
}

func waitReport(t *testing.T, b *fake.Backend, what string, ok func(agentapi.Report) bool) agentapi.Report {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		if r, found := b.LastReport(eUID); found && ok(r) {
			return r
		}
		if time.Now().After(deadline) {
			r, _ := b.LastReport(eUID)
			js, _ := json.MarshalIndent(r.Redacted(), "", "  ")
			t.Fatalf("timed out waiting for %s; last report:\n%s", what, js)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// exercise drives one agent through a run, verification and a stop, through
// the real endpoint over TLS with the pinned certificate.
func exercise(t *testing.T, ep endpoint, j job) {
	t.Helper()
	cmd, out := j.start(t)
	ep.backend.SetDesired(eUID, runState(j.engLog, &agentapi.Checks{VerificationGeneration: 1, Verify: &agentapi.VerifyRequest{}}))
	r := waitReport(t, ep.backend, "verification with the worker running", func(r agentapi.Report) bool {
		in := r.Instance
		return in != nil && len(in.Workers) == 1 && in.Workers[0].Phase == agentapi.PartRunning &&
			in.Checks != nil && in.Checks.Verify != nil && in.Checks.Verify.Done
	})
	if v := r.Instance.Checks.Verify; v.EngineVersion != "0.32.0" || v.Settings["max-model-len"] != "4096" {
		t.Errorf("verification read %+v", v)
	}
	regs := ep.backend.Registrations(eUID)
	if len(regs) != 1 {
		t.Fatalf("registrations %+v", regs)
	}
	reg := regs[0]
	if err := agentapi.CheckAgentInstanceID(reg.Caller.AgentInstanceID); err != nil {
		t.Errorf("agent instance ID: %v", err)
	}
	arch, _ := os.ReadFile("/proc/sys/kernel/arch")
	if reg.Request.CPUArch != strings.TrimSpace(string(arch)) || reg.Caller.JobID != eJobID || reg.Caller.Tag != eTag {
		t.Errorf("registration %+v", reg)
	}

	ep.backend.SetDesired(eUID, agentapi.DesiredState{Action: agentapi.ActionStop, Settings: agentapi.AgentSettings{StopGrace: agentapi.Duration(2 * time.Second)}})
	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()
	select {
	case err := <-waitErr:
		if err != nil {
			t.Fatalf("the agent exited with %v after a stop, want 0", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the agent did not exit after a stop")
	}
	last, _ := ep.backend.LastReport(eUID)
	if w := last.Instance.Workers[0]; w.Phase != agentapi.PartStopped {
		t.Errorf("last report's worker %+v, want stopped", w)
	}
	if j.ssh {
		assertSSHReaped(t, out.String(), 1)
	}
	// Neither the credential nor the Instance's key reaches the agent's log.
	for name, secret := range map[string]string{"the credential": testCredential, "the Instance key": r.Instance.Key} {
		if secret == "" || strings.Contains(out.String(), secret) {
			t.Errorf("%s is in the agent's output (or empty: %q)", name, secret)
		}
	}
}

func shortRoot(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("", "hbe")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	return root
}

// Direct mode: the agent reaches the endpoint at agent.json's address, and
// trusts exactly the pinned certificate.
func TestAgentEndToEndDirect(t *testing.T) {
	root := shortRoot(t)
	ep := startEndpoint(t, root)
	j := newJob(t, root, agentapi.AgentConfig{
		ProtocolVersion: agentapi.ProtocolVersion, Tag: eTag, Mode: agentapi.DialHomeDirect,
		Endpoint: ep.addr, CertSPKISHA256: ep.pin,
	})
	exercise(t, ep, j)
}

// ssh mode: the agent keeps its own forward, ssh -N -L from a kernel-chosen
// port on the node's loopback address to the endpoint's loopback port, with
// the restricted key and pinned host key from the dial-home directory, and
// talks through it (HBEP-6, Dial-home modes).
func TestAgentEndToEndSSH(t *testing.T) {
	root := shortRoot(t)
	ep := startEndpoint(t, root)
	_, port, _ := net.SplitHostPort(ep.addr)
	p, _ := strconv.Atoi(port)
	dh := filepath.Join(root, "dial-home")
	if err := os.MkdirAll(dh, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{agentapi.DialHomeKeyFile, agentapi.DialHomeKnownHostsFile} {
		if err := os.WriteFile(filepath.Join(dh, f), []byte("test\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	j := newJob(t, root, agentapi.AgentConfig{
		ProtocolVersion: agentapi.ProtocolVersion, Tag: eTag, Mode: agentapi.DialHomeSSH,
		Endpoint: "127.0.0.1:22", SSH: &agentapi.SSHForward{User: "hb", AgentPort: p},
		CertSPKISHA256: ep.pin,
	})
	exercise(t, ep, j)
	lines, err := os.ReadFile(j.sshLog)
	if err != nil {
		t.Fatalf("the fake ssh recorded nothing: %v", err)
	}
	if !regexp.MustCompile(fmt.Sprintf(`"127\.0\.0\.1:[0-9]+:127\.0\.0\.1:%d"`, p)).Match(lines) || !strings.Contains(string(lines), "BatchMode=yes") {
		t.Fatalf("ssh invocation %s", lines)
	}
}

// A wrong pin: the agent refuses the endpoint, never registers, and runs
// nothing.
func TestAgentRefusesAnEndpointWithAnotherPin(t *testing.T) {
	root := shortRoot(t)
	ep := startEndpoint(t, root)
	j := newJob(t, root, agentapi.AgentConfig{
		ProtocolVersion: agentapi.ProtocolVersion, Tag: eTag, Mode: agentapi.DialHomeDirect,
		Endpoint: ep.addr, CertSPKISHA256: strings.Repeat("0", 64),
	})
	cmd, out := j.start(t)
	ep.backend.SetDesired(eUID, runState(j.engLog, nil))
	time.Sleep(1500 * time.Millisecond)
	if n := ep.backend.Calls(fake.MethodRegister); n != 0 {
		t.Fatalf("the agent registered %d times with an endpoint whose pin is not agent.json's", n)
	}
	if _, err := os.Stat(j.appLog); err == nil {
		t.Fatal("the agent ran Apptainer without a trusted endpoint")
	}
	_ = syscall.Kill(cmd.Process.Pid, syscall.SIGTERM)
	_ = cmd.Wait()
	if !strings.Contains(out.String(), "pin") {
		t.Errorf("the agent's log does not say why: %s", out.String())
	}
}

// -version prints the version and the protocol; no argument is a usage error.
func TestUsage(t *testing.T) {
	var b bytes.Buffer
	if code := run([]string{"-version"}, os.Getenv, &b); code != 0 || !strings.Contains(b.String(), agentapi.ProtocolVersion) {
		t.Fatalf("-version: %d %q", code, b.String())
	}
	if code := run(nil, os.Getenv, io.Discard); code != 2 {
		t.Fatalf("no argument: exit %d, want 2", code)
	}
}

// The ssh forward is kept in a reconnect loop: the fake ssh drops it every
// three seconds, the agent starts ssh again, and its reports keep arriving with the
// same worker process.
func TestAgentSSHForwardReconnects(t *testing.T) {
	root := shortRoot(t)
	ep := startEndpoint(t, root)
	_, port, _ := net.SplitHostPort(ep.addr)
	p, _ := strconv.Atoi(port)
	dh := filepath.Join(root, "dial-home")
	if err := os.MkdirAll(dh, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{agentapi.DialHomeKeyFile, agentapi.DialHomeKnownHostsFile} {
		if err := os.WriteFile(filepath.Join(dh, f), []byte("test\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	j := newJob(t, root, agentapi.AgentConfig{
		ProtocolVersion: agentapi.ProtocolVersion, Tag: eTag, Mode: agentapi.DialHomeSSH,
		Endpoint: "127.0.0.1:22", SSH: &agentapi.SSHForward{User: "hb", AgentPort: p},
		CertSPKISHA256: ep.pin,
	})
	j.extraEnv = []string{"HB_FAKE_SSH_DIE_AFTER=3s"}
	cmd, out := j.start(t)
	ep.backend.SetDesired(eUID, runState(j.engLog, nil))
	first := waitReport(t, ep.backend, "worker running", func(r agentapi.Report) bool {
		return r.Instance != nil && len(r.Instance.Workers) == 1 && r.Instance.Workers[0].Phase == agentapi.PartRunning
	})
	pid := first.Instance.Workers[0].PID
	deadline := time.Now().Add(30 * time.Second)
	for countLines(j.sshLog) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("ssh started %d times in 30 s; the forward was not restarted", countLines(j.sshLog))
		}
		time.Sleep(50 * time.Millisecond)
	}
	before := len(ep.backend.Reports(eUID))
	waitReport(t, ep.backend, "a report after the reconnects", func(agentapi.Report) bool {
		return len(ep.backend.Reports(eUID)) > before
	})
	last, _ := ep.backend.LastReport(eUID)
	if w := last.Instance.Workers[0]; w.PID != pid || w.Phase != agentapi.PartRunning {
		t.Fatalf("after the reconnects the worker is %+v, want the same running process %d", w, pid)
	}
	execs := 0
	for _, line := range strings.Split(readFile(j.appLog), "\n") {
		if strings.Contains(line, `"argv":["exec"`) {
			execs++
		}
	}
	if execs != 1 {
		t.Fatalf("%d worker starts across the reconnects, want 1", execs)
	}
	// A job end: SIGTERM to the agent, which stops and reaps its ssh.
	_ = syscall.Kill(cmd.Process.Pid, syscall.SIGTERM)
	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()
	select {
	case <-waitErr:
	case <-time.After(30 * time.Second):
		t.Fatal("the agent did not exit on SIGTERM")
	}
	assertSSHReaped(t, out.String(), 2)
}

func readFile(path string) string {
	b, _ := os.ReadFile(path)
	return string(b)
}

func countLines(path string) int {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	return strings.Count(string(b), "\n")
}
