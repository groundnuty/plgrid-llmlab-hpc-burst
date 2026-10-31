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
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

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
	cfg.StagingDir = j.stagingDir
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
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			_ = cmd.Wait()
		}
		if t.Failed() {
			t.Logf("agent output:\n%s", out.String())
		}
	})
	return cmd, &out
}

func runState(engLog string, checks *agentapi.Checks) agentapi.DesiredState {
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
	cmd, _ := j.start(t)
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

// ssh mode: the agent keeps its own forward, ssh -N -L from a Unix socket in
// its run directory to the endpoint's loopback port, with the restricted key
// and pinned host key from the dial-home directory, and talks through it.
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
	if !strings.Contains(string(lines), fmt.Sprintf(":127.0.0.1:%d", p)) || !strings.Contains(string(lines), "BatchMode=yes") {
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
