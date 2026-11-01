package main

// The binary's run function against a real API server with the committed CRDs:
// both per-Provider manager supervisors, each with Providers reconciled
// through it (the batch ones over the in-process fake Slurm, on an x86_64 and
// a GH200 (aarch64) partition; the managed one against the LLMLab double), the
// agent API on the agent endpoint with the endpoint certificate, answering
// through the lifecycle controller's backend, an Allocation event written by
// the Provider controller's recorder, a clean stop while a submission hangs,
// and the refusals, before anything starts, of a Provider-manager scheme
// without apps/v1 and of front settings agent.json would refuse. Everything
// but the platform's transport and those inputs is the path main takes.
//
// The API server is controller-runtime's envtest (etcd and kube-apiserver, no
// controller manager), because every block of internal/testenv's port range
// is taken (IMPL-COMMON, Envtest port blocks) and nothing here needs garbage
// collection. Its binaries are internal/testenv's pinned ones, and its ports,
// like every other port of this test, are chosen by the kernel. On a machine
// where other agents test, run it under the shared envtest lock.

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"github.com/go-logr/logr/funcr"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// logs holds what the binary logged through controller-runtime's logger,
// which main sets and run uses.
var logs = &syncBuffer{}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) line(s string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.WriteString(s)
	b.buf.WriteByte('\n')
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestMain(m *testing.M) {
	ctrl.SetLogger(funcr.New(func(prefix, args string) { logs.line(prefix + " " + args) }, funcr.Options{}))
	os.Exit(m.Run())
}

// apiServer is a running envtest control plane with the committed CRDs.
type apiServer struct {
	env        *envtest.Environment
	client     client.Client
	kubeconfig []byte
}

// startAPIServer starts etcd and kube-apiserver on ports the kernel chose,
// with internal/testenv's sha256-checked binaries, installs config/crd, and
// stops them at the end of the test, checking that no process is left and
// that the root k3s's ports still listen.
func startAPIServer(t *testing.T) *apiServer {
	t.Helper()
	guard := testenv.GuardSystemPorts()
	bins, err := testenv.FindBinaries()
	if err != nil {
		t.Fatalf("the control-plane binaries: %v", err)
	}
	dir := t.TempDir()
	etcdDir, certDir := filepath.Join(dir, "etcd"), filepath.Join(dir, "certs")
	for _, d := range []string{etcdDir, certDir} {
		if err := os.Mkdir(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	env := &envtest.Environment{
		BinaryAssetsDirectory: filepath.Dir(bins.KubeAPIServer),
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd")},
		ErrorIfCRDPathMissing: true,
		ControlPlane: envtest.ControlPlane{
			Etcd:      &envtest.Etcd{DataDir: etcdDir},
			APIServer: &envtest.APIServer{CertDir: certDir},
		},
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start the API server: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Errorf("stop the API server: %v", err)
		}
		for _, d := range []string{etcdDir, certDir} {
			if pids := processesNaming(d); len(pids) > 0 {
				t.Errorf("processes %v still run with %s on their command line", pids, d)
			}
		}
		if err := guard(); err != nil {
			t.Errorf("system port check: %v", err)
		}
	})
	if len(env.CRDs) != 5 {
		t.Fatalf("installed %d CRDs, want the 5 of config/crd", len(env.CRDs))
	}
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, v1alpha1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	c, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	return &apiServer{env: env, client: c, kubeconfig: env.KubeConfig}
}

// processesNaming returns the PIDs of the processes whose command line
// contains s.
func processesNaming(s string) []int {
	entries, _ := os.ReadDir("/proc")
	var pids []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == os.Getpid() {
			continue
		}
		b, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err == nil && bytes.Contains(b, []byte(s)) {
			pids = append(pids, pid)
		}
	}
	return pids
}

// freePortBase returns a port base whose ports the binary binds are free: the
// agent endpoint (+3), frps's plugin (+8) and the metrics
// (agentapi.ControllerMetricsPortOffset, +12, which #111 binds). The kernel
// chooses the agent endpoint's port; the others are checked, and all are
// released just before the binary binds them.
//
// The binary binds these ports, the health address and the LLMLab double's
// port (freePort) from an address, not from a listener a test could hand
// over, so another process can take one between its release here and the
// bind (review of PR 132: N5). The risk is accepted: the bind then fails and
// run returns an error, a failed test rather than a passing one, and a
// refusal's check that nothing listens fails if a stranger listens there.
func freePortBase(t *testing.T) int {
	t.Helper()
	for range 100 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		base := ln.Addr().(*net.TCPAddr).Port - agentapi.AgentEndpointPortOffset
		held := []net.Listener{ln}
		ok := base >= 1024 && base+agentapi.ControllerMetricsPortOffset <= 65535
		for _, off := range []int{agentapi.FrpsPluginPortOffset, agentapi.ControllerMetricsPortOffset} {
			if !ok {
				break
			}
			l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(base+off)))
			if err != nil {
				ok = false
				break
			}
			held = append(held, l)
		}
		for _, l := range held {
			_ = l.Close()
		}
		if ok {
			return base
		}
	}
	t.Fatal("found no port base with free agent, plugin and metrics ports in 100 tries")
	return 0
}

// freePort returns a loopback port the kernel chose, released (see
// freePortBase for the residual risk).
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func listening(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

// writeEndpointCert writes a self-signed agent endpoint certificate and key
// where the launcher puts them (agentapi.Host) and returns the certificate.
func writeEndpointCert(t *testing.T, host agentapi.Host) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: agentapi.DialHomeServerName},
		DNSNames: []string{agentapi.DialHomeServerName}, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kder, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(host.AgentCertFile()), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(host.AgentCertFile(), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(host.AgentKeyFile(), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kder}), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// binary is one run of the binary's run function, as main calls it. exited is
// closed once run has returned, with err set, so any number of waits see the
// return (review of PR 132: B4).
type binary struct {
	f      flags
	cancel context.CancelFunc
	exited chan struct{}
	err    error
}

func startBinary(t *testing.T, f flags, d deps) *binary {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	b := &binary{f: f, cancel: cancel, exited: make(chan struct{})}
	go func() {
		b.err = run(ctx, f, d)
		close(b.exited)
	}()
	t.Cleanup(func() { b.stop(t) })
	return b
}

// eventually waits until cond holds, for at most timeout. A run that returns
// meanwhile fails the wait at once, with run's error, rather than as a timeout
// of whatever the test waited for.
func (b *binary) eventually(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		select {
		case <-b.exited:
			t.Fatalf("run returned %v while the test waited until %s", b.err, what)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting until %s", timeout, what)
		}
		select {
		case <-b.exited:
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// refusal waits for a run that must refuse to start and returns its error. A
// run still going after 30 s started instead, which fails the test, and
// nothing the run binds may listen after a refusal.
func (b *binary) refusal(t *testing.T) error {
	t.Helper()
	select {
	case <-b.exited:
	case <-time.After(30 * time.Second):
		t.Fatal("run did not refuse within 30 s: it started instead")
	}
	for _, addr := range []string{b.f.host.AgentEndpointAddr(), b.f.healthAddr} {
		if listening(addr) {
			t.Errorf("%s listens after the refusal; nothing should have started", addr)
		}
	}
	return b.err
}

// stop cancels the run, as SIGTERM does, and returns its error; a run that
// does not return within a minute fails the test.
func (b *binary) stop(t *testing.T) error {
	t.Helper()
	if b.cancel != nil {
		b.cancel()
		b.cancel = nil
	}
	select {
	case <-b.exited:
		return b.err
	case <-time.After(time.Minute):
		t.Fatal("run did not return within a minute of its context's end")
		return nil
	}
}

// exampleFront is the direct-mode front address the tests record.
const exampleFront = "front.example.org:29443"

// prepareState prepares a state directory as hpcburst up leaves it: the
// kubeconfig, the private credentials directory, an x86_64 agent build, whose
// digest it returns, and the endpoint certificate; the recorded front is mode
// at addr.
func prepareState(t *testing.T, api *apiServer, mode, addr string) (flags, *x509.Certificate, string) {
	t.Helper()
	state := t.TempDir()
	h := agentapi.Host{StateDir: state, PortBase: freePortBase(t)}
	if err := os.WriteFile(filepath.Join(state, "kubeconfig"), api.kubeconfig, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{h.CredentialsDir(), h.AgentBuildsDir()} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	digest := writeAgentBuild(t, h, "x86_64")
	cert := writeEndpointCert(t, h)
	args := []string{"--state-dir", state, "--port-base", strconv.Itoa(h.PortBase),
		"--health-addr", net.JoinHostPort("127.0.0.1", strconv.Itoa(freePort(t))),
		"--front-mode", mode, "--front-addr", addr}
	f, err := parse(args, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	return f, cert, digest
}

// writeAgentBuild writes an example agent build for cpuArch where the
// launcher puts the builds, and returns its digest.
func writeAgentBuild(t *testing.T, h agentapi.Host, cpuArch string) string {
	t.Helper()
	agent := []byte("#!/bin/sh\n# an example agent build for " + cpuArch + "\n")
	if err := os.WriteFile(filepath.Join(h.AgentBuildsDir(), agentapi.AgentBuildName(cpuArch)), agent, 0o700); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(agent)
	return hex.EncodeToString(sum[:])
}

// The batch partitions of the example profile: an x86_64 one and a GH200
// (aarch64) one, as on Helios.
const (
	partX86   = "example-gpu"
	partGH200 = "example-gh200"
)

func batchProfile(name string) *v1alpha1.PlatformProfile {
	gpus := int32(4)
	maxTime, window := v1alpha1.Duration("48h"), v1alpha1.Duration("5m")
	return &v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: v1alpha1.PlatformProfileSpec{
		ProviderKind: v1alpha1.PlatformBatch, HoldRelease: true, JSONDataParser: "v0.0.42", InflightWindow: &window,
		StagingDir: "$SCRATCH/stage",
		Partitions: []v1alpha1.PartitionProfile{
			{Name: partX86, AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "A100", CPUArch: "x86_64"}, GPUsPerNode: &gpus, MaxTime: &maxTime},
			{Name: partGH200, AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "GH200", CPUArch: "aarch64"}, GPUsPerNode: &gpus, MaxTime: &maxTime},
		},
	}}
}

// newFakeSlurm is the in-process Slurm with both partitions, and the login
// session the batch platforms use.
func newFakeSlurm() (*fakeslurm.Platform, *fakeslurm.Session) {
	sl := fakeslurm.New(slurm.RealClock, fakeslurm.Config{Partitions: []fakeslurm.Partition{{Name: partX86, GPUOnly: true}, {Name: partGH200, GPUOnly: true}}})
	return sl, sl.Login(fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": "/home/example-user", "SCRATCH": "/scratch/example-user"}})
}

// sshCredentials writes a batch Provider's ssh credentials (mode 600), as
// setup_test.go in internal/providerctl/e2e builds them, and returns the
// file's path.
func sshCredentials(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	key, creds := filepath.Join(dir, "key"), filepath.Join(dir, "ssh")
	for path, content := range map[string]string{
		key:   "example key\n",
		creds: "HostName login.example.org\nUser example-user\nIdentityFile " + key + "\n",
	} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return creds
}

// batchProvider is a batch Provider on one partition of the profile (alpha
// allows exactly one).
func batchProvider(name, profile, partition, creds string) *v1alpha1.Provider {
	return &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: v1alpha1.ProviderSpec{
		PlatformProfileRef: v1alpha1.ObjectReference{Name: profile}, Account: "example-account",
		AllowedPartitions: []string{partition}, CredentialsRef: &v1alpha1.CredentialsReference{File: creds},
	}}
}

func batchAllocation(name, namespace, provider, partition, agentDigest string) *v1alpha1.Allocation {
	return &v1alpha1.Allocation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1alpha1.AllocationSpec{
			ProviderConfigReference: &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: provider},
			ForProvider: v1alpha1.AllocationParameters{Batch: &v1alpha1.BatchParameters{
				Partition: partition, TimeLimit: "30m", GPUs: 1, CPUsPerTask: 8, Memory: resource.MustParse("16Gi"), AgentDigest: agentDigest,
			}},
		},
	}
}

// writeCredential writes the Allocation's credential, as the lifecycle
// controller does once the UID exists, and returns it.
func writeCredential(t *testing.T, h agentapi.Host, a *v1alpha1.Allocation) string {
	t.Helper()
	cred, err := agentapi.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := agentapi.WriteKeyFile(agentapi.AllocationCredentialPath(h.CredentialsDir(), string(a.UID)), cred); err != nil {
		t.Fatal(err)
	}
	return cred
}

// held reports whether the Lease in the controller namespace has a holder.
func held(ctx context.Context, c client.Client, lease string) bool {
	var l coordinationv1.Lease
	err := c.Get(ctx, client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: lease}, &l)
	return err == nil && l.Spec.HolderIdentity != nil && *l.Spec.HolderIdentity != ""
}

// stagedConfig decodes the agent.json the Provider staged for the Allocation
// on the fake Slurm.
func stagedConfig(t *testing.T, sl *fakeslurm.Platform, a *v1alpha1.Allocation) agentapi.AgentConfig {
	t.Helper()
	suffix := "/" + agentapi.StagedConfigPath(v1alpha1.JobTag(a.Namespace, a.Name, a.UID))
	var found []string
	var cfg agentapi.AgentConfig
	for name, file := range sl.Files() {
		if !strings.HasSuffix(name, suffix) {
			continue
		}
		found = append(found, name)
		c, err := agentapi.DecodeAgentConfig(file.Content)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		cfg = c
	}
	if len(found) != 1 {
		t.Fatalf("staged files ending with %s: %v, want one", suffix, found)
	}
	return cfg
}

func TestControllerBinary(t *testing.T) {
	api := startAPIServer(t)
	ctx := context.Background()
	if err := api.client.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.ControllerNamespace}}); err != nil {
		t.Fatal(err)
	}

	t.Run("refuses a Provider-manager scheme without apps/v1", func(t *testing.T) {
		f, _, _ := prepareState(t, api, "direct", exampleFront)
		noApps := func() (*runtime.Scheme, error) {
			s := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, coordinationv1.AddToScheme, v1alpha1.AddToScheme} {
				if err := add(s); err != nil {
					return nil, err
				}
			}
			return s, nil
		}
		b := startBinary(t, f, deps{providerScheme: noApps})
		if err := b.refusal(t); err == nil || !strings.Contains(err.Error(), "apps/v1") || !strings.Contains(err.Error(), "ControllerRevision") {
			t.Fatalf("run returned %v, want a refusal naming apps/v1's ControllerRevision", err)
		}
	})

	// Front settings that agent.json would refuse stop the binary before
	// anything listens, with the field named (review of PR 132: B3). Without
	// the check the binary started and failed every submission.
	for _, tc := range []struct {
		name, mode, addr string
		d                deps
		want             string
	}{
		{"refuses a front address without a usable port", "direct", "front.example.org:0", deps{}, `endpoint "front.example.org:0"`},
		{"refuses a malformed frpc digest", "direct", exampleFront, deps{frpcDigests: func() (map[string]string, error) {
			return map[string]string{"x86_64": strings.Repeat("A", 64)}, nil
		}}, "cpuArch x86_64: frpcDigest"},
		{"refuses an ssh user agentapi refuses", "ssh", "cp.example.org:22", deps{username: func() (string, error) {
			return "-oProxyCommand", nil
		}}, `ssh.user "-oProxyCommand"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _, _ := prepareState(t, api, tc.mode, tc.addr)
			d := tc.d
			d.providerScheme = providerScheme
			b := startBinary(t, f, d)
			if err := b.refusal(t); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("run returned %v, want a refusal naming %q", err, tc.want)
			}
		})
	}

	t.Run("runs both supervisors and the agent API", func(t *testing.T) {
		testBinaryComponents(t, api)
	})
}

func testBinaryComponents(t *testing.T, api *apiServer) {
	ctx := context.Background()
	c := api.client
	from := len(logs.String()) // this run's log lines only, under -count=N
	f, cert, digestX86 := prepareState(t, api, "direct", exampleFront)
	digestGH200 := writeAgentBuild(t, f.host, "aarch64")

	// The batch Providers, one per partition, and the managed one, against the
	// LLMLab double on a port the kernel chose.
	sshCreds := sshCredentials(t)
	profile := batchProfile("wiring-slurm")
	x86Prov := batchProvider("wiring-batch", profile.Name, partX86, sshCreds)
	gh200Prov := batchProvider("wiring-gh200", profile.Name, partGH200, sshCreds)
	gwPort := freePort(t)
	gwKey := "example-key-wiring-0001"
	gw := llmlabtest.Start(t, llmlabtest.Options{PortLo: gwPort, PortHi: gwPort, Keys: []string{gwKey}})
	gw.SetModels(llmlabtest.Model{ID: "example-27b", Revision: "2026-09"})
	mcreds, err := provider.ManagedCredentials{Key: gwKey, BaseURL: gw.BaseURL()}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	mcredsPath := filepath.Join(t.TempDir(), "llmlab.json")
	if err := os.WriteFile(mcredsPath, mcreds, 0o600); err != nil {
		t.Fatal(err)
	}
	managedProfile := &v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: "wiring-llmlab"}, Spec: v1alpha1.PlatformProfileSpec{ProviderKind: v1alpha1.PlatformManaged}}
	managedProv := &v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "wiring-managed", Labels: map[string]string{v1alpha1.LabelPlatform: "llmlab"}},
		Spec: v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: managedProfile.Name},
			CredentialsRef: &v1alpha1.CredentialsReference{File: mcredsPath}},
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "wiring-allocs"}}
	for _, o := range []client.Object{profile, x86Prov, gh200Prov, managedProfile, managedProv, ns} {
		if err := c.Create(ctx, o); err != nil {
			t.Fatal(err)
		}
	}

	sl, session := newFakeSlurm()
	var platforms sync.Map // provider name -> calls, to show which Provider the platform served
	b := startBinary(t, f, deps{
		providerScheme: providerScheme,
		newPlatform: func(v slurm.Validated) (providerctl.Platform, io.Closer, error) {
			platforms.Store(v.Settings.Provider, true)
			return slurm.NewBatch(session, v.Profile, v.Settings, slurm.Options{PollInterval: time.Second, BusyInterval: 500 * time.Millisecond}), io.NopCloser(nil), nil
		},
	})

	// The lifecycle controller's health endpoint: the manager runs. It leads
	// through its Lease (review of PR 132: N2).
	b.eventually(t, "/healthz answers 200", 60*time.Second, func() bool {
		resp, err := http.Get("http://" + f.healthAddr + "/healthz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	b.eventually(t, "the lifecycle Lease is held", 60*time.Second, func() bool { return held(ctx, c, v1alpha1.LeaseLifecycle) })

	// The batch Providers, through the batch supervisor: each Allocation's job
	// is submitted over the fake Slurm, lifted and recorded.
	a := batchAllocation("alloc-a", ns.Name, x86Prov.Name, partX86, digestX86)
	g := batchAllocation("alloc-gh200", ns.Name, gh200Prov.Name, partGH200, digestGH200)
	for _, al := range []*v1alpha1.Allocation{a, g} {
		if err := c.Create(ctx, al); err != nil {
			t.Fatal(err)
		}
	}
	cred := writeCredential(t, f.host, a)
	writeCredential(t, f.host, g)
	get := func(obj client.Object) {
		t.Helper()
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			t.Fatal(err)
		}
	}
	for _, al := range []*v1alpha1.Allocation{a, g} {
		b.eventually(t, "the batch Allocation "+al.Name+"'s job is submitted, lifted and recorded", 90*time.Second, func() bool {
			get(al)
			return al.Annotations[v1alpha1.AnnotationJobID] != "" && al.Annotations[v1alpha1.AnnotationHoldLifted] != "" &&
				al.Status.AtProvider.Phase == v1alpha1.AllocationPending
		})
	}
	if n := sl.Count("sbatch"); n != 2 {
		t.Errorf("%d sbatch calls, want 2", n)
	}
	for _, p := range []string{x86Prov.Name, gh200Prov.Name} {
		if _, ok := platforms.Load(p); !ok {
			t.Errorf("the platform factory never served %s", p)
		}
	}
	if _, ok := platforms.Load(managedProv.Name); ok {
		t.Errorf("the batch platform factory served the managed Provider %s", managedProv.Name)
	}

	// agent.json as staged names the frpc of the partition's cpuArch: the
	// GH200 Allocation the arm64 build's, which frp-pins.txt pins, and the
	// other the amd64 one (review of PR 132: B1).
	pins := pinnedFrpc(t)
	for _, tc := range []struct {
		a    *v1alpha1.Allocation
		want string
	}{{a, pins["amd64"]}, {g, pins["arm64"]}} {
		if got := stagedConfig(t, sl, tc.a).FrpcDigest; got != tc.want {
			t.Errorf("%s's agent.json names frpc %s, want %s", tc.a.Name, got, tc.want)
		}
	}

	// The Provider controller's recorder: crossplane-runtime's Normal event for
	// the submission, regarding this Allocation, from hpcburst-provider.
	b.eventually(t, "a CreatedExternalResource event regarding the Allocation reaches the API", 60*time.Second, func() bool {
		var evs corev1.EventList
		if err := c.List(ctx, &evs, client.InNamespace(ns.Name)); err != nil {
			return false
		}
		for _, ev := range evs.Items {
			if ev.InvolvedObject.UID == a.UID && ev.InvolvedObject.Kind == "Allocation" && ev.Reason == "CreatedExternalResource" &&
				ev.Source.Component == v1alpha1.FieldManagerProvider && ev.Type == provider.EventKindOrdinary {
				return true
			}
		}
		return false
	})

	// The managed Provider, through the managed supervisor: the catalog read
	// from the double lands in its status.
	b.eventually(t, "the managed Provider's catalog lists the double's model", 90*time.Second, func() bool {
		get(managedProv)
		for _, ce := range managedProv.Status.Catalog {
			if ce.Name == "example-27b" && ce.Listed {
				return true
			}
		}
		return false
	})
	if n := gw.Count(http.MethodGet, "/v1/models"); n < 1 {
		t.Errorf("the double saw %d catalog reads, want at least 1", n)
	}

	// Each Provider runs in its own manager, leading through its own Lease.
	leases := []string{v1alpha1.LeaseLifecycle}
	for _, p := range []string{x86Prov.Name, gh200Prov.Name, managedProv.Name} {
		leases = append(leases, v1alpha1.ProviderLeaseName(p))
	}
	for _, l := range leases[1:] {
		if !held(ctx, c, l) {
			t.Errorf("the Lease %s is not held", l)
		}
	}

	// The agent API on the agent endpoint, with the endpoint certificate: the
	// pinned client trusts only that key. A call with the Allocation's tag and
	// credential but another job ID gets JobMismatch, which only a lookup that
	// found the Allocation through the manager's cache and a credential read
	// from the credentials directory can give; a wrong credential gets
	// Unauthenticated.
	pin := agentapi.PinOf(cert)
	tag := v1alpha1.JobTag(a.Namespace, a.Name, a.UID)
	if a.Annotations[v1alpha1.AnnotationJobID] == "999999999" {
		t.Fatal("the fake Slurm gave the job ID the test uses as the wrong one")
	}
	if code := register(t, f.host, pin, tag, cred, "999999999"); code != agentapi.CodeJobMismatch {
		t.Errorf("register with another job ID: %q, want %q", code, agentapi.CodeJobMismatch)
	}
	other, err := agentapi.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	if code := register(t, f.host, pin, tag, other, a.Annotations[v1alpha1.AnnotationJobID]); code != agentapi.CodeUnauthenticated {
		t.Errorf("register with another credential: %q, want %q", code, agentapi.CodeUnauthenticated)
	}
	// A call that passes authentication reaches the lifecycle controller's
	// backend, which accepts the registration (review of PR 132: N2). Until it
	// serves (its caches synced, this replica leading) it says
	// CodeUnavailable; any other answer fails.
	b.eventually(t, "a registration with the Allocation's credential and job ID is accepted", 60*time.Second, func() bool {
		switch code := register(t, f.host, pin, tag, cred, a.Annotations[v1alpha1.AnnotationJobID]); code {
		case "":
			return true
		case agentapi.CodeUnavailable:
			return false
		default:
			t.Fatalf("register with the Allocation's credential and job ID: %q, want a success", code)
			return false
		}
	})

	// A clean stop: run returns nil, the agent endpoint closes, every Lease
	// is released, and every component logged its start and its stop.
	if err := b.stop(t); err != nil {
		t.Fatalf("run returned %v after its context ended, want nil", err)
	}
	if listening(f.host.AgentEndpointAddr()) {
		t.Errorf("the agent endpoint %s still listens after the stop", f.host.AgentEndpointAddr())
	}
	for _, l := range leases {
		if held(ctx, c, l) {
			t.Errorf("the Lease %s is still held after the stop", l)
		}
	}
	out := logs.String()[from:]
	for _, name := range []string{"provider-supervisor-batch", "provider-supervisor-managed", "agent-api"} {
		for _, msg := range []string{"component starting", "component stopped"} {
			if !strings.Contains(out, fmt.Sprintf(`"msg"="%s" "component"="%s"`, msg, name)) {
				t.Errorf("the log has no %q line for %s", msg, name)
			}
		}
	}
}

// hangingPlatform is a batch platform whose Submit hangs, past the stop and
// whatever its context says, until the test releases it: a Provider
// controller still busy at a stop, as one writing a submission's outcome for
// up to 30 s is (providerctl's Create).
type hangingPlatform struct {
	providerctl.Platform
	entered chan<- struct{}
	release <-chan struct{}
}

func (p hangingPlatform) Submit(context.Context, provider.SubmitRequest) (string, provider.SubmitOutcome, error) {
	select {
	case p.entered <- struct{}{}:
	default:
	}
	<-p.release
	return "", provider.Unknown, errors.New("example: the submission was released after the stop")
}

// A stop while a submission hangs is a clean stop: run returns nil, and the
// Provider's Lease is released, because the Provider manager's grace
// (providerGrace) ends before the lifecycle manager's. With equal graces the
// outer one ran out first and run returned an error, so hpcburst down's
// SIGTERM became exit 1 (review of PR 132: N3). The wait is for the hanging
// call itself, and the bound is the graces', not a guessed window.
func TestControllerBinaryStopDuringSubmission(t *testing.T) {
	api := startAPIServer(t)
	ctx := context.Background()
	c := api.client
	f, _, digest := prepareState(t, api, "direct", exampleFront)
	profile := batchProfile("hang-slurm")
	prov := batchProvider("hang-batch", profile.Name, partX86, sshCredentials(t))
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "hang-allocs"}}
	for _, o := range []client.Object{&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.ControllerNamespace}}, profile, prov, ns} {
		if err := c.Create(ctx, o); err != nil {
			t.Fatal(err)
		}
	}
	_, session := newFakeSlurm()
	entered, release := make(chan struct{}, 1), make(chan struct{})
	b := startBinary(t, f, deps{
		providerScheme: providerScheme,
		newPlatform: func(v slurm.Validated) (providerctl.Platform, io.Closer, error) {
			batch := slurm.NewBatch(session, v.Profile, v.Settings, slurm.Options{PollInterval: time.Second, BusyInterval: 500 * time.Millisecond})
			return hangingPlatform{Platform: batch, entered: entered, release: release}, io.NopCloser(nil), nil
		},
	})
	// Registered after startBinary, so it runs first: a failed test releases
	// the call before the binary's stop.
	t.Cleanup(func() { close(release) })

	a := batchAllocation("alloc-hang", ns.Name, prov.Name, partX86, digest)
	if err := c.Create(ctx, a); err != nil {
		t.Fatal(err)
	}
	writeCredential(t, f.host, a)
	b.eventually(t, "the submission is under way", 90*time.Second, func() bool {
		select {
		case <-entered:
			return true
		default:
			return false
		}
	})
	if err := b.stop(t); err != nil {
		t.Fatalf("run returned %v after a stop during a submission, want nil (a clean stop, exit 0)", err)
	}
	for _, l := range []string{v1alpha1.LeaseLifecycle, v1alpha1.ProviderLeaseName(prov.Name)} {
		if held(ctx, c, l) {
			t.Errorf("the Lease %s is still held after the stop", l)
		}
	}
}

// register posts a registration to the agent endpoint with a client pinned to
// pin and returns the error code it gets ("" for a success).
func register(t *testing.T, h agentapi.Host, pin, tag, cred, jobID string) agentapi.Code {
	t.Helper()
	id, err := agentapi.NewAgentInstanceID()
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(agentapi.RegisterRequest{Node: "node-1", CPUArch: "x86_64"})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, "https://"+h.AgentEndpointAddr()+agentapi.PathRegister, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", agentapi.ContentType)
	req.Header.Set("Authorization", "Bearer "+cred)
	req.Header.Set(agentapi.HeaderTag, tag)
	req.Header.Set(agentapi.HeaderJobID, jobID)
	req.Header.Set(agentapi.HeaderAgentInstanceID, id)
	hc := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{TLSClientConfig: agentapi.ClientTLSConfig(pin)}}
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 == 2 {
		return ""
	}
	var e agentapi.Error
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		t.Fatalf("register: status %d with a body that is not an agent API error: %v", resp.StatusCode, err)
	}
	return e.Code
}
