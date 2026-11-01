package main

// The binary's run function against a real API server with the committed CRDs:
// both per-Provider manager supervisors, each with a Provider reconciled
// through it (the batch one over the in-process fake Slurm, the managed one
// against the LLMLab double), the agent API on the agent endpoint with the
// endpoint certificate, an Allocation event written by the Provider
// controller's recorder, and the refusal of a Provider-manager scheme without
// apps/v1. Everything but the platform's transport and that scheme is the
// path main takes.
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

// metricsPortOffset is the controller's metrics port in the launcher's block
// (HBEP-8, Metrics: base + 12), which #111 binds.
const metricsPortOffset = 12

// freePortBase returns a port base whose ports the binary binds are free: the
// agent endpoint (+3), frps's plugin (+8) and the metrics (+12). The kernel
// chooses the agent endpoint's port; the others are checked, and all are
// released just before the binary binds them.
func freePortBase(t *testing.T) int {
	t.Helper()
	for range 100 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		base := ln.Addr().(*net.TCPAddr).Port - agentapi.AgentEndpointPortOffset
		held := []net.Listener{ln}
		ok := base >= 1024 && base+metricsPortOffset <= 65535
		for _, off := range []int{agentapi.FrpsPluginPortOffset, metricsPortOffset} {
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

// freePort returns a loopback port the kernel chose, released.
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

func eventually(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting until %s", timeout, what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// binary is one run of the binary's run function, as main calls it.
type binary struct {
	f      flags
	cancel context.CancelFunc
	done   chan error
}

func startBinary(t *testing.T, f flags, d deps) *binary {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	b := &binary{f: f, cancel: cancel, done: make(chan error, 1)}
	go func() { b.done <- run(ctx, f, d) }()
	t.Cleanup(func() { b.stop(t) })
	return b
}

// stop cancels the run, as SIGTERM does, and returns its error; a run that
// does not return within a minute fails the test.
func (b *binary) stop(t *testing.T) error {
	t.Helper()
	if b.cancel == nil {
		return nil
	}
	b.cancel()
	b.cancel = nil
	select {
	case err := <-b.done:
		return err
	case <-time.After(time.Minute):
		t.Fatal("run did not return within a minute of its context's end")
		return nil
	}
}

// prepareState prepares a state directory as hpcburst up leaves it: the kubeconfig,
// the private credentials directory, an agent build and the endpoint
// certificate.
func prepareState(t *testing.T, api *apiServer) (flags, *x509.Certificate, string) {
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
	agent := []byte("#!/bin/sh\n# an example agent build\n")
	sum := sha256.Sum256(agent)
	if err := os.WriteFile(filepath.Join(h.AgentBuildsDir(), agentapi.AgentBuildName("x86_64")), agent, 0o700); err != nil {
		t.Fatal(err)
	}
	cert := writeEndpointCert(t, h)
	args := []string{"--state-dir", state, "--port-base", strconv.Itoa(h.PortBase),
		"--health-addr", net.JoinHostPort("127.0.0.1", strconv.Itoa(freePort(t))),
		"--front-mode", "direct", "--front-addr", "front.example.org:29443"}
	f, err := parse(args, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	return f, cert, hex.EncodeToString(sum[:])
}

func TestControllerBinary(t *testing.T) {
	api := startAPIServer(t)
	ctx := context.Background()
	c := api.client
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.ControllerNamespace}}); err != nil {
		t.Fatal(err)
	}

	t.Run("refuses a Provider-manager scheme without apps/v1", func(t *testing.T) {
		f, _, _ := prepareState(t, api)
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
		select {
		case err := <-b.done:
			b.cancel()
			b.cancel = nil
			if err == nil || !strings.Contains(err.Error(), "apps/v1") || !strings.Contains(err.Error(), "ControllerRevision") {
				t.Fatalf("run returned %v, want a refusal naming apps/v1's ControllerRevision", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("run did not refuse the scheme within 30 s: it started instead")
		}
		for _, addr := range []string{f.host.AgentEndpointAddr(), f.healthAddr} {
			if listening(addr) {
				t.Errorf("%s listens after the refusal; nothing should have started", addr)
			}
		}
	})

	t.Run("runs both supervisors and the agent API", func(t *testing.T) {
		testBinaryComponents(t, api)
	})
}

func testBinaryComponents(t *testing.T, api *apiServer) {
	ctx := context.Background()
	c := api.client
	f, cert, digest := prepareState(t, api)
	creds := t.TempDir()

	// The batch Provider: the profile, its ssh credentials (mode 600) and an
	// Allocation, as setup_test.go in internal/providerctl/e2e builds them.
	key, sshCreds := filepath.Join(creds, "key"), filepath.Join(creds, "ssh")
	for path, content := range map[string]string{
		key:      "example key\n",
		sshCreds: "HostName login.example.org\nUser example-user\nIdentityFile " + key + "\n",
	} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	gpus := int32(4)
	maxTime, window := v1alpha1.Duration("48h"), v1alpha1.Duration("5m")
	batchProfile := &v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: "wiring-slurm"}, Spec: v1alpha1.PlatformProfileSpec{
		ProviderKind: v1alpha1.PlatformBatch, HoldRelease: true, JSONDataParser: "v0.0.42", InflightWindow: &window,
		StagingDir: "$SCRATCH/stage",
		Partitions: []v1alpha1.PartitionProfile{{Name: "example-gpu", AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "A100", CPUArch: "x86_64"},
			GPUsPerNode: &gpus, MaxTime: &maxTime}},
	}}
	batchProv := &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: "wiring-batch"}, Spec: v1alpha1.ProviderSpec{
		PlatformProfileRef: v1alpha1.ObjectReference{Name: batchProfile.Name}, Account: "example-account",
		AllowedPartitions: []string{"example-gpu"}, CredentialsRef: &v1alpha1.CredentialsReference{File: sshCreds},
	}}

	// The managed Provider, against the LLMLab double on a port the kernel chose.
	gwPort := freePort(t)
	gwKey := "example-key-wiring-0001"
	gw := llmlabtest.Start(t, llmlabtest.Options{PortLo: gwPort, PortHi: gwPort, Keys: []string{gwKey}})
	gw.SetModels(llmlabtest.Model{ID: "example-27b", Revision: "2026-09"})
	mcreds, err := provider.ManagedCredentials{Key: gwKey, BaseURL: gw.BaseURL()}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	mcredsPath := filepath.Join(creds, "llmlab.json")
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
	for _, o := range []client.Object{batchProfile, batchProv, managedProfile, managedProv, ns} {
		if err := c.Create(ctx, o); err != nil {
			t.Fatal(err)
		}
	}

	sl := fakeslurm.New(slurm.RealClock, fakeslurm.Config{Partitions: []fakeslurm.Partition{{Name: "example-gpu", GPUOnly: true}}})
	session := sl.Login(fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": "/home/example-user", "SCRATCH": "/scratch/example-user"}})
	var platforms sync.Map // provider name -> calls, to show which Provider the platform served
	b := startBinary(t, f, deps{
		providerScheme: providerScheme,
		newPlatform: func(v slurm.Validated) (providerctl.Platform, io.Closer, error) {
			platforms.Store(v.Settings.Provider, true)
			return slurm.NewBatch(session, v.Profile, v.Settings, slurm.Options{PollInterval: time.Second, BusyInterval: 500 * time.Millisecond}), io.NopCloser(nil), nil
		},
	})

	// The lifecycle controller's health endpoint: the manager runs.
	eventually(t, "/healthz answers 200", 60*time.Second, func() bool {
		resp, err := http.Get("http://" + f.healthAddr + "/healthz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	// The batch Provider, through the batch supervisor: the Allocation's job is
	// submitted over the fake Slurm, lifted and recorded.
	a := &v1alpha1.Allocation{
		ObjectMeta: metav1.ObjectMeta{Name: "alloc-a", Namespace: ns.Name},
		Spec: v1alpha1.AllocationSpec{
			ProviderConfigReference: &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: batchProv.Name},
			ForProvider: v1alpha1.AllocationParameters{Batch: &v1alpha1.BatchParameters{
				Partition: "example-gpu", TimeLimit: "30m", GPUs: 1, CPUsPerTask: 8, Memory: resource.MustParse("16Gi"), AgentDigest: digest,
			}},
		},
	}
	if err := c.Create(ctx, a); err != nil {
		t.Fatal(err)
	}
	// The lifecycle controller writes the credential once the UID exists.
	cred, err := agentapi.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := agentapi.WriteKeyFile(agentapi.AllocationCredentialPath(f.host.CredentialsDir(), string(a.UID)), cred); err != nil {
		t.Fatal(err)
	}
	get := func(obj client.Object) {
		t.Helper()
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "the batch Allocation's job is submitted, lifted and recorded", 90*time.Second, func() bool {
		get(a)
		return a.Annotations[v1alpha1.AnnotationJobID] != "" && a.Annotations[v1alpha1.AnnotationHoldLifted] != "" &&
			a.Status.AtProvider.Phase == v1alpha1.AllocationPending
	})
	if n := sl.Count("sbatch"); n != 1 {
		t.Errorf("%d sbatch calls, want 1", n)
	}
	if _, ok := platforms.Load(batchProv.Name); !ok {
		t.Errorf("the platform factory never served %s", batchProv.Name)
	}
	if _, ok := platforms.Load(managedProv.Name); ok {
		t.Errorf("the batch platform factory served the managed Provider %s", managedProv.Name)
	}

	// The Provider controller's recorder: crossplane-runtime's Normal event for
	// the submission, regarding this Allocation, from hpcburst-provider.
	eventually(t, "a CreatedExternalResource event regarding the Allocation reaches the API", 60*time.Second, func() bool {
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
	eventually(t, "the managed Provider's catalog lists the double's model", 90*time.Second, func() bool {
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
	held := func(p string) bool {
		var l coordinationv1.Lease
		err := c.Get(ctx, client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.ProviderLeaseName(p)}, &l)
		return err == nil && l.Spec.HolderIdentity != nil && *l.Spec.HolderIdentity != ""
	}
	for _, p := range []string{batchProv.Name, managedProv.Name} {
		if !held(p) {
			t.Errorf("the Lease %s is not held", v1alpha1.ProviderLeaseName(p))
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

	// A clean stop: run returns nil, the agent endpoint closes, both Leases
	// are released, and every component logged its start and its stop.
	if err := b.stop(t); err != nil {
		t.Fatalf("run returned %v after its context ended, want nil", err)
	}
	if listening(f.host.AgentEndpointAddr()) {
		t.Errorf("the agent endpoint %s still listens after the stop", f.host.AgentEndpointAddr())
	}
	for _, p := range []string{batchProv.Name, managedProv.Name} {
		if held(p) {
			t.Errorf("the Lease %s is still held after the stop", v1alpha1.ProviderLeaseName(p))
		}
	}
	out := logs.String()
	for _, name := range []string{"provider-supervisor-batch", "provider-supervisor-managed", "agent-api"} {
		for _, msg := range []string{"component starting", "component stopped"} {
			if !strings.Contains(out, fmt.Sprintf(`"msg"="%s" "component"="%s"`, msg, name)) {
				t.Errorf("the log has no %q line for %s", msg, name)
			}
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
