// Package e2e runs the S4 scenarios: provider-slurm processes against an
// envtest kube-apiserver (no nodes, no cluster) and the fake Slurm on PATH.
// Expected values come from issue #24 and the HBEPs; the evidence is the fake
// Slurm's call log, never the reconciler's return values.
package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/apis/v1alpha1"
)

// Timings shared by all scenarios. MinJobAge is Helios's and Athena's
// measured value (profiles/*.draft.yaml).
const (
	minJobAge      = 10 * time.Second
	pollInterval   = time.Second
	reconcileTO    = 5 * time.Second
	inflightWindow = 12 * time.Second
)

type harness struct {
	work       string // per-run scratch directory
	slurmBin   string // directory with sbatch, squeue, sacct, scancel, fakeslurmctl
	provider   string // provider-slurm binary
	kubeconfig string
	cfg        *rest.Config
	k8s        client.Client
}

var h *harness

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	var err error
	h, err = setup()
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e setup:", err)
		return 2
	}
	env := envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "config", "crd")},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: assetsDir(),
	}
	t0 := time.Now()
	h.cfg, err = env.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "envtest start:", err)
		return 2
	}
	defer func() {
		if err := env.Stop(); err != nil {
			fmt.Fprintln(os.Stderr, "envtest stop:", err)
		}
	}()
	fmt.Fprintf(os.Stderr, "envtest: kube-apiserver and etcd up in %s at %s (work dir %s)\n", time.Since(t0).Round(time.Millisecond), h.cfg.Host, h.work)

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if h.k8s, err = client.New(h.cfg, client.Options{Scheme: scheme}); err != nil {
		fmt.Fprintln(os.Stderr, "client:", err)
		return 2
	}
	user, err := env.AddUser(envtest.User{Name: "provider-slurm", Groups: []string{"system:masters"}}, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "add user:", err)
		return 2
	}
	kc, err := user.KubeConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "kubeconfig:", err)
		return 2
	}
	h.kubeconfig = filepath.Join(h.work, "kubeconfig")
	if err := os.WriteFile(h.kubeconfig, kc, 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	return m.Run()
}

func assetsDir() string { return os.Getenv("KUBEBUILDER_ASSETS") }

// setup builds the provider and the fake Slurm into a fresh work directory
// under S4_WORK_ROOT (default: the system temp directory).
func setup() (*harness, error) {
	if assetsDir() == "" {
		return nil, fmt.Errorf("KUBEBUILDER_ASSETS is unset; run: export KUBEBUILDER_ASSETS=$(setup-envtest use 1.35.0 -p path)")
	}
	root := os.Getenv("S4_WORK_ROOT")
	if root == "" {
		root = filepath.Join(os.TempDir(), "s4-e2e")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	work, err := os.MkdirTemp(root, "run-")
	if err != nil {
		return nil, err
	}
	bin := filepath.Join(work, "bin")
	slurmBin := filepath.Join(work, "slurm")
	for _, d := range []string{bin, slurmBin} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	builds := map[string]string{
		filepath.Join(bin, "provider-slurm"): "../cmd/provider-slurm",
		filepath.Join(bin, "fakeslurm"):      "../cmd/fakeslurm",
	}
	for out, pkg := range builds {
		cmd := exec.Command("go", "build", "-o", out, pkg)
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("go build %s: %w", pkg, err)
		}
	}
	for _, name := range []string{"sbatch", "squeue", "sacct", "scancel", "fakeslurmctl"} {
		if err := os.Symlink(filepath.Join(bin, "fakeslurm"), filepath.Join(slurmBin, name)); err != nil {
			return nil, err
		}
	}
	return &harness{work: work, slurmBin: slurmBin, provider: filepath.Join(bin, "provider-slurm")}, nil
}
