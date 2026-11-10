package keeperproc

// The API server of Stage B's tests (#343's PR 4: the relay keeper's
// controller side with real controller processes): controller-runtime's
// envtest, etcd and kube-apiserver on ports the kernel chose, with
// internal/testenv's sha256-checked binaries and the committed CRDs, and what
// the tests read from it: a Provider's Lease as written, its Ready condition,
// an Allocation's RelayEndedForGood mark, and the RelayFailed Events regarding
// an Allocation. Every expected value a test checks is read here from the
// API server, never from what a controller reports.
//
// Provenance: startAPIServer and processesNaming are copied from
// cmd/hpcburst-controller/run_test.go, a test file of package main that cannot
// be imported. internal/testenv.Start is not used: its port blocks are all
// taken on the development machine (IMPL-COMMON, Envtest port blocks).

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

var quietOnce sync.Once

// apiServer is a running envtest control plane with the committed CRDs.
type apiServer struct {
	env        *envtest.Environment
	c          client.Client
	kubeconfig []byte
}

// providerScheme is the scheme the controller binary gives every Provider
// manager (cmd/hpcburst-controller's providerScheme): the built-in kinds,
// apps/v1 among them, which providerctl.New's accounting check needs, and the
// framework's. The test's own client reads with it too.
func providerScheme() (*runtime.Scheme, error) {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		return nil, err
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		return nil, err
	}
	return s, nil
}

// startAPIServer starts etcd and kube-apiserver on ports the kernel chose,
// installs config/crd, and stops them at the end of the test, checking that
// no process is left and that the root k3s's ports still listen.
func startAPIServer(t *testing.T) *apiServer {
	t.Helper()
	// envtest logs through controller-runtime's logger, which this process
	// does not show (each controller logs its own, to its standard error).
	quietOnce.Do(func() { ctrl.SetLogger(logr.Discard()) })
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
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "config", "crd")},
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
	s, err := providerScheme()
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	return &apiServer{env: env, c: c, kubeconfig: env.KubeConfig}
}

// processesNaming returns the PIDs of the processes whose command line
// contains s.
func processesNaming(s string) []int {
	entries, _ := os.ReadDir("/proc")
	var pids []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err == nil && strings.Contains(string(b), s) {
			pids = append(pids, pid)
		}
	}
	return pids
}

// leaseRecord is a Provider's Lease as written: its holder, its
// leaseTransitions and its renewTime ("" and zero times when it has none).
type leaseRecord struct {
	holder      string
	transitions int64
	renew       time.Time
	found       bool
}

// lease reads the Provider's Lease, provider-<name> in the controller
// namespace.
func (a *apiServer) lease(t *testing.T, provider string) leaseRecord {
	t.Helper()
	var l coordinationv1.Lease
	err := a.c.Get(context.Background(), client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.ProviderLeaseName(provider)}, &l)
	if err != nil {
		return leaseRecord{}
	}
	r := leaseRecord{found: true}
	if l.Spec.HolderIdentity != nil {
		r.holder = *l.Spec.HolderIdentity
	}
	if l.Spec.LeaseTransitions != nil {
		r.transitions = int64(*l.Spec.LeaseTransitions)
	}
	if l.Spec.RenewTime != nil {
		r.renew = l.Spec.RenewTime.Time
	}
	return r
}

// waitLease waits until the Provider's Lease, as written, satisfies cond, and
// returns it.
func (a *apiServer) waitLease(t *testing.T, provider, what string, d time.Duration, cond func(leaseRecord) bool) leaseRecord {
	t.Helper()
	var r leaseRecord
	waitFor(t, d, what, func() bool {
		r = a.lease(t, provider)
		return cond(r)
	})
	return r
}

// ready is the Provider's Ready condition, and whether it has one.
func (a *apiServer) ready(t *testing.T, provider string) (metav1.Condition, bool) {
	t.Helper()
	var p v1alpha1.Provider
	if err := a.c.Get(context.Background(), client.ObjectKey{Name: provider}, &p); err != nil {
		t.Fatalf("get Provider %s: %v", provider, err)
	}
	c := apimeta.FindStatusCondition(p.Status.Conditions, v1alpha1.ConditionProviderReady)
	if c == nil {
		return metav1.Condition{}, false
	}
	return *c, true
}

// allocation reads an Allocation, and whether it exists.
func (a *apiServer) allocation(t *testing.T, ns, name string) (*v1alpha1.Allocation, bool) {
	t.Helper()
	var al v1alpha1.Allocation
	if err := a.c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &al); err != nil {
		return nil, false
	}
	return &al, true
}

// mark is the Allocation's RelayEndedForGood condition, when it is True.
func mark(al *v1alpha1.Allocation) (status, reason, message string) {
	c := al.GetCondition(v1alpha1.ConditionRelayEndedForGood)
	return string(c.Status), string(c.Reason), c.Message
}

// relayFailedEvents counts the RelayFailed Events regarding the Allocation of
// uid in its namespace, each Event by the occurrences it records (a repeated
// event is one object whose count goes up).
func (a *apiServer) relayFailedEvents(t *testing.T, ns, uid string) int {
	t.Helper()
	var l corev1.EventList
	if err := a.c.List(context.Background(), &l, client.InNamespace(ns)); err != nil {
		t.Fatalf("list the Events: %v", err)
	}
	n := 0
	for _, e := range l.Items {
		if string(e.InvolvedObject.UID) != uid || e.Reason != v1alpha1.EventReasonRelayFailed {
			continue
		}
		c := max(int(e.Count), 1)
		if e.Series != nil {
			c = max(c, int(e.Series.Count))
		}
		n += c
	}
	return n
}
