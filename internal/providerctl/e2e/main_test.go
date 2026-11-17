// Package e2e runs the Slurm Provider's controller in real controller-runtime
// managers against the assembled test control plane of internal/testenv (a
// real kube-apiserver, etcd and kube-controller-manager), with the committed
// CRDs: the per-Provider supervisor, the leader Lease of each Provider, and
// an Allocation reconciled end to end over the in-process fake Slurm.
//
// It binds loopback ports through internal/testenv, on the block from 29212
// unless HPCBURST_TESTENV_PORT_BASE says otherwise, so that go test ./... can
// run it beside api/v1alpha1/e2e, which uses the default block from 29200.
// Run it under the shared envtest lock on a machine where other agents test
// too.
package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog/node/nodetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv/crdinstall"
)

var (
	cfg *rest.Config
	c   client.Client
)

func scheme() *runtime.Scheme {
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, apiextensionsv1.AddToScheme, v1alpha1.AddToScheme} {
		if err := add(s); err != nil {
			panic(err)
		}
	}
	return s
}

func TestMain(m *testing.M) {
	code := run(m)
	nodetest.Cleanup() // the controller binary the log rows built, unless HPCBURST_CONTROLLER_BIN named one
	os.Exit(code)
}

// portBase is this package's testenv port block; api/v1alpha1/e2e takes the
// default, 29200, and go test runs packages in parallel.
const portBase = "29212"

func run(m *testing.M) int {
	if os.Getenv("HPCBURST_TESTENV_PORT_BASE") == "" {
		if err := os.Setenv("HPCBURST_TESTENV_PORT_BASE", portBase); err != nil {
			fmt.Fprintln(os.Stderr, "set the port base:", err)
			return 1
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	guard := testenv.GuardSystemPorts()
	dir, err := os.MkdirTemp("", "hpcburst-providerctl-e2e-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "temporary directory:", err)
		return 1
	}
	defer os.RemoveAll(dir)
	cp, err := testenv.Start(ctx, dir, testenv.Options{
		BeforeControllerManager: func(ctx context.Context, cfg *rest.Config) error {
			cl, err := client.New(cfg, client.Options{Scheme: scheme()})
			if err != nil {
				return err
			}
			return installCRDs(ctx, cl)
		},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "start the control plane:", err)
		return 1
	}
	cfg = cp.Config
	code := 1
	func() {
		if c, err = client.New(cfg, client.Options{Scheme: scheme()}); err != nil {
			fmt.Fprintln(os.Stderr, "client:", err)
			return
		}
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.ControllerNamespace}}
		if err := c.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
			fmt.Fprintln(os.Stderr, "namespace:", err)
			return
		}
		code = m.Run()
	}()
	if err := cp.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "stop the control plane:", err)
		code = 1
	}
	if err := cp.AssertAllDead(); err != nil {
		fmt.Fprintln(os.Stderr, "leftover process:", err)
		code = 1
	}
	if err := guard(); err != nil {
		fmt.Fprintln(os.Stderr, "system port check:", err)
		code = 1
	}
	return code
}

// installCRDs installs the committed CRDs and waits until each is Established and its kind is served (crdinstall; issue #600).
func installCRDs(ctx context.Context, cl client.Client) error {
	crds, err := crdinstall.ReadCRDs(filepath.Join("..", "..", "..", "config", "crd"))
	if err != nil {
		return err
	}
	return crdinstall.InstallAndWait(ctx, cl, crds)
}
