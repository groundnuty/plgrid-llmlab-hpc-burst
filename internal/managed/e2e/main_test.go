package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

// portBase is issue #69's block of the test host's ports, 29216-29219
// (IMPL-COMMON.md, Envtest port blocks), used when HPCBURST_TESTENV_PORT_BASE
// is unset: packages run in parallel, so each one that starts a control plane
// has its own block.
const portBase = "29216"

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

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	if os.Getenv("HPCBURST_TESTENV_PORT_BASE") == "" {
		if err := os.Setenv("HPCBURST_TESTENV_PORT_BASE", portBase); err != nil {
			return fail("set the port base: %v", err)
		}
	}
	ctrllog.SetLogger(logr.Discard())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	guard := testenv.GuardSystemPorts()

	dir, err := os.MkdirTemp("", "hpcburst-managed-e2e-")
	if err != nil {
		return fail("temporary directory: %v", err)
	}
	defer os.RemoveAll(dir)
	cp, err := testenv.Start(ctx, dir, testenv.Options{
		BeforeControllerManager: func(ctx context.Context, cfg *rest.Config) error {
			cl, err := client.New(cfg, client.Options{Scheme: scheme()})
			if err != nil {
				return err
			}
			return installProviderCRD(ctx, cl)
		},
	})
	if err != nil {
		return fail("start the control plane: %v", err)
	}
	cfg = cp.Config

	code := 1
	func() {
		if c, err = client.New(cfg, client.Options{Scheme: scheme()}); err != nil {
			fmt.Fprintln(os.Stderr, "client:", err)
			return
		}
		// The launcher creates the Leases' namespace before it starts the
		// controller binary (docs/development.md, Process layout).
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.ControllerNamespace}}
		if err := c.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
			fmt.Fprintln(os.Stderr, "create the controller namespace:", err)
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

func fail(format string, args ...any) int {
	fmt.Fprintf(os.Stderr, "managed e2e: "+format+"\n", args...)
	return 1
}

// installProviderCRD applies the committed Provider CRD and waits until the
// API serves it.
func installProviderCRD(ctx context.Context, cl client.Client) error {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "config", "crd", "hpcburst.groundnuty.github.io_providers.yaml"))
	if err != nil {
		return err
	}
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096).Decode(crd); err != nil {
		return fmt.Errorf("decode the Provider CRD: %w", err)
	}
	if err := cl.Create(ctx, crd); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create %s: %w", crd.Name, err)
	}
	return wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 60*time.Second, true, func(ctx context.Context) (bool, error) {
		return cl.List(ctx, &v1alpha1.ProviderList{}) == nil, nil
	})
}
