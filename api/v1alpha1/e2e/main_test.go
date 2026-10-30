package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

const (
	hostAssembled = "assembled"
	hostK3s       = "k3s"
)

var (
	cfg *rest.Config
	c   client.Client
	// host is the control-plane host under test: hostAssembled or hostK3s.
	host string
)

func scheme() *runtime.Scheme {
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		clientgoscheme.AddToScheme, apiextensionsv1.AddToScheme, admissionregistrationv1.AddToScheme, v1alpha1.AddToScheme,
	} {
		if err := add(s); err != nil {
			panic(err)
		}
	}
	return s
}

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	host = os.Getenv("HPCBURST_TEST_HOST")
	if host == "" {
		host = hostAssembled
	}
	guard := testenv.GuardSystemPorts()

	var cp *testenv.ControlPlane
	switch host {
	case hostAssembled:
		dir, err := os.MkdirTemp("", "hpcburst-e2e-")
		if err != nil {
			return fail("temporary directory: %v", err)
		}
		defer os.RemoveAll(dir)
		cp, err = testenv.Start(ctx, dir, testenv.Options{
			// HPCBURST_TEST_BARE=1 leaves out the controller manager. It exists to
			// show that the cascade test fails on a bare API server; never set it
			// for a real run.
			WithoutControllerManager: os.Getenv("HPCBURST_TEST_BARE") == "1",
			BeforeControllerManager: func(ctx context.Context, cfg *rest.Config) error {
				cl, err := client.New(cfg, client.Options{Scheme: scheme()})
				if err != nil {
					return err
				}
				return installCRDs(ctx, cl)
			},
		})
		if err != nil {
			return fail("start the control plane: %v", err)
		}
		cfg = cp.Config
	case hostK3s:
		kc := os.Getenv("HPCBURST_KUBECONFIG")
		if kc == "" {
			return fail("HPCBURST_TEST_HOST=k3s needs HPCBURST_KUBECONFIG, the kubeconfig of the k3s the launcher (#41) started")
		}
		var err error
		if cfg, err = clientcmd.BuildConfigFromFlags("", kc); err != nil {
			return fail("kubeconfig %s: %v", kc, err)
		}
	default:
		return fail("HPCBURST_TEST_HOST=%q: want %q or %q", host, hostAssembled, hostK3s)
	}
	cfg.QPS, cfg.Burst = 200, 400

	code := 1
	func() {
		var err error
		if c, err = client.New(cfg, client.Options{Scheme: scheme()}); err != nil {
			fmt.Fprintln(os.Stderr, "client:", err)
			return
		}
		// Install from the committed YAML on every host. On the assembled stack
		// this is a second, idempotent pass; on k3s it is the install.
		if err := installCRDs(ctx, c); err != nil {
			fmt.Fprintln(os.Stderr, "install the CRDs:", err)
			return
		}
		code = m.Run()
	}()

	if cp != nil {
		if err := cp.Stop(); err != nil {
			fmt.Fprintln(os.Stderr, "stop the control plane:", err)
			code = 1
		}
		if err := cp.AssertAllDead(); err != nil {
			fmt.Fprintln(os.Stderr, "leftover process:", err)
			code = 1
		}
	}
	// A server that listened on a system port before the run listens after it.
	if err := guard(); err != nil {
		fmt.Fprintln(os.Stderr, "system port check:", err)
		code = 1
	}
	return code
}

func fail(format string, args ...any) int {
	fmt.Fprintf(os.Stderr, "e2e: "+format+"\n", args...)
	return 1
}

// configDir is the repository's config directory, relative to this package.
func configDir(sub string) string { return filepath.Join("..", "..", "..", "config", sub) }
