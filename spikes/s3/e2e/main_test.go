// Package e2e is the one end-to-end suite that runs, as one compiled binary,
// against every control-plane host option. The host is chosen only by the
// kubeconfig in S3_KUBECONFIG.
package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s3/kit"
)

var (
	cfg *rest.Config
	c   client.WithWatch
)

func TestMain(m *testing.M) {
	kc := os.Getenv("S3_KUBECONFIG")
	if kc == "" {
		fmt.Fprintln(os.Stderr, "S3_KUBECONFIG must name the kubeconfig of the host under test")
		os.Exit(2)
	}
	var err error
	cfg, err = clientcmd.BuildConfigFromFlags("", kc)
	if err != nil {
		fmt.Fprintln(os.Stderr, "kubeconfig:", err)
		os.Exit(2)
	}
	cfg.QPS, cfg.Burst = 200, 400
	c, err = client.NewWithWatch(cfg, client.Options{Scheme: kit.Scheme()})
	if err != nil {
		fmt.Fprintln(os.Stderr, "client:", err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	err = kit.InstallCRDs(ctx, c)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "install CRDs:", err)
		os.Exit(2)
	}
	os.Exit(m.Run())
}
