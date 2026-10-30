//go:build s3embedded

// Command s3-embedded runs kcp as a Go library, in one process together with the
// spike's controllers (HBEP-11: "one bundled binary for a single user"). It follows the
// start sequence of kcp's own cmd/kcp/kcp.go at the pinned release; the only additions are
// the in-process controller-runtime manager and the flag --embedded-controllers.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	cliflag "k8s.io/component-base/cli/flag"
	logsapiv1 "k8s.io/component-base/logs/api/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kcp-dev/embeddedetcd"

	"github.com/kcp-dev/kcp/cmd/kcp/options"
	kcpfeatures "github.com/kcp-dev/kcp/pkg/features"
	"github.com/kcp-dev/kcp/pkg/server"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s3/kit"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "s3-embedded:", err)
		os.Exit(1)
	}
}

func run() error {
	rootDir := ".kcp"
	for i, a := range os.Args {
		if a == "--root-directory" && i < len(os.Args)-1 {
			rootDir = os.Args[i+1]
		} else if strings.HasPrefix(a, "--root-directory=") {
			rootDir = strings.TrimPrefix(a, "--root-directory=")
		}
	}
	kcpOptions := options.NewOptions(rootDir)
	kcpOptions.Server.GenericControlPlane.Logs.Verbosity = logsapiv1.VerbosityLevel(2)

	fss := cliflag.NamedFlagSets{}
	kcpOptions.AddFlags(&fss)
	fs := pflag.NewFlagSet("s3-embedded", pflag.ExitOnError)
	for _, f := range fss.FlagSets {
		fs.AddFlagSet(f)
	}
	withControllers := fs.Bool("embedded-controllers", true, "run the spike's controllers in this process")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	rest.SetDefaultWarningHandler(rest.NoWarnings{})
	if err := logsapiv1.ValidateAndApply(kcpOptions.Server.GenericControlPlane.Logs, kcpfeatures.DefaultFeatureGate); err != nil {
		return err
	}
	ctx := genericapiserver.SetupSignalContext()
	completed, err := kcpOptions.Complete(ctx)
	if err != nil {
		return err
	}
	if errs := completed.Validate(); len(errs) > 0 {
		return utilerrors.NewAggregate(errs)
	}
	serverConfig, err := server.NewConfig(ctx, completed.Server)
	if err != nil {
		return err
	}
	completedConfig, err := serverConfig.Complete()
	if err != nil {
		return err
	}
	// As in kcp's main: etcd must be up before NewServer, and outlives the apiserver on shutdown.
	etcdCtx, etcdCancel := context.WithCancel(context.Background())
	defer etcdCancel()
	if completedConfig.EmbeddedEtcd.Config != nil {
		if err := embeddedetcd.NewServer(completedConfig.EmbeddedEtcd).Run(etcdCtx); err != nil {
			return err
		}
	}
	s, err := server.NewServer(completedConfig)
	if err != nil {
		return err
	}
	if *withControllers {
		go func() {
			if err := runControllers(ctx, rootDir); err != nil && ctx.Err() == nil {
				klog.ErrorS(err, "embedded controllers stopped")
			}
		}()
	}
	return s.Run(ctx)
}

// nopCanceller releases a Slot at once; the real provider plugin belongs to HBEP-4.
type nopCanceller struct{}

func (nopCanceller) Cancel(context.Context, *unstructured.Unstructured) error { return nil }

// runControllers waits for the server's admin kubeconfig, prepares the child workspace and
// the CRDs, and runs the manager against that workspace until ctx ends.
func runControllers(ctx context.Context, rootDir string) error {
	admin := filepath.Join(rootDir, "admin.kubeconfig")
	for {
		if _, err := os.Stat(admin); err == nil {
			if _, err := kit.PrepareKCPWorkspace(ctx, admin, filepath.Join(rootDir, "work.kubeconfig")); err == nil {
				break
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	rc, err := clientcmd.BuildConfigFromFlags("", filepath.Join(rootDir, "work.kubeconfig"))
	if err != nil {
		return err
	}
	c, err := client.New(rc, client.Options{Scheme: kit.Scheme()})
	if err != nil {
		return err
	}
	if err := kit.InstallCRDs(ctx, c); err != nil {
		return err
	}
	mgr, err := kit.Manager(rc, nopCanceller{})
	if err != nil {
		return err
	}
	return mgr.Start(ctx)
}
