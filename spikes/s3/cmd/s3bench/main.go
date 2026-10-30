// Command s3bench measures one control-plane host option for spike S3: start time,
// RSS idle and with ~100 objects, the shared e2e suite, and a minor-version upgrade.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
)

type bench struct {
	scratch string
	e2eBin  string
	outDir  string
	h       host
	live    []*running
	res     *result
}

func main() {
	scratch := flag.String("scratch", os.Getenv("SCRATCH"), "scratch directory for binaries, data and logs (required; default $SCRATCH)")
	option := flag.String("option", "", "envtest-bare | envtest-gc | kcp")
	e2e := flag.String("e2e", "", "compiled e2e test binary (default $SCRATCH/bin/e2e.test)")
	out := flag.String("out", "results", "directory for result JSON and scrubbed e2e logs")
	only := flag.String("only", "all", "all | main | upgrade (which parts to run)")
	flag.Parse()
	if *scratch == "" {
		fmt.Fprintln(os.Stderr, "set -scratch or $SCRATCH")
		os.Exit(2)
	}
	if *e2e == "" {
		*e2e = filepath.Join(*scratch, "bin/e2e.test")
	}
	var h host
	switch *option {
	case "envtest-bare":
		h = envtestHost{scratch: *scratch}
	case "envtest-gc":
		h = envtestHost{scratch: *scratch, gc: true}
	case "kcp":
		h = kcpHost{scratch: *scratch}
	default:
		fmt.Fprintln(os.Stderr, "unknown or missing -option")
		os.Exit(2)
	}
	for _, d := range []string{"logs", "data", "work/kcphome"} {
		if err := os.MkdirAll(filepath.Join(*scratch, d), 0o700); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if err := os.MkdirAll(filepath.Join(*out, "logs"), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctrl.SetLogger(logr.Discard()) // controller-runtime clients in the harness need no log output
	b := &bench{scratch: *scratch, e2eBin: *e2e, outDir: *out, h: h, res: newResult(h)}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if *only != "all" { // a partial run adds to the stored result of the same option
		if bs, rerr := os.ReadFile(filepath.Join(*out, h.name()+".json")); rerr == nil {
			var prev result
			if json.Unmarshal(bs, &prev) == nil {
				b.res = &prev
			}
		}
	}
	err := b.runAll(ctx, *only)
	b.cleanup() // nothing of ours may keep running
	b.res.finish()
	if werr := b.writeResult(); werr != nil {
		fmt.Fprintln(os.Stderr, "write result:", werr)
	}
	b.res.print(os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func (b *bench) cleanup() {
	for _, r := range b.live {
		_, _ = r.stopAll(30e9)
	}
	b.live = nil
}

func (b *bench) writeResult() error {
	bs, err := json.MarshalIndent(b.res, "", "  ")
	if err != nil {
		return err
	}
	// Result files are committed: no local path may appear in them.
	bs = bytes.ReplaceAll(bs, []byte(b.scratch), []byte("$SCRATCH"))
	return os.WriteFile(filepath.Join(b.outDir, b.h.name()+".json"), append(bs, '\n'), 0o644)
}
