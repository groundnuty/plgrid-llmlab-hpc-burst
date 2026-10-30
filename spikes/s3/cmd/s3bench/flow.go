package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"k8s.io/client-go/rest"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s3/kit"
)

const (
	settle     = 30 * time.Second
	sampleGap  = 5 * time.Second
	numSamples = 3
)

func (b *bench) logf(f string, a ...any) {
	fmt.Fprintf(os.Stderr, "[%s %s] %s\n", time.Now().UTC().Format("15:04:05"), b.h.name(), fmt.Sprintf(f, a...))
}

// gate waits until the shared VM has room: 1-minute load <= 4.0 and >= 8 GiB available.
func (b *bench) gate(ctx context.Context, why string) error {
	for waited := time.Duration(0); ; waited += 30 * time.Second {
		l, m := load1(), memAvailGiB()
		if l <= 4.0 && m >= 8 {
			return nil
		}
		if waited > 40*time.Minute {
			return fmt.Errorf("VM busy for 40 min before %s (load1 %.1f, avail %.1f GiB)", why, l, m)
		}
		b.logf("waiting before %s: load1 %.1f avail %.1f GiB", why, l, m)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(30 * time.Second):
		}
	}
}

func portsFree(ports []int) error {
	for _, p := range ports {
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
		if err != nil {
			return fmt.Errorf("port %d is not free: %w", p, err)
		}
		l.Close()
	}
	return nil
}

// waitReadyz polls an authenticated /readyz until it answers 200.
func waitReadyz(ctx context.Context, kubeconfig, ctxName string, procs []*proc, timeout time.Duration) error {
	end := time.Now().Add(timeout)
	for time.Now().Before(end) {
		for _, p := range procs {
			if !p.alive() {
				return fmt.Errorf("%s exited before it was ready (see its log)", p.name)
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, err := os.Stat(kubeconfig); err == nil {
			if rc, err := restConfig(kubeconfig, ctxName); err == nil {
				if hc, err := rest.HTTPClientFor(rc); err == nil {
					hc.Timeout = 1500 * time.Millisecond
					if resp, err := hc.Get(strings.TrimRight(rc.Host, "/") + "/readyz"); err == nil {
						resp.Body.Close()
						if resp.StatusCode == http.StatusOK {
							return nil
						}
					}
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("not ready after %s", timeout)
}

// startReady starts the host and returns when it is ready. For kcp the readiness probe
// uses the "base" context, which serves /readyz at the server root.
func (b *bench) startReady(ctx context.Context, ver, dir, tag string) (*running, timing, error) {
	if err := b.gate(ctx, "start of "+tag); err != nil {
		return nil, timing{}, err
	}
	tag = b.h.name() + "-" + tag // logs of different options must not share a file
	load := load1()
	r, err := b.h.start(ver, dir, tag)
	if err != nil {
		return nil, timing{}, err
	}
	b.live = append(b.live, r)
	readyCtx := r.ctxName
	if b.h.name() == "kcp" {
		readyCtx = "base"
	}
	t := timing{Load1: load}
	if err := waitReadyz(ctx, r.admin, readyCtx, r.procs, 120*time.Second); err != nil {
		return r, t, err
	}
	t.ReadyzMS = time.Since(r.t0).Milliseconds()
	if r.afterReady != nil {
		if err := r.afterReady(ctx); err != nil {
			return r, t, err
		}
	}
	t.ReadyMS = time.Since(r.t0).Milliseconds()
	return r, t, nil
}

// usable makes the host usable for the suite and returns the kubeconfig the e2e binary gets:
// kcp child workspace if needed, then the CRDs are installed and served.
func (b *bench) usable(ctx context.Context, r *running) (string, error) {
	kc := r.admin
	if r.prepare != nil {
		var err error
		if kc, err = r.prepare(ctx); err != nil {
			return "", err
		}
	}
	c, err := newClient(kc, r.ctxName)
	if err != nil {
		return "", err
	}
	cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	return kc, kit.InstallCRDs(cctx, c)
}

// checkListeners fails if any host process listens on a non-loopback address.
func (b *bench) checkListeners(r *running) ([]string, error) {
	var out []string
	var bad []string
	for _, l := range listeners(r.pids()) {
		s := fmt.Sprintf("%s:%d", l.Addr, l.Port)
		out = append(out, s)
		if !loopback(l) {
			bad = append(bad, s)
		}
	}
	if len(bad) > 0 {
		return out, fmt.Errorf("host listens on non-loopback addresses %v", bad)
	}
	return out, nil
}

// sample takes numSamples RSS readings of the host (and of the controller process, if any),
// sampleGap apart, after an initial settle. Returns the sample with the median host RSS.
func (b *bench) sample(ctx context.Context, r *running, ctrl *proc, wait time.Duration) *rssSample {
	select {
	case <-ctx.Done():
	case <-time.After(wait):
	}
	s := &rssSample{ByProc: map[string]float64{}}
	type one struct {
		host, ctrl float64
		by         map[string]float64
	}
	var all []one
	for i := 0; i < numSamples; i++ {
		if i > 0 {
			time.Sleep(sampleGap)
		}
		h, by := rss(r.pids())
		var c float64
		if ctrl != nil && ctrl.alive() {
			c, _ = rss([]int{ctrl.pid()})
		}
		all = append(all, one{h, c, by})
		s.MiB = append(s.MiB, round1(h))
		s.Load1 = append(s.Load1, load1())
		s.MemAvailGiB = append(s.MemAvailGiB, round1(memAvailGiB()))
	}
	sort.Slice(all, func(i, j int) bool { return all[i].host < all[j].host })
	m := all[len(all)/2]
	s.MedianMiB, s.ControllerMiB = round1(m.host), round1(m.ctrl)
	for k, v := range m.by {
		s.ByProc[k] = round1(v)
	}
	return s
}

func round1(f float64) float64 { return float64(int(f*10+0.5)) / 10 }

func dirMiB(path string) float64 {
	var n int64
	_ = filepath.Walk(path, func(_ string, fi os.FileInfo, err error) error {
		if err == nil && fi.Mode().IsRegular() {
			n += fi.Size()
		}
		return nil
	})
	return round1(float64(n) / 1048576)
}
