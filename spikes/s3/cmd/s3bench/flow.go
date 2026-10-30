package main

import (
	"context"
	"errors"
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

// settle, sampleGap and numSamples are variables so a unit test can run a sample quickly.
var (
	settle     = 30 * time.Second
	sampleGap  = 5 * time.Second
	numSamples = 3
)

const minFreeGiB = 3.0 // the shared VM's rule: wait while the root disk has less than this free

func (b *bench) logf(f string, a ...any) {
	fmt.Fprintf(os.Stderr, "[%s %s] %s\n", time.Now().UTC().Format("15:04:05"), b.h.name(), fmt.Sprintf(f, a...))
}

// gate waits until the shared VM has room: 1-minute load <= 4.0, >= 8 GiB of memory
// available and >= 3 GiB free on the disk that holds the scratch directory.
func (b *bench) gate(ctx context.Context, why string) error {
	for waited := time.Duration(0); ; waited += 30 * time.Second {
		l, m, d := load1(), memAvailGiB(), freeGiB(b.scratch)
		if l <= 4.0 && m >= 8 && d >= minFreeGiB {
			return nil
		}
		if waited > 40*time.Minute {
			return fmt.Errorf("VM busy for 40 min before %s (load1 %.1f, avail %.1f GiB, disk free %.1f GiB)", why, l, m, d)
		}
		b.logf("waiting before %s: load1 %.1f avail %.1f GiB, disk free %.1f GiB", why, l, m, d)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(30 * time.Second):
		}
	}
}

// portsFree fails if any port is already taken on the loopback address.
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

// checkOwned requires every expected port to have a LISTEN socket held by the launched
// processes (owned). A port that is listening but not in owned belongs to someone else and
// would let a foreign server answer a readiness probe; an empty owned set proves nothing
// started, so it is a failure too.
func checkOwned(expected []int, owned, all []listener) error {
	if len(owned) == 0 {
		return errors.New("the launched processes hold no listening socket")
	}
	have := map[int]bool{}
	for _, l := range owned {
		have[l.Port] = true
	}
	foreign := map[int]bool{}
	for _, l := range all {
		foreign[l.Port] = true
	}
	var missing, other []int
	for _, p := range expected {
		switch {
		case have[p]:
		case foreign[p]:
			other = append(other, p)
		default:
			missing = append(missing, p)
		}
	}
	if len(other) > 0 {
		return fmt.Errorf("ports %v are listening but held by another process, not by the launched ones", other)
	}
	if len(missing) > 0 {
		return fmt.Errorf("expected listeners on ports %v are missing", missing)
	}
	return nil
}

// verifyHost checks a started host: every launched process is alive, every expected port is
// held by the launched processes, and nothing listens beyond loopback. It returns the
// listeners it found.
func verifyHost(r *running) ([]string, error) {
	for _, p := range r.procs {
		if !p.alive() {
			return nil, fmt.Errorf("%s has exited (see its log)", p.name)
		}
	}
	owned := listeners(r.pids())
	if err := checkOwned(r.hostPorts, owned, allListeners()); err != nil {
		return nil, err
	}
	var out, bad []string
	for _, l := range owned {
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

// startReady starts the host and returns when it is ready and verified. Before the launch the
// host's ports must be free; after readiness every expected port must be held by the processes
// started here, so a probe cannot be answered by a foreign server. For kcp the readiness probe
// uses the "base" context, which serves /readyz at the server root.
func (b *bench) startReady(ctx context.Context, ver, dir, tag string) (*running, timing, error) {
	admit := b.admit
	if admit == nil {
		admit = b.gate
	}
	if err := admit(ctx, "start of "+tag); err != nil {
		return nil, timing{}, err
	}
	// Checked after the wait for a quiet VM: another process may have taken a port meanwhile.
	pl := b.h.ports()
	if err := portsFree(append(append([]int(nil), pl.Listen...), pl.Reserved...)); err != nil {
		return nil, timing{}, fmt.Errorf("before launching %s: %w", tag, err)
	}
	tag = b.h.name() + "-" + tag // logs of different options must not share a file
	load := load1()
	r, err := b.h.start(ver, dir, tag)
	if err != nil {
		return nil, timing{}, err
	}
	r.hostPorts = pl.Listen
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
	ls, err := verifyHost(r)
	if err != nil {
		return r, t, fmt.Errorf("%s: %w", tag, err)
	}
	if len(b.res.Listeners) == 0 {
		b.res.Listeners = ls
	}
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

// sample takes numSamples RSS readings of the host (and of the controller process, if any),
// sampleGap apart, after an initial settle. It returns the sample with the median host RSS.
// A sample is rejected, with an error, when a process of the host or the controller has
// exited or its /proc data cannot be read, at any of the readings.
func (b *bench) sample(ctx context.Context, r *running, ctrl *proc, wait time.Duration) (*rssSample, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
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
		for _, p := range r.procs {
			if !p.alive() {
				return nil, fmt.Errorf("sample %d rejected: %s has exited", i+1, p.name)
			}
		}
		h, by, err := rssOf(r.pids())
		if err != nil {
			return nil, fmt.Errorf("sample %d rejected: %w", i+1, err)
		}
		var c float64
		if ctrl != nil {
			if !ctrl.alive() {
				return nil, fmt.Errorf("sample %d rejected: the controller process (%s) has exited", i+1, ctrl.name)
			}
			if c, _, err = rssOf([]int{ctrl.pid()}); err != nil {
				return nil, fmt.Errorf("sample %d rejected: controller: %w", i+1, err)
			}
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
	return s, nil
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
