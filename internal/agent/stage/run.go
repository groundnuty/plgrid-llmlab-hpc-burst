package stage

import (
	"context"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
)

// runApptainer runs the apptainer binary with args in a process group of its
// own and returns once every member of the group is gone (PR #87 review, Astra
// B3, Opus B4: exec.CommandContext killed Apptainer alone, and a helper it had
// started, the squashfs builder for one, went on writing into staging).
//
// On ctx cancel the group gets TERM, then KILL after StopGrace. A leader that
// exits and leaves a member behind gives that member StopGrace to finish too.
// The caller removes its temporary output only after this returns, so nothing
// still writes to it.
//
// Apptainer's layer cache is a directory of its own under TmpDir for this one
// run, removed with it: the OCI layers of a pull are not needed once the image
// is written, and kept beside the images they pile up (Opus nit). So is its
// build tree (APPTAINER_TMPDIR): it is removed as soon as the run ends,
// written or failed, before the image is recorded and long before the engine
// starts, so a RAM-disk TmpDir gives its memory back before the engine loads
// its weights; Apptainer itself leaves the tree behind when it is killed
// (science-agent ruling, 1 Nov 2026, issue #125; HBEP-4).
//
// While it runs, the use of TmpDir is sampled with UsedSpace every
// SampleEvery, and once more when the run ends, before the tree is removed:
// the peak is returned (agentapi.Staging.BuildPeakTmpBytes). statfs counts a
// whole file system in one call, which on a per-job tmpfs ($MEMFS) is the
// job's own use, with no walk of a tree that changes as it is read; a spike
// shorter than the interval can be missed.
func (s *Stager) runApptainer(ctx context.Context, args []string) (int64, error) {
	cache, err := os.MkdirTemp(s.c.TmpDir, "hb-apptainer-cache-")
	if err != nil {
		return 0, err
	}
	defer removeAll(cache)
	build, err := os.MkdirTemp(s.c.TmpDir, "hb-apptainer-tmp-")
	if err != nil {
		return 0, err
	}
	defer removeAll(build)
	var sampled sampler
	stopSampling := sampled.start(s.c.UsedSpace, s.c.TmpDir, s.c.SampleEvery)
	g, err := procgroup.Start(procgroup.Spec{
		Path: s.c.Apptainer,
		Args: args,
		Env: append(append([]string(nil), s.c.BaseEnv...),
			"APPTAINER_CACHEDIR="+cache,
			"APPTAINER_TMPDIR="+build,
		),
		Stdout: s.c.Log,
		Stderr: s.c.Log,
	}, time.Now())
	if err != nil {
		stopSampling()
		return 0, err
	}
	stop := func() {
		g.Stop(s.c.StopGrace)
		<-g.Done()
	}
	select {
	case <-g.Exited():
		select {
		case <-g.Done():
		case <-ctx.Done():
			stop()
		case <-time.After(s.c.StopGrace):
			s.logf("apptainer %s: the process left members of its group running; stopping them", args[0])
			stop()
		}
	case <-ctx.Done():
		stop()
	}
	peak := stopSampling() // the last sample, before the deferred removals
	if code := g.ExitCode(); code != 0 {
		if err := ctx.Err(); err != nil {
			return peak, fmt.Errorf("apptainer %s: %w", args[0], err)
		}
		if code == procgroup.KilledExitCode {
			return peak, fmt.Errorf("apptainer %s: ended by a signal", args[0])
		}
		return peak, fmt.Errorf("apptainer %s: exit code %d", args[0], code)
	}
	return peak, nil
}

// logf writes one line to the log.
func (s *Stager) logf(format string, a ...any) {
	fmt.Fprintf(s.c.Log, "stage: "+format+"\n", a...)
}

// UsedSpace returns the bytes in use on the file system that holds path.
type UsedSpace func(path string) (int64, error)

// statfsUsed is UsedSpace from statfs(2): (blocks - free blocks) * block size.
func statfsUsed(path string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return int64(st.Blocks-st.Bfree) * int64(st.Bsize), nil
}

// sampler keeps the peak of a series of UsedSpace samples.
type sampler struct {
	mu   sync.Mutex
	peak int64
}

func (p *sampler) take(used UsedSpace, path string) {
	n, err := used(path)
	if err != nil {
		return // a failed sample counts for nothing
	}
	p.mu.Lock()
	p.peak = max(p.peak, n)
	p.mu.Unlock()
}

// start samples now and then every interval until the returned function is
// called; that function takes a last sample, waits for the sampling to stop
// and returns the peak.
func (p *sampler) start(used UsedSpace, path string, every time.Duration) func() int64 {
	p.take(used, path)
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				p.take(used, path)
			}
		}
	}()
	return func() int64 {
		close(done)
		<-stopped
		p.take(used, path)
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.peak
	}
}
