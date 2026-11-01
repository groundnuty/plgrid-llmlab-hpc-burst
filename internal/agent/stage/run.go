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
	defer s.removeTree(cache)
	build, err := os.MkdirTemp(s.c.TmpDir, "hb-apptainer-tmp-")
	if err != nil {
		return 0, err
	}
	defer s.removeTree(build)
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
	peak, measured := stopSampling() // the last sample, before the deferred removals
	if !measured {
		s.logf("apptainer %s: no sample of the use of %s succeeded; the build's peak is unknown", args[0], s.c.TmpDir)
	}
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

// removeTree removes a temporary tree of a run and logs a failure: a tree
// left on a RAM-disk TmpDir keeps its memory until the job ends (PR #138
// review, Opus 5).
func (s *Stager) removeTree(dir string) {
	if err := removeAll(dir); err != nil {
		s.logf("removing %s: %v", dir, err)
	}
}

// logf writes one line to the log.
func (s *Stager) logf(format string, a ...any) {
	fmt.Fprintf(s.c.Log, "stage: "+format+"\n", a...)
}

// UsedSpace returns the bytes in use on the file system that holds path.
type UsedSpace func(path string) (int64, error)

// statfsUsed is UsedSpace from statfs(2) (usedBytes).
func statfsUsed(path string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return usedBytes(st), nil
}

// usedBytes is a file system's use from its statfs(2) counts: (blocks - free
// blocks) * block size, f_blocks, f_bfree and f_bsize.
func usedBytes(st syscall.Statfs_t) int64 {
	return int64(st.Blocks-st.Bfree) * int64(st.Bsize)
}

// sampler keeps the peak of a series of UsedSpace samples, and whether any
// succeeded.
type sampler struct {
	mu   sync.Mutex
	peak int64
	ok   bool
}

func (p *sampler) take(used UsedSpace, path string) {
	n, err := used(path)
	if err != nil {
		return // a failed sample counts for nothing
	}
	p.mu.Lock()
	p.peak = max(p.peak, n)
	p.ok = true
	p.mu.Unlock()
}

// start samples now and then every interval until the returned function is
// called; that function takes a last sample, waits for the sampling to stop
// and returns the peak and whether any sample succeeded. With none, the peak
// is 0, which the report leaves out: a measurement that failed is not
// reported as a use of 0 bytes (PR #138 review, Opus 6).
func (p *sampler) start(used UsedSpace, path string, every time.Duration) func() (int64, bool) {
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
	return func() (int64, bool) {
		close(done)
		<-stopped
		p.take(used, path)
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.peak, p.ok
	}
}
