package providerctl

import (
	"context"
	"io"
	"strings"
	"sync"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// lineFaults is a Runner over the fake Slurm session that fails the commands
// whose line starts with a prefix, as often as armed. fakeslurm's FailNext
// fails whatever command comes next; these tests need one kind of command to
// fail while the others run (a failed scancel, an accounting outage).
type lineFaults struct {
	inner slurm.Runner

	mu    sync.Mutex
	rules []*lineRule
	hits  int
}

type lineRule struct {
	prefix string
	left   int // < 0: every time
	err    error
}

// fail makes the next n commands starting with prefix fail with err without
// running; n < 0 fails all of them until clear.
func (f *lineFaults) fail(prefix string, n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rules = append(f.rules, &lineRule{prefix: prefix, left: n, err: err})
}

func (f *lineFaults) clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rules = nil
}

// failed is how many commands the rules failed.
func (f *lineFaults) failed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits
}

func (f *lineFaults) Run(ctx context.Context, c slurm.Command) (slurm.Result, error) {
	f.mu.Lock()
	for _, r := range f.rules {
		if r.left != 0 && strings.HasPrefix(c.Line, r.prefix) {
			if r.left > 0 {
				r.left--
			}
			f.hits++
			f.mu.Unlock()
			return slurm.Result{}, r.err
		}
	}
	f.mu.Unlock()
	return f.inner.Run(ctx, c)
}

func (f *lineFaults) Generation() uint64 { return f.inner.Generation() }

// withLineFaults puts f between the Batch and the fake Slurm session.
func withLineFaults(f *lineFaults) realOption {
	return func(e *realEnv) {
		f.inner = e.session
		e.opts.NewPlatform = func(v slurm.Validated) (Platform, io.Closer, error) {
			e.batch = slurm.NewBatch(f, v.Profile, v.Settings, slurm.Options{Clock: e.clock})
			return e.batch, io.NopCloser(nil), nil
		}
	}
}
