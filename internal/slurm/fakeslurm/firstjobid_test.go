package fakeslurm

// New. Config.FirstJobID numbers a Platform's jobs apart from another's.

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// The first two sbatch jobs of a Platform with FirstJobID 7000000 are
// 7000000 and 7000001; the zero value keeps the first ID of 1001 (the
// default of every other test of this package), and 1002 follows it.
func TestFirstJobID(t *testing.T) {
	for _, c := range []struct {
		name       string
		first      int
		want, next int
	}{
		{"set", 7000000, 7000000, 7000001},
		{"zero keeps the default", 0, 1001, 1002},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newRig(t, Config{FirstJobID: c.first})
			if got, next := r.sbatch(), r.sbatch(); got != c.want || next != c.next {
				t.Errorf("the first two jobs are %d and %d, want %d and %d", got, next, c.want, c.next)
			}
		})
	}
}

// A negative FirstJobID is a test bug, refused as New refuses every Config it
// cannot use.
func TestFirstJobIDNegativePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Errorf("New accepted a negative FirstJobID")
		}
	}()
	New(fake.NewClock(t0), Config{FirstJobID: -1})
}
