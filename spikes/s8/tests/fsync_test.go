package tests

import (
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/evlayer"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/jsnode"
)

// countSyncCalls runs a host under strace, publishes n events one after another
// and returns how many fsync/fdatasync calls the whole host process made. The
// host is stopped through its stdin so the trace file is complete.
func countSyncCalls(t *testing.T, mode jsnode.SyncMode, n int) int {
	t.Helper()
	dir := scratchDir(t, "fsync-"+string(mode))
	trace := dir + "/strace.txt"
	h, url := startHost(t, dir+"/js", mode, "strace", "-f", "-qq", "-e", "trace=fsync,fdatasync", "-o", trace)
	js := connect(t, url)
	ctx := ctxT(t, 120*time.Second)
	if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName}); err != nil {
		t.Fatal(err)
	}
	publishSeq(ctx, t, js, "f", n, 3)
	h.Stop(30 * time.Second)
	b, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Count(string(b), "fsync(") + strings.Count(string(b), "fdatasync(")
}

// Evidence about the cause, not only the outcome: a SIGKILL cannot tell the two
// policies apart (the kernel keeps what was written), so count the fsync calls.
func TestFsyncCallsPerAcknowledgedPublish(t *testing.T) {
	if _, err := exec.LookPath("strace"); err != nil {
		t.Skip("strace not installed: fsync calls not counted (result: unknown)")
	}
	capacity(t)
	const n = 200
	for _, mode := range []jsnode.SyncMode{jsnode.SyncAlways, jsnode.SyncDefault} {
		base := countSyncCalls(t, mode, 0)
		with := countSyncCalls(t, mode, n)
		per := with - base
		t.Logf("RESULT mode=%s: fsync+fdatasync calls: %d with 0 publishes, %d with %d publishes => %d for the publishes", mode, base, with, n, per)
		switch mode {
		case jsnode.SyncAlways:
			if per < n {
				t.Errorf("sync_interval: always made %d sync calls for %d acknowledged publishes, want at least %d", per, n, n)
			}
		case jsnode.SyncDefault:
			if per > n/10 {
				t.Errorf("default policy made %d sync calls for %d publishes, want a handful at most", per, n)
			}
		}
	}
}
