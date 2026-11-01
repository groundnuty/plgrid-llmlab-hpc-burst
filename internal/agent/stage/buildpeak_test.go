package stage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// A build that fails still returns its peak TMPDIR use beside its error (PR
// #138 review, Astra B4): the script raises the use to 7,000 bytes, waits
// until a sample has seen it (observable state, no fixed window), and fails.
func TestFailedBuildReturnsItsPeak(t *testing.T) {
	s, _, root := newStager(t, nil)
	used, seen := filepath.Join(root, "used"), filepath.Join(root, "seen")
	if err := os.WriteFile(used, []byte("100"), 0o600); err != nil {
		t.Fatal(err)
	}
	s.c.SampleEvery = 5 * time.Millisecond
	s.c.UsedSpace = func(string) (int64, error) {
		b, err := os.ReadFile(used)
		if err != nil {
			return 0, err
		}
		n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		if err == nil && n == 7000 {
			os.WriteFile(seen, nil, 0o600)
		}
		return n, err
	}
	scriptApptainer(t, root, fmt.Sprintf(`echo 7000 > %[1]s
i=0; while [ ! -e %[2]s ] && [ $i -lt 2000 ]; do sleep 0.01; i=$((i+1)); done
exit 3
`, used, seen))
	res, err := s.Image(context.Background(), "docker.io/vllm/vllm-openai", digest, agentapi.ImageFormatSIF)
	if err == nil {
		t.Fatal("the build succeeded")
	}
	if res.BuildPeakTmpBytes != 7000 {
		t.Errorf("failed build: peak %d, want 7000", res.BuildPeakTmpBytes)
	}
}

// A build whose every sample failed reports no peak, so the report leaves the
// field out: a failed measurement is not a use of 0 bytes, nor whatever the
// failed call returned (Opus 6). The log says the peak is unknown.
func TestBuildWithNoGoodSampleReportsNoPeak(t *testing.T) {
	s, _, root := newStager(t, nil)
	log := &lockedBuffer{}
	s.c.Log = log
	s.c.SampleEvery = 5 * time.Millisecond
	s.c.UsedSpace = func(string) (int64, error) { return 4096, errors.New("statfs: input/output error") }
	scriptApptainer(t, root, `printf 'fake-sif\n' > "$2"`)
	res, err := s.Image(context.Background(), "docker.io/vllm/vllm-openai", digest, agentapi.ImageFormatSIF)
	if err != nil {
		t.Fatal(err)
	}
	if res.BuildPeakTmpBytes != 0 {
		t.Errorf("peak %d, want none", res.BuildPeakTmpBytes)
	}
	if !strings.Contains(log.String(), "the build's peak is unknown") {
		t.Errorf("log %q does not say the peak is unknown", log.String())
	}
}

// usedBytes is (f_blocks - f_bfree) * f_bsize, checked against counts worked
// out by hand (PR #138 review, Astra nit: a constant would have passed). The
// available count differs from the free one, so f_bavail in its place fails.
func TestUsedBytesFromStatfsCounts(t *testing.T) {
	for _, tc := range []struct {
		blocks, bfree, bavail uint64
		bsize                 int64
		want                  int64
	}{
		{1000, 250, 200, 4096, 3_072_000},                 // 750 * 4096
		{2_097_152, 0, 0, 4096, 8_589_934_592},            // a full 8 GiB tmpfs
		{6_291_456, 6_291_456, 6_000_000, 4096, 0},        // an empty one
		{1_000_000, 999_999, 900_000, 1_048_576, 1048576}, // one 1 MiB block
	} {
		st := syscall.Statfs_t{Blocks: tc.blocks, Bfree: tc.bfree, Bavail: tc.bavail, Bsize: tc.bsize, Frsize: tc.bsize}
		if got := usedBytes(st); got != tc.want {
			t.Errorf("blocks %d, free %d, size %d: %d bytes, want %d", tc.blocks, tc.bfree, tc.bsize, got, tc.want)
		}
	}
}

// lockedBuffer is a log sink that Apptainer's output and the stager's notices
// may write at once.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// A temporary tree of a run that cannot be removed is logged, not dropped: on
// a RAM-disk TmpDir it keeps its memory until the job ends (PR #138 review,
// Opus 5). The script takes the write permission off TmpDir, so neither the
// layer cache nor the build tree can be removed from it.
func TestUnremovableRunTreeIsLogged(t *testing.T) {
	s, _, root := newStager(t, nil)
	log := &lockedBuffer{}
	s.c.Log = log
	t.Cleanup(func() { os.Chmod(root, 0o700) })
	scriptApptainer(t, root, fmt.Sprintf("printf 'fake-sif\\n' > \"$2\" && chmod 500 %s\n", root))
	_, err := s.Image(context.Background(), "docker.io/vllm/vllm-openai", digest, agentapi.ImageFormatSIF)
	if cerr := os.Chmod(root, 0o700); cerr != nil {
		t.Fatal(cerr)
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"hb-apptainer-cache-", "hb-apptainer-tmp-"} {
		if !strings.Contains(log.String(), "stage: removing "+filepath.Join(root, prefix)) {
			t.Errorf("no log line for the %s tree that was left: %q", prefix, log.String())
		}
	}
}
