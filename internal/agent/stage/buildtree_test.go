package stage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The build tree (APPTAINER_TMPDIR) is a directory of its own under TmpDir,
// removed as soon as Apptainer's run ends: when Image returns, before the agent
// starts the engine (it starts the worker only once staging is done), the
// tree is gone, so a RAM-disk TmpDir has its memory back before the engine
// loads its weights (science-agent ruling, 1 Nov 2026, issue #125). The script
// leaves its tree behind, as an Apptainer that is killed does; the run that
// fails is checked too.
func TestBuildTreeIsGoneWhenImageReturns(t *testing.T) {
	for _, tc := range []struct {
		name string
		exit string
	}{{"written", "printf 'fake-sif\\n' > \"$2\""}, {"failed", "exit 3"}} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, root := newStager(t, nil)
			rec := filepath.Join(root, "tmpdir")
			scriptApptainer(t, root, fmt.Sprintf(`echo "$APPTAINER_TMPDIR" > %s
mkdir -p "$APPTAINER_TMPDIR/build-temp-1/rootfs/usr" && printf x > "$APPTAINER_TMPDIR/build-temp-1/rootfs/usr/f"
%s
`, rec, tc.exit))
			_, err := s.Image(context.Background(), "docker.io/vllm/vllm-openai", digest, agentapi.ImageFormatSIF)
			if (err == nil) != (tc.name == "written") {
				t.Fatalf("Image err %v", err)
			}
			b, rerr := os.ReadFile(rec)
			if rerr != nil {
				t.Fatalf("the script did not run: %v", rerr)
			}
			tree := strings.TrimSpace(string(b))
			if filepath.Dir(tree) != root || tree == root {
				t.Errorf("APPTAINER_TMPDIR %q is not a directory of its own under TmpDir %q", tree, root)
			}
			if _, err := os.Stat(tree); !os.IsNotExist(err) {
				t.Errorf("the build tree %q is still there when Image returns (%v)", tree, err)
			}
			if left, _ := filepath.Glob(filepath.Join(root, "hb-apptainer-*")); len(left) != 0 {
				t.Errorf("left in TmpDir: %v", left)
			}
		})
	}
}

// A cold build reports its peak use of TmpDir, sampled while Apptainer runs
// (science-agent ruling, 1 Nov 2026, issue #125). The script raises the use to
// 5,000 bytes, waits until a sample has seen it (observable state, no fixed
// window), drops it to 1,000 and finishes: the peak is 5,000, not the last
// sample. The build hook runs before Apptainer starts. A cache hit builds
// nothing, samples nothing and reports no peak.
func TestBuildReportsItsPeakTmpUse(t *testing.T) {
	s, _, root := newStager(t, nil)
	used, seen, hooked, order := filepath.Join(root, "used"), filepath.Join(root, "seen"), filepath.Join(root, "hooked"), filepath.Join(root, "order")
	if err := os.WriteFile(used, []byte("100"), 0o600); err != nil {
		t.Fatal(err)
	}
	var samples atomic.Int32
	s.c.SampleEvery = 5 * time.Millisecond
	s.c.UsedSpace = func(p string) (int64, error) {
		samples.Add(1)
		if p != root {
			return 0, fmt.Errorf("sampled %q, want TmpDir %q", p, root)
		}
		b, err := os.ReadFile(used)
		if err != nil {
			return 0, err
		}
		n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		if err == nil && n == 5000 {
			os.WriteFile(seen, nil, 0o600)
		}
		return n, err
	}
	scriptApptainer(t, root, fmt.Sprintf(`[ -e %[1]s ] && echo hooked-first > %[2]s
echo 5000 > %[3]s
i=0; while [ ! -e %[4]s ] && [ $i -lt 2000 ]; do sleep 0.01; i=$((i+1)); done
echo 1000 > %[3]s
printf 'fake-sif\n' > "$2"
`, hooked, order, used, seen))
	var hookDir string
	ctx := agent.WithImageBuild(context.Background(), func(tmp string) {
		hookDir = tmp
		os.WriteFile(hooked, nil, 0o600)
	})
	res, err := s.Image(ctx, "docker.io/vllm/vllm-openai", digest, agentapi.ImageFormatSIF)
	if err != nil {
		t.Fatal(err)
	}
	if res.BuildPeakTmpBytes != 5000 {
		t.Errorf("peak %d, want 5000", res.BuildPeakTmpBytes)
	}
	if hookDir != root {
		t.Errorf("the build hook got %q, want TmpDir %q", hookDir, root)
	}
	if b, _ := os.ReadFile(order); strings.TrimSpace(string(b)) != "hooked-first" {
		t.Errorf("the build hook did not run before Apptainer started")
	}
	samples.Store(0)
	hookDir = ""
	res, err = s.Image(ctx, "docker.io/vllm/vllm-openai", digest, agentapi.ImageFormatSIF)
	if err != nil || res.BuildPeakTmpBytes != 0 || samples.Load() != 0 || hookDir != "" {
		t.Errorf("cache hit: peak %d, %d samples, hook %q, err %v; want no build", res.BuildPeakTmpBytes, samples.Load(), hookDir, err)
	}
}
