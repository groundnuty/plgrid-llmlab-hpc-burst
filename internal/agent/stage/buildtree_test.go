package stage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
