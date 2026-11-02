package conformance

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
)

// The accounting rows write their own text to post (gpuacct_test.go), so they name where the front ran themselves
// (issue #224): the label the script passes, on the line after the verdict. The label is written here, not in the
// code, so finding it in the text proves it went through.
func TestTheAccountingTextNamesWhereTheFrontRan(t *testing.T) {
	const label = "an example host (inside the network)"
	t.Setenv(alpha.EnvFrontLabel, label)
	out := filepath.Join(t.TempDir(), "out")
	if err := os.Mkdir(out, 0o700); err != nil {
		t.Fatal(err)
	}
	w, _ := dryAccountingWorld(t, "gpu-accounting-athena", "", out)
	gpuAccountingBody(t, w, "gpu-accounting-athena")
	raw, err := os.ReadFile(filepath.Join(out, alpha.PostsFile))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(raw), "\n")
	if len(lines) < 3 || !strings.HasPrefix(lines[0], "gpu-accounting-athena: PASS") || lines[1] != "- front ran on: "+label {
		t.Errorf("the text to post lacks the front line right after the verdict:\n%s", raw)
	}
}

// A run with no usable label is refused, naming the variable and not repeating its value.
func TestAnAccountingRunWithoutAFrontLabelIsRefused(t *testing.T) {
	for _, label := range []string{"", "the development host", "dev1.example.org, outside the network"} {
		t.Setenv(alpha.EnvFrontLabel, label)
		rec := &stopRecorder{TB: t}
		ends(func() { frontLabel(rec) })
		if !strings.Contains(rec.failed, alpha.EnvFrontLabel) {
			t.Errorf("label %q: failure %q, want one naming %s", label, rec.failed, alpha.EnvFrontLabel)
		}
		if label != "" && strings.Contains(rec.failed, label) {
			t.Errorf("label %q: the refusal repeats it: %s", label, rec.failed)
		}
	}
	t.Setenv(alpha.EnvFrontLabel, "a host (outside the network)")
	rec := &stopRecorder{TB: t}
	ends(func() { frontLabel(rec) })
	if rec.failed != "" {
		t.Errorf("a valid label was refused: %s", rec.failed)
	}
}
