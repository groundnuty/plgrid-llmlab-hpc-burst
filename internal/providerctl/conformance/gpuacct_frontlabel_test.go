package conformance

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
)

// The accounting rows write their own text to post (gpuacct_test.go), so they name where the front ran themselves
// (issue #224): the label the script passes, on the line after the verdict. The label is written here, not in the
// code, so finding it in the text proves it went through.
func TestTheAccountingTextNamesWhereTheFrontRan(t *testing.T) {
	const label = "a cloud host, inside Cyfronet"
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
	if len(lines) < 3 || lines[0] != "gpu-accounting-athena: PASS (posts to #145)" || lines[1] != "- front ran on: "+label {
		t.Errorf("the text to post lacks the front line right after the verdict:\n%s", raw)
	}
}

// The first line of an accounting row's text to post names where it goes, as the owner reads it when posting: #145 alone
// (the science agent's ruling of 12 Nov 2026 on #145: #93, which the accounting rows named too, is closed and was about the
// hold lift). The line is written out here, so a target that comes back fails on the text that is posted and not only on the
// row's field. The Athena row's line is checked above; these are the other two.
func TestTheAccountingTextPostsToTheAlphaListAlone(t *testing.T) {
	t.Setenv(alpha.EnvFrontLabel, "a cloud host, inside Cyfronet")
	for _, row := range []string{"gpu-accounting-athena-now", "gpu-accounting-helios"} {
		out := filepath.Join(t.TempDir(), "out")
		if err := os.Mkdir(out, 0o700); err != nil {
			t.Fatal(err)
		}
		w, _ := dryAccountingWorld(t, row, "", out)
		gpuAccountingBody(t, w, row)
		raw, err := os.ReadFile(filepath.Join(out, alpha.PostsFile))
		if err != nil {
			t.Fatal(err)
		}
		if first, _, _ := strings.Cut(string(raw), "\n"); first != row+": PASS (posts to #145)" {
			t.Errorf("%s: the first line of the text to post is %q, want %q", row, first, row+": PASS (posts to #145)")
		}
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
		// The refusal names the roles that are allowed, so a label that is one of them is in it; nothing else may be.
		if label != "" && !slices.Contains(alpha.FrontRoles, label) && strings.Contains(rec.failed, label) {
			t.Errorf("label %q: the refusal repeats it: %s", label, rec.failed)
		}
	}
	t.Setenv(alpha.EnvFrontLabel, "a cloud host, outside Cyfronet's network")
	rec := &stopRecorder{TB: t}
	ends(func() { frontLabel(rec) })
	if rec.failed != "" {
		t.Errorf("a valid label was refused: %s", rec.failed)
	}
}
