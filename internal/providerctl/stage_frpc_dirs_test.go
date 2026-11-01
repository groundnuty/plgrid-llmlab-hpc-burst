package providerctl

// A path that cannot take a staged file (a directory is there) fails the install,
// and the submission stops before the claim. With plain "mv -f tmp dest" the
// install would move the verified temporary file inside the directory and exit 0:
// EnsureFile would report success, the claim and the sbatch would follow, and the
// job would start without its frpc or certificate (review of PR 157, B2). The
// install uses mv -fT, and the fake Slurm's mv behaves as GNU mv does
// (internal/slurm/fakeslurm, mv_real_test.go), so this test sees what a real
// login node would do.

import (
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

func TestADirectoryAtAStagedPathStopsTheSubmissionBeforeTheClaim(t *testing.T) {
	for _, tc := range []struct {
		name, file string
		rel        func(tag string) string
	}{
		{"frpc", "frpc", func(string) string { return "frpc/" + frpcX86Digest + "/frpc" }},
		{"the certificate", "dial-home.crt", func(tag string) string { return "alloc/" + tag + "/dial-home.crt" }},
		{"agent.json", "agent.json", func(tag string) string { return "alloc/" + tag + "/agent.json" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := newSlurm(clock, fakeslurm.Config{})
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", stageReal(t))
			a := e.newAlloc("alloc-a", 1)
			dir := realStaging + tc.rel(tagOf(a))
			if err := sl.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			e.run("alloc-a", 3)

			if n := sl.Count("sbatch"); n != 0 {
				t.Errorf("%d sbatch calls with a directory at the %s path, want 0", n, tc.name)
			}
			if got := e.get("alloc-a").Annotations[v1alpha1.AnnotationSubmission]; got != "" {
				t.Errorf("a submission record %q was written, want no claim", got)
			}
			// The directory is still a directory with nothing inside it, and no
			// temporary file of the install is left in the scratch space.
			if _, ok := sl.Dirs()[dir]; !ok {
				t.Errorf("the directory at the %s path is gone", tc.name)
			}
			for p := range sl.Files() {
				if strings.HasPrefix(p, dir+"/") {
					t.Errorf("file %s was moved inside the directory at the %s path", p, tc.name)
				}
				if strings.Contains(p, ".hbtmp.") {
					t.Errorf("temporary file %s was left behind", p)
				}
			}
			// The refusal names the file whose install failed.
			synced := ""
			for _, c := range e.get("alloc-a").Status.Conditions {
				if c.Type == "Synced" {
					synced = c.Message
				}
			}
			if want := "stage " + tc.file + ": install " + tc.file; !strings.Contains(synced, want) {
				t.Errorf("the Synced condition says %q, want it to contain %q", synced, want)
			}

			// The control: with the directory gone, the same Allocation submits once.
			if err := sl.Remove(dir); err != nil {
				t.Fatal(err)
			}
			e.run("alloc-a", 4)
			if n := sl.Count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls once the directory is gone, want 1", n)
			}
		})
	}
}
