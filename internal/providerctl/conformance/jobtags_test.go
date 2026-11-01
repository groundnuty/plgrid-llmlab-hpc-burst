package conformance

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// hack/live-checks.sh owns a conformance run's jobs by their tags, read from
// job-tags.txt. An Allocation's tag must be on disk before the Allocation can
// be submitted, so a run that is killed, or an sbatch reply that is lost,
// still leaves the script the names to look for.
func TestNewAllocRecordsItsJobTagBeforeAnySubmission(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	w := &world{t: t, p: newPrivate(t, dir), c: clientfake.NewClientBuilder().WithScheme(scheme).Build(), credDir: t.TempDir(), digest: strings.Repeat("a", 64)}
	w.s.partition, w.s.cpus, w.s.memory = "example-partition", 1, resource.MustParse("1Gi")

	a, b := w.newAlloc("first-allocation"), w.newAlloc("second-allocation-with-a-longer-name")

	path := filepath.Join(dir, jobTagsFile)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("no tag file: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("the tag file has mode %v, want 600", info.Mode().Perm())
	}
	raw, _ := os.ReadFile(path)
	got := strings.Fields(string(raw))
	want := []string{v1alpha1.JobTag(a.Namespace, a.Name, a.UID), v1alpha1.JobTag(b.Namespace, b.Name, b.UID)}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("tags %v, want %v", got, want)
	}
	// The script accepts a tag only in this shape, so a tag outside it would never be cancelled.
	shape := regexp.MustCompile(`^hb-[A-Za-z0-9._-]{1,60}$`)
	for _, tag := range got {
		if !shape.MatchString(tag) {
			t.Errorf("tag %q is not in the shape the script accepts", tag)
		}
	}
}
