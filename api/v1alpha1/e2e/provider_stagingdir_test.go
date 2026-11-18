package e2e

import (
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// A Provider's stagingDir (HBEP-4, Provider settings; #757, #735) has the form of the profile's: an absolute path, or
// one that starts with an environment variable. The pattern and the bounds are the profile field's, copied. The Provider's
// field has the profile's CEL rule too, in the same words (TestProviderStagingDirRefusesDotDotSegments), and ReadProfile refuses
// a ".." segment again (internal/slurm TestReadProfileProviderStagingDirRefusesDotDot), since a stored object is never trusted.
func TestProviderStagingDirForm(t *testing.T) {
	for _, dir := range []string{
		"relative/x", "$1x", "~/hb", "/scratch/h b", "/scratch/$(id)", "$", "$SCRATCH;x", "${SCRATCH}/x",
		"/" + strings.Repeat("a", 1024), // 1025 characters
	} {
		t.Run("refuse "+dir[:min(len(dir), 24)], func(t *testing.T) {
			p := newProvider(uniq("prov"))
			p.Spec.StagingDir = dir
			requireRefused(t, c.Create(bg(), p), "stagingDir")
		})
	}
	for _, dir := range []string{
		"$SCRATCH/x", "/abs/x", "$SCRATCH", "/net/scratch/example-user",
		"/" + strings.Repeat("a", 1023), // exactly 1024 characters
	} {
		t.Run("accept "+dir[:min(len(dir), 24)], func(t *testing.T) {
			p := newProvider(uniq("prov"))
			p.Spec.StagingDir = dir
			must(t, c.Create(bg(), p), "create a Provider with stagingDir "+dir)
			cleanup(t, p)
			var got v1alpha1.Provider
			must(t, c.Get(bg(), client.ObjectKeyFromObject(p), &got), "read back")
			if got.Spec.StagingDir != dir {
				t.Errorf("stagingDir read back as %q, want %q", got.Spec.StagingDir, dir)
			}
		})
	}
}

// An absent stagingDir stays absent: the profile's applies, and ReadProfile requires one of the two.
func TestProviderStagingDirIsOptional(t *testing.T) {
	none := newProvider(uniq("prov"))
	must(t, c.Create(bg(), none), "create a Provider without a stagingDir")
	cleanup(t, none)
	var got v1alpha1.Provider
	must(t, c.Get(bg(), client.ObjectKeyFromObject(none), &got), "read back")
	if got.Spec.StagingDir != "" {
		t.Errorf("an absent stagingDir read back as %q", got.Spec.StagingDir)
	}
}

// A Provider's stagingDir names no ".." segment, because the agent's configuration refuses one (agentapi.AgentConfig): the
// rule of the profile's field, in the same words, and the same rows as TestStagingDirRefusesDotDotSegments. A ".." inside a
// longer segment is an ordinary name. This is the rule providers:$.spec.stagingDir#0.
func TestProviderStagingDirRefusesDotDotSegments(t *testing.T) {
	const msg = "stagingDir must not contain a '..' segment"
	for _, dir := range []string{"/scratch/../etc", "$SCRATCH/..", "/scratch/a/../b", "/..", "/x/../y"} {
		t.Run("refuse "+dir, func(t *testing.T) {
			p := newProvider(uniq("prov"))
			p.Spec.StagingDir = dir
			requireRefused(t, c.Create(bg(), p), msg)
		})
	}
	for _, dir := range []string{"/scratch/a..b/x", "$SCRATCH/hpcburst", "/scratch/.../x", "/scratch/..a/b..", "/x/..y"} {
		t.Run("accept "+dir, func(t *testing.T) {
			p := newProvider(uniq("prov"))
			p.Spec.StagingDir = dir
			must(t, c.Create(bg(), p), "create with stagingDir "+dir)
			cleanup(t, p)
		})
	}
}
