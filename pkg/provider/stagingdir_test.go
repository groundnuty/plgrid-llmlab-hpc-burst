package provider

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// StagingDirOf is the one place that picks the staging directory (HBEP-4, Provider settings; #757): the Provider's own
// stagingDir wins, else the profile's, else "". The values are invented literals of the two forms the field takes.
func TestStagingDirOf(t *testing.T) {
	profile := func(dir string) Profile {
		return Profile{Spec: v1alpha1.PlatformProfileSpec{StagingDir: dir}}
	}
	for _, tc := range []struct {
		name     string
		profile  string
		provider string
		want     string
	}{
		{"the Provider's wins over the profile's", "$SCRATCH", "/invented/stage/path", "/invented/stage/path"},
		{"a Provider's variable form wins over a profile's path", "/profile/literal", "$SCRATCH/prov", "$SCRATCH/prov"},
		{"the profile's when the Provider gives none", "$SCRATCH", "", "$SCRATCH"},
		{"the Provider's when the profile gives none", "", "/invented/stage/path", "/invented/stage/path"},
		{"neither", "", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := StagingDirOf(profile(tc.profile), Settings{StagingDir: tc.provider}); got != tc.want {
				t.Errorf("StagingDirOf(profile %q, Provider %q) = %q, want %q", tc.profile, tc.provider, got, tc.want)
			}
		})
	}
}
