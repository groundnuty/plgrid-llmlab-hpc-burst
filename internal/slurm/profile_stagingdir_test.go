package slurm

// A Provider's own stagingDir at ReadProfile (HBEP-4, Provider settings and Core operations, ReadProfile; #757, #735).
// Every expected value is a literal written here: invented paths of the two forms the field takes, never read back from the
// code under test. A refusal names the fields and never a value: a literal cluster path carries a user name, so, like the
// account, it is account data.

import (
	"errors"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// uniqueSegment sits in the middle of the distinctive path of these rows: a refusal that echoed a truncated or joined form
// of the path would still hold it.
const uniqueSegment = "zq7f3akx"

// fieldError returns the refusal's entry for field, ok false when it names none.
func fieldError(err error, field string) (provider.FieldError, bool) {
	var inv *provider.InvalidProfileError
	if !errors.As(err, &inv) {
		return provider.FieldError{}, false
	}
	for _, f := range inv.Fields {
		if f.Field == field {
			return f, true
		}
	}
	return provider.FieldError{}, false
}

// The Provider's stagingDir wins over the profile's, and the profile stays as stored: nothing rewrites it, so a reader that
// takes the profile's field directly still sees the profile's own value (and fails the rows that name the Provider's).
func TestReadProfileProviderStagingDirWins(t *testing.T) {
	for _, tc := range []struct {
		name     string
		profile  string
		provider string
		want     string
	}{
		{"the Provider's path against the profile's variable", "$SCRATCH", "/invented/stage/path", "/invented/stage/path"},
		{"the Provider's variable against the profile's path", "/profile/literal/path", "$SCRATCH/prov", "$SCRATCH/prov"},
		{"a profile that gives none and a Provider that does", "", "/invented/stage/path", "/invented/stage/path"},
		{"a Provider that gives none", "$SCRATCH/stage", "", "$SCRATCH/stage"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newProfileRig(t)
			r.pp.Spec.StagingDir, r.prov.Spec.StagingDir = tc.profile, tc.provider
			v, err := r.read()
			if err != nil {
				t.Fatalf("ReadProfile = %v, want it accepted", err)
			}
			if got := provider.StagingDirOf(v.Profile, v.Settings); got != tc.want {
				t.Errorf("the staging directory is %q, want %q", got, tc.want)
			}
			if v.Settings.StagingDir != tc.provider {
				t.Errorf("Settings.StagingDir = %q, want the Provider's own %q", v.Settings.StagingDir, tc.provider)
			}
			if v.Profile.Spec.StagingDir != tc.profile {
				t.Errorf("the profile's stagingDir is %q, want it as stored, %q", v.Profile.Spec.StagingDir, tc.profile)
			}
		})
	}
}

// With neither field a batch Provider is refused. The refusal's field is spec.stagingDir, and its message names both fields.
func TestReadProfileRefusesWhenNeitherGivesAStagingDir(t *testing.T) {
	r := newProfileRig(t)
	r.pp.Spec.StagingDir, r.prov.Spec.StagingDir = "", ""
	_, err := r.read()
	f, ok := fieldError(err, "spec.stagingDir")
	if !ok {
		t.Fatalf("ReadProfile = %v, want spec.stagingDir named", err)
	}
	for _, name := range []string{"provider.spec.stagingDir", "PlatformProfile's spec.stagingDir"} {
		if !strings.Contains(f.Message, name) {
			t.Errorf("the message %q does not name %s", f.Message, name)
		}
	}
}

// The Provider's value is held to the form of the profile's CRD rule: no ".." segment, which agent.json refuses. The refusal
// names the field and never the value or any part of it. A ".." inside a longer segment is an ordinary name.
func TestReadProfileProviderStagingDirRefusesDotDot(t *testing.T) {
	// The subtest names carry no path: the rig's temporary directory is named after the test, and the credentials reader
	// holds the key file's path to letters, digits and ._:=,/+@-.
	for name, dir := range map[string]string{
		"in the middle":     "/invented-stage/" + uniqueSegment + "/../home",
		"after a variable":  "$SCRATCH/..",
		"last":              "/invented-stage/" + uniqueSegment + "/..",
		"the root's parent": "/..",
	} {
		t.Run("refuse "+name, func(t *testing.T) {
			r := newProfileRig(t) // the profile gives a valid one: the Provider's wins, so the Provider's is what is checked
			r.prov.Spec.StagingDir = dir
			_, err := r.read()
			f, ok := fieldError(err, "provider.spec.stagingDir")
			if !ok {
				t.Fatalf("ReadProfile = %v, want provider.spec.stagingDir named", err)
			}
			if !strings.Contains(f.Message, "'..'") {
				t.Errorf("the message %q does not say what is wrong", f.Message)
			}
			if strings.Contains(err.Error(), uniqueSegment) || strings.Contains(err.Error(), dir) {
				t.Errorf("the refusal echoes the value: %v", err)
			}
		})
	}
	for name, dir := range map[string]string{
		"dots inside a segment": "/invented/a..b/x",
		"a variable":            "$SCRATCH/hpcburst",
		"three dots":            "/invented/.../x",
		"dots at a segment end": "/invented/..a/b..",
	} {
		t.Run("accept "+name, func(t *testing.T) {
			r := newProfileRig(t)
			r.prov.Spec.StagingDir = dir
			if _, err := r.read(); err != nil {
				t.Errorf("ReadProfile = %v, want %q accepted", err, dir)
			}
		})
	}
}

// The shipped Sano profile gives no stagingDir. A Sano Provider gives it in its settings, and ReadProfile accepts it; a
// Sano Provider that gives none is refused (#757, #735).
func TestShippedSanoNeedsTheProvidersStagingDir(t *testing.T) {
	pp := readShippedSano(t)
	if pp.Spec.StagingDir != "" {
		t.Fatalf("the shipped Sano profile gives stagingDir %q: a cluster path never goes in a public profile", pp.Spec.StagingDir)
	}
	r := newProfileRig(t)
	r.pp = pp.DeepCopy()
	r.prov.Spec.PlatformProfileRef.Name = "sano"
	r.prov.Spec.AllowedPartitions = []string{"gpu"}

	r.prov.Spec.StagingDir = "/invented/stage/path"
	v, err := r.read()
	if err != nil {
		t.Fatalf("ReadProfile = %v, want the Sano Provider with its own stagingDir accepted", err)
	}
	if got := provider.StagingDirOf(v.Profile, v.Settings); got != "/invented/stage/path" {
		t.Errorf("the staging directory is %q, want /invented/stage/path", got)
	}

	r.prov.Spec.StagingDir = ""
	if _, err := r.read(); err == nil {
		t.Fatal("ReadProfile accepted a Sano Provider that gives no stagingDir")
	} else if _, ok := fieldError(err, "spec.stagingDir"); !ok {
		t.Errorf("ReadProfile = %v, want spec.stagingDir named", err)
	}
}
