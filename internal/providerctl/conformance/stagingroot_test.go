package conformance

// The live conformance check's staging root (issue #735's checklist, "the live conformance check's staging root"; #757): the root the run's
// staging directory is below comes from the Provider's own settings when the shipped profile has no stagingDir, as Sano's has none (a cluster
// path carries a user name). Before, loadProfile read the profile's stagingDir alone and refused a profile without one, so no conformance run
// could start on Sano. The expected values are literals of the test's own, never read back from the code that checks them.

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

const (
	exampleRoot = "/path/to/shared/example-stage"
	exampleRun  = exampleRoot + "/hpc-burst-conformance/run-0123456789ab"
)

// sanoFull is a complete set of settings for the shipped Sano gpu partition, one A100, with the Provider's own staging root.
func sanoFull() map[string]string {
	m := full()
	m["HPCBURST_CONF_PROFILE"], m["HPCBURST_CONF_PARTITION"] = "sano", "gpu"
	m["HPCBURST_CONF_GPUS"], m["HPCBURST_CONF_CPUS"], m["HPCBURST_CONF_MEMORY"] = "1", "4", "65536Mi"
	m["HPCBURST_CONF_STAGING_ROOT"], m["HPCBURST_CONF_STAGING"] = exampleRoot, exampleRoot+"/hpc-burst-conformance"
	return m
}

// The shipped Sano profile has no stagingDir: with the Provider's root a run below it is accepted, the profile is returned as shipped, and
// without one the run is refused, naming both places the root can come from. The control for the pass is the refusal.
func TestLoadProfileTakesTheProvidersStagingRootWhenTheProfileHasNone(t *testing.T) {
	pp, part, err := loadProfileWithRoot(profilesDir(), "sano", "gpu", exampleRun, exampleRoot)
	if err != nil {
		t.Fatalf("a run below the Provider's root on Sano: %v", err)
	}
	if pp.Spec.StagingDir != "" || part.Name != "gpu" {
		t.Errorf("the profile came back with stagingDir %q and partition %q; want it as shipped (none) and gpu", pp.Spec.StagingDir, part.Name)
	}
	_, _, err = loadProfileWithRoot(profilesDir(), "sano", "gpu", exampleRun, "")
	if err == nil || !strings.Contains(err.Error(), "names no stagingDir") || !strings.Contains(err.Error(), "HPCBURST_CONF_STAGING_ROOT") {
		t.Errorf("no root anywhere: %v, want a refusal that names the profile's stagingDir and HPCBURST_CONF_STAGING_ROOT", err)
	}
	// The same call on a profile that has a stagingDir is unchanged: it replaces it, as before.
	hel, _, err := loadProfile(profilesDir(), "helios", "plgrid", "$SCRATCH/example-run")
	if err != nil || hel.Spec.StagingDir != "$SCRATCH/example-run" {
		t.Errorf("helios: %v, stagingDir %q", err, hel.Spec.StagingDir)
	}
}

// A run's directory must be strictly below the Provider's root, and no message holds the root, which is account data.
func TestTheProvidersStagingRootBoundsTheRunsDirectory(t *testing.T) {
	for _, c := range []struct{ name, staging string }{
		{"the root itself", exampleRoot},
		{"a trailing slash", exampleRun + "/"},
		{"outside the root", "/path/to/elsewhere/run-0123456789ab"},
		{"a root that only shares a prefix", exampleRoot + "x/run-0123456789ab"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := loadProfileWithRoot(profilesDir(), "sano", "gpu", c.staging, exampleRoot)
			if err == nil || !strings.Contains(err.Error(), "not a directory below the Provider's stagingDir") {
				t.Fatalf("err %v, want a refusal that the directory is not below the Provider's stagingDir", err)
			}
			if strings.Contains(err.Error(), "example-stage") {
				t.Errorf("the refusal quotes the root: %v", err)
			}
		})
	}
}

// ReadSettings carries the root into the settings, holds it to the Provider's own form without quoting it, and builds the run's Provider with the
// run's directory as its stagingDir.
func TestReadSettingsHasTheProvidersStagingRoot(t *testing.T) {
	s, err := readSettings(env(sanoFull()))
	if err != nil {
		t.Fatal(err)
	}
	if s.stagingRoot != exampleRoot || !strings.HasPrefix(s.runStaging, exampleRoot+"/hpc-burst-conformance/run-") || s.profile.Spec.StagingDir != "" {
		t.Errorf("settings: root kept %t, run directory below root %t, profile stagingDir %q; want true, true and none",
			s.stagingRoot == exampleRoot, strings.HasPrefix(s.runStaging, exampleRoot+"/hpc-burst-conformance/run-"), s.profile.Spec.StagingDir)
	}
	if got := conformanceProvider(s).Spec.StagingDir; got != s.runStaging {
		t.Errorf("the Provider's stagingDir is not the run's directory")
	}
	for _, bad := range []string{"relative/dir", "/a/../b", "/has space"} {
		m := sanoFull()
		m["HPCBURST_CONF_STAGING_ROOT"] = bad
		if _, err := readSettings(env(m)); err == nil || !strings.Contains(err.Error(), "HPCBURST_CONF_STAGING_ROOT") || strings.Contains(err.Error(), bad) {
			t.Errorf("root %q: %v, want a refusal that names the setting and not the value", bad, err)
		}
	}
	// A profile that has its own stagingDir, with no root set, builds a Provider with none, as before.
	h, err := readSettings(env(full()))
	if err != nil {
		t.Fatal(err)
	}
	if conformanceProvider(h).Spec.StagingDir != "" {
		t.Errorf("a run on the profile's own stagingDir gave the Provider one")
	}
}

// The Provider the Sano run builds is read by the real ReadProfile on the shipped profile as it is: the Provider's stagingDir is what
// satisfies it, so without it the read fails (the refusal that made the check unrunnable on Sano), and with it the read keeps the run's directory.
func TestTheSanoRunsProviderPassesReadProfile(t *testing.T) {
	s, err := readSettings(env(sanoFull()))
	if err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	read := func(stagingDir string) (slurm.Validated, error) {
		dir := t.TempDir()
		key, creds := filepath.Join(dir, "key"), filepath.Join(dir, "creds")
		for path, content := range map[string]string{key: "example key\n", creds: "Host example-login\n  HostName login.example.org\n  User example-user\n  IdentityFile " + key + "\n  StrictHostKeyChecking yes\n"} {
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		prov := conformanceProvider(s)
		prov.Spec.CredentialsRef.File = creds
		prov.Spec.StagingDir = stagingDir
		c := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects([]client.Object{prov, s.profile}...).Build()
		return (&slurm.ProfileReader{Reader: c, Home: dir, DialHomeMode: agentapi.DialHomeHostDialed}).Read(context.Background(), prov.Name)
	}
	v, err := read(s.runStaging)
	if err != nil {
		t.Fatalf("ReadProfile refused the Sano run's Provider: %v", err)
	}
	if v.Settings.StagingDir != s.runStaging || v.Settings.QOS != "" {
		t.Errorf("the read kept staging dir %t and qos %q; want the run's directory and no qos", v.Settings.StagingDir == s.runStaging, v.Settings.QOS)
	}
	if _, err := read(""); err == nil || !strings.Contains(err.Error(), "stagingDir") {
		t.Errorf("the same Provider without its stagingDir: %v, want a refusal that names stagingDir", err)
	}
}

// The script builds each pre-staging row's helper from the row's own deployment and the image format of the row's partition (hack/live-checks.sh,
// stage_example and stage_image_format, which hack/build-stage-helper.sh takes). Both are read here from the script's text and compared with the
// package's row and with the shipped profile's imageFormat, so a helper that stages another deployment's image, or a sandbox where the partition
// reads a SIF, fails here and not on a cluster. The live table also names the row's example among its inputs, which is what --plan reads.
func TestEveryStageRowsHelperIsBuiltFromItsDeploymentAndItsPartitionsImageFormat(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "hack", "live-checks.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	fn := func(name string) string {
		i := strings.Index(script, "\n"+name+"() {")
		if i < 0 {
			t.Fatalf("hack/live-checks.sh has no %s", name)
		}
		return script[i : i+strings.Index(script[i:], "\n}\n")]
	}
	example, format := fn("stage_example"), fn("stage_image_format")
	pick := func(body, row string) string {
		for _, line := range strings.Split(body, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, row+") echo ") {
				return strings.TrimSuffix(strings.TrimPrefix(line, row+") echo "), " ;;")
			}
		}
		for _, line := range strings.Split(body, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "*) echo ") {
				return strings.TrimSuffix(strings.TrimPrefix(line, "*) echo "), " ;;")
			}
		}
		return ""
	}
	for _, r := range alpha.StageRows() {
		t.Run(r.Name, func(t *testing.T) {
			if got := pick(example, r.Name); got != r.DeploymentFile() {
				t.Errorf("the script builds %s's helper from %q, the row serves %q", r.Name, got, r.DeploymentFile())
			}
			_, part, err := loadProfile(profilesDir(), r.ProfileName(), r.Partition, "$SCRATCH/x")
			if r.ProviderStaging {
				_, part, err = loadProfileWithRoot(profilesDir(), r.ProfileName(), r.Partition, "/r/x", "/r")
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := pick(format, r.Name); got != string(part.ImageFormat) {
				t.Errorf("the script builds %s's helper to stage %q, the partition's image format is %q", r.Name, got, part.ImageFormat)
			}
			var line string
			for _, l := range strings.Split(script, "\n") {
				if strings.HasPrefix(l, r.Name+"|cluster|") {
					line = l
				}
			}
			if f := strings.Split(line, "|"); len(f) < 6 || !slices.Contains(strings.Split(f[5], ","), "examples/"+r.DeploymentFile()) {
				t.Errorf("the live table's inputs for %s do not name examples/%s: --plan would not select the row when the example changes", r.Name, r.DeploymentFile())
			}
		})
	}
}
