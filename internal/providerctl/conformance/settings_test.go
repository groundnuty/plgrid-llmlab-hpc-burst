package conformance

// The harness's inputs, tested without a platform. The expectations come from
// files this test writes, from HBEP-4 (Helios plgrid is a CPU partition that
// reads its JSON with data_parser v0.0.42; Athena's A100 partition reads
// v0.0.40) and from the issue (#100: the shipped partitions), not from a
// comparison of the loader with itself.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const testProfile = `apiVersion: hpcburst.groundnuty.github.io/v1alpha1
kind: PlatformProfile
metadata:
  name: example
spec:
  providerKind: batch
  partitions:
    - name: p-cpu
      gpuModel: none
      cpuArch: aarch64
      gpusPerNode: 0
    - name: p-gpu
      gpuModel: X900
      cpuArch: riscv64
      gpusPerNode: 2
  holdRelease: true
  jsonDataParser: v0.0.99
  stagingDir: $SCRATCH
`

func writeTestProfile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "example.yaml"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

// Everything the platform is comes from the profile file: the partition's
// accelerator class, the data_parser and the rest of the spec; only the
// staging directory is the run's.
func TestLoadProfileReadsThePartitionFromTheFile(t *testing.T) {
	dir := writeTestProfile(t, testProfile)
	pp, part, err := loadProfile(dir, "example", "p-gpu", "$SCRATCH/run-1")
	if err != nil {
		t.Fatal(err)
	}
	if part.Name != "p-gpu" || part.GPUModel != "X900" || part.CPUArch != "riscv64" || part.GPUsPerNode == nil || *part.GPUsPerNode != 2 {
		t.Errorf("partition = %+v, want p-gpu, X900, riscv64, 2 GPUs per node", part)
	}
	if pp.Name != "example" || pp.Spec.JSONDataParser != "v0.0.99" || !pp.Spec.HoldRelease || len(pp.Spec.Partitions) != 2 {
		t.Errorf("profile = %+v, want the file's name, data_parser, holdRelease and both partitions", pp.Spec)
	}
	if pp.Spec.StagingDir != "$SCRATCH/run-1" {
		t.Errorf("stagingDir = %q, want the run's $SCRATCH/run-1", pp.Spec.StagingDir)
	}
	if _, part, err = loadProfile(dir, "example", "p-cpu", "$SCRATCH/run-1"); err != nil || part.GPUModel != "none" || part.CPUArch != "aarch64" {
		t.Errorf("p-cpu: %+v, %v; want gpuModel none, aarch64", part, err)
	}
}

func TestLoadProfileRefusals(t *testing.T) {
	dir := writeTestProfile(t, testProfile)
	for _, tc := range []struct {
		name, profile, partition, staging, dir, want string
	}{
		{"a partition the profile does not list", "example", "p-other", "$SCRATCH/run", dir, `lists no partition "p-other"; it lists p-cpu, p-gpu`},
		{"a path as the name", "../example", "p-cpu", "$SCRATCH/run", dir, "without its extension"},
		{"a draft", "example.draft", "p-cpu", "$SCRATCH/run", dir, "without its extension"},
		{"a missing file", "nothing", "p-cpu", "$SCRATCH/run", dir, "no such file"},
		{"the staging root itself", "example", "p-cpu", "$SCRATCH", dir, "not a directory below the profile's stagingDir"},
		{"a directory outside the root", "example", "p-cpu", "/tmp/run", dir, "not a directory below the profile's stagingDir"},
		{"a root that only shares a prefix", "example", "p-cpu", "$SCRATCHES/run", dir, "not a directory below the profile's stagingDir"},
		{"a trailing slash", "example", "p-cpu", "$SCRATCH/run/", dir, "not a directory below the profile's stagingDir"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := loadProfile(tc.dir, tc.profile, tc.partition, tc.staging)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want one containing %q", err, tc.want)
			}
		})
	}
	// A key the schema does not know is refused (strict), not dropped.
	bad := writeTestProfile(t, strings.Replace(testProfile, "holdRelease: true", "holdRelese: true", 1))
	if _, _, err := loadProfile(bad, "example", "p-cpu", "$SCRATCH/run"); err == nil || !strings.Contains(err.Error(), "holdRelese") {
		t.Errorf("a misspelled key: error = %v, want one naming holdRelese", err)
	}
	// A manifest of another kind is not a profile.
	other := writeTestProfile(t, strings.Replace(testProfile, "kind: PlatformProfile", "kind: Provider", 1))
	if _, _, err := loadProfile(other, "example", "p-cpu", "$SCRATCH/run"); err == nil || !strings.Contains(err.Error(), "want a PlatformProfile") {
		t.Errorf("another kind: error = %v, want a refusal", err)
	}
}

// The shipped profiles are what the harness runs against: each loads, with the
// partitions of the issue (#100), and the fields a run depends on are the ones
// HBEP-4 gives.
func TestShippedProfilesLoadForTheHarness(t *testing.T) {
	for _, tc := range []struct {
		profile, partition, gpuModel, cpuArch, parser string
	}{
		{"helios", "plgrid", "none", "x86_64", "v0.0.42"},
		{"helios", "plgrid-gpu-gh200", "GH200", "aarch64", "v0.0.42"},
		{"athena", "plgrid-gpu-a100", "A100", "x86_64", "v0.0.40"},
		{"athena", "plgrid-now", "A100", "x86_64", "v0.0.40"}, // issue #93, comment 6557245889: listed for conformance runs
	} {
		t.Run(tc.profile+"/"+tc.partition, func(t *testing.T) {
			pp, part, err := loadProfile(profilesDir(), tc.profile, tc.partition, "$SCRATCH/hpc-burst-conformance")
			if err != nil {
				t.Fatalf("the harness cannot load a shipped profile: %v", err) // never a skip
			}
			if part.GPUModel != tc.gpuModel || part.CPUArch != tc.cpuArch || pp.Spec.JSONDataParser != tc.parser {
				t.Errorf("gpuModel %q, cpuArch %q, data_parser %q; want %q, %q, %q", part.GPUModel, part.CPUArch, pp.Spec.JSONDataParser, tc.gpuModel, tc.cpuArch, tc.parser)
			}
		})
	}
}

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

// full is a complete set of settings for the shipped Helios plgrid partition.
func full() map[string]string {
	return map[string]string{
		"HPCBURST_CONF_PROFILE": "helios", "HPCBURST_CONF_PARTITION": "plgrid", "HPCBURST_CONF_CREDENTIALS": "/private/creds",
		"HPCBURST_CONF_ACCOUNT": "example-account", "HPCBURST_CONF_GPUS": "0", "HPCBURST_CONF_CPUS": "1", "HPCBURST_CONF_MEMORY": "1Gi",
		"HPCBURST_CONF_STAGING": "$SCRATCH/hpc-burst-conformance", "HPCBURST_CONF_HOST_LOCK": "/private/lock",
		"HPCBURST_CONF_CONTROL_DIR": "/private/ctl", "HPCBURST_CONF_OUT": "/private/out",
	}
}

func TestReadSettings(t *testing.T) {
	s, err := readSettings(env(full()))
	if err != nil {
		t.Fatal(err)
	}
	if s.account != "example-account" || s.partition != "plgrid" || s.gpus != 0 || s.cpus != 1 || s.memory.String() != "1Gi" ||
		s.part.Name != "plgrid" || s.staging != "$SCRATCH/hpc-burst-conformance" ||
		!regexp.MustCompile(`^\$SCRATCH/hpc-burst-conformance/run-[0-9a-f]{12}$`).MatchString(s.profile.Spec.StagingDir) {
		t.Errorf("settings = %+v", s)
	}

	// Every missing setting is named at once, and the account never is quoted.
	_, err = readSettings(env(map[string]string{"HPCBURST_CONF_ACCOUNT": "secret-account-name"}))
	if err == nil {
		t.Fatal("empty settings accepted")
	}
	for _, k := range []string{"HPCBURST_CONF_PROFILE", "HPCBURST_CONF_PARTITION", "HPCBURST_CONF_CREDENTIALS", "HPCBURST_CONF_GPUS",
		"HPCBURST_CONF_CPUS", "HPCBURST_CONF_MEMORY", "HPCBURST_CONF_STAGING", "HPCBURST_CONF_HOST_LOCK", "HPCBURST_CONF_CONTROL_DIR", "HPCBURST_CONF_OUT"} {
		if !strings.Contains(err.Error(), k+" is not set") {
			t.Errorf("the error does not name %s: %v", k, err)
		}
	}
	if strings.Contains(err.Error(), "secret-account-name") {
		t.Error("the error quotes the account")
	}

	// A setting the profile holds now stops the run, rather than being ignored.
	for _, k := range retired {
		m := full()
		m[k] = "anything"
		if _, err := readSettings(env(m)); err == nil || !strings.Contains(err.Error(), k+" is not read any more") {
			t.Errorf("%s still set: error = %v, want a refusal naming it", k, err)
		}
	}

	m := full()
	m["HPCBURST_CONF_GPUS"], m["HPCBURST_CONF_MEMORY"] = "two", "lots"
	if _, err := readSettings(env(m)); err == nil || !strings.Contains(err.Error(), "HPCBURST_CONF_GPUS is not a number") || !strings.Contains(err.Error(), "HPCBURST_CONF_MEMORY is not a quantity") {
		t.Errorf("malformed numbers: error = %v", err)
	}
}
