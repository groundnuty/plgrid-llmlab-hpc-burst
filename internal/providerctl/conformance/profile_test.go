package conformance

// The conformance run's inputs. The platform under test comes from a shipped
// PlatformProfile (profiles/helios.yaml or profiles/athena.yaml, HBEP-4), so
// the run exercises the file an operator applies, not a profile the harness
// invents. Only what a profile cannot hold comes from the environment: the
// account, the credentials, the host lock, the private directories, the
// staging subdirectory of this run and the size of its jobs.

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// profilesDir is the repository's profiles directory, relative to this package.
func profilesDir() string { return filepath.Join("..", "..", "..", "profiles") }

// profileName is the form of a profile's name: the stem of a file in profiles/.
// A draft ("helios.draft.yaml") is not a PlatformProfile and has a dot.
var profileName = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// retired are the settings the profile holds now. One that is still set is an
// error, not an override that is silently ignored.
var retired = []string{"HPCBURST_CONF_GPU_MODEL", "HPCBURST_CONF_CPU_ARCH", "HPCBURST_CONF_DATA_PARSER"}

// settings is one run's inputs.
type settings struct {
	creds, account, partition, staging, lock, control, out string
	// runStaging is this run's own staging directory, below staging
	// (newRunStaging); the profile's stagingDir is it, and it is removed when the
	// run ends.
	runStaging string
	gpus, cpus int32
	memory     resource.Quantity

	// profile is the shipped PlatformProfile, whole, with its stagingDir
	// replaced by staging; part is the entry of the partition under test.
	profile *v1alpha1.PlatformProfile
	part    v1alpha1.PartitionProfile
}

// loadProfile reads the shipped profile name from dir, finds partition in it
// and returns the profile with its stagingDir replaced by staging. staging
// must be a directory below the profile's own stagingDir (a conformance run
// never writes into the root of the user's scratch space).
func loadProfile(dir, name, partition, staging string) (*v1alpha1.PlatformProfile, v1alpha1.PartitionProfile, error) {
	var none v1alpha1.PartitionProfile
	if !profileName.MatchString(name) {
		return nil, none, fmt.Errorf("profile %q: want the name of a file in profiles/ without its extension, for example helios", name)
	}
	file := filepath.Join(dir, name+".yaml")
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, none, fmt.Errorf("profile %q: %w", name, err)
	}
	var pp v1alpha1.PlatformProfile
	if err := yaml.UnmarshalStrict(raw, &pp); err != nil {
		return nil, none, fmt.Errorf("profile %q: %w", name, err)
	}
	if pp.Kind != "PlatformProfile" || pp.APIVersion != v1alpha1.GroupVersion.String() {
		return nil, none, fmt.Errorf("profile %q is a %s of %s; want a PlatformProfile of %s", name, pp.Kind, pp.APIVersion, v1alpha1.GroupVersion)
	}
	var listed []string
	var part *v1alpha1.PartitionProfile
	for i := range pp.Spec.Partitions {
		listed = append(listed, pp.Spec.Partitions[i].Name)
		if pp.Spec.Partitions[i].Name == partition {
			part = &pp.Spec.Partitions[i]
		}
	}
	if part == nil {
		sort.Strings(listed)
		return nil, none, fmt.Errorf("profile %q lists no partition %q; it lists %s", name, partition, strings.Join(listed, ", "))
	}
	root := pp.Spec.StagingDir
	if root == "" || !strings.HasPrefix(staging, root+"/") || strings.HasSuffix(staging, "/") {
		return nil, none, fmt.Errorf("HPCBURST_CONF_STAGING is not a directory below the profile's stagingDir %q", root)
	}
	pp.Spec.StagingDir = staging
	return &pp, *part, nil
}

// readSettings reads the run's settings from env, a lookup that returns "" for
// an unset name. Every missing or malformed setting is reported at once.
func readSettings(env func(string) string) (settings, error) {
	var problems []string
	get := func(k string) string {
		v := env(k)
		if v == "" {
			problems = append(problems, k+" is not set")
		}
		return v
	}
	num := func(k string) int32 {
		v := get(k)
		if v == "" {
			return 0
		}
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			problems = append(problems, k+" is not a number")
		}
		return int32(n)
	}
	s := settings{
		creds: get("HPCBURST_CONF_CREDENTIALS"), account: get("HPCBURST_CONF_ACCOUNT"), partition: get("HPCBURST_CONF_PARTITION"),
		staging: get("HPCBURST_CONF_STAGING"), lock: get("HPCBURST_CONF_HOST_LOCK"), control: get("HPCBURST_CONF_CONTROL_DIR"),
		out: get("HPCBURST_CONF_OUT"), gpus: num("HPCBURST_CONF_GPUS"), cpus: num("HPCBURST_CONF_CPUS"),
	}
	if m := get("HPCBURST_CONF_MEMORY"); m != "" {
		q, err := resource.ParseQuantity(m)
		if err != nil {
			problems = append(problems, "HPCBURST_CONF_MEMORY is not a quantity")
		}
		s.memory = q
	}
	for _, k := range retired {
		if env(k) != "" {
			problems = append(problems, k+" is not read any more: the profile holds it; unset it")
		}
	}
	profile := get("HPCBURST_CONF_PROFILE")
	if len(problems) > 0 {
		return s, fmt.Errorf("%s", strings.Join(problems, "; "))
	}
	runDir, err := newRunStaging(s.staging)
	if err != nil {
		return s, err
	}
	s.runStaging = runDir
	pp, part, err := loadProfile(profilesDir(), profile, s.partition, s.runStaging)
	if err != nil {
		return s, err
	}
	s.profile, s.part = pp, part
	return s, nil
}
