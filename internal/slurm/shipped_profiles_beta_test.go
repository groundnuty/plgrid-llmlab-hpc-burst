package slurm

// The beta fields of the shipped profiles (HBEP-4, Platform profile schema, The
// remaining groups; Graduation criteria, Beta). The expected values are written
// by hand from the science agent's rulings on #625 (comment 6908815428) and from
// the schema's own sources: Helios plgrid weighs a CPU and has no GPU, so cpu-hour;
// every GPU partition is billed by the GPU-hour; Athena plgrid-gpu-a100 lists
// ssh and host-dialed; Athena plgrid-now lists host-dialed only, in both Athena
// profiles; Helios lists none; LLMLab sets no gateway group. None is read back
// from the profile under test.

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

type betaPartition struct {
	unit  v1alpha1.BillingUnit
	modes []v1alpha1.ProfileDialHomeMode
}

func loadShippedProfile(t *testing.T, file string) v1alpha1.PlatformProfile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "profiles", file))
	if err != nil {
		t.Fatalf("read the shipped profile: %v", err) // a missing file fails; it never skips
	}
	var pp v1alpha1.PlatformProfile
	if err := yaml.UnmarshalStrict(raw, &pp); err != nil {
		t.Fatalf("%s: %v", file, err)
	}
	return pp
}

func TestShippedProfilesSetTheBetaPartitionFields(t *testing.T) {
	both := []v1alpha1.ProfileDialHomeMode{v1alpha1.ProfileDialHomeSSH, v1alpha1.ProfileDialHomeHostDialed}
	hostDialedOnly := []v1alpha1.ProfileDialHomeMode{v1alpha1.ProfileDialHomeHostDialed}
	for file, want := range map[string]map[string]betaPartition{
		"helios.yaml": {
			"plgrid":           {unit: v1alpha1.BillingUnitCPUHour},
			"plgrid-gpu-gh200": {unit: v1alpha1.BillingUnitGPUHour},
		},
		"athena.yaml": {
			"plgrid-gpu-a100": {unit: v1alpha1.BillingUnitGPUHour, modes: both},
			"plgrid-now":      {unit: v1alpha1.BillingUnitGPUHour, modes: hostDialedOnly},
		},
		"athena-now.yaml": {
			"plgrid-now": {unit: v1alpha1.BillingUnitGPUHour, modes: hostDialedOnly},
		},
	} {
		pp := loadShippedProfile(t, file)
		if len(pp.Spec.Partitions) != len(want) {
			t.Errorf("%s lists %d partitions; want %d", file, len(pp.Spec.Partitions), len(want))
		}
		for _, p := range pp.Spec.Partitions {
			w, ok := want[p.Name]
			if !ok {
				t.Errorf("%s lists partition %s, which this test does not expect: give it a billingUnit and decide its dialHomeModes", file, p.Name)
				continue
			}
			if p.BillingUnit != w.unit {
				t.Errorf("%s %s: billingUnit %q, want %q", file, p.Name, p.BillingUnit, w.unit)
			}
			if !slices.Equal(p.DialHomeModes, w.modes) {
				t.Errorf("%s %s: dialHomeModes %v, want %v", file, p.Name, p.DialHomeModes, w.modes)
			}
		}
	}
}

// LLMLab's profile gets no gateway group until its two facts are measured
// (the science agent, #625 comment 6908815428), and it has no partitions.
func TestShippedLLMLabProfileHasNoGatewayGroup(t *testing.T) {
	pp := loadShippedProfile(t, "llmlab.yaml")
	if pp.Spec.Gateway != nil {
		t.Errorf("llmlab.yaml sets a gateway group %+v; it stays unset until measured", pp.Spec.Gateway)
	}
	if len(pp.Spec.Partitions) != 0 {
		t.Errorf("llmlab.yaml lists %d partitions; a managed profile has none", len(pp.Spec.Partitions))
	}
}

// A launcher in a mode reads exactly the Providers the rulings let it read:
// Athena plgrid-now lists host-dialed only, so an ssh-mode launcher is refused
// there until probe 2 is recorded on it, and reads plgrid-gpu-a100; Helios
// lists nothing, so a launcher in either mode reads both of its partitions.
func TestShippedProfilesUnderALauncherInEachMode(t *testing.T) {
	for _, c := range []struct {
		file, partition string
		mode            agentapi.DialHomeMode
		reads           bool
	}{
		{"athena.yaml", "plgrid-gpu-a100", agentapi.DialHomeSSH, true},
		{"athena.yaml", "plgrid-gpu-a100", agentapi.DialHomeHostDialed, true},
		{"athena.yaml", "plgrid-gpu-a100", agentapi.DialHomeDirect, true},
		{"athena.yaml", "plgrid-now", agentapi.DialHomeSSH, false},
		{"athena.yaml", "plgrid-now", agentapi.DialHomeHostDialed, true},
		{"athena.yaml", "plgrid-now", agentapi.DialHomeDirect, true},
		{"athena-now.yaml", "plgrid-now", agentapi.DialHomeSSH, false},
		{"athena-now.yaml", "plgrid-now", agentapi.DialHomeHostDialed, true},
		{"helios.yaml", "plgrid-gpu-gh200", agentapi.DialHomeSSH, true},
		{"helios.yaml", "plgrid-gpu-gh200", agentapi.DialHomeHostDialed, true},
		{"helios.yaml", "plgrid", agentapi.DialHomeSSH, true},
		{"helios.yaml", "plgrid", agentapi.DialHomeHostDialed, true},
	} {
		t.Run(c.file+"/"+c.partition+"/"+string(c.mode), func(t *testing.T) {
			pp := loadShippedProfile(t, c.file)
			r := newProfileRig(t)
			r.pp = pp.DeepCopy()
			r.prov.Spec.PlatformProfileRef.Name = pp.Name
			r.prov.Spec.AllowedPartitions = []string{c.partition}
			_, err := r.readUnder(c.mode)
			if c.reads && err != nil {
				t.Errorf("a launcher in %s mode is refused: %v", c.mode, err)
			}
			if !c.reads && err == nil {
				t.Errorf("a launcher in %s mode reads the Provider; want a refusal naming the partition's dialHomeModes", c.mode)
			}
		})
	}
}
