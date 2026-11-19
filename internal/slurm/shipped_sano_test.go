package slurm

// profiles/sano.yaml and ReadProfile's checks of qosByGpus (#735; HBEP-4 as amended by #745). The expected values are the
// science agent's #735 comment of 18 Nov 2026 and #735's probe 3, written here as literals, never read back from the
// file by the code that wrote it.

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

func readShippedSano(t *testing.T) v1alpha1.PlatformProfile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "profiles", "sano.yaml"))
	if err != nil {
		t.Fatalf("read the shipped profile: %v", err) // a missing file fails; it never skips
	}
	var pp v1alpha1.PlatformProfile
	if err := yaml.UnmarshalStrict(raw, &pp); err != nil {
		t.Fatalf("sano.yaml: %v", err)
	}
	return pp
}

// The shipped Sano profile holds the values of the science agent's #735 comment and of probe 3, and the Slurm
// provider's own checks accept it once it has a stagingDir, which no fact gave it (so ReadProfile refuses it until an
// operator's copy sets one: a stated limit of #735).
func TestShippedSanoProfile(t *testing.T) {
	pp := readShippedSano(t)
	if pp.Name != "sano" || pp.Spec.ProviderKind != v1alpha1.PlatformBatch {
		t.Fatalf("the profile is %q of kind %q; want sano, batch", pp.Name, pp.Spec.ProviderKind)
	}
	if len(pp.Spec.Partitions) != 1 {
		t.Fatalf("%d partitions, want exactly gpu (test is too short for an Instance)", len(pp.Spec.Partitions))
	}
	part := pp.Spec.Partitions[0]
	if part.Name != "gpu" || part.GPUModel != "A100" || part.CPUArch != "x86_64" {
		t.Errorf("partition %q of %q on %q, want gpu of A100 on x86_64", part.Name, part.GPUModel, part.CPUArch)
	}
	if part.GPUsPerNode == nil || *part.GPUsPerNode != 8 || part.CPUsPerGPU == nil || *part.CPUsPerGPU != 4 ||
		part.MemPerGPU == nil || part.MemPerGPU.Cmp(*qty("65536Mi")) != 0 {
		t.Errorf("gpusPerNode %v, cpusPerGpu %v, memPerGpu %v; want 8, 4, 65536Mi (the science agent's #735 decision of 19 Nov 2026)", part.GPUsPerNode, part.CPUsPerGPU, part.MemPerGPU)
	}
	if part.MaxTime == nil || *part.MaxTime != "168h" || part.ImageFormat != v1alpha1.ImageFormatSIF || part.QOS != "" {
		t.Errorf("maxTime %v, imageFormat %q, partition qos %q; want 168h, sif and none", part.MaxTime, part.ImageFormat, part.QOS)
	}
	wantByGPUs := []v1alpha1.QOSByGPUs{{GPUs: 1, QOS: "gpu1"}, {GPUs: 2, QOS: "gpu2"}, {GPUs: 4, QOS: "gpu4"}, {GPUs: 8, QOS: "gpu8"}}
	if len(part.QOSByGPUs) != 4 {
		t.Fatalf("qosByGpus %v, want %v", part.QOSByGPUs, wantByGPUs)
	}
	for i, w := range wantByGPUs {
		if part.QOSByGPUs[i] != w {
			t.Errorf("qosByGpus[%d] = %v, want %v", i, part.QOSByGPUs[i], w)
		}
	}
	// Sano never lists ssh: its compute nodes have no home directories, where ssh mode's agent reads the dial-home key
	// (HBEP-4, the platform profile schema; #735's probe 1).
	if len(part.DialHomeModes) != 1 || part.DialHomeModes[0] != v1alpha1.ProfileDialHomeMode("host-dialed") {
		t.Errorf("dialHomeModes %v, want [host-dialed]", part.DialHomeModes)
	}
	if !pp.Spec.HoldRelease {
		t.Errorf("holdRelease is false, want true (the probe of 18 Nov 2026)")
	}
	// Probe 4 of #735 (facts.slurm.time_format): SLURM_TIME_FORMAT=%s gives epoch seconds in sbatch --test-only's start, so
	// Sano's profile decides the estimate's reading. Without the field Estimate answers unknown and runs no command (HBEP-4,
	// Estimate). The literal is the value of the probe, not the constant the code reads.
	if pp.Spec.StartEstimateTime != "epoch-seconds" {
		t.Errorf("startEstimateTime %q, want epoch-seconds (probe 4 of #735)", pp.Spec.StartEstimateTime)
	}

	// Each QOS: a 3-day wall and the caps of probe 3, per job. 1,061.75 GiB is 1,087,232 MiB.
	for _, w := range []struct {
		name       string
		gpus, cpus int32
		mem        string
	}{{"gpu1", 1, 12, "143360Mi"}, {"gpu2", 2, 24, "286720Mi"}, {"gpu4", 4, 48, "573440Mi"}, {"gpu8", 8, 92, "1087232Mi"}} {
		q, ok := v1alpha1.FindQOS(&pp.Spec, w.name)
		if !ok {
			t.Errorf("qos %s is not listed", w.name)
			continue
		}
		if q.MaxWall == nil || *q.MaxWall != "72h" || q.MaxGPUsPerJob == nil || *q.MaxGPUsPerJob != w.gpus ||
			q.MaxCPUsPerJob == nil || *q.MaxCPUsPerJob != w.cpus || q.MaxMemPerJob == nil || q.MaxMemPerJob.Cmp(*qty(w.mem)) != 0 {
			t.Errorf("qos %s = wall %v, gpus %v, cpus %v, mem %v; want 72h, %d, %d, %s", w.name, q.MaxWall, q.MaxGPUsPerJob, q.MaxCPUsPerJob, q.MaxMemPerJob, w.gpus, w.cpus, w.mem)
		}
		if q.MaxSubmitPerUser != nil || q.MaxGPUsPerUser != nil || q.MaxCPUsPerUser != nil || q.MaxMemPerUser != nil {
			t.Errorf("qos %s types a per-user limit, which no fact gave", w.name)
		}
	}
	// The per-GPU shares are at most Sano's maximum (HBEP-4, "Never above one GPU's share": 11 CPUs and 135,904 MiB, a cap, not a value to
	// equal), and they fit the QOS they select at every count the list names, times the count.
	if *part.CPUsPerGPU > 11 || part.MemPerGPU.Cmp(*qty("135904Mi")) > 0 {
		t.Errorf("cpusPerGpu %d and memPerGpu %s are above the node's per-GPU share of 11 and 135904Mi", *part.CPUsPerGPU, part.MemPerGPU.String())
	}
	for _, n := range []int32{1, 2, 3, 4, 5, 8} {
		q, err := v1alpha1.QOSForGPUs(part, "", n)
		if err != nil {
			t.Fatal(err)
		}
		lim, err := v1alpha1.EffectiveQOSLimits(&pp.Spec, part, q)
		if err != nil {
			t.Fatal(err)
		}
		cpus, mem := n**part.CPUsPerGPU, part.MemPerGPU.DeepCopy()
		mem.Mul(int64(n))
		if cpus > *lim.MaxCPUsPerJob || mem.Cmp(*lim.MaxMemPerJob) > 0 {
			t.Errorf("%d GPUs: %d CPUs and %s under %s, above its caps of %d and %s", n, cpus, mem.String(), q, *lim.MaxCPUsPerJob, lim.MaxMemPerJob.String())
		}
	}

	// ReadProfile accepts it for a Provider that sets no qos and an operator's stagingDir; without one it refuses it.
	r := newProfileRig(t)
	r.pp = pp.DeepCopy()
	r.pp.Spec.StagingDir = "$SCRATCH/stage"
	r.prov.Spec.PlatformProfileRef.Name = "sano"
	r.prov.Spec.AllowedPartitions = []string{"gpu"}
	if v, err := r.read(); err != nil || v.Profile.Partition.Name != "gpu" || v.Settings.QOS != "" {
		t.Fatalf("ReadProfile = %+v, %v; want the partition gpu and no qos", v.Profile.Partition.Name, err)
	}
	r.pp.Spec.StagingDir = ""
	var inv *provider.InvalidProfileError
	if _, err := r.read(); !errors.As(err, &inv) || !hasField(inv, "spec.stagingDir") {
		t.Errorf("without a stagingDir ReadProfile = %v; want spec.stagingDir named", err)
	}
}

func hasField(inv *provider.InvalidProfileError, field string) bool {
	for _, f := range inv.Fields {
		if f.Field == field {
			return true
		}
	}
	return false
}

// A Provider that sets qos on a partition with qosByGpus is refused (#745, ruling c: the QOS's one source), and a
// qosByGpus the CRD's own rules refuse is refused again, since a stored object is never trusted blindly.
func TestReadProfileQOSByGPUs(t *testing.T) {
	four := func(r *profileRig) {
		r.pp.Spec.QOS = []v1alpha1.QOSProfile{{Name: "gpu1"}, {Name: "gpu2"}, {Name: "gpu4"}, {Name: "gpu8"}}
		r.pp.Spec.Partitions[0].QOSByGPUs = []v1alpha1.QOSByGPUs{{GPUs: 1, QOS: "gpu1"}, {GPUs: 2, QOS: "gpu2"}, {GPUs: 4, QOS: "gpu4"}, {GPUs: 8, QOS: "gpu8"}}
	}
	r := newProfileRig(t)
	four(r)
	if v, err := r.read(); err != nil || v.Settings.QOS != "" {
		t.Fatalf("a partition with qosByGpus and no Provider qos: Settings.QOS %q, error %v; want it read", v.Settings.QOS, err)
	}
	for _, c := range []struct {
		name  string
		mod   func(r *profileRig)
		field string
		say   string
	}{
		{"a Provider qos beside qosByGpus", func(r *profileRig) { four(r); r.prov.Spec.QOS = "gpu1" }, "provider.spec.qos", "qosByGpus"},
		{"a Provider qos beside qosByGpus, though it is listed elsewhere", func(r *profileRig) {
			four(r)
			r.pp.Spec.QOS = append(r.pp.Spec.QOS, v1alpha1.QOSProfile{Name: "normal"})
			r.prov.Spec.QOS = "normal"
		}, "provider.spec.qos", "one source"},
		{"an entry the profile does not list", func(r *profileRig) {
			four(r)
			r.pp.Spec.QOS = r.pp.Spec.QOS[:3]
		}, "spec.partitions[0].qosByGpus[3].qos", "not listed"},
		{"counts that do not increase", func(r *profileRig) {
			four(r)
			r.pp.Spec.Partitions[0].QOSByGPUs[2].GPUs = 2
		}, "spec.partitions[0].qosByGpus[2].gpus", "strictly increasing"},
		{"a count of 0", func(r *profileRig) {
			four(r)
			r.pp.Spec.Partitions[0].QOSByGPUs[0].GPUs = 0
		}, "spec.partitions[0].qosByGpus[0].gpus", "strictly increasing"},
		{"an unsafe qos", func(r *profileRig) {
			four(r)
			r.pp.Spec.QOS[1].Name = "gpu2;id"
			r.pp.Spec.Partitions[0].QOSByGPUs[1].QOS = "gpu2;id"
		}, "spec.partitions[0].qosByGpus[1].qos", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newProfileRig(t)
			c.mod(r)
			_, err := r.read()
			var inv *provider.InvalidProfileError
			if !errors.As(err, &inv) {
				t.Fatalf("got %v, want an InvalidProfileError", err)
			}
			for _, f := range inv.Fields {
				if f.Field == c.field && strings.Contains(f.Message, c.say) {
					return
				}
			}
			t.Errorf("field %s with %q not named in %v", c.field, c.say, inv.Fields)
		})
	}
}

// ReadProfile refuses the shipped Sano profile under the ssh dial-home mode, and accepts it under host-dialed: the
// agent of an ssh-mode job reads <login HOME>/.hpcburst/dial-home, which a Sano compute node cannot reach, so its jobs
// would never register (HBEP-4: "Sano never lists ssh"; Carbon's review of #756).
func TestShippedSanoProfileIsRefusedUnderSSHMode(t *testing.T) {
	pp := readShippedSano(t)
	r := newProfileRig(t)
	r.pp = pp.DeepCopy()
	r.pp.Spec.StagingDir = "$SCRATCH/stage"
	r.prov.Spec.PlatformProfileRef.Name = "sano"
	r.prov.Spec.AllowedPartitions = []string{"gpu"}
	if _, err := r.readUnder(agentapi.DialHomeHostDialed); err != nil {
		t.Fatalf("under host-dialed: %v", err)
	}
	_, err := r.readUnder(agentapi.DialHomeSSH)
	var inv *provider.InvalidProfileError
	if !errors.As(err, &inv) || !hasField(inv, "spec.partitions[gpu].dialHomeModes") {
		t.Fatalf("under ssh: %v; want spec.partitions[gpu].dialHomeModes named", err)
	}
}
