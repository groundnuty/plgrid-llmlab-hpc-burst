package v1alpha1

import (
	"strings"
	"testing"
	"time"

	"k8s.io/utils/ptr"
)

// The QOS by GPU count (HBEP-4, The Slurm dialect, as amended by #745). The
// expectations are written by hand from the rule: the first entry whose gpus is
// at least the count; a count above the last entry has no QOS; without
// qosByGpus, the Provider's qos. None is read back through QOSForGPUs.

func sanoGPU() PartitionProfile {
	return PartitionProfile{Name: "gpu", QOSByGPUs: []QOSByGPUs{{GPUs: 1, QOS: "gpu1"}, {GPUs: 2, QOS: "gpu2"}, {GPUs: 4, QOS: "gpu4"}, {GPUs: 8, QOS: "gpu8"}}}
}

func TestQOSForGPUsPicksTheFirstEntryThatFits(t *testing.T) {
	for _, tc := range []struct {
		name      string
		partition PartitionProfile
		provider  string
		gpus      int32
		want      string
		refused   bool
	}{
		{name: "1 GPU", partition: sanoGPU(), gpus: 1, want: "gpu1"},
		{name: "2 GPUs", partition: sanoGPU(), gpus: 2, want: "gpu2"},
		{name: "3 GPUs go to the next entry up", partition: sanoGPU(), gpus: 3, want: "gpu4"},
		{name: "4 GPUs", partition: sanoGPU(), gpus: 4, want: "gpu4"},
		{name: "5 GPUs go to the next entry up", partition: sanoGPU(), gpus: 5, want: "gpu8"},
		{name: "8 GPUs, the last entry", partition: sanoGPU(), gpus: 8, want: "gpu8"},
		{name: "9 GPUs, above the last entry", partition: sanoGPU(), gpus: 9, refused: true},
		{name: "a count far above the last entry", partition: sanoGPU(), gpus: 1000, refused: true},
		{name: "no qosByGpus: the Provider's qos", partition: PartitionProfile{Name: "gpu"}, provider: "fast", gpus: 3, want: "fast"},
		{name: "no qosByGpus and no Provider qos: none", partition: PartitionProfile{Name: "gpu"}, gpus: 3, want: ""},
		{name: "no qosByGpus: no upper bound on the count", partition: PartitionProfile{Name: "gpu"}, provider: "fast", gpus: 1000, want: "fast"},
		{name: "gaps between the counts", partition: PartitionProfile{Name: "p", QOSByGPUs: []QOSByGPUs{{GPUs: 2, QOS: "small"}, {GPUs: 100, QOS: "big"}}}, gpus: 50, want: "big"},
		{name: "one entry", partition: PartitionProfile{Name: "p", QOSByGPUs: []QOSByGPUs{{GPUs: 8, QOS: "only"}}}, gpus: 8, want: "only"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := QOSForGPUs(tc.partition, tc.provider, tc.gpus)
			if tc.refused {
				if err == nil || got != "" {
					t.Fatalf("QOSForGPUs = %q, %v; want no QOS and an error", got, err)
				}
				// The error names the partition and the count, and the last entry.
				for _, w := range []string{"partition " + tc.partition.Name, " GPUs is above the 8 "} {
					if !strings.Contains(err.Error(), w) {
						t.Errorf("error %q lacks %q", err, w)
					}
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("QOSForGPUs = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

// The per-job caps come from the first QOS in Slurm's order that sets them,
// like the per-user caps: a partition QOS's per-job cap comes before the job
// QOS's, unless the job QOS has overPartQos (#745; the science agent's order,
// HBEP-4, Which limit applies).
func TestEffectiveQOSLimitsPerJobCapsFollowSlurmsOrder(t *testing.T) {
	partQOS := QOSProfile{Name: "part-qos", MaxCPUsPerJob: ptr.To(int32(8)), MaxMemPerJob: qty("64Gi")}
	jobQOS := QOSProfile{Name: "gpu1", MaxWall: dur("72h"), MaxGPUsPerJob: ptr.To(int32(1)), MaxCPUsPerJob: ptr.To(int32(12)), MaxMemPerJob: qty("140Gi")}
	jobOver := jobQOS
	jobOver.Name, jobOver.OverPartQOS = "gpu1-over", true
	spec := qosSpec(partQOS, jobQOS, jobOver)
	part := PartitionProfile{Name: "gpu", QOS: "part-qos"}

	// The partition QOS's CPU and memory caps come first; the GPU cap, which it
	// does not set, comes from the job QOS.
	got, err := EffectiveQOSLimits(spec, part, "gpu1")
	if err != nil {
		t.Fatal(err)
	}
	if got.MaxCPUsPerJob == nil || *got.MaxCPUsPerJob != 8 || got.MaxMemPerJob == nil || got.MaxMemPerJob.Cmp(*qty("64Gi")) != 0 ||
		got.MaxGPUsPerJob == nil || *got.MaxGPUsPerJob != 1 {
		t.Errorf("limits = cpus %v, mem %v, gpus %v; want 8, 64Gi, 1", got.MaxCPUsPerJob, got.MaxMemPerJob, got.MaxGPUsPerJob)
	}
	for k, want := range map[string]string{"maxCpusPerJob": "part-qos", "maxMemPerJob": "part-qos", "maxGpusPerJob": "gpu1", "maxWall": "gpu1"} {
		if got.From[k] != want {
			t.Errorf("From[%s] = %q, want %q", k, got.From[k], want)
		}
	}

	// With overPartQos on the job QOS its own caps come first.
	got, err = EffectiveQOSLimits(spec, part, "gpu1-over")
	if err != nil {
		t.Fatal(err)
	}
	if got.MaxCPUsPerJob == nil || *got.MaxCPUsPerJob != 12 || got.MaxMemPerJob == nil || got.MaxMemPerJob.Cmp(*qty("140Gi")) != 0 {
		t.Errorf("limits = cpus %v, mem %v; want 12 and 140Gi", got.MaxCPUsPerJob, got.MaxMemPerJob)
	}
	if got.MaxWall == nil || *got.MaxWall != 72*time.Hour {
		t.Errorf("maxWall = %v, want 72h", got.MaxWall)
	}

	// With no partition QOS (Sano's gpu partition), the chosen QOS's caps apply.
	got, err = EffectiveQOSLimits(spec, PartitionProfile{Name: "gpu"}, "gpu1")
	if err != nil {
		t.Fatal(err)
	}
	if got.MaxCPUsPerJob == nil || *got.MaxCPUsPerJob != 12 || got.From["maxCpusPerJob"] != "gpu1" {
		t.Errorf("maxCpusPerJob = %v from %q, want 12 from gpu1", got.MaxCPUsPerJob, got.From["maxCpusPerJob"])
	}
}

// The GPU bound on a qosByGpus partition is the smaller of gpusPerNode and the
// last entry's gpus, then the chosen QOS's maxGpusPerUser and maxGpusPerJob
// (HBEP-4, Submission, GPU sizing; #745). Worked by hand.
func TestAllocationGPUBoundOnAQOSByGPUsPartition(t *testing.T) {
	withNode := func(n int32) PartitionProfile { p := sanoGPU(); p.GPUsPerNode = ptr.To(n); return p }
	noNode := sanoGPU()
	for _, tc := range []struct {
		name   string
		part   PartitionProfile
		limits QOSLimits
		want   GPUBound
		ok     bool
	}{
		{"Sano: 8 per node and a last entry of 8", withNode(8), QOSLimits{}, GPUBound{8, GPUBoundPerNode}, true},
		{"a node smaller than the last entry", withNode(4), QOSLimits{}, GPUBound{4, GPUBoundPerNode}, true},
		{"a node larger than the last entry", withNode(16), QOSLimits{}, GPUBound{8, GPUBoundQOSByGPUs}, true},
		{"no gpusPerNode: the last entry", noNode, QOSLimits{}, GPUBound{8, GPUBoundQOSByGPUs}, true},
		{"gpu4 with maxGpusPerJob 2", withNode(8), QOSLimits{MaxGPUsPerJob: ptr.To(int32(2))}, GPUBound{2, LimitMaxGPUsPerJob}, true},
		{"a per-user cap below the rest", withNode(8), QOSLimits{MaxGPUsPerUser: ptr.To(int32(3)), MaxGPUsPerJob: ptr.To(int32(5))}, GPUBound{3, LimitMaxGPUsPerUser}, true},
		{"a per-job cap of 4 below the per-user cap", withNode(8), QOSLimits{MaxGPUsPerUser: ptr.To(int32(6)), MaxGPUsPerJob: ptr.To(int32(4))}, GPUBound{4, LimitMaxGPUsPerJob}, true},
		{"a tie names gpusPerNode first", withNode(8), QOSLimits{MaxGPUsPerUser: ptr.To(int32(8)), MaxGPUsPerJob: ptr.To(int32(8))}, GPUBound{8, GPUBoundPerNode}, true},
		{"a partition with no qosByGpus: as before", PartitionProfile{Name: "p", GPUsPerNode: ptr.To(int32(8))}, QOSLimits{MaxGPUsPerUser: ptr.To(int32(1))}, GPUBound{1, LimitMaxGPUsPerUser}, true},
		{"no bound at all", PartitionProfile{Name: "p"}, QOSLimits{}, GPUBound{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := AllocationGPUBound(tc.part, tc.limits)
			if got != tc.want || ok != tc.ok {
				t.Errorf("AllocationGPUBound = %+v, %v; want %+v, %v", got, ok, tc.want, tc.ok)
			}
		})
	}
}
