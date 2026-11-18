package providerctl

// JobSubmitted's note names the QOS the job was sent under (HBEP-8, Allocation events; HBEP-4, The Slurm dialect,
// The QOS by GPU count): the one qosByGpus chooses for the Allocation's GPUs, and for a partition without it the
// Provider's qos, or the association's default when there is none (review of #756, Astra 10).

import (
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

func TestSubmittedNoteNamesTheQOSSent(t *testing.T) {
	byGPUs := []v1alpha1.QOSByGPUs{{GPUs: 1, QOS: "gpu1"}, {GPUs: 2, QOS: "gpu2"}, {GPUs: 4, QOS: "gpu4"}, {GPUs: 8, QOS: "gpu8"}}
	for _, tc := range []struct {
		name     string
		byGPUs   []v1alpha1.QOSByGPUs
		provider string
		gpus     int32
		want     string
	}{
		{"1 GPU under qosByGpus", byGPUs, "", 1, "QOS gpu1,"},
		{"3 GPUs under qosByGpus", byGPUs, "", 3, "QOS gpu4,"},
		{"8 GPUs under qosByGpus", byGPUs, "", 8, "QOS gpu8,"},
		{"a Provider qos and no qosByGpus", nil, "now", 1, "QOS now,"},
		{"no qos at all", nil, "", 1, "QOS the association's default,"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &external{v: slurm.Validated{
				Settings: provider.Settings{QOS: tc.provider},
				Profile:  provider.Profile{Partition: v1alpha1.PartitionProfile{Name: "gpu", QOSByGPUs: tc.byGPUs}},
			}}
			a := &v1alpha1.Allocation{}
			a.Spec.ForProvider.Batch = &v1alpha1.BatchParameters{Partition: "gpu", GPUs: tc.gpus, TimeLimit: "72h"}
			note := e.submittedNote(a, v1alpha1.SubmissionRecord{Attempt: 1})
			if !strings.Contains(note, tc.want) {
				t.Errorf("note %q does not say %q", note, tc.want)
			}
		})
	}
}
