package lifecycle

import (
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// rigExtra is a Provider a test added beside the rig's own: its partition is
// listed in the rig's profile, and it has its own fake Slurm and Provider
// controller, synced in every Step.
type rigExtra struct {
	name  string
	batch *pfake.Batch
	prov  *pfake.Controller
}

// addProvider adds a Provider that allows one new partition of the rig's
// profile, of the rig's accelerator class, with the given maxTime, edited by
// f. Its fake Slurm is configured by cfg (nil: defaults).
func (rg *rig) addProvider(name, partition string, maxTime time.Duration, f func(*v1alpha1.PartitionProfile), cfg func(*pfake.Config)) *rigExtra {
	rg.t.Helper()
	var prof v1alpha1.PlatformProfile
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProfile}, &prof); err != nil {
		rg.t.Fatal(err)
	}
	pp := v1alpha1.PartitionProfile{
		Name: partition, AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: rigGPU, CPUArch: rigArch},
		MaxTime: dur(maxTime), GPUsPerNode: ptrTo(int32(4)), CPUsPerGPU: ptrTo(int32(16)),
		MemPerGPU: ptrTo(resource.MustParse("120Gi")),
	}
	if f != nil {
		f(&pp)
	}
	prof.Spec.Partitions = append(prof.Spec.Partitions, pp)
	if err := rg.c.Update(rg.ctx, &prof); err != nil {
		rg.t.Fatal(err)
	}
	rg.create(&v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: newUID(), Generation: 1, Labels: map[string]string{v1alpha1.LabelPlatform: "helios"}},
		Spec:       v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: rigProfile}, AllowedPartitions: []string{partition}},
	})
	bcfg := pfake.Config{Partitions: []pfake.Partition{{Name: partition, GPUModel: rigGPU, GPUsPerNode: 4, MaxTime: maxTime}}}
	if cfg != nil {
		cfg(&bcfg)
	}
	b := pfake.NewBatch(rg.clock, bcfg)
	x := &rigExtra{name: name, batch: b, prov: &pfake.Controller{Client: rg.c, Provider: name, Batch: b, Clock: rg.clock, HoldRelease: prof.Spec.HoldRelease}}
	rg.extra = append(rg.extra, x)
	return x
}

// batchOf returns the fake Slurm of an Allocation's Provider.
func (rg *rig) batchOf(a *v1alpha1.Allocation) *pfake.Batch {
	if ref := a.Spec.ProviderConfigReference; ref != nil {
		for _, x := range rg.extra {
			if x.name == ref.Name {
				return x.batch
			}
		}
	}
	return rg.batch
}

// providerRef is an Allocation's reference to its Provider.
func providerRef(name string) *xpv2.ProviderConfigReference {
	return &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: name}
}
