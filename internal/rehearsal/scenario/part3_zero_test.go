package scenario_test

// The end of issue #193's scenario: both served deployments scale to 0 and
// hpcburst down stops the stack, held to #146's step (g); the Allocation the
// requeue failed on purpose is the one allowed to have been Failed.

import (
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

func (p3 *part3) scaleToZero() {
	r := p3.rig
	r.t.Helper()
	uids := map[types.UID]string{p3.requeued.uid: p3.requeued.alloc}
	for _, a := range r.allocations() {
		uids[a.UID] = a.Name
	}
	insts := r.watch.uids()
	for _, in := range r.instances() {
		if !slices.Contains(insts, in.UID) {
			insts = append(insts, in.UID)
		}
	}
	patchAt := time.Now()
	for _, d := range p3.deps {
		var md v1alpha1.ModelDeployment
		if !r.get(&md, d.name, namespace) {
			r.t.Fatalf("scale to 0: no ModelDeployment %s", d.name)
		}
		patch := client.MergeFrom(md.DeepCopy())
		md.Spec.Instances = 0
		ctx, cancel := stack.CallCtx(r.t)
		err := r.c.Patch(ctx, &md, patch)
		cancel()
		if err != nil {
			r.t.Fatalf("scale %s to 0: %v", d.name, err)
		}
	}
	r.waitAllEnded(insts)
	r.checkShutdown(insts, patchAt)
	r.checkAllocations(uids, map[types.UID]bool{p3.requeued.uid: true})
	r.waitRecords(uids)
	r.waitJobsGone()
	r.checkProxiesOffline(r.watch.uids())
	r.assertNoOrphans("part 3")
	r.downAndCheck()
}
