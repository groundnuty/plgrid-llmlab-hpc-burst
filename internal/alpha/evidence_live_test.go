package alpha

// What a failed live run keeps (evidence.go; issue #503). The steps keep the stack's logs and each Instance's agent log in the record
// directory when the run failed, whichever way: a Fatalf in a step (the test is failed), or a failed check or a broken run (Run says so
// through KeepEvidence). A run that passed, and a NOT RUN, keep nothing. Nothing here is posted.

import (
	"context"
	"fmt"

	v1alpha1 "github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// KeepEvidence is Run's word that the run failed in a way the steps did not Fatalf on (see Run, evidenceKeeper).
func (r *liveRun) KeepEvidence() { r.keepEvidence = true }

// keepStackLogs copies the stack's logs into the record directory (KeepStackLogs). It is for the end of a failed run, once the stack has
// stopped: the files are complete. Its result goes to the test's private log by name and count only.
func (r *liveRun) keepStackLogs() {
	if r.h == nil || r.cfg.Out == "" {
		return
	}
	kept, err := KeepStackLogs(r.h.State, r.cfg.Out)
	if err != nil {
		r.t.Logf("the stack's logs could not all be kept for a failed run: %v", err)
	}
	if len(kept) > 0 {
		r.t.Logf("%s: the run failed: the stack's logs (%d files) are kept in the record directory's %s directory, never posted", r.cfg.Row.Name, len(kept), EvidenceDir)
	}
}

// keepAgentLogs reads the agent's log of the job of each Instance of the run, over the run's own transport, and keeps it in the record
// directory. It runs while the API still answers and the jobs still run; a read that fails is logged and the others go on. Best effort:
// a run that failed before it had an Instance, a transport or an API has nothing to keep.
func (r *liveRun) keepAgentLogs(ctx context.Context) {
	if r.c == nil || r.batch == nil || r.tr == nil || r.cfg.Out == "" || r.w.md == nil {
		return
	}
	cctx, cancel := stack.CallCtx(r.t)
	var il v1alpha1.InstanceList
	err := r.c.List(cctx, &il, client.InNamespace(r.w.md.Namespace))
	cancel()
	if err != nil {
		r.t.Logf("the run's Instances could not be listed to keep their agents' logs: %v", err)
		return
	}
	for i, in := range il.Items {
		b, err := r.agentLog(ctx, in)
		if err != nil {
			r.t.Logf("the agent's log of Instance %d could not be read to keep it: %v", i+1, err)
			continue
		}
		if err := KeepBytes(r.cfg.Out, fmt.Sprintf("agent-%d.log", i+1), b); err != nil {
			r.t.Logf("the agent's log of Instance %d could not be kept: %v", i+1, err)
			continue
		}
		r.t.Logf("%s: the run failed: the agent's log of Instance %d (%d bytes) is kept in the record directory's %s directory, never posted", r.cfg.Row.Name, i+1, len(b), EvidenceDir)
	}
}
