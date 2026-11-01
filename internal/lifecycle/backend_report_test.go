package lifecycle

import (
	"errors"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// TestBackendRefusesDuplicateWorkerNames: a report that lists a worker name
// twice would give duplicate series and fail the whole scrape. The Backend
// refuses it as a bad request, at its boundary and before it stores anything,
// so the earlier report stays the latest and /metrics answers 200 with one
// series per worker. A report of two distinctly named workers is accepted.
func TestBackendRefusesDuplicateWorkerNames(t *testing.T) {
	rg, in, alloc := healthRig(t, func() afake.Engine { return afake.Engine{Health: knownHealth} })
	uid := string(in.UID)
	a, ok := rg.allocation(alloc)
	if !ok {
		t.Fatalf("no Allocation %s", alloc)
	}
	ra := rg.agentOf(alloc)
	caller := agentapi.Caller{Namespace: rigNS, Allocation: alloc, AllocationUID: string(a.UID), AgentInstanceID: ra.agent.AgentInstanceID()}
	rep, ok := ra.agent.LastReport()
	if !ok || rep.Instance == nil || len(rep.Instance.Workers) != 1 {
		t.Fatalf("last report %+v, want an Instance report of one worker", rep)
	}
	withWorkers := func(ws ...agentapi.WorkerReport) agentapi.Report {
		r := rep
		inst := *rep.Instance
		inst.Workers = ws
		r.Instance = &inst
		return r
	}
	w0 := rep.Instance.Workers[0]
	_, atBefore, _ := rg.backend.latest(a.UID)

	rg.clock.Advance(time.Second) // a stored report would carry a later time
	err := rg.backend.Report(rg.ctx, caller, withWorkers(w0, w0))
	var ae *agentapi.Error
	if !errors.As(err, &ae) || ae.Code != agentapi.CodeBadRequest {
		t.Fatalf("a report listing worker-0 twice: %v, want an agentapi.Error of code BadRequest", err)
	}
	if got, at, _ := rg.backend.latest(a.UID); at != atBefore || len(got.Instance.Workers) != 1 {
		t.Fatalf("after the refusal the latest report is at %s with %d workers, want the earlier one (%s, 1 worker)", at, len(got.Instance.Workers), atBefore)
	}
	ex := scrape(t, rg) // fails the test unless /metrics answers 200
	if n := len(ex.find("hpcburst_instance_running_requests", map[string]string{"instance_uid": uid})); n != 1 {
		t.Fatalf("%d running-requests series after the refused report, want 1", n)
	}

	w1 := w0
	w1.Name = "worker-1"
	if err := rg.backend.Report(rg.ctx, caller, withWorkers(w0, w1)); err != nil {
		t.Fatalf("a report of worker-0 and worker-1: %v, want it accepted", err)
	}
	ex = scrape(t, rg)
	if n := len(ex.find("hpcburst_instance_running_requests", map[string]string{"instance_uid": uid})); n != 2 {
		t.Fatalf("%d running-requests series for two distinct workers, want 2", n)
	}
}
