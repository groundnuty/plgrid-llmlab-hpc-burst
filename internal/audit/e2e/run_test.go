package e2e

// ONE combined run (issue #660, PR B; plan 5a). The real lifecycle controller runs on its own ServiceAccount's token
// against a test API server that writes the audit log under the embedded policy with rotation at 1 MB. Two agents
// and the person act through their own credentials: agent A's Operation is planned by the controller, confirmed by
// A, and approved by the person; agent B makes an Operation of its own; a Secret is created. Every assertion reads
// the API server's audit log FILES (the plain one and the rotated gzip ones), each event under its own user.username.
//
// What each part shows, and what it does not (the phases are in this file, their assertions in checks_test.go, the size
// report in size_test.go and the rotation in rotation_test.go):
//   - (b) the Operation's life: the create by the requester at RequestResponse with its spec and purpose, the lifecycle
//     identity's operations/status write with the plan, the confirm, the Approval's create by the approver with its
//     plan ID, and the two agents under different user names, neither the administrator's;
//   - (e) a Secret whose value is a random marker is created, read and patched, and the marker, in its raw and
//     base64 forms, is in no log file, plain or gunzipped, as a whole token; the same matcher finds a marker placed in an
//     Operation's logged body, so a clean result is not a matcher that cannot see a body;
//   - (d) the log rotates, with more rotated files than the old maxbackup=3 kept, each a gzip file and none removed,
//     the first request's line still found in the oldest file;
//   - the controllers' Lease reads and renewals are not in the log (rule 1, None), shown beside a renewal that did
//     happen (the Lease's own renewTime) and a Lease create that is logged;
//   - the size of the log is reported per Operation and per minute of an idle controller.
//
// The levels of every rule are (a)'s, in config/audit, judged by k8s.io/apiserver's own evaluator.

import (
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// scenario is the one run's state: what each phase made and the next one reads.
type scenario struct {
	*world
	rd   *reader
	prov *v1alpha1.Provider

	keptAlloc v1alpha1.Allocation // md2's started Allocation, held by no Instance: rule 3's subject

	plannedA               *v1alpha1.Operation // agent A's Operation as planned, whose plan ID the confirm and the Approval carry
	planID                 string
	purposeA, purposeB     string    // the Operations' purposes; B's holds markerB
	markerB, secretMarker  string    // markerB is the control of (e), in a logged body; secretMarker is the Secret's value
	windowStart, windowEnd time.Time // the span in which the two Operations were made, for the size report
}

func TestTheAuditLogOfAnOperationsLifeAndItsRotation(t *testing.T) {
	s := newScenario(t)
	s.deploy()
	s.keep()
	s.operations()
	s.secret()
	set := s.waitLife()
	t.Run("b: each write under its own identity, at its level, with its content", func(t *testing.T) { s.checkLife(t, set) })
	t.Run("rule 3: the lifecycle's update of an Allocation is logged at Request, with its body", func(t *testing.T) { s.checkAllocationWrite(t, set) })
	t.Run("e: the Secret's requests are logged at Metadata, and its content is not in any file", func(t *testing.T) { s.checkSecretLogged(t, set) })
	t.Run("lease: the controller's renewals happened and are not in the log", s.checkLease)
	t.Run("size: the log per Operation and per minute of an idle controller", func(t *testing.T) {
		s.world.reportSize(t, s.rd, s.windowStart, s.windowEnd, 2)
	})
	t.Run("d: rotation keeps every file, gzipped", func(t *testing.T) { s.world.rotation(t, s.rd) })
	t.Run("e: after the rotation, in every file", s.checkSecretAfterRotation)
	t.Run("no controller identity was refused", s.checkNoRefusals)
}

// newScenario starts the world, the Provider and the lifecycle controller on its own token.
func newScenario(t *testing.T) *scenario {
	t.Helper()
	w := newWorld(t)
	s := &scenario{world: w, rd: newReader(w.logPath), prov: w.makeProvider()}
	w.keepProviderFresh(s.prov)
	w.startLifecycle(nil)
	return s
}

// deploy makes the ModelDeployment: the controller, on its own token, makes an Instance and an Allocation, still
// pending (no Provider controller runs here), so the End below has a pending Instance to cancel, which needs approval.
func (s *scenario) deploy() {
	t, w := s.t, s.world
	t.Helper()
	if err := w.admin.c.Create(w.ctx, deployment("md")); err != nil {
		t.Fatal(err)
	}
	eventually(t, 90*time.Second, "an Instance and its Allocation, made by the lifecycle controller", func() (bool, string) {
		var insts v1alpha1.InstanceList
		var allocs v1alpha1.AllocationList
		if err := w.admin.c.List(w.ctx, &insts, client.InNamespace(exampleNS)); err != nil {
			return false, err.Error()
		}
		if err := w.admin.c.List(w.ctx, &allocs, client.InNamespace(exampleNS)); err != nil {
			return false, err.Error()
		}
		return len(insts.Items) == 1 && len(allocs.Items) == 1, fmt.Sprintf("%d Instances, %d Allocations", len(insts.Items), len(allocs.Items))
	})
}

// operations has agents A and B create their Operations, A confirm its plan and the person approve it.
func (s *scenario) operations() {
	t, w := s.t, s.world
	t.Helper()
	windowStart := time.Now()
	const purposeA = "end the example deployment once its run is over"
	secretMarker := randomHex(t, 16)       // (e): the Secret's value
	purposeB := "note " + randomHex(t, 16) // (e)'s control: the same kind of marker, in an Operation's purpose, which IS logged
	markerB := strings.TrimPrefix(purposeB, "note ")

	opA := endOperation("end-a", w.a, purposeA)
	if err := w.a.c.Create(w.ctx, opA); err != nil {
		t.Fatalf("agent A creates its Operation: %v", err)
	}
	opB := endOperation("end-b", w.b, purposeB)
	if err := w.b.c.Create(w.ctx, opB); err != nil {
		t.Fatalf("agent B creates its Operation: %v", err)
	}
	plannedA := w.waitPlan("end-a")
	w.waitPlan("end-b")
	planID := plannedA.Status.Plan.ID

	// A confirms the plan it read, and the person approves it, each as itself.
	var fresh v1alpha1.Operation
	if err := w.a.c.Get(w.ctx, client.ObjectKeyFromObject(opA), &fresh); err != nil {
		t.Fatal(err)
	}
	fresh.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: planID}
	if err := w.a.c.Update(w.ctx, &fresh); err != nil {
		t.Fatalf("agent A confirms its plan: %v", err)
	}
	if err := w.admin.c.Create(w.ctx, w.approval("ap-person", plannedA, planID, w.admin)); err != nil {
		t.Fatalf("the person's Approval: %v", err)
	}
	eventually(t, 90*time.Second, "the controller counts the Approval", func() (bool, string) {
		op := w.getOperation("end-a")
		c := apimeta.FindStatusCondition(op.Status.Conditions, v1alpha1.ConditionExecutable)
		if c == nil {
			return false, fmt.Sprintf("phase %q, no Executable condition", op.Status.Phase)
		}
		return c.Reason == v1alpha1.ReasonExecutionNotBuilt, fmt.Sprintf("phase %q, Executable %s: %s", op.Status.Phase, c.Reason, c.Message)
	})
	windowEnd := time.Now()
	s.plannedA, s.planID, s.purposeA, s.purposeB, s.markerB, s.secretMarker, s.windowStart, s.windowEnd =
		plannedA, planID, purposeA, purposeB, markerB, secretMarker, windowStart, windowEnd
}

// secret has the person create, read and patch a Secret whose value is the random marker.
func (s *scenario) secret() {
	t, w := s.t, s.world
	t.Helper()
	secretMarker := s.secretMarker
	secrets := w.admin.c
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "audit-secret", Namespace: exampleNS}, Data: map[string][]byte{"password": []byte(secretMarker)}}
	if err := secrets.Create(w.ctx, sec); err != nil {
		t.Fatal(err)
	}
	var got corev1.Secret
	if err := secrets.Get(w.ctx, client.ObjectKeyFromObject(sec), &got); err != nil || string(got.Data["password"]) != secretMarker {
		t.Fatalf("read the Secret back: %v", err) // the Secret holds the marker: its absence from the log is not an absence from the world
	}
	if err := secrets.Patch(w.ctx, &got, client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"labels":{"example.test/touched":"yes"}}}`))); err != nil {
		t.Fatal(err)
	}
}

// waitLife reads the log's FILES until everything (b) and (e) look for is in them, and returns what it read.
func (s *scenario) waitLife() *logSet {
	w, rd, planID, name := s.world, s.rd, s.planID, s.keptAlloc.Name
	// Everything (b) says, read from the log's FILES.
	return w.waitLog(rd, "(b) the Operation's life and the Secret's requests", func(s *logSet) bool {
		return len(s.find("create", "operations", "", "end-a", w.a.user)) > 0 &&
			len(s.find("create", "operations", "", "end-b", w.b.user)) > 0 &&
			len(s.find("update", "operations", "", "end-a", w.a.user)) > 0 &&
			len(s.find("create", "approvals", "", "ap-person", w.admin.user)) > 0 &&
			hasPlanWrite(s, w.lc.user, "end-a", planID) &&
			hasIdleWrite(s, w.lc.user, name) &&
			len(s.find("create", "secrets", "", "audit-secret", w.admin.user)) > 0 &&
			len(s.find("patch", "secrets", "", "audit-secret", w.admin.user)) > 0 &&
			len(s.find("get", "secrets", "", "audit-secret", w.admin.user)) > 0
	})
}
