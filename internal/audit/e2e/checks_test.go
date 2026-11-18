package e2e

// What each part of the one run asserts, each from the audit log's FILES (see run_test.go).

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// checkLife is (b): each write is under its own identity, at its level, with its content.
func (s *scenario) checkLife(t *testing.T, set *logSet) {
	w, planID, plannedA, purposeA, purposeB := s.world, s.planID, s.plannedA, s.purposeA, s.purposeB
	// The Operation's create by its requester, at RequestResponse, with its spec and its purpose.
	for _, c := range []struct {
		name, deployment string
		by               who
		want             string
	}{{"end-a", "md", w.a, purposeA}, {"end-b", "md2", w.b, purposeB}} {
		evs := set.find("create", "operations", "", c.name, c.by.user)
		if len(evs) != 1 {
			t.Errorf("Operation %s: %d create events by %s, want 1", c.name, len(evs), c.by.user)
			continue
		}
		e := evs[0]
		op := e.requestOperation(t)
		if e.Level != "RequestResponse" || e.ResponseStatus.Code != 201 || len(e.ResponseObject) == 0 ||
			op.Spec.Action != v1alpha1.OperationActionEnd || op.Spec.ModelDeploymentRef.Name != c.deployment || op.Spec.Owner != c.by.user ||
			op.Annotations[v1alpha1.AnnotationPurpose] != c.want {
			t.Errorf("the create of %s is logged as %s (code %d, response %t), with spec %+v and purpose %q; want RequestResponse, 201, a response, the End of %s owned by %s, purpose %q",
				c.name, e.Level, e.ResponseStatus.Code, len(e.ResponseObject) > 0, op.Spec, op.Annotations[v1alpha1.AnnotationPurpose], c.deployment, c.by.user, c.want)
		}
	}
	// The lifecycle identity's status write carries the plan (hasPlanWrite waited for one); never the person's or an
	// agent's: nobody else wrote an Operation's status.
	for _, e := range set.find("update", "operations", "status", "", "") {
		if e.User.Username != w.lc.user {
			t.Errorf("an Operation's status was written by %q, want the lifecycle identity %q only: %s", e.User.Username, w.lc.user, e)
		}
	}
	var planWrite *auditEvent
	for _, e := range set.find("update", "operations", "status", "end-a", w.lc.user) {
		if e.Level != "RequestResponse" {
			t.Errorf("a status write of end-a is logged at %s, want RequestResponse: %s", e.Level, e)
		}
		if op := e.requestOperation(t); op.Status.Plan != nil && op.Status.Plan.ID == planID && planWrite == nil {
			e := e
			planWrite = &e
		}
	}
	if planWrite == nil {
		t.Fatalf("no status write of end-a by the lifecycle identity carries the plan %s", planID)
	}
	if plan := planWrite.requestOperation(t).Status.Plan; len(plan.Targets) == 0 {
		t.Errorf("the logged plan %s has no targets", planID)
	}
	// A's confirm, at RequestResponse, with the plan ID it confirmed.
	confirms := set.find("update", "operations", "", "end-a", w.a.user)
	if c := confirms[len(confirms)-1]; c.Level != "RequestResponse" || c.requestOperation(t).Spec.Confirm == nil || c.requestOperation(t).Spec.Confirm.PlanID != planID {
		t.Errorf("A's confirm is logged as %s with %q, want RequestResponse and the plan %s", c, c.RequestObject, planID)
	}
	// The Approval's create by the approver, with its plan ID and the Operation it names.
	aps := set.find("create", "approvals", "", "ap-person", w.admin.user)
	if len(aps) != 1 {
		t.Fatalf("%d Approval creates by the person, want 1", len(aps))
	}
	ap := aps[0].requestApproval(t)
	if aps[0].Level != "RequestResponse" || ap.Spec.PlanID != planID || ap.Spec.Operation.Name != "end-a" || ap.Spec.Operation.UID != plannedA.UID ||
		ap.Spec.Approver.Username != w.admin.user {
		t.Errorf("the Approval is logged as %s with spec %+v, want RequestResponse, plan %s, Operation end-a (%s), approver %s", aps[0], ap.Spec, planID, plannedA.UID, w.admin.user)
	}
	// The two agents are two user names, neither the administrator's, as the API server said at SelfSubjectReview.
	a, b := set.find("create", "operations", "", "end-a", "")[0], set.find("create", "operations", "", "end-b", "")[0]
	if a.User.Username == b.User.Username || a.User.Username == w.admin.user || b.User.Username == w.admin.user ||
		a.User.Username != "system:serviceaccount:hpcburst-agents:agent-a" || b.User.Username != "system:serviceaccount:hpcburst-agents:agent-b" {
		t.Errorf("the two agents' creates are logged under %q and %q (the administrator is %q), want agent-a and agent-b of hpcburst-agents", a.User.Username, b.User.Username, w.admin.user)
	}
	// No WRITE of an Operation, Approval or ApprovalPolicy in this run is logged below RequestResponse (its reads are Metadata).
	for _, e := range set.events {
		switch e.ObjectRef.Resource {
		case "operations", "approvals", "approvalpolicies":
			write := e.Verb == "create" || e.Verb == "update" || e.Verb == "patch" || e.Verb == "delete" || e.Verb == "deletecollection"
			if write && e.ObjectRef.APIGroup == v1alpha1.Group && e.Level != "RequestResponse" && e.ResponseStatus.Code != 0 {
				t.Errorf("a write of %s is logged at %s, want RequestResponse: %s", e.ObjectRef.Resource, e.Level, e)
				return
			}
		}
	}
}

// checkEnd is (b)'s execution: the approved End ran, and the log holds its outcome status write with the per-target outcomes, and
// for each Allocation it released the release-cause update at level Request, naming the Operation, and then the delete. The log's
// files are read, never the objects: the Operation and its Allocations are read back only to know which names to look for.
func (s *scenario) checkEnd(t *testing.T, set *logSet) {
	w := s.world
	done := w.getOperation("end-a")
	var outcomeWrite *auditEvent
	for _, e := range set.find("update", "operations", "status", "end-a", w.lc.user) {
		e := e
		if op := e.requestOperation(t); op.Status.Phase == v1alpha1.OperationPhaseDone && len(op.Status.Targets) > 0 {
			outcomeWrite = &e
		}
	}
	if outcomeWrite == nil {
		t.Fatalf("no status write of end-a by the lifecycle identity carries phase Done and the per-target outcomes")
	}
	if outcomeWrite.Level != "RequestResponse" {
		t.Errorf("the outcome write is logged at %s, want RequestResponse", outcomeWrite.Level)
	}
	for _, o := range outcomeWrite.requestOperation(t).Status.Targets {
		if o.State != v1alpha1.TargetStateDone {
			t.Errorf("the logged outcome of %s %s is %s (%s), want Done", o.Kind, o.Name, o.State, o.Reason)
		}
	}
	released := w.releasedAllocations(set, done)
	if len(released) == 0 {
		t.Fatalf("the plan names no Allocation that the End released")
	}
	for _, name := range released {
		var cause, del *auditEvent
		for _, e := range set.find("update", "allocations", "", name, w.lc.user) {
			e := e
			var a v1alpha1.Allocation
			if jsonUnmarshal(e.RequestObject, &a) == nil && a.Annotations[v1alpha1.AnnotationReleaseCause] != "" && cause == nil {
				cause = &e
			}
		}
		if ds := set.find("delete", "allocations", "", name, w.lc.user); len(ds) > 0 {
			del = &ds[0]
		}
		if cause == nil || del == nil {
			t.Errorf("Allocation %s: cause write %v, delete %v in the log, want both", name, cause != nil, del != nil)
			continue
		}
		var a v1alpha1.Allocation
		_ = jsonUnmarshal(cause.RequestObject, &a)
		if ns, op, uid, ok := v1alpha1.ParseReleaseCauseOperation(a.Annotations[v1alpha1.AnnotationReleaseCause]); !ok || ns != exampleNS || op != "end-a" || uid != done.UID {
			t.Errorf("Allocation %s: the logged release cause is %q, want the Operation end-a %s", name, a.Annotations[v1alpha1.AnnotationReleaseCause], done.UID)
		}
		if cause.Level != "Request" || del.Level != "Request" {
			t.Errorf("Allocation %s: the cause write is logged at %s and the delete at %s, want Request for both", name, cause.Level, del.Level)
		}
		if !cause.RequestReceivedTimestamp.Before(del.RequestReceivedTimestamp) && !cause.RequestReceivedTimestamp.Equal(del.RequestReceivedTimestamp) {
			t.Errorf("Allocation %s: the cause write (%s) is not before the delete (%s): the cause must be logged before the delete", name, cause.RequestReceivedTimestamp, del.RequestReceivedTimestamp)
		}
	}
}

// checkReadsAreMetadata is the S3 policy in the log FILE: the controllers' reads of Operations, Approvals and ApprovalPolicies, which
// they make on every reconcile pass, are logged at Metadata with no body, and no logged body, of any level, carries managedFields.
func (s *scenario) checkReadsAreMetadata(t *testing.T, set *logSet) {
	w := s.world
	lists := set.find("list", "operations", "", "", w.lc.user)
	if len(lists) == 0 {
		t.Fatalf("the lifecycle identity listed Operations on no pass in this run, so no read of them is in the log to judge")
	}
	for _, e := range lists {
		if e.Level != "Metadata" || len(e.RequestObject) != 0 || len(e.ResponseObject) != 0 {
			t.Errorf("a list of Operations by the lifecycle identity is logged at %s with a body of %d and %d bytes, want Metadata and none: %s", e.Level, len(e.RequestObject), len(e.ResponseObject), e)
			break
		}
	}
	// The writes still carry their bodies: an Operation's create is the one the run made (checkLife reads its content).
	if creates := set.find("create", "operations", "", "end-a", w.a.user); len(creates) != 1 || len(creates[0].RequestObject) == 0 {
		t.Errorf("agent A's Operation create has no body: %v", creates)
	}
	var withBodies, withManaged int
	for _, e := range set.events {
		if len(e.RequestObject) == 0 && len(e.ResponseObject) == 0 {
			continue
		}
		withBodies++
		if managedFieldsBytes(e.RequestObject)+managedFieldsBytes(e.ResponseObject) > 0 {
			withManaged++
			if withManaged == 1 {
				t.Errorf("a logged body carries managedFields: %s", e)
			}
		}
	}
	if withBodies == 0 {
		t.Errorf("no event of the run carries a body, so the managedFields check judged nothing")
	}
	if withManaged > 1 {
		t.Errorf("%d of the %d events with a body carry managedFields", withManaged, withBodies)
	}
}

// checkSecretLogged is (e), before the rotation: the Secret's requests are at Metadata with no body, and the matcher sees a body.
func (s *scenario) checkSecretLogged(t *testing.T, set *logSet) {
	w, markerB := s.world, s.markerB
	for _, verb := range []string{"create", "get", "patch"} {
		evs := set.find(verb, "secrets", "", "audit-secret", w.admin.user)
		if len(evs) == 0 {
			t.Errorf("no %s of the Secret in the log", verb)
			continue
		}
		for _, e := range evs {
			if e.Level != "Metadata" || len(e.RequestObject) != 0 || len(e.ResponseObject) != 0 {
				t.Errorf("the Secret's %s is logged at %s with a body (%d, %d bytes), want Metadata and none", verb, e.Level, len(e.RequestObject), len(e.ResponseObject))
			}
		}
	}
	// The control: the same matcher finds a marker where the log DOES hold a body, so a clean result below is not
	// a matcher that cannot see one.
	if files := set.filesWithToken(markerB); len(files) == 0 {
		t.Errorf("the matcher does not find agent B's marker, which is in its Operation's logged purpose: a Secret check with it proves nothing")
	}
	if evs := set.find("create", "operations", "", "end-b", w.b.user); len(evs) == 1 && !containsToken(evs[0].RequestObject, markerB) {
		t.Errorf("the control's marker is not in the request body of agent B's create")
	}
}

// checkLease: the lifecycle Lease is held and has been renewed, and no controller's Lease read or renewal is in the log.
func (s *scenario) checkLease(t *testing.T) {
	w, rd := s.world, s.rd
	var lease coordinationv1.Lease
	eventually(t, 90*time.Second, "the lifecycle Lease is held and has been renewed", func() (bool, string) {
		if err := w.admin.c.Get(w.ctx, types.NamespacedName{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle}, &lease); err != nil {
			return false, err.Error()
		}
		s := lease.Spec
		return s.HolderIdentity != nil && *s.HolderIdentity != "" && s.AcquireTime != nil && s.RenewTime != nil && s.RenewTime.After(s.AcquireTime.Time),
			fmt.Sprintf("lease %+v", s)
	})
	set := rd.read(t)
	// The control: the log does record a Lease event of the lifecycle identity, its create (rule 1 lists no create).
	if len(set.find("create", "leases", "", v1alpha1.LeaseLifecycle, w.lc.user)) == 0 {
		t.Errorf("the log holds no create of the lifecycle Lease by %s: the log does not see the controller's Lease at all", w.lc.user)
	}
	for _, e := range set.events {
		if e.ObjectRef.Resource != "leases" {
			continue
		}
		switch e.Verb {
		case "get", "update", "list", "watch":
			if strings.HasPrefix(e.User.Username, "system:serviceaccount:hpcburst-system:") {
				t.Errorf("a controller's Lease read or renewal is in the log, though rule 1 puts it at None: %s", e)
				return
			}
		}
	}
}

// checkSecretAfterRotation is (e), after the rotation: the Secret's value is in no file, plain or gunzipped.
func (s *scenario) checkSecretAfterRotation(t *testing.T) {
	rd, secretMarker, markerB := s.rd, s.secretMarker, s.markerB
	set := rd.read(t)
	if len(set.rotatedNames()) < 5 {
		t.Fatalf("only %d rotated files: the Secret's lines would not have been through a rotation", len(set.rotatedNames()))
	}
	for _, form := range []struct{ name, token string }{
		{"raw", secretMarker},
		{"base64", base64.StdEncoding.EncodeToString([]byte(secretMarker))},
		{"unpadded base64", base64.RawStdEncoding.EncodeToString([]byte(secretMarker))},
		{"URL base64", base64.URLEncoding.EncodeToString([]byte(secretMarker))},
		{"unpadded URL base64", base64.RawURLEncoding.EncodeToString([]byte(secretMarker))},
	} {
		if files := set.filesWithToken(form.token); len(files) != 0 {
			t.Errorf("the Secret's value, in its %s form, is in the audit log files %v", form.name, files)
		}
	}
	if len(set.filesWithToken(markerB)) == 0 {
		t.Errorf("the control's marker is gone from every file after the rotation")
	}
}

// checkNoRefusals: no request of a controller identity was answered 403.
func (s *scenario) checkNoRefusals(t *testing.T) {
	rd := s.rd
	set := rd.read(t)
	for _, e := range set.events {
		if e.ResponseStatus.Code == 403 && strings.HasPrefix(e.User.Username, "system:serviceaccount:hpcburst-system:") {
			t.Errorf("a controller identity was refused (403), so its role lacks what the code calls (config/rbac/controllers.yaml): %s", e)
		}
	}
}

// checkAllocationWrite is rule 3 in the log FILE: the lifecycle identity's update of an Allocation is at level Request, with
// the request body (the idle-since annotation it wrote), and no response body: the level is not RequestResponse. The levels
// of every rule are judged by the API server's own evaluator, in config/audit; this is what the API server wrote.
func (s *scenario) checkAllocationWrite(t *testing.T, set *logSet) {
	w := s.world
	var n int
	for _, e := range set.find("update", "allocations", "", s.keptAlloc.Name, w.lc.user) {
		n++
		if e.Level != "Request" || len(e.RequestObject) == 0 || len(e.ResponseObject) != 0 {
			t.Errorf("the lifecycle identity's update of Allocation %s is logged at %s with a request body of %d bytes and a response body of %d, want Request, a request body and no response body: %s",
				s.keptAlloc.Name, e.Level, len(e.RequestObject), len(e.ResponseObject), e)
		}
	}
	if n == 0 {
		t.Errorf("the log holds no update of Allocation %s by the lifecycle identity", s.keptAlloc.Name)
	}
}
