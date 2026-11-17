package e2e

// ONE combined run (issue #660, PR B; plan 5a). The real lifecycle controller runs on its own ServiceAccount's token
// against a test API server that writes the audit log under the embedded policy with rotation at 1 MB. Two agents
// and the person act through their own credentials: agent A's Operation is planned by the controller, confirmed by
// A, and approved by the person; agent B makes an Operation of its own; a Secret is created. Every assertion reads
// the API server's audit log FILES (the plain one and the rotated gzip ones), each event under its own user.username.
//
// What each part shows, and what it does not:
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
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func randomHex(t testing.TB, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// keepProviderFresh re-marks the Provider ready every 15 s, as its controller would, so that the lifecycle
// controller keeps seeing a recent successful read for as long as the run lasts. It stops with the test.
func (w *world) keepProviderFresh(prov *v1alpha1.Provider) {
	ctx, cancel := context.WithCancel(w.ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(15 * time.Second):
			}
			var cur v1alpha1.Provider
			if err := w.admin.c.Get(ctx, client.ObjectKeyFromObject(prov), &cur); err != nil {
				continue
			}
			apimeta.SetStatusCondition(&cur.Status.Conditions, metav1.Condition{Type: v1alpha1.ConditionProviderReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPolled, LastTransitionTime: metav1.Now()})
			at := metav1.NewTime(time.Now().UTC().Truncate(time.Second))
			cur.Status.LastSuccessfulStatusRead = &at
			_ = w.admin.c.Status().Update(ctx, &cur, client.FieldOwner(v1alpha1.FieldManagerProvider)) // the next tick tries again
		}
	}()
	w.t.Cleanup(func() { cancel(); wg.Wait() })
}

// endOperation builds an End Operation of the deployment, owned by the identity that creates it.
func endOperation(name string, owner who, purpose string) *v1alpha1.Operation {
	return &v1alpha1.Operation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: exampleNS, Annotations: map[string]string{v1alpha1.AnnotationPurpose: purpose}},
		Spec:       v1alpha1.OperationSpec{Action: v1alpha1.OperationActionEnd, ModelDeploymentRef: v1alpha1.ObjectReference{Name: "md"}, Owner: owner.user},
	}
}

func (w *world) getOperation(name string) *v1alpha1.Operation {
	w.t.Helper()
	var op v1alpha1.Operation
	if err := w.admin.c.Get(w.ctx, types.NamespacedName{Namespace: exampleNS, Name: name}, &op); err != nil {
		w.t.Fatalf("get Operation %s: %v", name, err)
	}
	return &op
}

// waitPlan waits until the controller has written the Operation's plan, and returns the Operation.
func (w *world) waitPlan(name string) *v1alpha1.Operation {
	w.t.Helper()
	var op *v1alpha1.Operation
	eventually(w.t, 90*time.Second, "the plan of Operation "+name, func() (bool, string) {
		var cur v1alpha1.Operation
		if err := w.admin.c.Get(w.ctx, types.NamespacedName{Namespace: exampleNS, Name: name}, &cur); err != nil {
			return false, err.Error()
		}
		op = &cur
		return cur.Status.Phase == v1alpha1.OperationPhasePlanned && cur.Status.Plan != nil, fmt.Sprintf("phase %q, plan %v", cur.Status.Phase, cur.Status.Plan != nil)
	})
	return op
}

func (w *world) approval(name string, op *v1alpha1.Operation, planID string, by who) *v1alpha1.Approval {
	return &v1alpha1.Approval{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: exampleNS},
		Spec: v1alpha1.ApprovalSpec{Operation: v1alpha1.OperationReference{Name: op.Name, UID: op.UID}, PlanID: planID,
			Approver: v1alpha1.ApproverIdentity{Username: by.user, Groups: by.groups}},
	}
}

func TestTheAuditLogOfAnOperationsLifeAndItsRotation(t *testing.T) {
	w := newWorld(t)
	rd := newReader(w.logPath)
	prov := w.makeProvider()
	w.keepProviderFresh(prov)
	w.startLifecycle(nil)

	// The deployment: the controller, on its own token, makes an Instance and an Allocation, still pending (no
	// Provider controller runs here), so the End below has a pending Instance to cancel, which needs approval.
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

	// ---- (b) the Operation's life ----
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

	// The Secret and a ConfigMap, as the person (e).
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

	// Everything (b) says, read from the log's FILES.
	set := w.waitLog(rd, "(b) the Operation's life and the Secret's requests", func(s *logSet) bool {
		return len(s.find("create", "operations", "", "end-a", w.a.user)) > 0 &&
			len(s.find("create", "operations", "", "end-b", w.b.user)) > 0 &&
			len(s.find("update", "operations", "", "end-a", w.a.user)) > 0 &&
			len(s.find("create", "approvals", "", "ap-person", w.admin.user)) > 0 &&
			hasPlanWrite(s, w.lc.user, "end-a", planID) &&
			len(s.find("create", "secrets", "", "audit-secret", w.admin.user)) > 0 &&
			len(s.find("patch", "secrets", "", "audit-secret", w.admin.user)) > 0 &&
			len(s.find("get", "secrets", "", "audit-secret", w.admin.user)) > 0
	})
	t.Run("b: each write under its own identity, at its level, with its content", func(t *testing.T) {
		// The Operation's create by its requester, at RequestResponse, with its spec and its purpose.
		for _, c := range []struct {
			name string
			by   who
			want string
		}{{"end-a", w.a, purposeA}, {"end-b", w.b, purposeB}} {
			evs := set.find("create", "operations", "", c.name, c.by.user)
			if len(evs) != 1 {
				t.Errorf("Operation %s: %d create events by %s, want 1", c.name, len(evs), c.by.user)
				continue
			}
			e := evs[0]
			op := e.requestOperation(t)
			if e.Level != "RequestResponse" || e.ResponseStatus.Code != 201 || len(e.ResponseObject) == 0 ||
				op.Spec.Action != v1alpha1.OperationActionEnd || op.Spec.ModelDeploymentRef.Name != "md" || op.Spec.Owner != c.by.user ||
				op.Annotations[v1alpha1.AnnotationPurpose] != c.want {
				t.Errorf("the create of %s is logged as %s (code %d, response %t), with spec %+v and purpose %q; want RequestResponse, 201, a response, the End of md owned by %s, purpose %q",
					c.name, e.Level, e.ResponseStatus.Code, len(e.ResponseObject) > 0, op.Spec, op.Annotations[v1alpha1.AnnotationPurpose], c.by.user, c.want)
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
		// No Operation, Approval or ApprovalPolicy request of this run is logged below RequestResponse.
		for _, e := range set.events {
			switch e.ObjectRef.Resource {
			case "operations", "approvals", "approvalpolicies":
				if e.ObjectRef.APIGroup == v1alpha1.Group && e.Level != "RequestResponse" && e.ResponseStatus.Code != 0 {
					t.Errorf("a request on %s is logged at %s, want RequestResponse: %s", e.ObjectRef.Resource, e.Level, e)
					return
				}
			}
		}
	})

	t.Run("e: the Secret's requests are logged at Metadata, and its content is not in any file", func(t *testing.T) {
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
	})

	t.Run("lease: the controller's renewals happened and are not in the log", func(t *testing.T) {
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
	})

	t.Run("size: the log per Operation and per minute of an idle controller", func(t *testing.T) {
		w.reportSize(t, rd, windowStart, windowEnd, 2)
	})

	t.Run("d: rotation keeps every file, gzipped", func(t *testing.T) {
		w.rotation(t, rd)
	})

	t.Run("e: after the rotation, in every file", func(t *testing.T) {
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
	})

	t.Run("no controller identity was refused", func(t *testing.T) {
		set := rd.read(t)
		for _, e := range set.events {
			if e.ResponseStatus.Code == 403 && strings.HasPrefix(e.User.Username, "system:serviceaccount:hpcburst-system:") {
				t.Errorf("a controller identity was refused (403), so its role lacks what the code calls (config/rbac/controllers.yaml): %s", e)
			}
		}
	})
}

// hasPlanWrite reports whether the lifecycle identity's status write of the Operation carries the plan.
func hasPlanWrite(s *logSet, lifecycleUser, name, planID string) bool {
	for _, e := range s.find("update", "operations", "status", name, lifecycleUser) {
		var op v1alpha1.Operation
		if len(e.RequestObject) > 0 && jsonUnmarshal(e.RequestObject, &op) == nil && op.Status.Plan != nil && op.Status.Plan.ID == planID {
			return true
		}
	}
	return false
}

// reportSize logs the log's size, in two figures. Per Operation: the bytes of the events on Operations, their status,
// Approvals and ApprovalPolicies in the window the two Operations lived in, split into writes (by anyone) and reads by
// the controllers; the test's own polling reads are listed apart and left out of the figure. Per minute of an idle
// controller: the lifecycle identity's events over a quiet window, by resource and verb, with the share that the
// objects' managedFields are of the bodies. The figures come from the stand-in's envtest run: projected to a week they
// are a projection from the stand-in (HBEP-10, Audit, Size; the science agent's ruling S3), never a live week's.
func (w *world) reportSize(t *testing.T, rd *reader, from, to time.Time, operations int) {
	t.Helper()
	set := rd.read(t)
	type tally struct{ n, bytes, mf int }
	var all tally
	classes := map[string]*tally{}
	var written, controllerReads, testReads tally
	for _, e := range set.events {
		if e.RequestReceivedTimestamp.Before(from) || e.RequestReceivedTimestamp.After(to) {
			continue
		}
		all.n++
		all.bytes += e.size
		switch e.ObjectRef.Resource {
		case "operations", "approvals", "approvalpolicies":
		default:
			continue
		}
		kind := e.ObjectRef.Resource
		if e.ObjectRef.Subresource != "" {
			kind += "/" + e.ObjectRef.Subresource
		}
		isRead := e.Verb == "get" || e.Verb == "list" || e.Verb == "watch"
		by := userKind(e.User.Username, w)
		controller := by == "lifecycle" || by == "provider"
		key := kind + " " + map[bool]string{true: "read", false: "write"}[isRead] + " by " + by
		if classes[key] == nil {
			classes[key] = &tally{}
		}
		classes[key].n++
		classes[key].bytes += e.size
		switch {
		case !isRead:
			written.bytes += e.size
		case controller:
			controllerReads.bytes += e.size
		default:
			testReads.bytes += e.size
		}
	}
	t.Logf("size: the window of the %d Operations, %.1f s: %d events, %d bytes of log in all", operations, to.Sub(from).Seconds(), all.n, all.bytes)
	for key, c := range classes {
		t.Logf("size:   %-48s %5d events %8d bytes", key, c.n, c.bytes)
	}
	t.Logf("size: per Operation: %d bytes of writes and %d bytes of the controllers' reads (the test's own polling, %d bytes, left out)",
		written.bytes/operations, controllerReads.bytes/operations, testReads.bytes)

	// An idle controller: the lifecycle identity's events over a window in which the test makes no request of its own.
	const quiet = 30 * time.Second
	start := time.Now()
	time.Sleep(quiet)
	end := time.Now()
	set = rd.read(t)
	perUser := map[string]*tally{}
	lifecycleBy := map[string]*tally{}
	var lifecycleBodies tally
	for _, e := range set.events {
		if e.RequestReceivedTimestamp.Before(start) || e.RequestReceivedTimestamp.After(end) {
			continue
		}
		u := userKind(e.User.Username, w)
		if perUser[u] == nil {
			perUser[u] = &tally{}
		}
		perUser[u].n++
		perUser[u].bytes += e.size
		if u != "lifecycle" {
			continue
		}
		r := e.ObjectRef.Resource
		if e.ObjectRef.Subresource != "" {
			r += "/" + e.ObjectRef.Subresource
		}
		key := e.Verb + " " + r + " at " + e.Level
		if lifecycleBy[key] == nil {
			lifecycleBy[key] = &tally{}
		}
		lifecycleBy[key].n++
		lifecycleBy[key].bytes += e.size
		lifecycleBodies.bytes += len(e.RequestObject) + len(e.ResponseObject)
		lifecycleBodies.mf += managedFieldsBytes(e.RequestObject) + managedFieldsBytes(e.ResponseObject)
	}
	perMinute := int(time.Minute / quiet)
	for u, c := range perUser {
		t.Logf("size: idle %.0f s, %-10s %4d events %7d bytes, %d bytes per minute", quiet.Seconds(), u, c.n, c.bytes, c.bytes*perMinute)
	}
	for key, c := range lifecycleBy {
		t.Logf("size:   an idle lifecycle controller: %-44s %4d events %7d bytes in %.0f s", key, c.n, c.bytes, quiet.Seconds())
	}
	if c := perUser["lifecycle"]; c != nil {
		t.Logf("size: an idle lifecycle controller writes %d bytes of audit log per minute (%d events), from the stand-in; managedFields are %d of the %d body bytes in its events",
			c.bytes*perMinute, c.n*perMinute, lifecycleBodies.mf, lifecycleBodies.bytes)
	} else {
		t.Logf("size: an idle lifecycle controller wrote no event in %.0f s", quiet.Seconds())
	}
}

// managedFieldsBytes is the size of every managedFields array in a logged body, found at any depth.
func managedFieldsBytes(raw json.RawMessage) int {
	if len(raw) == 0 {
		return 0
	}
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return 0
	}
	var walk func(any) int
	walk = func(x any) int {
		n := 0
		switch x := x.(type) {
		case map[string]any:
			for k, val := range x {
				if k == "managedFields" {
					b, _ := json.Marshal(val)
					n += len(b)
					continue
				}
				n += walk(val)
			}
		case []any:
			for _, val := range x {
				n += walk(val)
			}
		}
		return n
	}
	return walk(v)
}

// userKind names a user for the size report without printing a name that is not ours.
func userKind(user string, w *world) string {
	switch user {
	case w.lc.user:
		return "lifecycle"
	case w.pv.user:
		return "provider"
	case w.a.user:
		return "agent-a"
	case w.b.user:
		return "agent-b"
	case w.admin.user:
		return "person"
	}
	return "other"
}

// rotation sends requests until the log has rotated more times than the old maxbackup=3 kept, then shows that every
// rotated file is gzip, that none was removed, and that the first request's line is still in the oldest file.
func (w *world) rotation(t *testing.T, rd *reader) {
	t.Helper()
	dir := filepath.Dir(w.logPath)
	backups := func() map[string]bool {
		ents, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]bool{}
		for _, e := range ents {
			if base, ok := backupBase(e.Name()); ok {
				out[base] = true
			}
		}
		return out
	}
	seen := backups() // every rotated file name seen at any moment, ever
	before := len(seen)

	cfg := *w.admin.cfg
	cfg.QPS, cfg.Burst = 1000, 2000
	cs, err := kubernetes.NewForConfig(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	get := func(name, pad string) {
		err := cs.CoreV1().RESTClient().Get().AbsPath("/api/v1/namespaces/"+exampleNS+"/configmaps/"+name).Param("pad", pad).Do(w.ctx).Error()
		if err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("GET %s: %v", name, err)
		}
	}
	first := "rotation-first-" + randomHex(t, 8)
	get(first, "")
	pad := strings.Repeat("x", 4000)
	const batch, workers = 200, 8
	sent := 0
	for len(seen)-before < 5 {
		if sent >= 8000 {
			t.Fatalf("%d requests of 4 KB did not make the log rotate 5 times (the size is %d MB): %d new rotated files", sent, rotationMB, len(seen)-before)
		}
		var wg sync.WaitGroup
		for k := 0; k < workers; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < batch/workers; i++ {
					get("rotation-bulk-"+randomHex(t, 4), pad)
				}
			}()
		}
		wg.Wait()
		sent += batch
		for name := range backups() {
			seen[name] = true
		}
	}
	last := "rotation-last-" + randomHex(t, 8)
	get(last, "")
	t.Logf("rotation: %d requests of 4 KB made %d new rotated files (log size %d MB)", sent, len(seen)-before, rotationMB)

	// The gzip of a rotated file runs in the background: wait until no plain rotated file is left.
	eventually(t, 90*time.Second, "every rotated file is compressed", func() (bool, string) {
		ents, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		var plain []string
		for _, e := range ents {
			if _, ok := backupBase(e.Name()); ok && !strings.HasSuffix(e.Name(), ".gz") {
				plain = append(plain, e.Name())
			}
		}
		return len(plain) == 0, fmt.Sprintf("plain rotated files still there: %v", plain)
	})
	for name := range backups() {
		seen[name] = true
	}

	// Rotated files exist FIRST: a run that never rotates proves nothing about the rest.
	set := w.waitLog(rd, "the last request after the rotation", func(s *logSet) bool {
		return len(s.find("get", "configmaps", "", last, w.admin.user)) > 0
	})
	rotated := set.rotatedNames()
	if len(rotated) < 5 {
		t.Fatalf("%d rotated files, want at least 5 (more than the old maxbackup=3 keeps): the log did not rotate enough to show what is kept", len(rotated))
	}
	have := map[string]bool{}
	for _, n := range rotated {
		have[n] = true
	}
	for name := range seen {
		if !have[name] {
			t.Errorf("the rotated file %s was seen and is gone: a file was removed", name)
		}
	}
	for _, f := range set.files {
		if base, ok := backupBase(f.name); ok {
			if !f.gz || f.name != base+".gz" {
				t.Errorf("the rotated file %s is not a gzip file by name", f.name)
				continue
			}
			if err := isGzip(filepath.Join(dir, f.name)); err != nil {
				t.Errorf("rotated file %s: %v", f.name, err)
			}
			if len(f.text) == 0 {
				t.Errorf("rotated file %s decompresses to nothing", f.name)
			}
		}
		if f.mode.Perm()&0o077 != 0 {
			t.Errorf("the audit file %s has mode %v: owner only is the rule", f.name, f.mode.Perm())
		}
	}
	if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm()&0o077 != 0 {
		t.Errorf("the audit log's directory: %v, %v, want owner only", fi, err)
	}
	// The first request's line is still in the log: the oldest file was not the one that was removed.
	if len(set.find("get", "configmaps", "", first, w.admin.user)) == 0 {
		t.Errorf("the first request of the rotation (%s) is in no file: the oldest rotated file was removed", first)
	}
	// Every line of every file is a whole JSON event: parseEvents failed the read otherwise.
	var total int
	for _, f := range set.files {
		total += len(f.text)
	}
	t.Logf("rotation: %d files (%d rotated, all gzip), %d bytes of log in all, decompressed", len(set.files), len(rotated), total)
}
