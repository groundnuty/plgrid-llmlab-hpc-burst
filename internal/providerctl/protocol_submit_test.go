package providerctl

// What keeps a submission from happening, and what it does not change:
// a Rejected outcome and its backoff, the prerequisites (credential, front,
// agent build), the login-node paths, the dial-home key, the management
// policies, and a spec change on an established Allocation. Expected values
// come from HBEP-4 (Submission, Transport, Two controllers), HBEP-7 open
// question 11, HBEP-11 and the doc comment of pkg/provider/fake/controller.go.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/sets"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// A Rejected outcome (HBEP-7 open question 11, Outcomes: "the next attempt
// waits for backoff"; fake.Controller doc: phase Submitting until
// RejectBackoff has passed, then attempt n+1 runs). One minute is the default
// backoff; the wait is over when the age is no longer below it.
func TestRejectedOutcomeWaitsForTheBackoffThenRetries(t *testing.T) {
	for _, m := range ptModes {
		t.Run(m.name, func(t *testing.T) {
			e := newEnv(t, m.mods...)
			e.batch.Enqueue(fake.Script{Outcome: provider.Rejected, Reason: "Batch job submission failed: example refusal"})
			e.newAlloc("a")
			e.run("a", 2)

			o := e.get("a").Status.AtProvider
			if o.Phase != v1alpha1.AllocationSubmitting || !strings.Contains(o.Reason, "example refusal") {
				t.Fatalf("phase %s reason %q, want Submitting with the scheduler's reason", o.Phase, o.Reason)
			}
			if r := ptRecordOf(e, "a"); r == nil || r.State != v1alpha1.SubmissionRejected || r.Attempt != 1 {
				t.Fatalf("record %+v, want attempt 1 Rejected", r)
			}
			if n := e.count("sbatch"); n != 1 {
				t.Fatalf("%d sbatch calls, want 1", n)
			}

			ptStep(e, "a", ptBackoff-time.Second)
			e.run("a", 2)
			if n := e.count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls 59 s after the rejection, want 1", n)
			}
			if p := ptPhase(e, "a"); p != v1alpha1.AllocationSubmitting {
				t.Errorf("phase %s inside the backoff, want Submitting", p)
			}

			e.clock.Advance(time.Second)
			e.run("a", 3)
			if n := e.count("sbatch"); n != 2 {
				t.Errorf("%d sbatch calls after the backoff, want 2", n)
			}
			if r := ptRecordOf(e, "a"); r == nil || r.Attempt != 2 || r.State != v1alpha1.SubmissionDone {
				t.Errorf("record %+v, want attempt 2 Done (attempt 1 plus 1)", r)
			}
			if got := ptJobID(e, "a"); got != ptFirstJob {
				t.Errorf("job ID %q, want %s: a Rejected attempt registers no job", got, ptFirstJob)
			}
		})
	}
}

// ptHarnessFront is the front newEnv configures.
func ptHarnessFront() *FrontSettings {
	return &FrontSettings{Mode: agentapi.DialHomeDirect, Endpoint: "front.example.org:29443", CertPin: strings.Repeat("c", 64)}
}

// The prerequisites (create.go: prerequisites; docs/development.md): no
// credential file, no front, or an agent build with another digest keeps the
// Allocation Submitting with a reason, with no sbatch, no staged file and no
// Rejected record (the wait is not an attempt). Each case is also its own
// control: once the cause is fixed, the same Allocation submits once.
func TestPrerequisitesKeepTheAllocationFromSubmitting(t *testing.T) {
	cases := []struct {
		name   string
		mods   []envOption
		why    string
		breaks func(e *env, a *v1alpha1.Allocation)
		fix    func(e *env, a *v1alpha1.Allocation)
	}{
		{
			name: "no credential file", why: "credential",
			breaks: func(e *env, a *v1alpha1.Allocation) {
				if err := os.Remove(agentapi.AllocationCredentialPath(e.credDir, string(a.UID))); err != nil {
					e.t.Fatal(err)
				}
			},
			fix: func(e *env, a *v1alpha1.Allocation) { e.writeCredential(a) },
		},
		{
			name: "no front", why: "no front is configured",
			mods:   []envOption{func(e *env) { e.opts.Front = nil }},
			breaks: func(*env, *v1alpha1.Allocation) {},
			fix: func(e *env, _ *v1alpha1.Allocation) {
				e.opts.Front = ptHarnessFront()
				e.build()
			},
		},
		{
			name: "an agent build with another digest", why: "digest",
			breaks: func(e *env, _ *v1alpha1.Allocation) {
				if err := os.WriteFile(filepath.Join(e.buildDir, agentapi.AgentBuildName(testArch)), []byte("#!/bin/sh\n# another build, longer than the first\n"), 0o700); err != nil {
					e.t.Fatal(err)
				}
			},
			fix: func(e *env, _ *v1alpha1.Allocation) {
				if err := os.WriteFile(filepath.Join(e.buildDir, agentapi.AgentBuildName(testArch)), []byte("#!/bin/sh\n# an example agent build\n"), 0o700); err != nil {
					e.t.Fatal(err)
				}
			},
		},
	}
	for _, tc := range cases {
		for _, m := range ptModes {
			t.Run(tc.name+"/"+m.name, func(t *testing.T) {
				e := newEnv(t, append(append([]envOption{}, m.mods...), tc.mods...)...)
				a := e.newAlloc("a")
				tc.breaks(e, a)
				e.run("a", 3)
				if n := e.count("sbatch"); n != 0 {
					t.Fatalf("%d sbatch calls with the prerequisite missing, want 0", n)
				}
				if n := e.count("ensure-file"); n != 0 {
					t.Errorf("%d files staged with the prerequisite missing, want 0", n)
				}
				got := e.get("a")
				if r := ptRecordOf(e, "a"); r != nil {
					t.Errorf("record %+v written, want none: waiting is not an attempt", r)
				}
				if o := got.Status.AtProvider; o.Phase != v1alpha1.AllocationSubmitting || !strings.Contains(o.Reason, tc.why) {
					t.Errorf("phase %s reason %q, want Submitting with %q", o.Phase, o.Reason, tc.why)
				}

				tc.fix(e, a)
				e.run("a", 4)
				if n := e.count("sbatch"); n != 1 {
					t.Errorf("%d sbatch calls once the prerequisite is met, want 1", n)
				}
			})
		}
	}
}

func ptSetPathErr(e *env, err error) {
	e.platform.mu.Lock()
	defer e.platform.mu.Unlock()
	e.platform.pathErr = err
}

// An unresolved login-node path (HBEP-4, Transport; provider.ErrPathUnresolved):
// no file call and no sbatch, no claim; the Provider gets PathUnresolved. Once
// the path resolves, the Allocation submits.
func TestUnresolvedPathMakesNoFileCallAndNoSbatch(t *testing.T) {
	for _, m := range ptModes {
		t.Run(m.name, func(t *testing.T) {
			e := newEnv(t, m.mods...)
			ptSetPathErr(e, fmt.Errorf("%w: HOME is unset on the login node", provider.ErrPathUnresolved))
			e.newAlloc("a")
			e.run("a", 3)
			if n := e.count("sbatch"); n != 0 {
				t.Errorf("%d sbatch calls, want 0", n)
			}
			if n := e.count("ensure-file"); n != 0 {
				t.Errorf("%d file calls, want 0", n)
			}
			if r := ptRecordOf(e, "a"); r != nil {
				t.Errorf("record %+v written before the paths resolved, want none", r)
			}
			if c := e.get("a").GetCondition(xpv2.TypeSynced); c.Status != corev1.ConditionFalse || !strings.Contains(c.Message, "unresolved") {
				t.Errorf("Synced = %s %q, want False naming the unresolved path", c.Status, c.Message)
			}
			if err := e.ctl.SyncStatus(e.ctx); err != nil {
				t.Fatal(err)
			}
			var prov v1alpha1.Provider
			if err := e.c.Get(e.ctx, client.ObjectKey{Name: testProvider}, &prov); err != nil {
				t.Fatal(err)
			}
			if c := apimeta.FindStatusCondition(prov.Status.Conditions, v1alpha1.ConditionPathUnresolved); c == nil || c.Status != "True" {
				t.Errorf("Provider PathUnresolved = %+v, want True", c)
			}

			ptSetPathErr(e, nil)
			e.run("a", 4)
			if n := e.count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls once the path resolves, want 1", n)
			}
		})
	}
}

// ssh dial-home mode needs the dial-home key files on the platform (HBEP-4,
// Submission; create.go): with none, no file call, no sbatch and no claim, and
// the error names DialHomeKeyMissing. With the keys, the same Allocation submits.
func TestMissingDialHomeKeyKeepsTheSubmissionBack(t *testing.T) {
	sshFront := func(e *env) {
		e.opts.Front = &FrontSettings{Mode: agentapi.DialHomeSSH, Endpoint: "control.example.org:22",
			CertPin: strings.Repeat("c", 64), SSHUser: "example-user", AgentPort: 29444}
	}
	for _, m := range ptModes {
		t.Run(m.name, func(t *testing.T) {
			e := newEnv(t, append(append([]envOption{}, m.mods...), sshFront)...)
			e.platform.keysPresent = false
			e.newAlloc("a")
			e.run("a", 3)
			if e.platform.keyChecks == 0 {
				t.Fatal("the dial-home keys were never checked")
			}
			if n := e.count("sbatch"); n != 0 {
				t.Errorf("%d sbatch calls with the key missing, want 0", n)
			}
			if n := e.count("ensure-file"); n != 0 {
				t.Errorf("%d file calls with the key missing, want 0", n)
			}
			if r := ptRecordOf(e, "a"); r != nil {
				t.Errorf("record %+v written with the key missing, want none", r)
			}
			if c := e.get("a").GetCondition(xpv2.TypeSynced); c.Status != corev1.ConditionFalse || !strings.Contains(c.Message, v1alpha1.ConditionDialHomeKeyMissing) {
				t.Errorf("Synced = %s %q, want False naming %s", c.Status, c.Message, v1alpha1.ConditionDialHomeKeyMissing)
			}

			e.platform.keysPresent = true
			e.run("a", 4)
			if n := e.count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls with the keys present, want 1", n)
			}
		})
	}
}

// The management policies (HBEP-4: Update is never called; setup.go
// SupportedPolicies): the supported set is exactly Observe, Create, Delete and
// LateInitialize. The fake client runs no CEL, so the reconciler itself must
// refuse any other set, with a ReconcileError condition and not one platform
// call. The control, the default set, submits.
func TestUnsupportedManagementPoliciesAreRefused(t *testing.T) {
	supported := SupportedPolicies()
	want := sets.New(xpv2.ManagementActionObserve, xpv2.ManagementActionCreate, xpv2.ManagementActionDelete, xpv2.ManagementActionLateInitialize)
	if len(supported) != 1 || !supported[0].Equal(want) {
		t.Fatalf("SupportedPolicies() = %v, want exactly %v", supported, want.UnsortedList())
	}
	if !sets.New(v1alpha1.DefaultManagementPolicies()...).Equal(want) {
		t.Fatalf("DefaultManagementPolicies() = %v, want the supported set", v1alpha1.DefaultManagementPolicies())
	}

	cases := map[string][]xpv2.ManagementAction{
		"observe only":       {xpv2.ManagementActionObserve},
		"with update":        {xpv2.ManagementActionObserve, xpv2.ManagementActionCreate, xpv2.ManagementActionUpdate, xpv2.ManagementActionDelete, xpv2.ManagementActionLateInitialize},
		"all":                {xpv2.ManagementActionAll},
		"no late initialize": {xpv2.ManagementActionObserve, xpv2.ManagementActionCreate, xpv2.ManagementActionDelete},
	}
	for name, policies := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			a := e.newAlloc("a")
			a.Spec.ManagementPolicies = policies
			if err := e.c.Update(e.ctx, a); err != nil {
				t.Fatal(err)
			}
			e.run("a", 3)
			c := e.get("a").GetCondition(xpv2.TypeSynced)
			if c.Status != corev1.ConditionFalse || c.Reason != xpv2.ReasonReconcileError {
				t.Errorf("Synced = %s/%s %q, want False/ReconcileError", c.Status, c.Reason, c.Message)
			}
			if n := len(e.batch.Calls()); n != 0 {
				t.Errorf("%d platform calls for an unsupported policy set, want 0", n)
			}
			if got := e.get("a"); len(got.Finalizers) != 0 {
				t.Errorf("finalizers %v added to an Allocation the controller refuses", got.Finalizers)
			}
		})
	}

	t.Run("control: the default set", func(t *testing.T) {
		e := newEnv(t)
		e.newAlloc("a")
		e.run("a", 4)
		if n := e.count("sbatch"); n != 1 {
			t.Errorf("%d sbatch calls with the default policies, want 1", n)
		}
	})
}

// A spec change on an established Allocation (HBEP-4: a submitted job is never
// edited; HBEP-11: the spec is immutable, which the CRD's CEL rules enforce and
// the fake client does not, so the controller is tested for what it does when a
// change gets through). Update is not a management policy, so nothing follows:
// no sbatch, scancel, lift or file call, and the status and the record stay.
// The strict zero-call version, counting the reads too, runs on the real Batch
// in the end-to-end test.
func TestSpecChangeOnAnEstablishedAllocationChangesNothing(t *testing.T) {
	changes := map[string]func(a *v1alpha1.Allocation){
		"modelDeploymentRef": func(a *v1alpha1.Allocation) {
			a.Spec.ModelDeploymentRef = &v1alpha1.ObjectReference{Name: "another-deployment"}
		},
		"forProvider.batch.timeLimit": func(a *v1alpha1.Allocation) {
			a.Spec.ForProvider.Batch.TimeLimit = "45m"
		},
	}
	for name, change := range changes {
		for _, m := range ptModes {
			t.Run(name+"/"+m.name, func(t *testing.T) {
				e := newEnv(t, m.mods...)
				e.newAlloc("a")
				e.run("a", 4)
				before := e.get("a")
				if before.Status.AtProvider.Phase != v1alpha1.AllocationRunning || ptJobID(e, "a") != ptFirstJob {
					t.Fatalf("precondition: phase %s job %q", before.Status.AtProvider.Phase, ptJobID(e, "a"))
				}
				mark := ptMark(e)

				a := e.get("a")
				change(a)
				if err := e.c.Update(e.ctx, a); err != nil {
					t.Fatal(err)
				}
				e.run("a", 4)

				for _, op := range []string{"sbatch", "scancel", "scontrol-release", "ensure-file", "remove-allocation-files", "set-time-limit"} {
					if n := ptCountSince(e, mark, op); n != 0 {
						t.Errorf("%d %s calls after the spec change, want 0", n, op)
					}
				}
				after := e.get("a")
				if !apiequality.Semantic.DeepEqual(before.Status.AtProvider, after.Status.AtProvider) {
					t.Errorf("status.atProvider changed:\n before %+v\n after  %+v", before.Status.AtProvider, after.Status.AtProvider)
				}
				for _, k := range []string{v1alpha1.AnnotationSubmission, v1alpha1.AnnotationJobID, v1alpha1.AnnotationHoldLifted} {
					if before.Annotations[k] != after.Annotations[k] {
						t.Errorf("annotation %s changed from %q to %q", k, before.Annotations[k], after.Annotations[k])
					}
				}
			})
		}
	}
}
