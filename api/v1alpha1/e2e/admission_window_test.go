package e2e

// The window is closed, not shortened (issue #589, PR 3; team-lead, 15 Nov 2026). No moment of up may let
// an agent's role grant a write before the policy that bounds it is enforced. The launcher's
// InstallAgentAccess is up's step: the policies, the narrow (read-only) agent role, a bounded wait until
// each policy refuses its dry-run probe, and only then the wide role. These tests run that exported step
// against a real API server with its test seam, which HOLDS the wait (the hook runs after the narrow role
// is applied, and the wait does not begin until it returns), and ask the API server about agent A:
//
//   - while the wait is held, A's own SelfSubjectAccessReview shows ModelDeployment create and update, and
//     modeldeployments/scale update, DENIED, and a real create with an owner reference and a real lower
//     count are both refused. The step has not returned: the hold is shown, not assumed;
//   - once the probes refuse, all three are ALLOWED, and the same two real writes are refused by the guard;
//   - with a probe that never refuses (the policy's binding withheld), the step fails with an error that
//     names the policy, and afterwards A's review still shows the three DENIED;
//   - the same on a restart, a second run on a host whose role is already wide: during the hold the three
//     are denied again.
//
// A window test whose hold does not hold would check the role only after the probes refused, and pass for a
// launcher that widens first. The mutant that applies the wide rules before the wait fails the first test
// while the step is held.

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

type stepResult struct {
	writes bool
	err    error
}

// windowRows are the accesses the wide role adds that matter for capacity: create and update of a
// ModelDeployment, and update of its scale subresource.
func windowRows(ns string) []accessRow {
	const G = frameworkGroup
	return []accessRow{
		{G, "modeldeployments", "", ns, "create", true},
		{G, "modeldeployments", "", ns, "update", true},
		{G, "modeldeployments", "scale", ns, "update", true},
	}
}

// requireAll fails unless the API server's answer to each row, for agent A's own credential, is want.
func requireAll(t *testing.T, a *agent, rows []accessRow, want bool, when string) {
	t.Helper()
	for _, r := range rows {
		got, err := ask(a.cs, r)
		if err != nil || got != want {
			t.Errorf("%s: A's review of %s = allowed %v, %v, want allowed=%v", when, r, got, err, want)
		}
	}
}

// heldStep is InstallAgentAccess running with its wait held.
type heldStep struct {
	released chan struct{}
	finished chan struct{} // closed when the step has returned
	result   stepResult    // valid after finished is closed
	once     bool
}

// release lets the wait begin.
func (h *heldStep) release() {
	if !h.once {
		h.once = true
		close(h.released)
	}
}

// returned reports whether the step has returned.
func (h *heldStep) returned() bool {
	select {
	case <-h.finished:
		return true
	default:
		return false
	}
}

// wait blocks until the step has returned and gives its result.
func (h *heldStep) wait(t *testing.T) stepResult {
	t.Helper()
	select {
	case <-h.finished:
	case <-time.After(90 * time.Second):
		t.Fatal("the step did not return after its wait was released")
	}
	return h.result
}

// startHeld runs InstallAgentAccess in a goroutine with its wait held, and returns when the hook has run
// (the policies are installed and the narrow role applied). The step is released and awaited when the
// test ends, so that none is left running.
func startHeld(t *testing.T) *heldStep {
	t.Helper()
	h := &heldStep{released: make(chan struct{}), finished: make(chan struct{})}
	held := make(chan struct{})
	go func() {
		defer close(h.finished)
		w, err := launcher.InstallAgentAccess(bg(), cfg, io.Discard, launcher.AgentAccessOptions{
			BeforeWait: func(ctx context.Context) error {
				close(held)
				select {
				case <-h.released:
				case <-ctx.Done():
				}
				return nil
			},
		})
		h.result = stepResult{w, err}
	}()
	t.Cleanup(func() {
		h.release()
		<-h.finished
	})
	select {
	case <-held:
	case <-h.finished:
		t.Fatalf("the step ended before its wait was reached: %+v", h.result)
	case <-time.After(60 * time.Second):
		t.Fatal("the step never reached its wait")
	}
	return h
}

// windowFixture is agent A, a namespace with a ModelDeployment A owns (made by the admin, since A may not
// create one while its role is narrow), and the real writes the window must refuse.
type windowFixture struct {
	a    *agent
	ns   string
	rows []accessRow
	md   *v1alpha1.ModelDeployment
}

func newWindowFixture(t *testing.T) *windowFixture {
	t.Helper()
	cleanUpPolicies(t)
	installAgentRoles(t)
	f := &windowFixture{a: issueAgents(t, "agent-a")[0]}
	f.ns = newNamespace(t)
	f.rows = windowRows(f.ns)
	f.md = agentMD(f.a, f.ns, uniq("md"))
	must(t, c.Create(bg(), f.md), "the admin's ModelDeployment for A")
	f.md = storedMD(t, f.md)
	return f
}

// realWrites makes the two real writes the AC names, and the scale, as A, and returns their errors: a
// create that carries an owner reference, a lower count through the object, and a lower count through the
// scale subresource. Nothing is stored when they are refused (checked by the caller).
func (f *windowFixture) realWrites(t *testing.T) (create, lower, scaled error) {
	t.Helper()
	requireAgentIdentity(t, f.a)
	withRef := agentMD(f.a, f.ns, uniq("md-ref"))
	withRef.OwnerReferences = []metav1.OwnerReference{ownerRefToAConfigMap()}
	create = f.a.c.Create(bg(), withRef)
	if gerr := c.Get(bg(), client.ObjectKeyFromObject(withRef), &v1alpha1.ModelDeployment{}); !apierrors.IsNotFound(gerr) {
		t.Errorf("the create with an owner reference was stored: %v", gerr)
	}
	lower = agentWriter(f.a).update(f.md, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	scaled = agentWriter(f.a).scaleTo(f.md, 1)
	if got := storedMD(t, f.md); got.Spec.Instances != 3 {
		t.Errorf("spec.instances = %d after the refused writes, want 3", got.Spec.Instances)
	}
	return create, lower, scaled
}

// rbacRefused reports whether err is RBAC's refusal: a Forbidden that names no policy.
func rbacRefused(err error) bool {
	return apierrors.IsForbidden(err) && !strings.Contains(err.Error(), "ValidatingAdmissionPolicy")
}

func TestTheAgentsWritesStayOffWhileTheEnforcementWaitIsHeld(t *testing.T) {
	f := newWindowFixture(t)
	for _, phase := range []string{"first start, from the narrow role", "restart, from the wide role"} {
		t.Run(phase, func(t *testing.T) {
			requireAgentIdentity(t, f.a)
			step := startHeld(t)
			assertHeld := func(when string) {
				t.Helper()
				if step.returned() {
					t.Fatalf("%s: the step returned (%+v) while its wait was held", when, step.result)
				}
			}

			// The hold. The policies are installed (step 1 precedes the hook) and the narrow role is
			// applied (step 2); the wait has not begun, so nothing says they are enforced.
			assertHeld("at the hook")
			states, err := launcher.CheckAdmission(bg(), cfg)
			must(t, err, "check the policies in the hold")
			for _, s := range states {
				if !s.Present {
					t.Errorf("in the hold the policy %s is not installed: %+v", s.Name, s)
				}
			}
			for _, r := range f.rows {
				waitUntilDenied(t, f.a.cs, r) // the authorizer has learned the narrow role, on a restart too
			}
			assertHeld("after the narrow role was seen")
			requireAll(t, f.a, f.rows, false, "while the wait is held")
			create, lower, scaled := f.realWrites(t)
			for what, err := range map[string]error{"a create with an owner reference": create, "a lower count": lower, "a lower count through scale": scaled} {
				if !rbacRefused(err) {
					t.Errorf("A's %s while the wait is held: %v, want RBAC's refusal (the role is narrow)", what, err)
				}
			}
			assertHeld("after the real writes")
			requireAll(t, f.a, f.rows, false, "after the real writes, still held")

			// The release: the wait runs, the probes refuse, and only then the wide role.
			step.release()
			r := step.wait(t)
			must(t, r.err, "the step after its release")
			if !r.writes {
				t.Fatal("the step returned without opening the writes")
			}
			// It returned only after each policy refused its probe: one probe, no polling.
			requireEnforced(t)
			for _, row := range f.rows {
				waitUntilAllowed(t, f.a.cs, row)
			}
			create, lower, scaled = f.realWrites(t)
			requireDenied(t, "A's create with an owner reference once the writes are open", create, capacityGuard, "cannot create a ModelDeployment with ownerReferences")
			requireDenied(t, "A's lower count once the writes are open", lower, capacityGuard, "cannot lower spec.instances")
			requireDenied(t, "A's lower count through scale once the writes are open", scaled, capacityGuard, "cannot lower a ModelDeployment's count with scale")
		})
	}
}

// A probe that never refuses (the policy's binding withheld, and shown not to be enforced) fails the step
// with an error that names the policy, opens no write, and leaves the role narrow, whether the host was
// narrow before or already wide.
func TestAProbeThatNeverRefusesFailsTheStepAndLeavesTheRoleNarrow(t *testing.T) {
	f := newWindowFixture(t)
	for _, start := range []string{"narrow", "wide"} {
		for _, policy := range []string{capacityGuard, ownerPin, approverPin} {
			t.Run(start+" role, "+policy+" withheld", func(t *testing.T) {
				if start == "wide" {
					writes, err := launcher.InstallAgentAccess(bg(), cfg, io.Discard, launcher.AgentAccessOptions{})
					must(t, err, "open the writes first")
					if !writes {
						t.Fatal("the writes did not open")
					}
					for _, r := range f.rows {
						waitUntilAllowed(t, f.a.cs, r)
					}
				} else {
					must(t, launcher.InstallAgentRBACNarrow(bg(), cfg, io.Discard), "narrow the role")
					for _, r := range f.rows {
						waitUntilDenied(t, f.a.cs, r)
					}
				}
				withhold := func(ctx context.Context) error {
					must(t, c.Delete(ctx, &admissionregistrationv1.ValidatingAdmissionPolicyBinding{ObjectMeta: metav1.ObjectMeta{Name: policy}}), "withhold the binding of "+policy)
					waitPolicyNotEnforced(t, policy)
					return nil
				}
				writes, err := launcher.InstallAgentAccess(bg(), cfg, io.Discard, launcher.AgentAccessOptions{EnforcementTimeout: 2 * time.Second, BeforeWait: withhold})
				if err == nil || writes || !strings.Contains(err.Error(), policy) {
					t.Fatalf("a probe that never refuses: writes %v, error %v, want a failure that names %s", writes, err, policy)
				}
				for _, other := range []string{capacityGuard, ownerPin, approverPin} {
					if other != policy && strings.Contains(err.Error(), other) {
						t.Errorf("the error %q names %s, which was enforced", err, other)
					}
				}
				// Afterwards the role is still the narrow one: create, update and scale are DENIED.
				if start == "wide" {
					for _, r := range f.rows {
						waitUntilDenied(t, f.a.cs, r) // the narrow role was applied before the wait; the authorizer learns it
					}
				}
				requireAll(t, f.a, f.rows, false, "after the step that failed")
				create, lower, _ := f.realWrites(t)
				if !rbacRefused(create) || !rbacRefused(lower) {
					t.Errorf("after the failed step A's create: %v, lower count: %v, want RBAC's refusals", create, lower)
				}
			})
		}
	}
}
