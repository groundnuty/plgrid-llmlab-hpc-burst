package lifecycle

// What the lifecycle controller's failing events on an Instance may copy from its
// Allocation (HBEP-8, Allocation events, No job ID, path or name in a note; the
// science agent's ruling on #640, 16 Nov 2026). Every typed event goes to the
// durable log in beta, which is kept for the grant and may be exported, so a note
// names no path, user name, account or grant. The Provider writes text of its own
// into the Allocation: the profile reader's error into status.atProvider.reason
// for a Refused Allocation, and a relay's cause into the RelayEndedForGood
// condition, whose cause can quote the agent's or the staging directory's path.
// The Instance's event gives the reason code and a fixed description, and the
// Instance's own end message (status, not logged) keeps the detail.
//
// The synthetic values are the same shape as real ones and none is real. The
// expectations are literals, never built by a lifecycle formatter.

import (
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const (
	noteUser    = "example-user-x1"
	noteHome    = "/home/" + noteUser
	noteStaging = noteHome + "/hpcburst/stage"
	noteAccount = "example-account-a1"
	noteGrant   = "example-grant-g1"
)

// firstAllocation runs the rig until the deployment has an Instance on an
// Allocation, and returns both names.
func firstAllocation(t *testing.T, rg *rig) (inst, alloc string) {
	t.Helper()
	rg.create(mdSpec("md"))
	rg.until(10*time.Minute, 10*time.Second, "an Instance on an Allocation", func() bool {
		ins := rg.instances("md")
		return len(ins) > 0 && ins[0].Spec.AllocationRef.Name != ""
	})
	in := rg.instances("md")[0]
	return in.Name, in.Spec.AllocationRef.Name
}

// noPathUserAccountOrGrant fails when text holds the synthetic path, user name,
// account or grant as a token, or any word that starts a path.
func noPathUserAccountOrGrant(t *testing.T, what, text string) {
	t.Helper()
	for _, bad := range []string{noteHome, noteStaging, noteUser, noteAccount, noteGrant} {
		if hasToken(text, bad) {
			t.Errorf("%s names %q: %q", what, bad, text)
		}
	}
	if strings.Contains(text, "/home/") || strings.Contains(text, "/hpcburst") {
		t.Errorf("%s holds a path: %q", what, text)
	}
}

// A Refused Allocation whose status.atProvider.reason is the profile reader's
// error, with a path under a synthetic home and the synthetic user, account and
// grant in it, ends its Instance with an AllocationRefused event whose note is
// the code and a fixed description.
func TestAnAllocationRefusedEventNamesNoPathUserAccountOrGrant(t *testing.T) {
	rg := newRig(t)
	instName, allocName := firstAllocation(t, rg)
	reason := "provider example: invalid profile or settings: provider.spec.credentialsRef.file: credentials file: line 4: keyword " +
		noteHome + "/.hpcburst/creds-x1 is not allowed in a credentials file; provider.spec.account: " + noteAccount + " of grant " + noteGrant + " is refused for user " + noteUser
	a, ok := rg.allocation(allocName)
	if !ok {
		t.Fatalf("Allocation %s is gone", allocName)
	}
	a.Status.AtProvider.Phase, a.Status.AtProvider.Reason = v1alpha1.AllocationRefused, reason
	if err := rg.c.Status().Update(rg.ctx, a); err != nil {
		t.Fatal(err)
	}
	rg.until(10*time.Minute, 10*time.Second, "a failing AllocationRefused event", func() bool {
		return len(rg.rec.Find("failing", v1alpha1.ReasonAllocationRefused)) > 0
	})
	ev := rg.rec.Find("failing", v1alpha1.ReasonAllocationRefused)
	if len(ev) != 1 {
		t.Fatalf("failing/%s events %+v, want exactly one", v1alpha1.ReasonAllocationRefused, ev)
	}
	noPathUserAccountOrGrant(t, "the failing event's note", ev[0].Message)
	if want := "Allocation " + allocName + " is Refused; status.atProvider.reason has the reason"; ev[0].Message != want {
		t.Errorf("the failing event's note is %q, want %q", ev[0].Message, want)
	}
	// The detail stays in status: the Instance's end message, which is not logged,
	// still holds the Provider's text, so the source did reach the pass.
	in := rg.instance(instName)
	healthy := apimeta.FindStatusCondition(in.Status.Conditions, "Healthy")
	if healthy == nil || !strings.Contains(healthy.Message, noteHome+"/.hpcburst/creds-x1") {
		t.Errorf("the Instance's Healthy condition %+v does not keep the Provider's text, so the injected values did not reach the pass", healthy)
	}
}

// A RelayEndedForGood Allocation, whose condition message quotes the agent's
// staging path as an EndFailed from the relay line does, ends its Instance with
// an event whose note is the code and a fixed description.
func TestARelayEndedForGoodEventNamesNoPathUserAccountOrGrant(t *testing.T) {
	rg := newRig(t)
	instName, allocName := firstAllocation(t, rg)
	msg := "the control relay ended for good: the relay line: agent binary \"" + noteStaging + "/agent\" is not a path the relay accepts; user " + noteUser + ", account " + noteAccount + ", grant " + noteGrant
	a, ok := rg.allocation(allocName)
	if !ok {
		t.Fatalf("Allocation %s is gone", allocName)
	}
	a.SetConditions(xpv2.Condition{Type: v1alpha1.ConditionRelayEndedForGood, Status: corev1.ConditionTrue, Reason: v1alpha1.ReasonRelayFailed,
		Message: msg, LastTransitionTime: metav1.NewTime(rg.clock.Now())})
	if err := rg.c.Status().Update(rg.ctx, a); err != nil {
		t.Fatal(err)
	}
	rg.until(10*time.Minute, 10*time.Second, "a failing RelayEndedForGood event", func() bool {
		return len(rg.rec.Find("failing", string(v1alpha1.ConditionRelayEndedForGood))) > 0
	})
	ev := rg.rec.Find("failing", string(v1alpha1.ConditionRelayEndedForGood))
	if len(ev) != 1 {
		t.Fatalf("failing/RelayEndedForGood events %+v, want exactly one", ev)
	}
	noPathUserAccountOrGrant(t, "the failing event's note", ev[0].Message)
	if want := "a relay of Allocation " + allocName + " ended for good; the Allocation's RelayEndedForGood condition has the cause"; ev[0].Message != want {
		t.Errorf("the failing event's note is %q, want %q", ev[0].Message, want)
	}
	in := rg.instance(instName)
	healthy := apimeta.FindStatusCondition(in.Status.Conditions, "Healthy")
	if healthy == nil || !strings.Contains(healthy.Message, noteStaging) {
		t.Errorf("the Instance's Healthy condition %+v does not keep the relay's cause, so the injected values did not reach the pass", healthy)
	}
}
