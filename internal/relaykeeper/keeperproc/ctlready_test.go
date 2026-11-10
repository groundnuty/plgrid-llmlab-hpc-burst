package keeperproc

// HBEP-6's keeper items for a Provider's Ready with REAL controller processes
// (Test plan, The relay keeper; #343's PR 4, Stage B):
//   - "a second Provider's set for a held login host is refused, and its
//     Ready is LoginHostInUse" (the refusal is PR 3's test; this is the Ready
//     half, written by a real controller);
//   - "with the keeper's socket missing, the Provider is Ready False with
//     RelayKeeperUnavailable and no Allocation is created; at the first
//     successful exchange it's Ready again; a running Allocation's relays are
//     untouched throughout".
// Ready is read from the Provider's status in the API server; the refusals
// off the wire by the tap; the submissions from the fake's own call log.

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// waitReady waits until the Provider's Ready, read from the API server, has
// status and reason, and returns it.
func (g *rig) waitReady(provider string, status metav1.ConditionStatus, reason string) metav1.Condition {
	g.t.Helper()
	var c metav1.Condition
	waitFor(g.t, 2*time.Minute, "Provider "+provider+"'s Ready "+string(status)+" with "+reason, func() bool {
		var ok bool
		c, ok = g.api.ready(g.t, provider)
		return ok && c.Status == status && c.Reason == reason
	})
	return c
}

// A second Provider's real controller on a login host another Provider holds
// in the keeper: the holder is a double of an older Provider whose relays and
// serving stream run on the login node. The real controller's sets are
// refused for the host, naming it and the holder, and its Provider's Ready,
// in the API server, is False with LoginHostInUse naming both. The holder's
// relays and stream run on, whole.
func TestARealControllerOnAHeldLoginHostIsLoginHostInUse(t *testing.T) {
	g := newRig(t)
	j := g.w.submit("hb-kp-ihld-0123456789ab", 30*time.Second)
	holder := g.w.startDouble("holder", g.k.sock)
	mustReport(t, holder.lead(g.w.set(provA, 1, true, g.w.entry(j, gen1))), "the holder's set")
	up := g.k.waitUp(provA, j)
	pins := g.k.pins(up)
	s := g.w.openStream(j, serving)
	s.mustEcho(64<<10, "before the second Provider's controller")
	host := strings.ToLower(g.w.creds.HostName)
	p := g.provider("example-c")
	c := g.startController("c", p.name, g.tap.path)
	r := g.tap.snapshot(g.tap.waitReq(t, 2*time.Minute, "a set of C's refused", func(r *tapReq) bool {
		return r.pid == c.pid() && r.set != nil && r.refused != nil
	}))
	if r.refused.Reason != wire.RefusedLoginHost || r.refused.Holder != provA.name || r.refused.LoginHost != host {
		t.Errorf("C's set was refused %+v, want %s naming login host %s and its holder %s", *r.refused, wire.RefusedLoginHost, host, provA.name)
	}
	s.mustEcho(32<<10, "after C's refused set")
	cond := g.waitReady(p.name, metav1.ConditionFalse, v1alpha1.ReasonLoginHostInUse)
	if !strings.Contains(cond.Message, host) || !strings.Contains(cond.Message, provA.name) {
		t.Errorf("Provider %s's Ready message %q does not name the login host %s and its holder %s", p.name, cond.Message, host, provA.name)
	}
	for range 3 {
		s.mustEcho(32<<10, "with C's Provider LoginHostInUse")
	}
	for _, rq := range g.tap.requests(from(c.pid())) {
		if rq.report != nil {
			t.Errorf("the keeper accepted C's %s", rq.describe())
		}
	}
	ps, _ := g.k.provider(p)
	if ps.Refused[wire.RefusedLoginHost] == 0 || len(ps.Relays) != 0 {
		t.Errorf("C's Provider in the keeper's status: %d refusals for the login host and relays %+v, want some and none", ps.Refused[wire.RefusedLoginHost], ps.Relays)
	}
	for _, ch := range agentapi.RelayChannels {
		if pins[ch].exited() {
			t.Errorf("the holder's %s relay's ssh, held through its pidfd, exited", ch)
		}
		if rs, ok := g.k.upOf(provA, j.tag, ch); !ok || rs.PID != up[ch].PID || rs.Starts != 1 {
			t.Errorf("the holder's %s relay %+v, want it up as it was, PID %d", ch, rs, up[ch].PID)
		}
	}
	s.whole()
}

// agentBuild writes an example agent build for the rig's cpuArch into the
// agent-builds directory and returns its SHA-256, an Allocation's agent
// digest. It is never run: the job it would be staged for is refused.
func (g *rig) agentBuild() string {
	g.t.Helper()
	b := []byte("#!/bin/sh\n# an example agent build for " + cpuArch + ", never run\nexit 3\n")
	if err := os.WriteFile(filepath.Join(g.buildsDir, agentapi.AgentBuildName(cpuArch)), b, 0o700); err != nil {
		g.t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// credential writes the Allocation's credential, as the lifecycle controller
// does once its UID exists.
func (g *rig) credential(a *v1alpha1.Allocation) {
	g.t.Helper()
	key, err := agentapi.NewKey()
	if err != nil {
		g.t.Fatal(err)
	}
	if err := agentapi.WriteKeyFile(agentapi.AllocationCredentialPath(g.credsDir, string(a.UID)), key); err != nil {
		g.t.Fatal(err)
	}
}

// A real controller whose keeper socket is missing: A leads first, through
// the tap, and the keeper runs a running Allocation's relays, pinned, with a
// serving stream; A stops cleanly. The tap's socket is then removed, and a
// pending Allocation is made, with all that its submission needs (its
// credential, an agent build of its digest, the front's certificate and frpc
// build), so only admission stops it. C, a new real controller, takes the
// Lease: its Provider is Ready False with RelayKeeperUnavailable, its Create
// refuses the pending Allocation (crossplane's Synced condition says so) and
// no sbatch for it reaches the fake. Once the tap listens again, the
// keeper's first answer makes the Provider Ready True, and the pending
// Allocation's sbatch then reaches the fake (the positive control: the fake
// refuses it by script, so nothing runs). The running Allocation's relays are
// the very same processes throughout, with E, N and one start each, and the
// stream runs on, whole.
func TestARealControllerWithItsKeeperSocketMissingIsUnavailableAndAdmitsNothing(t *testing.T) {
	g := newRig(t)
	p := g.provider("example-a")
	al, j := g.running(p, "kp-unav")
	own := g.w.allSbatches()
	a := g.startController("a", p.name, g.tap.path)
	_, up := g.leads(a, p, j)
	g.adopted(al, j)
	pins := g.k.pins(up)
	s := g.w.openStream(j, serving)
	s.mustEcho(64<<10, "before the keeper's socket goes")
	a.term()
	s.mustEcho(32<<10, "after A's stop")
	g.tap.stop()
	if _, err := os.Stat(g.tap.path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the keeper socket the controller is given is still there: %v", err)
	}
	q := g.allocation(p, "kp-pend", g.agentBuild())
	g.credential(q)
	g.w.plat.Script(fakeslurm.Reject("example refusal of the scripted fake"))
	c := g.startController("c", p.name, g.tap.path)
	cond := g.waitReady(p.name, metav1.ConditionFalse, v1alpha1.ReasonRelayKeeperUnavailable)
	if !strings.Contains(cond.Message, "relay keeper") {
		t.Errorf("Provider %s's Ready message %q does not name the relay keeper", p.name, cond.Message)
	}
	waitFor(t, 2*time.Minute, "the pending Allocation's submission refused", func() bool {
		qa, ok := g.api.allocation(t, allocNS, q.Name)
		if !ok {
			return false
		}
		sc := qa.GetCondition(xpv2.TypeSynced)
		return sc.Status == "False" && strings.Contains(sc.Message, "refusing to submit") && strings.Contains(sc.Message, "relay keeper")
	})
	if n := g.w.sbatches(tagOf(q)); n != 0 {
		t.Errorf("%d sbatch calls for the pending Allocation while the keeper is unavailable, want none", n)
	}
	for range 3 {
		s.mustEcho(32<<10, "while the keeper is unavailable to C")
	}
	if l := g.api.lease(t, p.name); l.holder == "" {
		t.Fatalf("no copy holds the Lease while C runs: %+v", l)
	}
	g.tap.start()
	g.waitReady(p.name, metav1.ConditionTrue, v1alpha1.ReasonPolled)
	s.mustEcho(32<<10, "once the keeper answers")
	waitFor(t, 3*time.Minute, "the pending Allocation's sbatch reaching the fake once the keeper answers", func() bool {
		return g.w.sbatches(tagOf(q)) == 1
	})
	for _, rq := range g.tap.requests(from(c.pid())) {
		if rq.set != nil && rq.refused != nil {
			t.Errorf("the keeper refused C's %s: %+v", rq.describe(), *rq.refused)
		}
	}
	for _, ch := range agentapi.RelayChannels {
		if pins[ch].exited() {
			t.Errorf("the %s relay's ssh, held through its pidfd, exited", ch)
		}
		rs, ok := g.k.upOf(p, j.tag, ch)
		if !ok || rs.PID != up[ch].PID || rs.Epoch != up[ch].Epoch || rs.Seq != up[ch].Seq || rs.Starts != 1 {
			t.Errorf("the %s relay %+v, want it untouched: PID %d, (E, N) (%d, %d), 1 start", ch, rs, up[ch].PID, up[ch].Epoch, up[ch].Seq)
		}
	}
	for range 3 {
		s.mustEcho(32<<10, "at the end")
	}
	g.ownSbatches(own + 1)
	s.whole()
}
