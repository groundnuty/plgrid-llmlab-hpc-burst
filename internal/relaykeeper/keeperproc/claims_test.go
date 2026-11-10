package keeperproc

// HBEP-6's keeper item for the login host (Test plan, The relay keeper): "a
// second Provider's set for a held login host is refused" (its Ready being
// LoginHostInUse is the controller's, PR 4's). The holder is the Provider
// whose relays run there; the login host is its HostName as ssh -G resolves
// it, the login node's 127.0.0.1 here.

import (
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// A second Provider's set for a held login host is refused, naming the
// holder, while the holder's relays and stream run there: a newer Provider's,
// and an older one's too, since the holder's relays run. Neither gets a relay
// or an srun line, the refusals are counted, and the holder's relays and
// stream run on, whole.
//
// Mutant (keeper.go, handleSet): "if holder != "" {" made "if false {", so
// the second Provider's set is accepted.
func TestASecondProvidersSetForAHeldLoginHostIsRefused(t *testing.T) {
	w := newWorld(t)
	j1 := w.submit("hb-kp-hlda-0123456789ab", 30*time.Second)
	j2 := w.submit("hb-kp-hldb-0123456789ab", 5*time.Second)
	k := w.startKeeper("")
	holder := w.startDouble("holder", k.sock)
	mustReport(t, holder.lead(w.set(provA, 1, true, w.entry(j1, gen1))), "the holder's set")
	up := k.waitUp(provA, j1)
	s := w.openStream(j1, serving)
	s.mustEcho(64<<10, "before the second Provider's set")
	host := strings.ToLower(w.creds.HostName)
	for _, p := range []prov{provB, provOld} {
		d := w.startDouble(p.name, k.sock)
		ref := mustRefuse(t, d.set(w.set(p, 1, true, w.entry(j2, gen1))), p.name+"'s set")
		if ref.Reason != wire.RefusedLoginHost || ref.Holder != provA.name || ref.LoginHost != host {
			t.Errorf("%s's set was refused %+v, want %s naming login host %s and its holder %s", p.name, ref, wire.RefusedLoginHost, host, provA.name)
		}
		s.mustEcho(32<<10, "after "+p.name+"'s refused set")
		ps, _ := k.provider(p)
		if ps.Refused[wire.RefusedLoginHost] != 1 || len(ps.Relays) != 0 {
			t.Errorf("%s in the keeper's status: %d refusals for the login host and relays %+v, want 1 and none", p.name, ps.Refused[wire.RefusedLoginHost], ps.Relays)
		}
	}
	w.noSession(j2, serving, "for a refused Provider's Allocation")
	if n := len(w.sruns(j2.id)); n != 0 {
		t.Errorf("%d srun lines for the refused Providers' job, want none", n)
	}
	for _, ch := range agentapi.RelayChannels {
		if r, ok := k.upOf(provA, j1.tag, ch); !ok || r.PID != up[ch].PID || r.Starts != 1 {
			t.Errorf("the holder's %s relay %+v, want it up as it was, PID %d", ch, r, up[ch].PID)
		}
	}
	s.mustEcho(32<<10, "at the end")
	s.whole()
}
