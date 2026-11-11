package scenario_test

// HBEP-6's relay keeper item "after restore, the real leader's lower epoch is
// accepted" on the whole stack (Test plan, The relay keeper (beta); The
// keeper's state file: "The launcher's restore clears the accepted Lease
// epochs, since a restored store rolls leaseTransitions back"; issue #343,
// PR 5). The natural path: up on the older k3s with --relay-keeper, upgrade,
// which backs the store up, down, restore --backup, up --relay-keeper. One
// leadership hand-over after the upgrade raises the keeper's fence above the
// epoch the restored Lease gives the next leader: without it the fence would
// equal that epoch, which the keeper accepts whether or not restore cleared
// anything, and the test would show nothing.

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
)

// keeperOlderK3s is the release the restore test starts on and restores to;
// keeperNewerK3s, the rehearsal's own, is what it upgrades to.
const (
	keeperOlderK3s = "1.35.9"
	keeperNewerK3s = "1.36.5"
)

// TestRehearsalKeeperRestoreAcceptsTheLeadersLowerEpoch: after a restore, the
// real leader's Lease epoch, lower than the keeper's fence before it, is
// accepted: the keeper starts the running Allocation's relays at its next E,
// frpc logs in again over them, and a stream crosses whole and in order.
//
//livecheck:none the keeper rehearsal; hack/rehearsal.sh runs it, with REHEARSAL_RUN naming it
func TestRehearsalKeeperRestoreAcceptsTheLeadersLowerEpoch(t *testing.T) {
	b := requireRehearsal(t)
	if fi, err := os.Stat(filepath.Join(b.K3sDir, "k3s-"+keeperOlderK3s, "k3s")); err != nil || fi.Mode()&0o111 == 0 {
		t.Fatalf("the restore test upgrades from k3s %s, missing from HPCBURST_BIN_DIR (hack/fetch-k3s.sh DIR %s %s): %v", keeperOlderK3s, keeperOlderK3s, keeperNewerK3s, err)
	}
	// No watch: down and the restore stop and roll back the API it would follow.
	r := startKeeperRig(t, rigSetup{noWatch: true, k3sVersion: keeperOlderK3s})
	in := r.keeperStart()
	key := r.fc.KeyFor(in)
	v0 := r.keeperRelays("on k3s "+keeperOlderK3s, in.Status.JobID)
	ref := r.quietReference("the quiet reference stream", in, key, nil)
	l0, ok := r.providerLease()
	if !ok || l0.holder == "" {
		r.t.Fatalf("the Provider's Lease has no holder: %+v", l0)
	}
	r.waitFence("before the upgrade", l0.transitions)

	// The upgrade backs the store up with the Lease at l0's epoch. The whole
	// group restarts, the keeper at its next E, and the next leader takes the
	// Lease at the next epoch.
	start := time.Now()
	backup, log, err := r.h.UpgradeTo(keeperNewerK3s)
	if err != nil {
		r.t.Fatalf("upgrade to %s: %v\n%s", keeperNewerK3s, err, log)
	}
	r.t.Logf("the upgrade took %s; its backup %s", time.Since(start).Round(100*time.Millisecond), backup)
	r.c = stack.KubeClient(r.t, r.h.State)
	l1 := r.newLeader("after the upgrade", nil, l0.holder)
	if l1.transitions != l0.transitions+1 {
		r.t.Errorf("after the upgrade the Lease's leaseTransitions went %d -> %d; client-go adds one on a holder change", l0.transitions, l1.transitions)
	}
	r.waitFence("after the upgrade", l1.transitions)
	v1 := r.keeperRelays("after the upgrade", in.Status.JobID)
	if v1.epoch != v0.epoch+1 {
		r.t.Errorf("the keeper after the upgrade is at E %d, was %d: want the next", v1.epoch, v0.epoch)
	}
	r.tunnelBack("after the upgrade", in)

	// One hand-over raises the fence above the epoch the restored Lease gives.
	ctl, _, err := pinEntry(r.t, r.h.State, "controller", v1.sup)
	if err != nil {
		r.t.Fatalf("pin the controller: %v", err)
	}
	if err := ctl.signal(syscall.SIGTERM); err != nil {
		r.t.Fatalf("TERM the controller %d through its pidfd: %v", ctl.pid, err)
	}
	ctl.waitExited(r.t, stack.WaitDeadline)
	l2 := r.newLeader("after the hand-over", ctl, l1.holder)
	r.waitFence("after the hand-over", l2.transitions)
	fence, _ := r.keeperFence()

	// Down, restore, up: the store and its Lease go back to l0's epoch.
	if err := r.h.DownErr(); err != nil {
		r.t.Fatalf("down: %v", err)
	}
	if out, err := r.h.RestoreFrom(backup); err != nil {
		r.t.Fatalf("restore --backup %s: %v\n%s", backup, err, out)
	}
	if out, err := r.h.UpAgain("--relay-keeper"); err != nil {
		r.t.Fatalf("up --relay-keeper after the restore: %v\n%s", err, out)
	}
	r.c = stack.KubeClient(r.t, r.h.State)
	// The restored Lease is the backup's: released by the old leader, or still
	// naming it, so the leader after the restore is another holder than l0's.
	ln := r.newLeader("after the restore", nil, l0.holder)
	if ln.transitions >= fence {
		r.t.Fatalf("the leader after the restore is at Lease epoch %d, not below the keeper's fence %d before it: the test would show nothing", ln.transitions, fence)
	}
	r.waitFence("after the restore, the leader's lower epoch", ln.transitions)
	r.t.Logf("after the restore the keeper accepted the leader's Lease epoch %d, below its fence %d before the restore (the backup's Lease at %d)", ln.transitions, fence, l0.transitions)
	vn := r.keeperRelays("after the restore", in.Status.JobID)
	if vn.epoch != v1.epoch+1 {
		r.t.Errorf("the keeper after the restore is at E %d, was %d: want the next", vn.epoch, v1.epoch)
	}
	if got := providerPart(r.keeperReport()).Refused; got[wire.RefusedEpoch] != 0 {
		r.t.Errorf("the keeper refused %d sets or acknowledgements for their epoch after the restore: %v", got[wire.RefusedEpoch], got)
	}
	r.tunnelBack("after the restore", in)
	key = r.fc.KeyFor(in)
	first, done := r.fc.StartStream(keeperStreamBody(), key)
	r.firstToken("after the restore", first)
	r.wholeStream("after the restore", r.keeperStream("after the restore", done), in, ref)
	if n := r.keeperStarts("after the restore"); n != 3 {
		r.t.Errorf("the keeper's state file records %d starts, want 3: up, the upgrade and the up after the restore", n)
	}
}
