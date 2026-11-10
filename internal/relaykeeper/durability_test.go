package relaykeeper

// New for the review of PR 354 (B1): every change is written and synced
// before it takes effect or is reported (HBEP-6, The keeper's state file).

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// readOnly makes the state file's directory refuse new files until undone,
// so a write fails before its rename.
func (r *rig) readOnly(t *testing.T) func() {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root writes into a read-only directory")
	}
	dir := filepath.Dir(r.path)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	undo := func() { _ = os.Chmod(dir, 0o700) }
	t.Cleanup(undo)
	return undo
}

const stepLimit = "srun refused the step: step limit reached for this job"

// A final end whose write fails is pending: it is on no disk and in no
// report, its Allocation starts and restarts nothing (the listing hides it,
// and its channel is seeded final), and every request is refused until the
// write lands; then the end is on disk and reported (the review of PR 354,
// B1).
func TestAFinalEndWhoseWriteFailsIsPendingUntilItLands(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	e := entry(1)
	accepted(t, k, set("example-a", 1, true, "login.example", e))
	w, _ := fr.want("example-a", e.Tag)
	undo := r.readOnly(t)
	w.OnFinal(agentapi.RelayServing, hostdial.EndFailed, stepLimit, k.Epoch(), 4)
	if ends := r.disk().Ends; len(ends) != 0 {
		t.Fatalf("an end whose write failed is on disk: %+v", ends)
	}
	if jobs, err := w.Jobs.ListByTag(context.Background(), e.Tag); err != nil || len(jobs) != 0 {
		t.Errorf("the listing of an Allocation with a pending end: %+v, %v; want no job, so nothing starts", jobs, err)
	}
	if got := w.Finals(); got[agentapi.RelayServing] != "failed: "+stepLimit {
		t.Errorf("the seeds of an Allocation with a pending end: %q, want its serving channel", got)
	}
	if got := refused(t, k, wire.KindSet, set("example-a", 1, true, "login.example", e)); got.Reason != wire.RefusedState {
		t.Errorf("a set while the end is pending and the disk refuses it: %+v, want refused for the state file", got)
	}
	undo()
	rep := accepted(t, k, set("example-a", 1, true, "login.example", e))
	if len(rep.Ends) != 1 || rep.Ends[0].Seq != 4 {
		t.Errorf("the report once the write landed: %+v, want the end", rep.Ends)
	}
	if ends := r.disk().Ends; len(ends) != 1 || ends[0].Decided {
		t.Errorf("the state file once the write landed: %+v, want the end, undecided", ends)
	}
}

// Settle, the keeper process's timer, writes a pending end with no request.
func TestSettleWritesAPendingEnd(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	e := entry(1)
	accepted(t, k, set("example-a", 1, true, "login.example", e))
	w, _ := fr.want("example-a", e.Tag)
	undo := r.readOnly(t)
	w.OnFinal(agentapi.RelayControl, hostdial.EndFailed, stepLimit, k.Epoch(), 3)
	if err := k.Settle(); err == nil {
		t.Error("Settle succeeded with the disk refusing writes")
	}
	undo()
	if err := k.Settle(); err != nil {
		t.Fatal(err)
	}
	if ends := r.disk().Ends; len(ends) != 1 {
		t.Errorf("the state file after Settle: %+v, want the end", ends)
	}
}

// A keeper that restarts while an end is pending loses it: it was never on
// disk, and never reported, so no leader acted on it. The restarted keeper
// knows no end, and seeds nothing: this is the stated limit of a disk that
// refuses writes (the relay's process died with the keeper, and a lasting
// cause ends the channel again).
func TestARestartWhileAnEndIsPendingKnowsNoEnd(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	e := entry(1)
	accepted(t, k, set("example-a", 1, true, "login.example", e))
	w, _ := fr.want("example-a", e.Tag)
	undo := r.readOnly(t)
	w.OnFinal(agentapi.RelayServing, hostdial.EndFailed, stepLimit, k.Epoch(), 4)
	undo()
	k2, fr2 := r.open()
	rep := accepted(t, k2, set("example-a", 1, true, "login.example", e))
	w2, _ := fr2.want("example-a", e.Tag)
	if len(rep.Ends) != 0 || len(r.disk().Ends) != 0 || len(w2.Finals()) != 0 {
		t.Errorf("after the restart: ends %+v, on disk %+v, seeds %q; want none", rep.Ends, r.disk().Ends, w2.Finals())
	}
}

// A directory sync that fails after the rename leaves the keeper's memory at
// the file's state, never below it: a stale set from a lower epoch is still
// refused, so the fence is never lowered (Opus's example: epoch 9 renamed
// onto the disk, a stale epoch 6). Until a write syncs, every request is
// refused and nothing takes effect; then the set takes effect.
func TestASyncFailureAfterTheRenameNeverLowersAFence(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	accepted(t, k, set("example-a", 5, true, "login.example", entry(1)))
	fr.log()
	failSync := true
	saved := fsops.syncDir
	fsops.syncDir = func(d *os.File) error {
		if failSync {
			return errors.New("an injected EIO")
		}
		return saved(d)
	}
	t.Cleanup(func() { fsops.syncDir = saved })
	if got := refused(t, k, wire.KindSet, set("example-a", 9, true, "login.example", entry(2))); got.Reason != wire.RefusedState {
		t.Fatalf("the set whose write did not sync: %+v, want refused for the state file", got)
	}
	if d := r.disk().Providers["example-a"].HighestEpoch; d != 9 {
		t.Fatalf("the fence on disk %d, want 9: the rename was made", d)
	}
	if got := refused(t, k, wire.KindSet, set("example-a", 6, true, "login.example", entry(1))); got.Reason != wire.RefusedState {
		t.Errorf("a set while no write syncs: %+v, want refused for the state file", got)
	}
	if calls := fr.log(); len(calls) != 0 {
		t.Errorf("sets took effect while no write synced: %q", calls)
	}
	failSync = false
	if got := refused(t, k, wire.KindSet, set("example-a", 6, true, "login.example", entry(1))); got.Reason != wire.RefusedEpoch || got.HighestEpoch != 9 {
		t.Errorf("the stale epoch 6 after the sync: %+v, want refused for its epoch, naming 9", got)
	}
	if d := r.disk().Providers["example-a"].HighestEpoch; d != 9 {
		t.Errorf("the fence on disk %d, want 9", d)
	}
	accepted(t, k, set("example-a", 9, true, "login.example", entry(2)))
	if calls := fr.log(); !slices.Contains(calls, "want example-a "+entry(2).Tag) {
		t.Errorf("the set at epoch 9 after the sync took no effect: %q", calls)
	}
}

// Each write syncs the file and then its directory, once each: a write that
// skipped either could be lost by a crash after the keeper acted on it.
func TestEachWriteSyncsTheFileAndItsDirectory(t *testing.T) {
	r := newRig(t)
	k, _ := r.open()
	var calls []string
	savedFile, savedDir := fsops.syncFile, fsops.syncDir
	fsops.syncFile = func(f *os.File) error {
		calls = append(calls, "file "+filepath.Base(f.Name()))
		return savedFile(f)
	}
	fsops.syncDir = func(d *os.File) error {
		calls = append(calls, "dir "+filepath.Base(d.Name()))
		return savedDir(d)
	}
	t.Cleanup(func() { fsops.syncFile, fsops.syncDir = savedFile, savedDir })
	accepted(t, k, set("example-a", 3, true, "login.example", entry(1)))
	if len(calls) != 2 || !strings.HasPrefix(calls[0], "file .state-") || calls[1] != "dir "+filepath.Base(filepath.Dir(r.path)) {
		t.Errorf("the syncs of one write: %q, want the temporary file's, then the directory's", calls)
	}
}
