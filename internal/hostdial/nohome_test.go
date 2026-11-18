package hostdial_test

// HBEP-4's and HBEP-6's amendment of 18 Nov 2026 (#735's probe 1): on a
// cluster whose compute nodes have no home directory (Sano), srun from the
// login node starts a step in the login's home and the node refuses it
// ("couldn't chdir"), so the relay's line names the Allocation directory with
// --chdir. This row runs the real path of harness_test.go on such a cluster
// (the fake Slurm's RunMode.NoComputeHome), where a relay started without the
// flag never gets a step. The expected values are the amendment's rules and
// the directories the harness made; none is read from RelayLine.

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Both relays come up and carry bytes on a cluster with no home on its compute
// nodes, and each relay's step runs in the Allocation's directory. It fails
// when the relay's line leaves --chdir out: the step is then refused, no
// session ever opens and the greeting never comes.
func TestHostDialedRelaysStartWhereComputeNodesHaveNoHome(t *testing.T) {
	w := newWorld(t, func(c *worldConfig) { c.noComputeHome = true })
	j := w.submit("hb-relay-nohome-0123456789ab", 30*time.Second)

	// The control: this cluster does refuse a step with no --chdir, so a relay
	// that comes up below came up because of the flag. The same step with the
	// Allocation directory runs there.
	res, err := w.node.Session().Run(t.Context(), slurm.Command{Line: "srun --overlap --jobid=" + j.id + " --ntasks=1 true"})
	if err != nil || res.ExitCode != 1 || !strings.Contains(string(res.Stderr), "couldn't chdir to '") {
		t.Fatalf("the control: an srun step with no --chdir on this cluster gave exit %d, stderr %q, %v; want exit 1 and the chdir refusal", res.ExitCode, res.Stderr, err)
	}
	res, err = w.node.Session().Run(t.Context(), slurm.Command{Line: "srun --overlap --jobid=" + j.id + " --ntasks=1 --chdir=" + j.allocDir + " pwd"})
	if err != nil || res.ExitCode != 0 || strings.TrimSpace(string(res.Stdout)) != j.allocDir {
		t.Fatalf("the control: an srun step with --chdir=<the Allocation directory> gave exit %d, stdout %q, stderr %q, %v; want its directory", res.ExitCode, res.Stdout, res.Stderr, err)
	}

	own, _, _ := w.owner(hostdial.OwnerOptions{})
	own.Want(w.target(j))
	waitUp(t, own, j.tag)
	roundTrip(t, j, control, "control", payload(50_000))
	roundTrip(t, j, serving, "serving", payload(50_000))

	want, err := filepath.EvalSymlinks(j.allocDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, ch := range agentapi.RelayChannels {
		pids := stepPIDs(t, j.id, ch)
		if len(pids) != 1 {
			t.Fatalf("the %s relay has %d live steps in job %s, want 1: %v", ch, len(pids), j.id, pids)
		}
		got, err := os.Readlink("/proc/" + strconv.Itoa(pids[0]) + "/cwd")
		if err != nil || got != want {
			t.Errorf("the %s relay's step runs in %q (%v), want the Allocation directory %q", ch, got, err, want)
		}
	}
	for _, line := range w.sruns(j.id) {
		if strings.Contains(line, " relay ") && !strings.Contains(line, " --chdir="+j.allocDir+" ") {
			t.Errorf("a relay ran %q, without --chdir=%s", line, j.allocDir)
		}
	}
}
