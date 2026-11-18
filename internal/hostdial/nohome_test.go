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
	want, err := filepath.EvalSymlinks(j.allocDir) // what /proc and pwd say, if the harness's directory sits behind a link
	if err != nil {
		t.Fatal(err)
	}

	// The control: this cluster does refuse a step with no --chdir, so a relay
	// that comes up below came up because of the flag. The same step with the
	// Allocation directory runs there.
	res, err := w.node.Session().Run(t.Context(), slurm.Command{Line: "srun --overlap --jobid=" + j.id + " --ntasks=1 true"})
	if err != nil || res.ExitCode != 1 || !strings.Contains(string(res.Stderr), "couldn't chdir to '") {
		t.Fatalf("the control: an srun step with no --chdir on this cluster gave exit %d, stderr %q, %v; want exit 1 and the chdir refusal", res.ExitCode, res.Stderr, err)
	}
	res, err = w.node.Session().Run(t.Context(), slurm.Command{Line: "srun --overlap --jobid=" + j.id + " --ntasks=1 --chdir=" + j.allocDir + " pwd"})
	if err != nil || res.ExitCode != 0 || strings.TrimSpace(string(res.Stdout)) != want {
		t.Fatalf("the control: an srun step with --chdir=<the Allocation directory> gave exit %d, stdout %q, stderr %q, %v; want its directory", res.ExitCode, res.Stdout, res.Stderr, err)
	}

	own, _, _ := w.owner(hostdial.OwnerOptions{})
	own.Want(w.target(j))
	waitUp(t, own, j.tag)

	// What the owner sent over ssh: the fake logs each srun line it was given, refused or not (the control's two steps above
	// are among them, and are not relay lines). A line without --chdir fails here at once and by name, before the round trips
	// below fail on their minute's wait for a greeting.
	relayLines := func() (out []string) {
		for _, line := range w.sruns(j.id) {
			if strings.Contains(line, " relay ") {
				out = append(out, line)
			}
		}
		return out
	}
	waitFor(t, time.Minute, "an srun line for each of job "+j.id+"'s two relays", func() bool { return len(relayLines()) >= 2 })
	for _, line := range relayLines() {
		if !strings.Contains(line, " --ntasks=1 --chdir="+j.allocDir+" ") {
			t.Errorf("a relay ran %q, without --chdir=%s after --ntasks=1", line, j.allocDir)
		}
	}

	roundTrip(t, j, control, "control", payload(50_000))
	roundTrip(t, j, serving, "serving", payload(50_000))

	// Each relay's step runs in the Allocation directory, as /proc says. A relay that was restarted leaves its old step for
	// a moment, so wait for exactly one live step per channel.
	for _, ch := range agentapi.RelayChannels {
		var cwds []string
		end := time.Now().Add(time.Minute)
		for {
			cwds = cwds[:0]
			for _, pid := range stepPIDs(t, j.id, ch) {
				cwd, err := os.Readlink("/proc/" + strconv.Itoa(pid) + "/cwd")
				if err != nil {
					cwd = "(unreadable)"
				}
				cwds = append(cwds, cwd)
			}
			if len(cwds) == 1 && cwds[0] == want {
				break
			}
			if time.Now().After(end) {
				t.Fatalf("the %s relay's live steps of job %s run in %q, want exactly one, in the Allocation directory %q", ch, j.id, cwds, want)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}
