package scenario_test

// The one-host rehearsal of issue #146: the real hpcburst up (k3s, the
// controller, frps and the front, supervised) manages a batch Provider whose
// cluster is the fake Slurm in run mode, reached over real ssh and sshd. The
// real hb-agent starts inside the fake job, with the engine double as its
// engine. Everything is asserted from the API, events, the front's usage
// files and /metrics, never from log lines.
//
// It runs only with HPCBURST_REHEARSAL=1 (hack/rehearsal.sh), so the ordinary
// go test ./... skips it; with that set, a missing piece fails the test.

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

//livecheck:none the package's TestMain: it runs the tests, it is not one
func TestMain(m *testing.M) {
	enginedouble.Dispatch() // the fake apptainer and engine roles
	rehearsal.Dispatch()    // the login node's forced command
	start := time.Now()
	code := m.Run()
	if os.Getenv("HPCBURST_REHEARSAL") == "1" {
		// Issue #146, criterion 4: the rehearsal should take under 15 minutes.
		// It is a measured target, not a bound: hack/rehearsal.sh sets
		// -test.timeout well above it, so that a slow run still cleans up.
		took := time.Since(start).Round(time.Second)
		fmt.Printf("the rehearsal's tests took %s (issue #146's target: under %s)\n", took, rehearsalTarget)
		if took > rehearsalTarget {
			fmt.Printf("FINDING: the rehearsal took %s, above issue #146's target of %s\n", took, rehearsalTarget)
		}
	}
	os.Exit(code)
}

// rehearsalTarget is issue #146's time for the whole rehearsal.
const rehearsalTarget = 15 * time.Minute

// ports146: issue #146's block is 29500-29539. hpcburst up takes launcher
// base 29520 (29520-29532); the front's client and dial-home listeners take
// 29500 and 29501.
var ports146 = stack.Ports{Base: 29520, ClientAddr: "127.0.0.1:29500", DialHomeListen: "127.0.0.1:29501"}

// requireRehearsal finds what the rehearsal runs: the four binaries of this
// repo, built by hack/rehearsal.sh into $HPCBURST_REHEARSAL_BIN, k3s from
// $HPCBURST_BIN_DIR, and the pinned frps and frpc from $HPCBURST_FRP_DIR.
func requireRehearsal(t *testing.T) stack.Binaries {
	t.Helper()
	if os.Getenv("HPCBURST_REHEARSAL") != "1" {
		t.Skip("the one-host rehearsal runs the real stack, and only with HPCBURST_REHEARSAL=1 (hack/rehearsal.sh)")
	}
	dir := os.Getenv("HPCBURST_REHEARSAL_BIN")
	b := stack.Binaries{
		HPCBurst: filepath.Join(dir, "hpcburst"), Controller: filepath.Join(dir, "hpcburst-controller"),
		Front: filepath.Join(dir, "hpcburst-front"), Agent: filepath.Join(dir, "hb-agent"),
		K3sDir: os.Getenv("HPCBURST_BIN_DIR"),
	}
	if missing := b.MissingBuilds(); len(missing) > 0 {
		t.Fatalf("HPCBURST_REHEARSAL=1 but these binaries are missing (HPCBURST_REHEARSAL_BIN, HPCBURST_BIN_DIR): %v", missing)
	}
	b.Frps, b.Frpc = testenv.FrpBinaries(t)
	repo, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	b.Repo = repo
	return b
}

// rigCluster is the stack's fake cluster with the rehearsal's own fixtures.
type rigCluster struct {
	*stack.Cluster
	// cmds records every command the controller ran on the cluster, with
	// the slot holders just before it (cmdlog_test.go).
	cmds *cmdLog
	// gate holds engine starts while armed; nil unless the rig asks for it
	// (enginegate_test.go).
	gate *engineGate
}

// startRigCluster starts the stack's fake cluster with the login node's command
// record and, when gated, the engine gate in front of the jobs' apptainer; the
// gate holds nothing until armed.
func startRigCluster(t *testing.T, dir string, shape stack.ClusterShape, gated bool) *rigCluster {
	t.Helper()
	c := &rigCluster{}
	shape.Around = func(plat *fakeslurm.Platform) func(string, func() (slurm.Result, error)) (slurm.Result, error) {
		c.cmds = &cmdLog{plat: plat}
		return c.cmds.around
	}
	if gated {
		shape.WrapApptainer = func(fakes string) string {
			var wrapper string
			c.gate, wrapper = installEngineGate(t, dir, fakes)
			return wrapper
		}
	}
	c.Cluster = stack.StartCluster(t, dir, shape, enginedouble.Install, nil)
	return c
}
