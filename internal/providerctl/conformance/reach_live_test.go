package conformance

import (
	"io"
	"os"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// TestReach is the run on one platform: one minimal job that tries a TCP connect to the gateway's forwarded port
// (reach_run_test.go). The settings of TestConformance apply, with HPCBURST_CONF_PROBE_HOST, _PORT and _LABEL.
//
// A dry run (HPCBURST_ALPHA_DRY=1, for hack/livechecks) runs the same body over a fake Slurm in run mode, where
// the job is the same probe script, run on this machine against the stand-ins the test points it at. It refuses to
// start when a live setting is present, and its log says it is dry.
//
//livecheck:reach-helios,reach-athena
func TestReach(t *testing.T) {
	if os.Getenv("HPCBURST_ALPHA_DRY") == "1" {
		dryReach(t)
		return
	}
	s := load(t)
	target, err := readProbeTarget(os.Getenv, liveProbePrefix)
	if err != nil {
		t.Fatal(err)
	}
	w := buildWorld(t, s, worldKit{agent: target.agent(reachConnectSeconds, reachReadSeconds), dial: func(v slurm.Validated) (slurm.Runner, io.Closer, error) {
		ssh, err := sshrunNew(s, v)
		return ssh, ssh, err
	}})
	reachBody(t, w, "reach-"+s.profile.Name, target)
}
