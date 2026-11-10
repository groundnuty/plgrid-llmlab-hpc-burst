package conformance

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// TestReachHostDialed is the run on one platform: the real agent in host-dialed mode with no Instance, in one short job,
// and the two relays the Provider's controller starts to it through the login node (hostdialed_run_test.go). The
// settings of TestConformance apply, with HPCBURST_CONF_AGENT_BUILDS: the agent-builds directory hack/build-agents.sh
// made, whose build for the partition's cpuArch the controller stages and the job runs.
//
// A dry run (HPCBURST_ALPHA_DRY=1, for hack/livechecks) runs the same body over a fake Slurm in run mode behind a
// user-level sshd login node, with the agent built for this machine (hostdialed_dry_test.go). It refuses to start when a
// live setting is present, and its log says it is dry.
//
//livecheck:reach-host-dialed-helios,reach-host-dialed-athena
func TestReachHostDialed(t *testing.T) {
	if os.Getenv("HPCBURST_ALPHA_DRY") == "1" {
		dryReachHostDialed(t)
		return
	}
	s := load(t)
	agent, err := readAgentBuild(os.Getenv("HPCBURST_CONF_AGENT_BUILDS"), s.part.CPUArch)
	if err != nil {
		t.Fatal(err)
	}
	g := newHostDialedRig(t, s.control, "", nil)
	w := buildWorld(t, s, worldKit{agent: agent, hostDialed: g, dial: func(v slurm.Validated) (slurm.Runner, io.Closer, error) {
		ssh, err := sshrunNew(s, v)
		return ssh, ssh, err
	}})
	g.startHostSide(t, w)
	hostDialedBody(t, w, g, "reach-host-dialed-"+s.profile.Name)
}

// readAgentBuild reads the agent build for cpuArch from the agent-builds directory: a linux executable (ELF), the real
// agent, never a stand-in. A missing directory or build is named.
func readAgentBuild(dir, cpuArch string) ([]byte, error) {
	if dir == "" || !filepath.IsAbs(dir) {
		return nil, fmt.Errorf("HPCBURST_CONF_AGENT_BUILDS must name the agent-builds directory (hack/build-agents.sh), as an absolute path")
	}
	b, err := os.ReadFile(filepath.Join(dir, agentapi.AgentBuildName(cpuArch)))
	if err != nil {
		return nil, fmt.Errorf("the agent build for %s: %w", cpuArch, err)
	}
	if !bytes.HasPrefix(b, []byte("\x7fELF")) {
		return nil, fmt.Errorf("the agent build for %s is not a linux executable", cpuArch)
	}
	return b, nil
}
