package agentapi

// Host-dialed mode's relay contract (HBEP-6, Dial-home modes, host-dialed
// mode): the two channels, the srun line the batch Provider's controller runs
// on the login node for each, and where the job side's Unix sockets are. The
// controller, the agent and the relay step all read these, so they agree by
// construction.

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// RelayChannel is one of host-dialed mode's two channels. Each has its own
// relay, so control and the token tunnel never share a connection (HBEP-6,
// Risks: head-of-line blocking).
type RelayChannel string

const (
	// RelayControl carries the agent's control traffic; its relay ends at the
	// agent endpoint's loopback port (base+3).
	RelayControl RelayChannel = "control"
	// RelayServing carries frpc's wss; its relay ends at the dial-home
	// listener's loopback port (base+7).
	RelayServing RelayChannel = "serving"
)

// RelayChannels are both channels, control first.
var RelayChannels = []RelayChannel{RelayControl, RelayServing}

// ParseRelayChannel returns the channel named s, or an error naming the two.
func ParseRelayChannel(s string) (RelayChannel, error) {
	for _, c := range RelayChannels {
		if string(c) == s {
			return c, nil
		}
	}
	return "", fmt.Errorf("relay channel %q: want %q or %q", s, RelayControl, RelayServing)
}

// RelaySubcommand is the agent binary's first argument that makes it a relay:
// "<agent binary> relay <channel>".
const RelaySubcommand = "relay"

// jobIDForm is a Slurm job ID: digits only, no leading zero.
var jobIDForm = regexp.MustCompile(`^[1-9][0-9]{0,19}$`)

// RelayLine is the command line a relay runs on the login node (HBEP-6,
// Dial-home modes, host-dialed mode):
//
//	srun -u --overlap --jobid=<job> --ntasks=1 <agent binary> relay <channel>
//
// -u is required: without it srun held output that ends without a newline
// until the stream closed (probe of 9 Nov 2026), and a multiplexed session's
// frames do not end in newlines. agentPath is the staged agent binary's
// absolute path on the cluster (StagingDir joined with StagedAgentPath), a
// safe path, so every word is plain and needs no quoting.
func RelayLine(jobID, agentPath string, ch RelayChannel) (string, error) {
	var errs []error
	if !jobIDForm.MatchString(jobID) {
		errs = append(errs, fmt.Errorf("job ID %q is not a Slurm job ID", jobID))
	}
	if !safePath.MatchString(agentPath) || filepath.Clean(agentPath) != agentPath {
		errs = append(errs, fmt.Errorf("agent binary %q: want a clean absolute path of letters, digits and ._:=,/-", agentPath))
	}
	if _, err := ParseRelayChannel(string(ch)); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return "", err
	}
	return "srun -u --overlap --jobid=" + jobID + " --ntasks=1 " + agentPath + " " + RelaySubcommand + " " + string(ch), nil
}

// DefaultRelayRoot is the node-local directory that holds each job's relay
// directory (RelayDirPrefix). It is not TMPDIR: a step that srun starts from the login node
// gets the login session's environment, not the batch script's TMPDIR (which
// a partition's tmpDir may set to a per-job memory file system), so a path
// from TMPDIR could differ between the agent and its relay. Both run on the
// job's node, where /tmp is the same for every step of the job (the job's own
// /tmp under a job container, the node's otherwise).
const DefaultRelayRoot = "/tmp"

// EnvRelayRoot overrides DefaultRelayRoot. It exists for tests, whose jobs
// and steps run on the test's host and share its environment; a cluster
// leaves it unset, since a step started from the login node would not see a
// value the batch script set.
const EnvRelayRoot = "HPCBURST_RELAY_ROOT"

// maxRelaySocketPath is the longest relay socket path: a Unix socket path is
// at most 107 bytes and a NUL.
const maxRelaySocketPath = 107

// RelayRoot is the relay root for an environment: EnvRelayRoot when it is
// set, else DefaultRelayRoot.
func RelayRoot(getenv func(string) string) string {
	if r := getenv(EnvRelayRoot); r != "" {
		return r
	}
	return DefaultRelayRoot
}

// RelayDirPrefix is the start of a job's relay directory's name under the
// relay root: hb-relay-<job ID>-. The agent makes the directory with a random
// rest (os.MkdirTemp), mode 700, so another user on the node cannot create it
// first and stop the agent (review of PR 301, F5); it publishes the name in
// the user's staging directory (RelayPointerPath), where the relay reads it.
func RelayDirPrefix(jobID string) (string, error) {
	if !jobIDForm.MatchString(jobID) {
		return "", fmt.Errorf("job ID %q is not a Slurm job ID", jobID)
	}
	return "hb-relay-" + jobID + "-", nil
}

// CheckRelayDir refuses a relay directory that is not one the agent of job
// would make under root: <root>/hb-relay-<job ID>-<letters and digits>, clean,
// and short enough for its sockets.
func CheckRelayDir(root, jobID, dir string) error {
	prefix, err := RelayDirPrefix(jobID)
	if err != nil {
		return err
	}
	if !safePath.MatchString(root) || filepath.Clean(root) != root {
		return fmt.Errorf("relay root %q: want a clean absolute path of letters, digits and ._:=,/-", root)
	}
	rest, ok := strings.CutPrefix(filepath.Base(dir), prefix)
	if filepath.Clean(dir) != dir || filepath.Dir(dir) != root || !ok || rest == "" || !relayRest.MatchString(rest) {
		return fmt.Errorf("relay directory %q is not %s/%s<random> for job %s", dir, root, prefix, jobID)
	}
	if longest := RelaySocket(dir, RelayControl); len(longest) > maxRelaySocketPath {
		return fmt.Errorf("relay socket %q is %d bytes, above the %d a Unix socket path allows: use a shorter relay root", longest, len(longest), maxRelaySocketPath)
	}
	return nil
}

// relayRest is the random part os.MkdirTemp appends: decimal digits.
var relayRest = regexp.MustCompile(`^[0-9A-Za-z]{1,20}$`)

// RelayPointerDirName is the directory in the user's staging directory that
// holds each running job's relay pointer, mode 700.
const RelayPointerDirName = "relay"

// RelayPointerPath is where the agent of job publishes its relay directory:
// <stagingDir>/relay/<job ID>, a file of the user's own, mode 600, holding the
// directory's path. The staging directory is on the cluster's shared file
// system and the user's, so no other user can place a pointer there; the
// sockets themselves stay on the node, under the relay root, since Unix
// sockets on a shared file system are not reliable.
func RelayPointerPath(stagingDir, jobID string) (string, error) {
	if !jobIDForm.MatchString(jobID) {
		return "", fmt.Errorf("job ID %q is not a Slurm job ID", jobID)
	}
	if !safePath.MatchString(stagingDir) || filepath.Clean(stagingDir) != stagingDir {
		return "", fmt.Errorf("staging directory %q: want a clean absolute path of letters, digits and ._:=,/-", stagingDir)
	}
	return filepath.Join(stagingDir, RelayPointerDirName, jobID), nil
}

// StagingDirOfAgent is the staging directory a staged agent binary runs from:
// the relay step's program is <stagingDir>/agent/<digest>/hb-agent
// (StagedAgentPath), so the relay finds its job's pointer without being told.
func StagingDirOfAgent(agentPath string) (string, error) {
	digestDir := filepath.Dir(agentPath)
	agentDir := filepath.Dir(digestDir)
	if filepath.Clean(agentPath) != agentPath || !filepath.IsAbs(agentPath) || filepath.Base(agentPath) != AgentBinaryName ||
		!pinForm.MatchString(filepath.Base(digestDir)) || filepath.Base(agentDir) != "agent" {
		return "", fmt.Errorf("%q is not a staged agent, <stagingDir>/agent/<digest>/%s", agentPath, AgentBinaryName)
	}
	return filepath.Dir(agentDir), nil
}

// RelaySocket is the agent's Unix socket for a channel in the relay
// directory: <dir>/<channel>.sock.
func RelaySocket(dir string, ch RelayChannel) string {
	return filepath.Join(dir, string(ch)+".sock")
}

// RelayChannelPorts maps each channel to its loopback port on the
// control-plane host, the only port its relay dials (HBEP-6, Dial-home modes,
// host-dialed mode): control to the agent endpoint, serving to the dial-home
// listener.
func (h Host) RelayChannelPorts() map[RelayChannel]int {
	return map[RelayChannel]int{RelayControl: h.AgentEndpointPort(), RelayServing: h.DialHomePort()}
}

// RelayTargetAddr is a channel's loopback target on the control-plane host:
// 127.0.0.1:<port>.
func RelayTargetAddr(port int) string { return "127.0.0.1:" + strconv.Itoa(port) }
