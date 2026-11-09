package agentapi

// Host-dialed mode's relay contract (HBEP-6, Dial-home modes, host-dialed
// mode, as PR 298 settles it at b12aa9f): the two channels, the srun line the
// batch Provider's controller runs on the login node for each, the
// rendezvous file in the Allocation's directory, the agent's greeting and the
// relay's hello on the agent's socket, and the preamble the relay writes to
// the host side. The controller, the agent and the relay step all read these,
// so they agree by construction.

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"path"
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

// RelaySubcommand is the agent binary's first argument that makes it a relay.
const RelaySubcommand = "relay"

var (
	// jobIDForm is a Slurm job ID: digits only, no leading zero.
	jobIDForm = regexp.MustCompile(`^[1-9][0-9]{0,19}$`)
	// uidForm is an Allocation's UID as it goes on a command line: a
	// Kubernetes UID (a UUID) or the like, letters, digits and '-'.
	uidForm = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,63}$`)
)

// RelayArgs are a relay's arguments after "relay":
//
//	<channel> --dir <Allocation directory> --allocation <UID> --epoch <E> --seq <N>
//
// Dir is the Allocation's directory on the cluster (StagingDir joined with
// AllocationDir), where the agent publishes its socket (RelaySocketFile).
// Allocation is the Allocation's UID, which the agent's greeting must name.
// Epoch is the provider-<name> Lease's leaseTransitions when the relay's
// controller took the lead, and Seq a sequence per channel, larger for each
// later relay of the same epoch: the agent keeps the session with the highest
// (Epoch, Seq) on each channel and refuses a lower one as superseded
// (Fencing).
type RelayArgs struct {
	Channel    RelayChannel
	Dir        string
	Allocation string
	Epoch      int64
	Seq        int64
}

// Validate returns every problem with a, joined.
func (a RelayArgs) Validate() error {
	var errs []error
	if _, err := ParseRelayChannel(string(a.Channel)); err != nil {
		errs = append(errs, err)
	}
	if !safePath.MatchString(a.Dir) || path.Clean(a.Dir) != a.Dir {
		errs = append(errs, fmt.Errorf("--dir %q: want a clean absolute path of letters, digits and ._:=,/-", a.Dir))
	}
	if !uidForm.MatchString(a.Allocation) {
		errs = append(errs, fmt.Errorf("--allocation %q is not an Allocation UID", a.Allocation))
	}
	if a.Epoch < 0 {
		errs = append(errs, fmt.Errorf("--epoch %d: want 0 or more", a.Epoch))
	}
	if a.Seq < 1 {
		errs = append(errs, fmt.Errorf("--seq %d: want 1 or more", a.Seq))
	}
	return errors.Join(errs...)
}

// words are a's arguments in their one order.
func (a RelayArgs) words() []string {
	return []string{string(a.Channel), "--dir", a.Dir, "--allocation", a.Allocation,
		"--epoch", strconv.FormatInt(a.Epoch, 10), "--seq", strconv.FormatInt(a.Seq, 10)}
}

// ParseRelayArgs reads a relay's arguments after "relay": the channel, then
// the four flags, each required.
func ParseRelayArgs(args []string) (RelayArgs, error) {
	if len(args) == 0 {
		return RelayArgs{}, errors.New("no relay channel")
	}
	ch, err := ParseRelayChannel(args[0])
	if err != nil {
		return RelayArgs{}, err
	}
	a := RelayArgs{Channel: ch, Epoch: -1}
	fs := flag.NewFlagSet("relay", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&a.Dir, "dir", "", "")
	fs.StringVar(&a.Allocation, "allocation", "", "")
	fs.Int64Var(&a.Epoch, "epoch", -1, "")
	fs.Int64Var(&a.Seq, "seq", 0, "")
	if err := fs.Parse(args[1:]); err != nil {
		return RelayArgs{}, fmt.Errorf("relay %s: %w", ch, err)
	}
	if fs.NArg() != 0 {
		return RelayArgs{}, fmt.Errorf("relay %s: unexpected arguments %q", ch, fs.Args())
	}
	return a, a.Validate()
}

// RelayLine is the command line a relay runs on the login node (HBEP-6,
// Dial-home modes, host-dialed mode):
//
//	srun -u --overlap --jobid=<job> --ntasks=1 <agent binary> relay <channel> --dir <dir> --allocation <UID> --epoch <E> --seq <N>
//
// -u is required: without it srun held output that ends without a newline
// until the stream closed (probe of 9 Nov 2026), and with it the task's
// standard output is a pty with OPOST cleared, which carried all 256 byte
// values unchanged. agentPath is the staged agent binary's absolute path on
// the cluster (StagingDir joined with StagedAgentPath). Every word is plain,
// so none needs quoting.
func RelayLine(jobID, agentPath string, a RelayArgs) (string, error) {
	var errs []error
	if !jobIDForm.MatchString(jobID) {
		errs = append(errs, fmt.Errorf("job ID %q is not a Slurm job ID", jobID))
	}
	if !safePath.MatchString(agentPath) || filepath.Clean(agentPath) != agentPath {
		errs = append(errs, fmt.Errorf("agent binary %q: want a clean absolute path of letters, digits and ._:=,/-", agentPath))
	}
	if err := a.Validate(); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return "", err
	}
	return strings.Join(append([]string{"srun", "-u", "--overlap", "--jobid=" + jobID, "--ntasks=1", agentPath, RelaySubcommand}, a.words()...), " "), nil
}

// RelaySocketFileName is the rendezvous file in the Allocation's directory:
// the agent writes its socket's absolute path there, mode 600, by a temporary
// file and a rename, and the relay reads it (HBEP-6, The rendezvous).
const RelaySocketFileName = "relay-socket"

// RelaySocketFile is <dir>/relay-socket.
func RelaySocketFile(allocDir string) string { return path.Join(allocDir, RelaySocketFileName) }

// MaxRelaySocketPath is the longest socket path the rendezvous file may name:
// a Unix socket path is at most 107 bytes and a NUL.
const MaxRelaySocketPath = 107

// The lines of the relay's exchange (HBEP-6, The wire and The rendezvous),
// each ending in a newline on the wire:
//   - the agent's greeting, which it writes first on each connection to its
//     socket: "hpcburst-agent/1 <Allocation UID>";
//   - the relay's hello, once it has checked the greeting:
//     "relay/1 <channel> <E> <N>";
//   - the agent's answer: RelayAccepted, then the session, or
//     RelaySuperseded, and the connection closes;
//   - the preamble, which the relay writes first to its standard output,
//     before the session: "hpcburst-relay/1 <Allocation UID>".
const (
	RelayAccepted   = "ok"
	RelaySuperseded = "superseded"
)

// RelayGreeting is the agent's greeting for the Allocation uid.
func RelayGreeting(uid string) string { return "hpcburst-agent/1 " + uid }

// RelayPreamble is the relay's first line to the host side for uid.
func RelayPreamble(uid string) string { return "hpcburst-relay/1 " + uid }

// RelayHello is a relay's hello for a.
func RelayHello(a RelayArgs) string {
	return "relay/1 " + string(a.Channel) + " " + strconv.FormatInt(a.Epoch, 10) + " " + strconv.FormatInt(a.Seq, 10)
}

// ParseRelayHello reads a relay's hello line, without its newline.
func ParseRelayHello(line string) (ch RelayChannel, epoch, seq int64, err error) {
	f := strings.Fields(line)
	if len(f) != 4 || f[0] != "relay/1" {
		return "", 0, 0, fmt.Errorf("a relay hello %q: want relay/1 <channel> <E> <N>", line)
	}
	if ch, err = ParseRelayChannel(f[1]); err != nil {
		return "", 0, 0, err
	}
	if epoch, err = strconv.ParseInt(f[2], 10, 64); err != nil || epoch < 0 {
		return "", 0, 0, fmt.Errorf("a relay hello's epoch %q", f[2])
	}
	if seq, err = strconv.ParseInt(f[3], 10, 64); err != nil || seq < 1 {
		return "", 0, 0, fmt.Errorf("a relay hello's sequence %q", f[3])
	}
	return ch, epoch, seq, nil
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
