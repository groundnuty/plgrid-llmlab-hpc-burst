package launcher

// checkOwned is adapted from spikes/s3/cmd/s3bench/flow.go (this repository, PR #36). The
// free-port check also reads the kernel's socket tables, because binding 127.0.0.1 alone
// misses a holder on [::1] or on a wildcard address.

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"syscall"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Ports are the loopback ports of one launcher. All of them derive from one base, so an
// operator reserves a block and nothing falls back to k3s's defaults (6443, 6444, 10250),
// which a system k3s on the same machine may hold.
type Ports struct {
	API   int `json:"api"`   // k3s's hidden --apiserver-port: the internal apiserver listener
	HTTPS int `json:"https"` // --https-listen-port: the supervisor and the external-facing apiserver
	KCM   int `json:"kcm"`   // the embedded controller manager's secure port
	LB    int `json:"lb"`    // --lb-server-port: reserved and never bound (no agent), so it cannot hit the default
	// Controller is the lifecycle controller's /healthz (HBEP-11). Agent is the agent endpoint,
	// agentapi.Host.AgentEndpointPort (issue #60's server): reserved, never bound by the launcher.
	// A state file from before the controllers existed has neither.
	Controller int `json:"controller,omitempty"`
	Agent      int `json:"agent,omitempty"`
}

// Base is the start of the block, the port the controllers' --port-base names. PortsFromBase puts
// the apiserver there.
func (p Ports) Base() int { return p.API }

// DefaultPortBase is the default start of the block. It is far from every k3s default.
const DefaultPortBase = 29220

// controllerPortOffset is the lifecycle controller's /healthz, relative to the base. It is the last
// port of the block.
const controllerPortOffset = 6

// PortsFromBase lays the first four ports out as spike S3 did (base, base+1, base+2 and base+5)
// and adds the controllers' two: the agent endpoint at agentapi.AgentEndpointPortOffset (base+3,
// shared with the controller through agentapi.Host) and the controller's /healthz at base+6.
// The shared block (agentapi.Host) also holds base+4, the front's /healthz, and base+7, the
// front's ssh dial-home listener (agentapi.DialHomePortOffset): both are issue #64's, and this
// launcher neither checks nor binds them.
func PortsFromBase(base int) (Ports, error) {
	if base < 1024 || base+controllerPortOffset > 65535 {
		return Ports{}, fmt.Errorf("port base %d is out of range: the block base..base+%d must lie in 1024..65535", base, controllerPortOffset)
	}
	host := agentapi.Host{PortBase: base}
	return Ports{API: base, HTTPS: base + 1, KCM: base + 2, LB: base + 5, Controller: base + controllerPortOffset, Agent: host.AgentEndpointPort()}, nil
}

// Expected are the ports the running k3s must hold, and no others. The controllers' ports are
// not among them: status checks those per supervised process.
func (p Ports) Expected() []int { return []int{p.API, p.HTTPS, p.KCM} }

// All are the ports that must be free before a start: the expected ones and the reserved ones.
// The agent endpoint's is among them, so an operator's listener on it makes up refuse.
func (p Ports) All() []int {
	return []int{p.API, p.HTTPS, p.KCM, p.Agent, p.LB, p.Controller}
}

// ControllerAddr is where the lifecycle controller serves /healthz.
func (p Ports) ControllerAddr() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(p.Controller))
}

// checkPortsFree fails, naming the port, when any of the ports is already taken: listening on
// any address in the kernel's tables, or not bindable on 127.0.0.1.
func checkPortsFree(ports []int) error {
	socks, err := listenSockets()
	if err != nil {
		return fmt.Errorf("cannot read the listening sockets, so cannot tell whether the ports are free: %w", err)
	}
	for _, p := range ports {
		for _, s := range socks {
			if s.Port == p {
				return fmt.Errorf("port %d is already taken: something listens on %s", p, s.Listener)
			}
		}
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
		if err != nil {
			if errors.Is(err, syscall.EADDRINUSE) {
				return fmt.Errorf("port %d is already taken: 127.0.0.1:%d is in use", p, p)
			}
			return fmt.Errorf("port %d cannot be bound on 127.0.0.1: %w", p, err)
		}
		l.Close()
	}
	return nil
}

// verifyListeners compares what the launched processes hold with what they must hold. owned are
// the LISTEN sockets of the launched process tree, others every other LISTEN socket on the
// machine. It returns one sentence per problem; none means the host holds exactly its expected
// ports, all on loopback. An empty owned set proves nothing started, so it is a problem too.
func verifyListeners(expected []int, owned, others []sock) []string {
	if len(owned) == 0 {
		return []string{"the launched processes hold no listening socket"}
	}
	have := map[int]bool{}
	var nonLoop []Listener
	for _, s := range owned {
		have[s.Port] = true
		if !s.Loopback() {
			nonLoop = append(nonLoop, s.Listener)
		}
	}
	foreign := map[int]bool{}
	for _, s := range others {
		foreign[s.Port] = true
	}
	want := map[int]bool{}
	var probs []string
	for _, p := range expected {
		want[p] = true
		switch {
		case have[p]:
		case foreign[p]:
			probs = append(probs, fmt.Sprintf("port %d is listening, but another process holds it, not the launched one", p))
		default:
			probs = append(probs, fmt.Sprintf("expected listener on port %d is missing", p))
		}
	}
	var extra []int
	for p := range have {
		if !want[p] {
			extra = append(extra, p)
		}
	}
	sort.Ints(extra)
	if len(extra) > 0 {
		probs = append(probs, fmt.Sprintf("the launched processes also listen on unexpected ports %v", extra))
	}
	if len(nonLoop) > 0 {
		probs = append(probs, fmt.Sprintf("listeners on non-loopback addresses: %v", nonLoop))
	}
	return probs
}
