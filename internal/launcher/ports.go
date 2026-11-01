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
	// FrontHealth is the front's loopback /healthz (issue #64, HBEP-5). DialHome is the front's
	// dial-home listener in ssh mode, 127.0.0.1:base+7, where each platform's restricted key
	// forwards (@hpc-burst-science-agent, 31 Oct 2026): reserved, never bound by the launcher. The
	// front's client listener, and its dial-home listener in direct mode, are set in its own
	// configuration file, not taken from this block.
	FrontHealth int `json:"front_health,omitempty"`
	DialHome    int `json:"dial_home,omitempty"`
}

// Base is the start of the block, the port the controllers' --port-base names. PortsFromBase puts
// the apiserver there.
func (p Ports) Base() int { return p.API }

// DefaultPortBase is the default start of the block. It is far from every k3s default.
const DefaultPortBase = 29220

// controllerPortOffset is the lifecycle controller's /healthz, relative to the base.
const controllerPortOffset = 6

// frontHealthPortOffset is the front's /healthz, relative to the base (issue #64; the parent's
// allocation of the block's free port, 31 Oct 2026).
const frontHealthPortOffset = 4

// dialHomePortOffset is the front's ssh-mode dial-home listener, relative to the base:
// agentapi.DialHomePortOffset (7). Each platform's restricted VM key permits forwards to exactly
// 127.0.0.1:base+3 (the agent endpoint) and 127.0.0.1:base+7, so a launcher started with another
// base needs new restricted keys (docs/development.md, The launcher, Ports).
//
// TODO(#64): warn at up when the base differs from the one the configured restricted keys were
// made for. Nothing records that base yet: the step that makes the keys does not exist, so there
// is nothing to compare with.
const dialHomePortOffset = agentapi.DialHomePortOffset

// blockLastOffset is the block's last port relative to the base: the lifecycle controller's
// /metrics, agentapi.ControllerMetricsPortOffset (12, issue #111). The block is the 13 ports
// base..base+12.
const blockLastOffset = agentapi.ControllerMetricsPortOffset

// PortsFromBase lays the first four ports out as spike S3 did (base, base+1, base+2 and base+5)
// and adds the controllers' two: the agent endpoint at agentapi.AgentEndpointPortOffset (base+3,
// shared with the controller through agentapi.Host) and the controller's /healthz at base+6.
// base+4 is the front's /healthz (frontHealthPortOffset), and base+7 the front's ssh-mode
// dial-home listener (dialHomePortOffset). The rest of the shared block belongs to the other
// components: base+8, the frps server plugin the controller binds, and base+9 to base+11, frps's
// own (HBEP-6); and base+12, the controller's /metrics (issue #111). The whole block,
// base..base+12, must fit below 65536.
func PortsFromBase(base int) (Ports, error) {
	if base < 1024 || base+blockLastOffset > 65535 {
		return Ports{}, fmt.Errorf("port base %d is out of range: the block base..base+%d must lie in 1024..65535", base, blockLastOffset)
	}
	host := agentapi.Host{PortBase: base}
	return Ports{API: base, HTTPS: base + 1, KCM: base + 2, LB: base + 5, Controller: base + controllerPortOffset, Agent: host.AgentEndpointPort(),
		FrontHealth: base + frontHealthPortOffset, DialHome: host.DialHomePort()}, nil
}

// Expected are the ports the running k3s must hold, and no others. The controllers' ports are
// not among them: status checks those per supervised process.
func (p Ports) Expected() []int { return []int{p.API, p.HTTPS, p.KCM} }

// All are the ports that must be free before a start: the whole block, base..base+12. k3s binds
// three of them and the controller, the front and frps the others, so an operator's listener on
// any of them makes up refuse. down warns about a foreign listener left on any of them.
func (p Ports) All() []int {
	all := make([]int, 0, blockLastOffset+1)
	for port := p.Base(); port <= p.Base()+blockLastOffset; port++ {
		all = append(all, port)
	}
	return all
}

// FrontHealthAddr is where the front serves /healthz.
//
// base+4 and base+7 are checked free before k3s starts but bound by the front only later, after
// its first lists: the same window as base+3 and base+6, which the controller binds. The front's
// client listener, and its dial-home listener in direct mode, come from its own configuration and
// are not checked against the block. up waits for the front's /healthz (supervised.WaitReady), so a
// port taken in that window fails up instead of leaving a front that cannot start.
func (p Ports) FrontHealthAddr() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(p.FrontHealth))
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

// verifyExactListeners is verifyListeners for a process whose every reachable socket is known
// (frps, HBEP-6): owned must be exactly want, matched by protocol, address and port, and nothing
// else, TCP or UDP, on any address. others are the other processes' reachable sockets; one on a
// port of want, at any address, is a problem too. It returns one sentence per problem.
func verifyExactListeners(want, owned, others []sock) []string {
	have, wanted, ports := map[string]bool{}, map[string]bool{}, map[int]bool{}
	for _, s := range owned {
		have[s.key()] = true
	}
	var probs []string
	for _, w := range want {
		wanted[w.key()], ports[w.Port] = true, true
		if !have[w.key()] {
			probs = append(probs, fmt.Sprintf("expected listener %s is missing", w.key()))
		}
	}
	var extra, foreign []string
	for _, s := range owned {
		if !wanted[s.key()] {
			extra = append(extra, s.key())
		}
	}
	for _, s := range others {
		if ports[s.Port] {
			foreign = append(foreign, s.key())
		}
	}
	sort.Strings(extra)
	sort.Strings(foreign)
	if len(extra) > 0 {
		probs = append(probs, fmt.Sprintf("it also listens on %v, which it must not", extra))
	}
	if len(foreign) > 0 {
		probs = append(probs, fmt.Sprintf("other processes listen on its ports: %v", foreign))
	}
	return probs
}
