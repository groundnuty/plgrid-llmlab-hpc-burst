package agentserver

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The methods of controller-runtime's manager.Runnable and
// manager.LeaderElectionRunnable, which the lifecycle controller's manager
// uses. They are spelled out because importing the manager package would add
// dependencies to go.mod.
var (
	_ interface{ Start(context.Context) error } = (*Server)(nil)
	_ interface{ NeedLeaderElection() bool }    = (*Server)(nil)
)

func TestCheckLoopbackAcceptsOnlyLoopbackIPLiterals(t *testing.T) {
	cases := []struct {
		addr string
		ok   bool
	}{
		{"127.0.0.1:29283", true},
		{"127.5.6.7:29283", true},
		{"[::1]:29283", true},
		{"0.0.0.0:29283", false},
		{"[::]:29283", false},
		{":29283", false},
		{"192.0.2.1:29283", false},
		{"10.0.0.1:29283", false},
		{"localhost:29283", false}, // a name can resolve to anything
		{"example.com:29283", false},
		{"127.0.0.1", false},       // no port
		{"127.0.0.1:0", false},     // no port to serve
		{"127.0.0.1:99999", false}, // not a port
	}
	for _, c := range cases {
		t.Run(c.addr, func(t *testing.T) {
			if err := checkLoopback(c.addr); (err == nil) != c.ok {
				t.Errorf("checkLoopback(%q) = %v, want ok=%v", c.addr, err, c.ok)
			}
		})
	}
}

func TestStartRefusesAnAddressThatIsNotLoopbackAndListensOnNothing(t *testing.T) {
	x := newRig(t)
	x.srv.addr = "0.0.0.0:29285" // a Host cannot produce this; the check must still hold
	err := x.srv.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "not a loopback IP address") {
		t.Fatalf("Start = %v, want a refusal of a non-loopback address", err)
	}
	if c, derr := net.DialTimeout("tcp", "127.0.0.1:29285", 200*time.Millisecond); derr == nil {
		c.Close()
		t.Error("something listens on the refused address")
	}
}

// TestStartServesOnTheHostsLoopbackAddress tries each port base whose agent
// endpoint port is in issue #60's block, because a free port is not known
// before: Start cannot be handed a listener that is already held.
func TestStartServesOnTheHostsLoopbackAddress(t *testing.T) {
	for port := portFirst; port <= portLast; port++ {
		if startOn(t, port) {
			return
		}
	}
	t.Fatalf("every port in %d-%d is in use", portFirst, portLast)
}

// startOn runs Start for a Host whose agent endpoint port is port, checks that
// the endpoint answers over TLS with the pin, and stops it. It returns false,
// without failing, when the port is in use.
func startOn(t *testing.T, port int) (started bool) {
	t.Helper()
	x := newRig(t)
	x.host.PortBase = port - agentapi.AgentEndpointPortOffset
	x.srv.addr = x.host.AgentEndpointAddr() // as New derives it from the Host
	pin := writeEndpointCert(t, x.host)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- x.srv.Start(ctx) }()
	ep := &endpoint{rig: x, base: "https://" + x.srv.addr, addr: x.srv.addr, pin: pin}
	ep.client = ep.clientFor(pin)

	answered := false
	for deadline := time.Now().Add(5 * time.Second); !answered && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		select {
		case err := <-done:
			if errors.Is(err, syscall.EADDRINUSE) {
				return false
			}
			t.Fatalf("Start returned %v", err)
		default:
		}
		resp, err := ep.client.Get(ep.base + "/agent/v1/unknown")
		if err == nil {
			resp.Body.Close()
			answered = resp.StatusCode == http.StatusNotFound
			break
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Start returned %v after its context ended, want nil", err)
	}
	if !answered {
		t.Fatalf("nothing answered on %s", x.srv.addr)
	}
	return true
}

func TestTheServerNeedsNoLeaderElection(t *testing.T) {
	if newRig(t).srv.NeedLeaderElection() {
		t.Error("NeedLeaderElection = true, want false: a replica that cannot serve answers Unavailable")
	}
}

func TestNewRefusesAConfigWithoutWhatItNeeds(t *testing.T) {
	good := newRig(t)
	cases := map[string]func(*Config){
		"no Lookup":          func(c *Config) { c.Lookup = nil },
		"no Backend":         func(c *Config) { c.Backend = nil },
		"no CredentialsDir":  func(c *Config) { c.CredentialsDir = "" },
		"no Host":            func(c *Config) { c.Host = agentapi.Host{} },
		"negative poll":      func(c *Config) { c.PollInterval = -time.Second },
		"negative long poll": func(c *Config) { c.LongPollTimeout = -time.Second },
	}
	for name, mod := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := Config{Lookup: good.lookup, CredentialsDir: good.host.CredentialsDir(), Backend: good.backend, Host: good.host}
			mod(&cfg)
			if s, err := New(cfg); err == nil {
				t.Errorf("New accepted the config: %+v", s)
			}
		})
	}
	if _, err := New(Config{Lookup: good.lookup, CredentialsDir: "d", Backend: good.backend, Host: good.host}); err != nil {
		t.Errorf("New refused a config with the required fields only (nil Logger, zero timings): %v", err)
	}
}
