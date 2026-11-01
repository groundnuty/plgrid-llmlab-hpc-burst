package launcher

import (
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func TestPortsFromBase(t *testing.T) {
	p, err := PortsFromBase(29220)
	if err != nil {
		t.Fatal(err)
	}
	want := Ports{API: 29220, HTTPS: 29221, KCM: 29222, Agent: 29223, LB: 29225, Controller: 29226}
	if p != want {
		t.Fatalf("got %+v, want %+v", p, want)
	}
	if got := (agentapi.Host{PortBase: 29220}).AgentEndpointPort(); p.Agent != got {
		t.Errorf("the reserved agent port is %d, the shared host layout's agent endpoint is %d", p.Agent, got)
	}
	if p.Base() != 29220 {
		t.Errorf("base %d", p.Base())
	}
	if got := p.Expected(); len(got) != 3 || got[0] != 29220 || got[1] != 29221 || got[2] != 29222 {
		t.Errorf("expected ports %v", got)
	}
	// The ports that must be free before a start are base..base+6 less base+4: the agent
	// endpoint (base+3) and the controller's health port (base+6) join the four of the launcher
	// itself. base+4 (the front's /healthz) and base+7 (its ssh dial-home listener) are the
	// front's and not checked.
	if got := p.All(); fmt.Sprint(got) != "[29220 29221 29222 29223 29225 29226]" {
		t.Errorf("all ports %v", got)
	}
	if got := p.ControllerAddr(); got != "127.0.0.1:29226" {
		t.Errorf("controller address %q", got)
	}
	for _, bad := range []int{0, 80, 1023, 65530, 65531, 70000, -1} {
		if _, err := PortsFromBase(bad); err == nil {
			t.Errorf("port base %d accepted", bad)
		}
	}
	// The launcher's own ports end at base+6, so the last base it accepts is 65529.
	if p, err := PortsFromBase(65529); err != nil || p.Controller != 65535 || p.Agent != 65532 {
		t.Errorf("port base 65529: %+v, %v", p, err)
	}
}

// TestAgentEndpointPortIsInTheBlock: a listener on base+3, the agent endpoint's port (reserved for
// the controller's server, never bound by the launcher), makes the check that up runs refuse.
func TestAgentEndpointPortIsInTheBlock(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		skipOrFail(t, fmt.Sprintf("cannot listen on loopback: %v", err))
	}
	defer l.Close()
	held := portOf(t, l)
	ports, err := PortsFromBase(held - 3)
	if err != nil {
		t.Fatal(err)
	}
	// The block's other ports are not reserved here, and on a busy machine any
	// of them can belong to another process, so the check runs on base+3 alone:
	// the block must contain it, and checkPortsFree must report a listener on it.
	if !slices.Contains(ports.All(), held) {
		t.Fatalf("the block at base %d does not contain base+3 (port %d): %v", held-3, held, ports.All())
	}
	err = checkPortsFree([]int{held})
	if err == nil || !strings.Contains(err.Error(), "port "+strconv.Itoa(held)) {
		t.Fatalf("a listener on base+3 (port %d): want an error naming it, got %v", held, err)
	}
}

func portOf(t *testing.T, l net.Listener) int {
	t.Helper()
	return l.Addr().(*net.TCPAddr).Port
}

func TestCheckPortsFree(t *testing.T) {
	// The expected message names the port, whichever address the holder uses.
	hold := func(t *testing.T, network, addr string) int {
		t.Helper()
		l, err := net.Listen(network, addr)
		if err != nil {
			skipOrFail(t, fmt.Sprintf("cannot listen on %s: %v", addr, err))
		}
		t.Cleanup(func() { l.Close() })
		return portOf(t, l)
	}
	for _, c := range []struct{ name, addr string }{
		{"ipv4 loopback", "127.0.0.1:0"},
		{"ipv6 loopback", "[::1]:0"},
		{"ipv4 wildcard", "0.0.0.0:0"},
		{"dual-stack wildcard", ":0"},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := hold(t, "tcp", c.addr)
			err := checkPortsFree([]int{p})
			if err == nil {
				t.Fatalf("port %d is held on %s, yet reported free", p, c.addr)
			}
			if !strings.Contains(err.Error(), "port "+strconv.Itoa(p)) {
				t.Errorf("the error does not name the port: %v", err)
			}
		})
	}
	t.Run("free", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		p := portOf(t, l)
		l.Close()
		if err := checkPortsFree([]int{p}); err != nil {
			t.Errorf("a free port was refused: %v", err)
		}
	})
	t.Run("one taken among free ones", func(t *testing.T) {
		taken := hold(t, "tcp", "127.0.0.1:0")
		l, _ := net.Listen("tcp", "127.0.0.1:0")
		free := portOf(t, l)
		l.Close()
		err := checkPortsFree([]int{free, taken})
		if err == nil || !strings.Contains(err.Error(), "port "+strconv.Itoa(taken)) {
			t.Fatalf("want an error naming port %d, got %v", taken, err)
		}
	})
}

func s(addr string, port int, inode string) sock {
	return sock{Listener{Addr: addr, Port: port}, inode}
}

func TestVerifyListeners(t *testing.T) {
	expected := []int{100, 101, 102}
	cases := []struct {
		name          string
		owned, others []sock
		wantProblems  []string // substrings; none means clean
	}{
		{"exactly the expected, with an extra ::1 for one port",
			[]sock{s("127.0.0.1", 100, "1"), s("127.0.0.1", 101, "2"), s("::1", 101, "3"), s("127.0.0.1", 102, "4")}, nil, nil},
		{"nothing owned proves nothing started", nil, []sock{s("127.0.0.1", 100, "9")}, []string{"no listening socket"}},
		{"expected port answered by another process",
			[]sock{s("127.0.0.1", 100, "1"), s("127.0.0.1", 102, "4")}, []sock{s("127.0.0.1", 101, "9")}, []string{"port 101", "another process"}},
		{"expected port missing",
			[]sock{s("127.0.0.1", 100, "1"), s("127.0.0.1", 101, "2")}, nil, []string{"port 102", "missing"}},
		{"unexpected extra port",
			[]sock{s("127.0.0.1", 100, "1"), s("127.0.0.1", 101, "2"), s("127.0.0.1", 102, "3"), s("127.0.0.1", 6443, "4")}, nil, []string{"unexpected ports [6443]"}},
		{"non-loopback listener",
			[]sock{s("127.0.0.1", 100, "1"), s("0.0.0.0", 101, "2"), s("127.0.0.1", 102, "3")}, nil, []string{"non-loopback"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := strings.Join(verifyListeners(expected, c.owned, c.others), "; ")
			if len(c.wantProblems) == 0 && got != "" {
				t.Fatalf("unexpected problems: %s", got)
			}
			for _, w := range c.wantProblems {
				if !strings.Contains(got, w) {
					t.Errorf("problems %q lack %q", got, w)
				}
			}
		})
	}
}
