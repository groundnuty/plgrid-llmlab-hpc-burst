package launcher

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
)

func TestPortsFromBase(t *testing.T) {
	p, err := PortsFromBase(29220)
	if err != nil {
		t.Fatal(err)
	}
	want := Ports{API: 29220, HTTPS: 29221, KCM: 29222, LB: 29225, Controller: 29226, Agent: 29227}
	if p != want {
		t.Fatalf("got %+v, want %+v", p, want)
	}
	if got := p.Expected(); len(got) != 3 || got[0] != 29220 || got[1] != 29221 || got[2] != 29222 {
		t.Errorf("expected ports %v", got)
	}
	// The launcher's block is base..base+7 less base+3 and base+4: the controller's health port
	// and the reserved agent API port join the four that must be free before a start.
	if got := p.All(); fmt.Sprint(got) != "[29220 29221 29222 29225 29226 29227]" {
		t.Errorf("all ports %v", got)
	}
	if got := p.ControllerAddr(); got != "127.0.0.1:29226" {
		t.Errorf("controller address %q", got)
	}
	for _, bad := range []int{0, 80, 1023, 65529, 65531, 70000, -1} {
		if _, err := PortsFromBase(bad); err == nil {
			t.Errorf("port base %d accepted", bad)
		}
	}
	// The block ends at base+7, so the last base that fits is 65528.
	if p, err := PortsFromBase(65528); err != nil || p.Agent != 65535 {
		t.Errorf("port base 65528: %+v, %v", p, err)
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
