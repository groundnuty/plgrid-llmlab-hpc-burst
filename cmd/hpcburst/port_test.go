package main

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestUpRefusesWhenAPortIsTaken: for each port the launcher needs, and for each way a foreign
// process can hold it, `up` must refuse before starting anything, name the port, and leave the
// holder alone. The last case is the control: with the ports free, the same command starts k3s,
// so the refusals above were due to the held ports and nothing else.
func TestUpRefusesWhenAPortIsTaken(t *testing.T) {
	const base = 29230 // 29230 apiserver, 29231 https, 29232 controller manager, 29235 reserved
	h := newHost(t)
	roles := map[string]int{"apiserver": base, "https": base + 1, "controller manager": base + 2, "reserved lb": base + 5}
	type holder struct{ name, network, addr string }
	var holders []holder
	for role, p := range roles {
		holders = append(holders, holder{role + " on 127.0.0.1", "tcp", "127.0.0.1:" + strconv.Itoa(p)})
	}
	holders = append(holders,
		holder{"https on ::1", "tcp", "[::1]:" + strconv.Itoa(base+1)},
		holder{"https on the wildcard address", "tcp", ":" + strconv.Itoa(base+1)},
	)
	for _, hd := range holders {
		t.Run(hd.name, func(t *testing.T) {
			l, err := net.Listen(hd.network, hd.addr)
			if err != nil {
				if requireIntegration() {
					t.Fatalf("cannot hold %s, so the case is not tested: %v", hd.addr, err)
				}
				t.Skipf("cannot hold %s: %v", hd.addr, err)
			}
			defer l.Close()
			port := l.Addr().(*net.TCPAddr).Port

			r := cli(t, h.upArgs(base)...)
			if r.code != exitFail {
				t.Fatalf("exit %d, want 1", r.code)
			}
			if !strings.Contains(r.err, "port "+strconv.Itoa(port)) || !strings.Contains(r.err, "refusing to start") {
				t.Errorf("the message does not name port %d: %q", port, r.err)
			}
			if _, err := os.Stat(filepath.Join(h.dir, "state.json")); err == nil {
				t.Error("a record was written: something was started")
			}
			if pids := k3sProcessesUsing(h.dir); len(pids) != 0 {
				t.Errorf("processes were started: %v", pids)
			}
			// Nothing in the whole block listens except the holder, which is this test process.
			for _, row := range listeningInRange(t, 29220, 29239) {
				if !strings.Contains(row, ":"+strconv.Itoa(port)+" ") || !strings.Contains(row, strconv.Itoa(os.Getpid())) {
					t.Errorf("an unexpected listener in the block: %s", row)
				}
			}
			// The holder is untouched: it still accepts connections.
			go func() {
				if c, err := l.Accept(); err == nil {
					c.Close()
				}
			}()
			c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), 2*time.Second)
			if err != nil && hd.network == "tcp" && strings.HasPrefix(hd.addr, "[::1]") {
				c, err = net.DialTimeout("tcp", net.JoinHostPort("::1", strconv.Itoa(port)), 2*time.Second)
			}
			if err != nil {
				t.Errorf("the holder no longer accepts: %v", err)
			} else {
				c.Close()
			}
		})
	}

	t.Run("control: the same command starts k3s once the ports are free", func(t *testing.T) {
		r := cli(t, h.upArgs(base, "--json")...)
		if r.code != exitOK {
			t.Fatalf("exit %d: %s", r.code, r.err)
		}
		st := readState(t, h.dir)
		if !running(st.Processes[0].PID) {
			t.Fatal("k3s is not running")
		}
		if got := listenersOf(t, st.Processes[0].PID); len(got) < 3 {
			t.Errorf("k3s holds %v", got)
		}
	})
}
