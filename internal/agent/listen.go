package agent

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

// The local ends of the ssh forwards (HBEP-6, Dial-home modes, Local ends):
// a port on the node's IPv4 loopback address that the agent asks the kernel
// for once, kept for the Allocation's life and bound again at each reconnect.

// forwardPorts are the loopback ports this agent has handed to its
// forwards, each with the forward it belongs to. The kernel can offer a port
// again as soon as the probe that got it is closed, before the forward it
// went to has bound it with ssh: two forwards that asked at the same moment
// once got the same port, and one had to move at once (the timing sweep, 1
// Nov 2026: tunnel and control both on 42079, 1 ms apart). Ports are handed
// out under one lock, and a port the kernel offers that a forward holds is
// kept bound while the kernel is asked again, so it cannot be offered twice.
// A forward keeps its port until it has a replacement (PR #144 review): if it
// gave the port up first and then got no new one, the kernel could hand the
// port to another forward while State still named it as this one's.
var forwardPorts = loopbackPorts{owner: map[int]*SSHForward{}}

// probeLoopback binds 127.0.0.1:port; port 0 asks the kernel for a free one.
// A test replaces it to make the kernel offer the same port twice.
var probeLoopback = func(port int) (net.Listener, error) {
	return net.Listen("tcp4", "127.0.0.1:"+strconv.Itoa(port))
}

// maxPortProbes bounds take's requests to the kernel.
const maxPortProbes = 64

type loopbackPorts struct {
	mu    sync.Mutex
	owner map[int]*SSHForward // each port handed out, and the forward that holds it
}

// take asks the kernel for a free port on 127.0.0.1 that no forward of this
// agent holds, and gives it to f: it binds port 0, reads the port it got and
// releases it for ssh to bind.
func (l *loopbackPorts) take(f *SSHForward) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var held []net.Listener // ports forwards hold, bound until take returns
	defer func() {
		for _, ln := range held {
			_ = ln.Close()
		}
	}()
	for range maxPortProbes {
		ln, err := probeLoopback(0)
		if err != nil {
			return 0, fmt.Errorf("ask the kernel for a loopback port: %w", err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		if l.owner[port] != nil {
			held = append(held, ln)
			continue
		}
		if err := ln.Close(); err != nil {
			return 0, fmt.Errorf("release loopback port %d: %w", port, err)
		}
		l.owner[port] = f
		return port, nil
	}
	return 0, fmt.Errorf("the kernel offered %d loopback ports, all held by this agent's forwards", maxPortProbes)
}

// canBindAgain reports whether f's port can be bound now, by binding it and
// releasing it at once. An error means another process holds the port, and
// the forward must move (HBEP-6, A port that cannot be bound again); f keeps
// the port until it has taken a replacement (giveUp). A port that is free
// now can still be taken before ssh binds it; ssh then exits on
// ExitOnForwardFailure, and the next attempt finds the port taken.
func (l *loopbackPorts) canBindAgain(f *SSHForward, port int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if o := l.owner[port]; o != nil && o != f {
		return fmt.Errorf("loopback port %d is held by this agent's forward %q", port, o.Name)
	}
	ln, err := probeLoopback(port)
	if err != nil {
		return err
	}
	if err := ln.Close(); err != nil {
		return err
	}
	l.owner[port] = f
	return nil
}

// giveUp releases f's port: when f has moved to a replacement, whatever made
// it move, and when f ends. A port that another forward holds is left alone.
func (l *loopbackPorts) giveUp(f *SSHForward, port int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.owner[port] == f {
		delete(l.owner, port)
	}
}

// listensOn reports whether the process pid holds a socket listening on
// 127.0.0.1:port: a LISTEN entry for that address in /proc/net/tcp whose
// socket inode is one of the process's descriptors. A connect would not do:
// it succeeds against any process that took the port, and through ssh it
// opens a channel to the far end.
func listensOn(pid, port int) bool {
	inodes := socketInodes(pid)
	if len(inodes) == 0 {
		return false
	}
	f, err := os.Open("/proc/net/tcp")
	if err != nil {
		return false
	}
	defer f.Close()
	// /proc/net/tcp writes an IPv4 address as the hex of its in-memory bytes,
	// little-endian on amd64 and arm64: 127.0.0.1 is 0100007F. State 0A is
	// TCP_LISTEN; the inode is the tenth field.
	want := fmt.Sprintf("0100007F:%04X", port)
	sc := bufio.NewScanner(f)
	sc.Scan() // the header
	for sc.Scan() {
		fl := strings.Fields(sc.Text())
		if len(fl) < 10 || fl[1] != want || fl[3] != "0A" {
			continue
		}
		if inodes[fl[9]] {
			return true
		}
	}
	return false
}

// socketInodes returns the inodes of the sockets among pid's descriptors.
func socketInodes(pid int) map[string]bool {
	dir := "/proc/" + strconv.Itoa(pid) + "/fd"
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	out := map[string]bool{}
	for _, e := range entries {
		target, err := os.Readlink(dir + "/" + e.Name())
		if err != nil {
			continue
		}
		if ino, ok := strings.CutPrefix(target, "socket:["); ok {
			out[strings.TrimSuffix(ino, "]")] = true
		}
	}
	return out
}
