package agent

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

// The local ends of the ssh forwards (HBEP-6, Dial-home modes, Local ends):
// a port on the node's IPv4 loopback address that the agent asks the kernel
// for once, kept for the Allocation's life and bound again at each reconnect.

// kernelLoopbackPort asks the kernel for a free port on 127.0.0.1: it binds
// port 0, reads the port it got and releases it for ssh to bind.
func kernelLoopbackPort() (int, error) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("ask the kernel for a loopback port: %w", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		return 0, fmt.Errorf("release loopback port %d: %w", port, err)
	}
	return port, nil
}

// canBindAgain reports whether 127.0.0.1:port can be bound now, by binding it
// and releasing it at once. An error means another process holds the port,
// and the forward must move (HBEP-6, A port that cannot be bound again). A
// port that is free now can still be taken before ssh binds it; ssh then
// exits on ExitOnForwardFailure, and the next attempt finds the port taken.
func canBindAgain(port int) error {
	ln, err := net.Listen("tcp4", "127.0.0.1:"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	return ln.Close()
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
