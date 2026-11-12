package agent

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

// CheckEnvoySockets checks, after the router has started, what it listens on. It replaces HBEP-5's
// check from /proc that the router listens on loopback only, since the router's listener is a Unix
// socket (the science agent's design, relayed on #429, 12 Nov 2026). It requires:
//   - each of want is a socket, not a symbolic link, of mode 0600, owned by this process's user;
//   - the Unix sockets the process pid listens on are exactly want, matched by the inodes among its
//     descriptors in /proc/net/unix (an inode, never a count of descriptors, which a runner can
//     inherit more of);
//   - the process listens on no TCP port, IPv4 or IPv6 (a LISTEN entry in /proc/net/tcp or tcp6 with
//     one of its inodes), and holds no UDP socket. A connection it opened, such as a health check to
//     the agent's readiness port, is not a listener and is allowed.
func CheckEnvoySockets(pid int, want ...string) error {
	uid := uint32(os.Getuid())
	for _, p := range want {
		fi, err := os.Lstat(p)
		if err != nil {
			return refuse("its socket %s: %v", p, err)
		}
		st, _ := fi.Sys().(*syscall.Stat_t)
		switch {
		case fi.Mode()&os.ModeSocket == 0:
			return refuse("%s is not a socket (mode %v)", p, fi.Mode())
		case fi.Mode().Perm() != 0o600:
			return refuse("%s has mode %#o, want 0600: another user could connect", p, fi.Mode().Perm())
		case st == nil || st.Uid != uid:
			return refuse("%s is not owned by this user", p)
		}
	}
	inodes := socketInodes(pid)
	unix, err := listeningUnixSockets(inodes)
	if err != nil {
		return refuse("reading /proc/net/unix: %v", err)
	}
	wantSet := map[string]bool{}
	for _, p := range want {
		wantSet[p] = true
	}
	var extra, missing []string
	for p := range unix {
		if !wantSet[p] {
			extra = append(extra, p)
		}
	}
	for p := range wantSet {
		if !unix[p] {
			missing = append(missing, p)
		}
	}
	sort.Strings(extra)
	sort.Strings(missing)
	if len(extra) > 0 || len(missing) > 0 {
		return refuse("process %d listens on Unix sockets %v that it should not, and not on %v that it should", pid, extra, missing)
	}
	for _, f := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		addr, err := tcpListener(f, inodes)
		if err != nil {
			return refuse("reading %s: %v", f, err)
		}
		if addr != "" {
			return refuse("process %d listens on TCP %s (%s): the router listens on its Unix socket only", pid, addr, f)
		}
	}
	for _, f := range []string{"/proc/net/udp", "/proc/net/udp6"} {
		addr, err := anySocket(f, inodes)
		if err != nil {
			return refuse("reading %s: %v", f, err)
		}
		if addr != "" {
			return refuse("process %d holds a UDP socket %s (%s)", pid, addr, f)
		}
	}
	return nil
}

// listeningUnixSockets returns the paths of the listening Unix sockets among inodes. In
// /proc/net/unix the fields are Num, RefCount, Protocol, Flags, Type, St, Inode and Path; a listening
// socket's Flags carry __SO_ACCEPTCON (0x10000).
func listeningUnixSockets(inodes map[string]bool) (map[string]bool, error) {
	f, err := os.Open("/proc/net/unix")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := map[string]bool{}
	sc := bufio.NewScanner(f)
	sc.Scan() // the header
	for sc.Scan() {
		fl := strings.Fields(sc.Text())
		if len(fl) < 7 || !inodes[fl[6]] {
			continue
		}
		flags, err := strconv.ParseUint(fl[3], 16, 32)
		if err != nil {
			return nil, fmt.Errorf("flags %q: %v", fl[3], err)
		}
		if flags&0x10000 == 0 {
			continue
		}
		path := "(unnamed)"
		if len(fl) >= 8 {
			path = fl[7]
		}
		out[path] = true
	}
	return out, sc.Err()
}

// tcpListener returns the local address of the first LISTEN entry (state 0A) among inodes in a
// /proc/net/tcp file, or "". The inode is the tenth field.
func tcpListener(path string, inodes map[string]bool) (string, error) {
	return scanNet(path, inodes, func(state string) bool { return state == "0A" })
}

// anySocket returns the local address of the first entry among inodes in a /proc/net file, or "".
func anySocket(path string, inodes map[string]bool) (string, error) {
	return scanNet(path, inodes, func(string) bool { return true })
}

func scanNet(path string, inodes map[string]bool, match func(state string) bool) (string, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return "", nil // no IPv6 on this host
	}
	if err != nil {
		return "", err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Scan() // the header
	for sc.Scan() {
		fl := strings.Fields(sc.Text())
		if len(fl) >= 10 && inodes[fl[9]] && match(fl[3]) {
			return fl[1], nil
		}
	}
	return "", sc.Err()
}
