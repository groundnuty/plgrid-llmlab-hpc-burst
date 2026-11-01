package launcher

// Adapted from spikes/s3/cmd/s3bench/proc.go (this repository, spike S3, PR #36): the /proc
// readers for descendants, VmRSS and listening sockets. The identity records (start time, boot
// ID) are new: S3's harness was the long-lived parent of what it started, and the launcher is
// not, so it has to recognise its processes across invocations. The decisions built on these
// readers (alive or not, ours or not, which sockets are ours) are in handle.go.

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
)

// procRoot is the proc filesystem. Tests point it at a fake tree.
var procRoot = "/proc"

func procPath(pid int, file string) string { return fmt.Sprintf("%s/%d/%s", procRoot, pid, file) }

// ProcRec identifies one process the launcher started. PID alone is not an identity, because
// the kernel reuses PIDs; the start time (field 22 of /proc/<pid>/stat, in clock ticks since
// boot) and the boot ID together are.
type ProcRec struct {
	Name       string `json:"name"`
	PID        int    `json:"pid"`
	StartTicks uint64 `json:"start_ticks"`
	BootID     string `json:"boot_id"`
}

// Verdict is what identify finds out about a recorded process.
type Verdict int

const (
	// Gone: the process has exited. Only the kernel's process handle (a pidfd) or the absence of
	// any process with the PID says so, never a /proc read that failed or came back empty.
	Gone Verdict = iota
	// Ours: a live process with the recorded PID, start time and boot ID.
	Ours
	// Foreign: a live process has the recorded PID, but it is not the recorded one (another start
	// time, or the record is from an earlier boot). It must never be signalled.
	Foreign
	// Unknown: the identity could not be established (no pidfd, an unreadable boot ID or /proc
	// entry). It is never signalled, and its record is kept.
	Unknown
)

func (v Verdict) String() string {
	switch v {
	case Gone:
		return "gone"
	case Ours:
		return "ours"
	case Foreign:
		return "foreign"
	case Unknown:
		return "unknown"
	}
	return "invalid"
}

// procStat holds the fields of /proc/<pid>/stat the launcher uses.
type procStat struct {
	State      byte
	PPID       int
	StartTicks uint64
}

// parseStat parses the content of /proc/<pid>/stat. The command name (field 2) sits in
// parentheses and may itself contain spaces and parentheses, so the fields after it are
// counted from the LAST ")".
func parseStat(s string) (procStat, error) {
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return procStat{}, errors.New("no closing parenthesis")
	}
	f := strings.Fields(s[i+1:])
	// f[0] is field 3 (state); field 22 (starttime) is f[19].
	if len(f) < 20 || len(f[0]) != 1 {
		return procStat{}, fmt.Errorf("only %d fields after the command name", len(f))
	}
	ppid, err := strconv.Atoi(f[1])
	if err != nil {
		return procStat{}, fmt.Errorf("ppid: %w", err)
	}
	st, err := strconv.ParseUint(f[19], 10, 64)
	if err != nil {
		return procStat{}, fmt.Errorf("starttime: %w", err)
	}
	return procStat{State: f[0][0], PPID: ppid, StartTicks: st}, nil
}

func readStat(pid int) (procStat, error) {
	b, err := os.ReadFile(procPath(pid, "stat"))
	if err != nil {
		return procStat{}, err
	}
	return parseStat(string(b))
}

// currentBootID is the kernel's random ID of this boot. After a reboot a recorded start time
// means nothing, so the ID is part of every record, and a boot ID that cannot be read is an
// error: without it no identity can be confirmed.
func currentBootID() (string, error) {
	b, err := os.ReadFile(procRoot + "/sys/kernel/random/boot_id")
	if err != nil {
		return "", fmt.Errorf("cannot read the boot ID: %w", err)
	}
	id := strings.TrimSpace(string(b))
	if id == "" {
		return "", errors.New("the boot ID is empty")
	}
	return id, nil
}

func zombieState(b byte) bool { return b == 'Z' || b == 'X' || b == 'x' }

// liveThreads reports whether a /proc scan finds a thread of the process in a state other than Z
// and X. It is only a cross-check, never evidence of death: while a multi-threaded program calls
// execve from a thread other than the leader (k3s re-executes itself this way), the leader
// reads Z and the task directory can briefly list no thread at all, though the process lives.
func liveThreads(pid int) bool {
	tasks, err := os.ReadDir(procPath(pid, "task"))
	if err != nil {
		return false
	}
	for _, t := range tasks {
		b, err := os.ReadFile(procPath(pid, "task/"+t.Name()+"/stat"))
		if err != nil {
			continue // the thread ended while we looked
		}
		if ts, err := parseStat(string(b)); err == nil && !zombieState(ts.State) {
			return true
		}
	}
	return false
}

// procTable reads the stat of every process once: a snapshot of parent links and start times.
func procTable() map[int]procStat {
	ents, _ := os.ReadDir(procRoot)
	out := map[int]procStat{}
	for _, e := range ents {
		if pid, err := strconv.Atoi(e.Name()); err == nil {
			if st, err := readStat(pid); err == nil {
				out[pid] = st
			}
		}
	}
	return out
}

// descendants returns the given PIDs plus every process below them. It is a snapshot, used only
// for measurements (RSS); decisions about processes go through verified handles (handle.go).
func descendants(roots []int) []int {
	table := procTable()
	set := map[int]bool{}
	for _, r := range roots {
		set[r] = true
	}
	for changed := true; changed; {
		changed = false
		for pid, st := range table {
			if !set[pid] && set[st.PPID] {
				set[pid] = true
				changed = true
			}
		}
	}
	out := make([]int, 0, len(set))
	for pid := range set {
		out = append(out, pid)
	}
	sort.Ints(out)
	return out
}

// exePath reads the /proc/<pid>/exe link: the executable the process runs now.
func exePath(pid int) (string, error) { return os.Readlink(procPath(pid, "exe")) }

// fdSocketInodes returns the inodes of the sockets one process holds.
func fdSocketInodes(pid int) map[string]bool {
	inodes := map[string]bool{}
	fds, _ := os.ReadDir(procPath(pid, "fd"))
	for _, fd := range fds {
		l, err := os.Readlink(procPath(pid, "fd/"+fd.Name()))
		if err == nil && strings.HasPrefix(l, "socket:[") {
			inodes[strings.TrimSuffix(strings.TrimPrefix(l, "socket:["), "]")] = true
		}
	}
	return inodes
}

// errNoRSS means /proc/<pid>/status was readable but has no VmRSS line (a zombie or a kernel thread).
var errNoRSS = errors.New("no VmRSS line")

// vmRSSKB reads VmRSS of one process. It returns an error, never a silent zero, when the
// process is gone, its status file cannot be read, or the file has no VmRSS.
func vmRSSKB(pid int) (int, error) {
	b, err := os.ReadFile(procPath(pid, "status"))
	if err != nil {
		return 0, err
	}
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "VmRSS:") {
			f := strings.Fields(l)
			if len(f) < 2 {
				return 0, fmt.Errorf("pid %d: malformed VmRSS line %q", pid, l)
			}
			v, err := strconv.Atoi(f[1])
			if err != nil {
				return 0, fmt.Errorf("pid %d: malformed VmRSS line %q: %w", pid, l, err)
			}
			return v, nil
		}
	}
	return 0, errNoRSS
}

// rssMiB sums VmRSS over the roots and their descendants. A root that cannot be read makes the
// whole reading an error: a sum that silently leaves a required process out is not a
// measurement. A descendant that vanished between listing and reading is skipped.
func rssMiB(roots []int) (float64, error) {
	if len(roots) == 0 {
		return 0, errors.New("no processes to measure")
	}
	isRoot := map[int]bool{}
	for _, r := range roots {
		isRoot[r] = true
	}
	var kb int
	for _, pid := range descendants(roots) {
		v, err := vmRSSKB(pid)
		switch {
		case err == nil:
			kb += v
		case !isRoot[pid] && (errors.Is(err, os.ErrNotExist) || errors.Is(err, errNoRSS)):
		default:
			return 0, fmt.Errorf("cannot measure pid %d: %w", pid, err)
		}
	}
	return float64(kb) / 1024, nil
}

// Listener is one TCP LISTEN socket.
type Listener struct {
	Addr string `json:"addr"`
	Port int    `json:"port"`
}

func (l Listener) String() string {
	if strings.Contains(l.Addr, ":") {
		return fmt.Sprintf("[%s]:%d", l.Addr, l.Port)
	}
	return fmt.Sprintf("%s:%d", l.Addr, l.Port)
}

// Loopback reports whether the listener is bound to a loopback address only.
func (l Listener) Loopback() bool {
	ip := net.ParseIP(l.Addr)
	return ip != nil && ip.IsLoopback()
}

type sock struct {
	Listener
	inode string
	proto string // "tcp" or "udp"
}

// key names a socket by protocol, address and port: "tcp 127.0.0.1:29229".
func (s sock) key() string { return s.proto + " " + s.Listener.String() }

// parseHexAddr decodes an address from /proc/net/tcp or tcp6: 4 or 16 bytes in groups of one
// 32-bit word each, written in host (little-endian) byte order.
func parseHexAddr(h string) (string, error) {
	if len(h) != 8 && len(h) != 32 {
		return "", fmt.Errorf("address %q has an unexpected length", h)
	}
	ip := make(net.IP, len(h)/2)
	for w := 0; w < len(h)/8; w++ {
		for b := 0; b < 4; b++ {
			v, err := strconv.ParseUint(h[w*8+b*2:w*8+b*2+2], 16, 8)
			if err != nil {
				return "", fmt.Errorf("address %q: %w", h, err)
			}
			ip[w*4+3-b] = byte(v)
		}
	}
	return ip.String(), nil
}

// parseListenTable returns the LISTEN rows (state 0A) of a /proc/net/tcp-style table.
func parseListenTable(content string) ([]sock, error) {
	return parseSockTable(content, "tcp", func(state, _ string) bool { return state == "0A" })
}

// parseUnconnectedTable returns the rows of a /proc/net/udp-style table that have no remote
// address: sockets that take datagrams from anyone, which is how a UDP server (KCP, QUIC) listens.
// A connected socket (a client's, such as a DNS query's) only talks to its one peer.
func parseUnconnectedTable(content string) ([]sock, error) {
	return parseSockTable(content, "udp", func(_, remote string) bool { return strings.Trim(remote, "0:") == "" })
}

// parseSockTable returns the rows of a /proc/net table (tcp, tcp6, udp, udp6: the same columns)
// that keep accepts, given the row's state and remote address as the kernel writes them.
func parseSockTable(content, proto string, keep func(state, remote string) bool) ([]sock, error) {
	lines := strings.Split(content, "\n")
	var out []sock
	for i, l := range lines {
		if i == 0 {
			continue // header
		}
		p := strings.Fields(l)
		if len(p) == 0 {
			continue
		}
		if len(p) < 10 {
			return nil, fmt.Errorf("line %d: %d fields, want at least 10", i+1, len(p))
		}
		if !keep(p[3], p[2]) {
			continue
		}
		hp := strings.Split(p[1], ":")
		if len(hp) != 2 {
			return nil, fmt.Errorf("line %d: bad local address %q", i+1, p[1])
		}
		port, err := strconv.ParseUint(hp[1], 16, 16)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		addr, err := parseHexAddr(hp[0])
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		out = append(out, sock{Listener: Listener{Addr: addr, Port: int(port)}, inode: p[9], proto: proto})
	}
	return out, nil
}

// listenSockets returns every TCP LISTEN socket of the network namespace. A table that cannot
// be read is an error: "no listener found" must never come from "could not look".
func listenSockets() ([]sock, error) {
	return readSockTables(parseListenTable, "net/tcp", "net/tcp6")
}

// boundSockets returns every socket of the network namespace that can be reached from outside it:
// TCP LISTEN sockets and UDP sockets without a remote address, IPv4 and IPv6.
func boundSockets() ([]sock, error) {
	tcp, err := listenSockets()
	if err != nil {
		return nil, err
	}
	udp, err := readSockTables(parseUnconnectedTable, "net/udp", "net/udp6")
	if err != nil {
		return nil, err
	}
	return append(tcp, udp...), nil
}

// readSockTables parses each of the /proc/net tables; the IPv6 one (the second) may be missing
// when IPv6 is not compiled in.
func readSockTables(parse func(string) ([]sock, error), v4, v6 string) ([]sock, error) {
	var out []sock
	for _, f := range []string{v4, v6} {
		b, err := os.ReadFile(procRoot + "/" + f)
		if err != nil {
			if f == v6 && errors.Is(err, os.ErrNotExist) {
				continue // IPv6 not compiled in
			}
			return nil, err
		}
		rows, err := parse(string(b))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		out = append(out, rows...)
	}
	return out, nil
}

func sortedPorts(ss []sock) []Listener {
	out := make([]Listener, 0, len(ss))
	for _, s := range ss {
		out = append(out, s.Listener)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Port != out[j].Port {
			return out[i].Port < out[j].Port
		}
		return out[i].Addr < out[j].Addr
	})
	return out
}
