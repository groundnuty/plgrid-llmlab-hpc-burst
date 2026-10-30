package launcher

// Adapted from spikes/s3/cmd/s3bench/proc.go (this repository, spike S3, PR #36): the /proc
// readers for descendants, VmRSS and listening sockets. The identity checks (start time, boot
// ID, zombies) are new: S3's harness was the long-lived parent of what it started, and the
// launcher is not, so it has to recognise its processes across invocations.

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
	// Gone: no such process, or a zombie (dead, only waiting to be reaped).
	Gone Verdict = iota
	// Ours: a live process with the recorded PID, start time and boot ID.
	Ours
	// Foreign: a live process has the recorded PID, but it is not the recorded one (the PID was
	// reused, or the machine rebooted since). It must never be signalled.
	Foreign
)

func (v Verdict) String() string {
	switch v {
	case Gone:
		return "gone"
	case Ours:
		return "ours"
	case Foreign:
		return "foreign"
	}
	return "unknown"
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
// means nothing, so the ID is part of every record.
func currentBootID() string {
	b, err := os.ReadFile(procRoot + "/sys/kernel/random/boot_id")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// newRec records the process with the given PID as it is now.
func newRec(name string, pid int) (ProcRec, error) {
	st, err := readStat(pid)
	if err != nil {
		return ProcRec{}, fmt.Errorf("read the identity of pid %d: %w", pid, err)
	}
	if isDead(pid, st) {
		return ProcRec{}, fmt.Errorf("pid %d has already exited", pid)
	}
	return ProcRec{Name: name, PID: pid, StartTicks: st.StartTicks, BootID: currentBootID()}, nil
}

func zombieState(b byte) bool { return b == 'Z' || b == 'X' || b == 'x' }

// isDead reports whether the process is dead: all of its threads are. The state letter of
// /proc/<pid>/stat is the state of the thread-group leader only, and it reads Z whenever the
// leader is gone while the process lives on. That is not rare. When a multi-threaded program
// calls execve from a thread other than the leader (k3s re-executes itself, after unpacking its
// embedded binaries), the kernel makes the old leader a zombie until the calling thread has taken
// over its PID. A launcher that took "Z" for "dead" would stop watching a live k3s and, on a
// failed start, would leave it running. So Z counts as dead only when no other thread of the
// process is alive.
func isDead(pid int, st procStat) bool {
	return zombieState(st.State) && !liveThreads(pid)
}

// liveThreads reports whether any thread of the process is in a state other than Z and X.
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

// identify decides whether rec is still alive and still the same process. It never signals.
// A process whose identity cannot be read for a reason other than "no such process" is treated
// as Foreign, the safe answer: Foreign is never signalled.
func identify(rec ProcRec) (Verdict, string) {
	if rec.PID <= 1 {
		return Foreign, fmt.Sprintf("recorded pid %d is not a process the launcher could have started", rec.PID)
	}
	st, err := readStat(rec.PID)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return Gone, fmt.Sprintf("pid %d does not exist", rec.PID)
	case err != nil:
		return Foreign, fmt.Sprintf("pid %d: cannot read its identity: %v", rec.PID, err)
	case isDead(rec.PID, st):
		return Gone, fmt.Sprintf("pid %d has exited (state %c)", rec.PID, st.State)
	}
	if rec.BootID != "" && rec.BootID != currentBootID() {
		return Foreign, fmt.Sprintf("pid %d: recorded before a reboot, so the process now using it is not the recorded one", rec.PID)
	}
	if st.StartTicks != rec.StartTicks {
		return Foreign, fmt.Sprintf("pid %d now belongs to another process (start time %d, recorded %d)", rec.PID, st.StartTicks, rec.StartTicks)
	}
	return Ours, ""
}

// descendants returns the given PIDs plus every process below them.
func descendants(roots []int) []int {
	ents, _ := os.ReadDir(procRoot)
	parent := map[int]int{}
	for _, e := range ents {
		if pid, err := strconv.Atoi(e.Name()); err == nil {
			if st, err := readStat(pid); err == nil {
				parent[pid] = st.PPID
			}
		}
	}
	set := map[int]bool{}
	for _, r := range roots {
		set[r] = true
	}
	for changed := true; changed; {
		changed = false
		for pid, pp := range parent {
			if !set[pid] && set[pp] {
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
}

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
		if p[3] != "0A" {
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
		out = append(out, sock{Listener{Addr: addr, Port: int(port)}, p[9]})
	}
	return out, nil
}

// listenSockets returns every TCP LISTEN socket of the network namespace. A table that cannot
// be read is an error: "no listener found" must never come from "could not look".
func listenSockets() ([]sock, error) {
	var out []sock
	for _, f := range []string{"net/tcp", "net/tcp6"} {
		b, err := os.ReadFile(procRoot + "/" + f)
		if err != nil {
			if f == "net/tcp6" && errors.Is(err, os.ErrNotExist) {
				continue // IPv6 not compiled in
			}
			return nil, err
		}
		rows, err := parseListenTable(string(b))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		out = append(out, rows...)
	}
	return out, nil
}

// socketInodes returns the inodes of the sockets the processes (and their descendants) hold.
func socketInodes(roots []int) map[string]bool {
	inodes := map[string]bool{}
	for _, pid := range descendants(roots) {
		fds, _ := os.ReadDir(procPath(pid, "fd"))
		for _, fd := range fds {
			l, err := os.Readlink(procPath(pid, "fd/"+fd.Name()))
			if err == nil && strings.HasPrefix(l, "socket:[") {
				inodes[strings.TrimSuffix(strings.TrimPrefix(l, "socket:["), "]")] = true
			}
		}
	}
	return inodes
}

// ownedListeners returns the LISTEN sockets held by the processes and their descendants, and
// every other LISTEN socket on the machine.
func ownedListeners(roots []int) (owned, others []sock, err error) {
	all, err := listenSockets()
	if err != nil {
		return nil, nil, err
	}
	inodes := socketInodes(roots)
	for _, s := range all {
		if inodes[s.inode] {
			owned = append(owned, s)
		} else {
			others = append(others, s)
		}
	}
	return owned, others, nil
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
