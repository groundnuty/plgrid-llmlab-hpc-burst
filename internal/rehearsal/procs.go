package rehearsal

// New, after internal/slurm/fakeslurm/procfs.go: the processes below a given
// one, read from /proc. sshd's session child calls setsid, so it leaves sshd's
// process group; the nodes find what to stop by parentage and verify each
// process by its start time, so a recycled PID is left alone. What the nodes
// signal, and how, is in signal.go.
//
// Issue #308. An sshd that had exited before its row was read left a row of
// PID 0, the walk from PID 0 returned every process on the host (PID 1 and the
// kernel's threads have parent 0), and the stop sent SIGTERM to all of them:
// the likely cause of the development VM's outage of 10 Nov 2026. Now:
//  1. spawn (pool.go) stores no sshd whose row it cannot read, or that has
//     no start time: it kills that one through its os.Process, which holds a
//     pidfd, and reaps it;
//  2. below walks nothing from a root of PID 1 or less, from a row with no
//     start time, nor from a root whose start time no longer matches;
//  3. signalProcs (signal.go) signals only a process strictly below the test
//     process now, by a walk whose every parent started no later than its
//     child; it refuses PID 1 and below, the test and its ancestors, and a
//     recorded process outside the test's tree is never signalled, and is a
//     reported leak if it is still alive at the stop's last deadline;
//  4. a forced command dies with its sshd (relayguard.go), so it never
//     leaves the test's tree, and a passing run reports no leak.
//
// What each guarantees alone. Guard 1: no row of an exited, unreadable or
// timeless sshd is stored; it does not cover a PID recycled between record's
// check that the sshd lives and its walk, which guard 2 does. Guard 2: no walk
// starts from a zero, timeless, exited or recycled root; the stored roots
// themselves still reach the stop. Guard 3: whatever a row's source,
// withRelayEnv's included, only a process strictly below the test binary now
// is signalled, never PID 1, the test or its ancestors; it does not tell one
// test's processes from another's in the same binary.

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// procRow is one live process: its ID, parent and start time in clock ticks
// since boot, which tells a recycled PID from the same process.
type procRow struct {
	pid, ppid int
	ticks     uint64
}

// readStat parses /proc/<pid>/stat: its state letter and its row. The fields
// after the command name, which may hold spaces and parentheses, start after
// the last ')'. gone is true for a process that no longer exists; a row that
// exists but cannot be read or parsed is an error.
func readStat(pid int) (state string, r procRow, gone bool, err error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	switch {
	case errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH):
		return "", procRow{}, true, nil
	case err != nil:
		return "", procRow{}, false, err
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return "", procRow{}, false, fmt.Errorf("/proc/%d/stat has no command name", pid)
	}
	// f[0] is the state (field 3), f[1] the parent, f[19] the start time.
	f := strings.Fields(s[i+1:])
	if len(f) < 20 {
		return "", procRow{}, false, fmt.Errorf("/proc/%d/stat has %d fields after the command name, want at least 20", pid, len(f))
	}
	ppid, err1 := strconv.Atoi(f[1])
	ticks, err2 := strconv.ParseUint(f[19], 10, 64)
	if err1 != nil || err2 != nil {
		return "", procRow{}, false, fmt.Errorf("/proc/%d/stat has an unreadable parent or start time", pid)
	}
	return f[0], procRow{pid: pid, ppid: ppid, ticks: ticks}, false, nil
}

// statRow is one process's row: live is false for one that has exited, a
// zombie included, and err is set for one whose row exists but cannot be read.
func statRow(pid int) (procRow, bool, error) {
	state, r, gone, err := readStat(pid)
	if err != nil || gone || state == "Z" || state == "X" {
		return procRow{}, false, err
	}
	return r, true, nil
}

// readProcRow is statRow with a row that cannot be read taken as not live, so
// it is never listed or walked.
func readProcRow(pid int) (procRow, bool) {
	r, live, err := statRow(pid)
	return r, live && err == nil
}

// liveProcs lists every live process on the host.
func liveProcs() []procRow {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var rows []procRow
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		if r, ok := readProcRow(pid); ok {
			rows = append(rows, r)
		}
	}
	return rows
}

// below returns the live processes under root, transitively, root excluded.
func below(root procRow) []procRow { return belowIn(liveProcs(), root) }

// belowIn is below over a listing. It walks nothing from a root of PID 1 or
// less, since every process descends from PID 1 and PID 1 and the kernel's
// threads have parent 0; from a root with no start time, which no process the
// nodes start has; nor from a root that is not in the listing with its
// recorded start time: that one has exited, or its PID names another process
// now, whose children are not the root's (issue #308).
func belowIn(rows []procRow, root procRow) []procRow {
	if root.pid <= 1 {
		return nil
	}
	if root.ticks == 0 {
		return nil
	}
	found := false
	for _, r := range rows {
		if r.pid == root.pid && r.ticks == root.ticks {
			found = true
			break
		}
	}
	if !found {
		return nil
	}
	in := map[int]bool{root.pid: true}
	var out []procRow
	for changed := true; changed; {
		changed = false
		for _, r := range rows {
			if !in[r.pid] && in[r.ppid] {
				in[r.pid] = true
				out = append(out, r)
				changed = true
			}
		}
	}
	return out
}

// withRelayEnv returns the live processes that hold relayPath in their
// environment: the forced command of the login node, wherever it now sits in
// the process tree. The path is the node's own socket, in a private
// directory; anything but an absolute path selects nothing.
func withRelayEnv(relayPath string) []procRow {
	if !strings.HasPrefix(relayPath, "/") {
		return nil
	}
	want := []byte(EnvRelay + "=" + relayPath)
	var out []procRow
	for _, r := range liveProcs() {
		b, err := os.ReadFile("/proc/" + strconv.Itoa(r.pid) + "/environ")
		if err != nil {
			continue
		}
		for _, kv := range splitNUL(b) {
			if string(kv) == string(want) {
				out = append(out, r)
				break
			}
		}
	}
	return out
}

func splitNUL(b []byte) [][]byte {
	var out [][]byte
	start := 0
	for i, c := range b {
		if c == 0 {
			out = append(out, b[start:i])
			start = i + 1
		}
	}
	if start < len(b) {
		out = append(out, b[start:])
	}
	return out
}

// anyLive reports whether any listed process is still the one listed.
func anyLive(rows []procRow) bool {
	for _, r := range rows {
		if now, ok := readProcRow(r.pid); ok && now.ticks == r.ticks {
			return true
		}
	}
	return false
}
