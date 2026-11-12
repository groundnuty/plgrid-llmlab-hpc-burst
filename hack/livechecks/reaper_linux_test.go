//go:build linux

package livechecks

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// setSubreaper makes this process the adoptive parent of every orphaned descendant.
func setSubreaper() error {
	return unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0)
}

// readProcTable reads every process's parent and name from /proc. The name sits in parentheses
// and may contain spaces and parentheses itself, so the fields after it start at the last ')'.
// A process that exits while the table is read is left out.
func readProcTable() (map[int]procInfo, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	table := map[int]procInfo{}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		s := string(b)
		l, r := strings.IndexByte(s, '('), strings.LastIndexByte(s, ')')
		if l < 0 || r < l {
			continue
		}
		rest := strings.Fields(s[r+1:]) // state, ppid, ...
		if len(rest) < 2 {
			continue
		}
		ppid, err := strconv.Atoi(rest[1])
		if err != nil {
			continue
		}
		table[pid] = procInfo{ppid: ppid, comm: s[l+1 : r], state: rest[0][0]}
	}
	return table, nil
}

// markedPIDs lists the live processes whose initial environment holds mark (a whole
// "NAME=value" entry), read from /proc/<pid>/environ: the processes of one test, which gave
// them a variable no other process has. A zombie has no environment, so it is not listed. The
// answer is a point-in-time list, for a test to wait on and to check; the only signalling
// from it is killMarked's, which checks the mark again just before each kill.
func markedPIDs(mark string) []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	want := []byte(mark + "\x00")
	var out []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		env, err := os.ReadFile("/proc/" + e.Name() + "/environ")
		if err == nil && bytes.Contains(env, want) {
			out = append(out, pid)
		}
	}
	return out
}

// killMarked kills the live processes that carry mark. It is a test's last resort for a
// process the code under test failed to stop: it checks the mark again immediately before the
// signal, so a pid that was reused is not signalled.
func killMarked(mark string) {
	want := []byte(mark + "\x00")
	for _, pid := range markedPIDs(mark) {
		env, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/environ")
		if err == nil && bytes.Contains(env, want) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
}

// hangDump says what the process tree of the reaper root is doing, for a test that gave up waiting on it (issue #494): each process
// (the reaper and its descendants) with its parent, its state, the kernel function it waits in and its command line, and, for the
// script, its signal sets: SigPnd and ShdPnd (signals waiting to be delivered), SigBlk, SigIgn and SigCgt (blocked, ignored and caught).
// A stop that the script never acted on shows as TERM (bit 15, mask 0x4000) pending or caught-and-blocked there. The flock locks held
// by any of them are listed too (/proc/locks), since a wait on a host lock is a wait on its holder. It reads /proc and signals nothing.
func hangDump(root int) string {
	table, err := readProcTable()
	if err != nil {
		return "no process table: " + err.Error()
	}
	pids := append([]int{root}, descendantsOf(root, table)...)
	in := map[int]bool{}
	var b strings.Builder
	for _, p := range pids {
		in[p] = true
		cmd, _ := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", p))
		wchan, _ := os.ReadFile(fmt.Sprintf("/proc/%d/wchan", p))
		args := strings.TrimSpace(strings.ReplaceAll(string(cmd), "\x00", " "))
		if len(args) > 160 {
			args = args[:160] + "..."
		}
		fmt.Fprintf(&b, "  pid %d ppid %d state %c wchan %s: %s\n", p, table[p].ppid, table[p].state, strings.TrimSpace(string(wchan)), args)
		if strings.Contains(string(cmd), "live-checks.sh") {
			st, _ := os.ReadFile(fmt.Sprintf("/proc/%d/status", p))
			for _, l := range strings.Split(string(st), "\n") {
				for _, k := range []string{"SigPnd", "ShdPnd", "SigBlk", "SigIgn", "SigCgt"} {
					if strings.HasPrefix(l, k+":") {
						b.WriteString("    " + l + "\n")
					}
				}
			}
		}
	}
	if locks, err := os.ReadFile("/proc/locks"); err == nil {
		for _, l := range strings.Split(string(locks), "\n") {
			f := strings.Fields(l)
			if len(f) > 4 {
				if pid, err := strconv.Atoi(f[4]); err == nil && in[pid] {
					b.WriteString("  lock: " + l + "\n")
				}
			}
		}
	}
	return b.String()
}
