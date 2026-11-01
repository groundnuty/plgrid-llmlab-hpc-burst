package fakeslurm

// New, after internal/launcher's leftover test (killTree): the processes of a
// run-mode job, read from /proc. Slurm tracks a job's processes in a cgroup;
// the fake tracks the session the batch shell leads, which every descendant
// stays in unless it calls setsid, plus the descendants of its members, which
// catches one that did while its parent lives.

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

// procRow is one live process: its ID, parent, session and start time in
// clock ticks since boot, which tells a recycled PID from the same process.
type procRow struct {
	pid, ppid, sid int
	ticks          uint64
}

// readProcRow parses /proc/<pid>/stat. Fields after the command name, which
// may hold spaces and parentheses, start after the last ')'. A zombie or a
// dead process is not live; false for it and for a process that is gone.
func readProcRow(pid int) (procRow, bool) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return procRow{}, false
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return procRow{}, false
	}
	// f[0] is the state (field 3), f[1] ppid, f[3] session, f[19] starttime.
	f := strings.Fields(s[i+1:])
	if len(f) < 20 || f[0] == "Z" || f[0] == "X" {
		return procRow{}, false
	}
	ppid, err1 := strconv.Atoi(f[1])
	sid, err2 := strconv.Atoi(f[3])
	ticks, err3 := strconv.ParseUint(f[19], 10, 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return procRow{}, false
	}
	return procRow{pid: pid, ppid: ppid, sid: sid, ticks: ticks}, true
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

// sessionProcs lists the live processes of the job whose batch shell is sid:
// the members of its session and, transitively, their children.
func sessionProcs(sid int) []procRow {
	rows := liveProcs()
	in := map[int]bool{}
	for _, r := range rows {
		if r.sid == sid {
			in[r.pid] = true
		}
	}
	for changed := len(in) > 0; changed; {
		changed = false
		for _, r := range rows {
			if !in[r.pid] && in[r.ppid] {
				in[r.pid] = true
				changed = true
			}
		}
	}
	var out []procRow
	for _, r := range rows {
		if in[r.pid] {
			out = append(out, r)
		}
	}
	return out
}

// signalProcs sends sig to each process that is still the one listed: a PID
// that now names a process with another start time was recycled and is left
// alone.
func signalProcs(rows []procRow, sig syscall.Signal) {
	for _, r := range rows {
		if now, ok := readProcRow(r.pid); ok && now.ticks == r.ticks {
			_ = syscall.Kill(r.pid, sig) // ESRCH: it ended since the check
		}
	}
}
