package rehearsal

// New, after internal/slurm/fakeslurm/procfs.go: the processes below a given
// one, read from /proc. sshd's session child calls setsid, so it leaves sshd's
// process group; the login node finds what to stop by parentage and verifies
// each process by its start time, so a recycled PID is left alone.

import (
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

// readProcRow parses /proc/<pid>/stat. The fields after the command name,
// which may hold spaces and parentheses, start after the last ')'. A zombie, a
// dead process and one that is gone are not live.
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
	// f[0] is the state (field 3), f[1] the parent, f[19] the start time.
	f := strings.Fields(s[i+1:])
	if len(f) < 20 || f[0] == "Z" || f[0] == "X" {
		return procRow{}, false
	}
	ppid, err1 := strconv.Atoi(f[1])
	ticks, err2 := strconv.ParseUint(f[19], 10, 64)
	if err1 != nil || err2 != nil {
		return procRow{}, false
	}
	return procRow{pid: pid, ppid: ppid, ticks: ticks}, true
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
func below(root int) []procRow {
	rows := liveProcs()
	in := map[int]bool{root: true}
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
// the process tree.
func withRelayEnv(relayPath string) []procRow {
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

// anyLive reports whether any listed process is still the one listed.
func anyLive(rows []procRow) bool {
	for _, r := range rows {
		if now, ok := readProcRow(r.pid); ok && now.ticks == r.ticks {
			return true
		}
	}
	return false
}
