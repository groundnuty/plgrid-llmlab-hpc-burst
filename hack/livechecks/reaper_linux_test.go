//go:build linux

package livechecks

import (
	"os"
	"strconv"
	"strings"

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
		table[pid] = procInfo{ppid: ppid, comm: s[l+1 : r]}
	}
	return table, nil
}
