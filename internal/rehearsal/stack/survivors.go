package stack

// What is left of a stack after hpcburst down: its processes as hpcburst
// status reported them before down, and its ports. Lifted from the rehearsal's
// step (g) (internal/rehearsal/scenario, step_zero_test.go: procStart, gone,
// stackProcesses and downAndCheck's checks, which now use these), so that a
// caller outside that package can check a stop the same way (PR #317's review:
// a test that expects its scenario to fail must still see the stack stop).

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Proc is one process of the stack, with its start time (/proc/<pid>/stat
// field 22, clock ticks since boot), so that a PID the kernel has since given
// to another process does not count as the same one. Recorded says it is one of
// the launcher's recorded processes (k3s), not the supervisor or a supervised
// entry.
type Proc struct {
	Name     string
	PID      int
	Recorded bool
	start    uint64
}

// procStart is a live process's start time (field 22 of /proc/<pid>/stat),
// false when there is none.
func procStart(pid int) (uint64, bool) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, false
	}
	i := strings.LastIndexByte(string(b), ')')
	if i < 0 {
		return 0, false
	}
	f := strings.Fields(string(b[i+1:])) // f[0] is field 3, the state
	if len(f) < 20 {
		return 0, false
	}
	n, err := strconv.ParseUint(f[19], 10, 64)
	return n, err == nil
}

// Gone reports whether the process has ended: no such PID, another process
// under it, or a zombie, which holds nothing.
func (p Proc) Gone() bool {
	st, ok := procStart(p.PID)
	return !ok || st != p.start || !Alive(p.PID)
}

// Processes lists every process hpcburst status reports for the host: the
// recorded processes it judges ours (k3s), the supervisor, and each supervised
// entry that runs (the controller, the front, frps). down removes its own
// record (internal/launcher/down.go, clearRecord), so status after down reports
// nothing whatever survived: read them before. A process status names that does
// not run is an error, as is a status that cannot be read.
func (h *Host) Processes() ([]Proc, error) {
	out, err := Run(h.T, Within(h.T, StatusTimeout), h.Env, h.B.HPCBurst, "status", "--state-dir", h.State, "--json", "--no-api")
	var st struct {
		Processes []struct {
			Name    string `json:"name"`
			PID     int    `json:"pid"`
			Verdict string `json:"verdict"`
		} `json:"processes"`
		Supervisor *struct {
			Running bool `json:"running"`
			PID     int  `json:"pid"`
		} `json:"supervisor"`
		Controllers []struct {
			Name    string `json:"name"`
			Running bool   `json:"running"`
			PID     int    `json:"pid"`
		} `json:"controllers"`
	}
	if jerr := json.Unmarshal([]byte(out), &st); jerr != nil {
		return nil, fmt.Errorf("hpcburst status --json (%v): %v: %.500s", err, jerr, out)
	}
	var procs []Proc
	var errs []string
	add := func(name string, pid int, recorded bool) {
		for _, p := range procs {
			if p.PID == pid {
				return // the supervisor is both a recorded process and the supervisor
			}
		}
		start, ok := procStart(pid)
		if !ok {
			errs = append(errs, fmt.Sprintf("%s=%d does not run", name, pid))
			return
		}
		procs = append(procs, Proc{Name: name, PID: pid, Recorded: recorded, start: start})
	}
	for _, p := range st.Processes {
		if p.Verdict == "ours" && p.PID > 0 {
			add(p.Name, p.PID, true)
		}
	}
	if st.Supervisor != nil && st.Supervisor.Running && st.Supervisor.PID > 0 {
		add("supervisor", st.Supervisor.PID, false)
	}
	for _, c := range st.Controllers {
		if c.Running && c.PID > 0 {
			add(c.Name, c.PID, false)
		}
	}
	if len(errs) > 0 {
		return procs, fmt.Errorf("hpcburst status reports processes that do not run: %s", strings.Join(errs, ", "))
	}
	return procs, nil
}

// stackEntries are the supervised entries every stack runs, which status must
// report running before down.
var stackEntries = []string{"supervisor", "controller", "front", "frps"}

// MissingComponents lists what procs, the stack's processes read before down,
// lack of its whole set: the supervisor, the controller, the front and frps,
// each running, and at least one recorded process of ours (k3s). A check of
// what is left after down proves nothing of a component it never saw, so a
// caller fails on any of these (the rehearsal's step (g), and the alpha
// stand-ins: PR #317's delta review). The words fit after "no".
func MissingComponents(procs []Proc) []string {
	var missing []string
	for _, want := range stackEntries {
		if !slices.ContainsFunc(procs, func(p Proc) bool { return p.Name == want }) {
			missing = append(missing, "running "+want)
		}
	}
	if !slices.ContainsFunc(procs, func(p Proc) bool { return p.Recorded }) {
		missing = append(missing, "recorded process of ours (k3s)")
	}
	return missing
}

// LeftPortRead is the end of Left's time that it keeps for its read of the
// listening ports, after its wait for the processes; ss answers in
// milliseconds.
const LeftPortRead = 5 * time.Second

// Left lists what is left of the stack after down: each of procs still
// running when its wait for them ends, LeftPortRead before by, and each of
// ports with a listener. It returns by by, so that a caller can keep time for
// what comes after it, and never stops the test; a port list it cannot read in
// that time is in the list too.
//
// What it cannot see: a child a process of the stack forked after both
// snapshots, status's before down and down's own of its process trees, can
// outlive its parent without holding a port checked here, and Left reports
// nothing of it. Proving that nothing of the stack is left would take tracking
// what it owns beyond the saved PIDs (a cgroup of its own, say); the
// rehearsal's step (g) had the same limit.
func Left(procs []Proc, ports []int, by time.Time) []string {
	waitEnd := by.Add(-LeftPortRead)
	var running []string
	for {
		running = running[:0]
		for _, p := range procs {
			if !p.Gone() {
				running = append(running, p.Name+"="+strconv.Itoa(p.PID))
			}
		}
		rest := time.Until(waitEnd)
		if len(running) == 0 || rest <= 0 {
			break
		}
		time.Sleep(min(200*time.Millisecond, rest))
	}
	left := make([]string, 0, len(running))
	for _, r := range running {
		left = append(left, r+" still runs")
	}
	ctx, cancel := context.WithDeadline(context.Background(), by)
	defer cancel()
	l, err := listening(ctx, ports)
	if err != nil {
		return append(left, err.Error())
	}
	for _, p := range l {
		left = append(left, "port "+strconv.Itoa(p)+" still listens")
	}
	return left
}
