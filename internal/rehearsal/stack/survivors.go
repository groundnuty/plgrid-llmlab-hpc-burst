package stack

// What is left of a stack after hpcburst down: its processes as hpcburst
// status reported them before down, and its ports. Lifted from the rehearsal's
// step (g) (internal/rehearsal/scenario, step_zero_test.go: procStart, gone,
// stackProcesses and downAndCheck's checks), so that a caller outside that
// package can check a stop the same way (PR #317's review: a test that expects
// its scenario to fail must still see the stack stop).

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Proc is one process of the stack, with its start time, so that another
// process under the same PID is not taken for it.
type Proc struct {
	Name  string
	PID   int
	start uint64
}

func (p Proc) String() string { return p.Name + "=" + strconv.Itoa(p.PID) }

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
	add := func(name string, pid int) {
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
		procs = append(procs, Proc{Name: name, PID: pid, start: start})
	}
	for _, p := range st.Processes {
		if p.Verdict == "ours" && p.PID > 0 {
			add(p.Name, p.PID)
		}
	}
	if st.Supervisor != nil && st.Supervisor.Running && st.Supervisor.PID > 0 {
		add("supervisor", st.Supervisor.PID)
	}
	for _, c := range st.Controllers {
		if c.Running && c.PID > 0 {
			add(c.Name, c.PID)
		}
	}
	if len(errs) > 0 {
		return procs, fmt.Errorf("hpcburst status reports processes that do not run: %s", strings.Join(errs, ", "))
	}
	return procs, nil
}

// Left lists what is left of the stack after down: each of procs still
// running once wait has passed, and each of ports with a listener. It never
// stops the test, so that a caller's clean-up goes on after it; a port list it
// cannot read is in the list too.
func Left(procs []Proc, ports []int, wait time.Duration) []string {
	deadline := time.Now().Add(wait)
	var running []string
	for {
		running = running[:0]
		for _, p := range procs {
			if !p.Gone() {
				running = append(running, p.String())
			}
		}
		if len(running) == 0 || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	left := make([]string, 0, len(running))
	for _, r := range running {
		left = append(left, r+" still runs")
	}
	l, err := listening(ports)
	if err != nil {
		return append(left, err.Error())
	}
	for _, p := range l {
		left = append(left, "port "+strconv.Itoa(p)+" still listens")
	}
	return left
}
