package launcher

import (
	"fmt"
	"net/http"
	"time"
)

// SupervisorHealth is the supervisor's identity check.
type SupervisorHealth struct {
	Running bool   `json:"running"`
	PID     int    `json:"pid,omitempty"`
	Detail  string `json:"detail,omitempty"`
}

// EntryHealth is the check of one supervised process.
type EntryHealth struct {
	Name      string      `json:"name"`
	Running   bool        `json:"running"` // a live child of the supervisor, verified through its pidfd
	PID       int         `json:"pid,omitempty"`
	State     string      `json:"state"`
	Restarts  int         `json:"restarts"`
	LastExit  *ExitRecord `json:"last_exit,omitempty"`
	Health    string      `json:"health_addr"`
	HealthzOK *bool       `json:"healthz_ok,omitempty"` // nil: not asked
	Detail    string      `json:"detail,omitempty"`
}

// healthzTimeout bounds one GET /healthz of a supervised process.
const healthzTimeout = 3 * time.Second

// checkSupervision judges the supervisor and each process it keeps. sup is the verified handle on
// the supervisor, nil when it is not running; the caller has already counted that against health.
// It reads supervisor.json only for the list of children and their identities: every child is
// checked here, through its own pidfd.
func checkSupervision(l layout, st *State, sup *procHandle, rep *StatusReport, bad func(string, ...any)) {
	var supRec *ProcRec
	for i := range st.Procs {
		if st.Procs[i].Name == supervisorName {
			supRec = &st.Procs[i]
		}
	}
	switch {
	case supRec == nil && st.Controllers == nil:
		return // no controller binary was found at up: no controllers run, and none were meant to
	case supRec == nil:
		rep.Supervisor = &SupervisorHealth{Detail: "not recorded"}
		bad("controllers are configured, but no supervisor is recorded, so none runs (hpcburst down, then up)")
		return
	}
	rep.Supervisor = &SupervisorHealth{Running: sup != nil, PID: supRec.PID}
	if sup == nil {
		for _, p := range rep.Processes {
			if p.Name == supervisorName {
				rep.Supervisor.Detail = p.Detail
				bad("process %s (pid %d) is not running: %s", p.Name, p.PID, p.Detail)
			}
		}
	}
	f, err := readSupervisorFile(l)
	switch {
	case err != nil:
		bad("cannot read the supervisor's record of its processes: %v", err)
		return
	case f.Supervisor != *supRec:
		bad("%s was written by another supervisor (pid %d), not the recorded one (pid %d)", l.supervisorFile(), f.Supervisor.PID, supRec.PID)
		return
	}
	for _, e := range f.Entries {
		eh, problems := checkEntry(sup, supRec.PID, e)
		rep.Controllers = append(rep.Controllers, eh)
		for _, p := range problems {
			bad("%s: %s", e.Name, p)
		}
	}
}

// checkEntry checks one supervised process the way the host's own are checked: it must be the
// recorded process (PID, start time and boot ID, through a pidfd), a child of the supervisor, and
// hold its health port, and only then does its /healthz count. After the answer it checks the
// identity and the port again, so an answer from something that took the port over means nothing.
func checkEntry(sup *procHandle, supPID int, e EntryRecord) (EntryHealth, []string) {
	eh := EntryHealth{Name: e.Name, State: e.State, Restarts: e.Restarts, LastExit: e.LastExit, Health: e.Health}
	var problems []string
	add := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }
	if e.Proc == nil {
		why := "never started"
		if e.LastExit != nil {
			why = fmt.Sprintf("last exit: %s, code %d, at %s", e.LastExit.Detail, e.LastExit.Code, e.LastExit.At.Format(time.RFC3339))
		}
		add("not running (%s; %s)", e.State, why)
		return eh, problems
	}
	eh.PID = e.Proc.PID
	h, v, detail := openRec(*e.Proc)
	if v != Ours {
		add("pid %d is not running: %s", e.Proc.PID, detail)
		return eh, problems
	}
	defer h.Close()
	st, _, _ := h.check()
	if st.PPID != supPID || sup == nil {
		add("pid %d is not a child of the supervisor (pid %d): its parent is pid %d", e.Proc.PID, supPID, st.PPID)
		return eh, problems
	}
	eh.Running = true
	port, err := parsePort(e.Health)
	if err != nil {
		add("health address %q: %v", e.Health, err)
		return eh, problems
	}
	if own, err := portOwned(h, port); err != nil {
		add("cannot list the sockets of pid %d: %v", e.Proc.PID, err)
		return eh, problems
	} else if !own {
		add("pid %d does not hold its health port %d", e.Proc.PID, port)
		return eh, problems
	}
	ok := true
	if err := healthz(e.Health, healthzTimeout); err != nil {
		ok = false
		add("GET /healthz on %s: %v", e.Health, err)
	}
	eh.HealthzOK = &ok
	if v, d := h.verify(); v != Ours {
		add("pid %d changed while it was being checked: %s", e.Proc.PID, d)
	} else if own, err := portOwned(h, port); err != nil || !own {
		add("pid %d did not hold port %d after the answer", e.Proc.PID, port)
	}
	return eh, problems
}

// healthz asks a supervised process for GET /healthz on its loopback address.
func healthz(addr string, timeout time.Duration) error {
	code, err := getStatus(&http.Client{Timeout: timeout}, "http://"+addr+"/healthz")
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("answered %d", code)
	}
	return nil
}
