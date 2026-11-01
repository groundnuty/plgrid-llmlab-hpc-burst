package launcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	// UsageState is the process's usage sink as its /healthz body states it in the usage member
	// (HBEP-8, Usage records): "ok" or "failing". The front reports it; it is empty for a process
	// whose body has no usage member, such as the controller.
	UsageState string `json:"usage_state,omitempty"`
	// Degraded names what is degraded while the process keeps serving: DegradedUsage while its
	// usage sink says failing. It is not a health problem: the report stays healthy and status
	// exits 0, and only this process's line says degraded (@hpc-burst-science-agent, 31 Oct 2026;
	// HBEP-8, Usage records, and HBEP-11).
	Degraded string `json:"degraded,omitempty"`
	Detail   string `json:"detail,omitempty"`
}

// DegradedUsage is EntryHealth.Degraded for a failing usage sink. Status prints it as "degraded:
// usage writes failing" (@hpc-burst-science-agent, 31 Oct 2026); it is never reported as unhealthy.
const DegradedUsage = "usage writes failing"

// The usage member's states (HBEP-8): ok, or failing while records are lost.
const (
	usageOK      = "ok"
	usageFailing = "failing"
)

// UsageUnknown is EntryHealth.UsageState for a front whose /healthz did not state a known usage
// state. Status prints "usage state unknown" for it; it is not a health problem either.
const UsageUnknown = "unknown"

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
// hold its health address (that address and port exactly), and only then does its /healthz on that
// address count. After the answer it checks the identity and the address again, so an answer from
// something that took the address over means nothing.
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
	if _, err := parsePort(e.Health); err != nil {
		add("health address %q: %v", e.Health, err)
		return eh, problems
	}
	if own, err := addrOwned(h, e.Health); err != nil {
		add("cannot list the sockets of pid %d: %v", e.Proc.PID, err)
		return eh, problems
	} else if !own {
		add("pid %d does not hold its health address %s", e.Proc.PID, e.Health)
		return eh, problems
	}
	if len(e.Ports) > 0 {
		// A process with a fixed set of ports (frps, HBEP-6) must hold exactly those, TCP on
		// 127.0.0.1, and nothing else reachable, TCP or UDP, on any address: a missing one is a
		// listener that failed, any other is a listener that must not exist (such as KCP or QUIC
		// on UDP), and a socket at another address on the same port lets something else answer.
		owned, others, err := treeBound(h)
		if err != nil {
			add("cannot list the sockets of pid %d: %v", e.Proc.PID, err)
			return eh, problems
		}
		want := make([]sock, 0, len(e.Ports))
		for _, p := range e.Ports {
			want = append(want, sock{Listener: Listener{Addr: "127.0.0.1", Port: p}, proto: "tcp"})
		}
		if probs := verifyExactListeners(want, owned, others); len(probs) > 0 {
			for _, p := range probs {
				add("pid %d: %s", e.Proc.PID, p)
			}
			return eh, problems
		}
	}
	ok := true
	body, err := healthz(e.Health, healthzTimeout)
	if err != nil {
		ok = false
		add("GET /healthz on %s: %v", e.Health, err)
	}
	eh.HealthzOK = &ok
	if ok {
		// A failing usage sink is stated, never counted against health: the front keeps serving.
		switch eh.UsageState = usageStateOf(body); {
		case eh.UsageState == usageFailing:
			eh.Degraded = DegradedUsage
		case e.Name == frontEntryName && eh.UsageState != usageOK:
			// The front always states its sink; a body without that member, cut at the read limit,
			// or with a state HBEP-8 does not name leaves it unknown, and status says so.
			eh.UsageState = UsageUnknown
		}
	}
	if v, d := h.verify(); v != Ours {
		add("pid %d changed while it was being checked: %s", e.Proc.PID, d)
	} else if own, err := addrOwned(h, e.Health); err != nil || !own {
		add("pid %d did not hold %s after the answer", e.Proc.PID, e.Health)
	}
	return eh, problems
}

// healthzBodyLimit caps how much of a /healthz body is read. The front's is a few hundred bytes
// plus one line per ModelDeployment.
const healthzBodyLimit = 1 << 20

// healthz asks a supervised process for GET /healthz on its loopback address and returns the body
// of a 200 answer. Any other status is an error.
func healthz(addr string, timeout time.Duration) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/healthz", nil)
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("answered %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, healthzBodyLimit))
	if err != nil {
		return nil, fmt.Errorf("read the answer: %w", err)
	}
	return body, nil
}

// usageStateOf returns the usage member's state from a /healthz body (HBEP-8: /healthz carries
// usage with state ok or failing), or "" when the body is not a JSON object with that member.
func usageStateOf(body []byte) string {
	var b struct {
		Usage *struct {
			State string `json:"state"`
		} `json:"usage"`
	}
	if json.Unmarshal(body, &b) != nil || b.Usage == nil {
		return ""
	}
	return b.Usage.State
}
