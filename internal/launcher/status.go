package launcher

import (
	"context"
	"fmt"
	"os"
	"time"
)

// Health values of a status report.
const (
	Healthy   = "healthy"
	Unhealthy = "unhealthy"
	Stopped   = "stopped"
)

// ProcHealth is the identity check of one recorded process.
type ProcHealth struct {
	Name    string `json:"name"`
	PID     int    `json:"pid"`
	Verdict string `json:"verdict"` // ours, gone or foreign
	Detail  string `json:"detail,omitempty"`
}

// StatusReport is the health of the host in a state directory, judged from the recorded
// processes and the sockets they hold. Every reason that makes it unhealthy is listed.
type StatusReport struct {
	Health    string       `json:"health"`
	Reasons   []string     `json:"reasons,omitempty"`
	StateDir  string       `json:"state_dir"`
	Version   string       `json:"version,omitempty"`
	StartedAt *time.Time   `json:"started_at,omitempty"`
	Processes []ProcHealth `json:"processes,omitempty"`
	Ports     *Ports       `json:"ports,omitempty"`
	Listeners []string     `json:"listeners,omitempty"` // held by the recorded processes
	RSSMiB    float64      `json:"rss_mib,omitempty"`
	Readyz    *bool        `json:"readyz_ok,omitempty"` // nil: not checked
	KCM       *bool        `json:"controller_manager_ok,omitempty"`
	Writes    *bool        `json:"writes_ok,omitempty"`
	Front     *Front       `json:"front,omitempty"`
}

// StatusOptions are the inputs of Status.
type StatusOptions struct {
	StateDir string
	// NoAPI skips the three questions asked of the API (/readyz, the controller manager's
	// /healthz, and a write probe); the process and socket checks always run.
	NoAPI bool
}

func ptr(b bool) *bool { return &b }

// Status reports the health of the host. The order is the point: it first checks that every
// recorded process is the one the launcher started (PID, start time, boot ID), then that the
// expected listeners are held by those processes, and only then asks the API, and only through
// the launcher's own kubeconfig, whose address is checked to be this host's. A listener that
// answers on a port but is not held by the recorded process never makes the host healthy.
func Status(ctx context.Context, o StatusOptions) (*StatusReport, error) {
	l, err := newLayout(o.StateDir)
	if err != nil {
		return nil, err
	}
	rep := &StatusReport{StateDir: l.dir}
	if _, err := os.Stat(l.dir); os.IsNotExist(err) {
		rep.Health, rep.Reasons = Stopped, []string{"no state directory: nothing was started here"}
		return rep, nil
	}
	st, err := l.loadState()
	if err != nil {
		return nil, err
	}
	if st == nil {
		rep.Health, rep.Reasons = Stopped, []string{"no launcher record: no host is running from this state directory"}
		return rep, nil
	}
	rep.Version, rep.StartedAt, rep.Ports, rep.Front = st.Version, &st.StartedAt, &st.Ports, &st.Front
	rep.Health = Healthy
	bad := func(format string, a ...any) {
		rep.Health = Unhealthy
		rep.Reasons = append(rep.Reasons, fmt.Sprintf(format, a...))
	}

	var pids []int
	for _, rec := range st.Procs {
		v, detail := identify(rec)
		rep.Processes = append(rep.Processes, ProcHealth{Name: rec.Name, PID: rec.PID, Verdict: v.String(), Detail: detail})
		if v != Ours {
			bad("process %s (pid %d) is not running: %s", rec.Name, rec.PID, detail)
			continue
		}
		pids = append(pids, rec.PID)
	}
	if rep.Health != Healthy {
		return rep, nil // nothing else is worth asking: the recorded host is not there
	}

	owned, others, err := ownedListeners(pids)
	if err != nil {
		return nil, err
	}
	for _, s := range sortedPorts(owned) {
		rep.Listeners = append(rep.Listeners, s.String())
	}
	for _, p := range verifyListeners(st.Ports.Expected(), owned, others) {
		bad("%s", p)
	}
	if rss, err := rssMiB(pids); err == nil {
		rep.RSSMiB = rss
	}
	if rep.Health != Healthy || o.NoAPI {
		return rep, nil
	}

	if fi, err := os.Stat(l.kubeconfig()); err != nil {
		bad("no kubeconfig (%v): the host never became ready, or a start is still in progress", err)
		return rep, nil
	} else if fi.Mode().Perm() != 0o600 {
		bad("kubeconfig %s has mode %v, want 0600", l.kubeconfig(), fi.Mode().Perm())
	}
	cfg, err := restConfig(l.kubeconfig(), st.Ports, 5*time.Second)
	if err != nil {
		bad("%v", err)
		return rep, nil
	}
	if err := readyz(cfg); err != nil {
		rep.Readyz = ptr(false)
		bad("apiserver /readyz: %v", err)
	} else {
		rep.Readyz = ptr(true)
	}
	if err := kcmHealthz(st.Ports.KCM, 5*time.Second); err != nil {
		rep.KCM = ptr(false)
		bad("controller manager /healthz: %v", err)
	} else {
		rep.KCM = ptr(true)
	}
	if rep.Readyz != nil && *rep.Readyz {
		if err := writeProbe(ctx, cfg, 15*time.Second); err != nil {
			rep.Writes = ptr(false)
			bad("writes are failing: %v", err)
		} else {
			rep.Writes = ptr(true)
		}
	}
	// The process and its sockets must still be the recorded ones after the API answered.
	for _, rec := range st.Procs {
		if v, d := identify(rec); v != Ours {
			bad("process %s (pid %d) disappeared while it was being checked: %s", rec.Name, rec.PID, d)
		}
	}
	return rep, nil
}
