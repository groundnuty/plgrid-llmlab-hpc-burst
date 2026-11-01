package launcher

import (
	"context"
	"fmt"
	"os"
	"time"

	"k8s.io/client-go/rest"
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
	Health     string       `json:"health"`
	Reasons    []string     `json:"reasons,omitempty"`
	StateDir   string       `json:"state_dir"`
	Version    string       `json:"version,omitempty"`
	StartedAt  *time.Time   `json:"started_at,omitempty"`
	Processes  []ProcHealth `json:"processes,omitempty"`
	Ports      *Ports       `json:"ports,omitempty"`
	Listeners  []string     `json:"listeners,omitempty"`  // held by the recorded processes
	Executable string       `json:"executable,omitempty"` // what k3s runs: must be under its own data directory
	RSSMiB     float64      `json:"rss_mib,omitempty"`
	Readyz     *bool        `json:"readyz_ok,omitempty"` // nil: not checked
	KCM        *bool        `json:"controller_manager_ok,omitempty"`
	Writes     *bool        `json:"writes_ok,omitempty"`
	GC         *bool        `json:"gc_ok,omitempty"` // the garbage-collector probe
	Front      *Front       `json:"front,omitempty"`
	// EventTTL is how long the API server keeps an Event, from the launcher's own record of the
	// arguments it started k3s with (State.EventTTL; HBEP-8, Retention), never from the API server.
	// EventTTLDefault is true when the record has none, as it has not after a start by an earlier
	// release: EventTTL is then the API server's own default, and a restart (down and up, or
	// upgrade) applies DefaultEventTTL. That host is not unhealthy.
	EventTTL        string `json:"event_ttl,omitempty"`
	EventTTLDefault bool   `json:"event_ttl_default,omitempty"`
	// Supervisor and Controllers are set when the launcher runs controllers: the supervisor's
	// identity check and each supervised process.
	Supervisor  *SupervisorHealth `json:"supervisor,omitempty"`
	Controllers []EntryHealth     `json:"controllers,omitempty"`
	// AgentBuilds is the check of the agent and frpc builds the PlatformProfiles' partitions need
	// (#158), set when controllers run. Its Checked is false when the API server was not asked
	// (--no-api, or an unhealthy host) or could not give the profiles, and Detail says why.
	AgentBuilds *AgentBuilds `json:"agent_builds,omitempty"`
}

// StatusOptions are the inputs of Status.
type StatusOptions struct {
	StateDir string
	// NoAPI skips the questions asked of the API (/readyz, the controller manager's /healthz,
	// the write probe and the garbage-collector probe); the process and socket checks always run.
	NoAPI bool
	// NoGCProbe skips the garbage-collector probe only.
	NoGCProbe bool
}

func ptr(b bool) *bool { return &b }

// statusStageHook, when set (by tests only), runs once the listeners have been checked, so a test
// can make the process exit at that point and show that the final identity check sees it.
var statusStageHook func(stage string)

// Status reports the health of the host. The order is the point: it first takes a pidfd for every
// recorded process and checks it is the one the launcher started (PID, start time, boot ID),
// then that k3s runs a binary from its own data directory, then that the expected listeners are
// held by those processes, and only then asks the API, and only through the launcher's own
// kubeconfig, whose address is checked to be this host's. After the API has answered it checks
// the listeners and the identities again: an answer counts only if the recorded process held the
// port before and after it. A listener that answers on a port but is not held by the recorded
// process never makes the host healthy.
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
	if rep.EventTTL = st.EventTTL; rep.EventTTL == "" {
		rep.EventTTL, rep.EventTTLDefault = apiserverEventTTL, true
	}
	rep.Health = Healthy
	bad := func(format string, a ...any) {
		rep.Health = Unhealthy
		rep.Reasons = append(rep.Reasons, fmt.Sprintf(format, a...))
	}

	var hs []*procHandle
	defer func() { closeAll(hs) }()
	var sup *procHandle // the supervisor, when it runs
	k3sGone := false
	for i, rec := range st.Procs {
		h, v, detail := openRec(rec)
		rep.Processes = append(rep.Processes, ProcHealth{Name: rec.Name, PID: rec.PID, Verdict: v.String(), Detail: detail})
		if v != Ours {
			// The supervisor's reason is given by checkSupervision, after k3s has been judged: a
			// dead supervisor must not make the checks of k3s's own state seem unnecessary.
			if rec.Name != supervisorName {
				bad("process %s (pid %d) is not running: %s", rec.Name, rec.PID, detail)
			}
			k3sGone = k3sGone || i == 0 // k3s is the first record; the supervisor follows it
			continue
		}
		hs = append(hs, h)
		if rec.Name == supervisorName {
			sup = h
		}
	}
	if k3sGone {
		return rep, nil // nothing else is worth asking: the recorded host is not there
	}
	// final checks that every recorded process is still the one checked at the start.
	final := func() {
		for _, h := range hs {
			if v, d := h.verify(); v != Ours {
				bad("process %s (pid %d) changed while it was being checked: %s", h.rec.Name, h.rec.PID, d)
			}
		}
	}
	// The ports k3s must hold are judged on k3s's own process tree; the controllers' ports are
	// checked per supervised process by checkSupervision.
	listeners := func() (owned, others []sock, err error) {
		tree := openTree(hs[0])
		defer closeAll(tree[1:])
		return ownedListeners(tree)
	}

	if exe, err := checkExe(l, hs[0]); err != nil {
		rep.Executable = exe
		bad("%v", err)
	} else {
		rep.Executable = exe
	}
	owned, others, err := listeners()
	if err != nil {
		return nil, err
	}
	for _, s := range sortedPorts(owned) {
		rep.Listeners = append(rep.Listeners, s.String())
	}
	for _, p := range verifyListeners(st.Ports.Expected(), owned, others) {
		bad("%s", p)
	}
	if rss, err := rssMiB([]int{hs[0].rec.PID}); err == nil {
		rep.RSSMiB = rss
	}
	hostBad := rep.Health != Healthy // k3s's own state, before the controllers are judged
	checkSupervision(l, st, sup, rep, bad)
	if statusStageHook != nil {
		statusStageHook("listeners")
	}
	if hostBad || o.NoAPI {
		if st.Controllers != nil {
			reason := "the host is unhealthy, so the API server was not asked"
			if o.NoAPI {
				reason = "--no-api: the profiles that say which builds are needed are read from the API server, which was not asked"
			}
			rep.AgentBuilds = &AgentBuilds{Dir: l.host(st.Ports).AgentBuildsDir(), Detail: reason}
		}
		final()
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
	if err := readyz(ctx, cfg); err != nil {
		rep.Readyz = ptr(false)
		bad("apiserver /readyz: %v", err)
	} else {
		rep.Readyz = ptr(true)
	}
	if err := kcmHealthz(ctx, st.Ports.KCM, 5*time.Second); err != nil {
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
	// The garbage-collector probe writes objects, so it runs only where writes work.
	if !o.NoGCProbe && rep.Writes != nil && *rep.Writes {
		if err := gcProbe(ctx, cfg, gcProbeTimeout); err != nil {
			rep.GC = ptr(false)
			bad("garbage collection: %v", err)
		} else {
			rep.GC = ptr(true)
		}
	}
	// The builds are staged by the controllers, so a host without them has none to check.
	if st.Controllers != nil {
		checkBuildsInStatus(ctx, cfg, l.host(st.Ports).AgentBuildsDir(), rep, bad)
	}
	// The answers count only if the recorded processes held the ports throughout: the listeners
	// again, then the identities.
	if owned, others, err := listeners(); err != nil {
		bad("cannot list the listeners after the API checks: %v", err)
	} else {
		for _, p := range verifyListeners(st.Ports.Expected(), owned, others) {
			bad("after the API checks: %s", p)
		}
	}
	final()
	return rep, nil
}

// checkBuildsInStatus sets rep.AgentBuilds from the API server's PlatformProfiles, and makes every
// missing or mismatched build a reason the host is unhealthy: a host whose first submission would
// be refused is not ready for work. Status exits 1 until the builds are fixed, and 0 after.
func checkBuildsInStatus(ctx context.Context, cfg *rest.Config, dir string, rep *StatusReport, bad func(string, ...any)) {
	if rep.Readyz == nil || !*rep.Readyz {
		rep.AgentBuilds = &AgentBuilds{Dir: dir, Detail: "the API server is not ready, so the PlatformProfiles were not read"}
		return
	}
	ab, err := CheckAgentBuilds(ctx, cfg, dir)
	if err != nil {
		rep.AgentBuilds = &AgentBuilds{Dir: dir, Detail: err.Error()}
		bad("agent builds could not be checked: %v", err)
		return
	}
	rep.AgentBuilds = ab
	for _, p := range ab.Problems {
		bad("agent builds: %s", p)
	}
}
