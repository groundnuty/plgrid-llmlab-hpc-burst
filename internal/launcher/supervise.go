package launcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// supervisorName is the name of the supervisor's ProcRec in state.json. It is appended after
// k3s's record, never put before it: status, upgrade and the tests read Procs[0] as k3s.
const supervisorName = "supervisor"

// The restart policy (HBEP-11: the launcher restarts a controller that exits): a restart waits 1 s,
// each further quick exit doubles the wait up to 30 s, and a child that ran for 60 s starts the
// sequence again. On SIGTERM each child gets 10 s before SIGKILL.
const (
	DefaultBackoffMin  = time.Second
	DefaultBackoffMax  = 30 * time.Second
	DefaultStableAfter = 60 * time.Second
	DefaultTermGrace   = 10 * time.Second
)

// The start-up handoff. The supervisor outlives the launcher that starts it, so a supervisor whose
// launcher died before it saved the supervisor's identity in state.json could neither be stopped by
// down nor stop itself. The launcher therefore passes it the read end of a pipe, at descriptor
// HandoffFD, and writes handoffGo only after state.json names the supervisor. The supervisor
// starts no process until it has read that byte; if the pipe closes first (the launcher died), or
// nothing arrives within the handoff timeout, it exits having started nothing.
const (
	// HandoffFD is the descriptor at which the supervisor receives the handoff pipe: the first of
	// exec.Cmd.ExtraFiles.
	HandoffFD = 3
	// DefaultHandoffTimeout bounds the wait for the launcher's confirmation, which it sends as soon
	// as one file write has returned.
	DefaultHandoffTimeout = 30 * time.Second

	handoffGo byte = 'g'
)

// OpenHandoff returns the supervisor's end of the handoff pipe, which the launcher passed at
// descriptor fd. It refuses a descriptor that is not an open pipe, and marks it close-on-exec, so
// no process the supervisor starts can inherit it.
func OpenHandoff(fd int) (*os.File, error) {
	if fd < HandoffFD {
		return nil, fmt.Errorf("handoff descriptor %d: the launcher passes it at %d or above", fd, HandoffFD)
	}
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil {
		return nil, fmt.Errorf("handoff descriptor %d: %w", fd, err)
	}
	if st.Mode&syscall.S_IFMT != syscall.S_IFIFO {
		return nil, fmt.Errorf("handoff descriptor %d is not a pipe", fd)
	}
	syscall.CloseOnExec(fd)
	return os.NewFile(uintptr(fd), "handoff"), nil
}

// awaitHandoff blocks until the launcher confirms, and returns an error when it cannot be relied on
// to have recorded the supervisor: the pipe closed without the byte, another byte, no byte within
// timeout, or ctx done. The read runs in a goroutine, because the descriptor the launcher passes is
// in blocking mode and cannot take a deadline; a read still blocked at a timeout ends with the
// process, which Supervise's caller exits at once.
func awaitHandoff(ctx context.Context, h io.Reader, timeout time.Duration) error {
	type result struct {
		b   byte
		err error
	}
	got := make(chan result, 1)
	go func() {
		var buf [1]byte
		_, err := io.ReadFull(h, buf[:])
		got <- result{buf[0], err}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case r := <-got:
		switch {
		case errors.Is(r.err, io.EOF):
			return errors.New("the launcher ended without confirming that it had recorded the supervisor: no process was started")
		case r.err != nil:
			return fmt.Errorf("read the launcher's confirmation: %w", r.err)
		case r.b != handoffGo:
			return fmt.Errorf("the launcher sent %q, not its confirmation: no process was started", r.b)
		}
		return nil
	case <-timer.C:
		return fmt.Errorf("the launcher did not confirm that it had recorded the supervisor within %s: no process was started", timeout)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// supervised is one process the supervisor keeps running: the controller, and the front when up
// was given one (issue #64, frontEntry).
type supervised struct {
	Name string
	Path string
	// Argv0, when set, is the process's argv[0] instead of Path: frps runs /proc/self/fd/3, and
	// ps shows this name instead.
	Argv0  string
	Args   []string // after argv[0]
	Health string   // host:port that serves GET /healthz
	// TermGrace is how long this process gets between SIGTERM and SIGKILL when the supervisor
	// stops; 0 means SuperviseOptions.TermGrace. The front's is FrontTermGrace, the controller's
	// ControllerTermGrace.
	TermGrace time.Duration
	// WaitReady makes the start (up, upgrade) wait, within its timeout, until this process holds
	// its health port and answers /healthz with 200. The front binds its listeners only after its
	// first lists, so a port taken in between would otherwise fail it after a successful up.
	WaitReady bool
	// Ports, when set, are exactly the ports the process must listen on, all on loopback; status
	// reports any other listener, or a missing one, as a problem. frps's three (HBEP-6).
	Ports []int
	// Env, when set, is the process's whole environment instead of the supervisor's (childEnv).
	Env []string
	// HealthSocket, when set, is the Unix socket the process answers a ping on, in place of a
	// host:port /healthz: the relay keeper's (relaykeeper.go). Health is then empty.
	HealthSocket string
	// Setsid starts the process in a session of its own: the relay keeper, so no signal to the
	// supervisor's process group reaches it or the relays it starts.
	Setsid bool
	// Open, when set, is called before each start. Its files are passed to the process from
	// descriptor 3 on (exec.Cmd.ExtraFiles) and closed after the start: frps's verified binary, a
	// sealed copy, which Path names as /proc/self/fd/3, so the bytes run are the bytes checked
	// (HBEP-15).
	Open func() ([]*os.File, error)
	// StartGate, when set, is consulted before each start of the process, the first and every
	// restart: false keeps it from starting, and the reason is written to its record in
	// supervisor.json, where status reads it (state "gated"). The runner asks again every
	// SuperviseOptions.GatePoll and starts the process once the gate opens. The durable log's gate is
	// the free-space floor (eventlog.go): HBEP-8, A log refused for space.
	StartGate func() (ok bool, reason string)

	// startAfter, when set, holds the runner's first start until it is closed; settle, when set, is
	// called once the process is healthy, or will not start now (its gate is closed, it failed to
	// start, or the wait ran out), and closes the channel the others wait for. The log "starts
	// first": the other entries wait on it (Supervise).
	startAfter <-chan struct{}
	settle     func()
	// ready, when set, is what "healthy" means for settle, instead of GET /healthz alone: the log is
	// ready once its two streams exist and a client with its token connects (eventlog_ready.go),
	// which /healthz does not say.
	ready func() bool
}

// supervisedSet is what the supervisor runs, from the record the launcher wrote. The controller's
// command line is the contract with cmd/hpcburst-controller, and these are exactly its flags: it
// fills an agentapi.Host from --state-dir and --port-base, and takes the credentials directory and
// the agent builds from there; its kubeconfigs are the controller identities' (--kubeconfig and
// --provider-kubeconfig, issued at every start, controller_identities.go). The front flags go only to
// a launcher that recorded a front.
func (s *State) supervisedSet(l layout) ([]supervised, error) {
	if s.Controllers == nil {
		return nil, errors.New("the launcher record names no controller binary: nothing to supervise")
	}
	if s.Ports.Controller == 0 {
		return nil, errors.New("the launcher record has no controller port")
	}
	health := s.Ports.ControllerAddr()
	// The controller reads its own identities' kubeconfigs, by path: one for the lifecycle and verification
	// managers and the agent API, one for the Provider managers (HBEP-10, Audit; #660). Never a token.
	args := []string{"--state-dir", l.dir, "--port-base", strconv.Itoa(s.Ports.Base()), "--health-addr", health,
		"--kubeconfig", l.controllerKubeconfig(ControllerLifecycleIdentity),
		"--provider-kubeconfig", l.controllerKubeconfig(ControllerProviderIdentity)}
	if s.Front.Set() {
		args = append(args, "--front-mode", s.Front.Mode)
		// Host-dialed mode records no address: the host dials the jobs (HBEP-6, Dial-home modes).
		if s.Front.Addr != "" {
			args = append(args, "--front-addr", s.Front.Addr)
		}
	}
	// Beta, opt-in: the router each new Instance runs (#429, #454). Without it, the command line is
	// as before, byte for byte.
	if s.InstanceRouter != "" {
		args = append(args, "--instance-router", s.InstanceRouter)
	}
	set := []supervised{{Name: controllerEntryName, Path: s.Controllers.Binary, Args: args, Health: health, TermGrace: controllerTermGrace}}
	front, err := s.frontEntry(l)
	if err != nil {
		return nil, err
	}
	if front != nil {
		set = append(set, *front)
	}
	if frps := s.frpsEntry(l); frps != nil {
		set = append(set, *frps)
	}
	if bulk := s.frpsBulkEntry(l); bulk != nil {
		set = append(set, *bulk)
		// The controller serves frps-bulk's plugin path, reads its list and stages the second
		// certificate (HBEP-6, The bulk path); without the bulk path, its command line is as before,
		// byte for byte.
		set[0].Args = append(set[0].Args, "--bulk-path")
	}
	if keeper := s.keeperEntry(l); keeper != nil {
		// The controller hands its host-dialed relays to the keeper (HBEP-6, The relay keeper); with
		// no keeper, its command line is as before, byte for byte.
		set[0].Args = append(set[0].Args, "--relay-keeper-socket", keeper.HealthSocket)
		set = append(set, *keeper)
	}
	if log := s.eventLogEntry(l); log != nil {
		// The controller gets the token file, only while the recorded choice is on; without the log, its
		// command line is as before, byte for byte. The log is last in the slice, and first to start
		// and last to stop by the order Supervise gives it (a slice position decides neither).
		set[0].Args = append(set[0].Args, s.eventLogClientArgs(l)...)
		set = append(set, *log)
	}
	return set, nil
}

// backoff is the pure restart-delay rule: the delay after an exit, given how long the child ran.
type backoff struct {
	min, max, stable time.Duration
	cur              time.Duration
}

func (b *backoff) next(uptime time.Duration) time.Duration {
	if b.cur == 0 || uptime >= b.stable {
		b.cur = b.min
	} else if b.cur *= 2; b.cur > b.max {
		b.cur = b.max
	}
	return b.cur
}

// Entry states in supervisor.json.
const (
	entryStarting = "starting" // nothing started yet
	entryRunning  = "running"
	entryBackoff  = "backoff" // exited; waiting to be started again
	entryStopped  = "stopped" // the supervisor was told to stop
	// entryGated is an entry whose start gate is closed: it is not running, on purpose, and the
	// reason is in EntryRecord.Gate. status reports it as degraded, not as a fault (the durable log
	// refused for space, HBEP-8).
	entryGated = "gated"
)

const supervisorSchema = 1

// SupervisorFile is supervisor.json, the supervisor's record of its children, rewritten atomically
// (mode 0600) at every start and exit. It carries a full identity per child, so that status can
// check each one through a pidfd instead of trusting the file.
type SupervisorFile struct {
	Schema     int           `json:"schema"`
	Supervisor ProcRec       `json:"supervisor"`
	UpdatedAt  time.Time     `json:"updated_at"`
	Entries    []EntryRecord `json:"entries"`
}

// EntryRecord is one supervised process.
type EntryRecord struct {
	Name      string      `json:"name"`
	Health    string      `json:"health"`
	State     string      `json:"state"`
	Proc      *ProcRec    `json:"proc,omitempty"` // the current child; set only while it runs
	StartedAt *time.Time  `json:"started_at,omitempty"`
	Restarts  int         `json:"restarts"` // starts after the first
	LastExit  *ExitRecord `json:"last_exit,omitempty"`
	// Ports are exactly the ports the process must listen on (supervised.Ports); none means only
	// the health port is checked.
	Ports []int `json:"ports,omitempty"`
	// HealthSocket is the Unix socket of a process whose health is a ping (supervised.HealthSocket):
	// the relay keeper. Such a process must listen on no port.
	HealthSocket string `json:"health_socket,omitempty"`
	// Gate is why a gated entry is not started (state entryGated); empty otherwise.
	Gate string `json:"gate,omitempty"`
}

// ExitRecord is how the previous child ended.
type ExitRecord struct {
	Code   int       `json:"code"`   // the exit code; -1 when a signal ended it, or it never started
	Detail string    `json:"detail"` // "exit status 3", "signal: killed", "start failed: ..."
	At     time.Time `json:"at"`
}

func readSupervisorFile(l layout) (*SupervisorFile, error) {
	b, err := os.ReadFile(l.supervisorFile())
	if err != nil {
		return nil, err
	}
	var f SupervisorFile
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("%s is not valid: %w", l.supervisorFile(), err)
	}
	if f.Schema != supervisorSchema {
		return nil, fmt.Errorf("%s has schema %d, this launcher reads schema %d", l.supervisorFile(), f.Schema, supervisorSchema)
	}
	return &f, nil
}

// reporter keeps supervisor.json current. Its writers are the entries' goroutines.
type reporter struct {
	mu  sync.Mutex
	l   layout
	log io.Writer
	f   SupervisorFile
}

func (r *reporter) update(name string, fn func(e *EntryRecord)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.f.Entries {
		if r.f.Entries[i].Name == name {
			fn(&r.f.Entries[i])
		}
	}
	r.writeLocked()
}

func (r *reporter) writeLocked() {
	r.f.UpdatedAt = time.Now().UTC()
	b, err := json.MarshalIndent(r.f, "", "  ")
	if err == nil {
		err = writeFileAtomic(r.l.supervisorFile(), append(b, '\n'), 0o600)
	}
	if err != nil {
		// Not fatal to the children, but loud: up and status judge from this file.
		logf(r.log, "cannot write %s: %v", r.l.supervisorFile(), err)
	}
}

// SuperviseOptions are the inputs of Supervise. The durations default to the policy above; tests
// shorten them.
type SuperviseOptions struct {
	StateDir    string
	BackoffMin  time.Duration
	BackoffMax  time.Duration
	StableAfter time.Duration
	TermGrace   time.Duration
	Log         io.Writer
	// Handoff is the read end of the launcher's handoff pipe (OpenHandoff). Supervise starts
	// nothing until it has read the launcher's confirmation, and closes Handoff afterwards. It is
	// required: a supervisor with no launcher to confirm it would be one nothing could account for.
	Handoff io.ReadCloser
	// HandoffTimeout bounds that wait; the default is DefaultHandoffTimeout.
	HandoffTimeout time.Duration
	// GatePoll is how often a closed start gate is asked again; the default is DefaultGatePoll.
	GatePoll time.Duration
	// EventLogStartWait bounds how long the controllers and the front wait for the log to be healthy
	// before they start (with the log on); the default is DefaultEventLogStartWait.
	EventLogStartWait time.Duration
	// onDelay, set by tests only, sees every restart delay as it is chosen.
	onDelay func(name string, delay time.Duration)
	// disk, set by tests only, replaces the reads of the free space and the sizes behind the log's
	// start gate (nil reads the file system).
	disk diskReader
}

func (o *SuperviseOptions) defaults() {
	for _, d := range []struct {
		v *time.Duration
		d time.Duration
	}{{&o.BackoffMin, DefaultBackoffMin}, {&o.BackoffMax, DefaultBackoffMax}, {&o.StableAfter, DefaultStableAfter}, {&o.TermGrace, DefaultTermGrace}, {&o.HandoffTimeout, DefaultHandoffTimeout},
		{&o.GatePoll, DefaultGatePoll}, {&o.EventLogStartWait, DefaultEventLogStartWait}} {
		if *d.v <= 0 {
			*d.v = d.d
		}
	}
}

// childEnv is the environment of a supervised process: the supervisor's, without the variables
// that would point a Kubernetes client at some other API server. The controller reads the
// kubeconfig in its --state-dir, and the invoking user's own KUBECONFIG must never replace it.
func childEnv(env []string) []string {
	var out []string
	for _, kv := range env {
		switch k, _, _ := strings.Cut(kv, "="); k {
		case "KUBECONFIG", "KUBERNETES_SERVICE_HOST", "KUBERNETES_SERVICE_PORT":
		default:
			out = append(out, kv)
		}
	}
	return out
}

// Supervise runs the processes the launcher recorded and restarts any that exits, until ctx is
// done. Then it stops each child (SIGTERM through its verified pidfd, SIGKILL after the grace
// period), waits for them, and returns nil. It is the body of "hpcburst supervise".
func Supervise(ctx context.Context, o SuperviseOptions) error {
	o.defaults()
	if o.Handoff == nil {
		return errors.New("no handoff from the launcher: only hpcburst up starts a supervisor")
	}
	// Nothing is read, written or started before the launcher confirms. The descriptor is closed
	// at once, success or not, so no process started below can inherit it.
	logf(o.Log, "supervisor pid %d waits for the launcher to record it", os.Getpid())
	err := awaitHandoff(ctx, o.Handoff, o.HandoffTimeout)
	_ = o.Handoff.Close()
	if err != nil {
		return err
	}
	l, err := newLayout(o.StateDir)
	if err != nil {
		return err
	}
	st, err := l.loadState()
	if err != nil {
		return err
	}
	if st == nil {
		return fmt.Errorf("%w (%s)", errNotRunning, l.dir)
	}
	specs, err := st.supervisedSet(l)
	if err != nil {
		return err
	}
	self, err := newRec(supervisorName, os.Getpid())
	if err != nil {
		return err
	}
	for _, d := range []string{l.logDir(), l.controllerDir()} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return err
		}
	}
	rep := &reporter{l: l, log: o.Log, f: SupervisorFile{Schema: supervisorSchema, Supervisor: self}}
	for _, sp := range specs {
		rep.f.Entries = append(rep.f.Entries, EntryRecord{Name: sp.Name, Health: sp.Health, State: entryStarting, Ports: sp.Ports})
		rep.f.Entries[len(rep.f.Entries)-1].HealthSocket = sp.HealthSocket
	}
	rep.mu.Lock()
	rep.writeLocked()
	rep.mu.Unlock()
	logf(o.Log, "supervisor pid %d runs %d process(es)", self.PID, len(specs))

	env := childEnv(os.Environ())
	// The durable log starts first and stops last (HBEP-8, The durable log; HBEP-11). It starts first
	// by a barrier: every other runner waits until the log is healthy, or will not start now (its
	// gate is closed, it failed to start, or EventLogStartWait is over), so nothing waits on a log
	// that does not come. Its gate is the free-space floor.
	for i := range specs {
		if specs[i].Name != eventLogEntryName {
			continue
		}
		disk := o.disk
		if disk == nil {
			disk = osDisk{}
		}
		specs[i].StartGate = st.eventLogGate(l, disk)
		barrier := newStartBarrier()
		specs[i].settle = func() { barrier.release() }
		for j := range specs {
			if j != i {
				specs[j].startAfter = barrier.ch
			}
		}
		// The timer is the barrier's bound. It logs only when it is what releases the barrier, so a log
		// that became healthy, was gated or exited in time leaves no line (Fable's review of #639).
		timer := time.AfterFunc(o.EventLogStartWait, func() {
			if barrier.release() {
				logf(o.Log, "%s: not healthy within %s: starting the controllers and the front without waiting for it", eventLogEntryName, o.EventLogStartWait)
			}
		})
		defer timer.Stop()
	}
	runInStopOrder(ctx, specs, func(ctx context.Context, sp supervised) {
		(&runner{l: l, spec: sp, env: env, rep: rep, o: o}).run(ctx)
	}, func(stage string) { logf(o.Log, "supervisor: stopping %s", stage) })
	logf(o.Log, "supervisor stopped")
	return nil
}

// startBarrier is a one-shot signal: release closes ch, once, and reports whether this call did.
type startBarrier struct {
	once sync.Once
	ch   chan struct{}
}

func newStartBarrier() *startBarrier { return &startBarrier{ch: make(chan struct{})} }

func (s *startBarrier) release() (released bool) {
	s.once.Do(func() { close(s.ch); released = true })
	return released
}

// runInStopOrder runs each spec's runner until ctx is done, then stops them in HBEP-11's order (on down
// and upgrade, #553): the front first, waiting until its runner has returned, which is once its process
// has exited (its shutdown grace, its cuts and its usage records, within FrontTermGrace's SIGKILL), then
// every other process together: frps and the bulk frps, the relay keeper, the controller. So the front
// cuts its requests in flight itself, as HBEP-5 says, before frps closes their tunnels. With the durable
// log (beta) a third stage follows, the log alone, waited for, so the front's and the controllers' last
// events and usage records reach it (HBEP-11, Single-user host). A set with no front stops all at once,
// the log still last. Until ctx is done each runner restarts its process as before. logStage, when set,
// is told each stage as it starts.
func runInStopOrder(ctx context.Context, specs []supervised, run func(context.Context, supervised), logStage func(string)) {
	frontCtx, stopFront := context.WithCancel(context.WithoutCancel(ctx))
	restCtx, stopRest := context.WithCancel(context.WithoutCancel(ctx))
	logCtx, stopLog := context.WithCancel(context.WithoutCancel(ctx))
	defer stopFront()
	defer stopRest()
	defer stopLog()
	var front, rest, durable sync.WaitGroup
	hasFront, hasLog := false, false
	for _, sp := range specs {
		wg, c := &rest, restCtx
		switch sp.Name {
		case frontEntryName:
			wg, c, hasFront = &front, frontCtx, true
		case eventLogEntryName:
			wg, c, hasLog = &durable, logCtx, true
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			run(c, sp)
		}()
	}
	<-ctx.Done()
	if hasFront {
		if logStage != nil {
			logStage("the front first")
		}
		stopFront()
		front.Wait()
	}
	if logStage != nil {
		logStage("the tunnel processes and the controllers")
	}
	stopRest()
	rest.Wait()
	if hasLog {
		if logStage != nil {
			logStage("the event log last")
		}
		stopLog()
		durable.Wait()
	}
}

// runner keeps one supervised process running.
type runner struct {
	l    layout
	spec supervised
	env  []string
	rep  *reporter
	o    SuperviseOptions
}

// start launches the child with its own pidfd. Pdeathsig makes the kernel kill it if the
// supervisor is killed, so a supervisor that dies hard leaves no controller behind.
func (r *runner) start() (*procHandle, *exec.Cmd, error) {
	lf, err := os.OpenFile(r.l.childLog(r.spec.Name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, nil, err
	}
	defer lf.Close()
	env := r.env
	if r.spec.Env != nil {
		env = r.spec.Env
	}
	argv0 := r.spec.Path
	if r.spec.Argv0 != "" {
		argv0 = r.spec.Argv0
	}
	cmd := &exec.Cmd{Path: r.spec.Path, Args: append([]string{argv0}, r.spec.Args...), Env: env, Dir: r.l.dir, Stdout: lf, Stderr: lf}
	if r.spec.Open != nil {
		files, err := r.spec.Open()
		if err != nil {
			return nil, nil, err
		}
		defer func() {
			for _, f := range files {
				f.Close()
			}
		}()
		cmd.ExtraFiles = files
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	if r.spec.Setsid {
		// The relay keeper only (supervised.Setsid): a session of its own.
		cmd.SysProcAttr.Setsid = true
	}
	h, err := startProc(r.spec.Name, cmd)
	return h, cmd, err
}

func (r *runner) run(ctx context.Context) {
	b := &backoff{min: r.o.BackoffMin, max: r.o.BackoffMax, stable: r.o.StableAfter}
	name := r.spec.Name
	stopped := func() { r.rep.update(name, func(e *EntryRecord) { e.State, e.Proc, e.Gate = entryStopped, nil, "" }) }
	if r.spec.startAfter != nil {
		// The durable log starts first: wait for it, or for the supervisor to stop.
		select {
		case <-r.spec.startAfter:
		case <-ctx.Done():
			stopped()
			return
		}
	}
	if r.spec.settle != nil {
		defer r.spec.settle() // whatever ends this runner, nobody waits on it any longer
	}
	// pause waits out the restart delay; false means the supervisor was told to stop meanwhile.
	pause := func(uptime time.Duration) bool {
		d := b.next(uptime)
		if r.o.onDelay != nil {
			r.o.onDelay(name, d)
		}
		logf(r.o.Log, "%s: restarting in %s", name, d)
		select {
		case <-time.After(d):
			return true
		case <-ctx.Done():
			return false
		}
	}
	everStarted := false
	for ctx.Err() == nil {
		if !r.passGate(ctx) {
			break
		}
		h, cmd, err := r.start()
		if err != nil {
			logf(r.o.Log, "%s: cannot start: %v", name, err)
			if r.spec.settle != nil {
				r.spec.settle()
			}
			r.rep.update(name, func(e *EntryRecord) {
				e.State, e.Proc = entryBackoff, nil
				e.LastExit = &ExitRecord{Code: -1, Detail: "start failed: " + err.Error(), At: time.Now().UTC()}
			})
			if !pause(0) {
				break
			}
			continue
		}
		started := time.Now()
		rec := h.rec
		r.rep.update(name, func(e *EntryRecord) {
			t := started.UTC()
			e.State, e.Proc, e.StartedAt, e.Gate = entryRunning, &rec, &t, ""
			if everStarted {
				e.Restarts++
			}
		})
		everStarted = true
		logf(r.o.Log, "%s: started as pid %d", name, rec.PID)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		if r.spec.settle != nil {
			// The runner the others wait for: they may start once its process is healthy.
			go func() {
				waitHealthzOK(r.spec.Health, r.spec.ready, done, ctx.Done())
				r.spec.settle()
			}()
		}
		select {
		case <-done:
		case <-ctx.Done():
			r.terminate(h, done)
		}
		var ps *os.ProcessState // nil if terminate gave up waiting: Wait may still be running
		select {
		case <-done:
			ps = cmd.ProcessState
		default:
		}
		h.Close()
		r.rep.update(name, func(e *EntryRecord) {
			e.State, e.Proc = entryBackoff, nil
			e.LastExit = &ExitRecord{Code: ps.ExitCode(), Detail: ps.String(), At: time.Now().UTC()}
		})
		logf(r.o.Log, "%s: pid %d exited: %s", name, rec.PID, ps)
		if ctx.Err() != nil || !pause(time.Since(started)) {
			break
		}
	}
	stopped()
}

// passGate consults the entry's start gate before a start. With no gate it returns true at once. A
// gate that is closed puts the entry in state gated, with the reason in its record, and the runner
// asks again every GatePoll; it returns true once the gate opens, and false when the supervisor is
// told to stop first. The reason is written when it changes, and the controllers and the front are
// not held back for a gated log.
func (r *runner) passGate(ctx context.Context) bool {
	gate := r.spec.StartGate
	if gate == nil {
		return true
	}
	name, last := r.spec.Name, ""
	for {
		ok, reason := gate()
		if ok {
			return true
		}
		if reason != last {
			last = reason
			logf(r.o.Log, "%s: not started: %s", name, reason)
			r.rep.update(name, func(e *EntryRecord) { e.State, e.Proc, e.Gate = entryGated, nil, reason })
		}
		if r.spec.settle != nil {
			r.spec.settle()
		}
		select {
		case <-time.After(r.o.GatePoll):
		case <-ctx.Done():
			return false
		}
	}
}

// waitHealthzOK returns once GET /healthz on addr answers 200 and ready, when set, holds, or one of
// the channels closes.
func waitHealthzOK(addr string, ready func() bool, stop ...<-chan struct{}) {
	for {
		if _, err := healthz(addr, time.Second); err == nil && (ready == nil || ready()) {
			return
		}
		for _, c := range stop {
			select {
			case <-c:
				return
			default:
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// terminate stops a child on shutdown: SIGTERM through the verified pidfd, SIGKILL after the grace
// period, then a bounded wait for the exit.
func (r *runner) terminate(h *procHandle, done <-chan struct{}) {
	name := r.spec.Name
	grace := r.spec.TermGrace
	if grace <= 0 {
		grace = r.o.TermGrace
	}
	if _, err := signalHandle(h, syscall.SIGTERM); err != nil {
		logf(r.o.Log, "%s: SIGTERM to pid %d: %v", name, h.rec.PID, err)
	}
	select {
	case <-done:
		return
	case <-time.After(grace):
	}
	logf(r.o.Log, "%s: pid %d is still running %s after SIGTERM: sending SIGKILL", name, h.rec.PID, grace)
	if _, err := signalHandle(h, syscall.SIGKILL); err != nil {
		logf(r.o.Log, "%s: SIGKILL to pid %d: %v", name, h.rec.PID, err)
	}
	select {
	case <-done:
	case <-time.After(killWait):
		logf(r.o.Log, "%s: pid %d is still alive %s after SIGKILL", name, h.rec.PID, killWait)
	}
}

// parsePort returns the port of a host:port address.
func parsePort(addr string) (int, error) {
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(p)
	if err != nil || n < 1 || n > 65535 {
		return 0, fmt.Errorf("%q is not a port", p)
	}
	return n, nil
}
