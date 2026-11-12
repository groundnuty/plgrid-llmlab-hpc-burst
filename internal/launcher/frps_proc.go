package launcher

// The frps process: start it detached and record it by PID plus start time, find it again later
// through handle.go's pidfd rules, and stop it (SIGTERM, then SIGKILL after a grace period, then
// reap). #61's supervisor calls these in the follow-up that registers frps as a supervised entry;
// nothing here depends on it.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"syscall"
	"time"
)

// FrpsProcName is the name of frps's ProcRec.
const FrpsProcName = "frps"

// frpsKillWait is how long a stop waits for frps to disappear after SIGKILL, and for its parent
// to reap it once it has.
const frpsKillWait = 10 * time.Second

// ErrFrpsReleased means that this FrpsProc's pidfd is closed, by Release or by an earlier Stop,
// so nothing can be signalled through it: the kernel may have given its descriptor number to
// another process. Nothing was signalled. StopFrps with Rec() finds frps again from its record.
var ErrFrpsReleased = errors.New("this frps handle is released, so its pidfd is closed and nothing is signalled through it; use StopFrps with the record")

// ErrFrpsForeign means that the recorded PID is alive but belongs to another process (its start
// time or boot ID differs from the record: the PID was reused). Nothing was signalled.
var ErrFrpsForeign = errors.New("the recorded pid belongs to another process, which was not signalled")

// FrpsSpec is everything one frps start needs.
type FrpsSpec struct {
	Bin    FrpsBinary // from ResolveFrps; the caller closes it after the start
	Config string     // the rendered configuration (WriteFrpsConfig)
	Log    string     // frps's standard output and error are appended here (FrpsLogFile)
	Dir    string     // the working directory
	Nice   int        // run under nice(1) with this value when not zero
	// ExitWithParent makes frps die with the process that starts it: the kernel sends it SIGKILL
	// when that process ends, however it ends (killed, or a test timeout's panic, which runs no
	// cleanup). Without it frps is detached and outlives its starter, which is what Release and
	// StopFrps with a record are for, and what the supervisor's own start (supervise.go) does not
	// rely on. The test rigs set it, so that a killed test binary leaves no frps on the rig's fixed
	// ports for the next rig to find (#484). It is off by default, so nothing that starts frps to
	// have it outlive the caller changes.
	ExitWithParent bool
}

// Argv is the full command. frps runs the descriptor of the verified binary (execPath), with
// strict configuration parsing on explicitly, so a mistyped key is an error and never a setting
// that silently stays at its default (a mistyped bindAddr would listen on every interface). With
// a nice value the command is wrapped in nice(1), which execs into frps, so the PID is still
// frps's.
func (s FrpsSpec) Argv() []string {
	a := []string{execPath(), "-c", s.Config, "--strict-config=true"}
	if s.Nice != 0 {
		a = append([]string{niceBin, "-n", strconv.Itoa(s.Nice)}, a...)
	}
	return a
}

// Env is the whole environment of frps: nothing is inherited, so no proxy variable of the
// launcher's user can reach it.
func (s FrpsSpec) Env() []string { return []string{"PATH=/usr/bin:/bin"} }

// FrpsProc is an frps started by this process: the held pidfd, and the Wait that reaps it.
type FrpsProc struct {
	h    *procHandle
	cmd  *exec.Cmd
	done chan struct{} // closed once cmd.Wait has returned
}

// StartFrps starts frps detached from the caller (own session, no stdin, output to the log) and
// returns without waiting for it to listen. The pidfd comes from clone itself, so it exists
// before the child could be reaped, and the identity (start time, boot ID) is read while it is
// held. The caller records Rec() before anything else, so that a crash from here on can still be
// cleaned up by StopFrps. A goroutine waits for the child, so it never stays a zombie while this
// process lives; Done reports that.
func StartFrps(s FrpsSpec) (*FrpsProc, error) {
	if s.Bin.file == nil {
		return nil, errors.New("the frps binary was not opened and verified")
	}
	if s.Config == "" || s.Log == "" || s.Dir == "" {
		return nil, errors.New("frps needs a configuration file, a log file and a working directory")
	}
	if _, err := os.Stat(s.Config); err != nil {
		return nil, fmt.Errorf("frps configuration: %w", err)
	}
	if s.Nice != 0 {
		if _, err := os.Stat(niceBin); err != nil {
			return nil, fmt.Errorf("a nice value needs %s: %w", niceBin, err)
		}
	}
	lf, err := os.OpenFile(s.Log, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	defer lf.Close()
	argv := s.Argv()
	cmd := &exec.Cmd{Path: argv[0], Args: argv, Env: s.Env(), Dir: s.Dir, Stdout: lf, Stderr: lf,
		ExtraFiles: []*os.File{s.Bin.file}}
	if s.Nice == 0 {
		cmd.Args[0] = s.Bin.Path // what ps shows; the file executed is still the descriptor
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	done := make(chan struct{})
	var h *procHandle
	if s.ExitWithParent {
		cmd.SysProcAttr.Pdeathsig = syscall.SIGKILL
		h, err = startOnLockedThread(FrpsProcName, cmd, done)
	} else {
		// startProc takes the pidfd from clone and records the identity while it is held; on a
		// failure it stops the child again and reaps it.
		if h, err = startProc(FrpsProcName, cmd); err == nil {
			go func() { _ = cmd.Wait(); close(done) }()
		}
	}
	if err != nil {
		return nil, err
	}
	return &FrpsProc{h: h, cmd: cmd, done: done}, nil
}

// startOnLockedThread is startProc for a child with Pdeathsig, which the kernel ties to the thread
// that created the child, not to the process: the child gets its signal when that thread exits
// (golang/go#27505). A goroutine's thread is the runtime's to end, and a goroutine that locks its
// thread and returns ends it. So the child is created, and then waited for, by one goroutine that
// locks its OS thread and never unlocks it: the thread lives until frps has exited and been reaped
// (done is closed then), and ends after it, when no child is left for the signal to reach. The
// caller's own goroutine and thread are free to move or end.
func startOnLockedThread(name string, cmd *exec.Cmd, done chan struct{}) (*procHandle, error) {
	type started struct {
		h   *procHandle
		err error
	}
	res := make(chan started, 1)
	go func() {
		runtime.LockOSThread()
		h, err := startProc(name, cmd)
		res <- started{h, err}
		if err != nil {
			return
		}
		_ = cmd.Wait()
		close(done)
	}()
	r := <-res
	return r.h, r.err
}

// Rec is the record of the process: its name, PID, start time and boot ID.
func (p *FrpsProc) Rec() ProcRec { return p.h.rec }

// Done is closed once frps has exited and been reaped.
func (p *FrpsProc) Done() <-chan struct{} { return p.done }

// ExitStatus describes how frps ended, once Done is closed ("exit status 1", "signal: terminated").
func (p *FrpsProc) ExitStatus() string {
	select {
	case <-p.done:
		return p.cmd.ProcessState.String()
	default:
		return "still running"
	}
}

// Release closes the pidfd without stopping frps: a caller that exits and leaves frps to be found
// again from its record (StopFrps). A Stop after Release fails with ErrFrpsReleased.
func (p *FrpsProc) Release() { p.h.Close() }

// Stop ends frps through the pidfd it holds, with the rules of StopFrps, and waits until it is
// reaped. It closes the handle.
func (p *FrpsProc) Stop(ctx context.Context, grace time.Duration) (FrpsStop, error) {
	if p.h.isClosed() {
		return FrpsStop{PID: p.h.rec.PID, Before: "released"}, ErrFrpsReleased
	}
	// Closed whether the stop succeeds or not: a second Stop gets ErrFrpsReleased, not a
	// descriptor number that may belong to someone else by then.
	defer p.h.Close()
	rep, err := frpsStopHeld(ctx, p.h, grace)
	if err != nil {
		return rep, err
	}
	select {
	case <-p.done:
	case <-time.After(frpsKillWait):
		return rep, fmt.Errorf("frps (pid %d) has exited, but was not reaped within %s", p.h.rec.PID, frpsKillWait)
	}
	return rep, nil
}

// FrpsStop reports one stop.
type FrpsStop struct {
	PID     int    `json:"pid"`
	Before  string `json:"before"`            // the verdict on the record: ours, gone, foreign or unknown
	Detail  string `json:"detail,omitempty"`  // why, when it was not ours
	Signals string `json:"signals,omitempty"` // "TERM", or "TERM+KILL"
	TookMS  int64  `json:"took_ms"`
}

// StopFrps stops the process in the record, and only that one. It takes a pidfd and verifies PID,
// start time and boot ID while holding it, before any signal. A process that no longer matches
// (the PID was reused) is never signalled: the stop fails with ErrFrpsForeign. So does one whose
// identity cannot be established (no pidfd, an unreadable boot ID), with another error. A
// process that is gone is a stop that succeeded. A verified one gets SIGTERM through its pidfd,
// and SIGKILL (verified again) after the grace period; a cancelled ctx cuts the grace period
// short, never the wait after SIGKILL. The exit is read from the pidfd. A caller that is not the
// process's parent does not reap it; that is its parent's job.
func StopFrps(ctx context.Context, rec ProcRec, grace time.Duration) (FrpsStop, error) {
	h, v, detail := openRec(rec)
	rep := FrpsStop{PID: rec.PID, Before: v.String(), Detail: detail}
	switch v {
	case Ours:
		defer h.Close()
		return frpsStopHeld(ctx, h, grace)
	case Gone:
		return rep, nil
	case Foreign:
		return rep, fmt.Errorf("not signalling %s: %w (%s)", rec.Name, ErrFrpsForeign, detail)
	}
	return rep, fmt.Errorf("not signalling %s: %s", rec.Name, detail)
}

func frpsStopHeld(ctx context.Context, h *procHandle, grace time.Duration) (rep FrpsStop, err error) {
	rep = FrpsStop{PID: h.rec.PID, Before: Ours.String()}
	start := time.Now()
	defer func() { rep.TookMS = time.Since(start).Milliseconds() }()
	one := []*procHandle{h}
	sent, err := signalHandle(h, syscall.SIGTERM)
	if err != nil {
		return rep, fmt.Errorf("SIGTERM to %s (pid %d): %w", h.rec.Name, h.rec.PID, err)
	}
	if sent {
		rep.Signals = "TERM"
	}
	if waitExited(ctx, one, grace) {
		return rep, nil
	}
	if sent, err = signalHandle(h, syscall.SIGKILL); err != nil {
		return rep, fmt.Errorf("SIGKILL to %s (pid %d): %w", h.rec.Name, h.rec.PID, err)
	} else if sent {
		rep.Signals += "+KILL"
	}
	// Not ctx: an interrupted stop still waits for what it killed to be gone.
	if !waitExited(context.Background(), one, frpsKillWait) {
		return rep, fmt.Errorf("%s (pid %d) is still alive %s after SIGKILL", h.rec.Name, h.rec.PID, frpsKillWait)
	}
	return rep, nil
}
