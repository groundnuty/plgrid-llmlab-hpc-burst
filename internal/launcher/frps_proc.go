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
	pidfd := -1
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, PidFD: &pidfd}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start frps: %w", err)
	}
	p := &FrpsProc{cmd: cmd, done: make(chan struct{})}
	go func() { _ = cmd.Wait(); close(p.done) }()
	if pidfd < 0 {
		// Not reaped yet, so the PID is still the child's and this kill cannot reach another process.
		_ = cmd.Process.Kill()
		return nil, errors.New("the kernel gave no pidfd for frps, so its identity could not be pinned; it was stopped again")
	}
	p.h = &procHandle{rec: ProcRec{Name: FrpsProcName, PID: cmd.Process.Pid}, fd: pidfd}
	if _, err := p.h.record(); err != nil {
		_ = pidfds.signal(p.h.fd, syscall.SIGKILL)
		p.h.Close()
		return nil, fmt.Errorf("record the identity of frps: %w", err)
	}
	return p, nil
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
