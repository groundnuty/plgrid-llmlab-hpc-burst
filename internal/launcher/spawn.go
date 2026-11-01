package launcher

import (
	"fmt"
	"os/exec"
	"syscall"
)

// startProc starts cmd and returns a handle on the child: the pidfd comes from clone itself, so it
// exists before the child could be reaped, and the identity (start time, boot ID) is read while it
// is held. The caller sets everything else in cmd (session, environment, files), records the
// identity before anything else, and calls cmd.Wait: this function reaps only a child it stops
// again because it could not be pinned.
func startProc(name string, cmd *exec.Cmd) (*procHandle, error) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	pidfd := -1
	cmd.SysProcAttr.PidFD = &pidfd
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", name, err)
	}
	reap := func() { go func() { _ = cmd.Wait() }() }
	if pidfd < 0 {
		// Not reaped yet, so the PID is still the child's and this kill cannot reach another process.
		_ = cmd.Process.Kill()
		reap()
		return nil, fmt.Errorf("the kernel gave no pidfd for %s, so its identity could not be pinned; it was stopped again", name)
	}
	h := &procHandle{rec: ProcRec{Name: name, PID: cmd.Process.Pid}, fd: pidfd}
	if _, err := h.record(); err != nil {
		_ = pidfds.signal(h.fd, syscall.SIGKILL)
		h.Close()
		reap()
		return nil, fmt.Errorf("record the identity of %s: %w", name, err)
	}
	return h, nil
}
