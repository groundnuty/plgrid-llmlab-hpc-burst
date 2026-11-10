package rehearsal

// New. Issue #308, D2 (the science agent's ruling, 10 Nov 2026): a forced
// command asks for SIGKILL at its sshd's exit, so it does not outlive its
// sshd's session process for longer than the kernel takes to deliver that
// signal.
//
// The forced command is this binary, exec'd by the login shell in sshd's
// session process (command="exec env ... <binary>", login.go). It starts no
// process of its own: it dials the node's relay socket and copies bytes. At its
// start it asks the kernel for SIGKILL when its parent exits. That parent is
// sshd's session process, which is single-threaded, so its exit is the
// thread's. Then it exits at once unless the parent is still an sshd: a parent
// that exited before the request was made has left the forced command to PID 1
// or a subreaper such as the user manager, and the request would never fire.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// init runs before the tests' TestMain, while the main goroutine is locked to
// the main thread. A parent-death signal belongs to the thread that asks for
// it, and the main thread lives as long as the process.
func init() {
	if os.Getenv(EnvRole) != RoleRelay {
		return
	}
	if err := relayGuard(parentDeathKill, os.Getppid, readComm); err != nil {
		os.Exit(guardFail(os.Getenv(EnvRelay), err))
	}
}

// parentDeathKill asks for SIGKILL when this thread's parent exits.
func parentDeathKill() error {
	return unix.Prctl(unix.PR_SET_PDEATHSIG, uintptr(syscall.SIGKILL), 0, 0, 0)
}

// relayGuard ties the forced command to its sshd, and says why when it will
// not run. The request comes first and the parent is read after it, so a
// parent that exited before the request is seen.
func relayGuard(deathSignal func() error, getppid func() int, comm func(int) string) error {
	ask := func() error {
		if err := deathSignal(); err != nil {
			return fmt.Errorf("ask for SIGKILL at the parent's exit: %w (issue #308)", err)
		}
		return nil
	}
	check := func() error {
		ppid := getppid()
		if ppid <= 1 {
			return fmt.Errorf("the parent is PID %d: the sshd exited before this forced command could tie itself to it (issue #308)", ppid)
		}
		if c := comm(ppid); !strings.HasPrefix(c, "sshd") {
			return fmt.Errorf("the parent %d is %q, not an sshd: it exited before this forced command could tie itself to it (issue #308)", ppid, c)
		}
		return nil
	}
	first, second := ask, check
	if err := first(); err != nil {
		return err
	}
	return second()
}

// guardFail reports a forced command that will not run, and returns its exit
// code. Standard error goes back over ssh, where the controller reads the 255
// as ssh's own failure, so the line is also appended to the node's sshd.log,
// beside the relay socket, when that log exists.
func guardFail(sock string, err error) int {
	if strings.HasPrefix(sock, "/") {
		if f, ferr := os.OpenFile(filepath.Join(filepath.Dir(sock), "sshd.log"), os.O_WRONLY|os.O_APPEND, 0); ferr == nil {
			fmt.Fprintf(f, "rehearsal login node: forced command %d: %v\n", os.Getpid(), err)
			f.Close()
		}
	}
	return clientFail("%v", err)
}
