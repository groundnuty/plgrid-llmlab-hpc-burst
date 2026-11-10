package rehearsal

// New. Issue #308, D2 (the science agent's ruling, 10 Nov 2026): a forced
// command never outlives its sshd, so it never leaves the test's tree, where
// the sweep could not signal it.
//
// The forced command is this binary, exec'd by the login shell in sshd's
// session process. It starts no process of its own: it dials the node's relay
// socket and copies bytes. At its start it asks the kernel for SIGKILL when
// its parent exits. That parent is sshd's session process, which is single-
// threaded, so its exit is the thread's. Then it exits at once unless the
// parent is still an sshd: a parent that exited before the request was made
// has left the forced command to PID 1 or the user manager, and the request
// would never fire.

import (
	"os"
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
	if code := relayGuard(parentDeathKill, os.Getppid, readComm); code != 0 {
		os.Exit(code)
	}
}

// parentDeathKill asks for SIGKILL when this thread's parent exits.
func parentDeathKill() error {
	return unix.Prctl(unix.PR_SET_PDEATHSIG, uintptr(syscall.SIGKILL), 0, 0, 0)
}

// relayGuard ties the forced command to its sshd, and returns 0 to go on or
// the code to exit with.
func relayGuard(deathSignal func() error, getppid func() int, comm func(int) string) int {
	if err := deathSignal(); err != nil {
		return clientFail("ask for SIGKILL at the parent's exit: %v (issue #308)", err)
	}
	ppid := getppid()
	if c := comm(ppid); !strings.HasPrefix(c, "sshd") {
		return clientFail("the parent %d is %q, not an sshd: it exited before this forced command could tie itself to it (issue #308)", ppid, c)
	}
	return 0
}
