package fakeslurm

// New. Run mode: how a process the fake starts (a batch shell, a step) is tied
// to the life of the process that hosts the fake, so a test that dies without
// running its cleanups (SIGKILL, SIGTERM from timeout(1), a -test.timeout
// panic, an out-of-memory kill) does not leave job shells looping for days
// (issue #274).

import (
	"os/exec"
	"runtime"
	"sync"
	"syscall"
)

// sessionAttr is how every process the fake starts is placed: in a session of
// its own, which is what the sweeps select by, and with SIGKILL as its parent-
// death signal, so the kernel ends it when the process that started it dies.
// Close, scancel and the time limit do the orderly ending; this is only for the
// host that never reaches them.
//
// It reaches the process itself, not what that process started: the kernel
// clears the signal on fork. What a job's batch shell started in the background
// is ended by the job's keeper (keeper.go), and what a step's program started
// is not: it outlives a host that died without Close, and only Close's sweep
// ends it.
func sessionAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true, Pdeathsig: syscall.SIGKILL}
}

// startReq is a process to start on the starter's thread.
type startReq struct {
	cmd  *exec.Cmd
	done chan error
}

var (
	starterOnce sync.Once
	starterReqs chan startReq
)

// startProc starts cmd on a thread that lives as long as the host process.
//
// A parent-death signal is sent when the thread that created the process
// exits, not when the process does (go.dev/issue/27505). The Go runtime ends a
// thread only when a goroutine that locked it exits without unlocking, which
// nothing here does, but a caller may: a goroutine that called
// runtime.LockOSThread and returns, with a job's sbatch among its calls, would
// otherwise kill the job. So every process is created on one goroutine that
// locks its thread for good. That thread dies with the host, and then the
// kernel signals the processes it created.
func startProc(cmd *exec.Cmd) error {
	starterOnce.Do(func() {
		starterReqs = make(chan startReq)
		go func() {
			runtime.LockOSThread() // never unlocked: see startProc
			for r := range starterReqs {
				r.done <- r.cmd.Start()
			}
		}()
	})
	done := make(chan error, 1)
	starterReqs <- startReq{cmd: cmd, done: done}
	return <-done
}
