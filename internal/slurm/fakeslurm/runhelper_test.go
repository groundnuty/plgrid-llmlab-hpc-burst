package fakeslurm

// New. A helper process for the run-mode tests, run by the test binary itself
// (TestMain): a program in a process group of its own, as hb-agent starts its
// engine, which a TERM sent to the job's process group would not reach.

import (
	"os"
	ossignal "os/signal"
	"strconv"
	"syscall"
	"time"
)

// setpgidTerm moves itself into a process group of its own, writes "<pid>
// <pgid>" to pidFile, and waits: a TERM makes it write "term" to termFile and
// exit, and with none it exits after two minutes.
func setpgidTerm(pidFile, termFile string) int {
	if err := syscall.Setpgid(0, 0); err != nil {
		return 2
	}
	term := make(chan os.Signal, 1)
	ossignal.Notify(term, syscall.SIGTERM)
	line := strconv.Itoa(os.Getpid()) + " " + strconv.Itoa(syscall.Getpgrp()) + "\n"
	if os.WriteFile(pidFile+".tmp", []byte(line), 0o644) != nil || os.Rename(pidFile+".tmp", pidFile) != nil {
		return 2
	}
	select {
	case <-term:
		if os.WriteFile(termFile+".tmp", []byte("term\n"), 0o644) != nil || os.Rename(termFile+".tmp", termFile) != nil {
			return 2
		}
		return 0
	case <-time.After(2 * time.Minute):
		return 0
	}
}
