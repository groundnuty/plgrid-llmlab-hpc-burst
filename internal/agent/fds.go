package agent

import (
	"os"
	"strconv"
	"syscall"
)

// CloseInheritedFDs marks every file descriptor above standard error
// close-on-exec, so no process the agent starts (Apptainer, the engine, ssh)
// inherits one the agent was started with. Go's exec passes a child only its
// standard streams and ExtraFiles, but an inherited descriptor without
// close-on-exec, such as a lock a parent held, stays open in every child,
// and an orphaned child then holds it after the agent is gone. The agent
// calls it once at its start.
func CloseInheritedFDs() error {
	d, err := os.Open("/proc/self/fd")
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, n := range names {
		fd, err := strconv.Atoi(n)
		if err != nil || fd <= 2 {
			continue
		}
		syscall.CloseOnExec(fd)
	}
	return nil
}
