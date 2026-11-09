package fakeslurm

// New. Run mode: the keeper, which ends everything a job left running when the
// process that hosts the fake dies without Close (issue #288).
//
// The parent-death signal of procstart.go reaches the batch shell only: the
// kernel clears it on fork, so a background child the shell started (a loop, a
// long sleep) outlives a host that was killed. Nothing the host does can run
// after SIGKILL, so the work is left to a helper that does not depend on the
// host: one sh per job, the keeper, in a session of its own, with no
// parent-death signal. Its stdin is a pipe whose only writer is the host. The
// kernel closes the host's descriptors when the host ends, however it ends, and
// the keeper reads that as the end: it kills every process of the job's
// session.
//
// The host talks to it in lines. The first is the batch shell's PID, which is
// the job's session ID. A second line says the job ended in order, and the
// keeper exits. End of input before the second line means the host died.
// Close, scancel, the time limit and a job that ends by itself all go through
// wait (runwait.go), which sends the second line and reaps the keeper before
// it reaps the batch shell, so a Close that returns leaves no keeper. The order
// matters: the keeper kills by session ID, the shell's reap frees that ID, and
// a keeper still armed then could kill an unrelated session that took it.
//
// Limits: a process that called setsid leaves the job's session and is not
// found; the keeper finds the job's processes by session ID alone, as the
// sweeps do, and it signals by PID where they use a pidfd, so a process of the
// job that exits and whose PID is reused between the keeper's listing and its
// kill would be signalled by mistake. That takes a wrap of the PID space inside
// one pass, and happens only after a host died. A step's program is covered by
// the parent-death signal alone, and what a step left running is not.

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// keeperScript is the keeper: POSIX sh, builtins only but sleep. Fields 1 to 4
// of what follows the command name in /proc/<pid>/stat are state, parent,
// process group and session; the command name may hold spaces, parentheses
// and newlines, so the whole record is read (a single read would stop at the
// name's newline) and cut at its last ") ". A zombie (Z) or a dead process
// (X) is not counted, since KILL cannot end it and its parent reaps it. The
// pass count bounds the loop: two quiet passes in a row end it, 400 passes
// (about 20 s) end it anyway.
const keeperScript = `read -r sid || exit 0
case $sid in ''|*[!0-9]*) exit 0;; esac
read -r _ && exit 0
quiet=0 pass=0
while [ "$quiet" -lt 2 ] && [ "$pass" -lt 400 ]; do
	pass=$((pass + 1)) hit=
	for st in /proc/[0-9]*/stat; do
		line=
		while IFS= read -r l; do line="$line $l"; done < "$st"
		[ -n "$line" ] || continue
		set -- ${line##*) }
		if [ "$4" = "$sid" ] && [ "$1" != Z ] && [ "$1" != X ]; then
			pid=${st#/proc/}
			kill -KILL "${pid%/stat}" && hit=1
		fi
	done 2>/dev/null
	if [ -n "$hit" ]; then quiet=0; else quiet=$((quiet + 1)); sleep 0.05; fi
done
`

// keeperStopWait bounds how long stop waits for a keeper to exit after the
// second line; it exits at once, so only a stuck process reaches the bound.
const keeperStopWait = 10 * time.Second

// keeper is a job's keeper as the host holds it.
type keeper struct {
	cmd  *exec.Cmd
	w    *os.File // the only writer of the keeper's stdin
	once sync.Once
}

// startKeeper starts the keeper of a job whose batch shell has not started
// yet; dir is its working directory and must exist.
func startKeeper(dir string) (*keeper, error) {
	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("keeper: %w", err)
	}
	cmd := exec.Command(jobShell, "-c", keeperScript, "keeper")
	cmd.Dir = dir
	cmd.Env = []string{"PATH=" + defaultPath}
	cmd.Stdin = pr
	// Its own session, so that a signal to the host's process group does not
	// take it; no parent-death signal, which would end it with the host.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	err = cmd.Start()
	pr.Close() // the keeper holds its own copy; keeping this one would leak a descriptor per job
	if err != nil {
		pw.Close()
		return nil, fmt.Errorf("keeper: %w", err)
	}
	return &keeper{cmd: cmd, w: pw}, nil
}

// attach tells the keeper which session is the job's. An error means the
// keeper is already gone, so the job would not be kept.
func (k *keeper) attach(sid int) error {
	if _, err := io.WriteString(k.w, strconv.Itoa(sid)+"\n"); err != nil {
		return fmt.Errorf("keeper: %w", err)
	}
	return nil
}

// stop ends the keeper in order, without it killing anything, and reaps it.
// It is safe to call more than once and on a nil keeper. A write that fails
// because the keeper is gone is not an error here, and a keeper that does not
// exit within keeperStopWait is killed.
func (k *keeper) stop() {
	if k == nil {
		return
	}
	k.once.Do(func() {
		_, _ = io.WriteString(k.w, "end\n")
		_ = k.w.Close()
		done := make(chan struct{})
		go func() { _ = k.cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(keeperStopWait):
			_ = k.cmd.Process.Kill()
			<-done
		}
	})
}
