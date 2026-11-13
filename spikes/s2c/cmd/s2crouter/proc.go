package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Signals, through pidfds only (never by name, pattern or process group, and never to a bare PID). A
// process is signalled only if it is the job's own: the root must be a child of the caller's parent
// (job.sh, which started it and recorded its PID; the probe and `s2crouter signal|stop` are job.sh's
// children too), and every other member must descend from it. Each member's pidfd is opened
// (os.FindProcess opens one on Linux) and its start time read again AFTER the open: a PID reused by
// another process between the listing and the open fails that check and is left alone, and once
// open, the pidfd pins the process, whatever later happens to the number.

type member struct {
	pid   int
	start uint64
	proc  *os.Process
}

// procStat reads a process's state, parent and start time from /proc/<pid>/stat.
func procStat(pid int) (state byte, ppid int, start uint64, err error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, 0, 0, err
	}
	i := bytes.LastIndexByte(b, ')')
	if i < 0 {
		return 0, 0, 0, fmt.Errorf("/proc/%d/stat: no comm", pid)
	}
	f := strings.Fields(string(b[i+1:]))
	if len(f) < 20 {
		return 0, 0, 0, fmt.Errorf("/proc/%d/stat: %d fields", pid, len(f))
	}
	ppid, _ = strconv.Atoi(f[1])
	start, err = strconv.ParseUint(f[19], 10, 64) // field 22 of stat: starttime
	return f[0][0], ppid, start, err
}

// openMembers lists root and, with tree, every descendant, and opens a verified pidfd for each.
func openMembers(root int, tree bool) ([]member, error) {
	_, ppid, start, err := procStat(root)
	if err != nil {
		return nil, fmt.Errorf("process %d: %w", root, err)
	}
	if ppid != os.Getppid() {
		return nil, fmt.Errorf("process %d is not the job's own: its parent is %d, the job is %d", root, ppid, os.Getppid())
	}
	listed := []member{{pid: root, start: start}}
	if tree {
		children := map[int][]member{}
		ents, err := os.ReadDir("/proc")
		if err != nil {
			return nil, err
		}
		for _, e := range ents {
			p, err := strconv.Atoi(e.Name())
			if err != nil {
				continue
			}
			if _, pp, st, err := procStat(p); err == nil {
				children[pp] = append(children[pp], member{pid: p, start: st})
			}
		}
		for i := 0; i < len(listed); i++ {
			listed = append(listed, children[listed[i].pid]...)
		}
	}
	var out []member
	for _, m := range listed {
		p, err := os.FindProcess(m.pid)
		if err != nil {
			continue
		}
		if _, _, st, err := procStat(m.pid); err != nil || st != m.start {
			p.Release() // gone, or the number now names another process
			continue
		}
		m.proc = p
		out = append(out, m)
	}
	return out, nil
}

// signalMembers sends sig to every member through its pidfd; an ended member is no error.
func signalMembers(ms []member, sig syscall.Signal) error {
	var first error
	for _, m := range ms {
		if err := m.proc.Signal(sig); err != nil && !errors.Is(err, os.ErrProcessDone) && first == nil {
			first = fmt.Errorf("process %d: %w", m.pid, err)
		}
	}
	return first
}

// running reports whether the member still runs: present, not a zombie, and the same process.
func running(m member) bool {
	state, _, st, err := procStat(m.pid)
	return err == nil && st == m.start && state != 'Z'
}

func release(ms []member) {
	for _, m := range ms {
		m.proc.Release()
	}
}

var signalNames = map[string]syscall.Signal{"TERM": syscall.SIGTERM, "KILL": syscall.SIGKILL, "INT": syscall.SIGINT}

// signalMain is `s2crouter signal -sig TERM|KILL|INT [-tree] PID`.
func signalMain(args []string) int {
	fs := flag.NewFlagSet("signal", flag.ContinueOnError)
	name := fs.String("sig", "TERM", "TERM, KILL or INT")
	tree := fs.Bool("tree", false, "the process and its descendants")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	sig, ok := signalNames[*name]
	pid, err := strconv.Atoi(fs.Arg(0))
	if !ok || err != nil || fs.NArg() != 1 || pid <= 1 {
		return fail("signal: want -sig TERM|KILL|INT [-tree] <pid>")
	}
	ms, err := openMembers(pid, *tree)
	if err != nil {
		return fail("signal: %v", err)
	}
	defer release(ms)
	if err := signalMembers(ms, sig); err != nil {
		return fail("signal: %v", err)
	}
	return 0
}

// stopMain is `s2crouter stop [-grace 15s] PID`: TERM to the process and its descendants, then, once
// grace has passed with the process still running, KILL to every member still running, each through
// the pidfd opened at the start.
func stopMain(args []string) int {
	fs := flag.NewFlagSet("stop", flag.ContinueOnError)
	grace := fs.Duration("grace", 15*time.Second, "time between TERM and KILL")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	pid, err := strconv.Atoi(fs.Arg(0))
	if err != nil || fs.NArg() != 1 || pid <= 1 {
		return fail("stop: want [-grace d] <pid>")
	}
	ms, err := openMembers(pid, true)
	if err != nil {
		return fail("stop: %v", err)
	}
	defer release(ms)
	_ = signalMembers(ms, syscall.SIGTERM)
	deadline := time.Now().Add(*grace)
	for running(ms[0]) && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond) // polling the root's state, bounded by grace
	}
	var left []member
	for _, m := range ms {
		if running(m) {
			left = append(left, m)
		}
	}
	_ = signalMembers(left, syscall.SIGKILL)
	return 0
}

// killTree is the probe's kill of the victim: SIGKILL to its whole tree, verified as above.
func killTree(pid int) error {
	ms, err := openMembers(pid, true)
	if err != nil {
		return err
	}
	defer release(ms)
	return signalMembers(ms, syscall.SIGKILL)
}
