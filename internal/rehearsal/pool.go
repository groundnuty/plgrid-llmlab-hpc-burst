package rehearsal

// New, split from sshd.go: the pool of sshds behind a node's listener, one per
// connection, and the stop that ends them and everything recorded under them
// (issue #308: guard 1 is in spawn; guards 2 and 3 are below and signalProcs).

import (
	"cmp"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

// sshdProc is one sshd the node started.
type sshdProc struct {
	cmd  *exec.Cmd
	self procRow
	done chan struct{}
	// walk lists the processes below a root: below, or a test's stand-in.
	walk func(root procRow) []procRow
	// below are the processes seen under it while it lived; mu guards them.
	mu    sync.Mutex
	below map[int]procRow
}

// record adds the processes below the sshd now. Once the sshd is reaped its
// PID may name another process, so nothing is walked from it (issue #308);
// below also refuses a root whose start time no longer matches.
func (p *sshdProc) record() {
	select {
	case <-p.done:
		return
	default:
	}
	walk := p.walk
	if walk == nil {
		walk = below
	}
	rows := walk(p.self)
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range rows {
		p.below[r.pid] = r
	}
}

// seen is what was recorded below the sshd, deepest first, by the parents
// it was recorded with: a process is signalled before its parent, so the stop
// never orphans what it has yet to signal (a session process whose sshd it
// has just ended would leave the test's tree). Ties go to the later start.
func (p *sshdProc) seen() []procRow {
	p.mu.Lock()
	defer p.mu.Unlock()
	depth := map[int]int{}
	var depthOf func(r procRow, n int) int
	depthOf = func(r procRow, n int) int {
		if d, ok := depth[r.pid]; ok {
			return d
		}
		d := 1
		if parent, ok := p.below[r.ppid]; ok && n < maxDepth {
			d = depthOf(parent, n+1) + 1
		}
		depth[r.pid] = d
		return d
	}
	rows := make([]procRow, 0, len(p.below))
	for _, r := range p.below {
		depthOf(r, 0)
		rows = append(rows, r)
	}
	slices.SortFunc(rows, func(a, b procRow) int {
		if c := cmp.Compare(depth[b.pid], depth[a.pid]); c != 0 {
			return c
		}
		return cmp.Compare(b.ticks, a.ticks)
	})
	return rows
}

// sshdPool starts an sshd per accepted connection and stops them all.
type sshdPool struct {
	bin, conf string
	log       *os.File
	// name is the node's, for a report: "login node" or "gateway".
	name string
	// api reads /proc and sends every signal of the pool.
	api *procAPI
	// walk is below and signal is signalProcs, guards 2 and 3; a test
	// replaces one to show what the others do alone.
	walk   func(root procRow) []procRow
	signal func(api *procAPI, node string, rows []procRow, sig syscall.Signal) ([]procRow, error)
	// wait bounds each of stop's two waits.
	wait time.Duration

	mu     sync.Mutex
	procs  []*sshdProc
	closed bool
	wg     sync.WaitGroup
}

func newPool(name, bin, conf string, log *os.File) *sshdPool {
	return &sshdPool{name: name, bin: bin, conf: conf, log: log, api: hostProcs(), walk: below, signal: signalProcs, wait: stopWait}
}

// guards fills in what a pool built by hand (tests) leaves unset with the
// guarded defaults, never with something that skips a guard.
func (s *sshdPool) guards() {
	if s.walk == nil {
		s.walk = below
	}
	if s.signal == nil {
		s.signal = signalProcs
	}
	if s.wait <= 0 {
		s.wait = stopWait
	}
	if s.name == "" {
		s.name = "node"
	}
}

// serve hands each connection the listener accepts to a new sshd, until the
// listener closes.
func (s *sshdPool) serve(ln net.Listener) {
	defer s.wg.Done()
	s.mu.Lock()
	s.guards()
	s.mu.Unlock()
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		s.spawn(c.(*net.TCPConn))
	}
}

// recordEvery is how often a live sshd's descendants are recorded. It is a
// sampling interval, not a wait: Close also records them once more.
const recordEvery = 50 * time.Millisecond

func (s *sshdPool) spawn(c *net.TCPConn) {
	defer c.Close()
	f, err := c.File()
	if err != nil {
		return
	}
	defer f.Close()
	cmd := exec.Command(s.bin, "-i", "-e", "-f", s.conf)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = f, f, s.log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if err := cmd.Start(); err != nil {
		s.mu.Unlock()
		return
	}
	pid := cmd.Process.Pid
	if s.api.started != nil {
		s.api.started(pid)
	}
	// The sshd is this process's child and unreaped, so its PID is still its
	// own. An sshd that has already exited (a client gone before auth) has no
	// live row: a zero or unreadable row is never stored, and nothing is ever
	// walked from it (issue #308). It is ended through its os.Process, which
	// holds a pidfd, and reaped here.
	self, live, err := s.api.row(pid)
	if err != nil || !live || self.pid != pid || self.ticks == 0 {
		s.mu.Unlock()
		fmt.Fprintf(s.log, "rehearsal: sshd %d had no live row when it was read (live %v, error %v): killed and reaped, not recorded\n", pid, live, err)
		if kerr := s.api.kill(cmd.Process); kerr != nil {
			fmt.Fprintf(s.log, "rehearsal: kill sshd %d: %v\n", pid, kerr)
		}
		_ = cmd.Wait()
		return
	}
	p := &sshdProc{cmd: cmd, self: self, done: make(chan struct{}), walk: s.walk, below: map[int]procRow{}}
	s.procs = append(s.procs, p)
	s.wg.Add(2)
	s.mu.Unlock()
	go func() { defer s.wg.Done(); _ = cmd.Wait(); close(p.done) }()
	go func() {
		defer s.wg.Done()
		tick := time.NewTicker(recordEvery)
		defer tick.Stop()
		for {
			p.record()
			select {
			case <-p.done:
				return
			case <-tick.C:
			}
		}
	}()
}

// stopWait is how long the TERM phase lasts before the KILL, and how long
// stop then waits for the pool's goroutines.
const stopWait = 5 * time.Second

// stop ends every sshd and everything seen under it. The caller has stopped
// the listener and the relay, so the forced commands end on their own; TERM
// then KILL, by pidfd and start time, take what does not, each process before
// its parent (extra first, then each sshd's recorded rows deepest first, then
// the sshd). The processes signalProcs refused are not waited for, and every
// refusal and leak is the error.
//
// It always returns, within twice s.wait and the time its reads take. The
// pool's own sshds are known for certain through cmd.Process, which holds a
// pidfd, so the KILL phase ends each of them that way whatever signalProcs
// decided. A goroutine of the pool still running after the second wait, and a
// recorded process still alive then (one stuck in the kernel), are errors,
// not a hang and not a nil.
func (s *sshdPool) stop(extra []procRow) error {
	s.mu.Lock()
	s.closed = true
	s.guards()
	procs := append([]*sshdProc(nil), s.procs...)
	s.mu.Unlock()
	all := append([]procRow(nil), extra...)
	for _, p := range procs {
		p.record()
		all = append(all, p.seen()...)
		if p.self.pid != 0 {
			all = append(all, p.self)
		}
	}
	var errs []error
	kept, err := s.signal(s.api, s.name, all, syscall.SIGTERM)
	errs = append(errs, err)
	deadline := time.Now().Add(s.wait)
	for anyLive(kept) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	kept, err = s.signal(s.api, s.name, kept, syscall.SIGKILL)
	errs = append(errs, err, s.killOwn(procs), s.waitIdle(deadline.Add(s.wait)))
	for time.Now().Before(deadline.Add(s.wait)) && anyLive(kept) {
		time.Sleep(20 * time.Millisecond)
	}
	errs = append(errs, s.survivors(kept))
	return errors.Join(errs...)
}

// survivors is the error for the signalled processes still alive at the
// stop's last deadline, named by command and start time.
func (s *sshdPool) survivors(kept []procRow) error {
	var left []string
	for _, r := range kept {
		if now, ok := readProcRow(r.pid); ok && now.ticks == r.ticks {
			left = append(left, fmt.Sprintf("%d (%s, started at tick %d)", r.pid, s.api.comm(r.pid), r.ticks))
		}
	}
	if len(left) == 0 {
		return nil
	}
	return fmt.Errorf("rehearsal: the %s's processes still alive %v after the KILL: %s (issue #308)", s.name, s.wait, strings.Join(left, ", "))
}

// killOwn ends each of the pool's sshds that has not been reaped through its
// os.Process, which holds a pidfd: the pool's own children, whatever
// signalProcs decided about them.
func (s *sshdPool) killOwn(procs []*sshdProc) error {
	var errs []error
	for _, p := range procs {
		select {
		case <-p.done:
			continue
		default:
		}
		if p.cmd == nil || p.cmd.Process == nil {
			continue // a test's stand-in
		}
		if err := s.api.kill(p.cmd.Process); err != nil {
			errs = append(errs, fmt.Errorf("rehearsal: kill sshd %d through its process: %w", p.cmd.Process.Pid, err))
		}
	}
	return errors.Join(errs...)
}

// waitIdle waits until the pool's goroutines have ended, or until the
// deadline, which is an error rather than a hang.
func (s *sshdPool) waitIdle(deadline time.Time) error {
	idle := make(chan struct{})
	go func() { s.wg.Wait(); close(idle) }()
	select {
	case <-idle:
		return nil
	case <-time.After(time.Until(deadline)):
		return fmt.Errorf("rehearsal: an sshd or a goroutine of its pool was still running %v after the KILL", s.wait)
	}
}
