package rig

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// EngineCmd builds an engine double's command: the test binary as the engine (enginedouble's Dispatch
// in its TestMain), serving on socket, logging to log, with modes and the Instance key. The spike's
// non-test code never imports the double (internal/agent's imports guard): the test binary supplies it.
type EngineCmd func(socket, log, modes, apiKey string) *exec.Cmd

// Exit is one engine process's end.
type Exit struct {
	Instance int
	PID      int
	At       time.Time
	Signal   string // the signal that ended it, "" for an exit
	Code     int
	Stopped  bool // the rig stopped it; false: it died on its own
}

// Doubles are the deployment's engine doubles, one per Instance, each a process of its own.
type Doubles struct {
	dir   string
	cmd   EngineCmd
	modes []string
	keys  []string

	mu    sync.Mutex
	procs []*proc
	exits []Exit
	// onDeath, when set, is told of every process that ends without the rig stopping it.
	onDeath func(i int)
}

type proc struct {
	cmd     *exec.Cmd
	pidfd   int
	done    chan struct{}
	stopped bool
}

// NewDoubles prepares n doubles in dir, each with its modes and Instance key; none runs yet.
func NewDoubles(dir string, cmd EngineCmd, modes []string, keys []string) *Doubles {
	return &Doubles{dir: dir, cmd: cmd, modes: modes, keys: keys, procs: make([]*proc, len(modes))}
}

// Socket is Instance i's serving socket.
func (d *Doubles) Socket(i int) string { return filepath.Join(d.dir, fmt.Sprintf("d%d.sock", i)) }

// Log is Instance i's engine log, which every incarnation appends to.
func (d *Doubles) Log(i int) string { return filepath.Join(d.dir, fmt.Sprintf("d%d.log", i)) }

// Start starts Instance i's double and waits until its /health answers 200. A pidfd is opened while
// the child is still unreaped (its Wait has not run), so a later signal reaches it or nobody.
func (d *Doubles) Start(i int) error {
	c := d.cmd(d.Socket(i), d.Log(i), d.modes[i], d.keys[i])
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // a group of its own: never the test's
	if err := c.Start(); err != nil {
		return err
	}
	fd, err := unix.PidfdOpen(c.Process.Pid, 0)
	if err != nil {
		return fmt.Errorf("pidfd_open: %w", err)
	}
	p := &proc{cmd: c, pidfd: fd, done: make(chan struct{})}
	d.mu.Lock()
	d.procs[i] = p
	d.mu.Unlock()
	go d.wait(i, p)
	return waitHealthy(d.Socket(i), 30*time.Second)
}

func (d *Doubles) wait(i int, p *proc) {
	err := p.cmd.Wait()
	_ = unix.Close(p.pidfd)
	e := Exit{Instance: i, PID: p.cmd.Process.Pid, At: time.Now()}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			e.Signal = ws.Signal().String()
		} else {
			e.Code = ee.ExitCode()
		}
	}
	d.mu.Lock()
	e.Stopped = p.stopped
	d.exits = append(d.exits, e)
	death := d.onDeath
	d.mu.Unlock()
	close(p.done)
	if !e.Stopped && death != nil {
		death(i)
	}
}

// Alive reports whether Instance i's current process runs.
func (d *Doubles) Alive(i int) bool {
	d.mu.Lock()
	p := d.procs[i]
	d.mu.Unlock()
	if p == nil {
		return false
	}
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

// Stop ends Instance i's process through its pidfd (KILL) and waits for it.
func (d *Doubles) Stop(i int) {
	d.mu.Lock()
	p := d.procs[i]
	if p != nil {
		p.stopped = true
	}
	d.mu.Unlock()
	if p == nil {
		return
	}
	select {
	case <-p.done:
		return
	default:
	}
	_ = unix.PidfdSendSignal(p.pidfd, unix.SIGKILL, nil, 0)
	<-p.done
}

// StopAll stops every double.
func (d *Doubles) StopAll() {
	for i := range d.procs {
		d.Stop(i)
	}
}

// Exits is every process end so far.
func (d *Doubles) Exits() []Exit {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]Exit(nil), d.exits...)
}

// waitHealthy waits until the double on socket answers /health with 200.
func waitHealthy(socket string, within time.Duration) error {
	c := unixClient(socket)
	defer c.CloseIdleConnections()
	deadline := time.Now().Add(within)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://engine/health", nil)
		resp, err := c.Do(req)
		cancel()
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the double on %s is not healthy within %s: %v", socket, within, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func unixClient(socket string) *http.Client {
	return &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socket)
	}}}
}
