package keeperproc

// The tap: a fault-injecting Unix-socket proxy in the test process, between
// the real controllers and the keeper. Each controller's
// --relay-keeper-socket is the tap's path. The tap is neither a keeper nor a
// controller: it reads each request's line, records it with the PID of the
// process that connected (SO_PEERCRED) and the time, and by its rule passes
// it to the keeper's socket and the reply back, holds it (it forwards nothing
// until the test releases it), or drops it (closes the connection unanswered).
// It records each reply as the keeper wrote it, so a test reads the keeper's
// own reports, acknowledgements and refusals, by sender, never what a
// controller says of them.
//
// The wire is one exchange per connection, one JSON line each way
// (internal/relaykeeper/wire), so a request is one line and its reply one.
//
// Provenance: new, for #343's PR 4 (Stage B).

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// tapAction is what the tap does with one request.
type tapAction int

const (
	tapPass tapAction = iota
	tapHold
	tapDrop
)

// tapReq is one request the tap read, and what became of it.
type tapReq struct {
	n    int
	pid  int       // the sender's process, by SO_PEERCRED
	at   time.Time // when its line was read
	kind wire.Kind
	set  *wire.DesiredSet
	ack  *wire.Ack
	line []byte

	action tapAction
	// Once it is forwarded: forwarded is when it was sent to the keeper;
	// replyKind and the decoded reply are the keeper's; answered is when the
	// reply was written back to the sender, zero when that write failed
	// (the sender gave up) or the keeper did not answer.
	forwarded time.Time
	replyKind wire.Kind
	report    *wire.Report
	acked     *wire.Acked
	refused   *wire.Refused
	err       string
	// replying is when the tap began to write the reply back: its sender
	// cannot have read it sooner.
	replying time.Time
	answered time.Time
	done     bool

	conn    *net.UnixConn
	release chan tapAction
}

// tap is the proxy.
type tap struct {
	t        *testing.T
	path     string
	upstream string

	mu    sync.Mutex
	ln    *net.UnixListener
	reqs  []*tapReq
	rule  func(r *tapReq) tapAction
	conns sync.WaitGroup
}

// newTap is a tap whose socket is at a path of its own, toward the keeper's
// socket upstream; it does not listen until start.
func newTap(t *testing.T, upstream string) *tap {
	t.Helper()
	dir, err := os.MkdirTemp("", "kt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	p := &tap{t: t, path: filepath.Join(dir, "k.sock"), upstream: upstream}
	if len(p.path) > agentapi.MaxRelaySocketPath {
		t.Fatalf("the tap's socket path is %d bytes, over %d: set a shorter TMPDIR", len(p.path), agentapi.MaxRelaySocketPath)
	}
	t.Cleanup(func() {
		// At the end the socket goes, nothing more is held, and every held
		// request is let go unanswered.
		p.stop()
		p.mu.Lock()
		p.rule = func(*tapReq) tapAction { return tapDrop }
		for _, r := range p.reqs {
			if r.action == tapHold && !r.done {
				select {
				case r.release <- tapDrop:
				default:
				}
			}
		}
		p.mu.Unlock()
		p.conns.Wait()
	})
	return p
}

// start listens on the tap's path.
func (p *tap) start() {
	p.t.Helper()
	ln, err := wire.Listen(p.path)
	if err != nil {
		p.t.Fatalf("the tap's socket: %v", err)
	}
	p.mu.Lock()
	p.ln = ln
	p.mu.Unlock()
	go func() {
		for {
			c, err := ln.AcceptUnix()
			if err != nil {
				return
			}
			p.conns.Add(1)
			go func() {
				defer p.conns.Done()
				p.serve(c)
			}()
		}
	}()
}

// stop closes the tap's socket, which removes it from its path: a
// controller's keeper socket is then missing.
func (p *tap) stop() {
	p.mu.Lock()
	ln := p.ln
	p.ln = nil
	p.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
}

// setUpstream points the tap at another keeper socket.
func (p *tap) setUpstream(sock string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.upstream = sock
}

// setRule sets what the tap does with each request from now on; nil passes
// everything.
func (p *tap) setRule(rule func(r *tapReq) tapAction) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rule = rule
}

// peerPID is the connecting process's PID, by SO_PEERCRED.
func peerPID(c *net.UnixConn) (int, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return 0, err
	}
	var cred *syscall.Ucred
	var cerr error
	if err := raw.Control(func(fd uintptr) {
		cred, cerr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return 0, err
	}
	if cerr != nil {
		return 0, cerr
	}
	return int(cred.Pid), nil
}

func (p *tap) serve(c *net.UnixConn) {
	defer c.Close()
	pid, err := peerPID(c)
	if err != nil {
		p.t.Errorf("the tap: SO_PEERCRED: %v", err)
		return
	}
	_ = c.SetReadDeadline(time.Now().Add(wire.ExchangeTimeout))
	line, err := wire.NewReader(c).ReadSlice('\n')
	if err != nil {
		return
	}
	r := &tapReq{pid: pid, at: time.Now(), line: append([]byte(nil), line...), conn: c, release: make(chan tapAction, 1)}
	var env struct {
		Kind wire.Kind       `json:"kind"`
		Body json.RawMessage `json:"body"`
	}
	if json.Unmarshal(line, &env) == nil {
		r.kind = env.Kind
		switch env.Kind {
		case wire.KindSet:
			r.set = &wire.DesiredSet{}
			_ = json.Unmarshal(env.Body, r.set)
		case wire.KindAck:
			r.ack = &wire.Ack{}
			_ = json.Unmarshal(env.Body, r.ack)
		}
	}
	// The rule is applied under the tap's lock, so the end's release of
	// every held request sees each one's action (a rule never takes the
	// tap's lock).
	p.mu.Lock()
	r.n = len(p.reqs)
	p.reqs = append(p.reqs, r)
	act := tapPass
	if p.rule != nil {
		act = p.rule(r)
	}
	r.action = act
	p.mu.Unlock()
	if act == tapHold {
		act = <-r.release
	}
	if act == tapPass {
		p.forward(r)
	}
	p.mu.Lock()
	r.done = true
	p.mu.Unlock()
}

// forward sends r's line to the keeper and its reply back to r's sender.
func (p *tap) forward(r *tapReq) {
	p.mu.Lock()
	up := p.upstream
	p.mu.Unlock()
	var d net.Dialer
	d.Timeout = wire.ExchangeTimeout
	kc, err := d.Dial("unix", up)
	p.mu.Lock()
	r.forwarded = time.Now()
	p.mu.Unlock()
	if err != nil {
		p.note(r, "dial the keeper: "+err.Error())
		return
	}
	defer kc.Close()
	_ = kc.SetDeadline(time.Now().Add(wire.ExchangeTimeout))
	if _, err := kc.Write(r.line); err != nil {
		p.note(r, "write to the keeper: "+err.Error())
		return
	}
	reply, err := wire.NewReader(kc).ReadSlice('\n')
	if err != nil {
		p.note(r, "the keeper's reply: "+err.Error())
		return
	}
	var env struct {
		Kind wire.Kind       `json:"kind"`
		Body json.RawMessage `json:"body"`
	}
	if err := json.Unmarshal(reply, &env); err != nil {
		p.note(r, "the keeper's reply: "+err.Error())
		return
	}
	p.mu.Lock()
	r.replyKind = env.Kind
	switch env.Kind {
	case wire.KindReport:
		r.report = &wire.Report{}
		_ = json.Unmarshal(env.Body, r.report)
	case wire.KindAcked:
		r.acked = &wire.Acked{}
		_ = json.Unmarshal(env.Body, r.acked)
	case wire.KindRefused:
		r.refused = &wire.Refused{}
		_ = json.Unmarshal(env.Body, r.refused)
	}
	p.mu.Unlock()
	p.mu.Lock()
	r.replying = time.Now()
	p.mu.Unlock()
	_ = r.conn.SetWriteDeadline(time.Now().Add(wire.ExchangeTimeout))
	if _, err := r.conn.Write(reply); err != nil {
		p.note(r, "write the reply to the sender: "+err.Error())
		return
	}
	p.mu.Lock()
	r.answered = time.Now()
	p.mu.Unlock()
}

func (p *tap) note(r *tapReq, s string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r.err = s
}

// let releases a held request with act: tapPass forwards it, tapDrop closes
// it unanswered. It returns once the tap is done with it.
func (p *tap) let(t *testing.T, r *tapReq, act tapAction) tapReq {
	t.Helper()
	p.mu.Lock()
	held := r.action == tapHold && !r.done
	p.mu.Unlock()
	if !held {
		t.Fatalf("the tap's request %d is not held", r.n)
	}
	r.release <- act
	var out tapReq
	waitFor(t, 30*time.Second, "the tap done with a released request", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		out = *r
		return r.done
	})
	return out
}

// requests is a copy of every request so far that pred accepts.
func (p *tap) requests(pred func(r *tapReq) bool) []tapReq {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []tapReq
	for _, r := range p.reqs {
		if pred == nil || pred(r) {
			out = append(out, *r)
		}
	}
	return out
}

// waitReq waits until a request that pred accepts is in the tap's record,
// read under the tap's lock, and returns the first one; its reply may not be
// in yet.
func (p *tap) waitReq(t *testing.T, d time.Duration, what string, pred func(r *tapReq) bool) *tapReq {
	t.Helper()
	var found *tapReq
	waitFor(t, d, what, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, r := range p.reqs {
			if pred(r) {
				found = r
				return true
			}
		}
		return false
	})
	return found
}

// snapshot is a copy of r, read under the tap's lock.
func (p *tap) snapshot(r *tapReq) tapReq {
	p.mu.Lock()
	defer p.mu.Unlock()
	return *r
}

// The predicates the tests use.

// from is a request from the process pid.
func from(pid int) func(r *tapReq) bool { return func(r *tapReq) bool { return r.pid == pid } }

// setFrom is a set from pid that the keeper answered with a report.
func reportedSetFrom(pid int) func(r *tapReq) bool {
	return func(r *tapReq) bool { return r.pid == pid && r.set != nil && r.report != nil }
}
