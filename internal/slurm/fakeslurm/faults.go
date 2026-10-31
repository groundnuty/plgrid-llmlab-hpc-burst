package fakeslurm

// New. Faults (FailNext), scripted sbatch outcomes (Script) and gates (Gate).
// S4 drove the same cases with environment variables and marker files
// (FAKE_SLURM_SBATCH_FAIL, _FAIL_ONCE, _LOST_REPLY_ONCE, _GATE).

import (
	"context"
	"sync"
	"time"
)

// Fault is a command that FailNext refused. It never ran, so it is not in
// Calls.
type Fault struct {
	User string
	// Words are the first simple command's words, when the line parses.
	Words []string
	// Err is the runner error the command got.
	Err  error
	Time time.Time
}

type faultRule struct {
	session *Session // nil: any session
	left    int
	err     error
}

// FailNext makes the next n commands, from any Session, return err as the
// runner error without running. Use slurm.ErrNotStarted for a refused login,
// slurm.ErrTransport or slurm.ErrTimeout otherwise. Each ErrNotStarted failure
// bumps the Generation of the Session it hit, as a reconnect would.
func (p *Platform) FailNext(n int, err error) { p.addRule(nil, n, err) }

// FailNext is Platform.FailNext for this Session only: another Session's
// commands pass, so two providers' masters can fail apart.
func (s *Session) FailNext(n int, err error) { s.p.addRule(s, n, err) }

func (p *Platform) addRule(s *Session, n int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rules = append(p.rules, &faultRule{session: s, left: n, err: err})
}

// takeFault consumes one pending fault for s's command, if any.
func (p *Platform) takeFault(s *Session, line string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range p.rules {
		if r.left <= 0 || (r.session != nil && r.session != s) {
			continue
		}
		r.left--
		f := Fault{User: s.user, Err: r.err, Time: p.clock.Now()}
		if cmds, err := parseLine(line); err == nil && len(cmds) > 0 {
			f.Words = cmds[0].callWords()
		}
		p.faulted = append(p.faulted, f)
		if isNotStarted(r.err) {
			s.gen.Add(1)
		}
		return r.err
	}
	return nil
}

// Faulted returns the commands that FailNext refused, oldest first.
func (p *Platform) Faulted() []Fault {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Fault(nil), p.faulted...)
}

// ---- scripted sbatch outcomes ----

type stepKind int

const (
	stepAccept stepKind = iota
	stepReject
	stepTransient
	stepLostReply
	stepUnparseable
	stepRename
	stepAsTimeLimit
	stepAdminHold
)

// SubmitStep is the outcome of one sbatch call, scripted with Script.
type SubmitStep struct {
	kind       stepKind
	reason     string
	registered bool
	n          int
	err        error
}

// Accept is the default outcome: the job is registered and its ID printed.
func Accept() SubmitStep { return SubmitStep{kind: stepAccept} }

// Reject refuses the submission with a reason: stderr "sbatch: error: Batch
// job submission failed: <reason>", exit 1, nothing registered.
func Reject(reason string) SubmitStep { return SubmitStep{kind: stepReject, reason: reason} }

// Transient fails with "Socket timed out on send/recv operation", exit 1. With
// registered true slurmctld has registered the job anyway: the reply was lost.
func Transient(registered bool) SubmitStep {
	return SubmitStep{kind: stepTransient, registered: registered}
}

// LostReply registers the job and then returns err as the runner error (for
// example slurm.ErrTimeout or slurm.ErrTransport), with no result: the
// command may have run, the caller cannot tell.
func LostReply(err error) SubmitStep { return SubmitStep{kind: stepLostReply, err: err} }

// Unparseable registers the job but prints garbage and exits 0.
func Unparseable() SubmitStep { return SubmitStep{kind: stepUnparseable} }

// Rename registers the job under its name cut to its first maxLen characters.
func Rename(maxLen int) SubmitStep { return SubmitStep{kind: stepRename, n: maxLen} }

// AsTimeLimit registers the job with another time limit, in minutes.
func AsTimeLimit(minutes int) SubmitStep { return SubmitStep{kind: stepAsTimeLimit, n: minutes} }

// AdminHold registers the job on an admin hold (JobHeldAdmin), whatever
// --hold said.
func AdminHold() SubmitStep { return SubmitStep{kind: stepAdminHold} }

// Script queues sbatch outcomes, consumed one per sbatch call in order. With
// the queue empty a call is accepted.
func (p *Platform) Script(steps ...SubmitStep) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.script = append(p.script, steps...)
}

// nextStep pops the next scripted outcome; the caller holds mu.
func (p *Platform) nextStep() SubmitStep {
	if len(p.script) == 0 {
		return Accept()
	}
	s := p.script[0]
	p.script = p.script[1:]
	return s
}

// ---- gates ----

// Gate holds one command until Release, so a test can order races.
type Gate struct {
	blocked, release       chan struct{}
	blockOnce, releaseOnce sync.Once
}

// Gate blocks the next command whose line starts with word, until Release.
// Several gates for one word hold the next commands in the order they were
// set. A gate stays until a command meets it.
func (p *Platform) Gate(word string) *Gate {
	g := &Gate{blocked: make(chan struct{}), release: make(chan struct{})}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gates[word] = append(p.gates[word], g)
	return g
}

// Wait returns once a command is blocked at the gate.
func (g *Gate) Wait() { <-g.blocked }

// Blocked is closed once a command is blocked at the gate.
func (g *Gate) Blocked() <-chan struct{} { return g.blocked }

// Release lets the held command go. Released before a command arrives, the
// gate lets that command pass at once.
func (g *Gate) Release() { g.releaseOnce.Do(func() { close(g.release) }) }

func (g *Gate) markBlocked() { g.blockOnce.Do(func() { close(g.blocked) }) }

// waitGate holds the command at the first gate for word, outside the lock. An
// ended context frees it with an error, and the command does not run.
func (p *Platform) waitGate(ctx context.Context, word string) error {
	p.mu.Lock()
	var g *Gate
	if gs := p.gates[word]; len(gs) > 0 {
		g, p.gates[word] = gs[0], gs[1:]
	}
	p.mu.Unlock()
	if g == nil {
		return nil
	}
	g.markBlocked()
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctxErr(ctx)
	}
}
