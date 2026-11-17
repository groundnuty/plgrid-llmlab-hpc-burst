package fakeslurm

// New (beta; HBEP-4, Core operations, Estimate). "sbatch --test-only", the
// "env SLURM_TIME_FORMAT=%s" prefix the plugin puts before it, and the controls
// of its answer.
//
// The reply shape is real sbatch's: on standard error, exit 0,
//
//	sbatch: Job <next job ID> to start at <T> using <cpus> processors on nodes <n> in partition <p>
//
// where <T> is epoch seconds when SLURM_TIME_FORMAT=%s is set on the command,
// else the local time in Config.TimeZone as YYYY-MM-DDTHH:MM:SS with no zone
// marker, which is the form S7 and #661's probe of 16 Nov 2026 saw on Athena
// (Slurm 23.11.7). The job ID printed is the ID the next submission would get; it
// is not taken.

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// envVarTimeFormat is the one variable the plugin sets before sbatch, and
// timeFormatEpoch its one value the fake reads.
const (
	envVarTimeFormat = "SLURM_TIME_FORMAT"
	timeFormatEpoch  = "%s"
)

// textTimeLayout is the default form of a time in sbatch's reply: local, no zone.
const textTimeLayout = "2006-01-02T15:04:05"

var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type testOnlyMode int

const (
	toStart  testOnlyMode = iota // the default: a start, now unless SetTestOnlyStart says another
	toRaw                        // a token verbatim where the start goes
	toOutput                     // standard error verbatim, exit 0
	toFail                       // exit 1 with an error line
)

// testOnlyControls are the controls of sbatch --test-only's answer, under mu.
type testOnlyControls struct {
	mode  testOnlyMode
	start time.Time
	text  string // the token (toRaw), the whole standard error (toOutput) or the error text (toFail)
	gate  *Gate  // when set, the command waits at it, outside mu
}

// SetTestOnlyStart makes every later sbatch --test-only say the job starts at t:
// as epoch seconds when SLURM_TIME_FORMAT=%s is set on the command, else as local
// text in Config.TimeZone. Until one of the controls is called the start is the
// platform clock's now, an empty queue.
func (p *Platform) SetTestOnlyStart(t time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.to.mode, p.to.start, p.to.text = toStart, t.UTC(), ""
}

// SetTestOnlyRaw makes every later sbatch --test-only print token verbatim where
// the start goes, whatever SLURM_TIME_FORMAT says: a platform that ignores the
// variable (the text form), or one that prints something that is no whole number
// of seconds. The rest of the line is the real one's.
func (p *Platform) SetTestOnlyRaw(token string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.to.mode, p.to.text = toRaw, token
}

// SetTestOnlyOutput makes every later sbatch --test-only exit 0 and print text
// to standard error and nothing else: a reply with no "to start at" line.
func (p *Platform) SetTestOnlyOutput(text string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.to.mode, p.to.text = toOutput, text
}

// FailTestOnly makes every later sbatch --test-only exit 1 with "sbatch: error:
// <text>".
func (p *Platform) FailTestOnly(text string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.to.mode, p.to.text = toFail, text
}

// HangTestOnly makes every later sbatch --test-only wait at the returned gate,
// after it has been logged and with no lock held, until Release or until the
// call's context ends: a command that does not answer. A context that ends first
// fails the command with the runner error a time-out is (slurm.ErrTimeout for a
// deadline). The test waits on the gate's Blocked, never on a sleep.
func (p *Platform) HangTestOnly() *Gate {
	g := &Gate{blocked: make(chan struct{}), release: make(chan struct{})}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.to.gate = g
	return g
}

// splitEnv reads "env NAME=value... command args" and returns the assignments
// and the command's words.
func splitEnv(words []string) (assign, rest []string, ok bool) {
	if len(words) == 0 || words[0] != "env" {
		return nil, nil, false
	}
	i := 1
	for ; i < len(words); i++ {
		name, _, found := strings.Cut(words[i], "=")
		if !found || !envName.MatchString(name) {
			break
		}
	}
	return words[1:i], words[i:], true
}

// commandWords are a simple command's words without a leading "env NAME=value"
// prefix: the command the line runs.
func commandWords(words []string) []string {
	if _, rest, ok := splitEnv(words); ok && len(rest) > 0 {
		return rest
	}
	return words
}

// Command is the call's command words without a leading env prefix:
// ["sbatch", ...] for "env SLURM_TIME_FORMAT=%s sbatch ...".
func (c Call) Command() []string { return commandWords(c.Words) }

// TestOnly reports whether the call is an sbatch --test-only, which submits
// nothing. A submission, with or without an env prefix, is not.
func (c Call) TestOnly() bool {
	w := c.Command()
	return len(w) > 0 && w[0] == "sbatch" && slices.Contains(w[1:], "--test-only")
}

// CountTestOnly is the number of sbatch --test-only calls, by any user.
func (p *Platform) CountTestOnly() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, c := range p.calls {
		if c.TestOnly() {
			n++
		}
	}
	return n
}

// CountSubmits is the number of sbatch calls that are submissions, by any user:
// Count("sbatch") less the --test-only calls.
func (p *Platform) CountSubmits() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, c := range p.calls {
		if w := c.Command(); len(w) > 0 && w[0] == "sbatch" && !c.TestOnly() {
			n++
		}
	}
	return n
}

// env is "env SLURM_TIME_FORMAT=%s sbatch [options]": the one prefix the plugin
// builds, since a Command carries no environment. Any other variable, value or
// command is refused, so a change in the dialect fails a test.
func (sh *shell) env(args []string) int {
	const usage = "env: the fake supports only: env " + envVarTimeFormat + "=" + timeFormatEpoch + " sbatch [options]"
	assign, rest, _ := splitEnv(append([]string{"env"}, args...))
	if len(assign) != 1 || len(rest) == 0 || rest[0] != "sbatch" {
		return sh.fail(2, "%s", usage)
	}
	name, val, _ := strings.Cut(assign[0], "=")
	switch {
	case name != envVarTimeFormat:
		return sh.fail(2, "%s", usage)
	case val != timeFormatEpoch:
		return sh.fail(1, "env: the fake reads only %s=%s, not %q", envVarTimeFormat, timeFormatEpoch, val)
	}
	sh.timeFormat = val
	defer func() { sh.timeFormat = "" }()
	return sh.sbatch(rest[1:])
}

// sbatchTestOnly answers sbatch --test-only. It is read-only: it registers
// nothing, takes no scripted step (a step belongs to a submission), and does not
// register a late job. The partition and QOS refusals are a submission's, since
// real sbatch applies them to a test too.
func (sh *shell) sbatchTestOnly(spec sbatchSpec) int {
	p := sh.p
	if text := p.policyRefusal(sh.s.user, spec); text != "" {
		sh.stderr.WriteString(text)
		return 1
	}
	if text := p.qosRefusal(sh.s.user, spec); text != "" {
		sh.stderr.WriteString(text)
		return 1
	}
	if g := p.to.gate; g != nil {
		if !sh.hang(g) {
			return 1
		}
	}
	c := p.to
	switch c.mode {
	case toFail:
		return sh.fail(1, "sbatch: error: %s", c.text)
	case toOutput:
		sh.stderr.WriteString(c.text)
		return 0
	}
	token := c.text
	if c.mode == toStart {
		start := c.start
		if start.IsZero() {
			start = sh.now
		}
		token = start.In(p.zone).Format(textTimeLayout)
		if sh.timeFormat == timeFormatEpoch {
			token = strconv.FormatInt(start.Unix(), 10)
		}
	}
	fmt.Fprintf(&sh.stderr, "sbatch: Job %d to start at %s using %d processors on nodes fakenode in partition %s\n",
		p.nextID, token, spec.cpus, spec.partition)
	return 0
}

// hang waits at g with mu let go of, until g is released or the call's context
// ends. It reports whether the command goes on; when it does not, the runner
// error is set.
func (sh *shell) hang(g *Gate) bool {
	p := sh.p
	g.markBlocked()
	p.mu.Unlock() // the command may wait for the whole of its bound; nothing else waits for it
	defer p.mu.Lock()
	select {
	case <-g.release:
		return true
	case <-sh.ctx.Done():
		sh.runErr = ctxErr(sh.ctx)
		return false
	}
}
