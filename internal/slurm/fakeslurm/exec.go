package fakeslurm

// New. Runs a parsed line: the "&&" chain, the call log and the dispatch to
// the commands.

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

func isNotStarted(err error) bool { return errors.Is(err, slurm.ErrNotStarted) }

// shell is the state of one line: the shell a non-interactive ssh command
// starts, with its umask, its standard input and the output so far.
type shell struct {
	p      *Platform
	s      *Session
	now    time.Time
	umask  fs.FileMode
	stdin  []byte
	stdout bytes.Buffer
	stderr bytes.Buffer
	// runErr is a runner error that a command decided on after it had done
	// its work (a lost sbatch reply); the line then returns it with no result.
	runErr error
}

// command is a simple command's implementation: the arguments after the
// command word, and the exit code.
type command func(sh *shell, args []string) int

var commands = map[string]command{
	"sbatch":    (*shell).sbatch,
	"squeue":    (*shell).squeue,
	"sacct":     (*shell).sacct,
	"scancel":   (*shell).scancel,
	"scontrol":  (*shell).scontrol,
	"printenv":  (*shell).printenv,
	"sha256sum": (*shell).sha256sum,
	"umask":     (*shell).setUmask,
	"mkdir":     (*shell).mkdir,
	"chmod":     (*shell).chmod,
	"mv":        (*shell).mv,
	"rm":        (*shell).rm,
	"test":      (*shell).test,
}

// execLine runs the commands of one line with mu held. Each command is logged
// as it begins; a command that exits non-zero ends the line.
func (p *Platform) execLine(s *Session, cmds []simpleCmd, stdin []byte) (slurm.Result, error) {
	sh := &shell{p: p, s: s, now: p.clock.Now(), umask: 0o022, stdin: stdin}
	code := 0
	for _, c := range cmds {
		stdinLen := 0
		if c.redirected {
			stdinLen = len(stdin)
		}
		p.logCall(s, c, stdinLen, sh.now)
		code = sh.run(c)
		if sh.runErr != nil {
			return slurm.Result{}, sh.runErr
		}
		if code != 0 {
			break
		}
	}
	return slurm.Result{Stdout: sh.stdout.Bytes(), Stderr: sh.stderr.Bytes(), ExitCode: code}, nil
}

// run dispatches one simple command.
func (sh *shell) run(c simpleCmd) int {
	if c.redirected {
		return sh.cat(c)
	}
	fn, ok := commands[c.words[0]]
	if !ok {
		return sh.fail(127, "bash: line 1: %s: command not found", c.words[0])
	}
	return fn(sh, c.words[1:])
}

// fail writes one line to standard error and returns the exit code.
func (sh *shell) fail(code int, format string, a ...any) int {
	fmt.Fprintf(&sh.stderr, format+"\n", a...)
	return code
}

// out writes to standard output.
func (sh *shell) out(format string, a ...any) { fmt.Fprintf(&sh.stdout, format, a...) }

// path resolves a file name word: an absolute path as it is (cleaned), a
// relative one against the user's HOME, where an ssh command starts.
func (sh *shell) path(word string) string {
	if !strings.HasPrefix(word, "/") {
		word = sh.s.env["HOME"] + "/" + word
	}
	return cleanPath(word)
}

// fileMode and dirMode are the modes a new file and a new directory get:
// 0666 and 0777 &^ umask.
func (sh *shell) fileMode() fs.FileMode { return 0o666 &^ sh.umask }
func (sh *shell) dirMode() fs.FileMode  { return 0o777 &^ sh.umask }
