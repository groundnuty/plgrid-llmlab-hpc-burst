package slurm

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Command is one command line that the login node's non-interactive shell runs
// (HBEP-4, Transport). Line is built only from this package's fixed templates
// and from values that passed CheckValue, joined by Line; Stdin carries file
// content, which never appears in Line.
type Command struct {
	// Line is the remote command line.
	Line string
	// Stdin is fed to the command's standard input; nil for none.
	Stdin []byte
	// Timeout bounds the command. Zero means provider.CommandTimeout.
	Timeout time.Duration
}

// Result is what a command that ran returned.
type Result struct {
	Stdout, Stderr []byte
	// ExitCode is the remote command's exit status.
	ExitCode int
}

// Runner runs commands on the login node. Implementations are safe for
// concurrent use: SSH over one multiplexed connection in production, and the
// in-process fake Slurm in tests.
type Runner interface {
	// Run runs one command. A nil error means the command ran and Result holds
	// its exit status. Otherwise the error wraps ErrNotStarted (nothing
	// reached the login node), ErrTimeout or ErrTransport (it may have run).
	Run(ctx context.Context, cmd Command) (Result, error)

	// Generation identifies the current connection to the login node. It
	// changes after every new connection, so callers resolve login-node paths
	// again (HBEP-4, Transport: paths are resolved once per connection).
	Generation() uint64
}

// Connector is a Runner that connects lazily. Connect brings the connection up
// when it is down, so Generation then names the connection the next command
// uses.
type Connector interface {
	Connect(ctx context.Context) error
}

// Transport error kinds. Their messages never carry a host name, a user or a
// path: they reach conditions and events.
var (
	// ErrNotStarted: the command never reached the login node, for example
	// because no connection could be made. A submission that never started is
	// Rejected (HBEP-4, Core operations).
	ErrNotStarted = errors.New("command never started: no connection to the login node")
	// ErrTimeout: the command ran past its time-out. A submission that timed
	// out is Unknown; a read that timed out is an error.
	ErrTimeout = errors.New("command timed out")
	// ErrTransport: the connection failed while the command may have run.
	ErrTransport = errors.New("transport error on the connection to the login node")
)

// safeValue is HBEP-4's pattern for every value placed in a command
// (Transport): letters, digits and ._:=,/-.
var safeValue = regexp.MustCompile(`^[A-Za-z0-9._:=,/-]+$`)

// CheckValue refuses a value that does not match HBEP-4's safe pattern, before
// any call. name says which value it is; the value itself is never in the
// error, because it may be an account.
func CheckValue(name, v string) error {
	if !safeValue.MatchString(v) {
		return fmt.Errorf("%s is empty or holds a character outside letters, digits and ._:=,/-", name)
	}
	return nil
}

// plainWord is a word the shell reads as itself: no quoting needed.
var plainWord = regexp.MustCompile(`^[A-Za-z0-9._:=,/@%+-]+$`)

// shellWord returns w as one shell word: as it is when the shell reads it
// literally, otherwise in single quotes. A word that holds a single quote, a
// newline or a NUL is refused; this package never builds one.
func shellWord(w string) (string, error) {
	if plainWord.MatchString(w) {
		return w, nil
	}
	if w == "" || strings.ContainsAny(w, "'\n\r\x00") {
		return "", fmt.Errorf("slurm: refusing to build a command word that is empty or holds a quote, a newline or a NUL")
	}
	return "'" + w + "'", nil
}

// Line joins words into one command line, quoting each as shellWord does.
func Line(words ...string) (string, error) {
	out := make([]string, len(words))
	for i, w := range words {
		q, err := shellWord(w)
		if err != nil {
			return "", err
		}
		out[i] = q
	}
	return strings.Join(out, " "), nil
}

// And joins command lines with the shell's &&, so each runs only when the one
// before succeeded. It is the only compound form this package builds, besides
// one "> file" redirection of standard input (writeTempLine).
func And(lines ...string) string { return strings.Join(lines, " && ") }
