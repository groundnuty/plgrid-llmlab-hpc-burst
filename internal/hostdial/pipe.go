package hostdial

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The relay's refusals (HBEP-6, The rendezvous and Fencing). "relay
// <channel>" reports each with a fixed line on its standard error
// (RelayMessage), which names no path, and a distinct exit code; the host
// side reads the line (ClassifyEnd) to tell a retried end from a final one.
var (
	// ErrNotReady: the rendezvous file is missing or not the user's own, or
	// its socket does not answer, or the agent greets for another
	// Allocation: "agent not ready", which the controller retries.
	ErrNotReady = errors.New("agent not ready")
	// ErrSuperseded: the agent keeps a relay with a higher (E, N) on the
	// channel; the controller does not start this one again.
	ErrSuperseded = errors.New("superseded")
)

// Exit codes of "relay <channel>".
const (
	ExitNotReady   = 4
	ExitSuperseded = 5
)

// maxRendezvous bounds the rendezvous file's size.
const maxRendezvous = 4096

// relayLinePrefix starts the relay's own lines on its standard error.
const relayLinePrefix = "hb-relay: "

// RelayMessage is the fixed line "relay <channel>" writes to standard error
// for err; it names the Allocation's UID and no path.
func RelayMessage(a agentapi.RelayArgs, err error) string {
	switch {
	case errors.Is(err, ErrSuperseded):
		return relayLinePrefix + "superseded: the agent of Allocation " + a.Allocation + " keeps a newer " + string(a.Channel) + " relay"
	case errors.Is(err, ErrNotReady):
		return relayLinePrefix + "agent not ready: Allocation " + a.Allocation + "'s agent has published no socket that answers on this node"
	}
	return relayLinePrefix + "the " + string(a.Channel) + " relay of Allocation " + a.Allocation + " failed"
}

// ExitCode is "relay <channel>"'s exit code for err (0 for nil).
func ExitCode(err error) int {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, ErrSuperseded):
		return ExitSuperseded
	case errors.Is(err, ErrNotReady):
		return ExitNotReady
	}
	return 1
}

// Relay is "relay <channel>" (HBEP-6, The rendezvous): it reads the socket's
// path from the Allocation's rendezvous file, connects, checks that the
// agent's greeting names its Allocation, says its channel and (E, N), and on
// the agent's acceptance writes the preamble to stdout and copies both ways
// between stdin and stdout and the socket until either ends. It writes
// nothing else to stdout: the session runs over it.
func Relay(a agentapi.RelayArgs, stdin io.Reader, stdout io.Writer) error {
	if err := a.Validate(); err != nil {
		return err
	}
	sock, err := readRendezvous(agentapi.RelaySocketFile(a.Dir))
	if err != nil {
		return err
	}
	conn, err := net.DialTimeout("unix", sock, exchangeTimeout)
	if err != nil {
		return fmt.Errorf("%w: the socket does not answer: %v", ErrNotReady, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(exchangeTimeout))
	greeting, err := readLine(conn)
	if err != nil {
		return fmt.Errorf("%w: no greeting: %v", ErrNotReady, err)
	}
	if greeting != agentapi.RelayGreeting(a.Allocation) {
		return fmt.Errorf("%w: the agent greeted %q, not Allocation %s", ErrNotReady, greeting, a.Allocation)
	}
	if _, err := io.WriteString(conn, agentapi.RelayHello(a)+"\n"); err != nil {
		return fmt.Errorf("%w: the hello: %v", ErrNotReady, err)
	}
	answer, err := readLine(conn)
	switch {
	case err != nil:
		return fmt.Errorf("%w: no answer: %v", ErrNotReady, err)
	case answer == agentapi.RelaySuperseded:
		return ErrSuperseded
	case answer != agentapi.RelayAccepted:
		return fmt.Errorf("%w: the answer %q", ErrNotReady, answer)
	}
	_ = conn.SetDeadline(time.Time{})
	if _, err := io.WriteString(stdout, agentapi.RelayPreamble(a.Allocation)+"\n"); err != nil {
		return fmt.Errorf("the relay's preamble: %w", err)
	}
	go func() {
		_, _ = io.Copy(conn, stdin)
		closeWrite(conn)
	}()
	_, err = io.Copy(stdout, conn)
	if errors.Is(err, net.ErrClosed) {
		err = nil
	}
	// The agent's side has ended; the copy from stdin may block on a read
	// that never returns, and the process's exit ends it.
	return err
}

// readRendezvous reads the socket's path from the rendezvous file, which must
// be a regular file of this user's with no group or other bits, naming an
// absolute path within agentapi.MaxRelaySocketPath.
func readRendezvous(file string) (string, error) {
	f, err := os.OpenFile(file, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return "", fmt.Errorf("%w: the rendezvous file: %v", ErrNotReady, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("%w: the rendezvous file: %v", ErrNotReady, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !fi.Mode().IsRegular() || fi.Mode().Perm()&0o077 != 0 || !ok || int(st.Uid) != os.Geteuid() {
		return "", fmt.Errorf("%w: the rendezvous file is not this user's own, mode 600", ErrNotReady)
	}
	b, err := io.ReadAll(io.LimitReader(f, maxRendezvous+1))
	if err != nil || len(b) > maxRendezvous {
		return "", fmt.Errorf("%w: the rendezvous file cannot be read", ErrNotReady)
	}
	sock := strings.TrimSpace(string(b))
	if !filepath.IsAbs(sock) || filepath.Clean(sock) != sock || len(sock) > agentapi.MaxRelaySocketPath {
		return "", fmt.Errorf("%w: the rendezvous file names %q, not an absolute socket path of at most %d bytes", ErrNotReady, sock, agentapi.MaxRelaySocketPath)
	}
	return sock, nil
}
