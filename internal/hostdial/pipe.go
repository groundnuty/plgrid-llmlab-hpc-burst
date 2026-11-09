package hostdial

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// pipeDialTimeout bounds the connection to the agent's socket: the agent is
// on the same node and either listens or is gone.
const pipeDialTimeout = 10 * time.Second

// Pipe is "relay <channel>": it connects to the channel's socket in the job's
// relay directory and hands stdin and stdout over, copying both ways until
// the agent closes the connection (a newer relay replaced this one, or the
// agent ended) or stdin ends (the relay's ssh went away, and srun closed the
// step's input). Then it returns, and the step ends. It writes nothing to
// stdout but the agent's bytes: the session runs over it.
//
// The directory must be this user's, mode 700, as the agent made it; Pipe
// does not create it, so a relay that starts before the agent finds nothing
// and ends, and the controller starts it again after its backoff.
func Pipe(dir string, ch agentapi.RelayChannel, stdin io.Reader, stdout io.Writer) error {
	if _, err := agentapi.ParseRelayChannel(string(ch)); err != nil {
		return err
	}
	if err := privateDir(dir, false); err != nil {
		return err
	}
	conn, err := net.DialTimeout("unix", agentapi.RelaySocket(dir, ch), pipeDialTimeout)
	if err != nil {
		return fmt.Errorf("the agent's %s socket: %w", ch, err)
	}
	defer conn.Close()
	in := make(chan error, 1)
	go func() {
		_, err := io.Copy(conn, stdin)
		closeWrite(conn)
		in <- err
	}()
	_, err = io.Copy(stdout, conn)
	if errors.Is(err, net.ErrClosed) {
		err = nil
	}
	// The agent's side has ended; whatever stdin still holds goes nowhere. The
	// copy from stdin may block on a read that never returns, and the
	// process's exit ends it.
	return err
}
