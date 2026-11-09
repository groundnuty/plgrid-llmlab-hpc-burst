package hostdial

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/yamux"
)

// targetDialTimeout bounds each dial of the channel's loopback port.
const targetDialTimeout = 10 * time.Second

// ServeStats count what one control-plane end has carried.
type ServeStats struct {
	// Streams counts the streams the job opened; Joined those whose target
	// dial succeeded.
	Streams, Joined atomic.Uint64
}

// Serve runs the control-plane end of one relay's session over rwc, the relay
// process's standard output (read) and input (write), until the session ends
// or ctx does. Each stream the job opens is joined to a new connection to
// target, the channel's loopback address (agentapi.RelayTargetAddr), and
// nothing else is ever dialed: a stream carries no address of its own. Serve
// opens no stream. It closes rwc and every joined connection before it
// returns.
func Serve(ctx context.Context, rwc io.ReadWriteCloser, target string, log *slog.Logger, stats *ServeStats) error {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if stats == nil {
		stats = &ServeStats{}
	}
	sess, err := yamux.Server(rwc, sessionConfig(log, "host"))
	if err != nil {
		_ = rwc.Close()
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = sess.Close() })
	defer stop()
	var wg sync.WaitGroup
	defer wg.Wait()
	defer sess.Close()
	for {
		st, err := sess.AcceptStream()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, yamux.ErrSessionShutdown) || errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		stats.Streams.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer st.Close()
			d := net.Dialer{Timeout: targetDialTimeout}
			conn, err := d.DialContext(ctx, "tcp4", target)
			if err != nil {
				log.Warn("a relay stream's target refused", "target", target, "err", err)
				return
			}
			stats.Joined.Add(1)
			defer conn.Close()
			// A session that ends closes the stream, which ends both copies.
			join(conn, st)
		}()
	}
}
