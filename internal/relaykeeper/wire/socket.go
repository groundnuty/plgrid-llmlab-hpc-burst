package wire

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// ExchangeTimeout bounds one exchange, on both ends: it covers the keeper's
// sync of its state file before it answers.
const ExchangeTimeout = 5 * time.Second

// Listen makes the keeper's socket at path: at most
// agentapi.MaxRelaySocketPath bytes, mode 0600, in an existing directory. A
// socket left at path by a keeper that is gone (nothing answers it) is
// replaced. A keeper that still answers there, or anything at path that is
// not a socket, is an error, and nothing is removed.
func Listen(path string) (*net.UnixListener, error) {
	if !filepath.IsAbs(path) || len(path) > agentapi.MaxRelaySocketPath {
		return nil, fmt.Errorf("relaykeeper: the socket's path must be absolute and at most %d bytes, not %d", agentapi.MaxRelaySocketPath, len(path))
	}
	if fi, err := os.Lstat(path); err == nil {
		if fi.Mode().Type() != fs.ModeSocket {
			return nil, errors.New("relaykeeper: something that is not a socket is at the socket's path")
		}
		c, err := net.DialTimeout("unix", path, time.Second)
		if err == nil {
			c.Close()
			return nil, errors.New("relaykeeper: another keeper answers at the socket's path")
		}
		if !errors.Is(err, syscall.ECONNREFUSED) {
			return nil, fmt.Errorf("relaykeeper: a socket at the socket's path neither answers nor refuses: %w", err)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("relaykeeper: remove the gone keeper's socket: %w", err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	// The directory is the launcher's state directory, mode 700, so nobody
	// else reaches the socket before this; the peer check refuses anyone who
	// does.
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return nil, err
	}
	return ln, nil
}

// Handler answers one request with its reply's kind and body. A refusal is
// KindRefused with a Refused.
type Handler func(ctx context.Context, req Envelope) (Kind, any)

// PeerUID reads a Unix connection's peer's user (SO_PEERCRED); a test
// replaces it to show that another user's peer is refused.
var PeerUID = func(c *net.UnixConn) (int, error) {
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
	return int(cred.Uid), nil
}

// Serve answers connections on ln until ctx ends, then closes ln and returns
// once every connection is done. Each connection is one exchange, within
// ExchangeTimeout: a peer of another user is closed with no answer; a request
// of another version, a malformed one, or a reply kind sent as a request is
// refused without reaching h; anything else gets h's reply.
func Serve(ctx context.Context, ln *net.UnixListener, h Handler, log *slog.Logger) error {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	stop := context.AfterFunc(ctx, func() { ln.Close() })
	defer stop()
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		c, err := ln.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer c.Close()
			serveOne(ctx, c, h, log)
		}()
	}
}

func serveOne(ctx context.Context, c *net.UnixConn, h Handler, log *slog.Logger) {
	_ = c.SetDeadline(time.Now().Add(ExchangeTimeout))
	uid, err := PeerUID(c)
	if err != nil || uid != os.Geteuid() {
		log.Warn("a connection to the keeper's socket was refused: not this user's", "peer", uid, "err", err)
		return
	}
	req, err := Read(NewReader(c))
	var ve *VersionError
	switch {
	case errors.As(err, &ve):
		_ = Write(c, KindRefused, Refused{Reason: RefusedVersion, Supported: []int{Version}, Message: ve.Error()})
		return
	case err != nil:
		_ = Write(c, KindRefused, Refused{Reason: RefusedMalformed, Message: err.Error()})
		return
	}
	if _, ok := ReplyKind(req.Kind); !ok {
		_ = Write(c, KindRefused, Refused{Reason: RefusedMalformed, Message: fmt.Sprintf("%s is not a request", req.Kind)})
		return
	}
	kind, body := h(ctx, req)
	if err := Write(c, kind, body); err != nil {
		log.Warn("the keeper's reply could not be written", "request", req.Kind, "err", err)
	}
}

// RefusedError is the keeper's refusal of a request.
type RefusedError struct{ Refused Refused }

func (e *RefusedError) Error() string {
	return fmt.Sprintf("the relay keeper refused the request (%s): %s", e.Refused.Reason, e.Refused.Message)
}

// Exchange sends one request, kind with body, to the keeper's socket at path
// on a connection of its own, and decodes the reply into reply, within
// ExchangeTimeout and ctx. A refusal is a *RefusedError; a reply of another
// kind than the request's is an error.
func Exchange(ctx context.Context, path string, kind Kind, body, reply any) error {
	want, ok := ReplyKind(kind)
	if !ok {
		return fmt.Errorf("relaykeeper: %s is not a request", kind)
	}
	ctx, cancel := context.WithTimeout(ctx, ExchangeTimeout)
	defer cancel()
	var d net.Dialer
	c, err := d.DialContext(ctx, "unix", path)
	if err != nil {
		return err
	}
	defer c.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = c.SetDeadline(dl)
	}
	stop := context.AfterFunc(ctx, func() { _ = c.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	if err := Write(c, kind, body); err != nil {
		return err
	}
	env, err := Read(NewReader(c))
	if err != nil {
		return fmt.Errorf("relaykeeper: the reply to %s: %w", kind, err)
	}
	switch env.Kind {
	case KindRefused:
		var r Refused
		if err := env.Decode(&r); err != nil {
			return err
		}
		return &RefusedError{Refused: r}
	case want:
		return env.Decode(reply)
	}
	return fmt.Errorf("relaykeeper: the reply to %s is %s, want %s", kind, env.Kind, want)
}
