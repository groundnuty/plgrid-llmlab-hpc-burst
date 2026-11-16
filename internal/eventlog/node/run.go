// Package node is HBEP-8's durable log as a process: an embedded nats-server with JetStream, the
// body of "hpcburst-controller eventlog". The launcher supervises it (internal/launcher).
//
// The layer that reads back what nats-server parsed is this repository's spike S8 (jsnode, PR #28),
// which started the server from a configuration text and printed one READY line with the effective
// options; its Start is the model of Run.
package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
)

// Options are the inputs of Run.
type Options struct {
	// ConfigPath is nats.conf, and CapsPath caps.json (internal/eventlog). The store is the directory
	// of the configuration file.
	ConfigPath, CapsPath string
	// Stdout gets the READY line, Stderr every refusal. nats-server's own log goes to Stderr too.
	Stdout, Stderr io.Writer
	// ReadyTimeout bounds the wait for the server to accept connections; 0 is 30 s.
	ReadyTimeout time.Duration
}

// RefusedError is the error of a log that will not serve: its settings are not what HBEP-8 says. It
// carries every difference. The process has written them to eventlog.RefusalFile before returning it.
type RefusedError struct{ Diffs []string }

func (e *RefusedError) Error() string {
	return "the event log refuses to serve: " + strings.Join(e.Diffs, "; ")
}

// Run starts the log and serves until ctx is done, then stops the server and returns nil.
//
//  1. It parses the configuration, and reads back what nats-server parsed (CheckParsed).
//  2. It starts the server and waits until it accepts connections.
//  3. In process, it creates each stream that is absent and sets an existing stream's cap, and nothing
//     else of it.
//  4. It reads both streams' configurations back and compares every setting (DiffStream).
//  5. It prints one READY line.
//
// A difference at 1 or 4 makes it write RefusalFile, stop and return a *RefusedError carrying all of
// them: it never serves with a setting that is not as HBEP-8 says.
func Run(ctx context.Context, o Options) error {
	if o.ReadyTimeout <= 0 {
		o.ReadyTimeout = 30 * time.Second
	}
	confDir := filepath.Dir(o.ConfigPath)
	refuse := func(diffs []string) error {
		for _, d := range diffs {
			fmt.Fprintln(o.Stderr, "eventlog: refusing to serve:", d)
		}
		text := strings.Join(diffs, "\n") + "\n"
		if err := eventlog.WriteFileAtomic(filepath.Join(confDir, eventlog.RefusalFile), []byte(text), 0o600); err != nil {
			fmt.Fprintln(o.Stderr, "eventlog: cannot write", eventlog.RefusalFile+":", err)
		}
		return &RefusedError{Diffs: diffs}
	}

	caps, err := eventlog.ReadCaps(o.CapsPath)
	if err != nil {
		return fmt.Errorf("the caps: %w", err)
	}
	opts, err := server.ProcessConfigFile(o.ConfigPath)
	if err != nil {
		// nats-server names the line and the setting it did not accept.
		return refuse([]string{fmt.Sprintf("the configuration %s does not parse: %v", o.ConfigPath, err)})
	}
	if diffs := CheckParsed(opts, confDir); len(diffs) > 0 {
		return refuse(diffs)
	}
	token := opts.Authorization
	opts.NoSigs = true // the subcommand handles SIGTERM itself, so the stop is its own
	opts.Logtime = true
	srv, err := server.NewServer(opts)
	if err != nil {
		return fmt.Errorf("create the server: %w", err)
	}
	srv.ConfigureLogger()
	stopped := make(chan struct{})
	go func() { srv.Start(); srv.WaitForShutdown(); close(stopped) }()
	stop := func() { srv.Shutdown(); srv.WaitForShutdown() }
	if !srv.ReadyForConnections(o.ReadyTimeout) {
		stop()
		return fmt.Errorf("the server was not ready for connections within %s", o.ReadyTimeout)
	}
	if !srv.JetStreamEnabled() {
		stop()
		return refuse([]string{"jetstream is not running although the configuration enables it"})
	}

	nc, err := nats.Connect("", nats.InProcessServer(srv), nats.Token(token), nats.Name("hpcburst-eventlog"))
	if err != nil {
		stop()
		return fmt.Errorf("connect to the server in process: %w", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		stop()
		return err
	}
	setup, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	var diffs []string
	for _, st := range []struct {
		name string
		cap  int64
	}{{eventlog.StreamEvents, caps.Events}, {eventlog.StreamUsage, caps.Usage}} {
		d, err := ensureStream(setup, js, wantConfig(st.name, caps), st.cap)
		if err != nil {
			stop()
			if ctx.Err() != nil {
				return nil // told to stop while it was starting: a clean stop, as after it serves
			}
			return fmt.Errorf("stream %s: %w", st.name, err)
		}
		diffs = append(diffs, d...)
	}
	if len(diffs) > 0 {
		stop()
		return refuse(diffs)
	}
	_ = os.Remove(filepath.Join(confDir, eventlog.RefusalFile)) // serving: a refusal on record is stale
	fmt.Fprintf(o.Stdout, "READY client=%s:%d monitor=%s:%d sync_always=%v sync_interval=%s store_dir=%s jetstream=%v token=present streams=%s,%s events_cap=%d usage_cap=%d\n",
		opts.Host, opts.Port, opts.HTTPHost, opts.HTTPPort, opts.SyncAlways, opts.SyncInterval, opts.StoreDir, srv.JetStreamEnabled(),
		eventlog.StreamEvents, eventlog.StreamUsage, caps.Events, caps.Usage)

	select {
	case <-ctx.Done():
		stop()
		return nil
	case <-stopped:
		return errors.New("the server stopped by itself")
	}
}

// ensureStream creates the stream when it is absent, with want; otherwise it sets only its MaxBytes
// to the cap. Then it reads the configuration back and returns every difference from want. It sets
// nothing but the cap on a stream that exists, so a stream with another setting (say discard old
// for the events) is reported, never silently corrected: nats-server would accept the correction,
// and the check would then prove nothing.
func ensureStream(ctx context.Context, js jetstream.JetStream, want jetstream.StreamConfig, maxBytes int64) ([]string, error) {
	s, err := js.Stream(ctx, want.Name)
	switch {
	case errors.Is(err, jetstream.ErrStreamNotFound):
		if _, err := js.CreateStream(ctx, want); err != nil {
			return nil, fmt.Errorf("create: %w", err)
		}
	case err != nil:
		return nil, err
	default:
		info, err := s.Info(ctx)
		if err != nil {
			return nil, err
		}
		if cfg := info.Config; cfg.MaxBytes != maxBytes {
			cfg.MaxBytes = maxBytes
			if _, err := js.UpdateStream(ctx, cfg); err != nil {
				return nil, fmt.Errorf("set the cap: %w", err)
			}
		}
	}
	s, err = js.Stream(ctx, want.Name)
	if err != nil {
		return nil, err
	}
	info, err := s.Info(ctx)
	if err != nil {
		return nil, err
	}
	return DiffStream(info.Config, want), nil
}
