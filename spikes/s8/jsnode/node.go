// Package jsnode starts an embedded nats-server with JetStream from a config
// text, exactly as a standalone nats-server would read it, so the setting under
// test is the literal "sync_interval: always" from the issue.
package jsnode

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// SyncMode selects the JetStream fsync policy.
type SyncMode string

const (
	// SyncDefault leaves sync_interval unset: the server fsyncs about every two minutes.
	SyncDefault SyncMode = "default"
	// SyncAlways is "sync_interval: always": every write is fsynced before it is acknowledged.
	SyncAlways SyncMode = "always"
)

// Config describes one embedded node.
type Config struct {
	StoreDir string   // JetStream store directory (must be on a large disk, not /home)
	Sync     SyncMode // SyncDefault or SyncAlways
	Port     int      // client port on 127.0.0.1; -1 picks a free one
}

// Node is a running embedded server.
type Node struct {
	Server *server.Server
	// Effective options as the server parsed them, for assertion by callers.
	SyncAlways   bool
	SyncInterval time.Duration
}

// ConfText renders the server configuration. It is exported so tests and the
// README can show exactly what was run.
func ConfText(c Config) (string, error) {
	if c.StoreDir == "" {
		return "", fmt.Errorf("jsnode: StoreDir is required")
	}
	if !filepath.IsAbs(c.StoreDir) {
		return "", fmt.Errorf("jsnode: StoreDir %q must be absolute", c.StoreDir)
	}
	sync := ""
	switch c.Sync {
	case SyncAlways:
		sync = "  sync_interval: always\n"
	case SyncDefault, "":
	default:
		return "", fmt.Errorf("jsnode: unknown sync mode %q", c.Sync)
	}
	port := c.Port
	if port == 0 {
		port = -1
	}
	return fmt.Sprintf("host: 127.0.0.1\nport: %d\njetstream {\n  store_dir: %q\n%s}\n", port, c.StoreDir, sync), nil
}

// Start writes the config next to the store (not inside it), starts the server
// and waits until it accepts connections.
func Start(c Config) (*Node, error) {
	text, err := ConfText(c)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(c.StoreDir, 0o700); err != nil {
		return nil, err
	}
	confPath := filepath.Clean(c.StoreDir) + ".conf"
	if err := os.WriteFile(confPath, []byte(text), 0o600); err != nil {
		return nil, err
	}
	opts, err := server.ProcessConfigFile(confPath)
	if err != nil {
		return nil, fmt.Errorf("jsnode: config: %w", err)
	}
	opts.NoLog = true
	opts.NoSigs = true
	s, err := server.NewServer(opts)
	if err != nil {
		return nil, err
	}
	go s.Start()
	if !s.ReadyForConnections(20 * time.Second) {
		s.Shutdown()
		return nil, fmt.Errorf("jsnode: server not ready within 20s")
	}
	if !s.JetStreamEnabled() {
		s.Shutdown()
		return nil, fmt.Errorf("jsnode: JetStream is not enabled")
	}
	return &Node{Server: s, SyncAlways: opts.SyncAlways, SyncInterval: opts.SyncInterval}, nil
}

// URL is the client URL (loopback only).
func (n *Node) URL() string { return n.Server.ClientURL() }

// Stop shuts the server down cleanly (the SIGKILL tests do not use this).
func (n *Node) Stop() {
	n.Server.Shutdown()
	n.Server.WaitForShutdown()
}
