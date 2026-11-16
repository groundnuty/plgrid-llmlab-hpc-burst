package node

// What the process checks before and after it serves (HBEP-8, The durable log, Where it runs: "It
// checks what it parsed"). S8, Finding 8: a setting nats-server did not parse looks like its default
// until power fails, so every check here reads a value nats-server holds, never the configuration
// file. Each function returns one line per difference, each naming the setting, so a refusal can say
// every way the log is wrong at once.

import (
	"fmt"
	"path/filepath"
	"slices"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
)

const loopback = "127.0.0.1"

// CheckParsed compares the options nats-server parsed from the configuration with what HBEP-8
// requires: sync_interval always (SyncAlways), JetStream on with its store at storeDir, the client
// and the monitoring listeners on loopback with a port each, and a token. storeDir is where the
// launcher keeps the store: the directory of the configuration file.
func CheckParsed(opts *server.Options, storeDir string) []string {
	var diffs []string
	add := func(format string, a ...any) { diffs = append(diffs, fmt.Sprintf(format, a...)) }
	if !opts.SyncAlways {
		add("sync_interval is not always as nats-server parsed it (SyncAlways is false, sync interval %s): an acknowledged write would not be synced before it is acknowledged", opts.SyncInterval)
	}
	if !opts.JetStream {
		add("jetstream is not enabled as nats-server parsed it")
	}
	if want := filepath.Clean(storeDir); filepath.Clean(opts.StoreDir) != want {
		add("store_dir is %q as nats-server parsed it, want %q", opts.StoreDir, want)
	}
	if opts.Host != loopback || opts.Port < 1 {
		add("the client listener is %q port %d as nats-server parsed it, want %s and a port (listen)", opts.Host, opts.Port, loopback)
	}
	if opts.HTTPHost != loopback || opts.HTTPPort < 1 {
		add("the monitoring listener is %q port %d as nats-server parsed it, want %s and a port (http)", opts.HTTPHost, opts.HTTPPort, loopback)
	}
	if opts.HTTPSPort != 0 {
		add("monitoring also listens over https on port %d, which the log does not use", opts.HTTPSPort)
	}
	if opts.Authorization == "" {
		add("no token as nats-server parsed it (authorization): a client could connect without one")
	}
	return diffs
}

// wantConfig is the configuration HBEP-8 gives a stream, with its cap.
func wantConfig(stream string, caps eventlog.Caps) jetstream.StreamConfig {
	c := jetstream.StreamConfig{
		Name:        stream,
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		Replicas:    1,
		Duplicates:  eventlog.DuplicateWindow,
		AllowDirect: true,
		Compression: jetstream.S2Compression,
		MaxAge:      0,
		MaxMsgs:     -1,
	}
	switch stream {
	case eventlog.StreamEvents:
		c.Subjects = []string{eventlog.SubjectEvents}
		c.Discard = jetstream.DiscardNew
		c.MaxBytes = caps.Events
	case eventlog.StreamUsage:
		c.Subjects = []string{eventlog.SubjectUsage}
		c.Discard = jetstream.DiscardOld
		c.MaxBytes = caps.Usage
	default:
		panic("eventlog/node: no such stream " + stream)
	}
	return c
}

// DiffStream compares a stream's configuration, as nats-server reports it, with want, and returns a
// line for each setting that differs. The settings compared are those of HBEP-8's Settings and
// Retention: subjects, file storage, one replica, limits retention, the discard policy, the
// duplicate window, direct get, S2 compression, the byte cap, and no age or message limit.
func DiffStream(got, want jetstream.StreamConfig) []string {
	var diffs []string
	name := want.Name
	add := func(setting string, got, want any) {
		diffs = append(diffs, fmt.Sprintf("%s: %s is %v, want %v", name, setting, got, want))
	}
	if !slices.Equal(got.Subjects, want.Subjects) {
		add("subjects", got.Subjects, want.Subjects)
	}
	if got.Storage != want.Storage {
		add("storage", got.Storage, want.Storage)
	}
	if got.Replicas != want.Replicas {
		add("replicas", got.Replicas, want.Replicas)
	}
	if got.Retention != want.Retention {
		add("retention", got.Retention, want.Retention)
	}
	if got.Discard != want.Discard {
		add("discard", discardName(got.Discard), discardName(want.Discard))
	}
	if got.Duplicates != want.Duplicates {
		add("duplicate window", got.Duplicates, want.Duplicates)
	}
	if got.AllowDirect != want.AllowDirect {
		add("allow_direct", got.AllowDirect, want.AllowDirect)
	}
	if got.Compression != want.Compression {
		add("compression", got.Compression, want.Compression)
	}
	if got.MaxBytes != want.MaxBytes {
		add("max_bytes", got.MaxBytes, want.MaxBytes)
	}
	if got.MaxAge != 0 {
		add("max_age", got.MaxAge, "none")
	}
	if got.MaxMsgs > 0 {
		add("max_msgs", got.MaxMsgs, "none")
	}
	if got.MaxMsgsPerSubject > 0 {
		add("max_msgs_per_subject", got.MaxMsgsPerSubject, "none")
	}
	if got.Sealed {
		add("sealed", true, false)
	}
	if got.Mirror != nil || len(got.Sources) > 0 {
		add("mirror or sources", "set", "none")
	}
	return diffs
}

func discardName(d jetstream.DiscardPolicy) string {
	switch d {
	case jetstream.DiscardOld:
		return "old"
	case jetstream.DiscardNew:
		return "new"
	}
	return fmt.Sprintf("policy %d", int(d))
}
