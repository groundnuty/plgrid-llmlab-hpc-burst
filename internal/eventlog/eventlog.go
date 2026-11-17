// Package eventlog holds the contract of HBEP-8's durable log (beta): the names of its two streams,
// the files the launcher writes for it (nats.conf, token, caps.json) and the sizes it takes. The
// process itself, an embedded nats-server, is in internal/eventlog/node, so a program that only
// reads these files (the launcher) does not link the server.
//
// The log is opt-in (hpcburst up --event-log). The launcher writes <state>/eventlog/nats.conf,
// token and caps.json, mode 600, in a directory of mode 700, and supervises
// "hpcburst-controller eventlog --config <state>/eventlog/nats.conf --caps-file
// <state>/eventlog/caps.json". The token is only in those files, never on a command line.
package eventlog

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// The two streams (HBEP-8, The durable log, Streams).
const (
	// StreamEvents holds the typed events, kept for the grant: discard new, so a full stream refuses
	// new events and keeps every old one.
	StreamEvents = "HB_EVENTS"
	// StreamUsage is the rolling copy of the usage files: discard old.
	StreamUsage = "HB_USAGE"

	// SubjectEvents is HB_EVENTS' subject filter: hb.ev.<kind>.<uid>.
	SubjectEvents = "hb.ev.>"
	// SubjectUsage is HB_USAGE's one subject.
	SubjectUsage = "hb.usage"
)

// DuplicateWindow is both streams' duplicate window: a retry within it of a publish that was
// stored stores nothing (HBEP-8, Delivery).
const DuplicateWindow = 2 * time.Minute

// The launcher's defaults for the settings it passes (HBEP-8, Retention, In the durable log).
const (
	// DefaultCap is each stream's cap, in logical bytes: 2 GiB.
	DefaultCap int64 = 2 << 30
	// DefaultFloor is the free space the log must leave on its store's file system: 15 GB.
	DefaultFloor int64 = 15_000_000_000
)

// The files of <state>/eventlog/.
const (
	ConfFile = "nats.conf"
	// TokenFile holds the token, which the controllers and the front read.
	TokenFile = "token"
	CapsFile  = "caps.json"
	// RefusalFile is written by the process, before it exits refusing to serve, with every setting
	// that was not as HBEP-8 says. hpcburst status reads it. The process removes it once it serves.
	RefusalFile = "refused.txt"
)

// ExitRefused is the exit status of "hpcburst-controller eventlog" when it refuses to serve because a
// setting is not what HBEP-8 says. 1 is any other failure and 2 a usage error.
const ExitRefused = 3

// TokenBytes is the token's length in random bytes: it is written as twice that many hex digits.
const TokenBytes = 32

// NewToken returns a new token: TokenBytes random bytes in lower-case hex.
func NewToken() (string, error) {
	b := make([]byte, TokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("read random bytes for the token: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// ValidToken reports whether s is a token NewToken could have made: 2*TokenBytes hex digits, lower case.
func ValidToken(s string) bool {
	if len(s) != 2*TokenBytes {
		return false
	}
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == TokenBytes && hex.EncodeToString(b) == s
}

// ReadToken reads the token file the launcher wrote (<state>/eventlog/token) and checks that it holds
// a token NewToken could have made. An error names the file and what was wrong, never the token.
func ReadToken(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("the durable log's token file: %w", err)
	}
	t := strings.TrimSpace(string(b))
	if !ValidToken(t) {
		return "", fmt.Errorf("the durable log's token file %s does not hold a token (%d lower-case hex digits)", path, 2*TokenBytes)
	}
	return t, nil
}

// Caps are the two streams' caps in logical bytes, which nats-server counts as each message's
// subject, headers and payload plus a fixed overhead (HBEP-8, Retention, In the durable log). The
// launcher writes them to caps.json at each start, and the process applies them to the streams.
type Caps struct {
	Events int64 `json:"events"`
	Usage  int64 `json:"usage"`
}

// Sum is the caps added together.
func (c Caps) Sum() int64 { return c.Events + c.Usage }

// Validate refuses a cap that is not positive: nats-server reads 0 or -1 as no limit, and the log
// is capped by design.
func (c Caps) Validate() error {
	if c.Events <= 0 {
		return fmt.Errorf("the events cap is %d bytes: it must be positive", c.Events)
	}
	if c.Usage <= 0 {
		return fmt.Errorf("the usage cap is %d bytes: it must be positive", c.Usage)
	}
	return nil
}

// WriteCaps writes the caps to path, mode 600, through a temporary file and a rename.
func WriteCaps(path string, c Caps) error {
	if err := c.Validate(); err != nil {
		return err
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return WriteFileAtomic(path, append(b, '\n'), 0o600)
}

// ReadCaps reads caps.json strictly: an unknown field, a trailing value or a cap that is not
// positive is an error naming the file.
func ReadCaps(path string) (Caps, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Caps{}, err
	}
	var c Caps
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return Caps{}, fmt.Errorf("%s: %w", path, err)
	}
	if dec.More() {
		return Caps{}, fmt.Errorf("%s: more than one JSON value", path)
	}
	if err := c.Validate(); err != nil {
		return Caps{}, fmt.Errorf("%s: %w", path, err)
	}
	return c, nil
}

// ConfText renders nats.conf: loopback listeners for clients and for monitoring, a token, and a
// JetStream store at storeDir that syncs every write before it is acknowledged (S8: sync_interval
// belongs to the jetstream block, and a misspelt setting looks like the default, which is why the
// process reads back what nats-server parsed). storeDir must be an absolute path without a quote,
// a backslash or a control character, which the configuration's string syntax would read
// differently.
func ConfText(storeDir string, clientPort, monitorPort int, token string) (string, error) {
	if !filepath.IsAbs(storeDir) {
		return "", fmt.Errorf("the store directory %q must be an absolute path", storeDir)
	}
	for _, r := range storeDir {
		if r == '"' || r == '\\' || r < 0x20 || r == 0x7f {
			return "", fmt.Errorf("the store directory %q holds a character the configuration's strings cannot carry", storeDir)
		}
	}
	for _, p := range []struct {
		name string
		port int
	}{{"client", clientPort}, {"monitoring", monitorPort}} {
		if p.port < 1 || p.port > 65535 {
			return "", fmt.Errorf("the %s port %d is not a port", p.name, p.port)
		}
	}
	if clientPort == monitorPort {
		return "", fmt.Errorf("the client and monitoring ports are both %d", clientPort)
	}
	if !ValidToken(token) {
		return "", errors.New("the token is not 64 lower-case hex digits")
	}
	return fmt.Sprintf("listen: 127.0.0.1:%d\nhttp: 127.0.0.1:%d\nauthorization {\n  token: %q\n}\njetstream {\n  store_dir: %q\n  sync_interval: always\n}\n",
		clientPort, monitorPort, token, storeDir), nil
}

// WriteFileAtomic writes data to path through a temporary file in the same directory, syncs it and
// renames it over path, so a reader never sees half a file.
func WriteFileAtomic(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	fail := func(err error) error { tmp.Close(); os.Remove(name); return err }
	if err := tmp.Chmod(mode); err != nil {
		return fail(err)
	}
	if _, err := tmp.Write(data); err != nil {
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(name)
		return err
	}
	if err := os.Rename(name, path); err != nil {
		os.Remove(name)
		return err
	}
	return nil
}
