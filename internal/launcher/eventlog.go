package launcher

// The durable log under the launcher (HBEP-8, The durable log (beta); HBEP-11, Single-user host, as
// amended 16 Nov 2026; issue #625, PR E1). With up --event-log the supervisor runs
// "hpcburst-controller eventlog", an embedded nats-server (internal/eventlog/node), beside the
// controllers: it is recorded by process ID plus start time like every child, its health is a live
// pidfd plus GET /healthz on base+17, and it restarts when it exits. It starts first, before the
// controllers and the front, and stops last (supervise.go). Its configuration, token and caps are
// files in <state>/eventlog/ (mode 700), which up writes whether or not the log may start.
//
// The caps must fit the disk: the supervisor consults a start gate before each start, and the log
// starts only when the free space on its store's file system, less what the streams and the
// upgrade's copy can still grow, stays at or above the floor (floorCheck). When it does not, the
// controllers and the front run without the log, loudly, and the supervisor checks again until it
// does.

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// eventLogEntryName is the log's entry in the supervised set and in supervisor.json: the
// subcommand's name.
const eventLogEntryName = "eventlog"

// EventLogTermGrace is how long the supervisor lets the log stop, from SIGTERM to SIGKILL. nats-server
// closes its streams in well under a second, and with sync_interval always a SIGKILL loses no
// acknowledged write either. The log stops last and alone (runInStopOrder), after the front and the
// others, so its stage comes after SupervisorGraceWithFront's two. It is kept short, and the
// grace down and upgrade give (DefaultGrace) is not raised for it: if every process before it
// used its whole grace, the grace is spent and down's SIGKILL reaches only the log, which is still
// stopping and loses nothing by it. TestTheLogStopsLastAndItsStageFitsTheMargin states the arithmetic.
const EventLogTermGrace = 4 * time.Second

// EventLogConfig is up --event-log with its settings, recorded in the launcher's state and kept
// across upgrade. A change takes down, then up with the new value.
type EventLogConfig struct {
	// Caps are the two streams' caps in logical bytes (--event-log-events-cap, --event-log-usage-cap).
	Caps eventlog.Caps `json:"caps"`
	// Floor is the free space the log leaves on its store's file system (--event-log-floor).
	Floor int64 `json:"floor"`
}

// Validate refuses a record that is not usable: a cap or the floor that is not positive.
func (c EventLogConfig) Validate() error {
	if err := c.Caps.Validate(); err != nil {
		return err
	}
	if c.Floor <= 0 {
		return fmt.Errorf("the free-space floor is %d bytes: it must be positive", c.Floor)
	}
	return nil
}

// withDefaults fills each size that was not given (HBEP-8, Retention, In the durable log): 2 GiB a
// stream, and a floor of 15 GB.
func (c EventLogConfig) withDefaults() EventLogConfig {
	if c.Caps.Events == 0 {
		c.Caps.Events = eventlog.DefaultCap
	}
	if c.Caps.Usage == 0 {
		c.Caps.Usage = eventlog.DefaultCap
	}
	if c.Floor == 0 {
		c.Floor = eventlog.DefaultFloor
	}
	return c
}

// eventLogEntry is the log's supervised entry, or nil when the launcher record does not run one. Its
// command line is the subcommand's contract (cmd/hpcburst-controller/eventlog.go): the two files
// up wrote. The token is in the configuration file only. The log runs in a session of its own, as the
// relay keeper does, so no signal to the supervisor's group reaches it, and listens on exactly its
// two loopback ports.
func (s *State) eventLogEntry(l layout) *supervised {
	if s.EventLog == nil || s.Controllers == nil {
		return nil
	}
	h := l.host(s.Ports)
	dir := h.EventLogDir()
	return &supervised{
		Name: eventLogEntryName, Path: s.Controllers.Binary,
		Args:   []string{eventLogEntryName, "--config", filepath.Join(dir, eventlog.ConfFile), "--caps-file", filepath.Join(dir, eventlog.CapsFile)},
		Health: h.EventLogMonitorAddr(), Ports: []int{h.EventLogPort(), h.EventLogMonitorPort()},
		Setsid: true, WaitReady: true, TermGrace: EventLogTermGrace,
		ready: func() bool { return eventLogStreamsExist(h.EventLogMonitorAddr()) },
	}
}

// eventLogClientArgs are the controllers' and the front's flags for the log: the token file, only
// while the recorded choice is on. The address comes from --port-base. In E1 nothing reads the file
// (the event publisher is E2's), and each program's help says so.
func (s *State) eventLogClientArgs(l layout) []string {
	if s.EventLog == nil {
		return nil
	}
	return []string{"--event-log-token-file", l.host(s.Ports).EventLogTokenFile()}
}

// prepareEventLogFiles writes <state>/eventlog/'s files for a start: the directory (mode 700), the
// token (32 random bytes in hex; one that is already there and well formed is kept, so clients that
// hold it stay valid, and its mode is set), nats.conf and caps.json (mode 600), and removes a refusal
// left by an earlier run, since this start decides afresh. They are written whether or not the log
// may start, so that clients connect once it does. The modes are checked after the writes.
func prepareEventLogFiles(h agentapi.Host, c EventLogConfig) error {
	if err := c.Validate(); err != nil {
		return err
	}
	dir := h.EventLogDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return err
	}
	tokenPath := h.EventLogTokenFile()
	token := ""
	if b, err := os.ReadFile(tokenPath); err == nil {
		if t := strings.TrimSpace(string(b)); eventlog.ValidToken(t) {
			token = t
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if token == "" {
		var err error
		if token, err = eventlog.NewToken(); err != nil {
			return err
		}
	}
	conf, err := eventlog.ConfText(dir, h.EventLogPort(), h.EventLogMonitorPort(), token)
	if err != nil {
		return err
	}
	for _, f := range []struct {
		path string
		data []byte
	}{{tokenPath, []byte(token)}, {filepath.Join(dir, eventlog.ConfFile), []byte(conf)}} {
		if err := eventlog.WriteFileAtomic(f.path, f.data, 0o600); err != nil {
			return err
		}
	}
	if err := eventlog.WriteCaps(filepath.Join(dir, eventlog.CapsFile), c.Caps); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(dir, eventlog.RefusalFile)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	for path, want := range map[string]os.FileMode{
		dir: 0o700, tokenPath: 0o600, filepath.Join(dir, eventlog.ConfFile): 0o600, filepath.Join(dir, eventlog.CapsFile): 0o600,
	} {
		if fi, err := os.Stat(path); err != nil {
			return err
		} else if fi.Mode().Perm() != want {
			return fmt.Errorf("%s has mode %v after it was written, want %v", path, fi.Mode().Perm(), want)
		}
	}
	return nil
}

// readEventLogRefusal returns the settings the log last refused to serve with, from the file it wrote
// before it exited (eventlog.RefusalFile), one per line, or "" when there is none.
func readEventLogRefusal(l layout) string {
	b, err := os.ReadFile(filepath.Join(l.eventLogDir(), eventlog.RefusalFile))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// diskReader is the one seam through which the free space and the sizes the floor needs are read,
// so a test can set them and record the paths asked about: a test that reads another path's free
// space proves nothing (HBEP-8, Test plan, The caps fit the disk).
type diskReader interface {
	// Free is the bytes available to an unprivileged user on the file system holding path.
	Free(path string) (int64, error)
	// Size is the bytes the tree at path takes on disk (blocks allocated, as du reports); 0 when
	// path does not exist.
	Size(path string) (int64, error)
}

// osDisk reads the real file system.
type osDisk struct{}

func (osDisk) Free(path string) (int64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", path, err)
	}
	return int64(st.Bavail) * int64(st.Bsize), nil
}

func (osDisk) Size(path string) (int64, error) {
	var total int64
	err := filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil // gone since the walk listed it: a file the log deleted
			}
			return err
		}
		fi, err := d.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if st, ok := fi.Sys().(*syscall.Stat_t); ok {
			total += st.Blocks * 512 // st_blocks counts 512-byte units
		} else {
			total += fi.Size()
		}
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	return total, err
}

// floorCheck is what the free-space floor judged. The log starts only when
//
//	free − (capsSum − storeBytes) − (capsSum − copyBytes) ≥ floor
//
// with each bracket clamped at 0: the free space on the store's file system, less what the streams
// can still grow (the caps' sum less the store directory's size on disk, which stands for the streams'
// bytes while the log is down; science-agent decision, for #625, R6) and less what the upgrade's copy
// can still add (the caps' sum less the size of the previous upgrade's copy, which the next one
// replaces), stays at or above the floor.
type floorCheck struct {
	Free, Store, PrevCopy int64 // the bytes read
	CapsSum, Floor        int64
	Need                  int64 // the free space the start needs: the floor and the two brackets
}

// OK reports whether the log may start.
func (f floorCheck) OK() bool { return f.Free >= f.Need }

// reason is what status shows for a log that was not started: the free space, the caps and the floor.
func (f floorCheck) reason(c EventLogConfig) string {
	return fmt.Sprintf("%s free on the store's file system, caps %s+%s, floor %s (it needs %s free: the floor, and what the streams and the upgrade's copy can still grow)",
		FormatByteSize(f.Free), FormatByteSize(c.Caps.Events), FormatByteSize(c.Caps.Usage), FormatByteSize(f.Floor), FormatByteSize(f.Need))
}

// floorNeed is the free space the log's start needs, for the sizes read (the formula above).
func floorNeed(capsSum, store, prevCopy, floor int64) int64 {
	return floor + max(0, capsSum-store) + max(0, capsSum-prevCopy)
}

// checkFloor reads the free space of the store's file system and the sizes of the store and of the
// previous upgrade's copy through d, and judges them. The three reads are of <state>/eventlog and
// <state>/backups/eventlog and no other path.
func checkFloor(d diskReader, l layout, c EventLogConfig) (floorCheck, error) {
	var f floorCheck
	var err error
	if f.Free, err = d.Free(l.eventLogDir()); err != nil {
		return f, fmt.Errorf("the free space of the event log's store: %w", err)
	}
	if f.Store, err = d.Size(l.eventLogDir()); err != nil {
		return f, fmt.Errorf("the size of the event log's store: %w", err)
	}
	if f.PrevCopy, err = d.Size(l.eventLogCopyDir()); err != nil {
		return f, fmt.Errorf("the size of the previous upgrade's copy of the event log's store: %w", err)
	}
	f.CapsSum, f.Floor = c.Caps.Sum(), c.Floor
	f.Need = floorNeed(f.CapsSum, f.Store, f.PrevCopy, f.Floor)
	return f, nil
}

// eventLogGate is the start gate of the log's entry: it is consulted before each start, the first
// and every restart, and says whether the caps fit the disk, with the reason when they do not. A
// read that fails closes the gate too: a log that cannot be shown to fit is not started.
func (s *State) eventLogGate(l layout, d diskReader) func() (bool, string) {
	c := *s.EventLog
	return func() (bool, string) {
		f, err := checkFloor(d, l, c)
		if err != nil {
			return false, "the free space could not be read, so the caps are not known to fit: " + err.Error()
		}
		if !f.OK() {
			return false, f.reason(c)
		}
		return true, ""
	}
}

// checkUpgradeFloor is upgrade's own use of the floor, made before anything stops: the upgrade
// replaces the previous copy with a new one of the store, so it refuses when
//
//	free + prevCopyBytes − storeBytes < floor
//
// naming the four sizes. The log's store is as it is now, running, since the copy is made of it.
func checkUpgradeFloor(d diskReader, l layout, c EventLogConfig) error {
	free, err := d.Free(l.eventLogDir())
	if err != nil {
		return fmt.Errorf("the free space of the event log's store: %w", err)
	}
	store, err := d.Size(l.eventLogDir())
	if err != nil {
		return fmt.Errorf("the size of the event log's store: %w", err)
	}
	prev, err := d.Size(l.eventLogCopyDir())
	if err != nil {
		return fmt.Errorf("the size of the previous upgrade's copy of the event log's store: %w", err)
	}
	if left := free + prev - store; left < c.Floor {
		return fmt.Errorf("refusing to upgrade: the copy of the event log's store would leave %s free (%s free on the store's file system, plus the previous copy's %s, less the store's %s), under the floor of %s; nothing was stopped. Free space, or lower --event-log-floor with down and up",
			FormatByteSize(left), FormatByteSize(free), FormatByteSize(prev), FormatByteSize(store), FormatByteSize(c.Floor))
	}
	return nil
}

// DefaultEventLogStartWait bounds how long the supervisor holds the controllers and the front back
// for the log's start (supervise.go): nothing waits on the log, so a log that is slow or crashes
// holds them for this long at most. It is under superviseStartTimeout, which bounds up's wait for
// every child.
const DefaultEventLogStartWait = 15 * time.Second

// DefaultGatePoll is how often the supervisor looks again at a start gate that is closed: the
// space may have come back (HBEP-8, A log refused for space: "checks the space again at each health
// pass, and starts the log once it fits").
const DefaultGatePoll = 10 * time.Second

// eventLogNotStarted returns why the supervisor holds the log back, from its record, or "" when it
// does not: the log's entry is gated.
func eventLogNotStarted(l layout) string {
	f, err := readSupervisorFile(l)
	if err != nil {
		return ""
	}
	for _, e := range f.Entries {
		if eventLogGated(e) {
			return e.Gate
		}
	}
	return ""
}

// eventLogStartWarning is the warning up and upgrade give when the start gate holds the log back for
// space (HBEP-8, A log refused for space): why, read from the supervisor's record, and the line to
// print. Both are "" while the log is not gated.
func eventLogStartWarning(l layout) (why, warning string) {
	why = eventLogNotStarted(l)
	if why == "" {
		return "", ""
	}
	return why, "warning: the event log was not started: " + why + "; the controllers and the front run without it, each emitter will raise EventsNotLogged once one publishes, and the supervisor starts the log once the space fits"
}

// eventLogGated reports whether e is the log's entry, held back by its start gate.
func eventLogGated(e EntryRecord) bool { return e.Name == eventLogEntryName && e.State == entryGated }
