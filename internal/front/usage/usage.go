// Package usage writes the front's usage records (HBEP-5, Usage records;
// HBEP-8): one JSON line per request, synced to disk before the next, with a
// monotonic counter persisted apart from the log in blocks, so that records
// lost in a crash show as a gap. A Write returns once its record is on disk. In alpha the records are also the
// attribution log (HBEP-9). No record holds a prompt, a response or a key.
//
// The records go to one file per UTC day, usage-YYYY-MM-DD.jsonl, named by the
// day of the write. Every file is kept: nothing here deletes or rewrites one,
// except the repair of the newest file's partial last line at open.
package usage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Record is one request's usage record. Its JSON member names are HBEP-5's.
// Pointers are null when unknown. ContentEvents and ErrorCode are additions:
// on a stream that did not ask for usage, the counts are null and the front's
// count of content events is kept in ContentEvents (countsSource
// front_events); ErrorCode holds a failed Responses stream's error.code.
type Record struct {
	Counter          uint64  `json:"counter"`
	RequestID        string  `json:"requestID"`
	StartTime        string  `json:"startTime"`
	EndTime          string  `json:"endTime"`
	ClientKeyID      string  `json:"clientKeyID"`
	Path             string  `json:"path"`
	RequestedModel   string  `json:"requestedModel"`
	Level            string  `json:"level"`
	Provider         string  `json:"provider"`
	ModelDeployment  string  `json:"modelDeployment"`
	DeploymentID     string  `json:"deploymentID"`
	Instance         string  `json:"instance"`
	InstanceUID      string  `json:"instanceUID"`
	Emitter          string  `json:"emitter"`
	Status           int     `json:"status"`
	Stream           bool    `json:"stream"`
	Finish           *string `json:"finish"`
	CutBy            *string `json:"cutBy"`
	PromptTokens     *int64  `json:"promptTokens"`
	CachedTokens     *int64  `json:"cachedTokens"`
	CompletionTokens *int64  `json:"completionTokens"`
	CountsSource     *string `json:"countsSource"`
	FirstTokenMs     *int64  `json:"firstTokenMs"`
	DurationMs       int64   `json:"durationMs"`
	ContentEvents    *int64  `json:"contentEvents,omitempty"`
	// ErrorCode is the error.code of a stream that ended with an engine
	// error after it started (response.failed), when the event has one.
	ErrorCode *string `json:"errorCode,omitempty"`
}

// Time formats a time as a usage record does: UTC, millisecond RFC 3339.
func Time(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z07:00") }

// Files in the usage directory.
const (
	// LegacyLogFile is the one log the front wrote before rotation. It is the
	// newest file while no daily file exists, and is never written again once
	// one does (HBEP-8, Usage records, Rotation keeps every file).
	LegacyLogFile = "usage.jsonl"
	CounterFile   = "counter"
	// BlockSize is how many counters the front reserves at a time.
	BlockSize = 1000

	dayLayout    = "2006-01-02"
	dayFilePre   = "usage-"
	dayFilePost  = ".jsonl"
	dayFileWidth = len(dayFilePre) + len(dayLayout) + len(dayFilePost)
)

// DayFile returns the name of the file that holds the records written on the
// UTC day of t: usage-YYYY-MM-DD.jsonl.
func DayFile(t time.Time) string { return dayFileName(t.UTC().Format(dayLayout)) }

func dayFileName(day string) string { return dayFilePre + day + dayFilePost }

// dayOf returns the day in a daily file's name, and whether name is one: the
// date must be a real one, so usage-2026-13-45.jsonl is not.
func dayOf(name string) (string, bool) {
	if len(name) != dayFileWidth || !strings.HasPrefix(name, dayFilePre) || !strings.HasSuffix(name, dayFilePost) {
		return "", false
	}
	day := name[len(dayFilePre) : len(name)-len(dayFilePost)]
	t, err := time.Parse(dayLayout, day)
	if err != nil || t.Format(dayLayout) != day {
		return "", false
	}
	return day, true
}

// Files returns the names of the usage files in dir, oldest first: the legacy
// usage.jsonl, if there is one, and then the daily files in date order. A
// reader that wants every record reads them in that order, and orders the
// records by counter.
func Files(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var legacy bool
	var days []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if e.Name() == LegacyLogFile {
			legacy = true
		} else if _, ok := dayOf(e.Name()); ok {
			days = append(days, e.Name())
		}
	}
	sort.Strings(days) // the names sort as their dates
	if legacy {
		days = append([]string{LegacyLogFile}, days...)
	}
	return days, nil
}

// newest returns the newest usage file in dir: the last daily file, else the
// legacy one, else "". Its day is "" for the legacy file.
func newest(dir string) (name, day string, err error) {
	files, err := Files(dir)
	if err != nil || len(files) == 0 {
		return "", "", err
	}
	name = files[len(files)-1]
	day, _ = dayOf(name)
	return name, day, nil
}

// logFile is the part of *os.File the sink uses. The tests put a file that
// fails in its place.
type logFile interface {
	Write([]byte) (int, error)
	Sync() error
	Truncate(int64) error
	Close() error
}

// pending is a queued record and the channel its Write waits on.
type pending struct {
	rec  Record
	done chan error // receives the outcome of the batch the record is in
}

// Options are the inputs of OpenOptions.
type Options struct {
	// Logger gets one Error record for each batch of records that fails to
	// be written or synced, naming the cause. Nil logs nothing.
	Logger *slog.Logger
	// Now is the clock that names a batch's file by its UTC day and times a
	// failure. Nil is time.Now.
	Now func() time.Time
}

// hooks are the seams the tests use to make the disk fail.
type hooks struct {
	// wrap is applied to every log file the sink opens.
	wrap func(*os.File) logFile
	// syncDir syncs a directory, after a new file was created in it.
	syncDir func(dir string) error
}

// Sink appends records to the usage files in its directory: the file of the
// UTC day of each write (see the package comment). Write assigns the counter and
// queues the record; one goroutine writes what is queued in one write and
// one fsync, so records are batched only when they arrive faster than the
// disk syncs (HBEP-5, Sink in alpha). A Write returns when the batch its
// record is in has been written and synced, or has failed.
type Sink struct {
	dir    string
	logger *slog.Logger
	now    func() time.Time
	h      hooks

	mu       sync.Mutex
	next     uint64 // the next counter to use
	blockEnd uint64 // the last counter persisted as reserved
	queue    []pending
	wake     chan struct{}
	closed   bool
	done     chan struct{}
	err      error  // the latest failure; nil once a batch succeeds
	failures uint64 // the batches and counter blocks that failed
	records  uint64 // the records written and synced
	lost     uint64 // the records not written: those of a failed batch, or one that got no counter
	since    time.Time

	// Only the writer goroutine uses these after Open.
	log  logFile // the open log: the file of day; nil until the first write when there is no daily file yet
	day  string  // the UTC day of log
	size int64   // the log's size when it holds only whole lines
	torn bool    // a failed write left a partial line that could not be cut off
}

// Open opens the sink in dir, creating it (mode 0700). The first counter is
// the persisted block end plus one, so a crash shows as a gap (HBEP-8). The
// newest usage file, whatever its day, is cut back to its last whole line if
// it ends in the middle of one, as a crash during a write leaves it, so that
// the next record starts a line of its own; the record that was cut shows as
// a gap too. No other file is touched. It logs nothing.
func Open(dir string) (*Sink, error) { return OpenOptions(dir, Options{}) }

// OpenLog is Open with a logger, which gets one Error record for each batch of
// records that fails to be written or synced, naming the cause. A nil logger
// logs nothing.
func OpenLog(dir string, logger *slog.Logger) (*Sink, error) {
	return OpenOptions(dir, Options{Logger: logger})
}

// OpenOptions is Open with the options.
func OpenOptions(dir string, o Options) (*Sink, error) {
	return open(dir, o, hooks{})
}

// open is OpenOptions with the disk's seams, which is how the tests make it
// fail.
func open(dir string, o Options, h hooks) (*Sink, error) {
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if h.wrap == nil {
		h.wrap = func(f *os.File) logFile { return f }
	}
	if h.syncDir == nil {
		h.syncDir = syncDir
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	end, err := readCounter(filepath.Join(dir, CounterFile))
	if err != nil {
		return nil, err
	}
	s := &Sink{dir: dir, logger: o.Logger, now: o.Now, h: h, next: end + 1, blockEnd: end,
		wake: make(chan struct{}, 1), done: make(chan struct{})}
	s.since = s.now()
	if err := s.repairNewest(); err != nil {
		return nil, err
	}
	go s.run()
	return s, nil
}

// repairNewest cuts a partial last line off the newest usage file, and only
// that one. A daily file stays open as the log, with its day, whatever the
// day: the first write rotates away from it when the clock has reached a later
// day, and a clock that is still on an earlier day (stepped back since the
// start) appends to it, so no older, closed file is ever repaired or written.
// The legacy file is closed after the repair: it is never written again.
func (s *Sink) repairNewest() error {
	name, day, err := newest(s.dir)
	if err != nil {
		return err
	}
	if name == "" {
		return nil
	}
	path := filepath.Join(s.dir, name)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	size, err := repairTail(f)
	if err != nil {
		f.Close()
		return fmt.Errorf("usage: repair the end of %s: %w", path, err)
	}
	if day == "" {
		return f.Close()
	}
	s.log, s.day, s.size = s.h.wrap(f), day, size
	return nil
}

// today is the UTC day of the sink's clock, as a file name holds it.
func (s *Sink) today() string { return s.now().UTC().Format(dayLayout) }

// Write assigns r its counter, queues it, and returns the counter once the
// record is on disk: written and synced. It returns an error instead when the
// counter block could not be persisted, the sink is closed, or the batch the
// record is in failed to be written or synced; a record whose sync failed may
// still be in the log. A failure is not remembered: the next Write tries the
// disk again, after the log is cut back to its last whole line.
func (s *Sink) Write(r Record) (uint64, error) {
	p, err := s.enqueue(r)
	if err != nil {
		return 0, err
	}
	return p.rec.Counter, <-p.done
}

func (s *Sink) enqueue(r Record) (pending, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return pending{}, errors.New("usage: the sink is closed")
	}
	if s.next > s.blockEnd {
		end := s.blockEnd + BlockSize
		if err := writeCounter(filepath.Join(s.dir, CounterFile), end); err != nil {
			err = fmt.Errorf("usage: persist the counter block: %w", err)
			// The record gets no counter and no batch: one record lost.
			s.failedLocked(err, 1)
			s.mu.Unlock()
			s.logger.Error("usage: the counter block could not be persisted", "error", err)
			return pending{}, err
		}
		s.blockEnd = end
	}
	r.Counter = s.next
	s.next++
	p := pending{rec: r, done: make(chan error, 1)}
	s.queue = append(s.queue, p)
	select {
	case s.wake <- struct{}{}:
	default:
	}
	s.mu.Unlock()
	return p, nil
}

func (s *Sink) run() {
	defer close(s.done)
	for {
		_, open := <-s.wake
		s.mu.Lock()
		batch := s.queue
		s.queue = nil
		closed := s.closed
		s.mu.Unlock()
		if len(batch) > 0 {
			s.flush(batch)
		}
		if !open || closed {
			s.mu.Lock()
			rest := s.queue
			s.queue = nil
			s.mu.Unlock()
			if len(rest) > 0 {
				s.flush(rest)
			}
			return
		}
	}
}

// repairTail cuts a partial last line off the log and syncs, and returns the
// log's size. A log that ends in a newline, or is empty, is left as it is.
func repairTail(f *os.File) (int64, error) {
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	end, err := lineEnd(f, st.Size())
	if err != nil {
		return 0, err
	}
	if end == st.Size() {
		return end, nil
	}
	if err := f.Truncate(end); err != nil {
		return 0, err
	}
	return end, f.Sync()
}

// lineEnd returns the offset just after the last newline in the first size
// bytes of f, or 0 when there is none. It reads backwards from size in chunks,
// so a log of any size costs one read when it ends in a newline.
func lineEnd(f *os.File, size int64) (int64, error) {
	buf := make([]byte, 4096)
	for hi := size; hi > 0; {
		lo := max(hi-int64(len(buf)), 0)
		chunk := buf[:hi-lo]
		if _, err := f.ReadAt(chunk, lo); err != nil {
			return 0, err
		}
		if i := bytes.LastIndexByte(chunk, '\n'); i >= 0 {
			return lo + int64(i) + 1, nil
		}
		hi = lo
	}
	return 0, nil
}

// flush writes and syncs batch, records the outcome, logs a failure, and wakes
// the Writes waiting on it. The outcome is recorded first, so that a Write
// that returns an error already finds it in Err and Stats.
func (s *Sink) flush(batch []pending) {
	err := s.commit(batch)
	s.mu.Lock()
	if err != nil {
		s.failedLocked(err, uint64(len(batch)))
	} else {
		s.records += uint64(len(batch))
		if s.err != nil {
			s.since = s.now() // the disk has recovered
		}
		s.err = nil
	}
	s.mu.Unlock()
	if err != nil {
		s.logger.Error("usage: a batch of records was not written to the log", "error", err,
			"records", len(batch), "firstCounter", batch[0].rec.Counter, "lastCounter", batch[len(batch)-1].rec.Counter)
	}
	for _, p := range batch {
		p.done <- err
	}
}

// failedLocked records a failure that lost n records. The caller holds s.mu.
func (s *Sink) failedLocked(err error, n uint64) {
	if s.err == nil {
		s.since = s.now() // the failure starts
	}
	s.err = err
	s.failures++
	s.lost += n
}

// commit appends batch to the log and syncs it. A write that fails or falls
// short can leave a partial line, which the next record would join; so the log
// is cut back to its size before the batch, and the batch's counters show as a
// gap. A sync that fails leaves whole lines, and they are kept. The log is the
// file of the UTC day of this write: the first batch after midnight opens the
// next day's file and leaves the closed one as it is.
func (s *Sink) commit(batch []pending) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, p := range batch {
		if err := enc.Encode(p.rec); err != nil {
			return fmt.Errorf("usage: encode a record: %w", err)
		}
	}
	if s.torn {
		// The partial line is still there; writing now would join it.
		if err := s.cutBack(); err != nil {
			return err
		}
	}
	if err := s.rotate(); err != nil {
		return err
	}
	n, err := s.log.Write(buf.Bytes())
	if err == nil && n != buf.Len() {
		err = io.ErrShortWrite
	}
	if err != nil {
		err = fmt.Errorf("usage: write the log: %w", err)
		if cerr := s.cutBack(); cerr != nil {
			err = errors.Join(err, cerr)
		}
		return err
	}
	s.size += int64(n)
	if err := s.log.Sync(); err != nil {
		return fmt.Errorf("usage: sync the log: %w", err)
	}
	return nil
}

// rotate makes the log the file of the UTC day of this write, when it is not
// already. It only moves forward: a clock that went back keeps the newer
// file, so a closed file is never written again. A new file's directory is
// synced, so the file survives a crash. When it fails, the old file stays the
// log and the next batch tries again.
func (s *Sink) rotate() error {
	day := s.today()
	if s.log != nil && day <= s.day {
		return nil
	}
	path := filepath.Join(s.dir, dayFileName(day))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("usage: open %s: %w", path, err)
	}
	size, err := repairTail(f)
	if err != nil {
		f.Close()
		return fmt.Errorf("usage: check the end of %s: %w", path, err)
	}
	if err := s.h.syncDir(s.dir); err != nil {
		f.Close()
		return fmt.Errorf("usage: sync the directory after opening %s: %w", path, err)
	}
	if old := s.log; old != nil {
		if err := old.Close(); err != nil {
			s.logger.Error("usage: closing the previous day's file failed", "day", s.day, "error", err)
		}
	}
	s.log, s.day, s.size, s.torn = s.h.wrap(f), day, size, false
	return nil
}

// cutBack truncates the log to s.size and syncs it. When the truncate fails,
// the next commit tries again before it writes.
func (s *Sink) cutBack() error {
	if err := s.log.Truncate(s.size); err != nil {
		s.torn = true
		return fmt.Errorf("usage: cut the log back to its last whole line: %w", err)
	}
	s.torn = false
	if err := s.log.Sync(); err != nil {
		return fmt.Errorf("usage: sync the log after cutting it back: %w", err)
	}
	return nil
}

// Err returns the error of the latest batch of records, or of the latest
// counter block that failed to be persisted: nil once a later batch has been
// written and synced.
func (s *Sink) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Stats is the sink's state at one moment (HBEP-8, Usage records: Failures
// are never silent).
type Stats struct {
	// Records is how many records have been written and synced since Open.
	Records uint64
	// Lost is how many records were not written: those of a batch that
	// failed to be written or synced, and those that got no counter because
	// their block could not be persisted.
	Lost uint64
	// Failures is how many batches and counter blocks failed.
	Failures uint64
	// Err is the latest failure, nil once a later batch has been written and
	// synced.
	Err error
	// Since is when the sink entered its current state: Open, the first
	// failure of a run of them, or the first batch that succeeded after one.
	Since time.Time
}

// Stats returns the sink's counts and state.
func (s *Sink) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{Records: s.records, Lost: s.lost, Failures: s.failures, Err: s.err, Since: s.since}
}

// Close writes what is queued and closes the log. It returns the error of the
// latest batch, if that failed. After a run in which no batch failed it
// persists the last counter used, so that the next run continues from it
// without a gap: only a crash leaves one. After a failure, even one later
// recovered from, it leaves the block end, as a crash does.
func (s *Sink) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.wake)
	s.mu.Unlock()
	<-s.done
	err := s.Err()
	if err == nil && s.Failures() == 0 {
		err = s.persistLast()
	}
	if s.log != nil {
		if cerr := s.log.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

// Failures returns how many times the sink has failed to put records on disk
// since it was opened: batches that failed to be written or synced, and
// counter blocks that failed to be persisted. A failed batch counts once,
// whatever the number of records in it.
func (s *Sink) Failures() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failures
}

// persistLast writes the last counter used as the counter file's value, unless
// it is that already (it is the block end after a run that used a whole block,
// and what Open read after one that wrote nothing).
func (s *Sink) persistLast() error {
	s.mu.Lock()
	last, end := s.next-1, s.blockEnd
	s.mu.Unlock()
	if last == end {
		return nil
	}
	if err := writeCounter(filepath.Join(s.dir, CounterFile), last); err != nil {
		return fmt.Errorf("usage: persist the last counter: %w", err)
	}
	return nil
}

func readCounter(path string) (uint64, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("usage: %s does not hold a counter: %w", path, err)
	}
	return n, nil
}

// writeCounter persists n: a temporary file, fsync, rename, fsync of the
// directory, so the block end is on disk before its first counter is used.
func writeCounter(path string, n uint64) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".counter.tmp*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	fail := func(err error) error { tmp.Close(); os.Remove(name); return err }
	if err := tmp.Chmod(0o600); err != nil {
		return fail(err)
	}
	if _, err := tmp.WriteString(strconv.FormatUint(n, 10) + "\n"); err != nil {
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
	return syncDir(dir)
}

// syncDir syncs the directory dir, so that a file created in it survives a
// crash.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
