// Package usage writes the front's usage records (HBEP-5, Usage records;
// HBEP-8): one JSON line per request, synced to disk before the next, with a
// monotonic counter persisted apart from the log in blocks, so that records
// lost in a crash show as a gap. In alpha the records are also the
// attribution log (HBEP-9). No record holds a prompt, a response or a key.
package usage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	LogFile     = "usage.jsonl"
	CounterFile = "counter"
	// BlockSize is how many counters the front reserves at a time.
	BlockSize = 1000
)

// logFile is the part of *os.File the sink uses. The tests put a file that
// fails in its place.
type logFile interface {
	Write([]byte) (int, error)
	Sync() error
	Truncate(int64) error
	Close() error
}

// Sink appends records to <dir>/usage.jsonl. Write assigns the counter and
// queues the record; one goroutine writes what is queued in one write and
// one fsync, so records are batched only when they arrive faster than the
// disk syncs (HBEP-5, Sink in alpha).
type Sink struct {
	dir string
	log logFile

	mu       sync.Mutex
	next     uint64 // the next counter to use
	blockEnd uint64 // the last counter persisted as reserved
	queue    []Record
	wake     chan struct{}
	closed   bool
	done     chan struct{}
	err      error // the first write or sync error; it is never cleared

	// Only the writer goroutine uses these two after Open.
	size int64 // the log's size when it holds only whole lines
	torn bool  // a failed write left a partial line that could not be cut off
}

// Open opens the sink in dir, creating it (mode 0700). The first counter is
// the persisted block end plus one, so a crash shows as a gap (HBEP-8). A log
// that ends in the middle of a line, as a crash during a write leaves it, is
// cut back to its last whole line, so that the next record starts a line of
// its own; the record that was cut shows as a gap too.
func Open(dir string) (*Sink, error) {
	return open(dir, func(f *os.File) logFile { return f })
}

// open is Open with the log file passed through wrap, which is how the tests
// make the disk fail.
func open(dir string, wrap func(*os.File) logFile) (*Sink, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	end, err := readCounter(filepath.Join(dir, CounterFile))
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, LogFile), os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	size, err := repairTail(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("usage: repair the end of %s: %w", filepath.Join(dir, LogFile), err)
	}
	s := &Sink{dir: dir, log: wrap(f), size: size, next: end + 1, blockEnd: end, wake: make(chan struct{}, 1), done: make(chan struct{})}
	go s.run()
	return s, nil
}

// Write assigns r its counter and queues it. It returns the counter, or an
// error when the counter block could not be persisted or the sink is closed.
// Once a write or sync of the log has failed, it returns that error with the
// counter: the failure stays reported, and the record is still queued, to be
// written if the disk has recovered.
func (s *Sink) Write(r Record) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errors.New("usage: the sink is closed")
	}
	if s.next > s.blockEnd {
		end := s.blockEnd + BlockSize
		if err := writeCounter(filepath.Join(s.dir, CounterFile), end); err != nil {
			return 0, fmt.Errorf("usage: persist the counter block: %w", err)
		}
		s.blockEnd = end
	}
	r.Counter = s.next
	s.next++
	s.queue = append(s.queue, r)
	select {
	case s.wake <- struct{}{}:
	default:
	}
	if s.err != nil {
		return r.Counter, fmt.Errorf("usage: the log failed earlier: %w", s.err)
	}
	return r.Counter, nil
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

// flush appends batch to the log and syncs it. A write that fails or falls
// short can leave a partial line, which the next record would join; so the log
// is cut back to its size before the batch, and the batch's counters show as a
// gap. A sync that fails leaves whole lines and they are kept.
func (s *Sink) flush(batch []Record) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, r := range batch {
		if err := enc.Encode(r); err != nil {
			s.fail(err)
			return
		}
	}
	if s.torn && !s.cutBack() {
		return // the partial line is still there; writing now would join it
	}
	n, err := s.log.Write(buf.Bytes())
	if err == nil && n != buf.Len() {
		err = io.ErrShortWrite
	}
	if err != nil {
		s.fail(err)
		s.cutBack()
		return
	}
	s.size += int64(n)
	if err := s.log.Sync(); err != nil {
		s.fail(err)
	}
}

// cutBack truncates the log to s.size and syncs it. It reports whether the
// log is back to whole lines; when the truncate fails, the next flush tries
// again before it writes.
func (s *Sink) cutBack() bool {
	if err := s.log.Truncate(s.size); err != nil {
		s.torn = true
		s.fail(err)
		return false
	}
	s.torn = false
	if err := s.log.Sync(); err != nil {
		s.fail(err)
	}
	return true
}

func (s *Sink) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

// Err returns the first write or sync error.
func (s *Sink) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Close writes what is queued and closes the log. After a clean run it
// persists the last counter used, so that the next run continues from it
// without a gap: only a crash leaves one. After a write or sync error it
// leaves the block end, so that what the failure dropped still shows.
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
	if err == nil {
		err = s.persistLast()
	}
	if cerr := s.log.Close(); err == nil {
		err = cerr
	}
	return err
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
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
