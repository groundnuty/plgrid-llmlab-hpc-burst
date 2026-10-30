package fakeslurm

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"syscall"
	"time"
)

// Event is one line of the FAKE_SLURM_LOG file.
type Event struct {
	TS     string   `json:"ts"`
	UnixMS int64    `json:"unix_ms"`
	Event  string   `json:"event"`
	Cmd    string   `json:"cmd"`
	Argv   []string `json:"argv"`
	Caller string   `json:"caller"`
	PID    int      `json:"pid"`
	JobID  int      `json:"job_id,omitempty"`
	Exit   *int     `json:"exit"`
}

// tx is one locked transaction: the loaded state plus the events to append.
type tx struct {
	app    *app
	st     *State
	dirty  bool
	events []Event
}

// emit queues a log event; exit nil logs null (sbatch_start).
func (t *tx) emit(event, cmd string, jobID int, exit *int) {
	now := t.app.env.Now().UTC()
	t.events = append(t.events, Event{
		TS: now.Format(time.RFC3339Nano), UnixMS: now.UnixMilli(),
		Event: event, Cmd: cmd, Argv: t.app.argv, Caller: t.app.cfg.Caller,
		PID: t.app.env.Pid, JobID: jobID, Exit: exit,
	})
}

func intp(i int) *int { return &i }

// locked runs fn under an exclusive flock on <state>.lock. It opens its own
// lock file descriptor (flock excludes between descriptors, so goroutines of
// one process exclude each other too), loads the state, runs fn, then writes
// the state atomically if fn marked it dirty and appends the queued events.
// The lock is released before locked returns; callers sleep or block only
// after that.
func (a *app) locked(fn func(t *tx) error) error {
	lf, err := os.OpenFile(a.cfg.StatePath+".lock", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("open lock: %w", err)
	}
	defer lf.Close() // closing the descriptor releases the flock
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("flock: %w", err)
	}
	if a.env.InLock != nil {
		defer a.env.InLock()() // the returned func runs while the lock is still held
	}
	st, err := loadState(a.cfg.StatePath)
	if err != nil {
		return err
	}
	t := &tx{app: a, st: st}
	ferr := fn(t)
	if t.dirty {
		if err := saveState(a.cfg.StatePath, st); err != nil {
			return err
		}
	}
	if err := appendEvents(a.cfg.LogPath, t.events); err != nil {
		return err
	}
	return ferr
}

func loadState(path string) (*State, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return newState(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state: %w", err)
	}
	st := newState()
	if len(b) == 0 {
		return st, nil
	}
	if err := json.Unmarshal(b, st); err != nil {
		return nil, fmt.Errorf("parse state %s: %w", path, err)
	}
	return st, nil
}

func saveState(path string, st *State) error {
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, b)
}

// writeFileAtomic writes a temp file in the same directory and renames it.
func writeFileAtomic(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, os.Getpid())
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

func appendEvents(path string, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open log: %w", err)
	}
	defer f.Close()
	var buf []byte
	for _, ev := range events {
		b, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		buf = append(append(buf, b...), '\n')
	}
	_, err = f.Write(buf)
	return err
}

// readEvents decodes the log; a missing file is an empty log. Callers hold
// the lock.
func readEvents(path string) ([]Event, error) {
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Event
	dec := json.NewDecoder(bufio.NewReader(f))
	for {
		var ev Event
		if err := dec.Decode(&ev); err == io.EOF {
			return out, nil
		} else if err != nil {
			return nil, fmt.Errorf("parse log %s: %w", path, err)
		}
		out = append(out, ev)
	}
}
