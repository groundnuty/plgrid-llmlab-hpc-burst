package front

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// The pin journal's file I/O (pins.go has the format and the store): reading it at the first sticky request, rewriting it compacted, and appending a record
// and syncing it.

// loadLocked reads the journal into memory, drops a last line that a crash cut short (its request was never sent: a pin is dispatched only after its
// line is synced), and compacts it when that drops anything. Any other line it can't read is an error: a pin it can't see is a conversation it could move.
func (s *pinStore) loadLocked() error {
	if s.path == "" {
		return errors.New("no usage directory")
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	_, statErr := os.Stat(s.path)
	created := errors.Is(statErr, os.ErrNotExist)
	raw, err := os.ReadFile(s.path)
	if err != nil && !created {
		return err
	}
	if cut := bytes.LastIndexByte(raw, '\n') + 1; cut != len(raw) {
		raw = raw[:cut]
		if err := os.Truncate(s.path, int64(cut)); err != nil {
			return err
		}
	}
	now := s.now()
	byW := map[types.UID]map[string]*pin{}
	lines := 0
	for i, line := range bytes.Split(raw, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var rec pinRecord
		if err := json.Unmarshal(line, &rec); err != nil || rec.Workload == "" || rec.Key == "" || (rec.Op != "pin" && rec.Op != "ext") {
			return fmt.Errorf("line %d is not a pin record", i+1)
		}
		lines++
		if byW[rec.Workload] == nil {
			byW[rec.Workload] = map[string]*pin{}
		}
		switch p := byW[rec.Workload][rec.Key]; {
		case rec.Op == "pin":
			ttl := rec.TTL.D()
			if ttl <= 0 || (rec.MD == "" && rec.ManagedID == "") {
				return fmt.Errorf("line %d is not a pin record", i+1)
			}
			byW[rec.Workload][rec.Key] = &pin{target: rec.pinTarget, ttl: ttl, lastEnd: rec.Expiry.Add(-ttl), journalled: rec.Expiry.Add(-ttl)}
		case p != nil:
			p.lastEnd, p.journalled = rec.Expiry.Add(-p.ttl), rec.Expiry.Add(-p.ttl)
		}
	}
	live := 0
	for w, keys := range byW {
		for k, p := range keys {
			if !p.live(now) {
				delete(keys, k)
				continue
			}
			live++
		}
		if len(keys) == 0 {
			delete(byW, w)
		}
	}
	s.byW, s.lines, s.swept = byW, lines, now
	if created || lines > live {
		if err := s.compactLocked(now); err != nil {
			if created {
				return err
			}
			// The old journal is whole and still serves: a failed compaction costs only its size.
			s.log.Warn("the pin journal could not be compacted at start", "error", err.Error())
		}
	}
	if s.f == nil {
		if err := s.openLocked(); err != nil {
			return err
		}
	}
	return nil
}

// openLocked opens the journal for appending and learns its size.
func (s *pinStore) openLocked() error {
	f, err := os.OpenFile(s.path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	s.f, s.size = f, st.Size()
	return nil
}

// compactLocked writes the live pins, one record each, to a new file and renames it over the journal, so the journal is never half-written: a crash
// leaves the old file or the new. It then syncs the directory, so the rename is on disk.
func (s *pinStore) compactLocked(now time.Time) error {
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".sticky-pins.tmp*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	lines := 0
	var buf bytes.Buffer
	for w, keys := range s.byW {
		for k, p := range keys {
			if !p.live(now) {
				delete(keys, k)
				continue
			}
			line, err := json.Marshal(pinRecord{Op: "pin", Workload: w, Key: k, pinTarget: p.target, TTL: Duration(p.ttl), Expiry: p.expiry(now)})
			if err != nil {
				_ = tmp.Close()
				return err
			}
			buf.Write(line)
			buf.WriteByte('\n')
			lines++
			p.journalled = now
		}
		if len(keys) == 0 {
			delete(s.byW, w)
		}
	}
	if _, err := tmp.Write(buf.Bytes()); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), s.path); err != nil {
		return err
	}
	if s.f != nil {
		_ = s.f.Close()
		s.f = nil
	}
	s.lines = lines
	openErr := s.openLocked()
	if syncErr := syncDir(dir); openErr == nil {
		return syncErr
	}
	return openErr
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// appendLocked writes one record and syncs it. A failed write is taken back, so a torn line never sits in the middle of the journal; when it can't be,
// the store reads the journal again before it serves the next request.
func (s *pinStore) appendLocked(rec pinRecord) error {
	if s.failWrites != nil {
		return s.failWrites
	}
	line, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if s.f == nil {
		s.loaded = false // read again, which opens it
		return errors.New("the pin journal is not open")
	}
	_, err = s.f.Write(append(line, '\n'))
	if err == nil {
		err = s.f.Sync()
	}
	if err != nil {
		if terr := s.f.Truncate(s.size); terr != nil {
			s.loaded = false
		}
		return err
	}
	s.size += int64(len(line)) + 1
	s.lines++
	return nil
}
