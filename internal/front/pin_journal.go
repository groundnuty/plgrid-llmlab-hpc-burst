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
// and syncing it. The store's memory is the authority once the journal has been read: a failed write never makes the store read the journal again, it makes
// the request that needed the write fail, until a write succeeds.

// loadLocked reads the journal into memory, drops a last line that a crash cut short (its request was never sent: a pin is dispatched only after its
// line is synced), converts every pin the journal shows in flight (live for one TTL from now) and compacts the journal when that, or anything else, changes
// it. A record it can't read is an error: a pin it can't see is a conversation it could move.
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
	byW, lines, err := replayPinRecords(raw)
	if err != nil {
		return err
	}
	converted, live := 0, 0
	for w, keys := range byW {
		for k, p := range keys {
			if p.busy {
				// The request that marked it died with the front at the latest: the pin expires one TTL from now, and that is journalled below.
				p.lastEnd, p.busy = now, false
				converted++
			}
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
	if created || converted > 0 || lines > live {
		if err := s.compactLocked(now); err != nil {
			if created || converted > 0 {
				return err // the journal does not show what the front serves by: no request is served on it
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

// replayPinRecords reads the journal's records in order into pins, and how many it read. A pin record makes the pin, in flight when it has no expiry; a busy
// record marks it in flight; an ext record sets its expiry, and one without an expiry is not a record.
func replayPinRecords(raw []byte) (map[types.UID]map[string]*pin, int, error) {
	byW := map[types.UID]map[string]*pin{}
	lines := 0
	for i, line := range bytes.Split(raw, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		bad := fmt.Errorf("line %d is not a pin record", i+1)
		var rec pinRecord
		if err := json.Unmarshal(line, &rec); err != nil || rec.Workload == "" || rec.Key == "" {
			return nil, 0, bad
		}
		lines++
		if byW[rec.Workload] == nil {
			byW[rec.Workload] = map[string]*pin{}
		}
		p := byW[rec.Workload][rec.Key]
		switch rec.Op {
		case "pin":
			ttl := rec.TTL.D()
			if ttl <= 0 || rec.MD == "" || (rec.ID == "" && rec.ManagedID == "") {
				return nil, 0, bad
			}
			p = &pin{target: rec.pinTarget, ttl: ttl, busy: rec.Expiry.IsZero()}
			if !p.busy {
				p.lastEnd, p.extended = rec.Expiry.Add(-ttl), rec.Expiry.Add(-ttl)
			}
			byW[rec.Workload][rec.Key] = p
		case "busy":
			if p != nil {
				p.busy = true
			}
		case "ext":
			if rec.Expiry.IsZero() {
				return nil, 0, bad
			}
			if p != nil {
				p.lastEnd, p.extended, p.busy = rec.Expiry.Add(-p.ttl), rec.Expiry.Add(-p.ttl), false
			}
		default:
			return nil, 0, bad
		}
	}
	return byW, lines, nil
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
// leaves the old file or the new. A pin with a request in flight is written in flight, an idle one with its expiry. What the pins record as journalled is
// set only once the rename has made the new file the journal. The directory is then synced, so the rename is on disk; when that fails the store appends
// nothing until it succeeds (appendLocked), since a crash could bring back the old journal without the records that came after.
func (s *pinStore) compactLocked(now time.Time) error {
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".sticky-pins.tmp*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	var written []*pin
	var buf bytes.Buffer
	for w, keys := range s.byW {
		for k, p := range keys {
			if !p.live(now) {
				delete(keys, k)
				continue
			}
			rec := pinRecord{Op: "pin", Workload: w, Key: k, pinTarget: p.target, TTL: Duration(p.ttl)}
			if p.inflight == 0 {
				rec.Expiry = p.lastEnd.Add(p.ttl)
			}
			line, err := json.Marshal(rec)
			if err != nil {
				_ = tmp.Close()
				return err
			}
			buf.Write(line)
			buf.WriteByte('\n')
			written = append(written, p)
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
	for _, p := range written {
		if p.busy = p.inflight > 0; !p.busy {
			p.extended = now
		}
	}
	if s.f != nil {
		_ = s.f.Close()
		s.f = nil
	}
	s.lines, s.broken = len(written), false
	openErr := s.openLocked()
	if syncErr := s.syncDirFn(dir); syncErr != nil {
		s.dirDirty = true
		return syncErr
	}
	s.dirDirty = false
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

// appendLocked writes one record and syncs it. It first makes the journal fit for it: a directory that a compaction could not sync is synced again, and a
// journal that a failed write could not be cut back is cut back, and when either fails the append fails. A write that fails is taken back, so a torn line
// never sits in the middle of the journal.
func (s *pinStore) appendLocked(rec pinRecord) error {
	if s.failWrites != nil {
		return s.failWrites
	}
	line, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if s.dirDirty {
		if err := s.syncDirFn(filepath.Dir(s.path)); err != nil {
			return fmt.Errorf("the pin journal's directory is not synced: %w", err)
		}
		s.dirDirty = false
	}
	if s.f == nil || s.broken {
		if err := s.reopenLocked(); err != nil {
			return err
		}
	}
	_, err = s.f.Write(append(line, '\n'))
	if err == nil {
		err = s.f.Sync()
	}
	if err != nil {
		if terr := s.f.Truncate(s.size); terr != nil {
			s.broken = true
		}
		return err
	}
	s.size += int64(len(line)) + 1
	s.lines++
	if s.appended != nil {
		s.appended(rec)
	}
	return nil
}

// reopenLocked opens the journal again, and when a failed append could not be taken back cuts it back to its size at the last whole record.
func (s *pinStore) reopenLocked() error {
	good, broken := s.size, s.broken
	if s.f != nil {
		_ = s.f.Close()
		s.f = nil
	}
	if err := s.openLocked(); err != nil {
		return err
	}
	if !broken {
		return nil
	}
	cur := s.size
	s.size = good // the last whole record's end, whatever the file holds now
	if cur < good {
		return errors.New("the pin journal is shorter than its last whole record")
	}
	if cur > good {
		if err := s.f.Truncate(good); err != nil {
			return err
		}
	}
	s.broken = false
	return nil
}
