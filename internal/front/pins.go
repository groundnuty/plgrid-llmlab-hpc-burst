package front

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// The sticky routes' pins and their journal (HBEP-5, Workloads, Sticky routes).
//
// A Workload with sessionAffinity Route pins each conversation, on its first request, to one target: one of our ModelDeployments at the deployment ID of
// the Instance that took the request, or a managed target. A pin is appended to the pin journal and synced BEFORE the request is dispatched, so a restart
// reloads every pin a request was ever sent under. The journal is one file in the usage directory, an existing front directory, so there is no new
// setting; its name, pinJournalFile, matches none of the usage files' (usage.jsonl, usage-<day>.jsonl, counter), which usage.Files lists by name.
//
// Each line is one JSON record. A "pin" record makes a pin: the Workload's UID, the SHA-256 of the session key, the target, the pin's own TTL (the one in
// force when it was made, which an edit of the Workload never changes), and, when the pin is idle, its expiry; one with no expiry marks the pin in flight,
// as its first request is. A "busy" record marks an idle pin in flight again, and is written before a request of it is dispatched, so a failed write fails
// the request (HBEP-5, Sticky routes, Lifetime). An "ext" record, written when the pin's last request ends, sets its expiry: the end plus the TTL, at most
// once a minute. A later "pin" of the same Workload and key supersedes the earlier one, as when a pin expired and the conversation came back. A pin that a
// reload finds marked in flight is live, since its request died with the front at the latest: it expires one TTL after the reload, and the reload
// journals that, so a second restart does not extend it again. The journal is compacted, dropping expired pins and superseded records, when it is loaded
// and whenever it grows past twice its live size.
//
// LOCKS. A key's lock (lockKey) is the outermost: it is held by a conversation's first request from its decision to its pin, possibly across a wait in the
// Workload's FIFO, so it is taken with the request's own context. The store's mutex comes after it and is a leaf of everything else: it is never held
// while a Workload's mutex, the router's or a group's is taken, and the router's Workload mutex is never taken under it.

// pinJournalFile is the pin journal's fixed name, in Config.UsageDir.
const pinJournalFile = "sticky-pins.jsonl"

// pinExtendEvery is how often a pin's expiry is journalled at most, so the journal never trails a pin's last request by more than this: after its last
// request a pin shows in the journal in flight, which is live, until the expiry is journalled.
const pinExtendEvery = time.Minute

// pinTarget is where a pin sends its conversation: a ModelDeployment, by UID, at one full deployment ID, or a managed target, by the UID of its
// managed-source ModelDeployment, its Provider and its managed ID, so that deleting the source ends the pin whatever other source lists the same entry.
// It is comparable, and the identity of a pin's target.
type pinTarget struct {
	MD        types.UID `json:"md"`
	ID        string    `json:"id,omitempty"` // the ModelDeployment's full deployment ID
	Provider  string    `json:"provider,omitempty"`
	ManagedID string    `json:"managedID,omitempty"`
}

func (t pinTarget) managed() bool { return t.ManagedID != "" }

// pin is one conversation's pin. Its fields below target are guarded by the store's mutex.
type pin struct {
	target pinTarget
	ttl    time.Duration
	// inflight is the conversation's requests in flight, a waiting one included. A pin never expires while any is.
	inflight int
	// lastEnd is the end of the conversation's last request, or the time of the event that made or extended the pin: it expires ttl after it.
	lastEnd time.Time
	// busy says the journal's last record of the pin marks it in flight (a pin or busy record), so that a crash leaves it live; it is cleared when the pin's
	// expiry is journalled. extended is when that expiry was last journalled (zero: never), which limits how often it is.
	busy     bool
	extended time.Time
}

func (p *pin) live(now time.Time) bool { return p.inflight > 0 || now.Before(p.lastEnd.Add(p.ttl)) }

// pinRecord is one line of the journal.
type pinRecord struct {
	Op       string    `json:"op"` // "pin", "busy" or "ext"
	Workload types.UID `json:"workload"`
	Key      string    `json:"key"` // the SHA-256 of the session key, in hex
	pinTarget
	TTL    Duration  `json:"ttl,omitempty"`
	Expiry time.Time `json:"expiry,omitzero"` // an ext's, and an idle pin's; none marks a pin in flight
}

// keyHash is the journal's name for a session key: its SHA-256 in hex, so no key is written down.
func keyHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// pinStore holds every live pin in memory, and the journal they were written to.
type pinStore struct {
	path string
	now  func() time.Time
	log  *slog.Logger

	mu     sync.Mutex
	loaded bool
	f      *os.File
	size   int64 // the journal's size when it was last written, for taking back a failed append
	lines  int   // the journal's records
	byW    map[types.UID]map[string]*pin
	locks  map[string]*keyLock
	swept  time.Time
	// failWrites, set by tests only, makes every append fail with it. beforeCreate, set by tests only, runs at the start of create, with no lock of the
	// store held: it holds a conversation's first request between its target's choice and its pin.
	// appended, set by tests only, is told each record that has been written and synced.
	failWrites   error
	beforeCreate func()
	appended     func(pinRecord)
	// broken says a failed append could not be taken back, so the journal may end in a torn line: no record is appended until it is cut back. The store's
	// memory stays the authority meanwhile (never replaced by a read of a journal that may lag it), and every request that needs a write is refused.
	broken bool
	// dirDirty says a rename made by a compaction is not known to be synced: no record is appended until the directory syncs.
	dirDirty  bool
	syncDirFn func(string) error
}

type keyLock struct {
	ch   chan struct{}
	refs int
}

func newPinStore(path string, now func() time.Time, log *slog.Logger) *pinStore {
	if log == nil {
		log = slog.Default()
	}
	return &pinStore{path: path, now: now, log: log, byW: map[types.UID]map[string]*pin{}, locks: map[string]*keyLock{}, syncDirFn: syncDir}
}

// lockKey takes the conversation's lock, or gives up when ctx ends.
func (s *pinStore) lockKey(ctx context.Context, w types.UID, hash string) (unlock func(), err error) {
	k := string(w) + "/" + hash
	s.mu.Lock()
	l := s.locks[k]
	if l == nil {
		l = &keyLock{ch: make(chan struct{}, 1)}
		s.locks[k] = l
	}
	l.refs++
	s.mu.Unlock()
	drop := func() {
		s.mu.Lock()
		if l.refs--; l.refs == 0 {
			delete(s.locks, k)
		}
		s.mu.Unlock()
	}
	select {
	case l.ch <- struct{}{}:
		return func() { <-l.ch; drop() }, nil
	case <-ctx.Done():
		drop()
		return nil, ctx.Err()
	}
}

// ensure loads the journal if it has not been, and says whether the store can serve: an unreadable journal is an error, and the next call reads it again.
// It runs before any request of a Workload with sessionAffinity is served (HBEP-5, Sticky routes, The journal).
func (s *pinStore) ensure() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ensureLocked()
}

func (s *pinStore) ensureLocked() error {
	if s.loaded {
		return nil
	}
	if s.f != nil {
		_ = s.f.Close()
		s.f = nil
	}
	if err := s.loadLocked(); err != nil {
		return fmt.Errorf("the pin journal can't be read: %w", err)
	}
	s.loaded = true
	return nil
}

// sweepLocked drops the pins that have expired, at most once a minute.
func (s *pinStore) sweepLocked(now time.Time) {
	if now.Sub(s.swept) < pinExtendEvery {
		return
	}
	s.swept = now
	for w, keys := range s.byW {
		for k, p := range keys {
			if !p.live(now) {
				delete(keys, k)
			}
		}
		if len(keys) == 0 {
			delete(s.byW, w)
		}
	}
}

// compactIfLargeLocked compacts the journal when it has grown past twice its live size (HBEP-5, Sticky routes, The journal). The live size is the pins in
// memory, which a sweep keeps within a minute of the live ones. A failed compaction costs only the journal's size, which is still whole.
func (s *pinStore) compactIfLargeLocked(now time.Time) {
	live := 0
	for _, keys := range s.byW {
		live += len(keys)
	}
	if s.lines > 2*live {
		if err := s.compactLocked(now); err != nil {
			s.log.Warn("the pin journal could not be compacted", "error", err.Error())
		}
	}
}

// atMaxLocked says whether the Workload holds maxPins live pins. It counts only when its pins in memory reach the bound, and then drops the expired ones
// first, so a Workload below it pays nothing for the check.
func (s *pinStore) atMaxLocked(w types.UID, maxPins int, now time.Time) bool {
	if len(s.byW[w]) < maxPins {
		return false
	}
	for k, p := range s.byW[w] {
		if !p.live(now) {
			delete(s.byW[w], k)
		}
	}
	return len(s.byW[w]) >= maxPins
}

// arrive counts a request of a conversation in flight on its live pin, if it has one. When the journal does not already show the pin in flight it first
// journals that, and a write that fails is an error: the request is not counted and not dispatched (HBEP-5, Sticky routes, Lifetime: an extension that can't
// be written fails its request). The caller holds the conversation's lock.
func (s *pinStore) arrive(w types.UID, hash string) (*pin, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureLocked(); err != nil {
		return nil, err
	}
	now := s.now()
	p := s.byW[w][hash]
	if p == nil {
		return nil, nil
	}
	if !p.live(now) {
		delete(s.byW[w], hash)
		return nil, nil
	}
	if !p.busy {
		if err := s.appendLocked(pinRecord{Op: "busy", Workload: w, Key: hash}); err != nil {
			return nil, err
		}
		p.busy = true
	}
	p.inflight++
	p.lastEnd = now
	s.sweepLocked(now)
	s.compactIfLargeLocked(now)
	return p, nil
}

// errMaxPins is the Workload at its maxPins.
var errMaxPins = errors.New("the Workload holds its maxPins live pins")

// create appends the pin of a conversation's first request to the journal and syncs it, and only then holds it in memory, with the request in flight on
// it. A Workload at its maxPins takes no new pin and its live pins are kept. The caller holds the conversation's lock.
func (s *pinStore) create(w types.UID, hash string, target pinTarget, ttl time.Duration, maxPins int) (*pin, error) {
	if h := s.beforeCreate; h != nil {
		h()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureLocked(); err != nil {
		return nil, err
	}
	now := s.now()
	if s.atMaxLocked(w, maxPins, now) {
		return nil, errMaxPins
	}
	if err := s.appendLocked(pinRecord{Op: "pin", Workload: w, Key: hash, pinTarget: target, TTL: Duration(ttl)}); err != nil {
		return nil, err
	}
	p := &pin{target: target, ttl: ttl, inflight: 1, lastEnd: now, busy: true}
	if s.byW[w] == nil {
		s.byW[w] = map[string]*pin{}
	}
	s.byW[w][hash] = p
	s.sweepLocked(now)
	s.compactIfLargeLocked(now)
	return p, nil
}

// reserved says whether the Workload is at its maxPins now: a first request that finds it so is refused before it takes a target.
func (s *pinStore) full(w types.UID, maxPins int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.atMaxLocked(w, maxPins, s.now())
}

// finish ends one request of the conversation. When it is the last in flight, the pin expires ttl after now, and that is journalled when the last expiry
// written for the pin is more than a minute old. A write that fails here is logged, not an answer: the request has been served, and the journal then still shows the pin
// in flight, which keeps it live, the safe direction.
func (s *pinStore) finish(w types.UID, hash string, p *pin) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if p.inflight--; p.inflight < 0 {
		p.inflight = 0
	}
	p.lastEnd = now
	if p.inflight > 0 || s.byW[w][hash] != p || !p.busy || now.Sub(p.extended) < pinExtendEvery || !s.loaded {
		return
	}
	if err := s.appendLocked(pinRecord{Op: "ext", Workload: w, Key: hash, Expiry: now.Add(p.ttl)}); err != nil {
		s.log.Warn("a pin's expiry could not be journalled at the end of its request", "error", err.Error())
		return
	}
	p.busy, p.extended = false, now
	s.sweepLocked(now)
	s.compactIfLargeLocked(now)
}

// close closes the journal.
func (s *pinStore) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f != nil {
		_ = s.f.Close()
		s.f = nil
	}
	s.loaded = false
}

// pinRef is one request's hold on its conversation's pin: done ends it, once.
type pinRef struct {
	s    *pinStore
	w    types.UID
	hash string
	p    *pin
	once sync.Once
}

func (r *pinRef) done() {
	if r == nil {
		return
	}
	r.once.Do(func() { r.s.finish(r.w, r.hash, r.p) })
}
