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
// force when it was made, which an edit of the Workload never changes) and its expiry. An "ext" record extends a pin's expiry. A later "pin" of the same
// Workload and key supersedes the earlier one, as when a pin expired and the conversation came back. The journal is compacted, dropping expired pins
// and superseded records, when it is loaded and whenever it grows past twice its live size.
//
// LOCKS. A key's lock (lockKey) is the outermost: it is held by a conversation's first request from its decision to its pin, possibly across a wait in the
// Workload's FIFO, so it is taken with the request's own context. The store's mutex comes after it and is a leaf of everything else: it is never held
// while a Workload's mutex, the router's or a group's is taken, and the router's Workload mutex is never taken under it.

// pinJournalFile is the pin journal's fixed name, in Config.UsageDir.
const pinJournalFile = "sticky-pins.jsonl"

// pinExtendEvery is how often a pin's extension is journalled at most: the journal never trails a pin's last request by more than this.
const pinExtendEvery = time.Minute

// pinTarget is where a pin sends its conversation: a ModelDeployment, by UID, at one full deployment ID, or a managed target, by Provider and managed ID.
// It is comparable, and the identity of a pin's target.
type pinTarget struct {
	MD        types.UID `json:"md,omitempty"` // "" for a managed target
	ID        string    `json:"id,omitempty"` // the ModelDeployment's full deployment ID
	Provider  string    `json:"provider,omitempty"`
	ManagedID string    `json:"managedID,omitempty"`
}

func (t pinTarget) managed() bool { return t.MD == "" }

// pin is one conversation's pin. Its fields below target are guarded by the store's mutex.
type pin struct {
	target pinTarget
	ttl    time.Duration
	// inflight is the conversation's requests in flight, a waiting one included. A pin never expires while any is.
	inflight int
	// lastEnd is the end of the conversation's last request, or the time of the event that made or extended the pin: it expires ttl after it.
	lastEnd time.Time
	// journalled is when the journal last got a record of this pin, which limits how often an extension is written.
	journalled time.Time
}

func (p *pin) live(now time.Time) bool { return p.inflight > 0 || now.Before(p.lastEnd.Add(p.ttl)) }

// expiry is the time the pin would expire at if its last request had ended now.
func (p *pin) expiry(now time.Time) time.Time {
	if p.inflight > 0 {
		return now.Add(p.ttl)
	}
	return p.lastEnd.Add(p.ttl)
}

// pinRecord is one line of the journal.
type pinRecord struct {
	Op       string    `json:"op"` // "pin" or "ext"
	Workload types.UID `json:"workload"`
	Key      string    `json:"key"` // the SHA-256 of the session key, in hex
	pinTarget
	TTL    Duration  `json:"ttl,omitempty"`
	Expiry time.Time `json:"expiry"`
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
	failWrites   error
	beforeCreate func()
}

type keyLock struct {
	ch   chan struct{}
	refs int
}

func newPinStore(path string, now func() time.Time, log *slog.Logger) *pinStore {
	return &pinStore{path: path, now: now, log: log, byW: map[types.UID]map[string]*pin{}, locks: map[string]*keyLock{}}
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

// arrive counts a request of a conversation in flight on its live pin, if it has one, and extends the pin. The extension is journalled at most once a
// minute; a write that fails is an error, and the request is not counted: the journal never trails by more than a minute because a request that
// would make it trail is refused (HBEP-5, Sticky routes, Lifetime). The caller holds the conversation's lock.
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
	if now.Sub(p.journalled) >= pinExtendEvery {
		if err := s.appendLocked(pinRecord{Op: "ext", Workload: w, Key: hash, Expiry: now.Add(p.ttl)}); err != nil {
			return nil, err
		}
		p.journalled = now
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
	if err := s.appendLocked(pinRecord{Op: "pin", Workload: w, Key: hash, pinTarget: target, TTL: Duration(ttl), Expiry: now.Add(ttl)}); err != nil {
		return nil, err
	}
	p := &pin{target: target, ttl: ttl, inflight: 1, lastEnd: now, journalled: now}
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

// finish ends one request of the conversation. The pin then expires ttl after now, and that extension is journalled when the last record of the pin is
// more than a minute old. A write that fails here is logged, not an answer: the request has been served, and the next request of the conversation
// writes it, or fails.
func (s *pinStore) finish(w types.UID, hash string, p *pin) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if p.inflight--; p.inflight < 0 {
		p.inflight = 0
	}
	p.lastEnd = now
	if p.inflight > 0 || s.byW[w][hash] != p || now.Sub(p.journalled) < pinExtendEvery || !s.loaded {
		return
	}
	if err := s.appendLocked(pinRecord{Op: "ext", Workload: w, Key: hash, Expiry: now.Add(p.ttl)}); err != nil {
		s.log.Warn("a pin's extension could not be journalled at the end of its request", "error", err.Error())
		return
	}
	p.journalled = now
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
