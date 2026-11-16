package forward

import "time"

// Stats is the forwarder's state at one moment.
type Stats struct {
	// Connected is true while the forwarder holds a connection to the log and its last look at the
	// stream and its last publish succeeded. It is false from the start until the first connection,
	// and from any failure until the next.
	Connected bool
	// LoggedKnown is false until the forwarder has read the stream's last counter (or chosen where
	// to start on an empty one); Logged is meaningless before that. A gauge built from it has no
	// value then: unknown is never 0 (HBEP-8, Unknown samples).
	LoggedKnown bool
	// Logged is the last counter the stream holds as far as the forwarder knows.
	Logged uint64
	// Synced is the last counter the sink has synced.
	Synced uint64
	// Unlogged is Synced less Logged, never below 0: HBEP-8's hpcburst_usage_unlogged_records. It is
	// 0 while LoggedKnown is false, which is not a reading of 0.
	Unlogged uint64
	// BehindSince is when Unlogged last rose above 0 and has stayed there; zero while it is 0, or
	// while Logged is unknown.
	BehindSince time.Time
	// Failures counts the connection, resume and publish attempts that failed.
	Failures uint64
	// Dropped counts the batches the sink offered while the channel was full.
	Dropped uint64
	// FromChannel and FromFiles count the messages published from each source.
	FromChannel, FromFiles uint64
	// Skipped counts the records passed over because their line was longer than a message.
	Skipped uint64
	// LastError is the latest failure, empty after a success.
	LastError string
}

// Stats returns the forwarder's state.
func (f *Forwarder) Stats() Stats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return Stats{Connected: f.connected, LoggedKnown: f.loggedKnown, Logged: f.logged, Synced: f.synced,
		Unlogged: f.unloggedLocked(), BehindSince: f.behindSince, Failures: f.failures, Dropped: f.dropped,
		FromChannel: f.fromChannel, FromFiles: f.fromFiles, Skipped: f.skipped, LastError: f.lastErr}
}

// unloggedLocked is the synced counter less the logged one, and 0 while the logged one is unknown:
// a gauge built from it has no value then (Stats.LoggedKnown).
func (f *Forwarder) unloggedLocked() uint64 {
	if !f.loggedKnown || f.synced <= f.logged {
		return 0
	}
	return f.synced - f.logged
}

// recomputeLocked keeps BehindSince: set when the unlogged count first rises above 0, cleared when
// it falls to 0. It runs at each change of Synced or Logged, so the time is when the forwarder
// fell behind, not when someone looked.
func (f *Forwarder) recomputeLocked() {
	if f.loggedKnown && f.unloggedLocked() > 0 {
		if f.behindSince.IsZero() {
			f.behindSince = f.now()
		}
		return
	}
	f.behindSince = time.Time{}
}

func (f *Forwarder) setLogged(n uint64) {
	f.mu.Lock()
	f.logged, f.loggedKnown = n, true
	f.recomputeLocked()
	f.mu.Unlock()
}

func (f *Forwarder) syncedNow() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.synced
}

// setUp marks the forwarder connected and clears its last error.
func (f *Forwarder) setUp() {
	f.mu.Lock()
	was := f.connected
	f.connected, f.lastErr = true, ""
	f.mu.Unlock()
	if !was {
		f.log.Info("usage forwarder: connected to the log", "addr", f.o.Addr)
	}
}

// setDown marks the forwarder not connected after err, and says so, once for a failure and again
// each warnEvery while it lasts.
func (f *Forwarder) setDown(err error) {
	f.mu.Lock()
	f.connected = false
	f.failures++
	f.lastErr = err.Error()
	warn := f.lastErr != f.warnedErr || f.now().Sub(f.warnedAt) >= warnEvery
	if warn {
		f.warnedErr, f.warnedAt = f.lastErr, f.now()
	}
	f.mu.Unlock()
	if warn {
		f.log.Warn("usage forwarder: the log is not reachable or did not accept the records; the files keep every record and the forwarder retries", "addr", f.o.Addr, "error", err.Error())
	}
}
