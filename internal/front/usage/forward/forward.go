// Package forward is the front's usage forwarder: it copies the usage files' records into the
// durable log's HB_USAGE stream (HBEP-8, The durable log, Who writes and Delivery; issue #625). The
// files stay the record. The stream is a rolling copy, so the forwarder never holds a record the
// files lack, and serving never waits for the log.
//
// What it takes, and from where:
//
//   - Only what the sink has synced. The sink hands each synced batch's exact bytes to Offer, which
//     puts them in a bounded channel and never waits. When the channel is full the batch is dropped
//     and the forwarder is marked behind: it then reads the files, up to the last counter the sink
//     synced and no further.
//   - From the files after a restart, after an outage, and after any publish that failed or was not
//     acknowledged. At each start of a connection it reads the stream's last message by direct get,
//     takes <last> from its Nats-Msg-Id, usage-<first>-<last>, and publishes from the next counter
//     present in the files after it. On an empty stream it starts where startAfter says (R4).
//
// One invariant keeps the two paths from duplicating each other: every line, from the channel or
// from a file, is published only when its counter is above the last one published, and a message
// holds only lines read from one place, in order. A message's id is its own first and last counter,
// so a retry of the same batch inside the log's two-minute duplicate window stores nothing, and a
// retry with other boundaries cannot overlap, since it starts after what the stream holds.
package forward

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// Defaults of Options.
const (
	// DefaultQueueBatches is how many synced batches wait for the forwarder before it falls back
	// to the files. A batch is the records that ended together, usually one.
	DefaultQueueBatches = 256
	// DefaultMaxLines is HBEP-8's bound on one message: up to 256 usage records.
	DefaultMaxLines = 256
	// DefaultMaxBytes keeps a message under nats-server's default maximum payload of 1 MiB with room
	// for the header and the subject (HBEP-8, The durable log, Streams).
	DefaultMaxBytes = 1<<20 - 4096
	// DefaultPublishTimeout bounds one publish's wait for its acknowledgement.
	DefaultPublishTimeout = 10 * time.Second
	// DefaultConnectTimeout bounds a connection attempt.
	DefaultConnectTimeout = 3 * time.Second

	minBackoff = 100 * time.Millisecond
	maxBackoff = 2 * time.Second
	// DefaultPollEvery is Options.PollEvery's default.
	DefaultPollEvery = 5 * time.Second
	// warnEvery rate-limits the forwarder's repeated warnings about the same failure.
	warnEvery = time.Minute
)

// Options are the inputs of New.
type Options struct {
	// Addr is the log's client address, 127.0.0.1:<port base + 16>, and Token the token read from
	// <state>/eventlog/token.
	Addr, Token string
	// Dir is the usage directory: the files the forwarder reads.
	Dir string
	// Now is the clock behind Stats.BehindSince; nil is time.Now.
	Now func() time.Time
	// Logger gets the forwarder's warnings; nil logs nothing.
	Logger *slog.Logger
	// QueueBatches, MaxLines and MaxBytes bound the channel and a message; 0 takes the default. A
	// test lowers them to make the files carry the load.
	QueueBatches, MaxLines, MaxBytes int
	// PublishTimeout and ConnectTimeout bound the waits on the log; 0 takes the default.
	PublishTimeout, ConnectTimeout time.Duration
	// PollEvery is how often an idle forwarder compares what the sink has synced with what it has
	// published, a safety net beside the channel; 0 takes the default, 5 s.
	PollEvery time.Duration
}

type batch struct {
	first, last uint64
	lines       []byte
}

// Forwarder copies synced usage records to the durable log. Offer is its intake; Run is its loop.
type Forwarder struct {
	o      Options
	log    *slog.Logger
	now    func() time.Time
	ch     chan batch
	wake   chan struct{}
	behind atomic.Bool // a batch was dropped: the channel no longer holds all that was synced
	// caughtUp counts the sessions that finished their first catch-up from the files: from then on
	// the forwarder takes batches from the channel. Tests wait on it.
	caughtUp atomic.Uint64

	// beforePublish, when set (tests only), is called with each message's first and last counter
	// just before it is published.
	beforePublish func(first, last uint64)

	mu          sync.Mutex
	connected   bool
	loggedKnown bool   // Logged holds a counter the log has acknowledged, or the stream's own last
	logged      uint64 // the last counter the stream holds, as far as the forwarder knows
	synced      uint64 // the last counter the sink has synced
	behindSince time.Time
	failures    uint64
	dropped     uint64
	fromChannel uint64
	fromFiles   uint64
	skipped     uint64
	lastErr     string
	warnedAt    time.Time
	warnedErr   string
}

// New makes a forwarder. It does nothing until Run.
func New(o Options) *Forwarder {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.QueueBatches <= 0 {
		o.QueueBatches = DefaultQueueBatches
	}
	if o.MaxLines <= 0 {
		o.MaxLines = DefaultMaxLines
	}
	if o.MaxBytes <= 0 {
		o.MaxBytes = DefaultMaxBytes
	}
	if o.PublishTimeout <= 0 {
		o.PublishTimeout = DefaultPublishTimeout
	}
	if o.ConnectTimeout <= 0 {
		o.ConnectTimeout = DefaultConnectTimeout
	}
	if o.PollEvery <= 0 {
		o.PollEvery = DefaultPollEvery
	}
	return &Forwarder{o: o, log: o.Logger, now: o.Now, ch: make(chan batch, o.QueueBatches), wake: make(chan struct{}, 1)}
}

// Offer is the sink's hook (usage.Options.Synced): it takes a synced batch, the lines exactly as
// the file holds them, and never waits. When the channel is full it drops the batch, counts it, and
// marks the forwarder behind, so that it reads the files. Offer takes ownership of lines.
func (f *Forwarder) Offer(first, last uint64, lines []byte) {
	f.mu.Lock()
	f.synced = max(f.synced, last)
	f.recomputeLocked()
	f.mu.Unlock()
	select {
	case f.ch <- batch{first: first, last: last, lines: lines}:
	default:
		f.mu.Lock()
		f.dropped++
		f.mu.Unlock()
		f.behind.Store(true)
		select {
		case f.wake <- struct{}{}:
		default:
		}
	}
}

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

// Run connects to the log and forwards until ctx ends. It returns once the connection is closed.
// lastSynced reads the sink's Stats().LastSynced: what the files hold when Run starts counts as
// synced, and every later batch arrives through Offer.
func (f *Forwarder) Run(ctx context.Context, lastSynced func() uint64) {
	f.mu.Lock()
	f.synced = max(f.synced, lastSynced())
	f.recomputeLocked()
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.connected = false
		f.mu.Unlock()
	}()
	backoff := minBackoff
	for ctx.Err() == nil {
		served, err := f.session(ctx)
		if ctx.Err() != nil {
			return
		}
		f.setDown(err)
		if served {
			backoff = minBackoff
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// session is one connection's life: connect, resume from the stream, catch up from the files, then
// take batches from the channel. It returns the error that ended it, and whether it got as far as
// serving.
func (f *Forwarder) session(ctx context.Context) (served bool, err error) {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	nc, err := nats.Connect("nats://"+f.o.Addr, nats.Token(f.o.Token), nats.NoReconnect(), nats.Timeout(f.o.ConnectTimeout),
		nats.Name("hpcburst-front-usage"), nats.ClosedHandler(func(*nats.Conn) { cancel() }))
	if err != nil {
		return false, fmt.Errorf("connect to the log at %s: %w", f.o.Addr, err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return false, err
	}
	st, err := js.Stream(sctx, eventlog.StreamUsage)
	if err != nil {
		return false, fmt.Errorf("the log has no usable stream %s: %w", eventlog.StreamUsage, closedAs(sctx, err))
	}
	info, err := st.Info(sctx)
	if err != nil {
		return false, fmt.Errorf("read the configuration of %s: %w", eventlog.StreamUsage, closedAs(sctx, err))
	}
	last, have, err := lastLogged(sctx, st)
	if err != nil {
		return false, closedAs(sctx, err)
	}
	if !have {
		// An empty stream (HBEP-8, Delivery; R4): start from the files that fit its cap.
		if last, err = startAfter(f.o.Dir, info.Config.MaxBytes); err != nil {
			return false, fmt.Errorf("choose where to start in the usage files: %w", err)
		}
	}
	if synced := f.syncedNow(); have && last > synced {
		f.log.Error("usage forwarder: the log holds counters past the last one in the usage files; records written now may carry counters the log already holds and are then not forwarded until the counters pass it",
			"logged", last, "files", synced)
	}
	f.setLogged(last)
	f.setUp()
	maxBytes := f.o.MaxBytes
	if mp := int(nc.MaxPayload()); mp > 0 {
		maxBytes = min(maxBytes, mp-4096)
	}
	return true, closedAs(sctx, f.serve(sctx, js, maxBytes))
}

// closedAs says a connection that closed under a call, in place of the call's context error.
func closedAs(ctx context.Context, err error) error {
	if err != nil && ctx.Err() != nil && errors.Is(err, context.Canceled) {
		return errors.New("the connection to the log closed")
	}
	return err
}

// lastLogged reads the stream's last message by direct get and returns the <last> of its
// Nats-Msg-Id, usage-<first>-<last>; have is false for an empty stream.
func lastLogged(ctx context.Context, st jetstream.Stream) (last uint64, have bool, err error) {
	m, err := st.GetLastMsgForSubject(ctx, eventlog.SubjectUsage)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read the last message of %s: %w", eventlog.StreamUsage, err)
	}
	id := m.Header.Get(nats.MsgIdHdr)
	_, last, ok := parseID(id)
	if !ok {
		return 0, false, fmt.Errorf("the last message of %s (sequence %d) has the id %q, not usage-<first>-<last>", eventlog.StreamUsage, m.Sequence, id)
	}
	return last, true, nil
}

// messageID is the Nats-Msg-Id of a message holding counters first to last.
func messageID(first, last uint64) string {
	return "usage-" + strconv.FormatUint(first, 10) + "-" + strconv.FormatUint(last, 10)
}

// parseID is messageID's inverse.
func parseID(id string) (first, last uint64, ok bool) {
	rest, found := strings.CutPrefix(id, "usage-")
	if !found {
		return 0, 0, false
	}
	a, b, found := strings.Cut(rest, "-")
	if !found {
		return 0, 0, false
	}
	var err error
	if first, err = strconv.ParseUint(a, 10, 64); err != nil {
		return 0, 0, false
	}
	if last, err = strconv.ParseUint(b, 10, 64); err != nil || last < first {
		return 0, 0, false
	}
	return first, last, true
}

// serve is a session's loop: catch up from the files whenever the channel cannot be trusted to hold
// everything (at the start, after a drop, or when the sink's synced counter is past what was
// published), and otherwise publish each batch the sink offers. It returns when a call fails or ctx
// ends.
func (f *Forwarder) serve(ctx context.Context, js jetstream.JetStream, maxBytes int) error {
	poll := time.NewTicker(f.o.PollEvery)
	defer poll.Stop()
	catchUp, first := true, true
	for {
		if catchUp || f.behind.Load() {
			if err := f.catchUp(ctx, js, maxBytes); err != nil {
				return err
			}
			catchUp = false
			if first {
				first = false
				f.caughtUp.Add(1)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case b := <-f.ch:
			if f.behind.Load() {
				continue // the files, not the channel, are now the source: they hold this batch too
			}
			if err := f.publishBatch(ctx, js, b.lines, maxBytes); err != nil {
				return err
			}
		case <-f.wake:
		case <-poll.C:
			// Records synced that the stream lacks, with nothing waiting in the channel to carry them
			// (a batch in the channel is published by the next turn, and is not a reason to read the files).
			catchUp = len(f.ch) == 0 && f.syncedNow() > f.loggedNow()
		}
	}
}

func (f *Forwarder) loggedNow() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.logged
}

// catchUp publishes what the files hold after the stream's last counter, up to the last counter the
// sink has synced. It first clears the behind mark and empties the channel: whatever the channel
// held was synced before it was emptied, so the files read next hold it, and a batch offered
// meanwhile either waits in the channel, where the counter filter drops what the files already gave,
// or is dropped and marks the forwarder behind again.
func (f *Forwarder) catchUp(ctx context.Context, js jetstream.JetStream, maxBytes int) error {
	f.behind.Store(false)
	for drained := false; !drained; {
		select {
		case <-f.ch:
		default:
			drained = true
		}
	}
	after, bound := f.loggedNow(), f.syncedNow()
	if bound <= after {
		return nil
	}
	b := f.newBatcher(ctx, js, maxBytes, sourceFiles)
	_, err := scanFiles(f.o.Dir, after, bound, func(file string, line int) {
		f.log.Warn("usage forwarder: a line of a usage file has no counter and is not forwarded", "file", file, "line", line)
	}, func(counter uint64, line []byte) error { return b.add(counter, line) })
	if err != nil {
		return fmt.Errorf("read the usage files after counter %d: %w", after, err)
	}
	return b.flush()
}

// publishBatch publishes the lines of one synced batch that carry a counter above the stream's last,
// in messages of at most MaxLines lines and maxBytes bytes.
func (f *Forwarder) publishBatch(ctx context.Context, js jetstream.JetStream, lines []byte, maxBytes int) error {
	b := f.newBatcher(ctx, js, maxBytes, sourceChannel)
	prev := f.loggedNow()
	for rest := lines; len(rest) > 0; {
		i := bytes.IndexByte(rest, '\n')
		if i < 0 {
			break // not a whole line: the sink writes whole ones
		}
		line := rest[:i+1]
		rest = rest[i+1:]
		counter, ok := usage.LineCounter(line)
		if !ok || counter <= prev {
			continue
		}
		prev = counter
		if err := b.add(counter, line); err != nil {
			return err
		}
	}
	return b.flush()
}

// source says where a message's lines came from, for Stats.
type source int

const (
	sourceChannel source = iota
	sourceFiles
)

// batcher collects lines into messages of at most maxLines lines and maxBytes bytes, and publishes
// each when it is full or flushed.
type batcher struct {
	f        *Forwarder
	ctx      context.Context
	js       jetstream.JetStream
	src      source
	maxBytes int

	buf         []byte
	first, last uint64
	lines       int
}

func (f *Forwarder) newBatcher(ctx context.Context, js jetstream.JetStream, maxBytes int, src source) *batcher {
	return &batcher{f: f, ctx: ctx, js: js, src: src, maxBytes: maxBytes}
}

// add takes one line, in rising counter order. A line longer than a message is passed over, after
// what was collected before it is published: a client's model name can be as long as a request body,
// the files hold the record, and the error log says so.
func (b *batcher) add(counter uint64, line []byte) error {
	if len(line) > b.maxBytes {
		if err := b.flush(); err != nil {
			return err
		}
		b.f.skip(counter, len(line))
		return nil
	}
	if b.lines > 0 && (b.lines >= b.f.o.MaxLines || len(b.buf)+len(line) > b.maxBytes) {
		if err := b.flush(); err != nil {
			return err
		}
	}
	if b.lines == 0 {
		b.first = counter
	}
	b.buf = append(b.buf, line...)
	b.last = counter
	b.lines++
	return nil
}

// flush publishes what was collected.
func (b *batcher) flush() error {
	if b.lines == 0 {
		return nil
	}
	err := b.f.publish(b.ctx, b.js, b.first, b.last, b.buf, b.src)
	b.buf, b.lines = b.buf[:0], 0
	return err
}

// skip passes over a record whose line is longer than a message: the position moves past it, so the
// next look does not read it again, and Stats.Skipped counts it.
func (f *Forwarder) skip(counter uint64, size int) {
	f.mu.Lock()
	f.skipped++
	f.logged = max(f.logged, counter)
	f.recomputeLocked()
	f.mu.Unlock()
	f.log.Error("usage forwarder: a record is longer than a message and is not forwarded; the files hold it", "counter", counter, "bytes", size)
}

// publish stores one message and waits for its acknowledgement. Nothing is retried here: any
// failure ends the session, and the next one reads the stream's last message again, so an
// unacknowledged message that was stored is not published twice.
func (f *Forwarder) publish(ctx context.Context, js jetstream.JetStream, first, last uint64, payload []byte, src source) error {
	if f.beforePublish != nil {
		f.beforePublish(first, last)
	}
	pctx, cancel := context.WithTimeout(ctx, f.o.PublishTimeout)
	defer cancel()
	id := messageID(first, last)
	ack, err := js.PublishMsg(pctx, &nats.Msg{Subject: eventlog.SubjectUsage, Data: payload}, jetstream.WithMsgID(id))
	if err != nil {
		return fmt.Errorf("publish %s: %w", id, err)
	}
	if ack.Duplicate {
		// The stream already holds this id, which the forwarder's position says it does not: its
		// view is stale. The next session reads the stream's last message.
		return fmt.Errorf("the log already holds %s, so the forwarder's position was stale", id)
	}
	f.mu.Lock()
	f.logged, f.loggedKnown = last, true
	if src == sourceFiles {
		f.fromFiles++
	} else {
		f.fromChannel++
	}
	f.recomputeLocked()
	f.mu.Unlock()
	return nil
}
