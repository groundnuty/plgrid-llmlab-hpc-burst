package forward

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
)

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

// add takes one line, in rising counter order, of at most maxBytes: a longer one is the scan's or
// publishBatch's to pass over (Forwarder.skip), never a message's.
func (b *batcher) add(counter uint64, line []byte) error {
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

// skip passes over a record whose line is longer than a message (a client's model name can be as
// long as a request body): the log gets none of it, the files hold it, the error log names its
// counter, and it is counted once in Stats.Skipped. It is not lag, so it is not in the gauge: the
// read position moves past it, so the next look does not read it again, and the record is neither
// waited for nor re-counted.
func (f *Forwarder) skip(counter uint64, size int) {
	f.mu.Lock()
	f.skippedSet[counter] = struct{}{}
	f.skipPos = max(f.skipPos, counter)
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
