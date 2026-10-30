package evlayer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Cursor is the position a stored-cursor subscriber has handled up to. EventID is
// the id of the event at Seq, so a resumed subscriber can tell that the stream
// still holds the same history (see ErrHistoryRewritten).
type Cursor struct {
	Seq     uint64 `json:"seq"`
	EventID string `json:"event_id,omitempty"`
}

// CursorStore keeps a Cursor across restarts. In the framework this is a control
// plane object (HBEP-10, HBEP-11); the spike uses a file.
type CursorStore interface {
	Load() (Cursor, error)
	Save(Cursor) error
}

// FileCursor stores a Cursor in a JSON file, replaced atomically and fsynced.
type FileCursor struct{ Path string }

// Load returns the zero Cursor when the file does not exist yet.
func (f FileCursor) Load() (Cursor, error) {
	b, err := os.ReadFile(f.Path)
	if errors.Is(err, os.ErrNotExist) {
		return Cursor{}, nil
	}
	if err != nil {
		return Cursor{}, err
	}
	var c Cursor
	if err := json.Unmarshal(b, &c); err != nil {
		return Cursor{}, fmt.Errorf("evlayer: cursor file %s: %w", f.Path, err)
	}
	return c, nil
}

// Save writes the cursor to a temp file, fsyncs it, renames it over the old one
// and fsyncs the directory.
func (f FileCursor) Save(c Cursor) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	tmp := f.Path + ".tmp"
	fd, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := fd.Write(b); err != nil {
		fd.Close()
		return err
	}
	if err := fd.Sync(); err != nil {
		fd.Close()
		return err
	}
	if err := fd.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, f.Path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(f.Path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// ErrHistoryRewritten means the stream no longer holds what the cursor says was
// handled: the stream ends before the cursor, or the event at the cursor differs.
// The usual cause is lost acknowledged writes (a power failure under the default
// fsync policy) or a re-created stream. The subscriber must re-read status.
var ErrHistoryRewritten = errors.New("evlayer: stream history does not match the stored cursor")

// TailConfig configures a stored-cursor subscriber.
type TailConfig struct {
	Stream    string
	Store     CursorStore
	SaveEvery int           // persist the cursor after this many handled records (default 1)
	IdleCheck time.Duration // how long to wait for a message before checking for a gap (default 500ms)
	// PullMaxMessages is how many messages the client may hold ahead of the handler
	// (the nats.go default is 500). A slow subscriber holds that many events in memory,
	// so retention reaches it only after it has worked through them.
	PullMaxMessages int
}

// Tail delivers every record after the stored cursor, in stream order, on an
// ordered consumer. Delivery is at least once: the cursor is saved only after the
// handler succeeds, so a crash replays at most SaveEvery records (de-duplicate by
// event id). Ordered consumers never redeliver out of order, so per-resource
// order is the stream order. Tail returns nil when ctx ends.
func Tail(ctx context.Context, js jetstream.JetStream, cfg TailConfig, h Handler) error {
	err := tail(ctx, js, cfg, h)
	if err != nil && ctx.Err() != nil && !errors.Is(err, ErrHistoryRewritten) {
		return nil // shutting down: the cursor is still at the last handled record
	}
	return err
}

func tail(ctx context.Context, js jetstream.JetStream, cfg TailConfig, h Handler) error {
	if cfg.SaveEvery == 0 {
		cfg.SaveEvery = 1
	}
	if cfg.IdleCheck == 0 {
		cfg.IdleCheck = 500 * time.Millisecond
	}
	st, err := js.Stream(ctx, cfg.Stream)
	if err != nil {
		return err
	}
	cur, err := cfg.Store.Load()
	if err != nil {
		return err
	}
	if err := verifyCursor(ctx, st, cur); err != nil {
		return err
	}
	gaps := gapTracker{stream: cfg.Stream}
	oc := jetstream.OrderedConsumerConfig{FilterSubjects: []string{SubjectsAll}}
	if cur.Seq > 0 {
		gaps.next = cur.Seq + 1
		oc.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		oc.OptStartSeq = cur.Seq + 1
	}
	cons, err := st.OrderedConsumer(ctx, oc)
	if err != nil {
		return err
	}
	var popts []jetstream.PullMessagesOpt
	if cfg.PullMaxMessages > 0 {
		popts = append(popts, jetstream.PullMaxMessages(cfg.PullMaxMessages))
	}
	it, err := cons.Messages(popts...)
	if err != nil {
		return err
	}
	defer it.Stop()
	go func() { <-ctx.Done(); it.Stop() }()

	sinceSave := 0
	save := func(force bool) error {
		if sinceSave == 0 || (!force && sinceSave < cfg.SaveEvery) {
			return nil
		}
		sinceSave = 0
		return cfg.Store.Save(cur)
	}
	defer func() { _ = save(true) }()

	for {
		m, err := it.Next(jetstream.NextMaxWait(cfg.IdleCheck))
		switch {
		case err == nil:
		case errors.Is(err, nats.ErrTimeout):
			if err := save(true); err != nil {
				return err
			}
			si, err := st.Info(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			if gap := gaps.idle(si.State.FirstSeq); gap != nil {
				if err := h(ctx, Record{Seq: gap.To, Gap: gap}); err != nil {
					return err
				}
				cur = Cursor{Seq: gap.To, EventID: cur.EventID}
				sinceSave++
			}
			continue
		case errors.Is(err, jetstream.ErrMsgIteratorClosed):
			return nil
		default:
			return err
		}
		md, err := m.Metadata()
		if err != nil {
			return err
		}
		seq := md.Sequence.Stream
		if seq < gaps.next { // already reported as a gap by an idle check, or a replay
			continue
		}
		if gap := gaps.observe(seq); gap != nil {
			if err := h(ctx, Record{Seq: gap.To, Gap: gap}); err != nil {
				return err
			}
		}
		rec, err := decodeRecord(seq, m.Data())
		if err != nil {
			return err
		}
		if err := h(ctx, rec); err != nil {
			return err
		}
		cur = Cursor{Seq: seq, EventID: rec.Event.ID()}
		sinceSave++
		if err := save(false); err != nil {
			return err
		}
	}
}

// verifyCursor checks that the stream still holds the history the cursor refers to.
func verifyCursor(ctx context.Context, st jetstream.Stream, cur Cursor) error {
	if cur.Seq == 0 {
		return nil
	}
	si, err := st.Info(ctx)
	if err != nil {
		return err
	}
	if cur.Seq > si.State.LastSeq {
		return fmt.Errorf("%w: cursor at %d but the stream ends at %d", ErrHistoryRewritten, cur.Seq, si.State.LastSeq)
	}
	if cur.Seq < si.State.FirstSeq || cur.EventID == "" {
		return nil // evicted: reported as a gap by the tracker, nothing to compare
	}
	raw, err := st.GetMsg(ctx, cur.Seq)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil // deleted by a per-subject limit: also surfaces as a gap
	}
	if err != nil {
		return err
	}
	e, err := Decode(raw.Data)
	if err != nil {
		return err
	}
	if e.ID() != cur.EventID {
		return fmt.Errorf("%w: sequence %d holds event %q, the cursor recorded %q", ErrHistoryRewritten, cur.Seq, e.ID(), cur.EventID)
	}
	return nil
}
