package evlayer

import (
	"context"
	"errors"
	"fmt"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// StreamOptions are the retention knobs a test or deployment sets.
type StreamOptions struct {
	Name     string
	MaxMsgs  int64         // 0 = unlimited
	MaxAge   time.Duration // 0 = unlimited
	MaxBytes int64         // 0 = unlimited
	Async    bool          // persist_mode: async (acks before flush): positive control only
}

// StreamConfig returns the stream definition used by every test: one file-backed
// replica, limits retention (evicts oldest first), a two-minute de-duplication
// window and direct get (needed by the re-sequencer).
func StreamConfig(o StreamOptions) jetstream.StreamConfig {
	c := jetstream.StreamConfig{
		Name:        o.Name,
		Subjects:    []string{SubjectsAll},
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		Replicas:    1,
		Discard:     jetstream.DiscardOld,
		MaxMsgs:     o.MaxMsgs,
		MaxAge:      o.MaxAge,
		MaxBytes:    o.MaxBytes,
		Duplicates:  2 * time.Minute,
		AllowDirect: true,
	}
	if o.MaxAge > 0 && o.MaxAge < c.Duplicates {
		c.Duplicates = o.MaxAge // the server refuses a duplicate window longer than MaxAge
	}
	if c.MaxMsgs == 0 {
		c.MaxMsgs = -1
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = -1
	}
	if o.Async {
		c.PersistMode = jetstream.AsyncPersistMode
	}
	return c
}

// EnsureStream creates the stream if absent and refuses to continue when an
// existing stream differs in the retention limits it was asked for.
func EnsureStream(ctx context.Context, js jetstream.JetStream, o StreamOptions) (jetstream.Stream, error) {
	want := StreamConfig(o)
	st, err := js.CreateOrUpdateStream(ctx, want)
	if err != nil {
		return nil, fmt.Errorf("evlayer: create stream %q: %w", o.Name, err)
	}
	got := st.CachedInfo().Config
	if got.MaxMsgs != want.MaxMsgs || got.MaxAge != want.MaxAge || got.Replicas != 1 || got.Storage != jetstream.FileStorage {
		return nil, fmt.Errorf("evlayer: stream %q config differs from the request: got msgs=%d age=%s replicas=%d", o.Name, got.MaxMsgs, got.MaxAge, got.Replicas)
	}
	return st, nil
}

// Connect opens a client connection and a JetStream handle.
func Connect(url string, opts ...nats.Option) (*nats.Conn, jetstream.JetStream, error) {
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	return nc, js, nil
}

// Publish stores one event and returns the broker's acknowledgement. The event
// id doubles as Nats-Msg-Id, so a retry of an unacknowledged publish with the
// same event cannot create a second copy inside the stream's duplicate window.
// An acknowledgement means the message is stored under the node's fsync policy.
func Publish(ctx context.Context, js jetstream.JetStream, e cloudevents.Event) (*jetstream.PubAck, error) {
	body, err := Encode(e)
	if err != nil {
		return nil, err
	}
	msg := &nats.Msg{Subject: NATSSubject(e.Subject()), Data: body}
	return js.PublishMsg(ctx, msg, jetstream.WithMsgID(e.ID()))
}

// PublishRetry retries Publish on timeouts and temporary errors until the
// context ends. It is at-least-once on the wire. The stream stores one copy as
// long as the retries fall inside its duplicate window (two minutes in
// StreamConfig); a retry later than that stores a second copy.
func PublishRetry(ctx context.Context, js jetstream.JetStream, e cloudevents.Event) (*jetstream.PubAck, error) {
	var last error
	for {
		ack, err := Publish(ctx, js, e)
		if err == nil {
			return ack, nil
		}
		last = err
		if ctx.Err() != nil {
			return nil, errors.Join(ctx.Err(), last)
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return nil, errors.Join(ctx.Err(), last)
		}
	}
}
