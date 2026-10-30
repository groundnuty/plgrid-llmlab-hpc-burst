package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/evlayer"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/jsnode"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// lostAckJS wraps a JetStream handle. Its first `lose` PublishMsg calls store the message for
// real and then report a timeout, as if the broker's acknowledgement had been lost on its way
// back: the failure a retry exists for (stored, but the publisher cannot know).
type lostAckJS struct {
	jetstream.JetStream
	lose  int
	mu    sync.Mutex
	calls int
}

func (l *lostAckJS) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	ack, err := l.JetStream.PublishMsg(ctx, msg, opts...)
	l.mu.Lock()
	l.calls++
	lost := l.calls <= l.lose
	l.mu.Unlock()
	if err == nil && lost {
		return nil, nats.ErrTimeout
	}
	return ack, err
}

// The acknowledgement is lost AFTER the broker stored the event. A plain Publish reports a
// failure; PublishRetry sends the same event again, the stream recognises the event id inside
// its duplicate window and answers with the original sequence, and exactly one copy is stored.
func TestPublishRetry_RecoversFromAnAcknowledgementLostAfterStorage(t *testing.T) {
	n, _ := inProcNode(t, jsnode.SyncAlways)
	js := connect(t, n.URL())
	ctx := ctxT(t, 30*time.Second)
	st, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName})
	if err != nil {
		t.Fatal(err)
	}
	stored := func() uint64 {
		si, err := st.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return si.State.Msgs
	}
	e, err := evlayer.NewEvent("lost-ack", "allocation", "r0", "io.hpcburst.test.v1", time.Now(), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Without a retry the publisher sees a failure although the event is stored.
	if _, err := evlayer.Publish(ctx, &lostAckJS{JetStream: js, lose: 1}, e); err == nil {
		t.Fatal("setup: the injected loss of the acknowledgement did not reach the publisher")
	}
	if stored() != 1 {
		t.Fatalf("setup: the broker should have stored the event before the acknowledgement was lost, stream holds %d", stored())
	}

	// With the retry, a fresh event whose first acknowledgement is lost ends with one copy.
	e2, err := evlayer.NewEvent("lost-ack-2", "allocation", "r0", "io.hpcburst.test.v1", time.Now(), nil)
	if err != nil {
		t.Fatal(err)
	}
	lossy := &lostAckJS{JetStream: js, lose: 1}
	ack, err := evlayer.PublishRetry(ctx, lossy, e2)
	if err != nil {
		t.Fatal(err)
	}
	if lossy.calls != 2 {
		t.Errorf("PublishRetry sent the event %d times, want 2 (one lost acknowledgement, one retry)", lossy.calls)
	}
	if !ack.Duplicate || ack.Sequence != 2 {
		t.Errorf("the retry was acknowledged as %+v, want the original sequence 2 flagged as a duplicate", ack)
	}
	if stored() != 2 {
		t.Errorf("stream holds %d messages, want 2: one copy of each event", stored())
	}
}

// A publisher that did not see an acknowledgement retries with the same event. The
// event id is the de-duplication key, so the stream holds one copy.
func TestPublishRetryIsDeduplicatedByEventID(t *testing.T) {
	n, _ := inProcNode(t, jsnode.SyncAlways)
	js := connect(t, n.URL())
	ctx := ctxT(t, 30*time.Second)
	st, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName})
	if err != nil {
		t.Fatal(err)
	}
	e, err := evlayer.NewEvent("same-id", "allocation", "r0", "io.hpcburst.test.v1", time.Now(), nil)
	if err != nil {
		t.Fatal(err)
	}
	a1, err := evlayer.PublishRetry(ctx, js, e)
	if err != nil {
		t.Fatal(err)
	}
	a2, err := evlayer.PublishRetry(ctx, js, e)
	if err != nil {
		t.Fatal(err)
	}
	si, _ := st.Info(ctx)
	if a1.Duplicate || !a2.Duplicate || a1.Sequence != a2.Sequence || si.State.Msgs != 1 {
		t.Fatalf("first=%+v second=%+v stream holds %d, want one copy and the second flagged duplicate", a1, a2, si.State.Msgs)
	}
}

// Subject tokens are validated before they reach the broker.
func TestResourceNamesAreValidated(t *testing.T) {
	for _, bad := range [][2]string{{"allocation", ""}, {"allocation", "a.b"}, {"allocation", "a*"}, {"allocation", "a>"}, {"alloc ation", "x"}, {"", "x"}, {"allocation", "a/b"}} {
		if _, err := evlayer.NewEvent("id", bad[0], bad[1], "t", time.Now(), nil); err == nil {
			t.Errorf("kind=%q name=%q was accepted", bad[0], bad[1])
		}
	}
}
