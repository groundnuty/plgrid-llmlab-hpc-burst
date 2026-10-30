package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/evlayer"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/jsnode"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type getterFunc func(ctx context.Context, seq uint64) (*jetstream.RawStreamMsg, error)

func (f getterFunc) GetMsg(ctx context.Context, seq uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	return f(ctx, seq)
}

func rawFor(t *testing.T, seq uint64, id string) *jetstream.RawStreamMsg {
	t.Helper()
	e, err := evlayer.NewEvent(id, "allocation", "r0", "io.hpcburst.test.v1", time.Now(), nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := evlayer.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	return &jetstream.RawStreamMsg{Sequence: seq, Data: b}
}

func seqsOf(as []acked) []uint64 {
	var out []uint64
	for _, a := range as {
		out = append(out, a.Seq)
	}
	return out
}

// Only the stream's own "message not found" counts a message as lost. The expected
// classification is fixed by construction of the fake, not read from the code under test.
func TestVerifyAcked_OnlyNotFoundCountsAsLost(t *testing.T) {
	acks := []acked{{Seq: 1, ID: "a"}, {Seq: 2, ID: "b"}, {Seq: 3, ID: "c"}, {Seq: 4, ID: "d"}}
	var timeouts3 int // seq 3 times out twice, then answers "not found"
	g := getterFunc(func(_ context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
		switch seq {
		case 1:
			return rawFor(t, 1, "a"), nil
		case 2:
			return rawFor(t, 2, "other"), nil // stored, but a different event
		case 3:
			if timeouts3 < 2 {
				timeouts3++
				return nil, nats.ErrTimeout
			}
			return nil, jetstream.ErrMsgNotFound
		default:
			return nil, jetstream.ErrMsgNotFound
		}
	})
	v, err := verifyAcked(context.Background(), g, acks, 5, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if got := seqsOf(v.Lost); len(got) != 2 || got[0] != 3 || got[1] != 4 {
		t.Errorf("lost = %v, want [3 4]", got)
	}
	if got := seqsOf(v.Wrong); len(got) != 1 || got[0] != 2 {
		t.Errorf("wrong id = %v, want [2]", got)
	}
}

// An error that is not "not found" must fail the verification, not be counted as a loss.
func TestVerifyAcked_UnavailableStreamIsAnErrorNotALoss(t *testing.T) {
	acks := []acked{{Seq: 1, ID: "a"}, {Seq: 2, ID: "b"}, {Seq: 3, ID: "c"}}
	g := getterFunc(func(_ context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
		if seq == 2 {
			return nil, nats.ErrTimeout
		}
		return rawFor(t, seq, map[uint64]string{1: "a", 3: "c"}[seq]), nil
	})
	v, err := verifyAcked(context.Background(), g, acks, 3, time.Millisecond)
	if !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("error = %v, want it to wrap the timeout", err)
	}
	if len(v.Lost) != 0 {
		t.Fatalf("a timeout was counted as %d lost message(s)", len(v.Lost))
	}
}

// The same through a real connection that is closed: every read fails, and none of
// them may count as a loss (that would let the async control pass on a dead connection).
func TestVerifyAcked_ClosedConnectionIsAnErrorNotALoss(t *testing.T) {
	n, _ := inProcNode(t, jsnode.SyncAlways)
	nc, js, err := evlayer.Connect(n.URL())
	if err != nil {
		t.Fatal(err)
	}
	ctx := ctxT(t, 30*time.Second)
	st, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName})
	if err != nil {
		t.Fatal(err)
	}
	acks := publishSeq(ctx, t, js, "v", 5, 2)
	if v, err := verifyAcked(ctx, st, acks, 2, time.Millisecond); err != nil || len(v.Lost)+len(v.Wrong) != 0 {
		t.Fatalf("setup: with a live connection every message should be found: %+v, %v", v, err)
	}
	nc.Close()
	v, err := verifyAcked(ctx, st, acks, 2, time.Millisecond)
	if err == nil {
		t.Fatalf("verification over a closed connection returned no error (lost=%d)", len(v.Lost))
	}
	if len(v.Lost) != 0 {
		t.Fatalf("a closed connection was counted as %d lost message(s)", len(v.Lost))
	}
}

type lookerFunc func(ctx context.Context, name string) (jetstream.Stream, error)

func (f lookerFunc) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	return f(ctx, name)
}

// A stream the server says is missing is a different fact from a server that does not answer.
func TestLookupStream_MissingIsNotUnavailable(t *testing.T) {
	ctx := context.Background()
	t.Run("server_says_missing", func(t *testing.T) {
		n, _ := inProcNode(t, jsnode.SyncAlways)
		js := connect(t, n.URL())
		st, missing, err := lookupStream(ctxT(t, 30*time.Second), js, "NOPE", 3, time.Millisecond)
		if err != nil || !missing || st != nil {
			t.Fatalf("lookup of a stream that does not exist: stream=%v missing=%v err=%v", st, missing, err)
		}
		if _, err := evlayer.EnsureStream(ctxT(t, 30*time.Second), js, evlayer.StreamOptions{Name: streamName}); err != nil {
			t.Fatal(err)
		}
		st, missing, err = lookupStream(ctxT(t, 30*time.Second), js, streamName, 3, time.Millisecond)
		if err != nil || missing || st == nil {
			t.Fatalf("lookup of an existing stream: stream=%v missing=%v err=%v", st, missing, err)
		}
	})
	t.Run("server_does_not_answer", func(t *testing.T) {
		calls := 0
		l := lookerFunc(func(context.Context, string) (jetstream.Stream, error) { calls++; return nil, nats.ErrTimeout })
		_, missing, err := lookupStream(ctx, l, streamName, 3, time.Millisecond)
		if missing || !errors.Is(err, nats.ErrTimeout) {
			t.Fatalf("an unavailable server was reported as missing=%v err=%v", missing, err)
		}
		if calls != 3 {
			t.Fatalf("an unavailable server was asked %d times, want 3 attempts", calls)
		}
	})
	t.Run("closed_connection", func(t *testing.T) {
		n, _ := inProcNode(t, jsnode.SyncAlways)
		nc, js, err := evlayer.Connect(n.URL())
		if err != nil {
			t.Fatal(err)
		}
		nc.Close()
		_, missing, err := lookupStream(ctxT(t, 30*time.Second), js, streamName, 2, time.Millisecond)
		if missing || err == nil {
			t.Fatalf("closed connection: missing=%v err=%v, want an error", missing, err)
		}
	})
}
