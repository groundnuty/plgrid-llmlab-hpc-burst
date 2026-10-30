package tests

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/evlayer"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/harness"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/jsnode"
	"github.com/nats-io/nats.go/jetstream"
)

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return v
	}
	return def
}

type killStats struct {
	Iterations, Acked, LostAcked, WrongID, UnackedPresent, IterationsWithLoss int
	MaxLostInIteration                                                        int
	// AckedPreKill and LostPreKill count the acknowledgements this process had read before
	// it sent the SIGKILL: the ones that were certainly given out before the crash.
	AckedPreKill, LostPreKill int
}

// msgGetter is the part of a stream the verification needs.
type msgGetter interface {
	GetMsg(ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error)
}

// streamLooker is the part of a JetStream handle the stream lookup needs.
type streamLooker interface {
	Stream(ctx context.Context, name string) (jetstream.Stream, error)
}

// verdict lists the acknowledged messages the stream does not hold, and those
// it holds with another event id.
type verdict struct{ Lost, Wrong []acked }

// retryable runs f until it succeeds or fails with the answer "not found", which
// is a real answer; every other error (timeout, closed connection, server still
// starting) says nothing about what the stream holds, so it is retried and then
// returned.
func retryable(ctx context.Context, attempts int, pause time.Duration, notFound error, f func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = f(); err == nil || errors.Is(err, notFound) {
			return err
		}
		select {
		case <-time.After(pause):
		case <-ctx.Done():
			return errors.Join(ctx.Err(), err)
		}
	}
	return err
}

// verifyAcked looks up every acknowledged sequence. Only the stream's own "message
// not found" answer counts a message as lost; any other error fails the
// verification instead of being counted as a loss, because a broken verification
// connection would otherwise make the async-persistence control pass without any loss.
func verifyAcked(ctx context.Context, g msgGetter, acks []acked, attempts int, pause time.Duration) (verdict, error) {
	var v verdict
	for _, a := range acks {
		var raw *jetstream.RawStreamMsg
		err := retryable(ctx, attempts, pause, jetstream.ErrMsgNotFound, func() (err error) {
			raw, err = g.GetMsg(ctx, a.Seq)
			return err
		})
		switch {
		case errors.Is(err, jetstream.ErrMsgNotFound):
			v.Lost = append(v.Lost, a)
			continue
		case err != nil:
			return v, fmt.Errorf("cannot tell whether acknowledged sequence %d is stored: %w", a.Seq, err)
		}
		if e, derr := evlayer.Decode(raw.Data); derr != nil || e.ID() != a.ID {
			v.Wrong = append(v.Wrong, a)
		}
	}
	return v, nil
}

// lookupStream returns the stream, or missing=true when the server says it does not
// exist. An unavailable server is an error, not a missing stream.
func lookupStream(ctx context.Context, js streamLooker, name string, attempts int, pause time.Duration) (st jetstream.Stream, missing bool, err error) {
	err = retryable(ctx, attempts, pause, jetstream.ErrStreamNotFound, func() (err error) {
		st, err = js.Stream(ctx, name)
		return err
	})
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil, true, nil
	}
	return st, false, err
}

func countIn(set []acked, of map[string]bool) int {
	n := 0
	for _, a := range set {
		if of[a.ID] {
			n++
		}
	}
	return n
}

// killLoop runs the SIGKILL scenario iters times. Each iteration:
//  1. a child process hosts the embedded server on a fresh store;
//  2. a second child process publishes from 8 workers and prints one line per acknowledgement,
//     which this process records;
//  3. at a random moment after the acknowledgements started, this process SIGKILLs the host;
//  4. the host restarts on the same store directory;
//  5. every recorded (sequence, event id) must be in the restarted stream.
func killLoop(t *testing.T, mode jsnode.SyncMode, async bool, iters int) killStats {
	// async is the positive control: losses and re-used sequences are expected and only logged.
	report := t.Errorf
	if async {
		report = t.Logf
	}
	seed := time.Now().UnixNano()
	if v := os.Getenv("S8_SEED"); v != "" {
		seed, _ = strconv.ParseInt(v, 10, 64)
	}
	t.Logf("mode=%s async_persist=%v iterations=%d seed=%d", mode, async, iters, seed)
	rng := rand.New(rand.NewSource(seed))
	var st killStats
	for it := 0; it < iters; it++ {
		capacity(t)
		dir := scratchDir(t, fmt.Sprintf("kill-%s-%d", mode, it))
		store := dir + "/js"
		host, url := startHost(t, store, mode)
		js := connect(t, url)
		if _, err := evlayer.EnsureStream(ctxT(t, 20*time.Second), js, evlayer.StreamOptions{Name: streamName, Async: async}); err != nil {
			t.Fatal(err)
		}
		pub, err := harness.Spawn("pub", map[string]string{"S8_URL": url, "S8_TAG": fmt.Sprintf("k%d", it), "S8_WORKERS": "8", "S8_RESOURCES": "5"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = pub.Kill() })

		var mu sync.Mutex
		var acks []acked
		var badLines []string
		doneCount := -1 // the publisher's own count of ACK lines, printed when it exits by itself
		collected := make(chan struct{})
		go func() {
			defer close(collected)
			for l := range pub.Lines {
				f := strings.Fields(l)
				mu.Lock()
				switch {
				case len(f) == 3 && f[0] == "ACK":
					seq, perr := strconv.ParseUint(f[1], 10, 64)
					if perr != nil {
						badLines = append(badLines, l)
					} else {
						acks = append(acks, acked{Seq: seq, ID: f[2]})
					}
				case len(f) == 2 && f[0] == "DONE":
					if n, perr := strconv.Atoi(f[1]); perr == nil {
						doneCount = n
					} else {
						badLines = append(badLines, l)
					}
				default:
					badLines = append(badLines, l)
				}
				mu.Unlock()
			}
		}()
		deadline := time.Now().Add(60 * time.Second)
		for {
			mu.Lock()
			n := len(acks)
			mu.Unlock()
			if n >= 50 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("iteration %d: only %d acknowledgements in 60s", it, n)
			}
			time.Sleep(2 * time.Millisecond)
		}
		time.Sleep(time.Duration(rng.Intn(250)) * time.Millisecond)
		// The acknowledgements read so far were given out before the crash. Keep them apart:
		// they are the ones a lossy capture of the publisher's output could not have invented.
		mu.Lock()
		preKill := append([]acked(nil), acks...)
		mu.Unlock()
		if err := host.KillStrict(); err != nil { // SIGKILL, no clean shutdown; fails if the host died any other way
			t.Fatalf("iteration %d: %v", it, err)
		}
		pub.Wait(5 * time.Second)
		<-collected // the publisher's stdout is read to end of file: nothing it wrote is dropped
		if err := pub.Err(); err != nil {
			t.Fatalf("iteration %d: reading the publisher's output: %v", it, err)
		}
		mu.Lock()
		got := append([]acked(nil), acks...)
		bad, dn := append([]string(nil), badLines...), doneCount
		mu.Unlock()
		_ = pub.Kill() // already gone unless it ignored its own failure; nothing to report
		if len(bad) > 0 {
			t.Fatalf("iteration %d: unparsable lines from the publisher: %q", it, bad)
		}
		if dn >= 0 && dn != len(got) {
			t.Fatalf("iteration %d: the publisher wrote %d acknowledgement lines but %d were read: the capture lost lines", it, dn, len(got))
		}
		if len(got) < len(preKill) {
			t.Fatalf("iteration %d: %d acknowledgements before the kill but only %d at the end", it, len(preKill), len(got))
		}
		preKillIDs := map[string]bool{}
		for _, a := range preKill {
			preKillIDs[a.ID] = true
		}

		// Restart on the same store directory.
		host2, url2 := startHost(t, store, mode)
		js2 := connect(t, url2)
		ctx := ctxT(t, 120*time.Second)
		stream, missing, err := lookupStream(ctx, js2, streamName, 20, 500*time.Millisecond)
		if err != nil {
			t.Fatalf("iteration %d: cannot look up the stream after the restart (server unavailable, not a missing stream): %v", it, err)
		}
		if missing {
			report("iteration %d: stream missing after SIGKILL restart: all %d acknowledged messages lost", it, len(got))
			st.Iterations++
			st.Acked += len(got)
			st.AckedPreKill += len(preKill)
			st.LostAcked += len(got)
			st.LostPreKill += len(preKill)
			st.IterationsWithLoss++
			if len(got) > st.MaxLostInIteration {
				st.MaxLostInIteration = len(got)
			}
			_ = host2.Kill() // end of the iteration
			continue
		}
		si, err := stream.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		v, err := verifyAcked(ctx, stream, got, 10, 300*time.Millisecond)
		if err != nil {
			t.Fatalf("iteration %d: %v", it, err)
		}
		lost, wrong := len(v.Lost), len(v.Wrong)
		var maxSeq uint64
		for _, a := range got {
			if a.Seq > maxSeq {
				maxSeq = a.Seq
			}
		}
		present := int(si.State.Msgs)
		st.Iterations++
		st.Acked += len(got)
		st.AckedPreKill += len(preKill)
		st.LostAcked += lost
		st.LostPreKill += countIn(v.Lost, preKillIDs) + countIn(v.Wrong, preKillIDs)
		st.WrongID += wrong
		st.UnackedPresent += present - (len(got) - lost)
		if lost+wrong > 0 {
			st.IterationsWithLoss++
			if lost+wrong > st.MaxLostInIteration {
				st.MaxLostInIteration = lost + wrong
			}
		}
		// A lost tail must not let the broker hand out an acknowledged sequence again.
		e, _ := evlayer.NewEvent(fmt.Sprintf("post-%d", it), "allocation", "r0", "io.hpcburst.test.v1", time.Now(), nil)
		ack, err := evlayer.Publish(ctx, js2, e)
		if err != nil {
			report("iteration %d: publish after restart: %v", it, err)
		} else if ack.Sequence <= maxSeq {
			report("iteration %d: sequence %d was handed out again after an acknowledgement of %d", it, ack.Sequence, maxSeq)
		}
		_ = host2.Kill() // end of the iteration
		if os.Getenv("S8_VERBOSE") == "1" || lost+wrong > 0 {
			t.Logf("iteration %d: acked=%d lost=%d wrong_id=%d stream_msgs=%d last_seq=%d", it, len(got), lost, wrong, present, si.State.LastSeq)
		}
	}
	t.Logf("RESULT mode=%s async=%v: iterations=%d acked=%d (before the kill: %d) lost_acked=%d (of those before the kill: %d) wrong_id=%d iterations_with_loss=%d unacked_but_present=%d",
		mode, async, st.Iterations, st.Acked, st.AckedPreKill, st.LostAcked, st.LostPreKill, st.WrongID, st.IterationsWithLoss, st.UnackedPresent)
	return st
}

// Acceptance test 1: SIGKILL after publish acknowledgements loses no acknowledged message.
func TestKill_SyncAlways_LosesNoAckedMessage(t *testing.T) {
	st := killLoop(t, jsnode.SyncAlways, false, envInt("S8_KILL_ITERS", 30))
	if st.LostAcked+st.WrongID > 0 || st.Acked == 0 {
		t.Fatalf("acknowledged messages lost under sync_interval: always: %+v", st)
	}
}

// Same scenario with the default fsync policy. A loss here is a finding, so it fails the test.
func TestKill_DefaultSync_LosesNoAckedMessage(t *testing.T) {
	st := killLoop(t, jsnode.SyncDefault, false, envInt("S8_KILL_ITERS", 30))
	if st.LostAcked+st.WrongID > 0 || st.Acked == 0 {
		t.Fatalf("acknowledged messages lost under the default fsync policy: %+v", st)
	}
}

// Positive control: persist_mode async acknowledges before the write, so SIGKILL
// can lose acknowledged messages. This shows the harness can see a loss, so it fails
// when it sees none.
func TestKill_Control_AsyncPersist_CanLose(t *testing.T) {
	st := killLoop(t, jsnode.SyncDefault, true, envInt("S8_KILL_ITERS", 30))
	if st.LostAcked+st.WrongID == 0 {
		t.Fatalf("CONTROL FAILED: async persist lost nothing in %d iterations, so the zero losses of the other two modes prove nothing about the harness", st.Iterations)
	} else {
		t.Logf("CONTROL OK: async persist lost %d acknowledged messages in %d of %d iterations (max %d in one)", st.LostAcked+st.WrongID, st.IterationsWithLoss, st.Iterations, st.MaxLostInIteration)
	}
}
