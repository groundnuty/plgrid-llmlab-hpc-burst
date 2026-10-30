package tests

import (
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
		t.Cleanup(func() { pub.Kill() })

		var mu sync.Mutex
		var acks []acked
		collected := make(chan struct{})
		go func() {
			defer close(collected)
			for l := range pub.Lines {
				f := strings.Fields(l)
				if len(f) != 3 || f[0] != "ACK" {
					continue
				}
				seq, _ := strconv.ParseUint(f[1], 10, 64)
				mu.Lock()
				acks = append(acks, acked{Seq: seq, ID: f[2]})
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
		host.Kill() // SIGKILL, no clean shutdown
		pub.Wait(5 * time.Second)
		<-collected
		mu.Lock()
		got := append([]acked(nil), acks...)
		mu.Unlock()
		pub.Kill()

		// Restart on the same store directory.
		host2, url2 := startHost(t, store, mode)
		js2 := connect(t, url2)
		ctx := ctxT(t, 120*time.Second)
		stream, err := js2.Stream(ctx, streamName)
		if err != nil {
			report("iteration %d: stream missing after SIGKILL restart: %v", it, err)
			st.IterationsWithLoss++
			host2.Kill()
			continue
		}
		si, err := stream.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		lost, wrong := 0, 0
		var maxSeq uint64
		for _, a := range got {
			if a.Seq > maxSeq {
				maxSeq = a.Seq
			}
			raw, err := stream.GetMsg(ctx, a.Seq)
			if err != nil {
				lost++
				continue
			}
			e, err := evlayer.Decode(raw.Data)
			if err != nil || e.ID() != a.ID {
				wrong++
			}
		}
		present := int(si.State.Msgs)
		st.Iterations++
		st.Acked += len(got)
		st.LostAcked += lost
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
		host2.Kill()
		if os.Getenv("S8_VERBOSE") == "1" || lost+wrong > 0 {
			t.Logf("iteration %d: acked=%d lost=%d wrong_id=%d stream_msgs=%d last_seq=%d", it, len(got), lost, wrong, present, si.State.LastSeq)
		}
	}
	t.Logf("RESULT mode=%s async=%v: iterations=%d acked=%d lost_acked=%d wrong_id=%d iterations_with_loss=%d unacked_but_present=%d",
		mode, async, st.Iterations, st.Acked, st.LostAcked, st.WrongID, st.IterationsWithLoss, st.UnackedPresent)
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
// can lose acknowledged messages. This shows the harness can see a loss. It never
// fails the suite; the result is logged.
func TestKill_Control_AsyncPersist_CanLose(t *testing.T) {
	st := killLoop(t, jsnode.SyncDefault, true, envInt("S8_KILL_ITERS", 30))
	if st.LostAcked+st.WrongID == 0 {
		t.Logf("CONTROL INCONCLUSIVE: async persist lost nothing in %d iterations, so the harness has not been shown to detect a loss", st.Iterations)
	} else {
		t.Logf("CONTROL OK: async persist lost %d acknowledged messages in %d of %d iterations (max %d in one)", st.LostAcked+st.WrongID, st.IterationsWithLoss, st.Iterations, st.MaxLostInIteration)
	}
}
