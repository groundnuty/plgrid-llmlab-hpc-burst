package tests

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/evlayer"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/harness"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/jsnode"
	"github.com/nats-io/nats.go/jetstream"
)

const streamName = "EV"

// scratchDir returns a fresh directory under the scratch area, removed at the
// end of the test unless S8_KEEP=1.
func scratchDir(t *testing.T, name string) string {
	t.Helper()
	d, err := harness.NewDir(strings.ReplaceAll(name, "/", "_"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if os.Getenv("S8_KEEP") != "1" {
			_ = os.RemoveAll(d)
		}
	})
	return d
}

func capacity(t *testing.T) {
	t.Helper()
	if err := harness.WaitForCapacity(context.Background(), 4.0, 8); err != nil {
		t.Fatal(err)
	}
}

// startHost starts a child process that hosts the embedded server on store.
// It checks the READY line: the effective fsync setting must be the requested
// one (a mistyped option would silently fall back to the default).
func startHost(t *testing.T, store string, mode jsnode.SyncMode, wrap ...string) (*harness.Child, string) {
	t.Helper()
	h, err := harness.Spawn("host", map[string]string{"S8_STORE": store, "S8_SYNC": string(mode)}, wrap...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Kill() })
	line, err := h.WaitLine("READY ", 60*time.Second)
	if err != nil {
		t.Fatalf("host start: %v", err)
	}
	f := strings.Fields(line) // READY url sync_always=.. sync_interval=..
	wantAlways := fmt.Sprintf("sync_always=%v", mode == jsnode.SyncAlways)
	if len(f) != 4 || f[2] != wantAlways {
		t.Fatalf("host reports %q, wanted %s", line, wantAlways)
	}
	return h, f[1]
}

func connect(t *testing.T, url string) jetstream.JetStream {
	t.Helper()
	nc, js, err := evlayer.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return js
}

// inProcNode starts an embedded node inside the test process.
func inProcNode(t *testing.T, mode jsnode.SyncMode) (*jsnode.Node, string) {
	t.Helper()
	dir := scratchDir(t, t.Name()+"-js")
	n, err := jsnode.Start(jsnode.Config{StoreDir: dir + "/js", Sync: mode})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Stop)
	return n, dir
}

func ctxT(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// acked is one publish the broker acknowledged.
type acked struct {
	Seq      uint64
	ID       string
	Resource string // "kind/name"
	N        int    // per-resource publish number, assigned by the test
}

// publishSeq publishes n events round-robin over resources, one at a time, and
// returns what the broker acknowledged. Expected orders in the tests are derived
// from this list, never from the subscriber under test.
func publishSeq(ctx context.Context, t *testing.T, js jetstream.JetStream, tag string, n, resources int) []acked {
	t.Helper()
	var out []acked
	per := map[string]int{}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("r%d", i%resources)
		key := evlayer.ResourceKey("allocation", name)
		per[key]++
		id := fmt.Sprintf("%s-%d", tag, i)
		e, err := evlayer.NewEvent(id, "allocation", name, "io.hpcburst.test.v1", time.Now(), map[string]int{"n": per[key]})
		if err != nil {
			t.Fatal(err)
		}
		ack, err := evlayer.Publish(ctx, js, e)
		if err != nil {
			t.Fatalf("publish %s: %v", id, err)
		}
		out = append(out, acked{Seq: ack.Sequence, ID: id, Resource: key, N: per[key]})
	}
	return out
}
