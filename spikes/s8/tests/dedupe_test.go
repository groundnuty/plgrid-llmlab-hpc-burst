package tests

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/evlayer"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/jsnode"
)

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
